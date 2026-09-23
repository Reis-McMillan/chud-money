//! One feed task: a Kalshi websocket session that streams the market's CF
//! Benchmarks 5Hz index and the tickers and orderbooks of the series' open
//! markets.
//!
//! Index values are broadcast to proxy subscribers and queued for QuestDB.
//! Ticker frames are queued for QuestDB. Orderbook frames are broadcast,
//! applied to the cached books and queued for QuestDB as an event log: the
//! snapshot Kalshi sends when a market is (re)subscribed, then its deltas. A
//! gap in the orderbook sequence numbers ends the session, so the reconnect
//! stores a fresh snapshot rather than a book with a hole in it. The series
//! rolls every 15 minutes, so open markets are re-polled and both per-market
//! subscriptions are updated in place. The session reconnects with
//! exponential backoff.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use super::FeedShared;
use crate::db::questdb::{BookKind, ContractBookRow, ContractTickerRow, IndexRow, Row, Table};
use crate::kalshi::book::{OrderBook, OrderbookMsg, price_to_dollars, qty_to_contracts};
use crate::kalshi::client::OpenMarket;
use crate::kalshi::stream::{
    CHANNEL_ORDERBOOK, CHANNEL_TICKER, CfValue5Hz, Envelope, Subscribed, TickerMsg, decimal, subscribe_index,
    subscribe_orderbook, subscribe_ticker, update_markets_cmd,
};
use crate::state::AppState;

const MARKET_REFRESH: Duration = Duration::from_secs(30);
const PING_INTERVAL: Duration = Duration::from_secs(10);
const STALE_AFTER: Duration = Duration::from_secs(30);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

const SOURCE_TICKER: &str = "ws_ticker";
const SOURCE_BOOK: &str = "ws_orderbook";

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sub {
    Index,
    Orderbook,
    Ticker,
}

/// Per-session bookkeeping shared between the select loop and `handle_text`.
#[derive(Default)]
struct Session {
    /// Command ids awaiting their `subscribed` reply.
    pending: HashMap<u64, Sub>,
    orderbook_sid: Option<u64>,
    ticker_sid: Option<u64>,
    /// Last `seq` seen per orderbook subscription.
    last_seq: HashMap<u64, u64>,
    /// Settlement target per open market, copied onto ticker rows.
    strikes: HashMap<String, Option<f64>>,
}

impl Session {
    fn set_markets(&mut self, markets: &[OpenMarket]) {
        self.strikes = markets.iter().map(|m| (m.ticker.clone(), m.floor_strike)).collect();
    }

    /// Sids of the per-market subscriptions to update on a rotation; each
    /// takes its own `update_subscription` command.
    fn market_sids(&self) -> Vec<u64> {
        [self.orderbook_sid, self.ticker_sid].into_iter().flatten().collect()
    }
}

pub async fn run_feed(state: AppState, shared: Arc<FeedShared>) {
    let tag = shared.market.tag.clone();
    let mut backoff = BACKOFF_MIN;
    loop {
        let started = Instant::now();
        let outcome = run_session(&state, &shared).await;
        {
            let mut st = shared.status.write().await;
            st.connected = false;
            st.reconnects += 1;
            match &outcome {
                Ok(()) => tracing::info!(%tag, "feed session closed cleanly"),
                Err(e) => {
                    tracing::warn!(%tag, error = format!("{e:#}"), "feed session failed");
                    st.last_error = Some(format!("{e:#}"));
                }
            }
        }
        // A session that lived a while earned a fresh backoff.
        if started.elapsed() > Duration::from_secs(60) {
            backoff = BACKOFF_MIN;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn send_cmd(ws: &mut Ws, cmd_id: &mut u64, build: impl FnOnce(u64) -> String) -> Result<u64> {
    *cmd_id += 1;
    ws.send(Message::text(build(*cmd_id))).await.context("sending ws command")?;
    Ok(*cmd_id)
}

/// Subscribe both per-market channels for `open`.
async fn subscribe_markets(
    ws: &mut Ws,
    cmd_id: &mut u64,
    session: &mut Session,
    open: &BTreeSet<String>,
) -> Result<()> {
    let id = send_cmd(ws, cmd_id, |id| subscribe_orderbook(id, open)).await?;
    session.pending.insert(id, Sub::Orderbook);
    let id = send_cmd(ws, cmd_id, |id| subscribe_ticker(id, open)).await?;
    session.pending.insert(id, Sub::Ticker);
    Ok(())
}

async fn run_session(state: &AppState, shared: &Arc<FeedShared>) -> Result<()> {
    let market = &shared.market;
    let tag = market.tag.as_str();
    let kalshi = &state.kalshi;

    let (mut ws, _) = connect_async(kalshi.ws_request()?).await.context("websocket handshake failed")?;
    tracing::info!(%tag, url = kalshi.endpoints.ws, "feed connected");
    // A fresh subscription re-sends snapshots; anything cached is from the old session.
    shared.books.write().await.clear();

    let mut cmd_id = 0u64;
    let mut session = Session::default();
    let mut subscribed: BTreeSet<String> = BTreeSet::new();

    let id = send_cmd(&mut ws, &mut cmd_id, |id| subscribe_index(id, &market.index_id)).await?;
    session.pending.insert(id, Sub::Index);

    let markets = kalshi.open_markets(&market.series_ticker).await?;
    session.set_markets(&markets);
    let open: BTreeSet<String> = markets.iter().map(|m| m.ticker.clone()).collect();
    if !open.is_empty() {
        subscribe_markets(&mut ws, &mut cmd_id, &mut session, &open).await?;
        subscribed = open;
    }
    {
        let mut st = shared.status.write().await;
        st.connected = true;
        st.last_error = None;
        st.open_tickers = subscribed.iter().cloned().collect();
        st.open_markets = markets;
    }
    tracing::info!(%tag, open = subscribed.len(), "subscribed");

    let mut refresh = interval(MARKET_REFRESH);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
    refresh.tick().await; // first tick is immediate; we just polled
    let mut ping = interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let stale = tokio::time::sleep(STALE_AFTER);
    tokio::pin!(stale);

    loop {
        tokio::select! {
            frame = ws.next() => {
                let Some(frame) = frame else { return Ok(()) };
                stale.as_mut().reset(Instant::now() + STALE_AFTER);
                match frame.context("websocket read failed")? {
                    Message::Text(text) => {
                        handle_text(state, shared, text.as_str(), &mut session).await?;
                    }
                    Message::Close(reason) => {
                        tracing::info!(%tag, ?reason, "websocket closed by server");
                        return Ok(());
                    }
                    _ => {}
                }
            }

            _ = refresh.tick() => {
                let markets = match kalshi.open_markets(&market.series_ticker).await {
                    Ok(markets) => markets,
                    Err(e) => {
                        tracing::warn!(%tag, error = %e, "market refresh failed");
                        continue;
                    }
                };
                session.set_markets(&markets);
                let open: BTreeSet<String> = markets.iter().map(|m| m.ticker.clone()).collect();
                // Strikes and times can be filled in after a market first
                // appears, so always publish the latest records.
                shared.status.write().await.open_markets = markets;
                let add: Vec<String> = open.difference(&subscribed).cloned().collect();
                let del: Vec<String> = subscribed.difference(&open).cloned().collect();
                if add.is_empty() && del.is_empty() {
                    continue;
                }
                tracing::info!(%tag, added = ?add, removed = ?del, "open markets changed");
                let sids = session.market_sids();
                if sids.is_empty() {
                    if !open.is_empty() {
                        subscribe_markets(&mut ws, &mut cmd_id, &mut session, &open).await?;
                    }
                } else {
                    for sid in sids {
                        if !add.is_empty() {
                            send_cmd(&mut ws, &mut cmd_id, |id| update_markets_cmd(id, sid, "add_markets", &add)).await?;
                        }
                        if !del.is_empty() {
                            send_cmd(&mut ws, &mut cmd_id, |id| update_markets_cmd(id, sid, "delete_markets", &del)).await?;
                        }
                    }
                }
                subscribed = open;
                shared.status.write().await.open_tickers = subscribed.iter().cloned().collect();
                if !del.is_empty() {
                    let mut books = shared.books.write().await;
                    for t in &del {
                        books.remove(t);
                    }
                }
            }

            _ = ping.tick() => ws.send(Message::Ping(Vec::new().into())).await.context("ping")?,

            _ = &mut stale => anyhow::bail!("no frames received for {STALE_AFTER:?}"),
        }
    }
}

/// Queue a live row without waiting; a full queue drops it and is counted.
async fn try_queue(state: &AppState, shared: &FeedShared, row: Row) {
    let tag = shared.market.tag.as_str();
    match state.questdb.ilp.try_send(row) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            tracing::warn!(%tag, "ilp queue full; dropping live row");
            shared.status.write().await.dropped_rows += 1;
        }
        Err(TrySendError::Closed(_)) => tracing::error!(%tag, "ilp writer gone"),
    }
}

/// Queue a row that must not be lost (a snapshot level), waiting for room.
async fn queue(state: &AppState, row: Row) -> Result<()> {
    state.questdb.ilp.send(row).await.map_err(|_| anyhow::anyhow!("ilp writer gone"))
}

async fn handle_text(state: &AppState, shared: &Arc<FeedShared>, text: &str, session: &mut Session) -> Result<()> {
    let tag = shared.market.tag.as_str();
    let env: Envelope = match serde_json::from_str(text) {
        Ok(e) => e,
        Err(_) => {
            tracing::debug!(%tag, %text, "unparsed frame");
            return Ok(());
        }
    };

    match env.typ.as_str() {
        "cfbenchmarks_value_5hz" => {
            let _ = shared.ticker_tx.send(Arc::from(text));
            let value: CfValue5Hz = match serde_json::from_value(env.msg) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(%tag, error = %e, %text, "bad 5hz frame");
                    return Ok(());
                }
            };
            let parsed = value.value();
            let now_ms = Utc::now().timestamp_millis();
            if let Some(v) = parsed {
                let row = IndexRow {
                    table: Table::Live,
                    index_id: value.index_id.clone(),
                    value: v,
                    ts_ms: value.source_ts_ms.or(value.received_at).unwrap_or(now_ms),
                    received_at_ms: value.received_at,
                    source: "ws_5hz",
                };
                try_queue(state, shared, row.into()).await;
            }
            let mut st = shared.status.write().await;
            st.ticker_msgs += 1;
            st.last_value = parsed.or(st.last_value);
            st.last_msg_at = Some(Utc::now());
        }
        "ticker" => {
            let msg: TickerMsg = match serde_json::from_value(env.msg) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(%tag, error = %e, %text, "bad ticker frame");
                    return Ok(());
                }
            };
            let now_ms = Utc::now().timestamp_millis();
            let row = ContractTickerRow {
                series_ticker: shared.market.series_ticker.clone(),
                floor_strike: session.strikes.get(&msg.market_ticker).copied().flatten(),
                ticker: msg.market_ticker,
                price: decimal(&msg.price_dollars),
                yes_bid: decimal(&msg.yes_bid_dollars),
                yes_ask: decimal(&msg.yes_ask_dollars),
                yes_bid_size: decimal(&msg.yes_bid_size_fp),
                yes_ask_size: decimal(&msg.yes_ask_size_fp),
                last_trade_size: decimal(&msg.last_trade_size_fp),
                volume: decimal(&msg.volume_fp),
                open_interest: decimal(&msg.open_interest_fp),
                dollar_volume: msg.dollar_volume,
                dollar_open_interest: msg.dollar_open_interest,
                ts_ms: msg.ts_ms.unwrap_or(now_ms),
                received_at_ms: now_ms,
                source: SOURCE_TICKER,
            };
            try_queue(state, shared, row.into()).await;
            let mut st = shared.status.write().await;
            st.contract_ticker_msgs += 1;
            st.last_msg_at = Some(Utc::now());
        }
        "orderbook_snapshot" | "orderbook_delta" => {
            // Kalshi numbers each subscription's frames; a hole means the
            // cached and stored books are wrong, so start over.
            if let Some(sid) = env.sid
                && let Some(last) = session.last_seq.insert(sid, env.seq)
                && env.seq != last + 1
            {
                anyhow::bail!("orderbook seq gap on sid {sid}: expected {}, got {}", last + 1, env.seq);
            }
            let is_snapshot = env.typ == "orderbook_snapshot";
            let now_ms = Utc::now().timestamp_millis();
            let mut rows: Vec<Row> = Vec::new();
            // Update the cached book and broadcast under the same lock so the
            // replay a new proxy client receives is ordered with the stream.
            let mut books = shared.books.write().await;
            match serde_json::from_value::<OrderbookMsg>(env.msg) {
                Ok(msg) => {
                    let ticker = msg.market_ticker.clone();
                    match books.get_mut(&ticker) {
                        Some(book) => {
                            book.apply_msg(&msg, env.seq);
                        }
                        None if is_snapshot => {
                            let mut book = OrderBook::default();
                            book.apply_msg(&msg, env.seq);
                            books.insert(ticker.clone(), book);
                        }
                        None => tracing::debug!(%tag, %ticker, "delta before snapshot; ignored"),
                    }
                    let row = |side: &'static str, kind, price, size| {
                        Row::ContractBook(ContractBookRow {
                            series_ticker: shared.market.series_ticker.clone(),
                            ticker: ticker.clone(),
                            side,
                            kind,
                            price: price_to_dollars(price),
                            size: qty_to_contracts(size),
                            seq: env.seq,
                            ts_ms: msg.ts_ms.unwrap_or(now_ms),
                            received_at_ms: now_ms,
                            source: SOURCE_BOOK,
                        })
                    };
                    if msg.is_snapshot() {
                        let (yes, no) = msg.snapshot_levels();
                        rows.extend(yes.iter().map(|&(p, q)| row("yes", BookKind::Snapshot, p, q)));
                        rows.extend(no.iter().map(|&(p, q)| row("no", BookKind::Snapshot, p, q)));
                    } else if let Some((side, price, delta)) = msg.delta_parts() {
                        rows.push(row(side.as_str(), BookKind::Delta, price, delta));
                    }
                }
                Err(e) => tracing::warn!(%tag, error = %e, %text, "bad orderbook frame"),
            }
            let _ = shared.orderbook_tx.send(Arc::from(text));
            drop(books);
            for row in rows {
                if is_snapshot {
                    queue(state, row).await?;
                } else {
                    try_queue(state, shared, row).await;
                }
            }
            let mut st = shared.status.write().await;
            st.orderbook_msgs += 1;
            st.last_msg_at = Some(Utc::now());
        }
        "subscribed" => {
            let sub = env.id.and_then(|id| session.pending.remove(&id));
            match serde_json::from_value::<Subscribed>(env.msg) {
                Ok(s) => {
                    if sub == Some(Sub::Orderbook) || s.channel == CHANNEL_ORDERBOOK {
                        session.orderbook_sid = Some(s.sid);
                    } else if sub == Some(Sub::Ticker) || s.channel == CHANNEL_TICKER {
                        session.ticker_sid = Some(s.sid);
                    }
                    tracing::info!(%tag, channel = %s.channel, sid = s.sid, "subscription confirmed");
                }
                Err(_) => tracing::info!(%tag, %text, "subscribed"),
            }
        }
        "ok" => tracing::debug!(%tag, %text, "command ok"),
        "error" => tracing::error!(%tag, %text, "kalshi ws error"),
        other => tracing::debug!(%tag, typ = other, "ignored frame"),
    }
    Ok(())
}
