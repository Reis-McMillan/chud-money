//! One feed task: a Kalshi websocket session that streams the market's CF
//! Benchmarks 5Hz index and the orderbooks of the series' open markets.
//!
//! Index values are broadcast to proxy subscribers and queued for QuestDB.
//! Orderbook frames are broadcast only. The series rolls every 15 minutes, so
//! open markets are re-polled and the orderbook subscription is updated in
//! place. The session reconnects with exponential backoff.

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
use crate::db::questdb::{IndexRow, Table};
use crate::kalshi::book::{OrderBook, OrderbookMsg};
use crate::kalshi::stream::{
    CfValue5Hz, Envelope, Subscribed, subscribe_index, subscribe_orderbook, update_markets_cmd,
};
use crate::state::AppState;

const MARKET_REFRESH: Duration = Duration::from_secs(30);
const PING_INTERVAL: Duration = Duration::from_secs(10);
const STALE_AFTER: Duration = Duration::from_secs(30);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sub {
    Ticker,
    Orderbook,
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

async fn run_session(state: &AppState, shared: &Arc<FeedShared>) -> Result<()> {
    let market = &shared.market;
    let tag = market.tag.as_str();
    let kalshi = &state.kalshi;

    let (mut ws, _) = connect_async(kalshi.ws_request()?).await.context("websocket handshake failed")?;
    tracing::info!(%tag, url = kalshi.endpoints.ws, "feed connected");
    // A fresh subscription re-sends snapshots; anything cached is from the old session.
    shared.books.write().await.clear();

    let mut cmd_id = 0u64;
    let mut pending: HashMap<u64, Sub> = HashMap::new();
    let mut orderbook_sid: Option<u64> = None;
    let mut subscribed: BTreeSet<String> = BTreeSet::new();

    let id = send_cmd(&mut ws, &mut cmd_id, |id| subscribe_index(id, &market.index_id)).await?;
    pending.insert(id, Sub::Ticker);

    let markets = kalshi.open_markets(&market.series_ticker).await?;
    let open: BTreeSet<String> = markets.iter().map(|m| m.ticker.clone()).collect();
    if !open.is_empty() {
        let id = send_cmd(&mut ws, &mut cmd_id, |id| subscribe_orderbook(id, &open)).await?;
        pending.insert(id, Sub::Orderbook);
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
                        handle_text(state, shared, text.as_str(), &mut pending, &mut orderbook_sid).await;
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
                match orderbook_sid {
                    Some(sid) => {
                        if !add.is_empty() {
                            send_cmd(&mut ws, &mut cmd_id, |id| update_markets_cmd(id, sid, "add_markets", &add)).await?;
                        }
                        if !del.is_empty() {
                            send_cmd(&mut ws, &mut cmd_id, |id| update_markets_cmd(id, sid, "delete_markets", &del)).await?;
                        }
                    }
                    None if !open.is_empty() => {
                        let id = send_cmd(&mut ws, &mut cmd_id, |id| subscribe_orderbook(id, &open)).await?;
                        pending.insert(id, Sub::Orderbook);
                    }
                    None => {}
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

async fn handle_text(
    state: &AppState,
    shared: &Arc<FeedShared>,
    text: &str,
    pending: &mut HashMap<u64, Sub>,
    orderbook_sid: &mut Option<u64>,
) {
    let tag = shared.market.tag.as_str();
    let env: Envelope = match serde_json::from_str(text) {
        Ok(e) => e,
        Err(_) => {
            tracing::debug!(%tag, %text, "unparsed frame");
            return;
        }
    };

    match env.typ.as_str() {
        "cfbenchmarks_value_5hz" => {
            let _ = shared.ticker_tx.send(Arc::from(text));
            let value: CfValue5Hz = match serde_json::from_value(env.msg) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(%tag, error = %e, %text, "bad 5hz frame");
                    return;
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
                match state.questdb.ilp.try_send(row) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => tracing::warn!(%tag, "ilp queue full; dropping live row"),
                    Err(TrySendError::Closed(_)) => tracing::error!(%tag, "ilp writer gone"),
                }
            }
            let mut st = shared.status.write().await;
            st.ticker_msgs += 1;
            st.last_value = parsed.or(st.last_value);
            st.last_msg_at = Some(Utc::now());
        }
        "orderbook_snapshot" | "orderbook_delta" => {
            // Update the cached book and broadcast under the same lock so the
            // replay a new proxy client receives is ordered with the stream.
            let mut books = shared.books.write().await;
            match serde_json::from_value::<OrderbookMsg>(env.msg) {
                Ok(msg) => {
                    let is_snapshot = env.typ == "orderbook_snapshot";
                    match books.get_mut(&msg.market_ticker) {
                        Some(book) => {
                            book.apply_msg(&msg, env.seq);
                        }
                        None if is_snapshot => {
                            let mut book = OrderBook::default();
                            book.apply_msg(&msg, env.seq);
                            books.insert(msg.market_ticker.clone(), book);
                        }
                        None => tracing::debug!(%tag, ticker = %msg.market_ticker, "delta before snapshot; ignored"),
                    }
                }
                Err(e) => tracing::warn!(%tag, error = %e, %text, "bad orderbook frame"),
            }
            let _ = shared.orderbook_tx.send(Arc::from(text));
            drop(books);
            shared.status.write().await.orderbook_msgs += 1;
        }
        "subscribed" => {
            let sub = env.id.and_then(|id| pending.remove(&id));
            match serde_json::from_value::<Subscribed>(env.msg) {
                Ok(s) => {
                    if sub == Some(Sub::Orderbook) || s.channel == "orderbook_delta" {
                        *orderbook_sid = Some(s.sid);
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
}
