//! The Coinbase half of a market's feed: an Advanced Trade websocket session
//! streaming one product's `ticker` and `level2` channels into QuestDB. A
//! market runs one session for its spot product and another for its
//! perpetual-style future; both speak the same wire format and differ only in
//! the tables they write (see [`CoinbaseMarket`]) and the status they update.
//!
//! Level2 is stored as an event log: the snapshot Coinbase sends on
//! subscribe (one row per level), then absolute per-level updates. A gap in
//! the connection's `sequence_num` ends the session so the reconnect stores
//! a fresh snapshot.
//!
//! For proxy clients the session also keeps the book in memory and
//! publishes its top levels on `FeedShared::coinbase_book_tx` at most every
//! `PUBLISH_INTERVAL`, and sends the last price of every ticker frame on
//! `FeedShared::ticker_tx` next to the BRTI frames. Subscriptions carry a JWT when a CDP key is configured
//! and go out unauthenticated otherwise. Reconnects back off exponentially.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::{CoinbaseFeedStatus, FeedShared};
use crate::coinbase::book::{CoinbaseBook, TOP_DEPTH};
use crate::coinbase::stream::{
    CHANNEL_HEARTBEATS, CHANNEL_L2_DATA, CHANNEL_LEVEL2, CHANNEL_SUBSCRIPTIONS, CHANNEL_TICKER, Envelope, L2Event,
    TickerEvent, WS_URL, decimal, rfc3339_nanos, subscribe_cmd,
};
use crate::db::questdb::{BookKind, CoinbaseBookRow, CoinbaseMarket, CoinbaseTickerRow, Row};
use crate::state::AppState;

const PING_INTERVAL: Duration = Duration::from_secs(10);
/// Heartbeats arrive every second, so this is generous.
const STALE_AFTER: Duration = Duration::from_secs(30);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// How often a changed book's top levels go out to proxy clients.
pub const PUBLISH_INTERVAL: Duration = Duration::from_millis(250);

const SOURCE_TICKER: &str = "ws_ticker";
const SOURCE_BOOK: &str = "ws_level2";

/// What one session streams and where it reports.
struct Session<'a> {
    state: &'a AppState,
    shared: &'a FeedShared,
    product: &'a str,
    market: CoinbaseMarket,
}

impl Session<'_> {
    fn tag(&self) -> &str {
        &self.shared.market.tag
    }

    async fn with_status(&self, f: impl FnOnce(&mut CoinbaseFeedStatus)) {
        if let Some(cb) = self.shared.status.write().await.coinbase_mut(self.market) {
            f(cb);
        }
    }
}

pub async fn run_coinbase_feed(state: AppState, shared: Arc<FeedShared>, product: String, market: CoinbaseMarket) {
    let session = Session { state: &state, shared: &shared, product: &product, market };
    let tag = session.tag();
    let mut backoff = BACKOFF_MIN;
    loop {
        let started = Instant::now();
        let outcome = run_session(&session).await;
        // The book is no longer maintained; do not replay it to new clients.
        shared.coinbase_tops.write().await.remove(&market);
        session
            .with_status(|cb| {
                cb.connected = false;
                cb.reconnects += 1;
                if let Err(e) = &outcome {
                    cb.last_error = Some(format!("{e:#}"));
                }
            })
            .await;
        match &outcome {
            Ok(()) => tracing::info!(%tag, %product, ?market, "coinbase session closed cleanly"),
            Err(e) => {
                tracing::warn!(%tag, %product, ?market, error = format!("{e:#}"), "coinbase session failed")
            }
        }
        if started.elapsed() > Duration::from_secs(60) {
            backoff = BACKOFF_MIN;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn run_session(session: &Session<'_>) -> Result<()> {
    let Session { state, product, market, .. } = *session;
    let tag = session.tag();
    let (mut ws, _) = connect_async(WS_URL).await.context("coinbase websocket handshake failed")?;
    tracing::info!(%tag, %product, ?market, url = WS_URL, "coinbase feed connected");

    // Coinbase drops a connection that has not subscribed within 5 seconds.
    for channel in [CHANNEL_HEARTBEATS, CHANNEL_TICKER, CHANNEL_LEVEL2] {
        let jwt = match &state.coinbase_auth {
            Some(auth) => Some(auth.ws_bearer().context("signing coinbase subscribe")?),
            None => None,
        };
        ws.send(Message::text(subscribe_cmd(channel, product, jwt.as_deref())))
            .await
            .with_context(|| format!("subscribing to coinbase {channel}"))?;
    }
    session
        .with_status(|cb| {
            cb.connected = true;
            cb.last_error = None;
        })
        .await;

    let mut local = Local::default();
    let mut ping = interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut publish = interval(PUBLISH_INTERVAL);
    publish.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let stale = tokio::time::sleep(STALE_AFTER);
    tokio::pin!(stale);

    loop {
        tokio::select! {
            frame = ws.next() => {
                let Some(frame) = frame else { return Ok(()) };
                stale.as_mut().reset(Instant::now() + STALE_AFTER);
                match frame.context("coinbase websocket read failed")? {
                    Message::Text(text) => handle_text(session, text.as_str(), &mut local).await?,
                    Message::Close(reason) => {
                        tracing::info!(%tag, %product, ?market, ?reason, "coinbase websocket closed by server");
                        return Ok(());
                    }
                    _ => {}
                }
            }

            _ = ping.tick() => ws.send(Message::Ping(Vec::new().into())).await.context("ping")?,

            _ = publish.tick(), if local.dirty && local.book.is_ready() => {
                local.dirty = false;
                let ts_ms = Utc::now().timestamp_millis();
                let frame: Arc<str> = Arc::from(local.book.top_frame(market, product, TOP_DEPTH, ts_ms));
                session.shared.coinbase_tops.write().await.insert(market, frame.clone());
                let _ = session.shared.coinbase_book_tx.send(frame);
            }

            _ = &mut stale => anyhow::bail!("no coinbase frames received for {STALE_AFTER:?}"),
        }
    }
}

/// Per-connection state: the sequence check and the in-memory book.
#[derive(Default)]
struct Local {
    last_seq: Option<u64>,
    book: CoinbaseBook,
    /// The book changed since its top was last published.
    dirty: bool,
}

/// `{"type":"coinbase_ticker","market":"spot"|"perp","product":..,"price":..,"ts_ms":..}`
fn ticker_frame(market: CoinbaseMarket, product: &str, price: f64, ts_ms: i64) -> String {
    serde_json::json!({
        "type": "coinbase_ticker",
        "market": market.label(),
        "product": product,
        "price": price,
        "ts_ms": ts_ms,
    })
    .to_string()
}

/// Queue a live row without waiting; a full queue drops it and is counted.
async fn try_queue(session: &Session<'_>, row: Row) {
    match session.state.questdb.ilp.try_send(row) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            tracing::warn!(tag = %session.tag(), market = ?session.market, "ilp queue full; dropping coinbase row");
            session.with_status(|cb| cb.dropped_rows += 1).await;
        }
        Err(TrySendError::Closed(_)) => {
            tracing::error!(tag = %session.tag(), "ilp writer gone")
        }
    }
}

async fn handle_text(session: &Session<'_>, text: &str, local: &mut Local) -> Result<()> {
    let Session { state, product, market, .. } = *session;
    let tag = session.tag();
    let env: Envelope = match serde_json::from_str(text) {
        Ok(e) => e,
        Err(_) => {
            tracing::debug!(%tag, %text, "unparsed coinbase frame");
            return Ok(());
        }
    };
    if env.channel.is_empty() {
        // Error replies have no channel: `{"type":"error","message":"..."}`.
        tracing::error!(%tag, %product, ?market, %text, "coinbase ws error");
        return Ok(());
    }
    if let Some(last) = local.last_seq.replace(env.sequence_num)
        && env.sequence_num != last + 1
    {
        anyhow::bail!("coinbase sequence gap: expected {}, got {}", last + 1, env.sequence_num);
    }
    let now = Utc::now();
    let now_ns = now.timestamp_nanos_opt().unwrap_or_default();
    let sent_ns = rfc3339_nanos(&env.timestamp).unwrap_or(now_ns);

    match env.channel.as_str() {
        c if c == CHANNEL_TICKER => {
            let mut last_price = None;
            for event in env.events {
                let event: TickerEvent = match serde_json::from_value(event) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(%tag, error = %e, %text, "bad coinbase ticker event");
                        continue;
                    }
                };
                for t in event.tickers.into_iter().filter(|t| t.product_id == product) {
                    let price = decimal(&t.price);
                    last_price = price.or(last_price);
                    let row = CoinbaseTickerRow {
                        market,
                        product: t.product_id,
                        price,
                        best_bid: decimal(&t.best_bid),
                        best_ask: decimal(&t.best_ask),
                        best_bid_qty: decimal(&t.best_bid_quantity),
                        best_ask_qty: decimal(&t.best_ask_quantity),
                        volume_24h: decimal(&t.volume_24_h),
                        low_24h: decimal(&t.low_24_h),
                        high_24h: decimal(&t.high_24_h),
                        low_52w: decimal(&t.low_52_w),
                        high_52w: decimal(&t.high_52_w),
                        pct_chg_24h: decimal(&t.price_percent_chg_24_h),
                        sequence_num: env.sequence_num,
                        ts_ns: sent_ns,
                        received_at_ms: now.timestamp_millis(),
                        source: SOURCE_TICKER,
                    };
                    try_queue(session, row.into()).await;
                }
            }
            if let Some(price) = last_price {
                let frame = ticker_frame(market, product, price, sent_ns / 1_000_000);
                let _ = session.shared.ticker_tx.send(Arc::from(frame));
            }
            session
                .with_status(|cb| {
                    cb.ticker_msgs += 1;
                    cb.last_price = last_price.or(cb.last_price);
                    cb.last_msg_at = Some(now);
                })
                .await;
        }
        c if c == CHANNEL_L2_DATA => {
            for event in env.events {
                let event: L2Event = match serde_json::from_value(event) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(%tag, error = %e, "bad coinbase level2 event");
                        continue;
                    }
                };
                if event.product_id != product {
                    continue;
                }
                let kind = match event.typ.as_str() {
                    "snapshot" => BookKind::Snapshot,
                    "update" => BookKind::Update,
                    other => {
                        tracing::warn!(%tag, typ = other, "unknown coinbase level2 event type");
                        continue;
                    }
                };
                if kind == BookKind::Snapshot {
                    tracing::info!(%tag, %product, ?market, levels = event.updates.len(), "coinbase level2 snapshot");
                    local.book.begin_snapshot();
                }
                for u in event.updates {
                    let side = match u.side.as_str() {
                        "bid" => "bid",
                        "offer" => "offer",
                        other => {
                            tracing::warn!(%tag, side = other, "unknown coinbase level2 side");
                            continue;
                        }
                    };
                    let (Ok(price), Ok(size)) =
                        (u.price_level.trim().parse::<f64>(), u.new_quantity.trim().parse::<f64>())
                    else {
                        tracing::warn!(%tag, price = %u.price_level, size = %u.new_quantity, "bad coinbase level");
                        continue;
                    };
                    local.book.apply(kind, side, price, size);
                    local.dirty = true;
                    let row = Row::CoinbaseBook(CoinbaseBookRow {
                        market,
                        product: event.product_id.clone(),
                        side,
                        kind,
                        price,
                        size,
                        sequence_num: env.sequence_num,
                        ts_ns: rfc3339_nanos(&u.event_time).unwrap_or(sent_ns),
                        received_at_ns: sent_ns,
                        source: SOURCE_BOOK,
                    });
                    if kind == BookKind::Snapshot {
                        // A partial snapshot is useless: wait for queue room.
                        state.questdb.ilp.send(row).await.map_err(|_| anyhow::anyhow!("ilp writer gone"))?;
                    } else {
                        try_queue(session, row).await;
                    }
                }
            }
            session
                .with_status(|cb| {
                    cb.book_msgs += 1;
                    cb.last_msg_at = Some(now);
                })
                .await;
        }
        c if c == CHANNEL_HEARTBEATS => session.with_status(|cb| cb.last_msg_at = Some(now)).await,
        c if c == CHANNEL_SUBSCRIPTIONS => {
            tracing::info!(%tag, %product, ?market, %text, "coinbase subscriptions")
        }
        other => tracing::debug!(%tag, channel = other, "ignored coinbase frame"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn ticker_frame_encoding() {
        let v: Value = serde_json::from_str(&ticker_frame(CoinbaseMarket::Spot, "BTC-USD", 42_500.5, 1_705_314_645_123)).unwrap();
        assert_eq!(
            v,
            json!({ "type": "coinbase_ticker", "market": "spot", "product": "BTC-USD", "price": 42_500.5, "ts_ms": 1_705_314_645_123_i64 })
        );
    }
}
