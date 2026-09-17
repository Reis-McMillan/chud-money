//! Websocket proxies. Each client gets a `broadcast::Receiver` on the feed's
//! channel; frames are forwarded verbatim as text. A slow client that falls
//! behind the channel buffer gets a `{"type":"lagged"}` notice and resumes
//! from the oldest retained frame rather than being disconnected.
//!
//! Both routes sit behind `middleware::authenticated`, which reads the token
//! from `?access_token=` on upgrades. It is checked at upgrade time only: an
//! open socket is not closed when the token later expires.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::Response;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use crate::error::AppError;
use crate::middleware::authenticated::AuthUser;
use crate::state::AppState;

/// `GET /ws/{tag}/ticker`
pub async fn ticker(
    Path(tag): Path<String>,
    State(state): State<AppState>,
    user: AuthUser,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let feed = state.feeds.get(&tag).await.ok_or_else(|| AppError::NotFound(format!("feed for '{tag}'")))?;
    let rx = feed.ticker_tx.subscribe();
    tracing::debug!(%tag, user = %user.id, "ticker upgrade authorized");
    Ok(ws.on_upgrade(move |socket| pump(socket, rx, Vec::new(), tag, "ticker")))
}

/// `GET /ws/{tag}/orderbook`
pub async fn orderbook(
    Path(tag): Path<String>,
    State(state): State<AppState>,
    user: AuthUser,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let feed = state.feeds.get(&tag).await.ok_or_else(|| AppError::NotFound(format!("feed for '{tag}'")))?;
    tracing::debug!(%tag, user = %user.id, "orderbook upgrade authorized");
    // Subscribe before reading the cache so no frame falls between the two;
    // an overlap is harmless because each snapshot carries the seq it is
    // current through and clients skip deltas at or below it.
    let rx = feed.orderbook_tx.subscribe();
    let snapshots: Vec<String> = {
        let books = feed.books.read().await;
        books.iter().map(|(ticker, book)| book.snapshot_frame(ticker)).collect()
    };
    Ok(ws.on_upgrade(move |socket| pump(socket, rx, snapshots, tag, "orderbook")))
}

async fn pump(
    mut socket: WebSocket,
    mut rx: broadcast::Receiver<Arc<str>>,
    preamble: Vec<String>,
    tag: String,
    kind: &'static str,
) {
    tracing::debug!(%tag, kind, replayed = preamble.len(), "proxy client connected");
    for frame in preamble {
        if socket.send(Message::Text(frame.into())).await.is_err() {
            return;
        }
    }
    loop {
        tokio::select! {
            frame = rx.recv() => match frame {
                Ok(text) => {
                    if socket.send(Message::Text(text.as_ref().into())).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Lagged(n)) => {
                    tracing::debug!(%tag, kind, dropped = n, "proxy client lagged");
                    let notice = format!(r#"{{"type":"lagged","dropped":{n}}}"#);
                    if socket.send(Message::Text(notice.into())).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Closed) => {
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
            },
            incoming = socket.recv() => match incoming {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {} // ignore client chatter; pings are answered by axum
            },
        }
    }
    tracing::debug!(%tag, kind, "proxy client disconnected");
}
