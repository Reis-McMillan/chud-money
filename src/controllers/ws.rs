//! Websocket proxies. Each client gets a `broadcast::Receiver` on the feed's
//! channel; frames are forwarded verbatim as text. A slow client that falls
//! behind the channel buffer gets a `{"type":"lagged"}` notice and resumes
//! from the oldest retained frame rather than being disconnected.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::Response;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use crate::error::AppError;
use crate::state::AppState;

/// `GET /ws/{tag}/ticker`
pub async fn ticker(
    Path(tag): Path<String>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let feed = state.feeds.get(&tag).await.ok_or_else(|| AppError::NotFound(format!("feed for '{tag}'")))?;
    let rx = feed.ticker_tx.subscribe();
    Ok(ws.on_upgrade(move |socket| pump(socket, rx, tag, "ticker")))
}

/// `GET /ws/{tag}/orderbook`
pub async fn orderbook(
    Path(tag): Path<String>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let feed = state.feeds.get(&tag).await.ok_or_else(|| AppError::NotFound(format!("feed for '{tag}'")))?;
    let rx = feed.orderbook_tx.subscribe();
    Ok(ws.on_upgrade(move |socket| pump(socket, rx, tag, "orderbook")))
}

async fn pump(mut socket: WebSocket, mut rx: broadcast::Receiver<Arc<str>>, tag: String, kind: &'static str) {
    tracing::debug!(%tag, kind, "proxy client connected");
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
