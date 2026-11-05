pub mod replay;
pub mod hidraw;
pub mod virtual_dongle;

use async_trait::async_trait;
use std::collections::VecDeque;
use std::io::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Mutex as TokioMutex;
use futures_util::{SinkExt, StreamExt};
use axum::extract::ws::Message;

#[async_trait]
pub trait AsyncTransport: Send + Sync {
    /// Reads bytes from the transport channel into the provided buffer.
    /// Returns the number of bytes read.
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Writes bytes to the transport channel.
    async fn write(&mut self, buf: &[u8]) -> Result<()>;
}

// ---------------------------------------------------------------------------
// GatewayTransport — concrete enum that unifies all transport backends.
//
// Engine uses this directly (not generic T) so the type doesn't propagate
// through every struct and function signature in the codebase.
// ---------------------------------------------------------------------------

use self::hidraw::HidrawTransport;
use self::replay::ReplayTransport;

pub type WsStream = Box<dyn futures_util::Stream<Item = std::result::Result<Message, axum::Error>> + Send + Unpin>;
pub type WsSink = Box<dyn futures_util::Sink<Message, Error = axum::Error> + Send + Unpin>;

/// Unified transport for all dongle connections.
///
/// Each variant wraps a specific backend. Engine, WebState, and EnginesMap
/// all use this concrete type, avoiding generic type parameter propagation.
#[derive(Clone)]
pub enum GatewayTransport {
    /// Local USB dongle via /dev/hidrawN.
    Hidraw(HidrawTransport),
    /// Remote dongle connected via WebSocket bridge.
    WebSocket {
        reader: Arc<TokioMutex<WsStream>>,
        writer: Arc<TokioMutex<WsSink>>,
        /// Unconsumed trailing bytes from a binary WebSocket message that was
        /// larger than the caller's read buffer.
        pending: Arc<TokioMutex<VecDeque<u8>>>,
        /// Number of Pong frames received so far. The reader loop is the only
        /// thing that polls the socket, so it counts pongs here for the bridge
        /// heartbeat to check.
        pongs: Arc<AtomicU64>,
    },
    /// Mock transport for unit/integration tests.
    Replay(ReplayTransport),
}

#[async_trait]
impl AsyncTransport for GatewayTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            GatewayTransport::Hidraw(t) => t.read(buf).await,
            GatewayTransport::WebSocket { reader, pending, pongs, .. } => {
                {
                    let mut pending = pending.lock().await;
                    if !pending.is_empty() {
                        let len = pending.len().min(buf.len());
                        for dst in &mut buf[..len] {
                            *dst = pending.pop_front().unwrap();
                        }
                        return Ok(len);
                    }
                }
                let mut reader = reader.lock().await;
                loop {
                    match reader.next().await {
                        Some(Ok(Message::Binary(data))) => {
                            let len = data.len().min(buf.len());
                            buf[..len].copy_from_slice(&data[..len]);
                            if data.len() > len {
                                pending.lock().await.extend(&data[len..]);
                            }
                            return Ok(len);
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "WebSocket stream ended",
                            ));
                        }
                        Some(Ok(Message::Pong(_))) => {
                            pongs.fetch_add(1, Ordering::SeqCst);
                        }
                        Some(Ok(_)) => {
                            // Skip other non-binary messages (Ping, Text)
                            continue;
                        }
                        Some(Err(e)) => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                format!("WebSocket read error: {}", e),
                            ));
                        }
                    }
                }
            }
            GatewayTransport::Replay(t) => t.read(buf).await,
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        match self {
            GatewayTransport::Hidraw(t) => t.write(buf).await,
            GatewayTransport::WebSocket { writer, .. } => {
                let mut writer = writer.lock().await;
                writer
                    .send(Message::Binary(buf.to_vec()))
                    .await
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            format!("WebSocket write error: {}", e),
                        )
                    })?;
                writer.flush().await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        format!("WebSocket flush error: {}", e),
                    )
                })?;
                Ok(())
            }
            GatewayTransport::Replay(t) => t.write(buf).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn websocket_transport_buffers_frames_larger_than_read_buf() {
        let payload: Vec<u8> = (0u8..20).collect();
        let stream: WsStream = Box::new(futures_util::stream::iter(vec![
            Ok(Message::Binary(payload.clone())),
        ]));
        let sink: WsSink = Box::new(
            futures_util::sink::drain().sink_map_err(|e: std::convert::Infallible| match e {}),
        );
        let mut transport = GatewayTransport::WebSocket {
            reader: Arc::new(TokioMutex::new(stream)),
            writer: Arc::new(TokioMutex::new(sink)),
            pending: Arc::new(TokioMutex::new(VecDeque::new())),
            pongs: Arc::new(AtomicU64::new(0)),
        };

        let mut received = Vec::new();
        let mut chunk = [0u8; 6];
        while received.len() < payload.len() {
            match transport.read(&mut chunk).await {
                Ok(n) => received.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        assert_eq!(
            received, payload,
            "trailing bytes of a WebSocket binary frame larger than buf must not be truncated"
        );
    }
}

