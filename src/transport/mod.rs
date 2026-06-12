pub mod replay;
pub mod hidraw;
pub mod virtual_dongle;

use async_trait::async_trait;
use std::io::Result;
use std::sync::Arc;

use tokio::sync::Mutex as TokioMutex;
use futures_util::{SinkExt, StreamExt};
use axum::extract::ws::{WebSocket, Message};

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
        reader: Arc<TokioMutex<futures_util::stream::SplitStream<WebSocket>>>,
        writer: Arc<TokioMutex<futures_util::stream::SplitSink<WebSocket, Message>>>,
    },
    /// Mock transport for unit/integration tests.
    Replay(ReplayTransport),
}

#[async_trait]
impl AsyncTransport for GatewayTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            GatewayTransport::Hidraw(t) => t.read(buf).await,
            GatewayTransport::WebSocket { reader, .. } => {
                let mut reader = reader.lock().await;
                loop {
                    match reader.next().await {
                        Some(Ok(Message::Binary(data))) => {
                            let len = data.len().min(buf.len());
                            buf[..len].copy_from_slice(&data[..len]);
                            return Ok(len);
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "WebSocket stream ended",
                            ));
                        }
                        Some(Ok(_)) => {
                            // Skip non-binary messages (Ping, Pong, Text)
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

