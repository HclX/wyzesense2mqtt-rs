pub mod replay;
pub mod hidraw;

use async_trait::async_trait;
use std::io::Result;

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
    // WebSocket variant will be added in Phase 3.
    // WebSocket { ... },
    /// Mock transport for unit/integration tests.
    Replay(ReplayTransport),
}

#[async_trait]
impl AsyncTransport for GatewayTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            GatewayTransport::Hidraw(t) => t.read(buf).await,
            GatewayTransport::Replay(t) => t.read(buf).await,
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        match self {
            GatewayTransport::Hidraw(t) => t.write(buf).await,
            GatewayTransport::Replay(t) => t.write(buf).await,
        }
    }
}
