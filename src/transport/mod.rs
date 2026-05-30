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
