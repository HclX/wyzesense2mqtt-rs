use super::AsyncTransport;
use async_trait::async_trait;
use std::io::Result;
use std::collections::{VecDeque, HashMap};
use std::sync::{Arc, Mutex};
use crate::protocol::packet::Packet;

#[derive(Clone)]
pub struct ReplayTransport {
    // Queue of raw bytes waiting to be read by the host
    read_queue: Arc<Mutex<VecDeque<u8>>>,
    // Log of raw bytes written by the host
    written_data: Arc<Mutex<Vec<u8>>>,
    // Automatic responses: Command ID -> Queue of response bytes
    responses: Arc<Mutex<HashMap<u16, VecDeque<Vec<u8>>>>>,
}

impl ReplayTransport {
    pub fn new() -> Self {
        Self {
            read_queue: Arc::new(Mutex::new(VecDeque::new())),
            written_data: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register an automatic response for a specific command ID.
    pub fn register_response(&self, cmd: u16, resp: Vec<u8>) {
        if let Ok(mut resps) = self.responses.lock() {
            resps.entry(cmd).or_insert_with(VecDeque::new).push_back(resp);
        }
    }

    /// Add data to the queue for the host to read.
    pub fn enqueue_read(&self, data: &[u8]) {
        let mut queue = self.read_queue.lock().unwrap();
        queue.extend(data.iter().copied());
    }

    /// Retrieve all bytes written by the host.
    pub fn get_written(&self) -> Vec<u8> {
        let written = self.written_data.lock().unwrap();
        written.clone()
    }

    /// Clear the history of written bytes.
    pub fn clear_written(&self) {
        let mut written = self.written_data.lock().unwrap();
        written.clear();
    }

    /// Checks if there is still unread data in the read queue.
    pub fn has_unread(&self) -> bool {
        let queue = self.read_queue.lock().unwrap();
        !queue.is_empty()
    }
}

#[async_trait]
impl AsyncTransport for ReplayTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut queue = self.read_queue.lock().unwrap();
        if queue.is_empty() {
            return Ok(0);
        }
        let to_read = std::cmp::min(buf.len(), queue.len());
        for i in 0..to_read {
            buf[i] = queue.pop_front().unwrap();
        }
        if to_read > 0 && tracing::enabled!(tracing::Level::TRACE) {
            let hex_str = buf[..to_read].iter().map(|b| format!("{:02X}", b)).collect::<Vec<String>>().join(",");
            tracing::trace!("WIRE READ (MOCK) ({} bytes): [{}]", to_read, hex_str);
        }
        Ok(to_read)
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        if tracing::enabled!(tracing::Level::TRACE) {
            let hex_str = buf.iter().map(|b| format!("{:02X}", b)).collect::<Vec<String>>().join(",");
            tracing::trace!("WIRE WRITE (MOCK) ({} bytes): [{}]", buf.len(), hex_str);
        }
        {
            let mut written = self.written_data.lock().unwrap();
            written.extend_from_slice(buf);
        }

        // Try to parse packet and trigger automatic response
        if let Ok((pkt, _)) = Packet::parse(buf) {
            let cmd = pkt.cmd();
            let mut resp_to_enqueue = None;
            {
                if let Ok(mut resps) = self.responses.lock() {
                    if let Some(queue) = resps.get_mut(&cmd) {
                        resp_to_enqueue = queue.pop_front();
                    }
                }
            }
            if let Some(resp) = resp_to_enqueue {
                self.enqueue_read(&resp);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replay_transport_read_write() {
        let mut transport = ReplayTransport::new();
        transport.enqueue_read(&[1, 2, 3, 4]);

        let mut buf = [0u8; 10];
        let bytes_read = transport.read(&mut buf).await.unwrap();
        assert_eq!(bytes_read, 4);
        assert_eq!(&buf[..4], &[1, 2, 3, 4]);

        transport.write(&[5, 6, 7]).await.unwrap();
        assert_eq!(transport.get_written(), vec![5, 6, 7]);
    }
}
