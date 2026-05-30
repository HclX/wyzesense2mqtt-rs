//! Packet capture transport wrapper for recording raw I/O during live dongle use.
//!
//! Wraps any `AsyncTransport` and logs all read/write operations to a JSONL file.
//! Each line records the timestamp, direction, raw bytes, and (for reads) the
//! HID length byte extraction result. These capture files can be replayed in
//! tests to verify parsing correctness without hardware.
//!
//! # Capture File Format (JSONL)
//!
//! Each line is a JSON object with the following fields:
//! ```json
//! {"ts_ms":1716570196992,"dir":"R","raw_hex":"3e55aa43042801016f...","proto_hex":"55aa43042801016f","proto_len":8}
//! {"ts_ms":1716570197050,"dir":"W","proto_hex":"aa554303270001...","proto_len":7}
//! ```
//!
//! - `ts_ms`: Unix epoch milliseconds
//! - `dir`: "R" = read (dongle→host), "W" = write (host→dongle)
//! - `raw_hex`: Raw HID frame bytes including length byte (reads only)
//! - `proto_hex`: Extracted protocol-level bytes (after HID framing for reads)
//! - `proto_len`: Number of valid protocol bytes

use super::AsyncTransport;
use async_trait::async_trait;
use std::io::{Result, Error, ErrorKind, Write, BufWriter};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

/// A single captured I/O operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureRecord {
    /// Unix epoch milliseconds
    pub ts_ms: u64,
    /// Direction: "R" for read (dongle→host), "W" for write (host→dongle)
    pub dir: String,
    /// Raw HID frame bytes as hex string (reads only, includes length byte)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_hex: Option<String>,
    /// Protocol-level bytes as hex string (after HID frame extraction for reads)
    pub proto_hex: String,
    /// Number of valid protocol bytes
    pub proto_len: usize,
}

impl CaptureRecord {
    /// Returns the protocol bytes as a Vec<u8>.
    pub fn protocol_bytes(&self) -> Vec<u8> {
        hex_to_bytes(&self.proto_hex)
    }

    /// Returns the raw HID frame bytes as a Vec<u8> (reads only).
    pub fn raw_bytes(&self) -> Option<Vec<u8>> {
        self.raw_hex.as_ref().map(|h| hex_to_bytes(h))
    }
}

/// Capture writer that appends records to a JSONL file.
struct CaptureWriter {
    writer: BufWriter<File>,
}

impl CaptureWriter {
    fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    fn write_record(&mut self, record: &CaptureRecord) -> Result<()> {
        let json = serde_json::to_string(record)
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
        writeln!(self.writer, "{}", json)?;
        self.writer.flush()
    }
}

/// Transport wrapper that records all I/O to a capture file.
///
/// Usage:
/// ```no_run
/// use wyzesense2mqtt_rs::transport::capture::CaptureTransport;
/// use wyzesense2mqtt_rs::transport::hidraw::HidrawTransport;
///
/// # async fn example() -> std::io::Result<()> {
/// let inner = HidrawTransport::open("/dev/hidraw0").await?;
/// let transport = CaptureTransport::new(inner, "captures/session.jsonl")?;
/// // Use `transport` as you would any AsyncTransport — all I/O is recorded.
/// # Ok(())
/// # }
/// ```
pub struct CaptureTransport<T: AsyncTransport> {
    inner: T,
    writer: Arc<Mutex<CaptureWriter>>,
}

impl<T: AsyncTransport + Clone> Clone for CaptureTransport<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            writer: Arc::clone(&self.writer),
        }
    }
}

impl<T: AsyncTransport> CaptureTransport<T> {
    /// Creates a new CaptureTransport wrapping `inner`, writing captures to `path`.
    pub fn new<P: AsRef<Path>>(inner: T, path: P) -> Result<Self> {
        let writer = CaptureWriter::new(path.as_ref())?;
        Ok(Self {
            inner,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn bytes_to_hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .filter_map(|i| {
            if i + 2 <= hex.len() {
                u8::from_str_radix(&hex[i..i + 2], 16).ok()
            } else {
                None
            }
        })
        .collect()
}

#[async_trait]
impl<T: AsyncTransport> AsyncTransport for CaptureTransport<T> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Read into a temporary buffer to capture the raw HID frame
        let mut raw_buf = vec![0u8; buf.len() + 1]; // +1 for potential length byte
        let n = self.inner.read(&mut raw_buf).await?;

        if n > 0 {
            // Copy into caller's buffer
            let copy_len = n.min(buf.len());
            buf[..copy_len].copy_from_slice(&raw_buf[..copy_len]);

            let record = CaptureRecord {
                ts_ms: now_ms(),
                dir: "R".to_string(),
                raw_hex: Some(bytes_to_hex(&raw_buf[..n])),
                proto_hex: bytes_to_hex(&buf[..copy_len]),
                proto_len: copy_len,
            };

            if let Ok(mut writer) = self.writer.lock() {
                let _ = writer.write_record(&record);
            }

            Ok(copy_len)
        } else {
            Ok(0)
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let record = CaptureRecord {
            ts_ms: now_ms(),
            dir: "W".to_string(),
            raw_hex: None,
            proto_hex: bytes_to_hex(buf),
            proto_len: buf.len(),
        };

        if let Ok(mut writer) = self.writer.lock() {
            let _ = writer.write_record(&record);
        }

        self.inner.write(buf).await
    }
}

// =============================================================================
// Capture File Loader — for test replay
// =============================================================================

/// Loads a JSONL capture file into a vector of CaptureRecords.
/// Skips malformed lines silently.
pub fn load_capture_file<P: AsRef<Path>>(path: P) -> Result<Vec<CaptureRecord>> {
    let content = std::fs::read_to_string(path)?;
    let records: Vec<CaptureRecord> = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    Ok(records)
}

/// Validates all records in a capture file by parsing each protocol packet.
/// Returns a summary of successes and failures.
pub fn validate_capture(records: &[CaptureRecord]) -> CaptureValidation {
    use crate::protocol::packet::Packet;

    let mut result = CaptureValidation::default();

    for (idx, record) in records.iter().enumerate() {
        let proto_bytes = record.protocol_bytes();
        if proto_bytes.is_empty() {
            result.empty += 1;
            continue;
        }

        // Try to parse one or more packets from the protocol bytes
        let mut offset = 0;
        let mut parsed_in_record = 0;
        while offset < proto_bytes.len() {
            match Packet::parse(&proto_bytes[offset..]) {
                Ok((pkt, consumed)) => {
                    result.packets.push(ParsedPacketInfo {
                        record_index: idx,
                        direction: record.dir.clone(),
                        cmd: pkt.cmd(),
                        payload_len: pkt.payload_bytes().map(|p| p.len()).unwrap_or(0),
                        offset,
                    });
                    offset += consumed;
                    parsed_in_record += 1;
                }
                Err(_) => {
                    if parsed_in_record == 0 {
                        result.parse_failures.push(idx);
                    }
                    // Remaining bytes might be a partial packet at end of frame
                    break;
                }
            }
        }
    }

    result
}

/// Summary of capture file validation.
#[derive(Debug, Default)]
pub struct CaptureValidation {
    /// Successfully parsed packets
    pub packets: Vec<ParsedPacketInfo>,
    /// Record indices that failed to parse any packet
    pub parse_failures: Vec<usize>,
    /// Records with empty protocol bytes
    pub empty: usize,
}

impl CaptureValidation {
    pub fn total_parsed(&self) -> usize {
        self.packets.len()
    }
    pub fn total_failed(&self) -> usize {
        self.parse_failures.len()
    }
}

/// Info about a successfully parsed packet.
#[derive(Debug)]
pub struct ParsedPacketInfo {
    pub record_index: usize,
    pub direction: String,
    pub cmd: u16,
    pub payload_len: usize,
    pub offset: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::replay::ReplayTransport;
    use crate::protocol::packet::Packet;
    use std::io::Write;

    #[test]
    fn test_hex_roundtrip() {
        let data = vec![0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F];
        let hex = bytes_to_hex(&data);
        assert_eq!(hex, "55aa43042801016f");
        let roundtripped = hex_to_bytes(&hex);
        assert_eq!(roundtripped, data);
    }

    #[test]
    fn test_capture_record_serialization() {
        let record = CaptureRecord {
            ts_ms: 1716570196992,
            dir: "R".to_string(),
            raw_hex: Some("3e55aa43042801016f".to_string()),
            proto_hex: "55aa43042801016f".to_string(),
            proto_len: 8,
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: CaptureRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.ts_ms, 1716570196992);
        assert_eq!(parsed.dir, "R");
        assert_eq!(parsed.proto_len, 8);
        assert_eq!(parsed.protocol_bytes(), vec![0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F]);
    }

    #[test]
    fn test_capture_record_write_has_no_raw() {
        let record = CaptureRecord {
            ts_ms: 1716570197050,
            dir: "W".to_string(),
            raw_hex: None,
            proto_hex: "aa554303270001".to_string(),
            proto_len: 7,
        };

        let json = serde_json::to_string(&record).unwrap();
        assert!(!json.contains("raw_hex"), "Write records should omit raw_hex");
    }

    #[tokio::test]
    async fn test_capture_transport_records_io() {
        let replay = ReplayTransport::new();
        // Enqueue a packet for reading
        replay.enqueue_read(&[0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F]);

        let capture_path = std::env::temp_dir().join("wyzesense_test_capture.jsonl");
        let mut transport = CaptureTransport::new(replay, &capture_path).unwrap();

        // Read
        let mut buf = [0u8; 128];
        let n = transport.read(&mut buf).await.unwrap();
        assert_eq!(n, 8);

        // Write
        let write_data = Packet::new_sync(0x27, vec![]).to_bytes();
        transport.write(&write_data).await.unwrap();

        // Verify capture file
        let records = load_capture_file(&capture_path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].dir, "R");
        assert_eq!(records[0].proto_len, 8);
        assert_eq!(records[1].dir, "W");

        // Parse the captured protocol bytes
        let (pkt, _) = Packet::parse(&records[0].protocol_bytes()).unwrap();
        assert_eq!(pkt.cmd(), 0x4328);

        // Cleanup
        let _ = std::fs::remove_file(&capture_path);
    }

    #[test]
    fn test_load_and_validate_synthetic_capture() {
        let capture_path = std::env::temp_dir().join("wyzesense_test_validate.jsonl");

        // Write synthetic capture records using known-good packet hex
        {
            let mut f = std::fs::File::create(&capture_path).unwrap();
            // Read: Inquiry response (verified parseable)
            writeln!(f, r#"{{"ts_ms":1000,"dir":"R","raw_hex":"0855aa43042801016f","proto_hex":"55aa43042801016f","proto_len":8}}"#).unwrap();
            // Write: Inquiry command (AA55 magic, cmd=0x4327, checksum=0x016C)
            writeln!(f, r#"{{"ts_ms":1001,"dir":"W","proto_hex":"aa55430327016c","proto_len":7}}"#).unwrap();
        }

        let records = load_capture_file(&capture_path).unwrap();
        assert_eq!(records.len(), 2);

        let validation = validate_capture(&records);
        assert!(validation.total_parsed() >= 2, "Expected at least 2 parsed packets, got {}. Failures: {:?}", validation.total_parsed(), validation.parse_failures);

        let _ = std::fs::remove_file(&capture_path);
    }
}
