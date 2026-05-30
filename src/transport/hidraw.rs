use super::AsyncTransport;
use async_trait::async_trait;
use std::io::{Result, Error, ErrorKind, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::fs::File;

#[derive(Clone)]
pub struct HidrawTransport {
    read_file: Arc<std::sync::Mutex<File>>,
    write_file: Arc<std::sync::Mutex<File>>,
}

impl HidrawTransport {
    /// Opens the character device at the specified path (e.g., "/dev/hidraw0") synchronously
    /// and duplicates the descriptor to separate read and write locks, preventing I/O deadlocks.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        
        // Open and clone file descriptor inside a blocking thread
        let (read_file, write_file) = tokio::task::spawn_blocking(move || {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path_buf)?;
            let f_clone = f.try_clone()?;
            Ok::<(_, _), std::io::Error>((f, f_clone))
        })
        .await
        .map_err(|_| Error::new(ErrorKind::Other, "Join error during file open"))??;

        Ok(Self {
            read_file: Arc::new(std::sync::Mutex::new(read_file)),
            write_file: Arc::new(std::sync::Mutex::new(write_file)),
        })
    }
}

/// Extracts valid protocol bytes from a raw HID frame read from the dongle.
///
/// The Wyze dongle's HID reports use byte[0] as a **length field** indicating
/// how many valid protocol data bytes follow. Bytes beyond `raw[1..1+length]`
/// are stale data from the dongle's internal ring buffer and must be discarded.
///
/// If byte[0] is `0x55` or `0xAA` (protocol magic bytes), the entire buffer
/// is treated as raw protocol data with no length prefix.
///
/// Returns the number of valid bytes written to `output`.
pub fn extract_hid_frame(raw: &[u8], output: &mut [u8]) -> usize {
    if raw.is_empty() {
        return 0;
    }
    let first_byte = raw[0];
    let is_length_byte = first_byte != 0x55 && first_byte != 0xAA;

    if is_length_byte {
        let mut length = first_byte as usize;
        if length > 0x3F {
            length = 0x3F; // Clamp to max HID payload (63 bytes)
        }
        let actual_len = length.min(raw.len() - 1).min(output.len());
        output[..actual_len].copy_from_slice(&raw[1..1 + actual_len]);
        actual_len
    } else {
        let len = raw.len().min(output.len());
        output[..len].copy_from_slice(&raw[..len]);
        len
    }
}

#[async_trait]
impl AsyncTransport for HidrawTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let file_handle = Arc::clone(&self.read_file);
        let buffer_len = buf.len();

        // Spawn the blocking read call onto Tokio's blocking pool
        // Uses the dedicated read_file descriptor, completely unblocked by concurrent writes!
        let res = tokio::task::spawn_blocking(move || {
            let mut file = file_handle.lock().unwrap();
            let mut temp_buf = vec![0u8; buffer_len + 1];
            match file.read(&mut temp_buf) {
                Ok(n) => Ok((n, temp_buf)),
                Err(e) => Err(e),
            }
        })
        .await
        .map_err(|_| Error::new(ErrorKind::Other, "Join error during blocking read"))?;

        match res {
            Ok((n, temp_buf)) if n > 0 => {
                let actual_len = extract_hid_frame(&temp_buf[..n], buf);

                if actual_len > 0 && tracing::enabled!(tracing::Level::TRACE) {
                    let raw_hex = temp_buf[..n].iter().map(|b| format!("{:02X}", b)).collect::<Vec<String>>().join(",");
                    let proto_hex = buf[..actual_len].iter().map(|b| format!("{:02X}", b)).collect::<Vec<String>>().join(",");
                    tracing::trace!("WIRE READ raw={} bytes:[{}] proto={} bytes:[{}]", n, raw_hex, actual_len, proto_hex);
                }
                Ok(actual_len)
            }
            Ok((n, _)) => Ok(n),
            Err(e) => Err(e),
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        if tracing::enabled!(tracing::Level::TRACE) {
            let hex_str = buf.iter().map(|b| format!("{:02X}", b)).collect::<Vec<String>>().join(",");
            tracing::trace!("WIRE WRITE ({} bytes): [{}]", buf.len(), hex_str);
        }

        let file_handle = Arc::clone(&self.write_file);
        let payload = buf.to_vec();

        // Spawn the blocking write call onto Tokio's blocking pool using the dedicated write descriptor
        tokio::task::spawn_blocking(move || {
            let mut file = file_handle.lock().unwrap();
            file.write_all(&payload)?;
            file.flush()
        })
        .await
        .map_err(|_| Error::new(ErrorKind::Other, "Join error during blocking write"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hid_frame_short_packet_discards_stale_data() {
        let mut raw = vec![
            0x08, 
            0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F,
        ];
        raw.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x37, 0x37, 0x43, 0x36,
                                 0x38, 0x31, 0x39, 0x33, 0x07, 0x15, 0x5F, 0x00,
                                 0x03, 0x15, 0x2E, 0x30, 0x11, 0x36, 0x26, 0x08,
                                 0x87, 0x55, 0xAA, 0x53, 0x1F, 0x19, 0x00, 0x00,
                                 0x01, 0x9E, 0x6C, 0x1F, 0x8D, 0xFE, 0xE8, 0x37,
                                 0x37, 0x43, 0x36, 0x38, 0x31, 0x39, 0x33, 0x07,
                                 0x18, 0x5F, 0x00, 0x03, 0x18, 0x06, 0x30]);

        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);

        assert_eq!(n, 8);
        assert_eq!(&output[..8], &[0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F]);
    }

    #[test]
    fn test_hid_frame_full_frame_multi_packet() {
        let mut raw = vec![0x3E]; 
        raw.extend_from_slice(&[0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F]);
        raw.extend_from_slice(&[0x55, 0xAA, 0x53, 0x1F, 0x19, 0x00, 0x00, 0x01,
                                 0x9E, 0x68, 0xB1, 0x59, 0xEA, 0xE8, 0x37, 0x37,
                                 0x43, 0x36, 0x38, 0x31, 0x39, 0x33, 0x07, 0x15,
                                 0x5F, 0x00, 0x03, 0x15, 0x2E, 0x30, 0x11, 0x36,
                                 0x26, 0x08, 0x87]);
        while raw.len() < 63 { 
            raw.push(0x00);
        }

        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);

        assert_eq!(n, 62);
        assert_eq!(&output[..2], &[0x55, 0xAA]);
        assert_eq!(&output[8..10], &[0x55, 0xAA]);
    }

    #[test]
    fn test_hid_frame_enr_response_length_0x17() {
        let raw = vec![
            0x17,
            0x55, 0xAA, 0x43, 0x13, 0x03,
            0x55, 0xFF, 0xEB, 0x67, 0xD8, 0xC5, 0xF8, 0x70,
            0xB9, 0x43, 0xB0, 0x21, 0xCC, 0x02, 0x3E, 0xEC,
            0x0A, 0xC8,
            0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00,
        ];

        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);

        assert_eq!(n, 23);
        assert_eq!(&output[..5], &[0x55, 0xAA, 0x43, 0x13, 0x03]);
        assert_eq!(&output[21..23], &[0x0A, 0xC8]);
    }

    #[test]
    fn test_hid_frame_mac_response_length_0x0f() {
        let raw = vec![
            0x0F,
            0x55, 0xAA, 0x43, 0x0B, 0x05,
            0x37, 0x37, 0x41, 0x38, 0x35, 0x41, 0x33, 0x36,
            0x03, 0x18,
            0xB0, 0x21, 0xCC, 0x02, 0x3E, 0xEC,
        ];

        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);

        assert_eq!(n, 15);
        let mac_bytes = &output[5..13];
        assert_eq!(std::str::from_utf8(mac_bytes).unwrap(), "77A85A36");
    }

    #[test]
    fn test_hid_frame_magic_byte_passthrough() {
        let raw = vec![0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F];

        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);

        assert_eq!(n, 8);
        assert_eq!(&output[..8], &raw);
    }

    #[test]
    fn test_hid_frame_magic_0xaa_passthrough() {
        let raw = vec![0xAA, 0x55, 0x43, 0x03, 0x27, 0x01, 0x6C];

        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);

        assert_eq!(n, 7);
        assert_eq!(&output[..7], &raw);
    }

    #[test]
    fn test_hid_frame_length_clamped_to_0x3f() {
        let mut raw = vec![0x7F];
        raw.extend_from_slice(&[0xAB; 70]);

        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);

        assert_eq!(n, 63);
    }

    #[test]
    fn test_hid_frame_empty_input() {
        let raw: Vec<u8> = vec![];
        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);
        assert_eq!(n, 0);
    }

    #[test]
    fn test_hid_frame_length_zero() {
        let raw = vec![0x00, 0xDE, 0xAD, 0xBE, 0xEF];
        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);
        assert_eq!(n, 0);
    }

    #[test]
    fn test_hid_frame_length_exceeds_available() {
        let raw = vec![0x14, 0x55, 0xAA, 0x43, 0x03, 0x27];
        let mut output = [0u8; 128];
        let n = extract_hid_frame(&raw, &mut output);
        assert_eq!(n, 5);
        assert_eq!(&output[..5], &[0x55, 0xAA, 0x43, 0x03, 0x27]);
    }
}

/// Scans Linux sysfs hidraw directory to automatically discover the assigned device node path
/// for the Wyze Sense USB Bridge (Vendor ID: 1a86, Product ID: e024).
pub fn discover_dongle_device() -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let sys_path = "/sys/class/hidraw";
    let dir = std::fs::read_dir(sys_path)
        .map_err(|e| Error::new(ErrorKind::NotFound, format!("Failed to open Linux /sys/class/hidraw directory: {}", e)))?;

    for entry in dir {
        if let Ok(entry) = entry {
            let path = entry.path();
            if let Ok(link) = std::fs::read_link(&path) {
                let link_str = link.to_string_lossy().to_lowercase();
                // Check for QinHeng Electronics Wyze Bridge signature (1a86:e024)
                if link_str.contains("1a86:e024") || (link_str.contains("1a86") && link_str.contains("e024")) {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        let dev_path = format!("/dev/{}", name);
                        return Ok(dev_path);
                    }
                }
            }
        }
    }

    Err(Box::new(Error::new(
        ErrorKind::NotFound,
        "Wyze Sense USB Dongle device node could not be discovered automatically. Please make sure it is plugged in.",
    )))
}
