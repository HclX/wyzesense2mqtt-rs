// ws_bridge: Transparent USB↔WebSocket relay for remote dongles
//
// Usage: ws_bridge --device /dev/hidraw0 --gateway ws://host:8080/ws/bridge
//
// This binary opens a local USB dongle via HidrawTransport and connects to
// a remote gateway server over WebSocket. It bidirectionally relays raw bytes
// between the USB device and the WebSocket connection, making the dongle
// appear as if it were directly attached to the gateway machine.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use wyzesense2mqtt_rs::transport::hidraw::HidrawTransport;
use wyzesense2mqtt_rs::transport::AsyncTransport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Setup tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Parse arguments
    let mut device_path = "/dev/hidraw0".to_string();
    let mut gateway_url = "ws://127.0.0.1:8080/ws/bridge".to_string();

    let args: Vec<String> = std::env::args().collect();
    let mut idx = 1;
    while idx < args.len() {
        match args[idx].as_str() {
            "--device" | "-d" => {
                if idx + 1 < args.len() {
                    device_path = args[idx + 1].clone();
                    idx += 2;
                } else {
                    return Err("Missing argument for --device".into());
                }
            }
            "--gateway" | "-g" => {
                if idx + 1 < args.len() {
                    gateway_url = args[idx + 1].clone();
                    idx += 2;
                } else {
                    return Err("Missing argument for --gateway".into());
                }
            }
            "--help" | "-h" => {
                println!("ws_bridge: USB↔WebSocket relay for remote Wyze Sense dongles");
                println!();
                println!("USAGE:");
                println!("    ws_bridge [OPTIONS]");
                println!();
                println!("OPTIONS:");
                println!("    -d, --device <PATH>     USB hidraw device path [default: /dev/hidraw0]");
                println!("    -g, --gateway <URL>     Gateway WebSocket URL [default: ws://127.0.0.1:8080/ws/bridge]");
                println!("    -h, --help              Print this help message");
                return Ok(());
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                eprintln!("Usage: ws_bridge [--device PATH] [--gateway URL]");
                return Err("Unknown argument".into());
            }
        }
    }

    info!("ws_bridge starting up");
    info!("  USB Device: {}", device_path);
    info!("  Gateway:    {}", gateway_url);

    // Reconnect loop
    loop {
        match run_bridge(&device_path, &gateway_url).await {
            Ok(_) => {
                info!("Bridge session ended cleanly");
            }
            Err(e) => {
                error!("Bridge session failed: {}", e);
            }
        }

        warn!("Reconnecting in 5 seconds...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_bridge(
    device_path: &str,
    gateway_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Open local USB dongle
    info!("Opening USB device: {}", device_path);
    let transport = HidrawTransport::open(device_path).await?;
    // Clone the transport so we can use it in two tasks
    let mut transport_read = transport.clone();
    let mut transport_write = transport;

    // 2. Connect to gateway WebSocket
    info!("Connecting to gateway: {}", gateway_url);
    let ws_url = format!("{}?device={}", gateway_url, device_path.replace('/', "%2F"));
    let (ws_stream, _response) = connect_async(&ws_url).await?;
    info!("WebSocket connection established");

    let (mut ws_writer, mut ws_reader) = ws_stream.split();

    // 3. Bidirectional relay
    // USB → WebSocket
    let usb_to_ws = tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            match transport_read.read(&mut buf).await {
                Ok(0) => {
                    info!("USB read returned 0 bytes, closing");
                    break;
                }
                Ok(n) => {
                    let data = buf[..n].to_vec();
                    if let Err(e) = ws_writer.send(Message::Binary(data)).await {
                        error!("Failed to send to WebSocket: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("USB read error: {}", e);
                    break;
                }
            }
        }
    });

    // WebSocket → USB
    let ws_to_usb = tokio::spawn(async move {
        while let Some(msg) = ws_reader.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if let Err(e) = transport_write.write(&data).await {
                        error!("USB write error: {}", e);
                        break;
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("WebSocket closed by server");
                    break;
                }
                Ok(_) => {
                    // Skip non-binary messages
                    continue;
                }
                Err(e) => {
                    error!("WebSocket read error: {}", e);
                    break;
                }
            }
        }
    });

    // Wait for either direction to finish
    tokio::select! {
        _ = usb_to_ws => {
            info!("USB→WS relay ended");
        }
        _ = ws_to_usb => {
            info!("WS→USB relay ended");
        }
    }

    Ok(())
}
