# Wyze Sense to MQTT Bridge (Rust) 📡

A high-performance, lightweight, asynchronous USB-to-MQTT gateway for **Wyze Sense (V1 & V2)** sub-GHz sensors, written in native **Rust**. Supports **multi-dongle** deployments with local USB + remote WebSocket bridges.

**Wyze Sense to MQTT Bridge (Rust)** bridges your physical Wyze Sense contact, motion, leak, and climate sensors directly into Home Assistant, Node-RED, or any MQTT broker with **zero cloud dependencies**, extremely fast response times, and a premium embedded Web UI dashboard. Connect multiple USB dongles — locally or via lightweight remote bridges — for whole-home coverage from a single gateway.

---

## ✨ Features

*   **🚀 Unified Daemon Architecture**: Runs a background MQTT bridge event loop, an Axum-powered REST web server, and an availability monitor concurrently in a single process with a negligible RAM footprint.
*   **🔌 Multi-Dongle Support**: Connect multiple USB dongles — locally or via remote WebSocket bridges — for whole-home RF coverage from a single gateway process.
*   **📡 Remote Dongle Bridges**: Deploy lightweight `dongle_bridge` binaries on remote machines to relay USB traffic over WebSocket back to the central gateway.
*   **🔄 Auto-Pairing & Disconnect Detection**: Sensors are automatically associated with their dongle. Disconnected dongles are detected instantly and sensors become unassociated until the dongle reconnects.
*   **🏠 Home Assistant Auto-Discovery**: Automatically registers sensors with Home Assistant showing battery states, signal strength (RSSI), and active/inactive telemetry states.
*   **🎨 Premium Embedded Web UI Dashboard**: An elegant, dark-mode control panel with per-dongle sensor cards, per-dongle scan/pair actions, and real-time disconnect indicators.
*   **🤝 Trait-Based Sensor Polymorphism**: Safe, type-secure modelling for Contact (V1/V2), Motion (V1/V2), Leak (V2), and Climate (V2) sensors.
*   **💻 Lock-Free CLI Subcommands**: Control pairing, trigger chimes, list sensors, or inject raw packets directly from your terminal *without stopping the background daemon* using automatic REST fallback routing.
*   **🔒 Safe Persistence**: Stores sensor database mappings persistently using atomic write operations to guarantee zero corruption during power losses.
*   **🧪 Comprehensive E2E Test Suite**: 14 full-stack integration tests using an in-process `VirtualDongle` simulator — no hardware required.

---

## 🛠️ Quick Start

The gateway requires physical connection to the Wyze Sense USB receiver (Bridge). The receiver typically exposes itself as `/dev/hidrawN` (usually `/dev/hidraw0`).

### Method A: Docker Compose (Recommended)

1.  Identify the `/dev/hidraw` path of your plugged-in dongle:
    ```bash
    ls -la /dev/hidraw*
    ```
2.  Create a directory to hold configuration and state files:
    ```bash
    mkdir -p config logs state
    ```
3.  Initialize the configuration file:
    ```bash
    docker run --rm ghcr.io/hclx/wyzesense2mqtt-rs:latest cat /app/config/config.yaml.template > config/config.yaml
    ```
    *Open `config/config.yaml` and configure your MQTT broker host, credentials, and logging.*
4.  Create a `docker-compose.yml` file:
    ```yaml
    services:
      wyzesense2mqtt-rs:
        container_name: wyzesense2mqtt-rs
        image: ghcr.io/hclx/wyzesense2mqtt-rs:latest
        restart: unless-stopped
        stop_signal: SIGINT
        devices:
          - "/dev/hidraw0:/dev/hidraw0"  # Map your USB dongle path here
        ports:
          - "8080:8080"                  # Web UI Port
        volumes:
          - ./config:/app/config         # Configuration directory
          - ./logs:/app/logs             # Rotation log outputs
          - ./state:/app/state           # Sensor state database
        environment:
          PUID: 1000                     # Run as your host user UID (prevents volume permission issues)
          PGID: 1000                     # Run as your host group GID
          TZ: UTC
    ```
5.  Start the container:
    ```bash
    docker compose up -d
    ```

---

### Method B: Bare Metal / Cargo Installation

1.  **Prerequisites**: Ensure you have `rustc` and `cargo` (Rust 1.80+) installed.
2.  **Clone & Compile**:
    ```bash
    git clone https://github.com/HclX/wyzesense2mqtt-rs.git
    cd wyzesense2mqtt-rs
    cargo build --release
    ```
    Compiled binaries:
    - `target/release/wyzesense2mqtt-rs` — Main gateway daemon
    - `target/release/dongle_bridge` — Remote dongle bridge relay
    - `target/release/virtual_dongle` — Virtual dongle simulator for testing
3.  **Setup USB Permissions** (Allows running without `sudo`):
    Create a udev rule at `/etc/udev/rules.d/99-wyzesense.rules`:
    ```text
    KERNEL=="hidraw*", ATTRS{idVendor}=="1a86", ATTRS{idProduct}=="e024", MODE="0666", GROUP="plugdev"
    ```
    Reload udev:
    ```bash
    sudo udevadm control --reload-rules && sudo udevadm trigger
    ```
4.  **Run the Daemon**:
    ```bash
    ./target/release/wyzesense2mqtt-rs --config config.yaml
    ```

---

## ⚙️ Configuration Profile (`config.yaml`)

A sectioned profile is used to control all subsystems. Here is a standard configuration template:

```yaml
# Wyze Sense to MQTT Bridge (Rust) Sectioned Configuration Profile
# ----------------------------------------------

# USB Dongle Settings
# "auto"          — auto-detect ALL /dev/hidraw* Wyze dongles
# "none"          — disable local USB (WebSocket bridge-only mode)
# "/dev/hidraw0"  — explicit path to a single dongle
usb:
  dongle: "auto"

# Remote Dongle Bridge Settings
# Enable to accept WebSocket connections from remote dongle_bridge instances
bridge:
  enabled: false
  # auth_token: "your_secret_token"    # Optional: require token for bridge auth

# Web Console Panel Settings
web:
  enabled: true
  port: 8080

# MQTT Broker Gateway Integration Settings
# (Set host to automatically enable Home Assistant publishing)
mqtt:
  enabled: true
  host: "localhost"
  port: 1883
  username: "homeassistant"
  password: "your_secure_password"
  self_topic_root: "wyzesense2mqtt"      # State publish root
  hass_topic_root: "homeassistant"      # Auto-Discovery root

# Diagnostics Structural Logging Level
# (Options: trace, debug, info, warn, error)
logging:
  level: "info"
  log_file: "logs/wyzesense2mqtt-rs.log"
  max_log_files: 7                      # Number of rotated files to keep
```

---

## 🎨 Web Dashboard UI Control Panel

Access the web panel by opening your browser to `http://localhost:8080` (or the port overridden in your config).

*   **🔌 Dongle-Centric Layout**: Each connected dongle appears as its own card showing MAC, firmware version, transport type (local/bridge), and remote address. Sensors are grouped under their owning dongle.
*   **🔋 Per-Dongle Sensor Tables**: Each dongle card shows its paired sensors with battery percentages, RSSI signal strength, firmware versions, and relative last-seen times.
*   **📦 Unassociated Sensors**: A special section (dashed border) shows sensors restored from state whose dongle is not currently online.
*   **⚙️ Per-Dongle Actions Modal**: Click the Actions button on any dongle to open a modal with:
    * **📡 Pairing Center** — Start/stop sensor scan (60s auto-timeout)
    * **🧹 Maintenance** — Purge ghost sensors from dongle NVRAM
    * **💻 Hex Console** — Send/receive raw HID packets for debugging
*   **🔄 Real-Time Disconnect Detection**: When a dongle disconnects, its card is instantly removed and sensors become unassociated.

---

## 💡 Home Assistant Integration

Once `mqtt` is enabled in your `config.yaml`, **Wyze Sense to MQTT Bridge (Rust)** automatically announces new devices. Home Assistant will register them as native integrations.

| Sensor Model | Entities Created | Mapped Telemetry State Values |
| :--- | :--- | :--- |
| **Contact (V1/V2)** | Binary Sensor, Battery, Signal | `open` / `closed` |
| **Motion (V1/V2)** | Binary Sensor, Battery, Signal | `active` / `inactive` |
| **Leak (V2)** | Moisture Binary Sensor, Probe Status, Battery, Signal | `wet` / `dry`, `connected` / `disconnected` |
| **Climate (V2)** | Temperature (C), Humidity (%), Battery, Signal | Floats (e.g. `22.45°C`, `48%`) |

---

## 💻 CLI Client Subcommands Reference

The single compiled binary acts as both the background daemon and a lightweight command-line client tool.

### Usage Layout
```bash
wyzesense2mqtt-rs [SUBCOMMAND] [OPTIONS]
```

### Subcommands
*   **`list`**: Queries and prints all paired sensor MACs recorded inside the dongle.
*   **`pair`**: Enters pair scanning mode, waits for the sensor reset pin trigger, exchanges crypto tokens, and binds the sensor dynamically.
*   **`unpair <MAC>`**: Permanently unpairs and deletes a sensor MAC address from the dongle.
*   **`chime <MAC>`**: Triggers play chime sequence on compatible chime-enabled sensors.
*   **`fix`**: Performs a quick diagnostics sweep and purges invalid "ghost" sensors (e.g. empty or corrupt MAC keys like `00000000`).
*   **`raw <HEX_BYTES>`**: Directly write raw hex packet sequences (e.g. `AA,55,43,03,04,01,49`) and wait 1s for return bytes.

> [!NOTE]  
> **How Lock-Free Command routing works**: If the background daemon is running, executing `wyzesense2mqtt-rs pair` in your terminal will not crash or conflict with `/dev/hidraw0`. The CLI tool automatically detects the running daemon via local HTTP check, routes the request over a REST call to the daemon process, and streams the output, giving you instant terminal control!

---

## 🔍 Troubleshooting

### 1. `/dev/hidraw0: Permission denied`
*   **In Docker**: Ensure your host user is mapped to the container using `PUID` and `PGID` environment variables, or run the container in privileged mode.
*   **On Host**: Add the udev rule listed in the bare-metal installation section and restart udev.

### 2. Multiple `/dev/hidraw` devices
With `usb.dongle: "auto"`, the gateway auto-discovers **all** matching Wyze Sense dongles via sysfs and creates an engine for each. If you want to restrict to a specific dongle, set `dongle: "/dev/hidrawX"` explicitly. To disable local USB entirely (WebSocket bridges only), set `dongle: "none"`.

### 3. Capturing raw data for debugging
To capture raw USB packet logs, increase log verbosity in `config.yaml`:
```yaml
logging:
  level: "trace"
```
This records all byte read/write transactions. You can extract captured frames for test replays using the Python script provided in `tools/extract_packets.py`.

---

## 🔌 Remote Dongle Bridge

Deploy the `dongle_bridge` binary on any machine with a USB dongle to relay traffic to your central gateway over WebSocket:

```bash
./dongle_bridge --device /dev/hidraw0 --gateway ws://gateway-host:8080/ws/bridge
```

Optional flags:
*   `--token SECRET` — Authenticate with the gateway (requires `bridge.auth_token` in gateway config)

The bridge is a transparent byte-level relay (~200 lines) with automatic reconnection. Multiple bridges can connect simultaneously for whole-home coverage.

---

## 🧪 Testing

The project includes a comprehensive E2E test suite using in-process `VirtualDongle` simulators — **no hardware required**.

```bash
# Run all tests (unit + integration + E2E)
cargo test

# Run only the 14 full-stack E2E tests
cargo test --test full_e2e_test
```

The E2E suite covers multi-dongle lifecycle, disconnect detection, sensor re-pairing across dongles, leak/climate events, and battery level propagation.

For development/manual testing, the `virtual_dongle` binary simulates dongles with YAML-configured pre-paired sensors:

```bash
./virtual_dongle --config dongle.yaml
```

---

## ⚖️ Legal Disclaimer

**This is a personal, hobbyist open-source project.** 

*   **No Affiliation**: This project is completely independent and has **no affiliation, association, authorization, endorsement, or official connection in any way** with **Wyze Labs, Inc.** or any of its subsidiaries or affiliates. The official Wyze website can be found at [https://wyze.com](https://wyze.com). "Wyze" as well as related names, marks, emblems, and images are registered trademarks of their respective owners.
*   **No Legal Responsibility & Warranty**: This software is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement. In no event shall the authors or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software. You use this gateway entirely at your own risk.

---

## 🙌 Acknowledgments

This project was heavily inspired by and builds upon the excellent reverse-engineering and bridge work done by **[@raetha](https://github.com/raetha)** in the original [wyzesense2mqtt](https://github.com/raetha/wyzesense2mqtt) Python implementation. We are deeply grateful to the open-source community contributors who made low-level sub-GHz Wyze sensor integrations possible.

---

## 📄 License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.


