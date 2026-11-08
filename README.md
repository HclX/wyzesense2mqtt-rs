# Wyze Sense to MQTT Bridge (Rust) 📡

A high-performance, lightweight, asynchronous USB-to-MQTT gateway for **Wyze Sense (V1 & V2)** sub-GHz sensors, written in native **Rust**. Supports **multi-dongle** deployments with local USB + remote WebSocket bridges.

**Wyze Sense to MQTT Bridge (Rust)** bridges your physical Wyze Sense contact, motion, leak, and climate sensors directly into Home Assistant, Node-RED, or any MQTT broker with **zero cloud dependencies**, extremely fast response times, and a premium embedded Web UI dashboard. Connect multiple USB dongles — locally or via lightweight remote bridges — for whole-home coverage from a single gateway.

---

> [!NOTE]
> **Branches and images.** This is the `ows` branch: the daemon with [OpenWyzeSense](https://github.com/HclX/openwyzesense) support (roaming between gateways, firmware install, relay and sensor updates). Its image is `ghcr.io/hclx/wyzesense2mqtt-rs:ows`. The `main` branch is the same daemon without OpenWyzeSense (images `dev`, and `latest` / version tags from `release`).

## ✨ Features

*   **🚀 Unified Daemon Architecture**: Runs a background MQTT bridge event loop, an Axum-powered REST web server, and an availability monitor concurrently in a single process with a negligible RAM footprint.
*   **🔌 Multi-Dongle Support**: Connect multiple USB dongles — locally or via remote WebSocket bridges — for whole-home RF coverage from a single gateway process.
*   **📡 Remote Dongle Bridges**: Deploy lightweight `dongle_bridge` binaries on remote machines to relay USB traffic over WebSocket back to the central gateway.
*   **🔄 Auto-Pairing & Disconnect Detection**: Sensors are automatically associated with their dongle. Disconnected dongles are detected instantly and sensors become unassociated until the dongle reconnects.
*   **🏠 Home Assistant Auto-Discovery**: Automatically registers sensors with Home Assistant showing battery states, signal strength (RSSI), and active/inactive telemetry states.
*   **🎨 Premium Embedded Web UI Dashboard**: An elegant, dark-mode control panel with per-dongle sensor cards, per-dongle scan/pair actions, and real-time disconnect indicators.
*   **🤝 Trait-Based Sensor Polymorphism**: Safe, type-secure modelling for Contact (V1/V2), Motion (V1/V2), Leak (V2), and Climate (V2) sensors.
*   **⌨️ Keypad Support**: The Wyze Sense keypad works without the Wyze Hub. The gateway answers the keypad the way the Hub does, with Home Assistant (e.g. Alarmo) holding the alarm state and checking PINs. See the Keypad section below and the Alarmo section for setup.
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

#### Replugging a dongle under Docker

A `devices:` entry gives the container the one device file that existed when it started. When a dongle is unplugged and plugged back in, it can come back as a different `/dev/hidrawN`, and the container then can't open it until you recreate it. To let the daemon follow dongles as they come and go (see [Multiple `/dev/hidraw` devices](#2-multiple-devhidraw-devices)), give the container the host's `/dev` and allow hidraw devices only:

```yaml
    # instead of `devices:`
    device_cgroup_rules:
      - "c 239:* rmw"   # 239: the first number `ls -l /dev/hidraw*` prints on your host
    volumes:
      - /dev:/dev
      # ...your other volumes
```

The rule limits the container to hidraw devices, and the daemon only opens Wyze dongles (USB ID `1a86:e024`), so other containers' devices are unaffected.

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

# OpenWyzeSense gateway features: off unless turned on here, except roaming,
# which is automatic. Stock dongles are detected and never touched.
# Env: OWS_DEDUP, OWS_FIX_ZERO_MAC, OWS_ANNOUNCE, OWS_ROAM
openwyzesense:
  dedup: false                          # Forward each sensor resend once
  fix_zero_mac: false                   # Give v1 contacts' zero-MAC reports their real MAC
  # roam: true                          # Roaming between gateways sharing one network identity:
                                        # automatic when unset; false keeps a gateway out
  roam_hysteresis_db: 10                # How much stronger another gateway must hear a sensor to take it
  # gateways:                           # Per-gateway overrides, keyed by gateway MAC
  #   "77E0000A": { roam: true, dedup: true }

# Keypad (see the Keypad section)
keypad:
  enabled: true
  alarm_state_topic: "wyzesense2mqtt/keypad/alarm_state"  # default; e.g. "alarmo/state"
  response_timeout_ms: 2000             # wait for HA before rejecting a PIN

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
| **Keypad** | Motion Binary Sensor, Button Event, Battery, Signal | `active` / `inactive`; events `disarm`, `arm_home`, `arm_away`, `panic` |

---

## ⌨️ Keypad

The Wyze Sense keypad never learns the alarm state on its own: it asks, and the Hub answers (on a button press, when PIN entry starts, and when its motion sensor wakes it). The gateway takes the Hub's place. Home Assistant stays in charge of the alarm and of checking PINs; the gateway only relays.

**PINs (codes) are managed in your alarm panel**, not the gateway (see the [Alarmo](#-alarmo-alarm-panel) section below).

| Topic | Direction | Retained | Payload |
| :--- | :--- | :--- | :--- |
| `<root>/keypad/alarm_state` (or `keypad.alarm_state_topic`) | HA → gateway | yes | HA alarm state: `disarmed`, `arming`, `armed_home`, `armed_away`, `pending`, `triggered`, … |
| `<root>/<MAC>/keypad/action` | gateway → HA | **no** | `{"action": "disarm" \| "arm_home" \| "arm_away", "code": "1234", "keypad": "<MAC>"}` (`code` only when a PIN was typed) |
| `<root>/keypad/pin_result` or `<root>/keypad/<MAC>/pin_result` | HA → gateway | no | `invalid` rejects the pending PIN immediately instead of after `response_timeout_ms`; the per-keypad topic (or `"keypad": "<MAC>"` in the payload) rejects only that keypad's request |
| `<root>/<MAC>/keypad/event` | gateway → HA | no | `{"event_type": "disarm" \| "arm_home" \| "arm_away" \| "panic"}` (HA event entity, never a PIN) |
| `homeassistant/status` | HA → gateway | no | HA birth (`online`): the gateway re-announces every known sensor's availability and state |

How requests are answered:
- **Disarm button:** the keypad is asked for a PIN. The PIN is then sent as a `disarm` action.
- **Home / Away button:** an `arm_*` action without a code is sent. If the alarm state changes to `arming` or the armed state within the timeout, the keypad shows it; otherwise it asks for a PIN and the PIN is sent with the same action.
- **PIN without a button** (e.g. during the entry delay): sent as `disarm`.
- **PIN result:** accepted once the alarm state reaches the requested state, rejected on `invalid` (publish to `<root>/keypad/<MAC>/pin_result` or name the `keypad` in the payload to reject just that one; a bare `invalid` on the shared topic rejects every pending request), `alarmo_failed_to_arm` with reason `invalid_code`, or timeout.
- **Side button:** only the `panic` event is published; what it does is up to your automations.

Keypad battery: every keypad event carries a battery byte on its own 0–155 scale (not the AON_BATMON encoding other sensors use). The gateway publishes it as an estimated `Battery` percentage (raw ÷ 155) plus a `Battery Voltage` diagnostic (an internal reading of ≈4.5 V on healthy packs — not a direct cell voltage). The percentage is a linear estimate; the true discharge shape is not yet characterized, so treat low readings as a prompt to recharge soon rather than an exact gauge.

Keypad motion: the built-in PIR is what wakes the display when someone approaches — it is not a latched occupancy detector. A wave publishes `active`, and the keypad reports `inactive` as soon as the PIR clears, so expect brief pulses on the binary sensor, and expect the dashboard to show `Clear` again right after motion stops. This matches the Hub's behaviour (it answers motion-detected with the current alarm mode and never replies to motion-cleared).

> [!IMPORTANT]
> The action topic carries PINs. It is never retained, but anything subscribed to it sees them, so restrict it with broker ACLs. PINs are redacted from the gateway's logs, except at `trace` level, where the USB transport logs raw bytes.

Availability self-healing: only `online` statuses are retained. A sleeping sensor's `offline` is not kept by the broker, so entities don't stay stuck "unavailable" for hours after the sensor wakes and reports again — the next report (or a gateway/HA restart, or HA's MQTT birth `homeassistant/status` → re-announce) restores the correct state. The same applies to the gateway's own LWT: an unclean shutdown marks entities unavailable without persisting that across broker restarts.

---

## 🚨 Alarmo (Alarm Panel)

The keypad works with any `alarm_control_panel`; the notes below use [Alarmo](https://github.com/nielsfaber/alarmo).

**PINs (codes) are managed in Alarmo**, not the gateway: open Alarmo → Codes and add the code under a user (enable "use disarm code" for the areas where it should apply). Every alarm_control_panel service call already accepts a `code`, so a keypad PIN is validated the same way as one typed into the HA UI.

Example Home Assistant automations for Alarmo (the same works for any `alarm_control_panel`):

```yaml
- alias: "Wyze keypad → alarm panel"
  mode: queued
  triggers:
    - trigger: mqtt
      topic: "wyzesense2mqtt/+/keypad/action"
  actions:
    - action: "alarm_control_panel.alarm_{{ trigger.payload_json.action }}"
      target:
        entity_id: alarm_control_panel.alarmo
      data: "{{ {'code': trigger.payload_json.code} if 'code' in trigger.payload_json else {} }}"
      continue_on_error: true

- alias: "Alarm state → Wyze keypads"
  triggers:
    - trigger: state
      entity_id: alarm_control_panel.alarmo
  actions:
    - action: mqtt.publish
      data:
        topic: "wyzesense2mqtt/keypad/alarm_state"
        payload: "{{ trigger.to_state.state }}"
        retain: true

- alias: "Alarmo rejected a code from a Wyze keypad"
  id: wyze_keypad_pin_result
  triggers:
    - trigger: event
      event_type: alarmo_failed_to_arm
  conditions:
    - condition: template
      value_template: "{{ trigger.event.data.reason == 'invalid_code' }}"
  actions:
    - action: mqtt.publish
      data:
        topic: "wyzesense2mqtt/keypad/pin_result"
        # Use "wyzesense2mqtt/keypad/<MAC>/pin_result" (or {"keypad": "<MAC>",
        # "result": "invalid"}) to reject only that keypad; a bare "invalid"
        # here rejects every pending PIN request.
        payload: "invalid"
```

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

Local dongles can be plugged in and out while the daemon runs. It checks for them every 2 seconds: a dongle plugged in (or back in) is attached, and an unplugged one is detached and its sensors are marked as having no dongle until it returns. The daemon also starts with no dongle plugged in and waits for one. A dongle that doesn't answer the handshake is retried when it is replugged, or after 60 seconds.

### 3. Capturing raw data for debugging
To capture raw USB packet logs, increase log verbosity in `config.yaml`:
```yaml
logging:
  level: "trace"
```
This records all byte read/write transactions (including keypad PIN digits). You can extract captured frames for test replays using the Python script provided in `tools/extract_packets.py`.

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

## 🛰️ OpenWyzeSense Gateways & Roaming

On every connect the daemon checks whether a gateway runs [OpenWyzeSense](https://github.com/HclX/openwyzesense) firmware (a stock dongle ignores the check). For ours, the dashboard and `GET /api/dongles` show the firmware version and its supported and enabled feature flags. The features in the `openwyzesense` config section are off unless you turn them on, either for all gateways or per gateway, except roaming (below), which is automatic. They are applied after the handshake, and re-applied whenever a gateway loses them (they reset when the gateway restarts).

Gateways can share one network identity (for example, several migrated from the same stock dongle, or a relay set up with the dongle's identity). A sensor is then heard by every gateway in range without re-pairing. Roaming turns on by itself once two of our gateways carry the same identity: without it, every one of them would answer every sensor of the network and report every join. `roam: true` turns it on for a gateway regardless; `roam: false` (for all gateways or one) keeps a gateway out. With roaming on:

*   **One owner per sensor.** Only the owning gateway ACKs a sensor. The others forward what they overhear, and the daemon merges all copies into one event.
*   **Automatic handover.** A sensor moves when its owner keeps missing reports that others hear, or (while the owner is cut off from the daemon) when every report in a row arrives retried because nobody answers it, or when another gateway hears it at least `hysteresis_db` stronger over many reports. A gateway that only lost its link to the daemon keeps its sensors, because its radio still answers them. The sensor keeps its address and never notices.
*   **Pairing.** The pairing gateway gets an address range above every address in use, and only one gateway of the network pairs at a time.

`GET /api/roaming` shows each gateway's table and how strongly each gateway hears each sensor. Design notes: [docs/roaming.md](docs/roaming.md).

### Installing OpenWyzeSense on a USB dongle

The dashboard can install OpenWyzeSense on a local USB dongle, update it, and put the stock firmware back: open the dongle's **Actions** dialog and use the **Firmware** section. The firmware comes from the [OpenWyzeSense release](https://github.com/HclX/openwyzesense/releases) this build is pinned to, and is checked against the release's SHA-256 before it is used.

1.  **Backup.** The whole chip is read and saved under `state/backups/<dongle MAC>-<time>/`. A backup taken while the dongle runs stock firmware is the only way back to stock, so keep it. The dongle is offline for about a minute and then comes back by itself.
2.  **Review.** The dashboard lists the sensors that carry over and any that don't (for example, two sensors sharing one address in the stock table): those need pairing again afterwards. Nothing has been written yet; **Cancel** leaves the dongle as it was.
3.  **Install.** The firmware is written and verified, and the sensor table is converted (about 20 seconds). The dongle normally comes back on its own; if it doesn't, the dashboard asks you to unplug it and plug it back in.

**Restore stock firmware** writes the newest stock backup back; sensors paired since that backup need pairing again.

### Updating an OpenWyzeSense relay

A bridged OpenWyzeSense relay (an ESP32, ESP32-S3 or ESP8266 next to a contact board) is updated with its own firmware: its Actions dialog shows the relay's chip and build and offers **Update relay**. The daemon downloads the relay image for that chip from the same release, checks it, and uploads it to the relay at the address its WebSocket connects from. The relay restarts into the new firmware, brings its contact board's radio up to the gateway firmware it carries, and reconnects; a build that can't rejoin Wi-Fi rolls back by itself, and the dashboard says so. A relay with an admin password asks for it in the dialog (it isn't stored); the bridge token (`bridge.auth_token`) is tried when no password is given. For a machine without internet access, set `openwyzesense.firmware_source` (or `OWS_FIRMWARE_SOURCE`) to a directory holding the release's `manifest.json` and dongle image.

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


