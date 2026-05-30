# wyzesense2mqtt: Original Python Project Analysis

This document outlines the structure, protocols, and behavior of the original [wyzesense2mqtt](https://github.com/HclX/wyzesense2mqtt) Python project. It serves as the foundational reference for our **Rust rewrite**, capturing how the system currently works and proposing design areas to improve during the next stages.

---

## 1. Conceptual Overview

The project is a **WyzeSense-to-MQTT Gateway**. It bridges Wyze Sense binary/environment sensors with Home Assistant or any other platform using MQTT discovery.

```
                   +--------------------------+
                   |   Wyze Sense USB Dongle  |
                   +-------------+------------+
                                 | USB HID (/dev/hidraw0)
                                 v
            +----------------------------------------+
            |            wyzesense2mqtt              |
            |  - USB Worker (read/write/handshake)   |
            |  - Event Handler (parse state/battery) |
            |  - MQTT Publisher & Subscriber         |
            +--------------------+-------------------+
                                 | MQTT Topics
                                 v
                   +-------------+------------+
                   |        MQTT Broker       |
                   +-------------+------------+
                                 |
                   +-------------v------------+
                   |      Home Assistant      |
                   +--------------------------+
```

### Target Hardware
* **USB Dongle:** Wyze Sense Bridge (WHSB1) / Neos Smart Bridge (N-LSP-US1).
* **Sensors:**
  * Contact Sensor V1 & V2 (door/window state)
  * Motion Sensor V1 & V2 (active/inactive state)
  * Leak Sensor V2 (dry/wet state + probe availability)
  * Climate Sensor V2 (temperature + humidity)

---

## 2. USB HID & Protocol Analysis (`wyzesense.py`)

The gateway communicates directly with the USB Dongle via raw HID reads and writes to `/dev/hidraw0` (or dynamically discovered hidraw paths).

### 2.1 USB HID Communication
* Opened with: `os.open(device, os.O_RDWR | os.O_NONBLOCK)`
* Raw read limit: Reads at most `0x40` (64) bytes at a time.
* Packets start with **Magic Bytes** (`0xAA55` when writing to the dongle, `0x55AA` or `0xAA55` when reading from it).

### 2.2 Packet Format
Every packet conforms to the following byte layout:

| Offset | Size (Bytes) | Name | Description / Values |
| :--- | :--- | :--- | :--- |
| 0 | 2 | **Magic Prefix** | `0xAA55` (Send) or `0x55AA`/`0xAA55` (Receive) |
| 2 | 1 | **Packet Type** | `0x43` (Sync - Host initiated), `0x53` (Async - Dongle initiated/Events) |
| 3 | 1 | **Length/State** | Payload length + 3, or ACK state payload |
| 4 | 1 | **Command ID** | Specific command or notification ID |
| 5 | Variable | **Payload** | Actual data payload of the command (size = Length - 3) |
| Variable | 2 | **Checksum** | Modulo 16-bit sum of all preceding packet bytes: `sum(bytes) & 0xFFFF` |

### 2.3 Command & Notification IDs

#### Host-Initiated Sync Commands
* `CMD_GET_ENR` (`0x4302`): Get ENR token for handshake.
* `CMD_GET_MAC` (`0x4304`): Read the MAC address of the USB dongle.
* `CMD_GET_KEY` (`0x4306`): Read the encryption key of the dongle.
* `CMD_INQUIRY` (`0x4327`): Query dongle readiness.
* `CMD_UPDATE_CC1310` (`0x4312`): Request CC1310 firmware update.
* `CMD_SET_CH554_UPGRADE` (`0x430E`): Request CH554 upgrade.

#### Dongle-Initiated Async Commands / Control
* `ASYNC_ACK` (`0x53FF`): Handshake acknowledgment.
* `CMD_FINISH_AUTH` (`0x5314`): Signal completion of host handshake.
* `CMD_GET_DONGLE_VERSION` (`0x5316`): Retrieve dongle version string.
* `CMD_START_STOP_SCAN` (`0x531C`): Toggle pairing mode (`0x01` to start, `0x00` to stop).
* `CMD_GET_SENSOR_R1` (`0x5321`): Pair handshake payload.
* `CMD_VERIFY_SENSOR` (`0x5323`): Bind/pair verification command.
* `CMD_DEL_SENSOR` (`0x5325`): Delete/unpair sensor by MAC.
* `CMD_DEL_ALL_SENSORS` (`0x533F`): Unpair all sensors.
* `CMD_GET_SENSOR_COUNT` (`0x532E`): Ask for the number of paired sensors.
* `CMD_GET_SENSOR_LIST` (`0x5330`): Fetch paired sensor MAC list.
* `CMD_PLAY_CHIME` (`0x5370`): Request chime/alarm execution (for certain devices).

#### Dongle-to-Host Async Notifications
* `NOTIFY_SENSOR_ALARM` (`0x5319`): Triggered when a sensor sends state (V1 & some V2).
* `NOTIFY_SENSOR_ALARM2` (`0x5355`): Used for V2 leak sensor.
* `NOTIFY_SENSOR_SCAN` (`0x5320`): Triggered when a new sensor is found during scan.
* `NOTIFY_SYNC_TIME` (`0x5332`): Periodic time sync request from dongle. Gateway must respond with `SyncTimeAck` containing UTC Unix timestamp in milliseconds (`struct.pack(">Q", int(time.time() * 1000))`).
* `NOTIFY_EVENT_LOG` (`0x5335`): General system logging events reported by the CC1310 microcontroller.

---

### 2.4 Unlock Handshake Sequence (`Dongle._Start`)
When first opening the device, the host **must** perform a handshake sequence. If this fails, the dongle will not report sensor events.

```
Host                                                     Dongle
 |                                                         |
 |--------------------- CMD_INQUIRY ---------------------->|
 |<------------------ INQUIRY_RESPONSE --------------------| (Assert result == 1)
 |                                                         |
 |---------- CMD_GET_ENR (with 16-byte token) ------------>|
 |<------------------ ENR_RESPONSE ------------------------| (16 bytes returned)
 |                                                         |
 |-------------------- CMD_GET_MAC ----------------------->|
 |<------------------ MAC_RESPONSE ------------------------| (8 bytes ASCII MAC)
 |                                                         |
 |------------------ CMD_GET_VERSION --------------------->|
 |<----------------- VERSION_RESPONSE ---------------------| (ASCII Version String)
 |                                                         |
 |------------------- CMD_FINISH_AUTH -------------------->|
 |<----------------- FINISH_AUTH_RESPONSE -----------------| (0-byte payload)
 v                                                         v
 [ Dongle is unlocked; events will now flow ]
```

---

## 3. Application Architecture (`wyzesense2mqtt.py`)

The service utilizes a multi-threaded polling approach:

1. **Worker Thread (`Dongle._Worker`):**
   - Runs an infinite loop doing non-blocking HID reads.
   - Parses incoming bytes to locate matching packet headers (`0x55AA` / `0xAA55`).
   - Dispatches packets to specific callback functions (e.g., `_OnSensorAlarm`, `_OnSyncTime`).
   - Manages synchronous request-reply execution by letting threads block on a `threading.Event` which gets signaled by the worker thread when a reply packet arrives.
   
2. **Main Thread:**
   - Boots configuration and logging.
   - Connects to the MQTT Broker asynchronously using `paho-mqtt`.
   - Starts the `Dongle` interface, forcing the unlock handshake.
   - Periodically monitors:
     - Thread health (e.g., catching any exception raised in the worker thread).
     - Sensor freshness / online-offline availability timeout.
     - Reconnecting to MQTT broker if connection drops.

### 3.1 Configuration Files
The application reads and writes configurations using standard YAML files stored in `config/`:
* **`config.yaml`**: Broker credentials, port, QoS, client identifier, and Home Assistant auto-discovery toggle.
* **`logging.yaml`**: Python dictionary configuration for rotating log file handlers.
* **`sensors.yaml`**: Lists paired sensor MACs, mapping them to a user-friendly `name`, `class` (door, window, motion, moisture), optional `invert_state` flag, `sw_version` (19 for V1, 23 for V2), and custom timeout values.
* **`state.yaml`**: Records the persistent state of sensors (last seen timestamp, current online/offline status) to retain status across gateway restarts.

---

## 4. Home Assistant & State Processing

### 4.1 Sensor Payload Parsing
Depending on the notification type, payloads are parsed as follows:

* **Standard alarm packets (`NOTIFY_SENSOR_ALARM` / `SensorEvent._AlarmParser`):**
  Unpacked as `>BBBBBHB` (Type, Battery, State, Sequence, RSSI, etc.).
  - **Battery:** Extracted and capped at `100%`. For V2 contact sensors (`switchv2`), the battery percentage is doubled since they run on a single 1.5V battery instead of two.
  - **Signal Strength (RSSI):** Stored as negative value (e.g. `-RSSI` in dBm).
  - **State:** Mapped to target binary sensor states (`closed`, `open`, `inactive`, `active`, `dry`, `wet`).

* **Leak alarm packets (`NOTIFY_SENSOR_ALARM2` / `SensorEvent._LeakParser`):**
  Unpacked as `>BBBBBBBBBBB` representing the main leak state, extension probe connection state, and probe availability.

* **Climate packets (`SensorEvent._ClimateParser`):**
  Unpacked as `>BBBBBBBBBB` to read and combine temperature and humidity.
  `temperature = temp_hi + (temp_lo / 100.0)`.

### 4.2 Availability Timeout
The gateway tracks when a device was last seen. If a sensor misses its periodic reporting period:
* **V1 Sensors:** Report every 4 hours -> Availability timeout defaults to **8 hours** (2 periods).
* **V2 Sensors:** Report every 2 hours -> Availability timeout defaults to **4 hours** (2 periods).
If the timeout expires, the gateway publishes an `offline` state to the sensor status topic.

### 4.3 Interactive Management via MQTT
The gateway subscribes to three control topics under `self_topic_root`:
1. **`self_topic_root/scan`**: Puts the dongle in scan mode. If a sensor is found, it is registered, added to `sensors.yaml`, and Home Assistant discovery topics are emitted.
2. **`self_topic_root/remove`**: Given a MAC address as the payload, it requests the dongle to unpair the sensor, deletes the entry from `sensors.yaml`, and clears all associated MQTT topics.
3. **`self_topic_root/reload`**: Reloads `sensors.yaml` without restarting the container/service.

---

## 5. Points of Consideration for our Rust Rewrite

While transitioning this project to Rust, we have a great opportunity to improve reliability, performance, and safety. Here are the initial architectural directions we should consider:

1. **HID / USB Library:**
   - Instead of rolling raw `os.read` on `/dev/hidraw0` directly, we should leverage the cross-platform [hidapi](https://crates.io/crates/hidapi) or [rusb](https://crates.io/crates/rusb) crate, or stick to asynchronous file handling with [tokio-file](https://crates.io/crates/tokio-file) / raw file handling in Linux depending on the goals.
   - Using `hidapi` gives us standard read/write features on USB devices across platforms safely.

2. **Concurrency Model:**
   - The Python version runs raw worker threads, global state manipulation, and sleep-polling.
   - **Rust Proposal:** Use **Tokio** for asynchronous task orchestration.
   - We can model the USB dongle interface as a task reading from the HID interface and dispatching events through a Tokio channel (`tokio::sync::mpsc`).
   - The MQTT interface can be an asynchronous task (e.g., using `rumqttc` or `mqtt-protocol`) listening to the channels and handling subscriptions.

3. **Safety & Parsing:**
   - Replacing Python's dynamic and error-prone `struct.unpack` parsing with structured, type-safe parsing.
   - Using crates like [binrw](https://crates.io/crates/binrw) or [nom](https://crates.io/crates/nom) to declaratively parse the binary packets without unsafe pointer slicing.

4. **Configuration Management:**
   - Using [serde](https://serde.rs/) alongside `serde_yaml` or standard JSON/TOML config files (which are often easier to handle without complex format engines).

---

## 6. Stage 1 Sign-Off & Next Steps

To align on this plan, please let me know:
1. Does this accurately capture the core mechanics of the Python project you want to rewrite?
2. What are the specific issues or "things you want to clarify/improve" right away?
3. Which layout or libraries would you prefer we target for the Rust side (e.g., async/Tokio, sync, specific serial/HID library preferences)?

Once you've reviewed this analysis, we will progress to **Stage 2: Requirements & Architecture Design**, where we formulate the precise design of our new Rust system.
