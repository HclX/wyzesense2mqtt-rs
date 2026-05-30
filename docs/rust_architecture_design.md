# Wyze Sense to MQTT Bridge (Rust): Unified Target Architecture Design

This document specifies the **Unified Target Architecture** for the **Wyze Sense to MQTT Bridge (Rust)**. To resolve USB hardware locking constraints (since Linux only allows one process to open `/dev/hidraw0` at a time), the project is compiled into a **single unified executable binary** (`wyzesensers`) that coordinates the background MQTT bridge gateway, the HTTP control REST server, and the CLI subcommands.

---

## 1. Conceptual Architecture

The single executable operates in one of two primary modes depending on CLI arguments:
1.  **Daemon Mode (Default)**: Spawns a central asynchronous coordinator holding a single USB `Engine` handle, while running the MQTT gateway adapter task and the Web HTTP server task concurrently in the background.
2.  **CLI Subcommand Mode**: Dispatched by calling subcommands (`wyzesense2mqtt-rs list`, `unpair <mac>`, `chime <mac>`, `pair`, `fix`, `raw <hex>`).
    *   **Daemon-Client Route (Primary)**: The CLI attempts a quick HTTP query to check if the local daemon is active on the network. If yes, it acts as a thin REST client, executing the requested operation via local HTTP REST requests (completely avoiding USB device locking!).
    *   **Direct HID Fallback (Secondary)**: If the local daemon is inactive, the CLI opens `/dev/hidraw0` directly to perform the target query offline and closes it immediately.

```
               Wyze Sense to MQTT Bridge (Rust) (Single Process)
        +------------------------------------------------------------------------------+
        |                                                                              |
        |   +-----------------------+  +-----------------------+  +----------------+   |
        |   |   Axum Web UI Task    |  |  MQTT Gateway Task    |  |  CLI Client    |   |
        |   | (REST Server: port)   |  |  (rumqttc Publisher)  |  |  Subcommand    |   |
        |   +-----------+-----------+  +-----------+-----------+  +-------+--------+   |
        |               |                          |                      |            |
        |               +-------------+------------+                      | (HTTP API) |
        |                             |                                   |            |
        |                             v                                   |            |
        |                    +--------+--------+                          |            |
        |                    |  Engine Actor   | <------------------------+            |
        |                    | (Arc<Mutex<E>>) |                                       |
        |                    +--------+--------+                                       |
        |                             |                                                |
        |                             v                                                |
        |                    +--------+--------+                                       |
        |                    |  USB hidraw0    |                                       |
        |                    +-----------------+                                       |
        |                                                                              |
        +------------------------------------------------------------------------------+
```

---

## 2. Unified Executable Modes

### 2.1 The Daemon Mode
Booted by executing `wyzesense2mqtt-rs` without subcommands.
- Instantiates a single `Engine` actor wrapped in `Arc<Mutex<Engine<HidrawTransport>>>`.
- Reads `config/sensors.yaml` to load configured sensor metadata.
- Performs the 5-step handshake verification to unlock the USB dongle.
- Spawns the background MQTT Gateway task listening on Tokio channels, publishing state payloads, and generating Home Assistant discovery templates.
- Spawns the background Axum HTTP Web Panel server, hosting the packed single-page Tailwind console dashboard.
- Spawns the background Availability Monitor task to safely track timeouts and persist states atomically to `state.json`.

### 2.2 The CLI Client Subcommands
Booted by appending subcommands to `wyzesense2mqtt-rs`.

#### Subcommand REST Mapping:
*   **`wyzesense2mqtt-rs list`**:
    *   *REST Endpoint*: `GET /api/sensors`
    *   *Fallback*: Calls `engine.get_sensor_list().await` directly via local HID.
*   **`wyzesense2mqtt-rs pair`**:
    *   *REST Endpoint*: Puts the server in scan mode via `POST /api/scan`, listens to the server's events, automatically calls `POST /api/verify` upon discovery, and stops scan.
    *   *Fallback*: Calls `engine.set_scan` and `engine.verify_sensor` locally.
*   **`wyzesense2mqtt-rs unpair <mac>`**:
    *   *REST Endpoint*: `DELETE /api/sensors/:mac`
    *   *Fallback*: Calls `engine.delete_sensor(mac)` locally.
*   **`wyzesense2mqtt-rs chime <mac>`**:
    *   *REST Endpoint*: `POST /api/chime/:mac`
    *   *Fallback*: Calls `engine.play_chime(mac)` locally.
*   **`wyzesense2mqtt-rs fix`**:
    *   *REST Endpoint*: `POST /api/fix`
    *   *Fallback*: Iterates over local list and purges invalid MAC entries.
*   **`wyzesense2mqtt-rs raw <hex_bytes>`**:
    *   *REST Endpoint*: `POST /api/raw`
    *   *Fallback*: Calls `engine.do_command` locally.

---

## 3. Benefits of the Unified Target Design
1.  **Robust Port Coordination**: Guarantees that the MQTT daemon and the Web Server never fight over `/dev/hidraw0` ownership.
2.  **Lock-Free Diagnostic CLI**: Users can run pairing commands, trigger chimes, list sensors, or run raw hex packet diagnostic injections directly from a terminal *while the background MQTT service is actively running* without stopping the daemon!
3.  **Single-File Deployments**: Serves as a single, packed executable containing the entire web console, REST endpoints, and CLI tools. Great for compact Docker images or Raspberry Pi execution!
