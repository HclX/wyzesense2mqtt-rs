# WyzeSenseRS: Implementation Plan

This document maps out the step-by-step development phases for **WyzeSenseRS**. We follow a highly modular process, ensuring each component is testable using the **Replay Test Harness** before we proceed to integration.

---

## Phase 1: Project Setup & Protocol Definitions
**Goal:** Bootstrap the Rust cargo crate, configure dependencies, and define type-safe binary structures for packet parsing.

1. **Initialize Cargo Project:**
   - Initialize a binary crate: `cargo init --bin`
   - Configure `Cargo.toml` with core dependencies:
     - `tokio` (features: `full`, async framework)
     - `binrw` (declarative binary structure parsing)
     - `serde` & `serde_yaml` & `serde_json` (configuration parsing & state serialization)
     - `async-trait` (async trait definition support)
     - `tracing` & `tracing-subscriber` (structured diagnostic logging)
     - `rumqttc` (async MQTT client)

2. **Protocol Types (`src/protocol/packet.rs`):**
   - Define `CommandType` enum (`Sync` = `0x43`, `Async` = `0x53`).
   - Define raw `Packet` structures conforming to the Wyze Sense protocol specifications (including prefix magic, payload length, command ID, and 16-bit sum checksum verification).
   - Write code to calculate and validate checksums.

3. **Telemetry Types (`src/protocol/telemetry.rs`):**
   - Define `TelemetryData` enum variants:
     - `Heartbeat` — battery, rssi, die temperature (°C from `AON_BATMON:TEMP`), event sequence counter
     - `Alarm` — battery, rssi, state, die temperature, event sequence
     - `AlarmData` — rssi, 12-byte ring buffer of per-slot alarm event counts (0xAB, Motion V1 only)
     - `Climate` — battery, rssi, temperature (°C), humidity (%)
     - `Leak` — battery, rssi, state, probe state, probe available
     - `Scanned` — firmware version
     - `Offline` — synthetic event for availability timeout
     - `UnknownEvent` — raw bytes for unrecognized event types
   - Write decoder functions mapping byte array slices into specific telemetry variants.
   - Battery byte is voltage-proportional (`raw / 32.0` V), not a percentage.
     Per-chemistry discharge curves in `src/protocol/battery.rs` convert to capacity %.
   - RSSI byte is dongle-appended (not part of the sensor's RF payload).
   - Known but rare event types: `0xD1` (Extended Data), `0xE1` (Extended Event), `0xE3` (Extended Status).

4. **Unit Tests:**
   - Write tests using hardcoded hex byte arrays of known events (e.g. Contact Sensor Open, Motion Sensor Active, Climate data) to assert correct structural parsing.

---

## Phase 2: Transport Layer Abstraction
**Goal:** Implement the `AsyncTransport` trait and build the replay mechanism for testing.

1. **Define `AsyncTransport` Trait (`src/transport/mod.rs`):**
   - Declare the trait with `async fn read` and `async fn write` operations.

2. **Implement `ReplayTransport` (`src/transport/replay.rs`):**
   - Build a transport backed by queues of pre-configured `Vec<u8>` chunks.
   - Simulates responses from the dongle when it receives host-initiated write commands.

3. **Implement `HidrawTransport` (`src/transport/hidraw.rs`):**
   - Wrap raw Unix file access `/dev/hidrawX` with async IO (`tokio::fs::File` or equivalent safe platform bindings).

4. **Tests:**
   - Assert that writing data to a `ReplayTransport` lets us read back simulated response blocks accurately.

---

## Phase 3: Handshake Actor & Coordinating Engine
**Goal:** Construct the async runner loop, implement the handshake unlock sequence, and run simulated integration tests.

1. **The Engine Struct (`src/engine/mod.rs`):**
   - Define `Engine` owning an `AsyncTransport` trait object.
   - Set up an event queue utilizing Tokio channels (`tokio::sync::mpsc::Sender`).

2. **Handshake Implementation:**
   - Write logic for the synchronous startup sequence:
     1. Send `Inquiry` -> Wait for Inquiry ACK.
     2. Send `GetEnr` -> Read 16-byte token.
     3. Send `GetMac` -> Read MAC.
     4. Send `GetVersion` -> Read version string.
     5. Send `FinishAuth` -> Finish authentication.

3. **Packet Reader Loop:**
   - Implement an async loop polling the transport for incoming magic headers (`0x55AA` / `0xAA55`), parsing complete packets, and handling asynchronous ACKs.
   - Route incoming notifications:
     - Respond to time synchronization request packets (`NOTIFY_SYNC_TIME`).
     - Forward telemetry packets into the event channel.

4. **Integration Replay Tests:**
   - Write an integration test initializing the `Engine` with a `ReplayTransport` populated with a real handshake log, verifying that `Engine` completes its handshake successfully and reads the correct MAC/Version.

---

## Phase 4: Configuration & Safe Persistence
**Goal:** Set up parser logic for configuration files and build robust state saving mechanisms resistant to power failures.

1. **Config Parsing (`src/config/sensors.rs`):**
   - Read and parse `config/sensors.yaml` mapping MAC keys to names, classes, and inversion flags.

2. **Atomic State Writer (`src/config/state.rs`):**
   - Keep runtime states (last seen, online/offline) in memory.
   - Implement a background task persisting state to `config/state.json`.
   - **Atomic Protocol**:
     1. Serialize current state into `config/state.json.tmp`.
     2. Flush bytes to disk safely.
     3. Execute an atomic rename over `config/state.json` (`std::fs::rename`).

3. **Availability Monitor:**
   - Run a periodic interval timer in the engine checking if any device's `last_seen` delta exceeds its configured timeout, emitting an offline state if it does.

---

## Phase 5: MQTT Gateway Adapter
**Goal:** Implement the external communication gateway, mapping telemetry events to MQTT states and Auto-Discovery templates.

1. **Broker Connection Loop (`src/gateway/mqtt.rs`):**
   - Orchestrate connection using `rumqttc`.
   - Handle reconnections and service status transitions (`online` / `offline`).

2. **Auto-Discovery Publisher:**
   - Construct Home Assistant binary sensor discovery configs matching the Rust telemetry enums.

3. **Telemetry Publisher:**
   - Consume `DongleEvent` structs from the core channel and publish JSON strings containing states, RSSI, and battery.

4. **Control Callback Router:**
   - Subscribe to `self_topic_root/scan`, `/remove`, and `/reload`.
   - Parse commands and execute them on the `Engine` (e.g. calling `engine.enable_scan()`, `engine.delete_sensor()`).

---

## Phase 6: E2E Validation & Diagnostics
**Goal:** Perform complete end-to-end mock simulations, finalize diagnostic captures, and compile the binary.

1. **Full Mock Integration Test:**
   - Script a complete E2E integration test simulating a full session:
     - Handshake -> Pair a mock contact sensor -> Trigger Open -> Trigger Heartbeat -> Trigger Timeout -> Unpair sensor.
     - Assert that all events are correctly parsed and output actions are matched.

2. **Traffic Capture Logger:**
   - If configured, write all incoming raw traffic to a debug log file (`config/diagnostics.bin`) for field diagnostics.

3. **Binary Compilation:**
   - Test building optimization releases: `cargo build --release`.
