# Multi-Dongle Support: Design Document

> **Status:** Fully Implemented (Phase 1–5)  
> **Branch:** `feature/multi-dongle-ws`  
> **Last updated:** 2026-06-12

---

## 1. Motivation

The current architecture is fundamentally single-dongle: one `Engine` owns one
`HidrawTransport` to one USB dongle at `/dev/hidraw0`. This creates limitations:

1. **Physical proximity** — The gateway process must run on the machine with
   the USB dongle physically attached. This forces a Raspberry Pi (or similar)
   deployment topology that may not be desirable.

2. **Coverage area** — A single Wyze Sense dongle has limited RF range (~30m
   indoors). Larger homes or multi-building setups need multiple dongles to
   cover all sensor zones.

3. **Single point of failure** — If the dongle disconnects or the USB bus
   resets, all sensors go offline simultaneously.

### Goal

Support **N dongles** (local USB + remote WebSocket-bridged) feeding into a
single centralized gateway process, sharing one MQTT connection and one web
dashboard.

---

## 2. High-Level Architecture

```
   Remote Machine A               Remote Machine B            Gateway Host
  ┌─────────────────┐           ┌─────────────────┐      ┌──────────────────────────┐
  │ ws-bridge       │           │ ws-bridge       │      │  wyzesense2mqtt-rs       │
  │ /dev/hidraw0 ◄──┤           │ /dev/hidraw0 ◄──┤      │                          │
  │    ▲             │           │    ▲             │      │  ┌────────────────────┐  │
  │    │ USB         │           │    │ USB         │      │  │ Web+WS :8080      │  │
  │    ▼             │           │    ▼             │      │  │  /ws/bridge ◄─────┤  │
  │ WS client ──────┼───────────┼─ WS client ──────┼─────►│  └────┬───────────────┘  │
  └─────────────────┘    WS     └─────────────────┘  WS   │       │                  │
                                                          │       ▼                  │
                       ┌──── /dev/hidraw0 (local) ───────►│  ┌─────────────────────┐ │
                       │                                   │  │  Engines Registry   │ │
                       │                                   │  │  HashMap<MAC, Eng>  │ │
                       │                                   │  └────┬──┬──┬─────────┘ │
                       │                                   │       │  │  │            │
                       │                                   │       ▼  ▼  ▼            │
                       │                                   │  ┌─────────────────────┐ │
                       │                                   │  │ SensorManager (SoT) │ │
                       │                                   │  │ HashMap<MAC,Sensor>  │ │
                       │                                   │  └────────┬────────────┘ │
                       │                                   │           │              │
                       │                                   │     ┌─────┴──────┐       │
                       │                                   │     ▼            ▼       │
                       │                                   │  ┌──────┐  ┌──────────┐  │
                       │                                   │  │ MQTT │  │ Web :8080│  │
                       │                                   │  └──────┘  └──────────┘  │
                       │                                   └──────────────────────────┘
```

---

## 3. Transport Abstraction

### 3.1 Design: `AsyncTransport` Trait + `GatewayTransport` Enum

```rust
/// Core transport abstraction — read/write raw HID frames.
#[async_trait]
pub trait AsyncTransport: Send + Sync + Clone + 'static {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    async fn write(&mut self, buf: &[u8]) -> io::Result<()>;
}
```

A concrete enum dispatches to the appropriate backend:

```rust
#[derive(Clone)]
pub enum GatewayTransport {
    Hidraw(HidrawTransport),
    WebSocket {
        sender: Arc<Mutex<SplitSink<WebSocketStream, Message>>>,
        receiver: Arc<Mutex<SplitStream<WebSocketStream>>>,
    },
    Replay(ReplayTransport),  // for testing
}
```

### 3.2 Why an Enum Instead of `dyn AsyncTransport`?

- **Clone-ability** — Engine clones the transport for its background reader
  task. Trait objects (`Box<dyn ..>`) don't support `Clone` without a custom
  `CloneBox` pattern. An enum is simpler.
- **No heap allocation** — Enum dispatch is zero-cost compared to vtable
  indirection.
- **Exhaustive matching** — New transport backends (e.g., serial)
  require updating the enum, which the compiler enforces.

### 3.3 Decision: Concrete Engine

`Engine` uses a `GatewayTransport` field directly (not generic `Engine<T>`).
The generic approach proved unwieldy — type parameters propagated through
`WebState`, MQTT gateway, and every function that touched an engine. Since
`GatewayTransport` already encapsulates the polymorphism, the generic
parameter adds complexity without benefit.

---

## 4. Remote Bridge Protocol: WebSocket

### 4.1 Why WebSocket Instead of Raw TCP?

Since we already have an Axum web server on `:8080`, we use a **WebSocket
endpoint** instead of a dedicated TCP port:

```
  [Remote USB Dongle] ◄──USB──► [ws-bridge] ◄──WebSocket──► [Gateway :8080/ws/bridge]
```

Benefits over raw TCP:

- **One less port** — reuses the existing web server, simplifying firewall
  rules and Docker port mappings
- **Free message framing** — WebSocket messages are already framed, so the
  TCP stream reassembly problem disappears entirely
- **HTTP upgrade semantics** — the initial handshake is a standard HTTP
  request, enabling future auth via headers/cookies
- **Reverse proxy friendly** — works through nginx, Caddy, Cloudflare
  Tunnels, etc.
- **Built-in ping/pong** — WebSocket keepalive is handled at the protocol
  level, simplifying disconnect detection

### 4.2 Design: Dumb Bidirectional Byte Pipe

The bridge process (`src/bin/dongle_bridge.rs`) is intentionally a **transparent
relay** — it forwards raw HID report bytes between USB and WebSocket with
zero packet interpretation:

- The gateway's `Engine` code is identical for local and remote dongles
- No protocol versioning needed between bridge and gateway
- The bridge binary is tiny and stateless (~140 lines)

Each HID report (≤64 bytes) is sent as a single WebSocket binary message.
The WebSocket framing guarantees message boundaries are preserved.

### 4.3 Gateway-Side Endpoint

```rust
// In web/server.rs route setup:
.route("/ws/bridge", get(ws_bridge_handler))
```

The handler upgrades HTTP to WebSocket, wraps the split stream into a
`GatewayTransport::WebSocket`, creates a new Engine, runs the handshake,
and registers the dongle in the engines map.

**Transport metadata** is captured during the upgrade:
- `transport_label` — `"local"` or `"bridge"`
- `device_path` — USB device path (e.g., `/dev/hidraw0`), sent as a query
  parameter by the bridge client
- `remote_addr` — IP:port of the connecting bridge, extracted via Axum's
  `ConnectInfo`

### 4.4 Reconnection Strategy (Bridge Side)

```
ws-bridge:
  loop {
    open USB dongle (retry every 5s on failure)
    connect to ws://gateway:8080/ws/bridge (retry every 5s on failure)
    spawn bidirectional pipe tasks (USB↔WebSocket)
    select! { either task fails } → close both, retry from top (2s backoff)
  }
```

---

## 5. Sensor Management: Single Source of Truth

### 5.1 Architecture

`SensorManager` is the **single source of truth** for all sensors. It holds
a `HashMap<String, WyzeSensor>` with every known sensor, regardless of which
dongle (if any) owns it.

```rust
pub struct WyzeSensor {
    pub mac: String,
    pub sensor_type: SensorType,
    pub dongle_mac: Option<String>,  // Which dongle owns this sensor (None = unassociated)
    // ... telemetry fields ...
}
```

### 5.2 Lifecycle

1. **Startup** — `load_all_from_state()` loads ALL sensors from `state.yaml`
   into the manager, including their persisted `dongle_mac` affinity.

2. **Dongle connects** — When a dongle reports its NVRAM sensor list,
   `assign_dongle(dongle_mac, nvram_macs)` sets `dongle_mac` on those sensors.
   New sensors (in NVRAM but not in state) are created with defaults.

3. **Pairing** — When the engine auto-verifies a scanned sensor into NVRAM,
   it emits a synthetic `TelemetryData::Paired { version }` event with
   `dongle_mac` set. `SensorManager` auto-discovers and associates the sensor.

4. **Live events** — Telemetry updates flow through the central event bus and
   update sensor state in the manager. Each `DongleEvent` carries
   `dongle_mac: Option<String>` for per-dongle tracking.

5. **Dongle disconnects** — `Engine::disconnect_notify` fires immediately
   on transport read error. The gateway removes the engine from `EnginesMap`
   and calls `unassign_dongle(dongle_mac)`, clearing the `dongle_mac` field
   on all sensors owned by that dongle.

6. **Persistence** — `save_state_to_disk()` writes the entire sensor list
   (including `dongle_mac`) to `state.yaml`.

### 5.3 State File Format (`state.yaml`)

```yaml
sensors:
  77A8C793:
    mac: 77A8C793
    sensor_type: motion
    last_seen: 1781245974
    battery: 100
    signal: -60
    state:
      kind: Motion
      is_active: false
    dongle_mac: 77A85A36   # Optional — omitted if unassociated
```

The `dongle_mac` field uses `#[serde(default, skip_serializing_if = "Option::is_none")]`
for backward compatibility with older state files that lack this field.

### 5.4 API Grouping

`GET /api/dongles` groups sensors from the SensorManager by their `dongle_mac`
field:

```json
{
  "dongles": [
    {
      "mac": "77A85A36",
      "transport": "bridge",
      "device_path": "/dev/hidraw0",
      "remote_addr": "192.168.1.50:60830",
      "sensors": [ /* sensors with dongle_mac == "77A85A36" */ ],
      "sensor_count": 2
    }
  ],
  "unassociated_sensors": [ /* sensors with dongle_mac == null */ ]
}
```

---

## 6. Engines Registry

### 6.1 Data Structure

```rust
type EnginesMap = Arc<tokio::sync::Mutex<HashMap<String, Engine>>>;
```

Keyed by **dongle MAC address** (obtained during handshake). This means:
- Each engine is uniquely identified by the dongle it manages
- Web API can target specific dongles by MAC
- Disconnect cleanup is straightforward: remove the MAC entry

### 6.2 Engine Metadata

Each `Engine` carries transport and lifecycle metadata:

```rust
pub struct Engine {
    // ... core fields ...
    pub transport_label: String,        // "local" or "bridge"
    pub device_path: String,            // e.g., "/dev/hidraw0"
    pub remote_addr: String,            // e.g., "192.168.1.50:60830"
    pub disconnect_notify: Arc<Notify>, // Fires on transport read error
    pub shared_dongle_mac: Arc<Mutex<Option<String>>>,  // Set during handshake
}
```

The `disconnect_notify` is fired immediately when the transport's read loop
encounters an error (USB disconnect or WebSocket close). Gateway watchers
listen on this notify to remove the engine from the map and call
`unassign_dongle()`.

---

## 7. Configuration

### 7.1 Config Schema

```yaml
usb:
  dongle: "auto"         # Local USB dongle path.
                         # "auto" = auto-detect /dev/hidraw*
                         # "none" = disable local USB (WebSocket-only mode)
                         # "/dev/hidraw0" = explicit path

bridge:
  enabled: false         # Enable WebSocket bridge endpoint at /ws/bridge
```

### 7.2 Deployment Modes

| Mode | `usb.dongle` | `bridge.enabled` | Use case |
|------|-------------|-------------------|----------|
| **Local only** | `"auto"` | `false` | Single dongle on same machine (default) |
| **Local + remote** | `"auto"` | `true` | Mixed: local dongle + remote bridges |
| **Remote only** | `"none"` | `true` | Gateway runs on a server, all dongles are remote |

---

## 8. Web Dashboard

### 8.1 Dongle-Centric Layout

The dashboard uses a **dongle-focused** design where each connected dongle
appears as a card with:

- Dongle MAC, firmware version, transport type (local/bridge), connection info
- Collapsible sensor table showing all sensors assigned to that dongle
- An **⚙️ Actions** button that opens a per-dongle modal dialog

An **Unassociated Sensors** card (dashed border) shows sensors restored from
`state.yaml` whose dongle is not currently online.

### 8.2 Actions Modal

Clicking **⚙️ Actions** on a dongle opens a modal dialog with:

| Section | Description |
|---------|-------------|
| **📡 Pairing Center** | Start/stop sensor scan (60s auto-timeout) |
| **🧹 Maintenance** | Purge ghost sensors from dongle NVRAM |
| **💻 Hex Console** | Send/receive raw HID packets for debugging |

Only one modal can be open at a time, preventing concurrent scan conflicts
across dongles. The scan auto-stops when the modal is closed.

---

## 9. API Reference

### 9.1 Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/dongles` | List all dongles with grouped sensors |
| `GET` | `/ws/bridge` | WebSocket upgrade — bridge connection |
| `POST` | `/api/scan` | Start/stop scan (`dongle_mac` required) |
| `DELETE` | `/api/sensors/:mac` | Unpair sensor from its dongle |
| `POST` | `/api/raw` | Send raw HID bytes to a dongle |
| `POST` | `/api/fix` | Purge ghost sensors |
| `GET` | `/api/events` | SSE stream for live updates |

### 9.2 Exclusive Scan Mode

**Constraint: Only one dongle may be in scan mode at any given time.**

The scan command **requires** a `dongle_mac` target. If a scan request
arrives while another dongle is already scanning, it is rejected.

---

## 10. Implementation Status

### Phase 1: Transport Abstraction ✅
- [x] `AsyncTransport` trait
- [x] `GatewayTransport` enum (Hidraw, WebSocket, Replay)
- [x] `Engine` uses `GatewayTransport` (concrete, not generic)
- [x] `ReplayTransport` with `tokio::sync::Notify`

### Phase 2: Multi-Engine Registry ✅
- [x] `EnginesMap` type alias
- [x] `main.rs` startup: USB engine creation → registry insertion
- [x] Central event bus: fan-in from all engines
- [x] `SensorManager` as single source of truth with `dongle_mac` per sensor
- [x] `load_all_from_state()` — loads entire state at startup
- [x] `assign_dongle()` / `unassign_dongle()` — dongle ownership
- [x] `save_state_to_disk()` persists `dongle_mac` affinity

### Phase 3: WebSocket Bridge ✅
- [x] `bridge.enabled` config option
- [x] `/ws/bridge` WebSocket upgrade handler with transport metadata
- [x] `src/bin/dongle_bridge.rs` standalone bridge binary
- [x] Reconnection logic with retry
- [x] `usb.dongle: "none"` gateway-only mode

### Phase 4: Dashboard UX ✅
- [x] Dongle-centric layout with per-dongle sensor grouping
- [x] Unassociated sensors section for orphaned sensors
- [x] Actions modal (Pair, Purge, Hex Console)
- [x] Transport metadata display (local/bridge, device path, remote IP)
- [x] `GET /api/dongles` REST endpoint
- [x] Consistent header/content widths

### Phase 5: Operational Robustness ✅
- [x] Dongle as HA device (MQTT discovery for dongle entities)
- [x] Per-dongle MQTT control topics
- [x] Multi-local USB auto-detection (`usb.dongle: "auto"` discovers all devices)
- [x] WebSocket auth for bridge connections (`bridge.auth_token` config)
- [x] Dongle disconnect detection + auto-cleanup (via `disconnect_notify`)

---

## 11. Resolved Questions

| # | Question | Decision | Rationale |
|---|----------|----------|-----------|
| 1 | Generic `Engine<T>` vs concrete? | **Concrete** with `GatewayTransport` enum | Generics propagated everywhere; enum is simpler |
| 2 | Transport protocol? | **WebSocket** over existing :8080 | One less port, free framing, proxy-friendly |
| 3 | CLI mode with multi-dongle? | **Daemon-only** (always via HTTP) | Direct HID is a single-dongle escape hatch |
| 4 | Sensor-to-dongle affinity? | **Track by dongle MAC** in SensorManager | Transport-agnostic; dongle MAC is stable identity |
| 5 | Multiple local USB? | **Supported by design**, not prioritized | Falls out naturally from engines registry |
| 6 | Single vs. per-engine sensor cache? | **Single** — SensorManager is the only source | Engine caches caused ghosting and orphan confusion |
| 7 | WebSocket auth for bridge connections? | **Token via query param** (`?token=`) | Simple, works with WS clients, config via `bridge.auth_token` |
| 8 | Dongle disconnect semantics? | **Reset to None** via `unassign_dongle()` | Sensors become "unassociated" until dongle reconnects. `disconnect_notify` fires immediately on transport error. |
| 9 | Multi-local USB path format? | **Single string `"auto"`** discovers ALL dongles | `discover_all_dongle_devices()` scans sysfs for all matches |

---

## 12. Testing Infrastructure

### 12.1 Virtual Dongle

The `VirtualDongle` (`src/transport/virtual_dongle.rs`) provides an in-process
protocol-level mock that automatically responds to handshake, scan, verify,
delete, and sensor list commands. It wraps `ReplayTransport` and supports:

- Pre-paired sensor lists via `with_sensors()`
- Event injection: `inject_scan_event()`, `inject_alarm_event()`, `inject_leak_event()`, `inject_climate_event()`, `inject_heartbeat_event()`
- Transport disconnect simulation via `disconnect()` (poison flag → BrokenPipe)

### 12.2 Test Harness

The `TestHarness` (`tests/test_harness/mod.rs`) boots the full system stack
(minus MQTT) for E2E testing:

- Creates engines from `VirtualDongle`s
- Wires up `SensorManager`, `EnginesMap`, and Axum web server on a random port
- Provides HTTP convenience methods (`get_dongles()`, `set_scan()`, `verify_sensor()`, etc.)
- Supports disconnect testing via `await_dongle_disconnect()`

### 12.3 E2E Test Suite

`tests/full_e2e_test.rs` contains **14 comprehensive tests** covering:

| Tests 1–8 | Core sensor lifecycle, multi-dongle isolation, scan API, raw packets, alarm/climate/leak events |
|-----------|---|
| Tests 9–14 | Auto-pairing, dongle disconnect cleanup, sensor re-pairing across dongles, leak state transitions, battery level propagation, three-dongle full lifecycle |

### 12.3 Standalone Virtual Dongle Binary

The `virtual_dongle` binary (`src/bin/virtual_dongle.rs`) provides a standalone
process that simulates a USB dongle over WebSocket. Configure with YAML:

```yaml
gateway: "ws://localhost:8080/ws/bridge"
mac: "LIVROOM1"
version: "V2.3.9"
sensors:
  - mac: FRONTDOR
    sensor_type: contact
    battery: 95
    rssi: 55
    state: closed
```

Run: `./virtual_dongle --config dongle.yaml`

Interactive CLI commands: `alarm`, `leak`, `climate`, `heartbeat`, `scan`, `pair`.
