# Multi-Dongle Support: Design Document

> **Status:** Draft  
> **Branch:** `feature/multi-dongle-tcp` (to be re-implemented on current `main`)  
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
                       │                                   │  │ Central Event Bus   │ │
                       │                                   │  │ mpsc<DongleEvent>   │ │
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

### 3.1 The Problem

`Engine` currently takes a concrete `HidrawTransport`. To support TCP-bridged
dongles, the Engine must be generic over the transport layer.

### 3.2 Design: `AsyncTransport` Trait + `GatewayTransport` Enum

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

### 3.3 Why an Enum Instead of `dyn AsyncTransport`?

- **Clone-ability** — Engine clones the transport for its background reader
  task. Trait objects (`Box<dyn ...>`) don't support `Clone` without a custom
  `CloneBox` pattern. An enum is simpler.
- **No heap allocation** — Enum dispatch is zero-cost compared to vtable
  indirection.
- **Exhaustive matching** — New transport backends (e.g., serial)
  require updating the enum, which the compiler enforces.

### 3.4 Open Question: Should Engine Be Generic or Concrete?

Two approaches:

| Approach | Pros | Cons |
|----------|------|------|
| `Engine<T: AsyncTransport>` | Clean generics, zero-cost dispatch | Monomorphization bloat, complex type signatures everywhere |
| `Engine` with `GatewayTransport` field | Simple, one concrete type | Slightly less "pure" |

**Leaning toward:** concrete `Engine` with `GatewayTransport` field. The
generic approach proved unwieldy in the first attempt — type parameters
propagated through `WebState`, MQTT gateway, and every function that touched
an engine. Since `GatewayTransport` already encapsulates the polymorphism, the
generic parameter adds complexity without benefit.

---

## 4. Remote Bridge Protocol: WebSocket

### 4.1 Why WebSocket Instead of Raw TCP?

The previous iteration used a dedicated TCP listener on a separate port
(8095). Since we already have an Axum web server on `:8080`, we can use a
**WebSocket endpoint** instead:

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

Axum has first-class WebSocket support via `axum::extract::ws`, so the server
side is ~20 lines of code.

### 4.2 Design: Dumb Bidirectional Byte Pipe

The bridge process is intentionally a **transparent relay** — it forwards raw
HID report bytes between USB and WebSocket with zero packet interpretation:

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

The handler upgrades the HTTP connection to WebSocket, wraps the split
stream into a `GatewayTransport::WebSocket`, creates a new Engine, runs
the handshake, and registers the dongle in the engines map.

### 4.4 Reconnection Strategy

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

## 5. Engines Registry

### 5.1 Data Structure

```rust
type EnginesMap = Arc<tokio::sync::Mutex<HashMap<String, Engine>>>;
```

Keyed by **dongle MAC address** (obtained during handshake). This means:
- Each engine is uniquely identified by the dongle it manages
- Web API can target specific dongles by MAC
- Disconnect cleanup is straightforward: remove the MAC entry

### 5.2 Lifecycle

```
  1. Transport connected (USB open or TCP accept)
  2. Engine created with shared event_tx channel
  3. Engine.start() spawns background reader loop
  4. Handshake executed (8s timeout)
  5. On success: engine registered in HashMap[dongle_mac]
  6. On reader EOF/error: engine emits disconnect event → removed from map
```

### 5.3 Open Question: Dongle Identity Before Handshake

The dongle's MAC is only known after the handshake completes (step 3 of 5:
GET_MAC). During handshake, the engine doesn't have a registry key yet. This
means:

- If handshake fails, there's nothing to clean up in the map (good)
- But the engine's reader loop is already running (started in step 3) and
  consuming the shared `event_tx` channel
- A failed handshake should cleanly stop the reader loop

**Need to verify:** Does the current Engine cleanly shut down its reader loop
when the handshake times out?

---

## 6. Configuration

### 6.1 Config Schema

```yaml
usb:
  dongles: ["auto"]       # List of local USB dongle paths.
                          # "auto" = auto-detect /dev/hidraw*
                          # "none" or [] = disable local USB (WebSocket-only mode)
                          # ["/dev/hidraw0", "/dev/hidraw1"] = explicit multi-dongle

bridge:
  enabled: false          # Enable WebSocket bridge endpoint at /ws/bridge
                          # (no extra port needed — uses the web server port)
```

### 6.2 Deployment Modes

| Mode | `usb.dongles` | `bridge.enabled` | Use case |
|------|--------------|-------------------|----------|
| **Local only** | `["auto"]` | `false` | Single dongle on same machine (current default) |
| **Multi-local** | `["/dev/hidraw0", "/dev/hidraw1"]` | `false` | Multiple USB dongles on same machine |
| **Local + remote** | `["auto"]` | `true` | Mixed: local dongle + remote bridges |
| **Remote only** | `[]` | `true` | Gateway runs on a server, all dongles are remote |

### 6.3 Multiple Local USB Dongles

Supporting multiple local USB dongles falls out naturally from the engines
registry design. When `usb.dongles` is a list, the startup code iterates
each path, opens a `HidrawTransport`, creates an Engine, runs the handshake,
and registers it in the map. Each dongle gets its own MAC-keyed entry.

`"auto"` mode scans `/dev/hidraw*` for Wyze Sense dongles (using the USB
VID/PID or the handshake inquiry response to identify valid dongles).

> **Priority:** Supported by design, not actively prioritized. The common
> use case is 1 local + N remote. Multi-local is a bonus.

### 6.4 CLI Mode with Multi-Dongle

The current CLI fallback opens `/dev/hidraw0` directly when the daemon isn't
running. With multi-dongle, the CLI should **always talk to the daemon via
HTTP** (require daemon to be running). Direct HID access is a legacy escape
hatch for single-dongle mode only.

---

## 7. API Changes

### 7.1 Web REST API

New endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/dongles` | List all connected dongles (MAC, version, scan status) |
| `GET` | `/ws/bridge` | WebSocket upgrade — bridge connection endpoint |

Modified endpoints — add optional `dongle_mac` parameter:

| Method | Path | Change |
|--------|------|--------|
| `POST` | `/api/scan` | **Requires** `dongle_mac` — only one dongle may scan at a time (see §7.5) |
| `DELETE` | `/api/sensors/:mac` | Routes to owning dongle via affinity |
| `POST` | `/api/verify` | Requires `dongle_mac` (must target specific dongle) |
| `POST` | `/api/raw` | Requires `dongle_mac` (must target specific dongle) |

### 7.2 Dongles as Home Assistant Devices

Each dongle should appear as its own **device** in Home Assistant via MQTT
auto-discovery. This gives users operational visibility and control in the
HA dashboard.

**Discovery entities per dongle:**

| Entity | Type | Description |
|--------|------|-------------|
| Dongle Status | `binary_sensor` (connectivity) | Online/offline availability |
| Firmware Version | `sensor` (diagnostic) | Dongle firmware version string |
| Connected Sensors | `sensor` (diagnostic) | Count of sensors in NVRAM |
| Scan Mode | `switch` | Toggle scan mode on/off |
| Disconnect | `button` | Gracefully remove dongle from registry |

**MQTT topic layout:**

```
wyzesense2mqtt/dongle/{dongle_mac}/status     → "online" / "offline"
wyzesense2mqtt/dongle/{dongle_mac}/state      → JSON state payload
wyzesense2mqtt/dongle/{dongle_mac}/scan/set   → "ON" / "OFF" (command)
wyzesense2mqtt/dongle/{dongle_mac}/disconnect → trigger (command)
homeassistant/*/wyzesense_dongle_{mac}/*/config → discovery configs
```

The "Disconnect" button makes sense for planned maintenance scenarios:
physically moving a dongle to a different location, or replacing a failing
dongle. It gracefully removes the dongle from the registry and marks its
sensors as unavailable, without crashing the whole system.

### 7.3 Per-Dongle MQTT Control Topics

Per-dongle control topics (scan requires targeting a specific dongle):

```
wyzesense2mqtt/dongle/{dongle_mac}/scan     → target specific dongle
wyzesense2mqtt/dongle/{dongle_mac}/remove   → target specific dongle
```

Legacy broadcast topics (scan broadcast is NOT supported):

```
wyzesense2mqtt/remove   → all dongles
```

### 7.4 Exclusive Scan Mode

**Constraint: Only one dongle may be in scan mode at any given time.**

Allowing multiple dongles to scan simultaneously creates problems:

- A newly powered-on sensor broadcasts its pairing advertisement to all
  dongles in range. If two dongles are scanning, both would discover the
  sensor, leading to a race condition during `verify_sensor`.
- The user has no control over which dongle "wins" the pairing.
- Duplicate discovery events in the UI create confusion.

**Enforcement:** The scan command **requires** a `dongle_mac` target. The
engines registry tracks which dongle (if any) is currently scanning. If a
scan request arrives while another dongle is already scanning, the system
should either:

1. **Reject** the request with an error ("dongle X is already scanning"), or
2. **Transfer** — automatically stop the current scanner and start the new one.

> **Leaning toward:** Option 1 (reject) for safety. The UI can show which
> dongle is currently scanning and offer a "stop" button.

### 7.5 Sensor-to-Dongle Affinity

**Decision: Track affinity by dongle MAC.**

SensorManager records `dongle_mac: Option<String>` per sensor. The affinity
is keyed on the **dongle MAC**, NOT the transport type. This means:

- A dongle connected via HidrawTransport and later reconnected via WebSocket
  has the same MAC — its sensors seamlessly re-associate.
- The transport is just plumbing; the dongle identity is what matters.
- Commands (delete, verify) are automatically routed to the owning dongle.

**Edge cases:**

- **Sensor in range of multiple dongles:** Each dongle has its own NVRAM
  sensor list. A sensor is only paired to one dongle. Events from that
  sensor will only arrive via the owning dongle.
- **Dongle offline:** Sensors affiliated with that dongle are marked
  unavailable. If the dongle reconnects (same MAC, any transport), the
  sensors come back online automatically.
- **Re-pairing:** If a sensor is unpaired from dongle A and re-paired to
  dongle B, the affinity updates to dongle B's MAC.

---

## 8. Implementation Plan

### Phase 1: Transport Abstraction (foundation)
- [ ] Define `AsyncTransport` trait in `src/transport/mod.rs`
- [ ] Define `GatewayTransport` enum (Hidraw, WebSocket, Replay)
- [ ] Make `Engine` use `GatewayTransport` (concrete, not generic)
- [ ] Fix `ReplayTransport` with `tokio::sync::Notify`
- [ ] Update all tests to use `GatewayTransport::Replay(..)`

### Phase 2: Multi-Engine Registry
- [ ] Introduce `EnginesMap` type alias
- [ ] Refactor `main.rs` startup: USB engine creation → registry insertion
- [ ] Central event bus: fan-in from all engines
- [ ] Event router: disconnect detection + MQTT forwarding
- [ ] Update web server to accept `EnginesMap`
- [ ] Update all web handlers for multi-engine iteration
- [ ] Add `dongle_mac: Option<String>` to SensorManager per sensor

### Phase 3: WebSocket Bridge
- [ ] Add `bridge.enabled` to `app_config.rs`
- [ ] Implement `/ws/bridge` WebSocket upgrade handler in web server
- [ ] Create `src/bin/ws_bridge.rs` standalone binary
- [ ] Reconnection logic with backoff

### Phase 4: Dongle as HA Device
- [ ] MQTT discovery for each dongle (connectivity, firmware, sensor count)
- [ ] Scan mode switch entity
- [ ] Disconnect button entity
- [ ] `GET /api/dongles` REST endpoint

### Phase 5: UX Polish
- [ ] Per-dongle MQTT control topics
- [ ] Web dashboard: dongle panel with status + controls
- [ ] Multi-local USB auto-detection (`usb.dongles: ["auto"]`)
- [ ] Update documentation

---

## 9. Resolved & Open Questions

### Resolved

| # | Question | Decision | Rationale |
|---|----------|----------|-----------|
| 1 | Generic `Engine<T>` vs concrete? | **Concrete** with `GatewayTransport` enum | Generics propagated everywhere; enum is simpler |
| 2 | Transport protocol? | **WebSocket** over existing :8080 | One less port, free framing, proxy-friendly |
| 3 | CLI mode with multi-dongle? | **Daemon-only** (always via HTTP) | Direct HID is a single-dongle escape hatch |
| 4 | Sensor-to-dongle affinity? | **Track by dongle MAC** | Transport-agnostic; dongle MAC is stable identity |
| 5 | Multiple local USB? | **Supported by design**, not prioritized | Falls out naturally from engines registry |

### Open

| # | Question | Notes |
|---|----------|-------|
| 6 | Clean shutdown on handshake timeout? | Verify engine reader loop stops cleanly |
| 7 | WebSocket auth for bridge connections? | Headers/tokens? Or trust-on-connect? |
| 8 | Dongle disconnect button semantics? | Graceful removal vs. full unpair of all sensors? |
