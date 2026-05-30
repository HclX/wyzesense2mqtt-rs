# Wyze Sense USB Dongle: Low-Level Serial Protocol & Packet Specification

This document serves as the definitive, low-level serial protocol specification for the **Wyze Sense USB HID Dongle (WHSB1 / CH554 bridge)**.

All data structures, byte layouts, and packet frames documented below are verified against the Python reference implementation ([wyzesense.py](scratch/wyzesense2mqtt/wyzesense2mqtt/wyzesense.py)) and corroborated with live production hex captures from the sub-GHz RF dongle interface.

---

## 1. HID Framing Layer

The host communicates with the USB dongle via **raw USB HID** read/write calls on a Linux `/dev/hidrawN` character device.

- **HID Interface Paths**: `/dev/hidraw0` (Linux), auto-discoverable via sysfs `1a86:e024`.
- **HID Read Size**: Each `read()` returns exactly one HID report frame (up to `0x40` / 64 bytes).
- **HID Write**: Raw protocol bytes are written directly; no HID framing is required on write.

### 1.1 HID Read Frame Format

The raw bytes returned from a single `read()` on the hidraw device are structured as:

```
+------------------+--------------------------------------------------+
| Length (1 Byte)  |    Protocol Data (up to 63 Bytes)                 |
+------------------+--------------------------------------------------+
 0                  1                                             Length
```

| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| **0** | 1 | **Length** | Number of valid protocol data bytes that follow. Max `0x3F` (63). |
| **1** | `Length` | **Protocol Data** | The actual protocol bytes. Bytes beyond `1 + Length` are **stale/garbage** and MUST be discarded. |

> **CRITICAL**: The first byte of each HID read is a **length byte**, NOT a HID Report ID. Only `data[1..1+length]` contains valid protocol data. The remaining bytes in the 64-byte HID frame are leftover from the dongle's internal ring buffer and will cause checksum failures if appended to the reassembly buffer.

The Python reference implementation handles this correctly:

```python
def _ReadRawHID(self):
    s = os.read(self.__fd, 0x40)       # Read one HID report (up to 64 bytes)
    length = s[0]                       # First byte = length of valid data
    if length > 0x3F:
        length = 0x3F                   # Clamp to max
    return s[1: 1 + length]             # Return ONLY the valid protocol bytes
```

Multiple valid protocol packets may be concatenated within a single HID frame when `length` is large (e.g., `0x3E` = 62 bytes worth of data). The receiver must maintain a reassembly buffer and parse packets from it using the protocol framing described in Section 2.

---

## 2. Protocol Packet Framing & Serialization

- **Endianness**: Big-Endian (network byte order) for all multi-byte integers.
- **Reassembly**: Protocol packets may span multiple HID frames. The receiver scans for the `0x55AA` or `0xAA55` magic prefix in the reassembly buffer to find packet boundaries.

Every protocol packet conforms to the following byte-level sequence:

```
+------------------+------------+--------------+------------+--------------------+------------------+
| Magic (2 Bytes)  | Type (1B)  |    b2 (1B)   | Cmd ID (1B)| Payload (Variable) | Checksum (2B)    |
+------------------+------------+--------------+------------+--------------------+------------------+
 0                  2            3              4            5                    N-2                N
```

### 2.1 Packet Fields

| Offset | Size (Bytes) | Field Name | Description |
| :--- | :--- | :--- | :--- |
| **0** | 2 | **Magic Prefix** | Always `0xAA55` when writing to the dongle. Either `0xAA55` or `0x55AA` when reading. |
| **2** | 1 | **Command Type** | `0x43` (Sync — Host-initiated request-reply) or `0x53` (Async — Dongle-initiated events/notifications). |
| **3** | 1 | **b2 (Length/ACK)** | For normal packets: `len(payload) + 3`. For ACK packets (`cmd_id=0xFF`): the lower byte of the acknowledged command. |
| **4** | 1 | **Command ID** | The specific command or event identifier code. |
| **5** | Variable | **Payload** | Bytes specific to the command (size = `b2 - 3`). |
| **N-2** | 2 | **Checksum** | `sum(bytes[0..N-2]) & 0xFFFF` — modulo 16-bit sum of all preceding bytes. |

### 2.2 Total Packet Length

- **ACK packets** (`cmd=0x53FF`): Always exactly **7 bytes**.
- **Normal packets**: Total length = `b2 + 4` bytes.
- **Empty-payload packets** (e.g. Inquiry): `b2 = 3`, total = 7 bytes.

### 2.3 ACK Packet Special Structure

When the full command word is `0x53FF` (Async ACK):

```
+--------+------+----+----+------+-----------+
| Magic  | 0x53 | b2 | FF | Checksum (2B)    |
+--------+------+----+----+-----------+------+
```

- `b2` holds the **lower byte** of the command being acknowledged.
- The acknowledged command is reconstructed as: `(0x53 << 8) | b2`.
- Payload is conceptually `ACK(0x53XX)` where `XX = b2`.

---

## 3. Command & Response Convention

- **Request-Reply**: The host sends command `CMD_X`, and expects the response on `CMD_X + 1`.
  - Example: Inquiry `0x4327` → response `0x4328`.
- **ACK Protocol**: For **every** received Async packet (Type=`0x53`) **except** `ASYNC_ACK` (`0x53FF`) itself, the host **must** send an `ASYNC_ACK` packet acknowledging the received command.
  - This applies to unsolicited notifications (alarms, time sync, scan events, event logs, sensor list items) AND responses to host-initiated async commands (version, auth, scan toggle, etc.).

---

## 4. Complete Command Reference

### 4.1 Sync Commands (Type `0x43` — Host Initiated)

| Command | Code | Payload (Write) | Response Code | Response Payload | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Inquiry** | `0x4327` | None | `0x4328` | `[0x01]` on success | Checks dongle readiness. |
| **Get ENR** | `0x4302` | 16 bytes (random token) | `0x4303` | 16-byte ENR token | Retrieves crypto token. Standard token: `[0x30]*16`. |
| **Get MAC** | `0x4304` | None | `0x4305` | 8-byte ASCII MAC | Retrieves dongle MAC address. |
| **Get Key** | `0x4306` | None | `0x4307` | 16-byte key | Retrieves encryption key. |
| **Update CC1310** | `0x4312` | None | `0x4313` | — | Triggers CC1310 firmware update. |
| **CH554 Upgrade** | `0x430E` | None | `0x430F` | — | Triggers CH554 firmware upgrade. |

### 4.2 Async Commands (Type `0x53` — Host → Dongle)

| Command | Code | Payload (Write) | Response Code | Response Payload | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Finish Auth** | `0x5314` | `[0xFF]` | `0x5315` | None (empty) | Finalizes handshake, unlocks RF radio. |
| **Get Version** | `0x5316` | None | `0x5317` | ASCII version string | Retrieves firmware version. |
| **Start/Stop Scan** | `0x531C` | `[0x01]`=start, `[0x00]`=stop | `0x531D` | `[0x01]` on success | Toggles pairing scan mode. |
| **Get Sensor R1** | `0x5321` | 8-byte MAC + 16-byte R1 token | `0x5322` | Variable | Exchanges crypto token with sensor during pairing. |
| **Verify Sensor** | `0x5323` | 8-byte MAC + `[0xFF, 0x04]` | `0x5324` | None (empty) | Permanently binds sensor to NVRAM. |
| **Delete Sensor** | `0x5325` | 8-byte ASCII MAC | `0x5326` | 8-byte MAC + `[0xFF]` | Unpairs a specific sensor. |
| **Get Sensor Count** | `0x532E` | None | `0x532F` | `[count]` (1 byte) | Returns number of paired sensors. |
| **Get Sensor List** | `0x5330` | `[count]` (1 byte) | `0x5331` × N | 8-byte ASCII MAC per response | Streams paired sensor MACs individually. |
| **Delete All Sensors** | `0x533F` | None | `0x5340` | `[0xFF]` on success | Unpairs all sensors. |
| **Play Chime** | `0x5370` | 8-byte MAC + `[ring_id, repeat_cnt, volume]` | `0x5371` | None (empty) | Triggers chime alarm on paired speaker. |

### 4.3 Async Notifications (Type `0x53` — Dongle → Host)

| Notification | Code | Description |
| :--- | :--- | :--- |
| **Sensor Alarm** | `0x5319` | Telemetry event (heartbeat, alarm, climate). See Section 6. |
| **Sensor Scan** | `0x5320` | New sensor detected during scan mode. See Section 5.2. |
| **Time Sync Request** | `0x5332` | Dongle requests current time. Host must reply with `0x5333`. |
| **Event Log** | `0x5335` | Dongle log entry (informational, can be ignored). |
| **Sensor Alarm2** | `0x5355` | Extended alarm event (leak sensor). See Section 6.4. |

---

## 5. Startup Handshake & Sensor Management

### 5.1 Initialization Handshake (5-Step Unlock Sequence)

Before the dongle begins transmitting sub-GHz sensor events, the host **must** perform this 5-step synchronous unlock handshake. If skipped, the RF radio remains inactive.

#### Step 1: Inquiry (`0x4327`)
- **Write**: `AA 55 43 03 27 01 6C`
- **Read**: `55 AA 43 04 28 01 01 6F`
- **Validation**: Response payload must be `[0x01]`.

#### Step 2: Get ENR Token (`0x4302`)
- **Write**: `Cmd=4302, Payload=[30,30,30,30,30,30,30,30,30,30,30,30,30,30,30,30]`
  - The ENR parameter is 4 × 32-bit integers `[0x30303030]*4` packed as **little-endian** (`<LLLL`), which produces 16 bytes of `0x30`.
- **Read**: `Cmd=4303, Payload=[55,ff,eb,67,d8,c5,f8,70,b9,43,b0,21,cc,02,3e,ec]`
- **Validation**: Response payload must be exactly 16 bytes.

#### Step 3: Get MAC Address (`0x4304`)
- **Write**: `AA 55 43 03 04 01 49`
- **Read**: `55 AA 43 0B 05 37 37 41 38 35 41 33 36 03 18`
  - Decoded MAC: `"77A85A36"` (8 ASCII characters)

#### Step 4: Get Dongle Version (`0x5316`)
- **Write**: `AA 55 53 03 16 01 6B`
- **Read (ACK)**: `55 AA 53 16 FF 02 67` — Dongle auto-ACKs the request.
- **Read (Response)**: `Cmd=5317, Payload=[30,2e,30,2e,30,2e,34,37,20,56,31,2e,38,20,47,61,74,65,77,61,79,20,47,57,33,55]`
  - Decoded: `"0.0.0.47 V1.8 Gateway GW3U"`
- **Host ACK**: Must send `ACK(5317)` back.

#### Step 5: Finish Authentication (`0x5314`)
- **Write**: `AA 55 53 04 14 FF 02 69`
- **Read (ACK)**: `55 AA 53 14 FF 02 65` — Dongle auto-ACKs.
- **Read (Response)**: `55 AA 53 03 15 01 6A` — Empty payload confirms auth complete.
- **Host ACK**: Must send `ACK(5315)` back.

### 5.2 Sensor Pairing Flow

The full pairing sequence (from the Python reference) is:

1. **Enable Scan** (`0x531C` with payload `[0x01]`) — puts dongle in discovery mode.
2. **Wait for Scan Notification** (`0x5320`) — dongle broadcasts when a sensor's reset pin is held.
   - Payload structure (11 bytes):
     - `payload[0]` = Event marker (`0xA3`)
     - `payload[1..9]` = 8-byte ASCII Sensor MAC
     - `payload[9]` = Sensor Type byte
     - `payload[10]` = Sensor firmware version
3. **Get Sensor R1** (`0x5321`) — Exchange crypto token with the discovered sensor.
   - Payload: 8-byte MAC + 16-byte R1 token (hardcoded: `b'Ok5HPNQ4lf77u754'`).
   - Timeout: 10 seconds.
4. **Disable Scan** (`0x531C` with payload `[0x00]`).
5. **Verify Sensor** (`0x5323`) — Permanently bind to NVRAM.
   - Payload: 8-byte MAC + `[0xFF, 0x04]`.

### 5.3 Retrieving Paired Sensor List

The sensor list retrieval is a **two-phase protocol**:

1. **Get Count** (`0x532E` → `0x532F`): Returns 1-byte count of paired sensors.
2. **Get List** (`0x5330` with payload `[count]` → multiple `0x5331` responses): The dongle streams `count` individual responses, each containing one 8-byte ASCII MAC address.

> **Important**: The count MUST be sent as payload to `0x5330`. Sending no payload may cause incomplete or unreliable sensor list retrieval.

### 5.4 Time Sync Protocol

When the dongle sends `NOTIFY_SYNC_TIME (0x5332)`, the host must reply with:
- **Command**: `0x5333` (NOTIFY_SYNC_TIME + 1)
- **Payload**: 8-byte big-endian `u64` representing current Unix epoch time in **milliseconds**.

---

## 6. Telemetry Packet Parsing

### 6.1 Alarm1 — `NOTIFY_SENSOR_ALARM` (`0x5319`)

All standard telemetry events arrive via `0x5319`. The payload has an **18-byte Common Header** followed by **Variable Event Data**.

```
+----------------------+-----------------+-----------------------+------------------+------------------------+
| Timestamp (8 Bytes)  | Event Type (1B) | Sensor MAC (8 Bytes)  | Sensor Type (1B) | Event Data (Variable)  |
+----------------------+-----------------+-----------------------+------------------+------------------------+
 0                      8                 9                       17                 18
```

#### 6.1.1 Common Header Fields

| Offset | Size | Name | Description |
| :--- | :--- | :--- | :--- |
| **0..8** | 8 | **Timestamp** | Big-endian `u64` millisecond Unix epoch. Divide by 1000.0 for seconds. |
| **8** | 1 | **Event Type** | `0xA1` Heartbeat, `0xA2` Alarm, `0xE8` Climate. |
| **9..17** | 8 | **Sensor MAC** | 8-byte ASCII MAC string. |
| **17** | 1 | **Sensor Type** | See Sensor Type Table below. |

#### Sensor Type Table

| Code | Name | Binary States |
| :--- | :--- | :--- |
| `0x01` | Contact Sensor V1 (`switch`) | `0x00`=Closed, `0x01`=Open |
| `0x02` | Motion Sensor V1 (`motion`) | `0x00`=Inactive, `0x01`=Active |
| `0x03` | Leak Sensor (`leak`) | `0x00`=Dry, `0x01`=Wet |
| `0x07` | Climate Sensor (`climate`) | N/A (temperature/humidity) |
| `0x0C` | Chime (`chime`) | N/A |
| `0x0E` | Contact Sensor V2 (`switchv2`) | `0x00`=Closed, `0x01`=Open |
| `0x0F` | Motion Sensor V2 (`motionv2`) | `0x00`=Inactive, `0x01`=Active |

### 6.2 Standard Alarm & Heartbeat Event Data (`0xA1` / `0xA2`)

The event data following the 18-byte header is **8 bytes**, unpacked as format `>BBBBBHB`:

```
+--------------+--------------+--------------+--------------+--------------+-------------------+-------------------+
| DataType(1B) | Battery(1B)  | Unknown(1B)  | Unknown(1B)  | State (1B)   | Sequence (2B, BE) | Signal Str. (1B)  |
+--------------+--------------+--------------+--------------+--------------+-------------------+-------------------+
 0              1              2              3              4              5                    7
```

| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| **0** | 1 | **Data Type** | Event subtype marker (e.g., `0x14`, `0x19`). Informational only. |
| **1** | 1 | **Battery** | Battery percentage (0–100). For Contact V2 (`0x0E`): raw value is **doubled** (`min(100, raw * 2)`) because it uses a single 1.5V battery. |
| **2** | 1 | **Unknown** | Reserved/unknown byte. |
| **3** | 1 | **Unknown** | Reserved/unknown byte. |
| **4** | 1 | **State** | Binary sensor state. See Sensor Type Table for state mapping. Only meaningful for Alarm events (`0xA2`). |
| **5..7** | 2 | **Sequence** | Big-endian `u16` packet sequence number. |
| **7** | 1 | **Signal Strength** | Unsigned RSSI value. Decoded dBm = `-raw_value`. |

> **⚠️ Previous Documentation Error**: The state byte is at **offset 4**, NOT offset 2. The Python reference implementation confirms this via `struct.unpack_from(">BBBBBHB", data)` where state is the **5th** unpacked field. Offsets 2 and 3 are unknown/reserved bytes.

**Heartbeat (`0xA1`)**: Uses the same 8-byte structure but the `State` field is not semantically used. Only `Battery` and `Signal Strength` are meaningful.

**Alarm (`0xA2`)**: All fields are meaningful. The `State` field indicates the current binary state of the sensor.

#### Verbatim Alarm Capture Example
```
Payload: [00,00,00,00,00,00,00,00, a2, 37,37,41,38,43,37,39,33, 02, 14,5c,00,01,00,00,12,3c]
         |--- Timestamp ---|  Evt  |---- MAC "77A8C793" ----|  Type
                                                              0x02=MotionV1

Event Data: [14, 5c, 00, 01, 00, 00, 12, 3c]
             DT  Bat  ??   ??  St   Seq---  RSSI
             
  Battery:  0x5C = 92%
  State:    0x00 = Inactive (at offset 4)
  RSSI:     0x3C = 60 → -60 dBm
```

### 6.3 Climate Event Data (`0xE8`)

The event data is **10 bytes**, unpacked as format `>BBBBBBBBBB`:

```
+--------------+--------------+--------------+--------------+--------------+--------------+--------------+-----------+--------------+-----------------+
| DataType(1B) | Battery(1B)  | Unknown(1B)  | Unknown(1B)  | TempHi (1B)  | TempLo (1B)  | Humidity(1B) | Unk.(1B)  | Sequence(1B) | Signal Str.(1B) |
+--------------+--------------+--------------+--------------+--------------+--------------+--------------+-----------+--------------+-----------------+
 0              1              2              3              4              5              6              7           8              9
```

| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| **1** | 1 | Battery | Battery percentage. |
| **4** | 1 | Temp Hi | Temperature integer part (°C). |
| **5** | 1 | Temp Lo | Temperature decimal part. |
| **6** | 1 | Humidity | Humidity percentage. |
| **9** | 1 | Signal Strength | Unsigned RSSI. dBm = `-raw_value`. |

**Temperature Decoding**: `temperature_celsius = temp_hi + (temp_lo / 100.0)`

#### Verbatim Climate Capture Example
```
Alarm1 Payload: [...header..., 18, 5f, 00, 03, 15, 2e, 30, 11, 36, 26]
                               DT  Bat  ??   ??  TH   TL   Hum  ??  Seq  RSSI

  Battery:     0x5F = 95%
  Temperature: 0x15 + (0x2E / 100) = 21 + 0.46 = 21.46°C
  Humidity:    0x30 = 48%
  RSSI:        0x26 = 38 → -38 dBm
```

### 6.4 Alarm2 — `NOTIFY_SENSOR_ALARM2` (`0x5355`)

Alarm2 packets are used for **Leak Sensor** events. They have a different header format — **no timestamp** field. The current system time should be used instead.

#### Alarm2 Header (10 bytes)

```
+-----------------+-----------------------+------------------+
| Event Type (1B) | Sensor MAC (8 Bytes)  | Sensor Type (1B) |
+-----------------+-----------------------+------------------+
 0                 1                       9
```

#### Leak Event Data (follows 10-byte header, 11 bytes)

Unpacked as format `>BBBBBBBBBBB`:

```
+---------+---------+--------------+---------+---------+-----------+---------------+------------------+---------+--------------+-----------------+
| Unk.(1B)| Unk.(1B)| Battery(1B)  | Unk.(1B)| Unk.(1B)| State(1B) | ProbeState(1B)| ProbeAvail.(1B)  | Unk.(1B)| Sequence(1B) | Signal Str.(1B) |
+---------+---------+--------------+---------+---------+-----------+---------------+------------------+---------+--------------+-----------------+
 0         1         2              3         4         5           6               7                  8         9              10
```

| Offset | Size | Field | Description |
| :--- | :--- | :--- | :--- |
| **2** | 1 | Battery | Battery percentage. |
| **5** | 1 | State | `0x00`=Dry, `0x01`=Wet. |
| **6** | 1 | Probe State | `0x00`=Dry, `0x01`=Wet (external probe). |
| **7** | 1 | Probe Available | `0x00`=No probe, `0x01`=Probe connected. |
| **10** | 1 | Signal Strength | Unsigned RSSI. dBm = `-raw_value`. |

---

## 7. Event Log (`0x5335`)

The event log notification contains timestamped diagnostic messages from the dongle.

```
+---------------------+-----------------+-----------------------------+
| Timestamp (8 Bytes) | Msg Len (1 Byte)| Message Data (Variable)     |
+---------------------+-----------------+-----------------------------+
 0                     8                 9
```

- **Timestamp**: Big-endian `u64` millisecond epoch.
- **Msg Len**: Length of the message data (includes the Msg Len byte itself).
- **Message Data**: Binary log data. Structure is not fully documented but may contain event/MAC/type/state/counter tuples for sensor activity.

The host should ACK this notification but may otherwise ignore the data.

---

## 8. Auxiliary Commands

### 8.1 Trigger Audio Chime Alarm (`0x5370`)
- **Payload**: 8-byte ASCII MAC + `[ring_id, repeat_count, volume]` (11 bytes total).
- **Parameter Ranges**:
  - `ring_id`: `0x00`–`0xFF` (ring tone selection)
  - `repeat_count`: `0x01`–`0xFF` (number of repetitions)
  - `volume`: `0x01` (quiet) to `0x09` (maximum), clamped.
- **Write Example**: `Cmd=5370, Payload=[37,37,42,44,35,31,34,44,01,01,09]`
  - Target: `"77BD514D"`, Ring=1, Repeat=1, Volume=9 (Max)
- **Response**: `Cmd=5371, Payload=[<None>]`

### 8.2 Delete Sensor (`0x5325`)
- **Payload**: 8-byte ASCII MAC.
- **Response** (`0x5326`): 8-byte MAC + `[0xFF]` (9 bytes).
  - Validate: echoed MAC must match, ack code must be `0xFF`.

### 8.3 Delete All Sensors (`0x533F`)
- **Payload**: None.
- **Response** (`0x5340`): `[0xFF]` on success.
