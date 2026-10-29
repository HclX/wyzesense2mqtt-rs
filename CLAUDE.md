# CLAUDE.md — Wyze Sense local control (ESP bridge + keypad support)

## Goal
Remove the Wyze cloud dependency from a Wyze Home Monitoring setup (Sense Hub, v2 sensors, keypad). Replace the Hub with a reflashed v1 Sense Bridge dongle, feed it into wyzesense2mqtt-rs, and run the alarm in Home Assistant (Alarmo), with optional Noonlight dispatch. This is one of the last cloud dependencies in the homelab.

## Working style
- Prefer direct diagnostic commands and iterative troubleshooting with real output over speculative fixes.
- When something depends on code or protocol details not yet seen, ask for the source or command output instead of guessing.

## Architecture
```
v2 sensors / keypad --sub-GHz--> Sense Bridge v1 (CC1310, HMS firmware)
  --USB HID--> ESP32-S3 (USB host, wyze-esp-bridge firmware)
  --WebSocket ws://<gateway>:8080/ws/bridge--> wyzesense2mqtt-rs (bridge-only mode)
  --MQTT discovery--> Home Assistant / Alarmo --> Noonlight integration
```

## Repos
- **HclX/wyzesense2mqtt-rs** — Rust gateway. **Target for keypad support.** Supports contact/motion V1/V2, leak V2, climate V2, MQTT discovery, and remote `dongle_bridge` instances (a transparent byte-level relay of about 200 lines, optional `--token`).
- **AK5nowman/WyzeSense** — C# reference. Keypad parsing is in `WyzeSenseCore/WyzeDongle.cs` (`keypadDataReceived`), command IDs are in `WyzeSenseCore/Packet.cs`, and the HMS firmware flasher is `WyzeSenseUpgrade`. The Blazor MQTT app is work-in-progress.
- **hadock/esp32-neos-alexa-bridge** — ESP32-S3 USB-host driver for the WyzeSense dongle (C++ port of HclX's protocol). Reference for the dongle write path.
- **konnected-io/noonlight-hass** — official Noonlight HA integration. Forks: Snuffy2 (config flow, address), KruseLuds (V2 API, sandbox mode, Alarmo-oriented).

## Hardware
- **Sense Bridge v1 dongle** — USB `1a86:e024`, CC1310 radio. Ordered.
  - **Before flashing HMS firmware:** AK5nowman issue #3. The HMS image's bootloader config is `0xc50cfec5`, but the Bridge's is `0xc501fec5`. Patch that byte from `0x0C` to `0x01`, or the dongle can never re-enter bootloader mode. In `hms_cc1310.bin` the CCFG dword `c5 0c fe c5` sits at file offset 0x1FFD8, so the byte to change is at **0x1FFD9** (verified 2026-10-02 by dumping the image; the flasher's 0xC5 sanity byte at 0x1FFDB is untouched). Patched image: `handoff/hms_cc1310-wyzebl.bin` (md5 5cd4a32f95775f13d53f8bb620cb2a44).
  - v2 sensors and the keypad must be removed from the Hub and re-paired to the dongle.
- **Sense Hub (FCC ID 2AUIUWSHB1)** — CC1310F128 radio, Realtek RTL8189FTV SDIO Wi-Fi, and a separate Linux-class SoC. **Not ESP-based.** No known reflash or root. Still useful for sniffing the UART between the SoC and the CC1310.
- **Waveshare ESP32-P4-WIFI6-POE-ETH**: preferred bridge board (USB-A host port, onboard PoE). Fallback: **Waveshare ESP32-S3-ETH (PoE)**. See the firmware section.
- **ESP32-S3-DevKitC-1 clone (N16R8, dual USB-C)**: being returned (erase flash first) — with the ports pointing right, **upper = COM** (shows up as `/dev/ttyACM0`) and **lower = OTG**. The **IN-OUT pad must be bridged** so the OTG port supplies 5V to the dongle. Power the board through COM. No headers soldered.
- **OTG adapter** — short aluminum USB-C to USB-A adapter, USB-A slot horizontal.
- **Logic analyzer** — KeeYees 24MHz 8-channel (fx2lafw driver, sigrok/PulseView). Ordered.

## Keypad protocol (from AK5nowman)
- Keypad sensor type: `0x05`. Inbound command `0x55`. Offsets below are after the 5-byte header.

| Offset | Meaning |
|---|---|
| `0x00` | Event type (maps to Wyze cloud codes P1301/P1302) |
| `0x01–0x08` | MAC address, ASCII |
| `0x0A` | Payload length |
| `0x0C` | Battery: 0–155 scale, not AON_BATMON (decoded 2026-10-04, see above) |
| `0x0E` | **Subtype** |
| `0x0F…` | Subtype data |
| `[0x0A]+0x0B` | Signal strength |

| Subtype | Meaning | Status |
|---|---|---|
| `0x02` | Mode button; raw+1 → Disarmed (2) / Home (3) / Away (4) | Decoded |
| `0x0A` | Keypad motion | Decoded |
| `0x08` | PIN: one digit per byte from `0x0F`, count = `[0x0A]` − 6 | Decoded |
| `0x06` | "Request profile," sent when PIN entry starts | Received, never answered |
| `0x0C` | Probably the emergency/side button | Not decoded |
| `0x12` | Leak sensor v2 (shares this path) | Decoded |

- **Battery (decoded 2026-10-04):** payload byte `0x0C` after the 5-byte header. Independent 0–155 scale, not AON_BATMON (healthy packs read 0x85–0x95 on all three keypads ≈ 4.2–4.7 V at /32). Gateway publishes pct = raw × 100 / 155 (estimated; discharge shape unmeasured) from every keypad event — button, PIN, motion, profile all carry the same header. Corroborated by raetha/wyzesense2mqtt ("0–155 raw scale, not AON_BATMON, ÷155"); its parse offset (data[7]) is wrong for real frames, only the scale was adopted.

- `WyzeKeyPadState` enum: Inactive 0, Active 1, Disarmed 2, Home 3, Away 4, Alarm 5.
- **Outbound:** `SendKeypadEvent = 0x53` (`CMD_SEND_KP_EVENT`) and `KeyPadEventPacket(cmd, statusByte)` are defined but **never called**. The packet has no MAC field, so it's probably incomplete. Two-way keypad sync is unverified.
- **Bug not to port:** the PIN event looks up the sensor type from `(WyzeSensorType)Data[0x0E]` (which is 8) instead of `KeyPad`.

## Plan: keypad support in wyzesense2mqtt-rs
1. Find the existing `0x55` / leak-V2 dispatch (`grep -rn "0x55\|0x12" src/`).
2. Add a keypad sensor type (`0x05`) and match arms for subtypes `0x02`, `0x0A`, `0x08`. Log `0x06` and `0x0C` as raw hex.
3. MQTT: publish mode and PIN as non-retained events (**never retain a PIN**); expose motion, battery, and signal through discovery.
4. HA: manual alarm panel / Alarmo. Validate the PIN in HA, not the gateway. Log real sequences first to learn whether mode or PIN arrives first.
5. Optional two-way: capture real `0x53` payloads and `0x06` replies by sniffing the Hub's UART.

## Plan: Hub UART sniff
- Unplug the Hub and **remove its internal 18650** before clipping anything.
- **Test pads (bottom of the Hub PCB, labeled):** `RXD_CC` / `TXD_CC` / `GND` = CC1310 UART (plus `T_CC`, `CC_K` = probably CC1310 cJTAG TMS/TCK). A second radio, **`BG21`** (Silicon Labs EFR32BG21, Bluetooth, probably for app onboarding), has `RXD_BG` / `TXD_BG` / `BG21_RST` and an SWD header. The top side has a CC1310 cJTAG header (`3.3V TMS TCK RST GND`).
- Logic analyzer CH0 → SoC-to-CC1310 line (outbound: `0x53`, replies to `0x06`). CH1 → CC1310-to-SoC line (inbound `0x55`). GND → Hub ground. Connect no power pins.
- PulseView at about 2MHz, UART decoder on both channels; try 115200 8N1 first, otherwise measure the narrowest pulse.
- Trip a sensor to identify the lines, then arm/disarm from the Wyze app and type a PIN on the keypad.

## wyze-esp-bridge firmware (ESP-IDF)
- **One source, three builds** (`build.sh`, ESP-IDF 5.5 Docker image), v2.0.0. Source `/srv/wyze-esp-bridge/` (moved out of handoff 2026-10-02), images in `/srv/wyze-esp-bridge/out/` and `handoff/`: `wyze-esp-bridge-<build>-merged.bin` (flash at 0x0) and `-ota.bin` (for `/ota`).
  - `p4` (chip rev v3.x) and `p4-rev1` (v0.x/v1.x; ESP-IDF treats them as incompatible hardware): **Waveshare ESP32-P4-WIFI6-POE-ETH, preferred board (2026-09-29).** Check the revision with `esptool --chip esp32p4 chip_id` before flashing.
  - `s3-eth`: Waveshare ESP32-S3-ETH (PoE), the fallback.
  - `handoff/wyze-esp-bridge-devkitc-merged.bin`: old Wi-Fi build for the returned DevKitC.
- **P4 board (from its schematic):** USB-A = USB 2.0 HS OTG (controller 0, the default for `usb_host_install`), with 5V through an always-on current-limited switch (DIO7003) from the board's 5V rail. That rail is fed by PoE (onboard), USB-C or the 5V terminal; USB-C appears to be reverse-protected (AO3401 + MMDT3906 ideal diode). USB-C goes through a CH343 to UART0 (GPIO37/38) with auto-reset, so flashing and the console work with the dongle plugged in. Ethernet: IP101 on RMII, MDC 31, MDIO 52, PHY reset 51, TXEN 49, TXD0/1 34/35, RXD0/1 29/30, CRS_DV 28, 50MHz clock in on 50 (= `ETH_ESP32_EMAC_DEFAULT_CONFIG()`). 32MB flash (16MB used), 32MB PSRAM. 55 × 68mm, 4 mounting holes 48 × 33mm apart. ESP32-C6 Wi-Fi co-processor unused.
- **S3-ETH board:** W5500 on SPI2 (MOSI 11, MISO 12, SCLK 13, CS 14, INT 10, RST 9). USB-C = native USB (GPIO19/20); its VBUS is the PoE 5V net, so on PoE it powers the dongle, but **never plug it into a PC while on PoE** (backfeed). Console on header GPIO43 TX / GPIO44 RX. After the firmware starts the USB host, USB flashing needs BOOT + RESET.
- Both: HTTP :80 `GET /status` (JSON, includes `board`), `GET /log` (16KB ring), `POST /ota` and `POST /reboot` (bridge token as Bearer or `?token=`). Two 3MB OTA slots with rollback: an image is marked valid when it gets an IP. Gateway URI/token in NVS, optional console setup, defaults from `sdkconfig.defaults`.
- Config: `sdkconfig.defaults` (shared) + `sdkconfig.defaults.<target>` (per board) + `sdkconfig.defaults.p4-rev1`. Components resolved: `usb_host_hid` 1.0.4 (`~1.0.3`: 1.1+ pulls in esp-usb's own USB stack), `esp_websocket_client` 1.8.0.
- **Status: all three compile cleanly, none run on hardware yet.**
- **Protocol decisions (verified earlier):** frame = protocol bytes only (strip the HID length byte, like `extract_hid_frame`); token as `?token=` query param; writes as SET_REPORT Output, report ID 0, interface 0 (dongle has no interrupt OUT endpoint).
- Build: `docker run --rm -v "$PWD":/project -w /project espressif/idf:release-v5.5 ./build.sh` (outputs in `out/`; write under `/project`, `../../` lands outside the mount).
- Flash the P4: `pipx run esptool --chip esp32p4 -p /dev/ttyACM0 write_flash 0x0 wyze-esp-bridge-p4-merged.bin`. Updates: `curl -H "Authorization: Bearer <token>" --data-binary @wyze-esp-bridge-p4-ota.bin http://<ip>/ota`.
- Expected logs: `wyze-esp-bridge 2.0.0 (esp32p4-eth)`, `network up, IP …`, `waiting for Wyze dongle`, then `Wyze dongle up, connecting to gateway` and `gateway connected`.
- Gateway config for bridge-only mode:
  ```yaml
  usb:
    dongle: "none"
  bridge:
    enabled: true
  ```

## Enclosure: `wyze_esp_bridge_p4_case.scad` (ESP32-P4 board)
- `handoff/wyze-esp-bridge/case/wyze_esp_bridge_p4_case.scad` (2026-09-29). Parametric, 60 × 74.3 × 27.1mm with lid. Screw-down tabs off by default (`ears`).
- Board on 4 standoffs; lid spacer tubes on the same holes; 4× M2.5 × 24–26mm screws hold board and lid (pilots run into the floor). Lid also snaps on: ridge on its lip (left/right/back only) into a groove inside those walls (`snap_grip` 0.3). `roof_extra` 2mm gives 2.5mm of wall above the RJ45 opening. Front: USB-A/USB-C stack (USB-C below the PCB), RJ45, TF. Left: RESET/BOOT pin holes. Lid: PWR LED hole, vents over the PoE transformer.
- Board frame: x from the left (buttons) edge, y from the connector edge. Holes x 3.5/51.5, y 25/58. USB stack x 12.2 (w 13.6), RJ45 x 29.95 (w 16.1), TF x 45.2; connectors overhang 1.75. Buttons y 12.5 (RESET) / 5.8 (BOOT). Taken from Waveshare's size drawing and the PCB layout page of the schematic.
- **Unmeasured (marked `MEASURE`):** `usba_z0`/`usba_h` (7.0), `usbc_zc` (−3.2), `rj_z0`/`rj_h` (13.5), `top_clear` (14.5), `below_clear` (4), `btn_z`. Rendered in Docker (OpenSCAD 2021.01): both parts manifold.
- The dongle plugs into the USB-A outside the case. Its size is unknown; if it's wider than ~35mm it may block the RJ45 plug (a short USB-A extension fixes that).

## Enclosure: `wyze_esp_bridge_case.scad` (obsolete: for the DevKitC-1)
- Parametric OpenSCAD. Layout along X: antenna overhang → board (flat, no headers) → OTG adapter (cradle underneath) → Sense Bridge standing up like a plate across the end (its USB-A plug is on its back face).
- **Rebuilt 2026-09-29** (the original stayed on RosaPC): `handoff/wyze-esp-bridge/case/wyze_esp_bridge_case.scad`, in the same project as the firmware (planned as its own repo).
- **Power:** molded USB-C pigtail socket in the -X end wall (its own bay ahead of the antenna), wired to the board's 5V/GND pins; the IN-OUT bridge passes 5V to the OTG port. The COM side slot is now optional (`com_slot`, default off). Lid has hold-down tabs over the board, adapter, bridge and socket, plus vents over the antenna end.
- Checked with OpenSCAD 2021.01 in Docker: base and lid are manifold. `show_parts = true` (F5) shows ghost parts for fit checks.
- The case widens and raises parts automatically based on the connector offsets.

| # | Variable | Value | Notes |
|---|---|---|---|
| 1 | `board_l` | 58.8 | PCB only |
| 2 | `board_w` | 28.65 | |
| 3 | `pin_h` | 0 | Flat underside |
| 4 | `comp_h` | 3.0 | 4.75 total incl. PCB |
| 5 | `otg_y` | 21.65 | 2.53 edge gap + ~4.47 half-shell; confirm 2.53 was to the port edge |
| 18 | `pcb_t` | 1.75 | |
| 19 | `port_off` | 1.48 | (4.70 − 1.75) / 2 |
| 20 | `port_ovh` | 1.18 | |
| — | `ant_ovh` | 5.99 | Antenna past PCB |
| 6 | `otg_body` | 20.75 | **Re-measure assembled:** PCB edge → USB-A face (plug shows exposed metal) |
| 12 | `otg_bw` | 14.62 | |
| 13 | `otg_bh` | 7.06 | |
| 14 | `otg_cz` | 3.15 | |
| 15 | `otg_az` | 4.17 | Shell center |
| 16/17 | `otg_cy`/`otg_ay` | 7.31 | Centered (from photo) |
| 7–11 | `dongle_*`, `plug_*` | **TBD** | Sense Bridge not arrived; measure with back face toward you, plug horizontal |
| — | `pwr_w` × `pwr_h` | 13.37 × 5.26 | USB-C power socket body |
| — | `pwr_d` | 8.21 | Socket body depth, front face → wire exit |
| — | `com_y` | 7.0 (assumed) | COM port centre from the PCB's -Y edge; measure |

- Export: F6 then F7 in OpenSCAD. Tune `clr` (0.4) for printer fit.

## Home Assistant / monitoring notes
- Rebuild arming logic in Alarmo; Alarmo `triggered` → `noonlight.create_alarm`, after the entry delay.
- Noonlight is inherently cloud (token refresh via a Konnected-hosted endpoint), US-only, and needs a US mobile number.
- Losing the Hub means losing its siren, battery backup, and cellular fallback. Put HA, router, and modem on a UPS; consider LTE failover.
- Keep Wyze Home Monitoring active until the HA/Noonlight path is tested end to end.

## Open items
- [ ] Erase the DevKitC clone's flash and return it
- [x] First compile of wyze-esp-bridge (2026-09-29)
- [x] Resolve the three VERIFY items
- [ ] Caliper the P4 case's `MEASURE` values, then print it
- [ ] Get the ESP32-P4-WIFI6-POE-ETH, check chip revision, flash the matching image, check `/status`, test with the dongle
- [x] Patch the HMS image bootloader byte, flash the dongle, confirm `lsusb` shows `1a86:e024` (done 2026-10-02; write verified by chip CRC32 906D8AA1 == file CRC32)
- [ ] Re-pair sensors and keypad to the dongle
- [ ] Hub UART sniff for `0x53` / `0x06` replies
- [ ] Keypad support PR to wyzesense2mqtt-rs
- [ ] Alarmo + Noonlight (sandbox first)
