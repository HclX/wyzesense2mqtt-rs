# Roaming across OpenWyzeSense gateways

Host side of OpenWyzeSense roaming. The gateway side is OpenWyzeSense
[`docs/roaming.md`](https://github.com/HclX/openwyzesense/blob/main/docs/roaming.md); the host request is
[HclX/openwyzesense#41](https://github.com/HclX/openwyzesense/issues/41).
The code is in `src/roaming/mod.rs`, and the wire formats are in
`src/protocol/ows.rs`.

## Wire

| What | Host → gateway | Gateway → host |
|---|---|---|
| Flags | `0x53F0 [0x01]` reads, `[0x01, flags_be32]` sets | `0x53F1 [0x01, enabled_be32, supported_be32]` |
| Build info | `0x53F0 [0x02, page]`, pages 0..2 | 53 / 55 / 27 B pages (version, board, product) |
| Network identity | `0x53F0 [0x04]` | `[0x04, status, pan_le16, short_le16, ext_le64]` |
| Sensor table | `0x53F0 [0x06, 0x04, index]` | `[0x06, 0x04, status, count, index, sub_le16, mac8, short_le16, type]` |
| Add a sensor | `0x53F0 [0x06, 0x03, mac8, short_le16, type]` | `[0x06, 0x03, status, count]` |
| Drop a sensor | stock `0x5325` (MAC) | stock `0x5326` |
| Address range | `0x53F0 [0x23, first_le16, last_le16]` | `[0x23, status, first_le16, last_le16]` |
| Overheard report | none | `0x53F1 [0x20, short_le16, <0x5319 payload>]` |

Every `0x53F1` reply goes to the host's engine as one async packet, and the
engine ACKs it. Replies are matched to their requests by sub-ID: the engine
keys pending requests on `0xF100 | sub`. An overheard report completes no
request. The engine turns it into a `DongleEvent` with `overheard_short` set.

A stock dongle ignores `0x53F0` (OpenWyzeSense `docs/stock-unknowns.md`, row D6). Its
only answer is the low-level ACK. The host probes once per connection with a
1.5 s timeout, and a gateway that gives no answer is treated as stock and left
alone.

## Feature flags (opt-in)

The `openwyzesense` config section sets `dedup`, `fix_zero_mac`, `announce`
and `roam`. Each can be set for all gateways, and overridden per gateway
under `gateways: { "<MAC>": {...} }`. All are off by default (OpenWyzeSense
R3). A gateway gets the configured flags that its firmware supports, right
after the probe on every connect. A configured flag that the firmware lacks
is logged. The host leaves flags it doesn't manage (OTA) as they are. Every
45 s the host re-reads the flags of each gateway that has flags configured,
and re-applies them if they are gone. Stock dongles never receive a flag.

## Event flow

Engines report into the coordinator. The coordinator forwards the merged
stream to the MQTT gateway and the `SensorManager`:

```
engines --raw_tx--> Roaming::process --event_tx--> MQTT gateway / SensorManager
```

For each event from a gateway on a roaming network, the coordinator:

1. Names the sensor. If an overheard report carries a zero MAC (a v1
   contact), the short address picks the sensor from the combined tables.
2. Drops an overheard report for a sensor that no gateway of ours owns.
3. Records the report's RSSI for the reporting gateway, smoothed with an
   EWMA.
4. Drops copies. A report is identified by the MAC, the event type and the
   sensor's event counter (or the contents, for a motion history), within
   30 s.
5. Stamps the event with the owner's MAC, so the `SensorManager` keeps
   recording the owner rather than whichever gateway heard the report first.

Events from stock dongles, and from a gateway that is alone on its network,
pass through unchanged.

## Lifecycle

- **Connect:** probe the flags, read the identity and the table. A sensor
  that another online gateway of the network already holds is removed from the
  newcomer, so the gateway that stayed up keeps it. Two different sensors on
  one short address are logged as an error, and one of them must be re-paired.
  Roaming groups are the gateways that support roaming and share an identity,
  except those configured `roam: false`. Roaming turns on by itself for every
  member once a group has two (a gateway left unset is "automatic"; `roam:
  true` forces it), and stays on. Without it, every gateway of the network
  ACKs every sensor and reports every join. A group needs two members before
  it does anything.
- **Disconnect:** the gateway is marked offline and its sensors stay with it
  (see the policy below). Its table stays cached.
- **Every 45 s:** re-read the flags (see above). A watchdog or brown-out
  reset, or a relay resetting its contact board, clears the flags without
  telling the host. A roaming gateway's table is also reloaded.
- **Pairing** (web `POST /api/scan` and MQTT `dongle/<mac>/scan/set`): any
  other gateway of the network that is pairing stops. The pairing gateway gets
  the range `[max short in the combined tables + 1, 0xFFFD]` right before
  `0x531C 01`. After the `Paired` event, its table is reloaded.

## Ownership policy

A sensor moves only on evidence about the radio. A gateway's link to the
host is no such evidence. When an ESP relay's WebSocket drops, its contact
board keeps running, keeps ACKing its sensors and buffers their reports.
Handing its sensors over at that point would give a sensor two owners whose
ACKs collide, which is exactly what `GW_FLAG_ROAM` exists to prevent. While
an owner is cut off from the host, the other gateways' overheard copies keep
its sensors' events flowing, still attributed to the owner.

A report is judged `settle` (5 s) after its first copy, once all its copies
(a sensor retries for about 2 s) and the owner's delivery are in. Signal
strength is checked on every report. The rules are a pure function
(`roaming::decide`) plus the per-report bookkeeping in `Tracker::settle`,
both with unit tests.

| Rule | Condition | Default |
|---|---|---|
| Not ACKed | `burst_reports` of the last `burst_window` reports that the owner didn't deliver reached the others as retry bursts: at least `burst_copies` copies, raised per sensor to one more than the most it sent while its owner was answering | 3 of 5, 4 copies |
| Owner missing reports | the owner is **connected**, and `missed_reports` reports in a row were overheard by others but never delivered by the owner (its radio missed them) | 3 |
| Stronger signal | both gateways have at least `min_samples` reports since the last handover, and the candidate's smoothed RSSI is `hysteresis_db` stronger | 5, 10 dB |

Measured on hardware (a stock v2 contact on test PAN 0x65FE, 2026-11-02): when
nobody answers, the sensor sends **6 copies** about 0.45 s apart. A newer event
cuts the retries of the previous one short, so a quick open/close shows 2
copies, then 6. When answered, it sends 1 copy. On the house's stock dongle,
healthy sensors send up to 3 copies on about 30% of their reports, because ACKs
get lost (OpenWyzeSense FACTS.md). A listener far from the sensor misses some
copies (it heard 6, 6, 4, 3, 2, 5 at −73 to −105 dBm). That's why the rule is
"3 bursts of at least 4 among the last 5" rather than every report in a row.

Bursts are counted whether or not the owner looks connected. A gateway that
loses power leaves its connection half-open until the heartbeat notices (up to
90 s), and meanwhile it still looks connected. If the old owner can't be reached
for the delete:
- **With burst evidence** (nobody answers the sensor), the handover goes ahead
  without it. When the old owner comes back, the recorded owner wins.
- **Without burst evidence**, the handover is abandoned. In that case it isn't
  held back by the timers, so the next report can try again.

The stronger-signal rule waits `cooldown` (30 min) after a handover of the same
sensor, and no rule moves a sensor within `min_interval` (60 s) of its last
handover. A failed handover also counts as a handover for these timers. Each
handover costs two flash writes (a delete and an add), so the stronger-signal
rule is deliberately slow. To move back, the old owner would have to become
10 dB stronger in turn. `hysteresis_db` can be set in `config.yaml`. The rest
are constants in `RoamingConfig::default()`. `GET /api/roaming` shows each
sensor's evidence (`missed_in_a_row`, `bursts_in_a_row`,
`most_copies_when_answered`).

To count a sensor's retries, the gateways must forward every copy. So a
gateway with `roam` never gets `DEDUP`, and the host drops the copies itself.
The host also drops a **stale replay**: an alarm or climate report whose
counter is not newer than the last one delivered for that sensor and event
type (the host remembers that counter for 24 h, and forgets it when the sensor
is paired again). This covers a returning relay sending on its buffer.
Heartbeats repeat the last event's counter, so they are dropped only as copies
within 30 s.

A **handover** first deletes the sensor at the old owner. This is skipped while
the old owner is disconnected, or when it's unreachable and the evidence is
bursts. Its table is cleaned when it reconnects: when two gateways hold one
sensor, the owner recorded in `state/roaming.yaml` keeps it. It then adds
the sensor at the new owner with the same short address and type. The sensor's
coordinator identity and address stay the same, so it does not re-pair. If the
add fails, the sensor is added back at the old owner. The new owner is saved in
`state.yaml` as the sensor's `dongle_mac`.

## Tests

- Unit tests: `src/protocol/ows.rs` (wire formats, the pairing range) and
  `src/roaming/mod.rs` (the policy, smoothing, dropping copies).
- `tests/roaming_test.rs`: two emulated gateways (`VirtualDongle::with_ows`)
  with real engines and the coordinator. Covers no flags unless configured,
  per-gateway flags, re-applying flags after an unannounced restart, leaving a
  stock dongle alone, merging copies, naming zero-MAC reports, a
  stronger-signal handover, hysteresis, a connected owner missing reports, an
  owner cut off from the host keeping its sensor until five retried reports in
  a row show its radio is gone, the old owner coming
  back with stale replays, and the pairing range.
- End to end (run by hand): two OpenWyzeSense `ows-host-gateway` processes
  sharing one identity and an `ows-host-sensor` on one simulated air, each
  gateway connected to this daemon through `tools/ws_bridge.py`. The sensor
  pairs on B within the range the daemon set. Its events arrive once, and both
  gateways hear them. When B's bridge stops (B's radio still running), the
  sensor stays on B and its events arrive through A. When B's gateway process
  dies, A hears every report retried, and the sensor moves to A after the
  fifth one.
  When B restarts from its flash file, it forgets the sensor. No counter is
  delivered twice, and the sensor never re-pairs.
