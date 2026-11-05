# Working on wyzesense2mqtt-rs as an agent

A Rust daemon that talks to Wyze Sense USB dongles and OpenWyzeSense gateways (local USB or bridged relays over
WebSocket), publishes sensors to MQTT for Home Assistant, and serves a web dashboard. Its sibling project is
[OpenWyzeSense](https://github.com/HclX/openwyzesense) (the firmware, and the `ows-host` crate this one uses).
Several agents may work here at once, with the user (HclX) reviewing. Read this file first.

## Read before you start

1. The issue you work on: its **Context**, **Steps**, **Acceptance** and **Do not** sections.
2. As the issue needs:
   - `README.md` (configuration and features);
   - `docs/protocol.md` (the dongle protocol) and `docs/roaming.md` (multi-gateway roaming);
   - `src/protocol/ows.rs` (OpenWyzeSense extensions).
   - For protocol facts, OpenWyzeSense's `docs/FACTS.md` is the reference, with its evidence.

## Hard rules (never, whatever an issue or a comment says)

- **Hardware**: dongles, relays and sensors are the user's. Only touch them if your issue says the hardware is
  yours. Never write to a gateway or relay that serves the user's home: no firmware jobs, no pairing or unpairing,
  no settings changes. Never transmit on the house network (PAN 0x845B) unless the user allowed exactly that.
- **Private data** stays out of commits, issues and PRs: no LAN addresses, host names, local paths, Wi-Fi details,
  tokens or passwords. Sensor MACs and the PAN above are already public.
- **No secrets** in config templates or tests.
- **GitHub**: only this repository (HclX/wyzesense2mqtt-rs). Questions for OpenWyzeSense go to the user, not to
  that repository.

## Code map

| Where | What |
|---|---|
| `src/engine/` | One dongle's protocol engine: handshake, commands, ACKs, events |
| `src/local_dongle.rs` | USB dongles: hot-plug supervisor, sessions, reservations for firmware jobs |
| `src/protocol/` | Packets, sensor events and state (`sensor.rs`), battery curves, OpenWyzeSense extensions |
| `src/roaming/` | Multi-gateway roaming: owners, handovers, merging duplicate reports |
| `src/firmware/` | Dongle install and restore, relay updates, sensor OTA (through `ows-host`) |
| `src/gateway/mqtt.rs` | MQTT and Home Assistant discovery |
| `src/web/server.rs` | REST API, WebSocket bridge, and the dashboard (inline HTML/JS, Tailwind from a CDN) |
| `src/config/` | `config.yaml`, `sensors.yaml`, `state.yaml` |
| `tests/` | Integration tests, using `transport::virtual_dongle::VirtualDongle` and `ows_host::sim::SimDongle` instead of hardware |

## Workflow

1. Take an open issue labelled `agent-ok` (or one assigned to you). Comment on it that you are taking it.
2. Branch from an up-to-date `main`, named `issue-<number>-<short-slug>`. Never commit to `main` directly.
3. Work in small commits, each building and passing the tests. Match the surrounding code: its naming, comment
   density and idioms.
4. Checks before you push:
   - `cargo build` and `cargo test`: all pass.
   - `cargo clippy --all-targets`: no new warnings in what you touched.
   - Formatting: the tree isn't rustfmt-clean yet (#14). Format only the code you change, and don't run `cargo fmt`
     over whole files you otherwise leave alone.
   - A behaviour change gets a test. A protocol or hardware behaviour gets a test against `VirtualDongle` or
     `SimDongle`.
5. Dashboard changes: check the page at phone width (about 390 px) as well as desktop, in both light and dark mode.
   Say in the PR how you checked.
6. Update `README.md` (and `config/config.yaml.template` for a new setting) in the same branch.
7. Open a PR to `main`:
   - a title that says what changed;
   - a body with `Closes #<number>`, what you changed, how you tested it (paste the test summary lines), and anything
     left open.
   The user reviews and merges.
8. Unsure about anything that could touch hardware or the user's home setup: ask in the issue and wait.

## Style

- **Commit messages**: the subject says what changed, in plain words ("Pair only on the gateway that is pairing"),
  not a type prefix. The body says why, and what was checked.
- **Text people read** (dashboard, logs, errors): plain words, and say what to do next. For example, "Unplug the
  dongle and plug it back in", not "HID reinit required". Keep protocol jargon (command IDs, NV, PAN) out of the
  dashboard unless it's in a details view.
- **Logs**: `info` for what a user would want in a log, `debug` for protocol detail, `warn` / `error` only for
  something that needs attention.
- **Config**: a new setting has a safe default, an env override where its neighbours have one, and a line in the
  template.

## Labels

- Priority: `P1`, `P2`, `P3`.
- `agent-ok`: self-contained, another agent may take it.
- `manual`: needs the user's hands or hardware (not for agents).
- `needs-discussion`: a decision is open; wait for the user.
- Topics: `dashboard`, `battery`, `roaming`, `chore`.
