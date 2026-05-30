#!/usr/bin/env python3
"""Extract raw USB packets from WyzeSenseRS trace logs.

Usage:
    # Output JSONL capture file (for programmatic use)
    python3 tools/extract_packets.py trace.log > captures/session.jsonl

    # Output human-readable summary
    python3 tools/extract_packets.py --summary trace.log

    # Generate Rust test stubs from captures
    python3 tools/extract_packets.py --rust-tests trace.log

    # Filter by direction
    python3 tools/extract_packets.py --filter=read trace.log
    python3 tools/extract_packets.py --filter=write trace.log

Supported log formats:
    New format (current code):
        WIRE READ raw=N bytes:[XX,XX,...] proto=M bytes:[YY,YY,...]
        WIRE WRITE (N bytes): [XX,XX,...]

    Old format (pre-refactor):
        WIRE READ (N of M bytes, HID length byte XX): [YY,YY,...]
        WIRE READ (N bytes, stripped virtual Report ID XX): [YY,YY,...]
        WIRE READ (N bytes, raw / no length prefix): [YY,YY,...]
        WIRE WRITE (N bytes): [XX,XX,...]
"""

import re
import sys
import json
import argparse
from datetime import datetime

# --- Regex patterns for both log formats ---

# New format: WIRE READ raw=N bytes:[XX,XX,...] proto=M bytes:[YY,YY,...]
RE_READ_NEW = re.compile(
    r'(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\s+.*WIRE READ raw=(\d+) bytes:\[([0-9A-Fa-f,]+)\] proto=(\d+) bytes:\[([0-9A-Fa-f,]+)\]'
)

# Old format: WIRE READ (N bytes, stripped virtual Report ID XX): [YY,YY,...]
# Old format: WIRE READ (N of M bytes, HID length byte XX): [YY,YY,...]
# Old format: WIRE READ (N bytes, raw / no length prefix): [YY,YY,...]
RE_READ_OLD = re.compile(
    r'(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\s+.*WIRE READ \((\d+)(?: of \d+)? bytes.*?\):\s*\[([0-9A-Fa-f,]+)\]'
)

# Write format (same in both old and new):
RE_WRITE = re.compile(
    r'(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\s+.*WIRE WRITE \((\d+) bytes\):\s*\[([0-9A-Fa-f,]+)\]'
)

# Python format (wyzesense2mqtt): Trying to parse: XX,XX,...
RE_READ_PYTHON = re.compile(
    r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+DEBUG\s+wyzesense2mqtt\s+Trying to parse:\s*([0-9A-Fa-f,]+)'
)

# Python format (wyzesense2mqtt): Sending: XX,XX,...
RE_WRITE_PYTHON = re.compile(
    r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s+DEBUG\s+wyzesense2mqtt\s+Sending:\s*([0-9A-Fa-f,]+)'
)

# Strip ANSI escape sequences (tracing-subscriber colors)
RE_ANSI = re.compile(r'\x1b\[[0-9;]*m')


def parse_hex_list(hex_str: str) -> list[int]:
    """Parse comma-separated hex string into bytes list."""
    return [int(h, 16) for h in hex_str.split(',') if h.strip()]


def hex_list_to_str(hex_str: str) -> str:
    """Convert comma-separated hex to continuous hex string."""
    return ''.join(h.strip().lower() for h in hex_str.split(',') if h.strip())


def parse_timestamp(ts_str: str) -> float:
    """Parse ISO or space-separated timestamp to epoch milliseconds."""
    try:
        clean_ts = ts_str.strip().replace(' ', 'T')
        if not clean_ts.endswith('Z') and '+' not in clean_ts:
            clean_ts += 'Z'
        dt = datetime.fromisoformat(clean_ts.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


# Known command IDs for human-readable output
CMD_NAMES = {
    0x4327: "Inquiry",
    0x4328: "InquiryResponse",
    0x4302: "RequestENR",
    0x4303: "ENRResponse",
    0x4304: "RequestMAC",
    0x4305: "MACResponse",
    0x5316: "RequestVersion",
    0x5317: "VersionResponse",
    0x5314: "FinishAuth",
    0x5315: "FinishAuthResponse",
    0x531C: "SetScan",
    0x531D: "SetScanResponse",
    0x5320: "SensorScanned",
    0x5321: "RequestR1",
    0x5322: "R1Response",
    0x5323: "VerifySensor",
    0x5324: "VerifySensorResponse",
    0x5325: "DeleteSensor",
    0x5326: "DeleteSensorResponse",
    0x5319: "SensorAlarm",
    0x5330: "RequestSensorList",
    0x5331: "SensorListItem",
    0x5332: "RequestTimeSync",
    0x5333: "TimeSyncResponse",
    0x53FF: "ACK",
}


def parse_packet_cmd(data: list[int]) -> tuple[int, str] | None:
    """Try to parse the command ID from protocol bytes."""
    if len(data) < 5:
        return None
    magic = (data[0] << 8) | data[1]
    if magic not in (0x55AA, 0xAA55):
        return None
    cmd = (data[2] << 8) | data[4]
    name = CMD_NAMES.get(cmd, f"Unknown(0x{cmd:04X})")
    return cmd, name


def extract_packets(lines: list[str]) -> list[dict]:
    """Extract all WIRE READ/WRITE entries from log lines."""
    records = []
    for line in lines:
        clean = RE_ANSI.sub('', line)

        # Try new read format first
        m = RE_READ_NEW.search(clean)
        if m:
            ts_str, raw_len, raw_hex, proto_len, proto_hex = m.groups()
            records.append({
                'ts_ms': parse_timestamp(ts_str),
                'dir': 'R',
                'raw_hex': hex_list_to_str(raw_hex),
                'raw_len': int(raw_len),
                'proto_hex': hex_list_to_str(proto_hex),
                'proto_len': int(proto_len),
            })
            continue

        # Try old read format
        m = RE_READ_OLD.search(clean)
        if m:
            ts_str, byte_count, hex_data = m.groups()
            hex_continuous = hex_list_to_str(hex_data)
            records.append({
                'ts_ms': parse_timestamp(ts_str),
                'dir': 'R',
                'proto_hex': hex_continuous,
                'proto_len': int(byte_count),
            })
            continue

        # Try write format
        m = RE_WRITE.search(clean)
        if m:
            ts_str, byte_count, hex_data = m.groups()
            hex_continuous = hex_list_to_str(hex_data)
            records.append({
                'ts_ms': parse_timestamp(ts_str),
                'dir': 'W',
                'proto_hex': hex_continuous,
                'proto_len': int(byte_count),
            })
            continue

        # Try Python read format
        m = RE_READ_PYTHON.search(clean)
        if m:
            ts_str, hex_data = m.groups()
            hex_continuous = hex_list_to_str(hex_data)
            proto_len = len(hex_continuous) // 2
            
            # Synthesize raw HID frame: length prefix + protocol bytes + trailing zero-padding
            raw_bytes = [proto_len] + parse_hex_list(hex_data)
            if len(raw_bytes) < 63:
                raw_bytes += [0] * (63 - len(raw_bytes))
            raw_hex_str = ''.join(f'{b:02x}' for b in raw_bytes)

            records.append({
                'ts_ms': parse_timestamp(ts_str),
                'dir': 'R',
                'raw_hex': raw_hex_str,
                'raw_len': len(raw_bytes),
                'proto_hex': hex_continuous,
                'proto_len': proto_len,
            })
            continue

        # Try Python write format
        m = RE_WRITE_PYTHON.search(clean)
        if m:
            ts_str, hex_data = m.groups()
            hex_continuous = hex_list_to_str(hex_data)
            records.append({
                'ts_ms': parse_timestamp(ts_str),
                'dir': 'W',
                'proto_hex': hex_continuous,
                'proto_len': len(hex_continuous) // 2,
            })
            continue

    return records


def output_jsonl(records: list[dict], direction_filter: str | None = None):
    """Output records as JSONL."""
    for rec in records:
        if direction_filter and rec['dir'] != direction_filter:
            continue
        print(json.dumps(rec))


def output_summary(records: list[dict]):
    """Output human-readable packet summary."""
    print(f"{'#':>4}  {'Dir':>3}  {'Bytes':>5}  {'Command':<25}  {'Hex (first 32 bytes)'}")
    print("-" * 80)
    for i, rec in enumerate(records):
        proto_bytes = parse_hex_list(','.join(
            rec['proto_hex'][j:j+2] for j in range(0, len(rec['proto_hex']), 2)
        ))
        pkt_info = parse_packet_cmd(proto_bytes)
        cmd_name = pkt_info[1] if pkt_info else "(fragment)"
        hex_preview = rec['proto_hex'][:64]  # First 32 bytes = 64 hex chars
        if len(rec['proto_hex']) > 64:
            hex_preview += "..."
        arrow = "<<<" if rec['dir'] == 'R' else ">>>"
        print(f"{i:4}  {arrow}  {rec['proto_len']:5}  {cmd_name:<25}  {hex_preview}")

    # Stats
    reads = sum(1 for r in records if r['dir'] == 'R')
    writes = sum(1 for r in records if r['dir'] == 'W')
    print(f"\nTotal: {len(records)} packets ({reads} reads, {writes} writes)")


def output_rust_tests(records: list[dict]):
    """Generate Rust test code from captured packets."""
    print("// Auto-generated from trace log by tools/extract_packets.py")
    print("// Verify expected values against Python reference implementation")
    print("use WyzeSenseRS::protocol::packet::Packet;\n")

    for i, rec in enumerate(records):
        proto_bytes = parse_hex_list(','.join(
            rec['proto_hex'][j:j+2] for j in range(0, len(rec['proto_hex']), 2)
        ))
        pkt_info = parse_packet_cmd(proto_bytes)
        if not pkt_info:
            continue

        cmd_id, cmd_name = pkt_info
        dir_label = "read" if rec['dir'] == 'R' else "write"
        test_name = f"test_capture_{dir_label}_{cmd_name.lower()}_{i}"
        hex_bytes = ', '.join(f'0x{rec["proto_hex"][j:j+2].upper()}' for j in range(0, len(rec['proto_hex']), 2))

        print(f"#[test]")
        print(f"fn {test_name}() {{")
        print(f"    // Captured {dir_label}: {cmd_name}")
        print(f"    let data = vec![{hex_bytes}];")
        print(f"    let (pkt, consumed) = Packet::parse(&data).expect(\"Failed to parse {cmd_name}\");")
        print(f"    assert_eq!(pkt.cmd(), 0x{cmd_id:04X});")
        if rec.get('raw_hex'):
            raw_bytes = ', '.join(f'0x{rec["raw_hex"][j:j+2].upper()}' for j in range(0, len(rec['raw_hex']), 2))
            print(f"    // Raw HID frame: [{raw_bytes}]")
        print(f"}}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Extract raw USB packets from WyzeSenseRS trace logs'
    )
    parser.add_argument('logfile', help='Path to trace log file')
    parser.add_argument('--summary', action='store_true',
                       help='Output human-readable summary instead of JSONL')
    parser.add_argument('--rust-tests', action='store_true',
                       help='Generate Rust test stubs from captures')
    parser.add_argument('--filter', choices=['read', 'write'],
                       help='Filter by direction (read=dongle→host, write=host→dongle)')
    args = parser.parse_args()

    with open(args.logfile, 'r') as f:
        lines = f.readlines()

    records = extract_packets(lines)

    if not records:
        print("No WIRE READ/WRITE entries found in log.", file=sys.stderr)
        sys.exit(1)

    direction_filter = None
    if args.filter:
        direction_filter = 'R' if args.filter == 'read' else 'W'

    if args.rust_tests:
        filtered = [r for r in records if not direction_filter or r['dir'] == direction_filter]
        output_rust_tests(filtered)
    elif args.summary:
        filtered = [r for r in records if not direction_filter or r['dir'] == direction_filter]
        output_summary(filtered)
    else:
        output_jsonl(records, direction_filter)


if __name__ == '__main__':
    main()
