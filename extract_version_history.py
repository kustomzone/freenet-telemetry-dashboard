#!/usr/bin/env python3
"""Extract peer version lifecycle events from all telemetry logs.

Produces a compact JSON file with per-peer startup/shutdown events,
suitable for the dashboard to build version rollout timeseries.
"""

import json
import os
import glob
import time

TELEMETRY_DIR = "/mnt/media/freenet-telemetry"
OUTPUT_FILE = "/var/www/freenet-dashboard/version_history.json"


def process_file(filepath, lifecycle):
    """Process a single OTLP JSON log file for peer_startup/shutdown events."""
    count = 0
    events = 0
    with open(filepath, 'r') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                batch = json.loads(line)
                for resource_log in batch.get("resourceLogs", []):
                    for scope_log in resource_log.get("scopeLogs", []):
                        for record in scope_log.get("logRecords", []):
                            count += 1

                            # Parse attributes (event_type and peer_id are here)
                            attrs = {}
                            for a in record.get("attributes", []):
                                k = a.get("key", "")
                                v = a.get("value", {})
                                attrs[k] = v.get("stringValue") or v.get("doubleValue") or v.get("intValue", "")

                            event_type = attrs.get("event_type", "")
                            if event_type not in ("peer_startup", "peer_shutdown"):
                                continue

                            peer_id = attrs.get("peer_id", "")
                            if not peer_id:
                                continue

                            timestamp = record.get("timeUnixNano", "0")
                            timestamp = int(timestamp) if isinstance(timestamp, str) else timestamp

                            # Parse body (JSON string in stringValue)
                            body = {}
                            body_raw = record.get("body", {})
                            if isinstance(body_raw, dict):
                                body_str = body_raw.get("stringValue", "")
                                if body_str:
                                    try:
                                        body = json.loads(body_str)
                                    except:
                                        pass

                            if event_type == "peer_startup":
                                version = body.get("version", "unknown")
                                if not version or version == "unknown":
                                    continue
                                lifecycle[peer_id] = {
                                    "version": version,
                                    "startup_time": timestamp,
                                    "shutdown_time": None,
                                }
                                events += 1
                            elif event_type == "peer_shutdown":
                                if peer_id in lifecycle:
                                    lifecycle[peer_id]["shutdown_time"] = timestamp
                                    events += 1
            except Exception:
                continue

    return count, events


def main():
    log_files = sorted(glob.glob(os.path.join(TELEMETRY_DIR, "logs-*.jsonl")))
    current = os.path.join(TELEMETRY_DIR, "logs.jsonl")
    if os.path.exists(current):
        log_files.append(current)

    print(f"Found {len(log_files)} log files")

    lifecycle = {}
    total_records = 0
    total_events = 0

    for i, filepath in enumerate(log_files):
        fname = os.path.basename(filepath)
        size_mb = os.path.getsize(filepath) / 1e6
        print(f"  [{i+1}/{len(log_files)}] {fname} ({size_mb:.0f}MB)...", end="", flush=True)
        t0 = time.time()
        records, events = process_file(filepath, lifecycle)
        elapsed = time.time() - t0
        total_records += records
        total_events += events
        print(f" {records} records, {events} lifecycle events ({elapsed:.1f}s)")

    print(f"\nTotal: {total_records} records, {total_events} lifecycle events")
    print(f"Unique peers with version data: {len(lifecycle)}")

    version_counts = {}
    for data in lifecycle.values():
        v = data["version"]
        version_counts[v] = version_counts.get(v, 0) + 1
    for v in sorted(version_counts.keys()):
        print(f"  {v}: {version_counts[v]} peers")

    # Write compact output: [version, startup_ns, shutdown_ns?]
    output = []
    for pid, data in lifecycle.items():
        entry = [data["version"], data["startup_time"]]
        if data["shutdown_time"]:
            entry.append(data["shutdown_time"])
        output.append(entry)

    with open(OUTPUT_FILE, 'w') as f:
        json.dump({"peers": output, "extracted_at": int(time.time())}, f)

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"\nWrote {OUTPUT_FILE} ({size_kb:.1f}KB, {len(output)} peers)")


if __name__ == "__main__":
    main()
