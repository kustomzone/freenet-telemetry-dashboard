#!/usr/bin/env python3
"""
Freenet Network Dashboard Generator

Parses telemetry logs and generates an HTML dashboard with ring visualization.
Peers are displayed on a ring based on their network location (0.0-1.0).
"""

import json
import hashlib
import math
import re
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

TELEMETRY_LOG = Path("/var/log/freenet-telemetry/logs.jsonl")
OUTPUT_HTML = Path("/var/www/freenet-dashboard/index.html")

# Time window for "active" peers (minutes)
ACTIVE_WINDOW_MINUTES = 30


def anonymize_ip(ip: str) -> str:
    """Convert IP to anonymous identifier."""
    h = hashlib.sha256(ip.encode()).hexdigest()[:8]
    return f"peer-{h}"


def is_public_ip(ip: str) -> bool:
    """Check if IP is a public (non-test) address."""
    if not ip:
        return False
    # Filter out localhost, private ranges, and test IPs
    if ip.startswith("127.") or ip.startswith("172.") or ip.startswith("10.") or ip.startswith("192.168."):
        return False
    if ip.startswith("0.") or ip == "localhost":
        return False
    return True


def parse_telemetry():
    """Parse telemetry logs and extract peer information."""
    peers = {}  # ip -> {location, last_seen, events}
    recent_events = []
    operations = defaultdict(lambda: {"requests": 0, "successes": 0})

    if not TELEMETRY_LOG.exists():
        return peers, recent_events, operations

    cutoff_time = datetime.now() - timedelta(minutes=ACTIVE_WINDOW_MINUTES)
    cutoff_nano = int(cutoff_time.timestamp() * 1_000_000_000)

    try:
        with open(TELEMETRY_LOG, 'r') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    batch = json.loads(line)
                    for resource_log in batch.get("resourceLogs", []):
                        for scope_log in resource_log.get("scopeLogs", []):
                            for record in scope_log.get("logRecords", []):
                                process_record(record, peers, recent_events, operations, cutoff_nano)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"Error reading telemetry: {e}")

    return peers, recent_events, operations


# Pattern to parse peer strings like: "PeerId@IP:port (@ location)"
PEER_PATTERN = re.compile(r'@(\d+\.\d+\.\d+\.\d+):(\d+)\s*\(@\s*([\d.]+)\)')


def parse_peer_string(peer_str):
    """Extract IP and location from peer string like 'PeerId@IP:port (@ location)'."""
    if not peer_str:
        return None, None
    match = PEER_PATTERN.search(peer_str)
    if match:
        ip = match.group(1)
        location = float(match.group(3))
        return ip, location
    return None, None


def process_record(record, peers, recent_events, operations, cutoff_nano):
    """Process a single log record."""
    attrs = {a["key"]: a["value"].get("stringValue") or a["value"].get("doubleValue")
             for a in record.get("attributes", [])}

    timestamp_raw = record.get("timeUnixNano", "0")
    timestamp = int(timestamp_raw) if isinstance(timestamp_raw, str) else timestamp_raw
    event_type = attrs.get("event_type", "")

    # Parse the body content
    body_str = record.get("body", {}).get("stringValue", "")
    body = {}
    if body_str:
        try:
            body = json.loads(body_str)
        except json.JSONDecodeError:
            pass

    # Get event type from body if not in attrs
    if not event_type:
        event_type = body.get("type", "")

    # Extract peer info from body fields
    peer_ip = None
    location = None

    # Try different body fields that might contain peer info
    for field in ["this_peer", "requester", "target", "connected_peer"]:
        if field in body:
            ip, loc = parse_peer_string(body[field])
            if ip and is_public_ip(ip):
                peer_ip = ip
                location = loc
                break

    # Track peer if found
    if peer_ip and is_public_ip(peer_ip):
        if peer_ip not in peers:
            peers[peer_ip] = {"location": None, "last_seen": 0, "event_count": 0}

        if location is not None:
            peers[peer_ip]["location"] = location

        if timestamp > peers[peer_ip]["last_seen"]:
            peers[peer_ip]["last_seen"] = timestamp
        peers[peer_ip]["event_count"] += 1

    # Track recent events (last 30 minutes)
    if timestamp > cutoff_nano and event_type:
        # Track operation stats
        if event_type.endswith("_request"):
            op_type = event_type.replace("_request", "")
            operations[op_type]["requests"] += 1
        elif event_type.endswith("_success"):
            op_type = event_type.replace("_success", "")
            operations[op_type]["successes"] += 1

        # Keep recent events for display (limit to last 50)
        if len(recent_events) < 50:
            recent_events.append({
                "time": timestamp,
                "type": event_type,
                "peer": anonymize_ip(peer_ip) if peer_ip and is_public_ip(peer_ip) else "unknown"
            })


def generate_html(peers, recent_events, operations):
    """Generate the dashboard HTML."""

    # Convert peers to anonymous list with locations
    peer_list = []
    for ip, data in peers.items():
        if is_public_ip(ip) and data["location"] is not None:
            peer_list.append({
                "id": anonymize_ip(ip),
                "location": data["location"],
                "events": data["event_count"],
                "last_seen": data["last_seen"]
            })

    # Sort by location for consistent display
    peer_list.sort(key=lambda p: p["location"])

    # Generate SVG ring with peers
    ring_svg = generate_ring_svg(peer_list)

    # Generate recent events table
    events_html = generate_events_table(recent_events)

    # Generate operations summary
    ops_html = generate_operations_summary(operations)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Freenet Network</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Space+Grotesk:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-primary: #06080c;
            --bg-secondary: #0d1117;
            --bg-tertiary: #161b22;
            --bg-panel: rgba(13, 17, 23, 0.8);
            --border-color: rgba(48, 54, 61, 0.6);
            --border-glow: rgba(0, 127, 255, 0.2);

            --text-primary: #e6edf3;
            --text-secondary: #8b949e;
            --text-muted: #484f58;

            /* Freenet brand colors */
            --accent-primary: #007FFF;
            --accent-light: #7ecfef;
            --accent-dark: #0052cc;
            --accent-glow: rgba(0, 127, 255, 0.4);

            --color-connect: #7ecfef;
            --color-put: #fbbf24;
            --color-get: #34d399;
            --color-update: #a78bfa;
            --color-subscribe: #f472b6;
            --color-error: #f87171;

            --font-mono: 'JetBrains Mono', 'SF Mono', Monaco, monospace;
            --font-sans: 'Space Grotesk', -apple-system, BlinkMacSystemFont, sans-serif;
        }}

        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: var(--font-sans);
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            overflow-x: hidden;
        }}

        /* Atmospheric background */
        body::before {{
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background:
                radial-gradient(ellipse at 50% 0%, rgba(0, 127, 255, 0.08) 0%, transparent 50%),
                radial-gradient(ellipse at 80% 50%, rgba(126, 207, 239, 0.05) 0%, transparent 40%),
                radial-gradient(ellipse at 20% 80%, rgba(0, 82, 204, 0.05) 0%, transparent 40%);
            pointer-events: none;
            z-index: 0;
        }}

        /* Grid overlay */
        body::after {{
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-image:
                linear-gradient(rgba(255,255,255,0.02) 1px, transparent 1px),
                linear-gradient(90deg, rgba(255,255,255,0.02) 1px, transparent 1px);
            background-size: 50px 50px;
            pointer-events: none;
            z-index: 0;
        }}

        .main-content {{
            flex: 1;
            padding: 24px;
            padding-bottom: 180px;
            position: relative;
            z-index: 1;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        /* Header */
        .header {{
            text-align: center;
            margin-bottom: 32px;
        }}

        .header-brand {{
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 16px;
        }}

        .header-logo {{
            width: 48px;
            height: 48px;
        }}

        .header h1 {{
            font-family: var(--font-mono);
            font-size: 2.5em;
            font-weight: 600;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, var(--accent-light) 0%, var(--accent-primary) 50%, var(--accent-dark) 100%);
            background-size: 200% auto;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            animation: shimmer 8s linear infinite;
        }}

        @keyframes shimmer {{
            0% {{ background-position: 0% center; }}
            100% {{ background-position: 200% center; }}
        }}

        .status-badge {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            margin-top: 12px;
            padding: 6px 16px;
            background: var(--bg-panel);
            border: 1px solid var(--border-color);
            border-radius: 20px;
            font-family: var(--font-mono);
            font-size: 0.8em;
            font-weight: 500;
            backdrop-filter: blur(10px);
        }}

        .status-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--text-muted);
        }}

        .status-dot.live {{
            background: var(--accent-primary);
            box-shadow: 0 0 12px var(--accent-glow);
            animation: pulse-glow 2s ease-in-out infinite;
        }}

        .status-dot.historical {{
            background: var(--color-put);
            box-shadow: 0 0 12px rgba(251, 191, 36, 0.4);
        }}

        .status-dot.disconnected {{
            background: var(--color-error);
        }}

        @keyframes pulse-glow {{
            0%, 100% {{ opacity: 1; transform: scale(1); }}
            50% {{ opacity: 0.7; transform: scale(1.2); }}
        }}

        /* Stats Grid */
        .stats {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px;
            margin-bottom: 24px;
        }}

        @media (max-width: 768px) {{
            .stats {{ grid-template-columns: repeat(2, 1fr); }}
        }}

        .stat {{
            background: var(--bg-panel);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 20px;
            text-align: center;
            backdrop-filter: blur(10px);
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }}

        .stat::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: linear-gradient(90deg, transparent, var(--accent-primary), transparent);
            opacity: 0;
            transition: opacity 0.3s;
        }}

        .stat:hover {{
            border-color: var(--border-glow);
            transform: translateY(-2px);
        }}

        .stat:hover::before {{
            opacity: 1;
        }}

        .stat-value {{
            font-family: var(--font-mono);
            font-size: 2.8em;
            font-weight: 300;
            color: var(--accent-primary);
            line-height: 1;
            letter-spacing: -0.02em;
        }}

        .stat-label {{
            font-size: 0.75em;
            font-weight: 500;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-top: 8px;
        }}

        .stat-hint {{
            font-size: 0.65em;
            color: var(--text-muted);
            margin-top: 4px;
            font-weight: 400;
            letter-spacing: 0;
            text-transform: none;
        }}

        /* Operation Stats */
        .op-stats-row {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 12px;
            margin-bottom: 24px;
        }}

        @media (max-width: 768px) {{
            .op-stats-row {{ grid-template-columns: repeat(2, 1fr); }}
        }}

        .op-stat {{
            background: var(--bg-panel);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 14px 16px;
            backdrop-filter: blur(10px);
            display: flex;
            align-items: center;
            gap: 12px;
        }}

        .op-stat-icon {{
            width: 36px;
            height: 36px;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.1em;
            flex-shrink: 0;
        }}

        .op-stat-icon.put {{ background: rgba(251, 191, 36, 0.15); color: var(--color-put); }}
        .op-stat-icon.get {{ background: rgba(52, 211, 153, 0.15); color: var(--color-get); }}
        .op-stat-icon.update {{ background: rgba(167, 139, 250, 0.15); color: var(--color-update); }}
        .op-stat-icon.subscribe {{ background: rgba(244, 114, 182, 0.15); color: var(--color-subscribe); }}

        .op-stat-content {{
            flex: 1;
            min-width: 0;
        }}

        .op-stat-header {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            margin-bottom: 4px;
        }}

        .op-stat-name {{
            font-family: var(--font-mono);
            font-size: 0.75em;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}

        .op-stat-rate {{
            font-family: var(--font-mono);
            font-size: 0.85em;
            font-weight: 500;
        }}

        .op-stat-rate.good {{ color: var(--color-get); }}
        .op-stat-rate.warn {{ color: var(--color-put); }}
        .op-stat-rate.bad {{ color: var(--color-error); }}

        .op-stat-details {{
            display: flex;
            gap: 12px;
            font-family: var(--font-mono);
            font-size: 0.7em;
            color: var(--text-muted);
        }}

        .op-stat-detail {{
            display: flex;
            align-items: center;
            gap: 4px;
        }}

        .op-stat-detail .label {{
            opacity: 0.7;
        }}

        /* Dashboard Grid */
        .dashboard {{
            display: grid;
            grid-template-columns: 1.2fr 1fr;
            gap: 20px;
        }}

        @media (max-width: 1000px) {{
            .dashboard {{ grid-template-columns: 1fr; }}
        }}

        .panel {{
            background: var(--bg-panel);
            border: 1px solid var(--border-color);
            border-radius: 20px;
            padding: 24px;
            backdrop-filter: blur(10px);
            position: relative;
        }}

        .panel-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 20px;
            padding-bottom: 16px;
            border-bottom: 1px solid var(--border-color);
        }}

        .panel-title {{
            font-family: var(--font-mono);
            font-size: 0.9em;
            font-weight: 500;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }}

        .panel-badge {{
            font-family: var(--font-mono);
            font-size: 0.7em;
            padding: 4px 10px;
            background: var(--bg-tertiary);
            border-radius: 12px;
            color: var(--text-muted);
        }}

        .panel-subtitle {{
            font-size: 0.75em;
            color: var(--text-muted);
            margin-top: -12px;
            margin-bottom: 16px;
            font-weight: 400;
        }}

        /* Filter Bar */
        .filter-bar {{
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            background: rgba(0, 0, 0, 0.2);
            border-bottom: 1px solid var(--border-color);
            min-height: 40px;
            flex-wrap: wrap;
        }}

        .filter-bar-label {{
            font-size: 0.7em;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-right: 4px;
        }}

        .filter-chip {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 8px 4px 10px;
            background: rgba(126, 207, 239, 0.15);
            border: 1px solid rgba(126, 207, 239, 0.3);
            border-radius: 16px;
            font-family: var(--font-mono);
            font-size: 0.75em;
            color: var(--accent-primary);
        }}

        .filter-chip.contract {{
            background: rgba(244, 114, 182, 0.15);
            border-color: rgba(244, 114, 182, 0.3);
            color: var(--color-subscribe);
        }}

        .filter-chip.peer {{
            background: rgba(52, 211, 153, 0.15);
            border-color: rgba(52, 211, 153, 0.3);
            color: var(--color-get);
        }}

        .filter-chip-close {{
            width: 14px;
            height: 14px;
            border-radius: 50%;
            background: rgba(255, 255, 255, 0.2);
            border: none;
            color: inherit;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 10px;
            line-height: 1;
            padding: 0;
        }}

        .filter-chip-close:hover {{
            background: rgba(255, 255, 255, 0.4);
        }}

        .filter-clear-all {{
            margin-left: auto;
            padding: 4px 10px;
            background: transparent;
            border: 1px solid var(--border-color);
            border-radius: 4px;
            color: var(--text-muted);
            font-size: 0.7em;
            cursor: pointer;
            transition: all 0.2s;
        }}

        .filter-clear-all:hover {{
            background: rgba(255, 255, 255, 0.05);
            color: var(--text-secondary);
        }}

        .filter-no-active {{
            font-size: 0.75em;
            color: var(--text-muted);
            font-style: italic;
        }}

        /* Contract Selector */
        .contract-dropdown {{
            position: relative;
        }}

        .contract-dropdown-btn {{
            display: flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            background: rgba(244, 114, 182, 0.1);
            border: 1px solid rgba(244, 114, 182, 0.3);
            border-radius: 4px;
            color: var(--color-subscribe);
            font-family: var(--font-mono);
            font-size: 0.75em;
            cursor: pointer;
            transition: all 0.2s;
        }}

        .contract-dropdown-btn:hover {{
            background: rgba(244, 114, 182, 0.2);
        }}

        .contract-dropdown-menu {{
            position: absolute;
            top: 100%;
            left: 0;
            margin-top: 4px;
            background: var(--bg-panel);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            min-width: 250px;
            max-height: 200px;
            overflow-y: auto;
            z-index: 100;
            display: none;
        }}

        .contract-dropdown-menu.open {{
            display: block;
        }}

        .contract-dropdown-item {{
            padding: 10px 12px;
            cursor: pointer;
            border-bottom: 1px solid var(--border-color);
            transition: background 0.2s;
        }}

        .contract-dropdown-item:last-child {{
            border-bottom: none;
        }}

        .contract-dropdown-item:hover {{
            background: rgba(255, 255, 255, 0.05);
        }}

        .contract-dropdown-key {{
            font-family: var(--font-mono);
            font-size: 0.85em;
            color: var(--text-primary);
        }}

        .contract-dropdown-stats {{
            font-size: 0.7em;
            color: var(--text-muted);
            margin-top: 2px;
        }}

        /* Event type legend */
        .event-legend {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            margin-top: 12px;
            padding-top: 12px;
            border-top: 1px solid var(--border-color);
        }}

        .event-legend-item {{
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 0.7em;
            color: var(--text-muted);
        }}

        .event-legend-dot {{
            width: 8px;
            height: 8px;
            border-radius: 2px;
        }}

        .event-legend-dot.connect {{ background: var(--color-connect); }}
        .event-legend-dot.put {{ background: var(--color-put); }}
        .event-legend-dot.get {{ background: var(--color-get); }}
        .event-legend-dot.update {{ background: var(--color-update); }}
        .event-legend-dot.subscribe {{ background: var(--color-subscribe); }}

        /* Ring Visualization */
        .ring-container {{
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 480px;
            position: relative;
        }}

        /* Legend */
        .topology-legend {{
            position: absolute;
            bottom: 16px;
            left: 16px;
            display: flex;
            flex-direction: column;
            gap: 8px;
            font-family: var(--font-mono);
            font-size: 0.7em;
        }}

        .legend-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            color: var(--text-secondary);
        }}

        .legend-marker {{
            width: 12px;
            height: 12px;
            border-radius: 50%;
        }}

        .legend-marker.gateway {{
            background: #f59e0b;
            box-shadow: 0 0 8px rgba(245, 158, 11, 0.5);
        }}

        .legend-marker.you {{
            background: #10b981;
            box-shadow: 0 0 8px rgba(16, 185, 129, 0.5);
        }}

        .legend-marker.peer {{
            background: var(--accent-primary);
        }}

        .legend-marker.selected {{
            background: var(--accent-light);
            box-shadow: 0 0 8px rgba(126, 207, 239, 0.5);
        }}

        /* Contract selector */
        .contract-selector {{
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 12px;
        }}

        .contract-selector label {{
            font-family: var(--font-mono);
            font-size: 0.75em;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}

        .contract-select {{
            font-family: var(--font-mono);
            font-size: 0.8em;
            padding: 6px 12px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            color: var(--text-primary);
            cursor: pointer;
            min-width: 180px;
        }}

        .contract-select:focus {{
            outline: none;
            border-color: var(--color-subscribe);
        }}

        .contract-select option {{
            background: var(--bg-secondary);
        }}

        /* Subscription tree info */
        .subscription-info {{
            font-family: var(--font-mono);
            font-size: 0.7em;
            color: var(--color-subscribe);
            margin-left: auto;
        }}

        /* Subscription arrow styles */
        .subscription-arrow {{
            stroke: var(--color-subscribe);
            stroke-width: 2;
            fill: none;
            opacity: 0.7;
            marker-end: url(#arrowhead);
        }}

        .subscription-arrow.animated {{
            stroke-dasharray: 8 4;
            animation: flowPink 1s linear infinite;
        }}

        @keyframes flowPink {{
            from {{ stroke-dashoffset: 12; }}
            to {{ stroke-dashoffset: 0; }}
        }}

        /* Message flow arrow styles */
        .message-flow-arrow {{
            stroke-width: 3;
            fill: none;
            opacity: 0.9;
            stroke-linecap: round;
        }}

        .message-flow-arrow.connect {{ stroke: var(--color-connect); marker-end: url(#arrow-connect); }}
        .message-flow-arrow.put {{ stroke: var(--color-put); marker-end: url(#arrow-put); }}
        .message-flow-arrow.get {{ stroke: var(--color-get); marker-end: url(#arrow-get); }}
        .message-flow-arrow.update {{ stroke: var(--color-update); marker-end: url(#arrow-update); }}
        .message-flow-arrow.subscribe {{ stroke: var(--color-subscribe); marker-end: url(#arrow-subscribe); }}
        .message-flow-arrow.other {{ stroke: var(--text-muted); marker-end: url(#arrow-other); }}

        .message-flow-arrow.animated {{
            stroke-dasharray: 12 6;
            animation: flowMessage 0.8s linear infinite;
        }}

        @keyframes flowMessage {{
            from {{ stroke-dashoffset: 18; }}
            to {{ stroke-dashoffset: 0; }}
        }}

        .ring-container svg {{
            max-width: 100%;
            height: auto;
            filter: drop-shadow(0 0 40px rgba(0, 127, 255, 0.15));
        }}

        .peer-node {{
            transition: all 0.3s ease;
        }}

        .peer-node:hover {{
            filter: brightness(1.3);
        }}

        .peer-node.highlighted {{
            fill: #fbbf24 !important;
            filter: drop-shadow(0 0 8px rgba(251, 191, 36, 0.8));
        }}

        .peer-node.selected {{
            fill: #f87171 !important;
            filter: drop-shadow(0 0 12px rgba(248, 113, 113, 0.9));
        }}

        .connection-line {{
            stroke: var(--accent-primary);
            stroke-width: 1.5;
            opacity: 0.3;
            stroke-linecap: round;
        }}

        .connection-line.animated {{
            stroke-dasharray: 8 4;
            animation: flow 1.5s linear infinite;
        }}

        @keyframes flow {{
            from {{ stroke-dashoffset: 24; }}
            to {{ stroke-dashoffset: 0; }}
        }}

        /* Events Panel */
        .events-panel {{
            max-height: 420px;
            overflow-y: auto;
            scrollbar-width: thin;
            scrollbar-color: var(--border-color) transparent;
        }}

        .events-panel::-webkit-scrollbar {{
            width: 6px;
        }}

        .events-panel::-webkit-scrollbar-track {{
            background: transparent;
        }}

        .events-panel::-webkit-scrollbar-thumb {{
            background: var(--border-color);
            border-radius: 3px;
        }}

        .event-item {{
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 12px 16px;
            margin-bottom: 4px;
            background: var(--bg-tertiary);
            border-radius: 10px;
            font-size: 0.85em;
            transition: all 0.2s ease;
            border-left: 3px solid transparent;
        }}

        .event-item:hover {{
            background: rgba(255, 255, 255, 0.03);
            cursor: pointer;
        }}

        .event-item.selected {{
            background: rgba(0, 127, 255, 0.15);
            border-left: 3px solid var(--accent-primary);
        }}

        .event-item.in-transaction {{
            background: rgba(0, 127, 255, 0.08);
            border-left: 3px solid rgba(0, 127, 255, 0.3);
        }}

        .event-item.new {{
            animation: slideIn 0.3s ease-out;
        }}

        @keyframes slideIn {{
            from {{ opacity: 0; transform: translateX(-10px); }}
            to {{ opacity: 1; transform: translateX(0); }}
        }}

        .event-time {{
            font-family: var(--font-mono);
            font-size: 0.9em;
            color: var(--text-muted);
            min-width: 70px;
        }}

        .event-badge {{
            font-family: var(--font-mono);
            font-size: 0.75em;
            font-weight: 500;
            padding: 4px 10px;
            border-radius: 6px;
            text-transform: lowercase;
        }}

        .event-badge.connect {{
            background: rgba(34, 211, 238, 0.15);
            color: var(--color-connect);
            border-left-color: var(--color-connect);
        }}
        .event-badge.put {{
            background: rgba(251, 191, 36, 0.15);
            color: var(--color-put);
        }}
        .event-badge.get {{
            background: rgba(52, 211, 153, 0.15);
            color: var(--color-get);
        }}
        .event-badge.update {{
            background: rgba(167, 139, 250, 0.15);
            color: var(--color-update);
        }}
        .event-badge.subscribe {{
            background: rgba(244, 114, 182, 0.15);
            color: var(--color-subscribe);
        }}
        .event-badge.other {{
            background: rgba(139, 148, 158, 0.15);
            color: var(--text-secondary);
        }}

        .event-peer {{
            font-family: var(--font-mono);
            font-size: 0.85em;
            color: var(--text-secondary);
        }}

        .event-contract {{
            font-family: var(--font-mono);
            font-size: 0.75em;
            color: var(--text-muted);
            margin-left: auto;
        }}

        /* Timeline Container */
        .timeline-container {{
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: linear-gradient(to top, var(--bg-primary) 0%, var(--bg-primary) 60%, transparent 100%);
            padding: 30px 24px 20px;
            z-index: 100;
        }}

        .timeline-wrapper {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        .timeline-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }}

        .timeline-info {{
            display: flex;
            align-items: baseline;
            gap: 16px;
        }}

        .timeline-time {{
            font-family: var(--font-mono);
            font-size: 1.8em;
            font-weight: 300;
            color: var(--accent-primary);
            letter-spacing: -0.02em;
        }}

        .timeline-date {{
            font-family: var(--font-mono);
            font-size: 0.85em;
            color: var(--text-muted);
        }}

        .timeline-mode {{
            font-family: var(--font-mono);
            font-size: 0.8em;
            font-weight: 600;
            padding: 8px 20px;
            border-radius: 8px;
            cursor: pointer;
            border: none;
            transition: all 0.2s ease;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}

        .timeline-mode.live {{
            background: var(--accent-primary);
            color: var(--bg-primary);
            box-shadow: 0 0 20px var(--accent-glow);
        }}

        .timeline-mode.historical {{
            background: var(--color-put);
            color: var(--bg-primary);
            box-shadow: 0 0 20px rgba(251, 191, 36, 0.3);
        }}

        .timeline-mode:hover {{
            transform: scale(1.02);
        }}

        /* Timeline Track */
        .timeline {{
            position: relative;
            height: 100px;
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            overflow: hidden;
            cursor: crosshair;
        }}

        .timeline-track {{
            position: absolute;
            top: 60%;
            left: 16px;
            right: 16px;
            height: 2px;
            background: var(--border-color);
            transform: translateY(-50%);
        }}

        /* Timeline Ruler */
        .timeline-ruler {{
            position: absolute;
            top: 8px;
            left: 16px;
            right: 16px;
            height: 24px;
            display: flex;
            justify-content: space-between;
            pointer-events: none;
        }}

        .timeline-tick {{
            display: flex;
            flex-direction: column;
            align-items: center;
            font-family: var(--font-mono);
            font-size: 0.65em;
            color: var(--text-muted);
        }}

        .timeline-tick::after {{
            content: '';
            width: 1px;
            height: 8px;
            background: var(--border-color);
            margin-top: 2px;
        }}

        .timeline-tick.major::after {{
            height: 14px;
            background: var(--text-muted);
        }}

        .timeline-events {{
            position: absolute;
            top: 35px;
            left: 16px;
            right: 16px;
            bottom: 10px;
        }}

        .timeline-marker {{
            position: absolute;
            width: 4px;
            border-radius: 2px;
            transition: all 0.15s ease;
            cursor: pointer;
        }}

        .timeline-marker:hover {{
            transform: scaleX(2);
            z-index: 5;
        }}

        .timeline-marker.connect {{ background: var(--color-connect); }}
        .timeline-marker.put {{ background: var(--color-put); }}
        .timeline-marker.get {{ background: var(--color-get); }}
        .timeline-marker.update {{ background: var(--color-update); }}
        .timeline-marker.subscribe {{ background: var(--color-subscribe); }}
        .timeline-marker.other {{ background: var(--text-muted); }}

        /* Playhead - Time Window Range */
        .timeline-playhead {{
            position: absolute;
            top: 35px;
            bottom: 10px;
            min-width: 40px;
            background: rgba(255, 255, 255, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.3);
            border-radius: 6px;
            z-index: 10;
        }}

        .timeline-playhead::before {{
            content: '';
            position: absolute;
            top: 0;
            bottom: 0;
            left: 50%;
            width: 2px;
            background: var(--text-primary);
            transform: translateX(-50%);
            box-shadow: 0 0 8px rgba(255, 255, 255, 0.5);
        }}

        .timeline-playhead-handle {{
            position: absolute;
            top: -8px;
            left: 50%;
            transform: translateX(-50%);
            width: 20px;
            height: 16px;
            background: var(--text-primary);
            border-radius: 4px 4px 0 0;
            cursor: grab;
            display: flex;
            align-items: center;
            justify-content: center;
        }}

        .timeline-playhead-handle::after {{
            content: '⋮⋮';
            font-size: 8px;
            color: var(--bg-primary);
            letter-spacing: 1px;
        }}

        .timeline-playhead-label {{
            position: absolute;
            bottom: -20px;
            left: 50%;
            transform: translateX(-50%);
            font-family: var(--font-mono);
            font-size: 0.6em;
            color: var(--text-muted);
            white-space: nowrap;
        }}

        /* Resize handles on playhead edges */
        .timeline-resize-handle {{
            position: absolute;
            top: 0;
            bottom: 0;
            width: 16px;
            cursor: ew-resize;
            z-index: 20;
        }}

        .timeline-resize-handle::before {{
            content: '';
            position: absolute;
            top: 50%;
            transform: translateY(-50%);
            width: 4px;
            height: 20px;
            background: rgba(255, 255, 255, 0.6);
            border-radius: 2px;
        }}

        .timeline-resize-handle:hover::before {{
            background: var(--text-primary);
        }}

        .timeline-resize-handle.left {{
            left: -8px;
        }}

        .timeline-resize-handle.left::before {{
            left: 6px;
        }}

        .timeline-resize-handle.right {{
            right: -8px;
        }}

        .timeline-resize-handle.right::before {{
            right: 6px;
        }}

        .playhead-window-label {{
            position: absolute;
            bottom: -18px;
            left: 50%;
            transform: translateX(-50%);
            font-family: var(--font-mono);
            font-size: 0.6em;
            color: var(--text-muted);
            white-space: nowrap;
        }}

        .timeline-live-zone {{
            position: absolute;
            right: 0;
            top: 0;
            bottom: 0;
            width: 60px;
            background: linear-gradient(to right, transparent, rgba(0, 212, 170, 0.08));
            border-left: 1px dashed rgba(0, 212, 170, 0.3);
        }}

        .timeline-labels {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 10px;
            padding: 0 4px;
        }}

        .timeline-label {{
            font-family: var(--font-mono);
            font-size: 0.7em;
            color: var(--text-muted);
        }}

        .timeline-help {{
            font-family: var(--font-mono);
            font-size: 0.65em;
            color: var(--text-muted);
            opacity: 0.7;
        }}

        /* Empty state */
        .empty-state {{
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 40px;
            color: var(--text-muted);
            text-align: center;
        }}

        .empty-state-icon {{
            font-size: 2em;
            margin-bottom: 12px;
            opacity: 0.5;
        }}

        /* Keyboard shortcuts hint */
        .shortcuts-hint {{
            position: fixed;
            bottom: 200px;
            right: 24px;
            font-family: var(--font-mono);
            font-size: 0.7em;
            color: var(--text-muted);
            opacity: 0;
            transition: opacity 0.3s;
            pointer-events: none;
        }}

        body:hover .shortcuts-hint {{
            opacity: 0.5;
        }}

        .shortcuts-hint kbd {{
            display: inline-block;
            padding: 2px 6px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            margin: 0 2px;
        }}

        /* Transaction filter */
        .filter-input {{
            font-family: var(--font-mono);
            font-size: 0.8em;
            padding: 8px 12px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            color: var(--text-primary);
            width: 200px;
            transition: all 0.2s;
        }}

        .filter-input:focus {{
            outline: none;
            border-color: var(--accent-primary);
            box-shadow: 0 0 0 3px var(--accent-glow);
        }}

        .filter-input::placeholder {{
            color: var(--text-muted);
        }}
    </style>
</head>
<body>
    <div class="main-content">
        <div class="container">
            <header class="header">
                <div class="header-brand">
                    <img src="https://freenet.org/freenet_logo.svg" alt="Freenet" class="header-logo">
                    <h1>FREENET</h1>
                </div>
                <div class="status-badge">
                    <span id="status-dot" class="status-dot"></span>
                    <span id="status-text">Connecting...</span>
                </div>
            </header>

            <div class="stats">
                <div class="stat" title="Computers running Freenet that we can see">
                    <div class="stat-value" id="peer-count">-</div>
                    <div class="stat-label">Peers</div>
                    <div class="stat-hint">nodes on network</div>
                </div>
                <div class="stat" title="Direct links between peers">
                    <div class="stat-value" id="connection-count">-</div>
                    <div class="stat-label">Connections</div>
                    <div class="stat-hint">peer-to-peer links</div>
                </div>
                <div class="stat" title="Network operations observed">
                    <div class="stat-value" id="event-count">-</div>
                    <div class="stat-label">Events</div>
                    <div class="stat-hint">operations tracked</div>
                </div>
                <div class="stat" title="Current time being displayed">
                    <div class="stat-value" id="time-display">--:--</div>
                    <div class="stat-label">Viewing</div>
                    <div class="stat-hint">drag timeline to travel</div>
                </div>
            </div>

            <!-- Operation Stats -->
            <div class="op-stats-row" id="op-stats-row">
                <div class="op-stat" title="Store new data on the network">
                    <div class="op-stat-icon put">&#8593;</div>
                    <div class="op-stat-content">
                        <div class="op-stat-header">
                            <span class="op-stat-name">PUT</span>
                            <span class="op-stat-rate" id="put-rate">--%</span>
                        </div>
                        <div class="op-stat-details">
                            <span class="op-stat-detail"><span id="put-total">-</span> stored</span>
                            <span class="op-stat-detail"><span id="put-p50">-</span> median time</span>
                        </div>
                    </div>
                </div>
                <div class="op-stat" title="Retrieve data from the network">
                    <div class="op-stat-icon get">&#8595;</div>
                    <div class="op-stat-content">
                        <div class="op-stat-header">
                            <span class="op-stat-name">GET</span>
                            <span class="op-stat-rate" id="get-rate">--%</span>
                        </div>
                        <div class="op-stat-details">
                            <span class="op-stat-detail"><span id="get-total">-</span> retrieved</span>
                            <span class="op-stat-detail"><span id="get-miss">-</span> not found</span>
                        </div>
                    </div>
                </div>
                <div class="op-stat" title="Modify existing data and notify subscribers">
                    <div class="op-stat-icon update">&#8635;</div>
                    <div class="op-stat-content">
                        <div class="op-stat-header">
                            <span class="op-stat-name">UPDATE</span>
                            <span class="op-stat-rate" id="update-rate">--%</span>
                        </div>
                        <div class="op-stat-details">
                            <span class="op-stat-detail"><span id="update-total">-</span> updates</span>
                            <span class="op-stat-detail"><span id="update-bcast">-</span> notifications sent</span>
                        </div>
                    </div>
                </div>
                <div class="op-stat" title="Watch for changes to specific data">
                    <div class="op-stat-icon subscribe">&#9733;</div>
                    <div class="op-stat-content">
                        <div class="op-stat-header">
                            <span class="op-stat-name">SUBSCRIBE</span>
                            <span class="op-stat-rate" id="sub-rate"></span>
                        </div>
                        <div class="op-stat-details">
                            <span class="op-stat-detail"><span id="sub-total">-</span> watchers</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="dashboard">
                <div class="panel">
                    <div class="panel-header">
                        <span class="panel-title">Network Topology</span>
                    </div>
                    <p class="panel-subtitle">Peers arranged by their network location (0.0-1.0). Click a peer to filter events.</p>
                    <div class="ring-container" id="ring-container">
                        <div class="empty-state">
                            <div class="empty-state-icon">&#9673;</div>
                            <div>Loading network state...</div>
                        </div>
                        <div class="topology-legend" id="topology-legend" style="display: none;">
                            <div class="legend-item">
                                <span class="legend-marker gateway"></span>
                                <span>Gateway</span>
                            </div>
                            <div class="legend-item" id="legend-you" style="display: none;">
                                <span class="legend-marker you"></span>
                                <span>You (<span id="your-hash">---</span>)</span>
                            </div>
                            <div class="legend-item">
                                <span class="legend-marker peer"></span>
                                <span>Peer</span>
                            </div>
                            <div class="legend-item">
                                <span class="legend-marker selected"></span>
                                <span>Selected</span>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="panel">
                    <div class="panel-header">
                        <span class="panel-title">Events</span>
                        <div class="contract-dropdown" id="contract-dropdown">
                            <button class="contract-dropdown-btn" onclick="toggleContractDropdown()">
                                <span>&#9733;</span>
                                <span id="contract-btn-label">Contracts</span>
                                <span>&#9662;</span>
                            </button>
                            <div class="contract-dropdown-menu" id="contract-menu"></div>
                        </div>
                    </div>
                    <div class="filter-bar" id="filter-bar">
                        <span class="filter-bar-label">Filters:</span>
                        <span class="filter-no-active" id="no-filters">None active</span>
                        <div id="filter-chips"></div>
                        <input type="text" class="filter-input" id="filter-input" placeholder="Search..." style="flex: 1; min-width: 80px;">
                        <button class="filter-clear-all" id="clear-all-btn" onclick="clearAllFilters()" style="display: none;">Clear all</button>
                    </div>
                    <div class="events-panel" id="events-panel">
                        <div class="empty-state">
                            <div class="empty-state-icon">&#8987;</div>
                            <div>Waiting for events...</div>
                        </div>
                    </div>
                    <div class="event-legend">
                        <span class="event-legend-item"><span class="event-legend-dot connect"></span> connect</span>
                        <span class="event-legend-item"><span class="event-legend-dot put"></span> store</span>
                        <span class="event-legend-item"><span class="event-legend-dot get"></span> get</span>
                        <span class="event-legend-item"><span class="event-legend-dot update"></span> update</span>
                        <span class="event-legend-item"><span class="event-legend-dot subscribe"></span> subscribe</span>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Timeline -->
    <div class="timeline-container">
        <div class="timeline-wrapper">
            <div class="timeline-header">
                <div class="timeline-info">
                    <span class="timeline-time" id="playhead-time">--:--:--</span>
                    <span class="timeline-date" id="playhead-date"></span>
                </div>
                <button class="timeline-mode live" id="mode-button" onclick="goLive()">LIVE</button>
            </div>
            <div class="timeline" id="timeline">
                <div class="timeline-ruler" id="timeline-ruler"></div>
                <div class="timeline-track"></div>
                <div class="timeline-events" id="timeline-events"></div>
                <div class="timeline-live-zone"></div>
                <div class="timeline-playhead" id="playhead">
                    <div class="timeline-resize-handle left" id="resize-left"></div>
                    <div class="timeline-playhead-handle"></div>
                    <div class="timeline-resize-handle right" id="resize-right"></div>
                    <div class="playhead-window-label" id="window-label">10 min</div>
                </div>
            </div>
            <div class="timeline-labels">
                <span class="timeline-label" id="timeline-start">--:--</span>
                <span class="timeline-help">Drag edges to resize window, drag center to scrub</span>
                <span class="timeline-label" id="timeline-end">Now</span>
            </div>
        </div>
    </div>

    <div class="shortcuts-hint">
        <kbd>&#8592;</kbd><kbd>&#8594;</kbd> Step &nbsp; <kbd>L</kbd> Go Live
    </div>

    <script>
        // State
        const allEvents = [];
        let timeRange = {{ start: 0, end: 0 }};
        let isLive = true;
        let currentTime = Date.now() * 1_000_000;
        let ws = null;
        let reconnectTimeout = null;
        let isDragging = false;
        let filterText = '';
        let selectedEvent = null;
        let highlightedPeers = new Set();
        let selectedPeerId = null;  // For filtering events by peer
        let gatewayPeerId = null;   // Gateway peer ID
        let yourPeerId = null;      // User's own peer ID
        let yourIpHash = null;      // User's IP hash
        let subscriptionData = {{}};  // contract_key -> subscription data
        let selectedContract = null; // Currently selected contract for subscription view
        let opStats = null;  // Operation statistics
        let displayedEvents = [];  // Events currently shown in the events panel

        const SVG_SIZE = 450;
        const CENTER = SVG_SIZE / 2;
        const RADIUS = 175;

        function getEventClass(eventType) {{
            if (!eventType) return 'other';
            if (eventType.includes('connect')) return 'connect';
            if (eventType.includes('put')) return 'put';
            if (eventType.includes('get')) return 'get';
            if (eventType.includes('update')) return 'update';
            if (eventType.includes('subscrib')) return 'subscribe';
            return 'other';
        }}

        function locationToXY(location) {{
            const angle = location * 2 * Math.PI - Math.PI / 2;
            return {{ x: CENTER + RADIUS * Math.cos(angle), y: CENTER + RADIUS * Math.sin(angle) }};
        }}

        function formatTime(tsNano) {{
            return new Date(tsNano / 1_000_000).toLocaleTimeString();
        }}

        function formatDate(tsNano) {{
            return new Date(tsNano / 1_000_000).toLocaleDateString(undefined, {{
                month: 'short', day: 'numeric'
            }});
        }}

        function renderRuler() {{
            const ruler = document.getElementById('timeline-ruler');
            ruler.innerHTML = '';

            if (timeRange.end <= timeRange.start) return;

            const duration = timeRange.end - timeRange.start;
            const durationMs = duration / 1_000_000;
            const durationMin = durationMs / 60000;

            // Determine appropriate tick interval
            let tickInterval;
            if (durationMin <= 10) tickInterval = 60000;       // 1 min
            else if (durationMin <= 30) tickInterval = 300000;  // 5 min
            else if (durationMin <= 60) tickInterval = 600000;  // 10 min
            else if (durationMin <= 120) tickInterval = 900000; // 15 min
            else tickInterval = 1800000;                        // 30 min

            const startMs = Math.ceil((timeRange.start / 1_000_000) / tickInterval) * tickInterval;
            const endMs = timeRange.end / 1_000_000;

            for (let ms = startMs; ms <= endMs; ms += tickInterval) {{
                const pos = ((ms * 1_000_000) - timeRange.start) / duration;
                if (pos < 0 || pos > 1) continue;

                const tick = document.createElement('div');
                tick.className = 'timeline-tick' + ((ms % (tickInterval * 2) === 0) ? ' major' : '');
                tick.style.position = 'absolute';
                tick.style.left = `${{pos * 100}}%`;
                tick.style.transform = 'translateX(-50%)';

                const time = new Date(ms);
                tick.textContent = time.toLocaleTimeString([], {{hour: '2-digit', minute:'2-digit'}});

                ruler.appendChild(tick);
            }}
        }}

        function selectEvent(event) {{
            selectedEvent = event;
            highlightedPeers.clear();

            if (event) {{
                // Move playhead to event time
                goToTime(event.timestamp);

                // Highlight the event's peer
                if (event.peer_id) {{
                    highlightedPeers.add(event.peer_id);
                }}

                // Also highlight connection peers
                if (event.connection) {{
                    highlightedPeers.add(event.connection[0]);
                    highlightedPeers.add(event.connection[1]);
                }}
            }}

            updateView();
        }}

        function selectPeer(peerId) {{
            if (selectedPeerId === peerId) {{
                // Clicking same peer clears selection
                selectedPeerId = null;
            }} else {{
                selectedPeerId = peerId;
            }}
            updateFilterBar();
            updateView();
        }}

        function clearPeerSelection() {{
            selectedPeerId = null;
            document.getElementById('filter-input').placeholder = 'Filter by tx...';
            updateView();
        }}

        let contractDropdownOpen = false;

        function toggleContractDropdown() {{
            contractDropdownOpen = !contractDropdownOpen;
            document.getElementById('contract-menu').classList.toggle('open', contractDropdownOpen);
        }}

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {{
            if (!e.target.closest('#contract-dropdown') && contractDropdownOpen) {{
                contractDropdownOpen = false;
                document.getElementById('contract-menu').classList.remove('open');
            }}
        }});

        function selectContract(contractKey) {{
            // Toggle selection if clicking the same contract
            if (selectedContract === contractKey) {{
                selectedContract = null;
            }} else {{
                selectedContract = contractKey || null;
            }}

            // Close dropdown
            contractDropdownOpen = false;
            document.getElementById('contract-menu').classList.remove('open');

            // Update filter bar
            updateFilterBar();
            updateView();
        }}

        function updateFilterBar() {{
            const chipsContainer = document.getElementById('filter-chips');
            const noFilters = document.getElementById('no-filters');
            const clearAllBtn = document.getElementById('clear-all-btn');

            let chips = [];

            if (selectedPeerId) {{
                chips.push(`<span class="filter-chip peer">Peer: ${{selectedPeerId.substring(0, 12)}}...<button class="filter-chip-close" onclick="clearPeerFilter()">×</button></span>`);
            }}

            if (selectedContract && subscriptionData[selectedContract]) {{
                const shortKey = subscriptionData[selectedContract].short_key;
                chips.push(`<span class="filter-chip contract">Contract: ${{shortKey}}<button class="filter-chip-close" onclick="clearContractFilter()">×</button></span>`);
            }}

            chipsContainer.innerHTML = chips.join('');

            const hasFilters = chips.length > 0 || filterText;
            noFilters.style.display = hasFilters ? 'none' : 'inline';
            clearAllBtn.style.display = hasFilters ? 'inline-block' : 'none';
        }}

        function clearPeerFilter() {{
            selectedPeerId = null;
            updateFilterBar();
            updateView();
        }}

        function clearContractFilter() {{
            selectedContract = null;
            updateFilterBar();
            updateView();
        }}

        function clearAllFilters() {{
            selectedPeerId = null;
            selectedContract = null;
            filterText = '';
            document.getElementById('filter-input').value = '';
            updateFilterBar();
            updateView();
        }}

        function formatLatency(ms) {{
            if (ms === null || ms === undefined) return '-';
            if (ms < 1000) return Math.round(ms) + 'ms';
            return (ms / 1000).toFixed(1) + 's';
        }}

        function getRateClass(rate) {{
            if (rate === null || rate === undefined) return '';
            if (rate >= 90) return 'good';
            if (rate >= 50) return 'warn';
            return 'bad';
        }}

        function updateOpStats() {{
            if (!opStats) return;

            // PUT
            const put = opStats.put;
            document.getElementById('put-rate').textContent = put.success_rate !== null ? put.success_rate + '%' : '-';
            document.getElementById('put-rate').className = 'op-stat-rate ' + getRateClass(put.success_rate);
            document.getElementById('put-total').textContent = put.total || 0;
            document.getElementById('put-p50').textContent = formatLatency(put.latency?.p50);

            // GET
            const get = opStats.get;
            document.getElementById('get-rate').textContent = get.success_rate !== null ? get.success_rate + '%' : '-';
            document.getElementById('get-rate').className = 'op-stat-rate ' + getRateClass(get.success_rate);
            document.getElementById('get-total').textContent = get.total || 0;
            document.getElementById('get-miss').textContent = get.not_found || 0;

            // UPDATE
            const update = opStats.update;
            document.getElementById('update-rate').textContent = update.success_rate !== null ? update.success_rate + '%' : '-';
            document.getElementById('update-rate').className = 'op-stat-rate ' + getRateClass(update.success_rate);
            document.getElementById('update-total').textContent = update.total || 0;
            document.getElementById('update-bcast').textContent = update.broadcasts || 0;

            // SUBSCRIBE
            const sub = opStats.subscribe;
            document.getElementById('sub-total').textContent = sub.total || 0;
        }}

        function updateContractDropdown() {{
            const menu = document.getElementById('contract-menu');
            const btnLabel = document.getElementById('contract-btn-label');
            const contracts = Object.keys(subscriptionData);

            if (contracts.length === 0) {{
                menu.innerHTML = '<div class="contract-dropdown-item" style="color: var(--text-muted);">No contracts</div>';
                btnLabel.textContent = 'Contracts (0)';
                return;
            }}

            btnLabel.textContent = `Contracts (${{contracts.length}})`;

            menu.innerHTML = contracts.map(key => {{
                const data = subscriptionData[key];
                const treeSize = Object.keys(data.tree || {{}}).length;

                return `
                    <div class="contract-dropdown-item" onclick="selectContract('${{key}}')">
                        <div class="contract-dropdown-key">${{data.short_key}}</div>
                        <div class="contract-dropdown-stats">${{data.subscribers.length}} subscribers &middot; ${{treeSize}} paths</div>
                    </div>
                `;
            }}).join('');
        }}

        function reconstructStateAtTime(targetTime) {{
            const peers = new Map();
            const connections = new Set();

            for (const event of allEvents) {{
                if (event.timestamp > targetTime) break;

                if (event.peer_id && event.location !== undefined) {{
                    peers.set(event.peer_id, {{
                        location: event.location,
                        ip_hash: event.peer_ip_hash
                    }});
                }}

                if (event.connection) {{
                    const key = [event.connection[0], event.connection[1]].sort().join('|');
                    connections.add(key);
                }}
            }}

            return {{ peers, connections }};
        }}

        function updateRingSVG(peers, connections, subscriberPeerIds = new Set()) {{
            const container = document.getElementById('ring-container');
            const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            svg.setAttribute('viewBox', `0 0 ${{SVG_SIZE}} ${{SVG_SIZE}}`);
            svg.setAttribute('width', SVG_SIZE);
            svg.setAttribute('height', SVG_SIZE);

            // Defs for glow effect and arrow markers
            const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
            defs.innerHTML = `
                <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
                    <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
                    <feMerge>
                        <feMergeNode in="coloredBlur"/>
                        <feMergeNode in="SourceGraphic"/>
                    </feMerge>
                </filter>
                <linearGradient id="ringGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" style="stop-color:#1a2a2a;stop-opacity:1" />
                    <stop offset="100%" style="stop-color:#0a1515;stop-opacity:1" />
                </linearGradient>
                <marker id="arrow-connect" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                    <polygon points="0 0, 8 3, 0 6" fill="#7ecfef"/>
                </marker>
                <marker id="arrow-put" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                    <polygon points="0 0, 8 3, 0 6" fill="#fbbf24"/>
                </marker>
                <marker id="arrow-get" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                    <polygon points="0 0, 8 3, 0 6" fill="#34d399"/>
                </marker>
                <marker id="arrow-update" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                    <polygon points="0 0, 8 3, 0 6" fill="#a78bfa"/>
                </marker>
                <marker id="arrow-subscribe" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                    <polygon points="0 0, 8 3, 0 6" fill="#f472b6"/>
                </marker>
                <marker id="arrow-other" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                    <polygon points="0 0, 8 3, 0 6" fill="#8b949e"/>
                </marker>
            `;
            svg.appendChild(defs);

            // Outer glow ring
            const glowRing = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            glowRing.setAttribute('cx', CENTER);
            glowRing.setAttribute('cy', CENTER);
            glowRing.setAttribute('r', RADIUS + 5);
            glowRing.setAttribute('fill', 'none');
            glowRing.setAttribute('stroke', 'rgba(0, 212, 170, 0.1)');
            glowRing.setAttribute('stroke-width', '20');
            svg.appendChild(glowRing);

            // Background ring
            const ring = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            ring.setAttribute('cx', CENTER);
            ring.setAttribute('cy', CENTER);
            ring.setAttribute('r', RADIUS);
            ring.setAttribute('fill', 'none');
            ring.setAttribute('stroke', '#1a2a2a');
            ring.setAttribute('stroke-width', '3');
            svg.appendChild(ring);

            // Inner reference circles
            [0.6, 0.3].forEach((scale, i) => {{
                const inner = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                inner.setAttribute('cx', CENTER);
                inner.setAttribute('cy', CENTER);
                inner.setAttribute('r', RADIUS * scale);
                inner.setAttribute('fill', 'none');
                inner.setAttribute('stroke', 'rgba(255,255,255,0.03)');
                inner.setAttribute('stroke-width', '1');
                inner.setAttribute('stroke-dasharray', '4,8');
                svg.appendChild(inner);
            }});

            // Location markers
            [0, 0.25, 0.5, 0.75].forEach(loc => {{
                const angle = loc * 2 * Math.PI - Math.PI / 2;
                const x = CENTER + (RADIUS + 25) * Math.cos(angle);
                const y = CENTER + (RADIUS + 25) * Math.sin(angle);

                const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                text.setAttribute('x', x);
                text.setAttribute('y', y);
                text.setAttribute('fill', '#484f58');
                text.setAttribute('font-size', '12');
                text.setAttribute('font-family', 'JetBrains Mono, monospace');
                text.setAttribute('text-anchor', 'middle');
                text.setAttribute('dominant-baseline', 'middle');
                text.textContent = loc.toFixed(2);
                svg.appendChild(text);
            }});

            // Draw connections
            connections.forEach(connKey => {{
                const [id1, id2] = connKey.split('|');
                const peer1 = peers.get(id1);
                const peer2 = peers.get(id2);
                if (peer1 && peer2) {{
                    const pos1 = locationToXY(peer1.location);
                    const pos2 = locationToXY(peer2.location);
                    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                    line.setAttribute('x1', pos1.x);
                    line.setAttribute('y1', pos1.y);
                    line.setAttribute('x2', pos2.x);
                    line.setAttribute('y2', pos2.y);
                    line.setAttribute('class', 'connection-line animated');
                    svg.appendChild(line);
                }}
            }});

            // Draw peers
            peers.forEach((peer, id) => {{
                const pos = locationToXY(peer.location);
                const isHighlighted = highlightedPeers.has(id);
                const isEventSelected = selectedEvent && selectedEvent.peer_id === id;
                const isPeerSelected = selectedPeerId === id;
                const isGateway = id === gatewayPeerId;
                const isYou = id === yourPeerId;
                const isSubscriber = subscriberPeerIds.has(id);

                // Determine colors based on peer type
                let fillColor = '#007FFF';  // Default peer blue
                let glowColor = 'rgba(0, 127, 255, 0.2)';
                let label = '';

                if (isGateway) {{
                    fillColor = '#f59e0b';  // Amber for gateway
                    glowColor = 'rgba(245, 158, 11, 0.3)';
                    label = 'GW';
                }} else if (isYou) {{
                    fillColor = '#10b981';  // Emerald for you
                    glowColor = 'rgba(16, 185, 129, 0.3)';
                    label = 'YOU';
                }} else if (isSubscriber) {{
                    fillColor = '#f472b6';  // Pink for subscriber
                    glowColor = 'rgba(244, 114, 182, 0.3)';
                }}

                // Override colors for selection states
                if (isEventSelected) {{
                    fillColor = '#f87171';
                    glowColor = 'rgba(248, 113, 113, 0.3)';
                }} else if (isPeerSelected) {{
                    fillColor = '#7ecfef';
                    glowColor = 'rgba(126, 207, 239, 0.4)';
                }} else if (isHighlighted) {{
                    fillColor = '#fbbf24';
                    glowColor = 'rgba(251, 191, 36, 0.3)';
                }}

                const nodeSize = (isHighlighted || isPeerSelected || isGateway || isYou || isSubscriber) ? 10 : 8;
                const glowSize = (isHighlighted || isPeerSelected || isGateway || isYou) ? 18 : 14;

                // Outer glow
                const glow = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                glow.setAttribute('cx', pos.x);
                glow.setAttribute('cy', pos.y);
                glow.setAttribute('r', glowSize);
                glow.setAttribute('fill', glowColor);
                svg.appendChild(glow);

                // Click target (larger invisible circle for easier clicking)
                const clickTarget = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                clickTarget.setAttribute('cx', pos.x);
                clickTarget.setAttribute('cy', pos.y);
                clickTarget.setAttribute('r', '20');
                clickTarget.setAttribute('fill', 'transparent');
                clickTarget.setAttribute('style', 'cursor: pointer;');
                clickTarget.onclick = () => selectPeer(id);
                svg.appendChild(clickTarget);

                // Main circle
                const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                circle.setAttribute('cx', pos.x);
                circle.setAttribute('cy', pos.y);
                circle.setAttribute('r', nodeSize);
                circle.setAttribute('fill', fillColor);
                circle.setAttribute('class', 'peer-node');
                circle.setAttribute('filter', 'url(#glow)');
                circle.setAttribute('style', 'pointer-events: none;');

                const peerType = isGateway ? ' (Gateway)' : isYou ? ' (You)' : '';
                const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
                title.textContent = `${{id}}${{peerType}}\\n#${{peer.ip_hash || ''}}\\nLocation: ${{peer.location.toFixed(4)}}\\nClick to filter events`;
                clickTarget.appendChild(title);
                svg.appendChild(circle);

                // Add label for gateway/you
                if (label && peers.size <= 15) {{
                    const labelText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                    labelText.setAttribute('x', pos.x);
                    labelText.setAttribute('y', pos.y + 24);
                    labelText.setAttribute('fill', fillColor);
                    labelText.setAttribute('font-size', '9');
                    labelText.setAttribute('font-family', 'JetBrains Mono, monospace');
                    labelText.setAttribute('font-weight', '600');
                    labelText.setAttribute('text-anchor', 'middle');
                    labelText.textContent = label;
                    svg.appendChild(labelText);
                }}

                // Label
                if (peers.size <= 12) {{
                    const angle = peer.location * 2 * Math.PI - Math.PI / 2;
                    const labelRadius = RADIUS - 30;
                    const lx = CENTER + labelRadius * Math.cos(angle);
                    const ly = CENTER + labelRadius * Math.sin(angle);

                    const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                    text.setAttribute('x', lx);
                    text.setAttribute('y', ly);
                    text.setAttribute('fill', '#8b949e');
                    text.setAttribute('font-size', '10');
                    text.setAttribute('font-family', 'JetBrains Mono, monospace');
                    text.setAttribute('text-anchor', 'middle');
                    text.setAttribute('dominant-baseline', 'middle');
                    text.textContent = `#${{peer.ip_hash || id.substring(5, 11)}}`;
                    svg.appendChild(text);
                }}
            }});

            // Center stats
            const centerGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');

            const countText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            countText.setAttribute('x', CENTER);
            countText.setAttribute('y', CENTER - 8);
            countText.setAttribute('fill', '#00d4aa');
            countText.setAttribute('font-size', '36');
            countText.setAttribute('font-family', 'JetBrains Mono, monospace');
            countText.setAttribute('font-weight', '300');
            countText.setAttribute('text-anchor', 'middle');
            countText.textContent = peers.size;
            centerGroup.appendChild(countText);

            const labelText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            labelText.setAttribute('x', CENTER);
            labelText.setAttribute('y', CENTER + 18);
            labelText.setAttribute('fill', '#484f58');
            labelText.setAttribute('font-size', '11');
            labelText.setAttribute('font-family', 'JetBrains Mono, monospace');
            labelText.setAttribute('text-anchor', 'middle');
            labelText.setAttribute('text-transform', 'uppercase');
            labelText.setAttribute('letter-spacing', '2');
            labelText.textContent = selectedContract ? 'SUBSCRIBERS' : 'PEERS';
            centerGroup.appendChild(labelText);

            svg.appendChild(centerGroup);

            // Draw message flow arrows for displayed events (only in event mode, not subscription mode)
            if (!selectedContract && displayedEvents && displayedEvents.length > 0) {{
                displayedEvents.forEach((event, idx) => {{
                    // Need both from and to peer with locations
                    if (!event.from_peer || !event.to_peer) return;
                    if (event.from_peer === event.to_peer) return;

                    const fromPeer = peers.get(event.from_peer);
                    const toPeer = peers.get(event.to_peer);

                    // Use location from event if peer not in current state
                    const fromLoc = fromPeer?.location ?? event.from_location;
                    const toLoc = toPeer?.location ?? event.to_location;

                    if (fromLoc === null || fromLoc === undefined || toLoc === null || toLoc === undefined) return;

                    const fromPos = locationToXY(fromLoc);
                    const toPos = locationToXY(toLoc);

                    // Calculate shorter line (don't overlap nodes)
                    const dx = toPos.x - fromPos.x;
                    const dy = toPos.y - fromPos.y;
                    const dist = Math.sqrt(dx*dx + dy*dy);
                    if (dist < 20) return;  // Too close

                    const offsetStart = 15 / dist;
                    const offsetEnd = 22 / dist;

                    const x1 = fromPos.x + dx * offsetStart;
                    const y1 = fromPos.y + dy * offsetStart;
                    const x2 = fromPos.x + dx * (1 - offsetEnd);
                    const y2 = fromPos.y + dy * (1 - offsetEnd);

                    const eventClass = getEventClass(event.event_type);
                    const isSelected = selectedEvent && selectedEvent.timestamp === event.timestamp;

                    const arrow = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                    arrow.setAttribute('x1', x1);
                    arrow.setAttribute('y1', y1);
                    arrow.setAttribute('x2', x2);
                    arrow.setAttribute('y2', y2);
                    arrow.setAttribute('class', `message-flow-arrow ${{eventClass}} animated`);
                    arrow.setAttribute('style', `opacity: ${{isSelected ? 1 : 0.6 - idx * 0.05}}`);
                    svg.appendChild(arrow);
                }});
            }}

            // Draw subscription tree arrows if a contract is selected
            if (selectedContract && subscriptionData[selectedContract]) {{
                const subData = subscriptionData[selectedContract];
                const tree = subData.tree;

                // Add arrowhead marker definition
                const marker = document.createElementNS('http://www.w3.org/2000/svg', 'marker');
                marker.setAttribute('id', 'arrowhead');
                marker.setAttribute('markerWidth', '10');
                marker.setAttribute('markerHeight', '7');
                marker.setAttribute('refX', '9');
                marker.setAttribute('refY', '3.5');
                marker.setAttribute('orient', 'auto');
                const arrowPath = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
                arrowPath.setAttribute('points', '0 0, 10 3.5, 0 7');
                arrowPath.setAttribute('fill', '#f472b6');
                marker.appendChild(arrowPath);
                defs.appendChild(marker);

                // Draw arrows for each edge in the tree
                Object.entries(tree).forEach(([fromId, toIds]) => {{
                    const fromPeer = peers.get(fromId);
                    if (!fromPeer) return;

                    toIds.forEach(toId => {{
                        const toPeer = peers.get(toId);
                        if (!toPeer) return;

                        const fromPos = locationToXY(fromPeer.location);
                        const toPos = locationToXY(toPeer.location);

                        // Calculate shorter line (don't overlap nodes)
                        const dx = toPos.x - fromPos.x;
                        const dy = toPos.y - fromPos.y;
                        const dist = Math.sqrt(dx*dx + dy*dy);
                        const offsetStart = 15 / dist;
                        const offsetEnd = 20 / dist;

                        const x1 = fromPos.x + dx * offsetStart;
                        const y1 = fromPos.y + dy * offsetStart;
                        const x2 = fromPos.x + dx * (1 - offsetEnd);
                        const y2 = fromPos.y + dy * (1 - offsetEnd);

                        const arrow = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                        arrow.setAttribute('x1', x1);
                        arrow.setAttribute('y1', y1);
                        arrow.setAttribute('x2', x2);
                        arrow.setAttribute('y2', y2);
                        arrow.setAttribute('class', 'subscription-arrow animated');
                        arrow.setAttribute('marker-end', 'url(#arrowhead)');
                        svg.appendChild(arrow);
                    }});
                }});
            }}

            container.innerHTML = '';
            container.appendChild(svg);
        }}

        function renderTimeline() {{
            const container = document.getElementById('timeline-events');
            container.innerHTML = '';

            if (allEvents.length === 0 || timeRange.end <= timeRange.start) return;

            const duration = timeRange.end - timeRange.start;

            // Group events by position to vary heights
            const eventsByPos = {{}};
            allEvents.forEach(event => {{
                const pos = Math.round((event.timestamp - timeRange.start) / duration * 1000);
                if (!eventsByPos[pos]) eventsByPos[pos] = [];
                eventsByPos[pos].push(event);
            }});

            Object.entries(eventsByPos).forEach(([pos, events]) => {{
                const posPercent = pos / 10;
                if (posPercent < 0 || posPercent > 100) return;

                // Stack events at same position
                events.forEach((event, idx) => {{
                    const marker = document.createElement('div');
                    marker.className = `timeline-marker ${{getEventClass(event.event_type)}}`;
                    marker.style.left = `${{posPercent}}%`;

                    // Vary height based on density
                    const height = Math.min(40, 15 + idx * 8);
                    marker.style.height = `${{height}}px`;
                    marker.style.top = `${{40 - height/2}}px`;
                    marker.style.opacity = 0.5 + Math.min(0.5, events.length * 0.1);

                    marker.title = `${{event.time_str}} - ${{event.event_type}}`;
                    marker.onclick = (e) => {{
                        e.stopPropagation();
                        goToTime(event.timestamp);
                    }};
                    container.appendChild(marker);
                }});
            }});

            document.getElementById('timeline-start').textContent = formatTime(timeRange.start);
        }}

        // Time window for events display (default 5 minutes each side = 10 min total)
        let timeWindowNs = 5 * 60 * 1_000_000_000;
        const MIN_TIME_WINDOW_NS = 1 * 60 * 1_000_000_000;  // 1 minute minimum
        const MAX_TIME_WINDOW_NS = 60 * 60 * 1_000_000_000; // 60 minutes maximum
        const MIN_PLAYHEAD_WIDTH_PX = 40;

        function updatePlayhead() {{
            if (timeRange.end <= timeRange.start) return;

            const duration = timeRange.end - timeRange.start;
            const timeline = document.getElementById('timeline');
            const timelineWidth = timeline.offsetWidth - 32; // Account for padding

            // Calculate window width as percentage
            const windowDuration = timeWindowNs * 2; // total window size
            let windowWidthPercent = (windowDuration / duration) * 100;

            // Ensure minimum width
            const minWidthPercent = (MIN_PLAYHEAD_WIDTH_PX / timelineWidth) * 100;
            windowWidthPercent = Math.max(windowWidthPercent, minWidthPercent);

            // Cap at 100%
            windowWidthPercent = Math.min(windowWidthPercent, 100);

            // Calculate center position
            const centerPos = (currentTime - timeRange.start) / duration;
            const clampedCenter = Math.min(Math.max(centerPos, 0), 1);

            // Calculate left edge (center - half width)
            let leftPos = clampedCenter * 100 - windowWidthPercent / 2;
            leftPos = Math.max(0, Math.min(leftPos, 100 - windowWidthPercent));

            const playhead = document.getElementById('playhead');
            playhead.style.left = `calc(${{leftPos}}% + 16px)`;
            playhead.style.width = `${{windowWidthPercent}}%`;

            document.getElementById('playhead-time').textContent = formatTime(currentTime);
            document.getElementById('playhead-date').textContent = formatDate(currentTime);
            document.getElementById('time-display').textContent = formatTime(currentTime).split(' ')[0];
        }}

        function updateView() {{
            // Always show time-windowed peers
            const {{ peers, connections }} = reconstructStateAtTime(currentTime);

            // Get subscription subscribers for highlighting (if contract selected)
            let subscriberPeerIds = new Set();
            if (selectedContract && subscriptionData[selectedContract]) {{
                subscriberPeerIds = new Set(subscriptionData[selectedContract].subscribers);
            }}

            updateRingSVG(peers, connections, subscriberPeerIds);

            document.getElementById('peer-count').textContent = peers.size;
            document.getElementById('connection-count').textContent = connections.size;

            // Update topology subtitle
            const topoSubtitle = document.querySelector('.panel-subtitle');
            if (selectedContract && subscriptionData[selectedContract]) {{
                const subData = subscriptionData[selectedContract];
                const visibleSubs = [...subscriberPeerIds].filter(id => peers.has(id)).length;
                topoSubtitle.textContent = `${{visibleSubs}}/${{subData.subscribers.length}} subscribers visible. Pink arrows show broadcast tree.`;
            }} else {{
                topoSubtitle.textContent = 'Peers arranged by their network location (0.0-1.0). Click a peer to filter events.';
            }}

            // Filter events within the time window
            let nearbyEvents = allEvents.filter(e =>
                Math.abs(e.timestamp - currentTime) < timeWindowNs
            );

            // Filter by selected peer
            if (selectedPeerId) {{
                nearbyEvents = nearbyEvents.filter(e =>
                    e.peer_id === selectedPeerId ||
                    (e.connection && (e.connection[0] === selectedPeerId || e.connection[1] === selectedPeerId))
                );
            }}

            // Filter by text input
            if (filterText) {{
                const filter = filterText.toLowerCase();
                nearbyEvents = nearbyEvents.filter(e =>
                    (e.event_type && e.event_type.toLowerCase().includes(filter)) ||
                    (e.peer_id && e.peer_id.toLowerCase().includes(filter)) ||
                    (e.contract && e.contract.toLowerCase().includes(filter))
                );
            }}

            // Filter by selected contract (show only subscribe/update events for this contract)
            if (selectedContract) {{
                nearbyEvents = nearbyEvents.filter(e =>
                    e.contract_full === selectedContract &&
                    (e.event_type.includes('subscribe') || e.event_type.includes('update') || e.event_type.includes('broadcast'))
                );
            }}

            nearbyEvents = nearbyEvents.slice(-30);

            // Update events title based on filtering
            const eventsTitle = document.getElementById('events-title');
            if (selectedContract && subscriptionData[selectedContract]) {{
                eventsTitle.textContent = `Events for ${{subscriptionData[selectedContract].short_key}}`;
            }} else if (selectedPeerId) {{
                eventsTitle.textContent = `Events for ${{selectedPeerId.substring(0, 12)}}...`;
            }} else {{
                eventsTitle.textContent = 'Events';
            }}

            const eventsPanel = document.getElementById('events-panel');
            if (nearbyEvents.length === 0) {{
                displayedEvents = [];
                const emptyMsg = selectedContract
                    ? 'No subscription events in this time range'
                    : 'No events in this time range';
                eventsPanel.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-state-icon">&#8709;</div>
                        <div>${{emptyMsg}}</div>
                    </div>
                `;
            }} else {{
                eventsPanel.innerHTML = nearbyEvents.map((e, idx) => {{
                    const isSelected = selectedEvent && selectedEvent.timestamp === e.timestamp && selectedEvent.peer_id === e.peer_id;
                    const classes = ['event-item'];
                    if (isSelected) classes.push('selected');
                    return `
                        <div class="${{classes.join(' ')}}" data-event-idx="${{idx}}" onclick="handleEventClick(${{idx}})">
                            <span class="event-time">${{e.time_str}}</span>
                            <span class="event-badge ${{getEventClass(e.event_type)}}">${{e.event_type}}</span>
                            <span class="event-peer">${{e.peer_id}}</span>
                            ${{e.contract ? `<span class="event-contract">${{e.contract}}</span>` : ''}}
                        </div>
                    `;
                }}).reverse().join('');

                // Store events for click handler and topology visualization
                displayedEvents = nearbyEvents;
            }}

            document.getElementById('event-count').textContent = allEvents.filter(e => e.timestamp <= currentTime).length;
            updatePlayhead();
        }}

        function handleEventClick(idx) {{
            if (displayedEvents && displayedEvents[idx]) {{
                selectEvent(displayedEvents[idx]);
            }}
        }}

        function goLive() {{
            isLive = true;
            currentTime = Date.now() * 1_000_000;
            selectedEvent = null;
            selectedPeerId = null;
            highlightedPeers.clear();
            document.getElementById('mode-button').className = 'timeline-mode live';
            document.getElementById('mode-button').textContent = 'LIVE';
            document.getElementById('status-dot').className = 'status-dot live';
            document.getElementById('status-text').textContent = 'Live';
            document.getElementById('events-title').textContent = 'Events';
            document.getElementById('filter-input').placeholder = 'Filter by tx...';
            updateView();
        }}

        function goToTime(time) {{
            isLive = false;
            currentTime = time;
            document.getElementById('mode-button').className = 'timeline-mode historical';
            document.getElementById('mode-button').textContent = 'HISTORICAL';
            document.getElementById('status-dot').className = 'status-dot historical';
            document.getElementById('status-text').textContent = 'Time Travel';
            document.getElementById('events-title').textContent = 'Events at ' + formatTime(time);
            updateView();
        }}

        function updateWindowLabel() {{
            const totalMinutes = Math.round(timeWindowNs * 2 / 60_000_000_000);
            let label;
            if (totalMinutes >= 60) {{
                label = `${{Math.round(totalMinutes / 60)}} hr`;
            }} else {{
                label = `${{totalMinutes}} min`;
            }}
            document.getElementById('window-label').textContent = label;
        }}

        function setupTimeline() {{
            const timeline = document.getElementById('timeline');

            function getTimeFromX(clientX) {{
                const rect = timeline.getBoundingClientRect();
                const pos = Math.max(0, Math.min(1, (clientX - rect.left - 16) / (rect.width - 32)));
                return timeRange.start + pos * (timeRange.end - timeRange.start);
            }}

            // Click on timeline background to jump
            timeline.addEventListener('click', (e) => {{
                if (e.target.closest('.timeline-playhead')) return;
                if (e.target.classList.contains('timeline-marker')) return;
                goToTime(getTimeFromX(e.clientX));
            }});

            // Drag states
            let dragMode = null; // 'move', 'resize-left', 'resize-right'

            // Center handle - move the whole window
            document.querySelector('.timeline-playhead-handle').addEventListener('mousedown', (e) => {{
                dragMode = 'move';
                e.preventDefault();
                e.stopPropagation();
            }});

            // Left edge - resize by moving left boundary
            document.getElementById('resize-left').addEventListener('mousedown', (e) => {{
                dragMode = 'resize-left';
                e.preventDefault();
                e.stopPropagation();
            }});

            // Right edge - resize by moving right boundary
            document.getElementById('resize-right').addEventListener('mousedown', (e) => {{
                dragMode = 'resize-right';
                e.preventDefault();
                e.stopPropagation();
            }});

            document.addEventListener('mousemove', (e) => {{
                if (!dragMode) return;

                const mouseTime = getTimeFromX(e.clientX);

                if (dragMode === 'move') {{
                    // Move the center to mouse position
                    goToTime(mouseTime);
                }} else if (dragMode === 'resize-left') {{
                    // Left edge: window goes from mouseTime to (currentTime + timeWindowNs)
                    const rightEdge = currentTime + timeWindowNs;
                    const newCenter = (mouseTime + rightEdge) / 2;
                    const newHalfWindow = (rightEdge - mouseTime) / 2;

                    if (newHalfWindow >= MIN_TIME_WINDOW_NS && newHalfWindow <= MAX_TIME_WINDOW_NS) {{
                        timeWindowNs = newHalfWindow;
                        currentTime = newCenter;
                        isLive = false;
                        updateWindowLabel();
                        updatePlayhead();
                        updateView();
                    }}
                }} else if (dragMode === 'resize-right') {{
                    // Right edge: window goes from (currentTime - timeWindowNs) to mouseTime
                    const leftEdge = currentTime - timeWindowNs;
                    const newCenter = (leftEdge + mouseTime) / 2;
                    const newHalfWindow = (mouseTime - leftEdge) / 2;

                    if (newHalfWindow >= MIN_TIME_WINDOW_NS && newHalfWindow <= MAX_TIME_WINDOW_NS) {{
                        timeWindowNs = newHalfWindow;
                        currentTime = newCenter;
                        isLive = false;
                        updateWindowLabel();
                        updatePlayhead();
                        updateView();
                    }}
                }}
            }});

            document.addEventListener('mouseup', () => {{
                dragMode = null;
            }});

            // Initialize window label
            updateWindowLabel();

            // Keyboard shortcuts
            document.addEventListener('keydown', (e) => {{
                if (e.target.tagName === 'INPUT') return;

                const step = (timeRange.end - timeRange.start) / 100;

                if (e.key === 'ArrowLeft') {{
                    goToTime(Math.max(timeRange.start, currentTime - step));
                }} else if (e.key === 'ArrowRight') {{
                    goToTime(Math.min(timeRange.end, currentTime + step));
                }} else if (e.key === 'l' || e.key === 'L') {{
                    goLive();
                }}
            }});

            // Filter input
            document.getElementById('filter-input').addEventListener('input', (e) => {{
                filterText = e.target.value;
                updateView();
            }});
        }}

        function handleMessage(data) {{
            if (data.type === 'state') {{
                console.log('Received initial state');

                // Extract gateway and user identification
                if (data.gateway_peer_id) {{
                    gatewayPeerId = data.gateway_peer_id;
                    console.log('Gateway:', gatewayPeerId);
                }}
                if (data.your_peer_id) {{
                    yourPeerId = data.your_peer_id;
                    yourIpHash = data.your_ip_hash;
                    console.log('You:', yourPeerId, '#' + yourIpHash);

                    // Update legend
                    document.getElementById('legend-you').style.display = 'flex';
                    document.getElementById('your-hash').textContent = '#' + yourIpHash;
                }}

                // Show legend
                document.getElementById('topology-legend').style.display = 'flex';

                // Store subscription data
                if (data.subscriptions) {{
                    subscriptionData = data.subscriptions;
                    updateContractDropdown();
                    console.log('Subscriptions:', Object.keys(subscriptionData).length, 'contracts');
                }}

                // Store and display operation stats
                if (data.op_stats) {{
                    opStats = data.op_stats;
                    updateOpStats();
                    console.log('Op stats loaded');
                }}

            }} else if (data.type === 'history') {{
                allEvents.length = 0;
                allEvents.push(...data.events);
                timeRange = data.time_range;
                timeRange.end = Date.now() * 1_000_000;
                currentTime = timeRange.end;

                console.log(`Loaded ${{allEvents.length}} events`);

                renderTimeline();
                renderRuler();
                updateView();

            }} else if (data.type === 'event') {{
                allEvents.push(data);
                timeRange.end = data.timestamp;

                if (timeRange.end > timeRange.start) {{
                    const container = document.getElementById('timeline-events');
                    const duration = timeRange.end - timeRange.start;
                    const pos = (data.timestamp - timeRange.start) / duration;

                    const marker = document.createElement('div');
                    marker.className = `timeline-marker ${{getEventClass(data.event_type)}}`;
                    marker.style.left = `${{pos * 100}}%`;
                    marker.style.height = '20px';
                    marker.style.top = '30px';
                    marker.title = `${{data.time_str}} - ${{data.event_type}}`;
                    container.appendChild(marker);
                }}

                if (isLive) {{
                    currentTime = data.timestamp;
                    updateView();
                }}
            }}
        }}

        function connect() {{
            // Use same host/port as the page, proxied through Caddy
            const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${{wsProtocol}}//${{window.location.host}}/ws`;
            ws = new WebSocket(wsUrl);

            ws.onopen = () => {{
                document.getElementById('status-dot').className = 'status-dot live';
                document.getElementById('status-text').textContent = 'Live';
                if (reconnectTimeout) {{ clearTimeout(reconnectTimeout); reconnectTimeout = null; }}
            }};

            ws.onmessage = (event) => {{
                try {{ handleMessage(JSON.parse(event.data)); }}
                catch (e) {{ console.error('Parse error:', e); }}
            }};

            ws.onclose = () => {{
                document.getElementById('status-dot').className = 'status-dot disconnected';
                document.getElementById('status-text').textContent = 'Reconnecting...';
                reconnectTimeout = setTimeout(connect, 3000);
            }};

            ws.onerror = () => ws.close();
        }}

        // Initialize
        setupTimeline();
        connect();
    </script>
</body>
</html>
"""
    return html


def generate_ring_svg(peers):
    """Generate SVG visualization of peers on a ring."""
    size = 400
    center = size / 2
    radius = 160

    svg_parts = [f'<svg viewBox="0 0 {size} {size}" width="{size}" height="{size}">']

    # Background circle (the ring)
    svg_parts.append(f'''
        <circle cx="{center}" cy="{center}" r="{radius}"
                fill="none" stroke="#222" stroke-width="2"/>
        <circle cx="{center}" cy="{center}" r="{radius - 30}"
                fill="none" stroke="#1a1a1a" stroke-width="1" stroke-dasharray="4,4"/>
    ''')

    # Location markers (0, 0.25, 0.5, 0.75)
    for loc in [0, 0.25, 0.5, 0.75]:
        angle = loc * 2 * math.pi - math.pi / 2  # Start from top
        x = center + (radius + 20) * math.cos(angle)
        y = center + (radius + 20) * math.sin(angle)
        svg_parts.append(f'''
            <text x="{x}" y="{y}" fill="#444" font-size="12"
                  text-anchor="middle" dominant-baseline="middle">{loc}</text>
        ''')

    # Draw peers
    for i, peer in enumerate(peers):
        location = peer["location"]
        angle = location * 2 * math.pi - math.pi / 2  # Start from top
        x = center + radius * math.cos(angle)
        y = center + radius * math.sin(angle)

        # Color based on activity (more events = brighter)
        intensity = min(255, 100 + peer["events"] * 10)
        color = f"rgb(0, {intensity}, {int(intensity * 0.65)})"

        # Peer dot
        svg_parts.append(f'''
            <circle cx="{x}" cy="{y}" r="8" fill="{color}" opacity="0.9">
                <title>{peer["id"]} @ {location:.3f}</title>
            </circle>
        ''')

        # Peer label (show for up to 10 peers)
        if len(peers) <= 10:
            label_radius = radius + 35
            lx = center + label_radius * math.cos(angle)
            ly = center + label_radius * math.sin(angle)
            svg_parts.append(f'''
                <text x="{lx}" y="{ly}" fill="#666" font-size="10"
                      text-anchor="middle" dominant-baseline="middle">{peer["id"][:10]}</text>
            ''')

    # Center text
    svg_parts.append(f'''
        <text x="{center}" y="{center - 10}" fill="#00d4aa" font-size="24"
              text-anchor="middle" font-weight="bold">{len(peers)}</text>
        <text x="{center}" y="{center + 15}" fill="#666" font-size="12"
              text-anchor="middle">peers</text>
    ''')

    svg_parts.append('</svg>')
    return '\n'.join(svg_parts)


def generate_events_table(events):
    """Generate HTML table for recent events."""
    if not events:
        return '<p style="color: #666; text-align: center; padding: 20px;">No recent events</p>'

    # Sort by time descending
    events.sort(key=lambda e: e["time"], reverse=True)

    rows = []
    for event in events[:15]:  # Show last 15
        ts = datetime.fromtimestamp(event["time"] / 1_000_000_000)
        time_str = ts.strftime('%H:%M:%S')

        # Determine event category for styling
        event_type = event["type"]
        if "connect" in event_type:
            css_class = "event-connect"
        elif "put" in event_type:
            css_class = "event-put"
        elif "get" in event_type:
            css_class = "event-get"
        elif "update" in event_type:
            css_class = "event-update"
        elif "subscrib" in event_type:
            css_class = "event-subscribe"
        else:
            css_class = "event-other"

        rows.append(f'''
            <tr>
                <td>{time_str}</td>
                <td><span class="event-type {css_class}">{event_type}</span></td>
                <td>{event["peer"]}</td>
            </tr>
        ''')

    return f'''
        <table class="events-table">
            <thead>
                <tr><th>Time</th><th>Event</th><th>Peer</th></tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
    '''


def generate_operations_summary(operations):
    """Generate operations summary HTML."""
    if not operations:
        return '<p style="color: #666;">No operations recorded</p>'

    cards = []
    for op_type in ["put", "get", "update", "connect"]:
        if op_type in operations:
            data = operations[op_type]
            requests = data["requests"]
            successes = data["successes"]
            rate = (successes / requests * 100) if requests > 0 else 0

            cards.append(f'''
                <div class="op-card">
                    <div class="op-name">{op_type.upper()}</div>
                    <div class="op-stats">{successes}/{requests} successful</div>
                    <div class="op-bar">
                        <div class="op-bar-fill" style="width: {rate}%"></div>
                    </div>
                </div>
            ''')

    return f'<div class="ops-grid">{"".join(cards)}</div>'


def main():
    print(f"Parsing telemetry from {TELEMETRY_LOG}...")
    peers, recent_events, operations = parse_telemetry()

    print(f"Found {len(peers)} public peers")
    print(f"Found {len(recent_events)} recent events")
    print(f"Operations: {dict(operations)}")

    print(f"Generating dashboard...")
    html = generate_html(peers, recent_events, operations)

    OUTPUT_HTML.write_text(html)
    print(f"Dashboard written to {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
