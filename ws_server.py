#!/usr/bin/env python3
"""
Freenet Telemetry WebSocket Server

Tails the telemetry log file and pushes events to connected clients in real-time.
Also tracks peer connections to build network topology.
Supports time-travel by buffering event history.
"""

import asyncio
import json
import hashlib
import re
import time
from datetime import datetime
from pathlib import Path
from collections import deque

import websockets

TELEMETRY_LOG = Path("/var/log/freenet-telemetry/logs.jsonl")
WS_PORT = 3134

# Event history buffer (last 2 hours)
MAX_HISTORY_AGE_NS = 2 * 60 * 60 * 1_000_000_000  # 2 hours in nanoseconds
event_history = deque()  # List of events with timestamps

# Connected WebSocket clients
clients = set()

# Network state (current/live)
peers = {}  # ip -> {id, location, last_seen, connections: set()}
connections = set()  # frozenset({ip1, ip2})

# Subscription trees per contract
# contract_key -> {subscribers: set(ip), broadcasts: [(from_ip, to_ip, timestamp)]}
subscriptions = {}  # contract_key -> subscription data

# Operation statistics
op_stats = {
    "put": {"requests": 0, "successes": 0, "latencies": []},
    "get": {"requests": 0, "successes": 0, "not_found": 0, "latencies": []},
    "update": {"requests": 0, "successes": 0, "broadcasts": 0, "latencies": []},
    "subscribe": {"requests": 0, "successes": 0},
}

# Track pending operations by transaction ID for latency calculation
# tx_id -> {"op": "put"|"get"|"update", "start_ns": timestamp}
pending_ops = {}

# Transaction tracking - store full event sequences for timeline lanes
# tx_id -> {"op": type, "contract": key, "events": [...], "start_ns": ts, "end_ns": ts, "status": "pending"|"success"|"failed"}
MAX_TRANSACTIONS = 500  # Keep last N transactions
transactions = {}  # tx_id -> transaction data
transaction_order = []  # List of tx_ids in order for pruning

# Pattern to parse peer strings like: "PeerId@IP:port (@ location)"
PEER_PATTERN = re.compile(r'(\w+)@(\d+\.\d+\.\d+\.\d+):(\d+)\s*\(@\s*([\d.]+)\)')


def anonymize_ip(ip: str) -> str:
    """Convert IP to anonymous identifier."""
    if not ip:
        return "unknown"
    h = hashlib.sha256(ip.encode()).hexdigest()[:8]
    return f"peer-{h}"


def ip_hash(ip: str) -> str:
    """Generate a short hash of IP for user self-identification."""
    if not ip:
        return ""
    return hashlib.sha256(ip.encode()).hexdigest()[:6]


def is_public_ip(ip: str) -> bool:
    """Check if IP is a public (non-test) address."""
    if not ip:
        return False
    if ip.startswith("127.") or ip.startswith("172.") or ip.startswith("10.") or ip.startswith("192.168."):
        return False
    if ip.startswith("0.") or ip == "localhost":
        return False
    return True


def parse_peer_string(peer_str):
    """Extract peer_id, IP, and location from peer string."""
    if not peer_str:
        return None, None, None
    match = PEER_PATTERN.search(peer_str)
    if match:
        peer_id = match.group(1)
        ip = match.group(2)
        location = float(match.group(4))
        return peer_id, ip, location
    return None, None, None


def prune_old_events():
    """Remove events older than MAX_HISTORY_AGE_NS."""
    now_ns = int(time.time() * 1_000_000_000)
    cutoff = now_ns - MAX_HISTORY_AGE_NS
    while event_history and event_history[0]["timestamp"] < cutoff:
        event_history.popleft()


def prune_old_transactions():
    """Keep only the last MAX_TRANSACTIONS."""
    global transaction_order
    while len(transaction_order) > MAX_TRANSACTIONS:
        old_tx_id = transaction_order.pop(0)
        if old_tx_id in transactions:
            del transactions[old_tx_id]


def track_transaction(tx_id, event_type, timestamp, peer_id, contract_key=None):
    """Track an event as part of a transaction for timeline lanes."""
    if not tx_id or tx_id == "00000000000000000000000000":
        return  # Skip null transaction IDs

    # Determine operation type from event
    op_type = None
    is_start = False
    is_end = False
    status = None

    if event_type == "put_request":
        op_type = "put"
        is_start = True
    elif event_type == "put_success":
        op_type = "put"
        is_end = True
        status = "success"
    elif event_type == "get_request":
        op_type = "get"
        is_start = True
    elif event_type == "get_success":
        op_type = "get"
        is_end = True
        status = "success"
    elif event_type == "get_not_found":
        op_type = "get"
        is_end = True
        status = "not_found"
    elif event_type == "update_request":
        op_type = "update"
        is_start = True
    elif event_type == "update_success":
        op_type = "update"
        is_end = True
        status = "success"
    elif event_type == "subscribe_request":
        op_type = "subscribe"
        is_start = True
    elif event_type == "subscribed":
        op_type = "subscribe"
        is_end = True
        status = "success"
    elif event_type in ("broadcast_emitted", "update_broadcast_emitted"):
        op_type = "broadcast"

    # Create or update transaction
    if tx_id not in transactions:
        if is_start or op_type:  # Start new transaction
            transactions[tx_id] = {
                "op": op_type or "unknown",
                "contract": contract_key,
                "events": [],
                "start_ns": timestamp,
                "end_ns": None,
                "status": "pending",
            }
            transaction_order.append(tx_id)
            prune_old_transactions()

    if tx_id in transactions:
        tx = transactions[tx_id]
        # Add event to transaction
        tx["events"].append({
            "timestamp": timestamp,
            "event_type": event_type,
            "peer_id": peer_id,
        })
        # Update end time and status
        if is_end:
            tx["end_ns"] = timestamp
            tx["status"] = status or "complete"
        elif timestamp > (tx["end_ns"] or 0):
            tx["end_ns"] = timestamp
        # Update contract if not set
        if contract_key and not tx["contract"]:
            tx["contract"] = contract_key


def process_record(record, store_history=True):
    """Process a telemetry record and return event data for clients."""
    attrs = {a["key"]: a["value"].get("stringValue") or a["value"].get("doubleValue")
             for a in record.get("attributes", [])}

    timestamp_raw = record.get("timeUnixNano", "0")
    timestamp = int(timestamp_raw) if isinstance(timestamp_raw, str) else timestamp_raw

    # Parse body
    body_str = record.get("body", {}).get("stringValue", "")
    body = {}
    if body_str:
        try:
            body = json.loads(body_str)
        except json.JSONDecodeError:
            pass

    event_type = attrs.get("event_type") or body.get("type", "")
    if not event_type:
        return None

    # Track operation statistics
    tx_id = body.get("id") or attrs.get("transaction_id")  # Transaction ID for correlating request/success

    if event_type == "put_request":
        op_stats["put"]["requests"] += 1
        if tx_id:
            pending_ops[tx_id] = {"op": "put", "start_ns": timestamp}
    elif event_type == "put_success":
        op_stats["put"]["successes"] += 1
        if tx_id and tx_id in pending_ops:
            latency_ms = (timestamp - pending_ops[tx_id]["start_ns"]) / 1_000_000
            if 0 < latency_ms < 300_000:  # Sanity check: < 5 minutes
                op_stats["put"]["latencies"].append(latency_ms)
                # Keep only last 1000 latencies
                if len(op_stats["put"]["latencies"]) > 1000:
                    op_stats["put"]["latencies"] = op_stats["put"]["latencies"][-1000:]
            del pending_ops[tx_id]
    elif event_type == "get_request":
        op_stats["get"]["requests"] += 1
        if tx_id:
            pending_ops[tx_id] = {"op": "get", "start_ns": timestamp}
    elif event_type == "get_success":
        op_stats["get"]["successes"] += 1
        if tx_id and tx_id in pending_ops:
            latency_ms = (timestamp - pending_ops[tx_id]["start_ns"]) / 1_000_000
            if 0 < latency_ms < 300_000:
                op_stats["get"]["latencies"].append(latency_ms)
                if len(op_stats["get"]["latencies"]) > 1000:
                    op_stats["get"]["latencies"] = op_stats["get"]["latencies"][-1000:]
            del pending_ops[tx_id]
    elif event_type == "get_not_found":
        op_stats["get"]["not_found"] += 1
        if tx_id and tx_id in pending_ops:
            del pending_ops[tx_id]
    elif event_type == "update_request":
        op_stats["update"]["requests"] += 1
        if tx_id:
            pending_ops[tx_id] = {"op": "update", "start_ns": timestamp}
    elif event_type == "update_success":
        op_stats["update"]["successes"] += 1
        if tx_id and tx_id in pending_ops:
            latency_ms = (timestamp - pending_ops[tx_id]["start_ns"]) / 1_000_000
            if 0 < latency_ms < 300_000:
                op_stats["update"]["latencies"].append(latency_ms)
                if len(op_stats["update"]["latencies"]) > 1000:
                    op_stats["update"]["latencies"] = op_stats["update"]["latencies"][-1000:]
            del pending_ops[tx_id]
    elif event_type in ("update_broadcast_emitted", "broadcast_emitted"):
        op_stats["update"]["broadcasts"] += 1
    elif event_type == "subscribe_request":
        op_stats["subscribe"]["requests"] += 1
    elif event_type == "subscribed":
        op_stats["subscribe"]["successes"] += 1

    # Extract peer info
    this_peer_id, this_ip, this_loc = parse_peer_string(body.get("this_peer", ""))
    other_peer_id, other_ip, other_loc = None, None, None

    # Check various fields for other peer
    for field in ["connected_peer", "target", "requester"]:
        if field in body:
            other_peer_id, other_ip, other_loc = parse_peer_string(body[field])
            if other_ip:
                break

    # Update peer state
    updated_peers = []
    for ip, loc, peer_id in [(this_ip, this_loc, this_peer_id), (other_ip, other_loc, other_peer_id)]:
        if ip and is_public_ip(ip) and loc is not None:
            if ip not in peers:
                peers[ip] = {
                    "id": anonymize_ip(ip),
                    "ip_hash": ip_hash(ip),
                    "location": loc,
                    "last_seen": timestamp,
                    "connections": set()
                }
                updated_peers.append(ip)
            else:
                peers[ip]["location"] = loc
                peers[ip]["last_seen"] = timestamp

    # Track connections (event_type is "connect" in attrs, "connected" in body)
    connection_added = None
    if event_type in ("connect", "connected") and this_ip and other_ip:
        if is_public_ip(this_ip) and is_public_ip(other_ip):
            conn = frozenset({this_ip, other_ip})
            if conn not in connections:
                connections.add(conn)
                if this_ip in peers:
                    peers[this_ip]["connections"].add(other_ip)
                if other_ip in peers:
                    peers[other_ip]["connections"].add(this_ip)
                connection_added = (anonymize_ip(this_ip), anonymize_ip(other_ip))

    # Track subscription tree data FIRST (before potentially returning None)
    contract_key = body.get("contract_key") or body.get("key")
    if contract_key:
        if contract_key not in subscriptions:
            subscriptions[contract_key] = {
                "subscribers": set(),
                "tree": {},  # from_peer_id -> [to_peer_ids]
            }

        sub_data = subscriptions[contract_key]

        # Track subscribed events
        if event_type == "subscribed":
            if this_ip and is_public_ip(this_ip):
                sub_data["subscribers"].add(anonymize_ip(this_ip))

        # Track broadcast tree from broadcast_emitted events
        body_type = body.get("type", "")
        if event_type in ("broadcast_emitted", "update_broadcast_emitted") or body_type == "broadcast_emitted":
            broadcast_to = body.get("broadcast_to", [])
            sender_str = body.get("sender", "")
            _, sender_ip, _ = parse_peer_string(sender_str)

            if sender_ip and is_public_ip(sender_ip):
                sender_id = anonymize_ip(sender_ip)
                if sender_id not in sub_data["tree"]:
                    sub_data["tree"][sender_id] = set()

                for target_str in broadcast_to:
                    _, target_ip, _ = parse_peer_string(target_str)
                    if target_ip and is_public_ip(target_ip):
                        target_id = anonymize_ip(target_ip)
                        sub_data["tree"][sender_id].add(target_id)
                        sub_data["subscribers"].add(target_id)

    # Determine which peer to show (prefer public IP)
    display_ip = None
    display_loc = None
    if this_ip and is_public_ip(this_ip):
        display_ip = this_ip
        display_loc = this_loc
    elif other_ip and is_public_ip(other_ip):
        display_ip = other_ip
        display_loc = other_loc

    if not display_ip:
        return None

    # Build event for client
    event = {
        "type": "event",
        "timestamp": timestamp,
        "event_type": event_type,
        "peer_id": anonymize_ip(display_ip),
        "peer_ip_hash": ip_hash(display_ip),
        "location": display_loc,
        "time_str": datetime.fromtimestamp(timestamp / 1_000_000_000).strftime('%H:%M:%S'),
    }

    # Include source/destination peers for message flow visualization
    if this_ip and is_public_ip(this_ip):
        event["from_peer"] = anonymize_ip(this_ip)
        event["from_location"] = this_loc
    if other_ip and is_public_ip(other_ip):
        event["to_peer"] = anonymize_ip(other_ip)
        event["to_location"] = other_loc

    # Include connection info if new connection
    if connection_added:
        event["connection"] = connection_added

    # Include contract info if present
    if contract_key:
        event["contract"] = contract_key[:12] + "..."
        event["contract_full"] = contract_key

    # Include transaction ID for timeline lanes
    if tx_id and tx_id != "00000000000000000000000000":
        event["tx_id"] = tx_id
        # Track this event as part of the transaction
        track_transaction(tx_id, event_type, timestamp, event["peer_id"], contract_key)

    # Store in history buffer
    if store_history:
        event_history.append(event)
        # Periodically prune old events
        if len(event_history) % 100 == 0:
            prune_old_events()

    return event


def get_operation_stats():
    """Get computed operation statistics."""
    def calc_percentiles(latencies):
        if not latencies:
            return {"p50": None, "p95": None, "p99": None}
        sorted_lat = sorted(latencies)
        n = len(sorted_lat)
        return {
            "p50": sorted_lat[int(n * 0.50)] if n > 0 else None,
            "p95": sorted_lat[int(n * 0.95)] if n > 1 else None,
            "p99": sorted_lat[int(n * 0.99)] if n > 2 else None,
        }

    def calc_rate(successes, requests):
        if requests == 0:
            return None
        return round(successes / requests * 100, 1)

    put = op_stats["put"]
    get = op_stats["get"]
    update = op_stats["update"]
    subscribe = op_stats["subscribe"]

    return {
        "put": {
            "total": put["requests"],
            "success_rate": calc_rate(put["successes"], put["requests"]),
            "latency": calc_percentiles(put["latencies"]),
        },
        "get": {
            "total": get["requests"] + get["successes"],  # get_success without get_request
            "success_rate": calc_rate(get["successes"], get["successes"] + get["not_found"]) if (get["successes"] + get["not_found"]) > 0 else None,
            "not_found": get["not_found"],
            "latency": calc_percentiles(get["latencies"]),
        },
        "update": {
            "total": update["requests"],
            "success_rate": calc_rate(update["successes"], update["requests"]),
            "broadcasts": update["broadcasts"],
            "latency": calc_percentiles(update["latencies"]),
        },
        "subscribe": {
            "total": subscribe["successes"],  # subscribed events
        },
    }


def get_subscription_trees():
    """Get subscription tree data for all contracts."""
    result = {}
    for contract_key, data in subscriptions.items():
        # Convert sets to lists for JSON serialization
        tree = {k: list(v) for k, v in data["tree"].items()}
        if tree or data["subscribers"]:  # Only include contracts with actual data
            result[contract_key] = {
                "subscribers": list(data["subscribers"]),
                "tree": tree,
                "short_key": contract_key[:12] + "...",
            }
    return result


def get_network_state():
    """Get current network state for new clients."""
    peer_list = []
    for ip, data in peers.items():
        if is_public_ip(ip):
            peer_list.append({
                "id": data["id"],
                "ip_hash": data.get("ip_hash", ip_hash(ip)),
                "location": data["location"],
            })

    conn_list = []
    for conn in connections:
        ips = list(conn)
        if len(ips) == 2 and is_public_ip(ips[0]) and is_public_ip(ips[1]):
            conn_list.append([anonymize_ip(ips[0]), anonymize_ip(ips[1])])

    return {
        "type": "state",
        "peers": peer_list,
        "connections": conn_list,
        "subscriptions": get_subscription_trees(),
        "op_stats": get_operation_stats(),
    }


def get_transactions_list():
    """Get list of transactions for timeline lanes."""
    result = []
    for tx_id in transaction_order:
        if tx_id in transactions:
            tx = transactions[tx_id]
            # Calculate duration
            duration_ms = None
            if tx["start_ns"] and tx["end_ns"]:
                duration_ms = (tx["end_ns"] - tx["start_ns"]) / 1_000_000

            result.append({
                "tx_id": tx_id,
                "op": tx["op"],
                "contract": tx["contract"][:12] + "..." if tx["contract"] else None,
                "contract_full": tx["contract"],
                "start_ns": tx["start_ns"],
                "end_ns": tx["end_ns"] or tx["start_ns"],  # Use start if no end yet
                "duration_ms": duration_ms,
                "status": tx["status"],
                "event_count": len(tx["events"]),
                "events": tx["events"],  # Include full event list for detail view
            })
    return result


def get_history():
    """Get event history for time-travel feature."""
    prune_old_events()
    return {
        "type": "history",
        "events": list(event_history),
        "transactions": get_transactions_list(),
        "time_range": {
            "start": event_history[0]["timestamp"] if event_history else 0,
            "end": event_history[-1]["timestamp"] if event_history else 0,
        }
    }


async def broadcast(message):
    """Send message to all connected clients."""
    if clients:
        msg = json.dumps(message)
        await asyncio.gather(*[client.send(msg) for client in clients], return_exceptions=True)


async def tail_log():
    """Tail the telemetry log and broadcast new events."""
    # Wait for file to exist
    while not TELEMETRY_LOG.exists():
        await asyncio.sleep(1)

    # Start at end of file
    with open(TELEMETRY_LOG, 'r') as f:
        f.seek(0, 2)  # Seek to end

        while True:
            line = f.readline()
            if not line:
                await asyncio.sleep(0.1)
                continue

            try:
                batch = json.loads(line)
                for resource_log in batch.get("resourceLogs", []):
                    for scope_log in resource_log.get("scopeLogs", []):
                        for record in scope_log.get("logRecords", []):
                            event = process_record(record, store_history=True)
                            if event:
                                await broadcast(event)
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Error processing line: {e}")


GATEWAY_IP = "5.9.111.215"
GATEWAY_PEER_ID = anonymize_ip(GATEWAY_IP)
GATEWAY_IP_HASH = ip_hash(GATEWAY_IP)


async def handle_client(websocket):
    """Handle a WebSocket client connection."""
    clients.add(websocket)

    # Get client IP for self-identification
    client_ip = None
    if websocket.remote_address:
        client_ip = websocket.remote_address[0]
    client_ip_hash = ip_hash(client_ip) if client_ip else ""
    client_peer_id = anonymize_ip(client_ip) if client_ip else ""

    print(f"Client connected from {client_ip} (#{client_ip_hash}). Total: {len(clients)}")

    try:
        # Send current network state with client identification
        state = get_network_state()
        state["your_ip_hash"] = client_ip_hash
        state["your_peer_id"] = client_peer_id
        state["gateway_peer_id"] = GATEWAY_PEER_ID
        state["gateway_ip_hash"] = GATEWAY_IP_HASH
        await websocket.send(json.dumps(state))

        # Send event history for time-travel
        history = get_history()
        await websocket.send(json.dumps(history))

        # Keep connection alive and handle messages
        async for message in websocket:
            # Could handle client messages here (e.g., request specific time range)
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(websocket)
        print(f"Client disconnected. Total: {len(clients)}")


async def load_initial_state():
    """Load existing telemetry to build initial network state and history."""
    if not TELEMETRY_LOG.exists():
        return

    print("Loading initial state from telemetry log...")
    count = 0
    now_ns = int(time.time() * 1_000_000_000)
    history_cutoff = now_ns - MAX_HISTORY_AGE_NS

    with open(TELEMETRY_LOG, 'r') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                batch = json.loads(line)
                for resource_log in batch.get("resourceLogs", []):
                    for scope_log in resource_log.get("scopeLogs", []):
                        for record in scope_log.get("logRecords", []):
                            # Check if event is within history window
                            timestamp_raw = record.get("timeUnixNano", "0")
                            timestamp = int(timestamp_raw) if isinstance(timestamp_raw, str) else timestamp_raw
                            store_in_history = timestamp >= history_cutoff

                            process_record(record, store_history=store_in_history)
                            count += 1
            except:
                continue

    print(f"Loaded {count} records. Found {len(peers)} peers, {len(connections)} connections.")
    print(f"Event history: {len(event_history)} events in buffer")


async def main():
    """Main entry point."""
    # Load existing state
    await load_initial_state()

    # Start WebSocket server
    print(f"Starting WebSocket server on port {WS_PORT}...")
    async with websockets.serve(handle_client, "0.0.0.0", WS_PORT):
        # Start log tailer
        await tail_log()


if __name__ == "__main__":
    asyncio.run(main())
