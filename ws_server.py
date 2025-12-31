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

# Peer presence timeline for historical reconstruction
# ip -> {id, ip_hash, location, first_seen_ns}
peer_presence = {}

# Subscription trees per contract
# contract_key -> {subscribers: set(ip), broadcasts: [(from_ip, to_ip, timestamp)]}
subscriptions = {}  # contract_key -> subscription data

# Seeding state per (contract, peer) - tracks each peer's subscription tree position
# contract_key -> {peer_id -> {is_seeding: bool, upstream: peer_str, downstream: [peer_str], downstream_count: int}}
seeding_state = {}  # contract_key -> {peer_id -> state}

# Contract state hashes per (contract, peer) - tracks state propagation
# contract_key -> {peer_id -> {hash: str, timestamp: int, event_type: str}}
contract_states = {}


def update_contract_state(contract_key, peer_id, state_hash, timestamp, event_type):
    """Update the known state hash for a (contract, peer) pair."""
    if not contract_key or not peer_id or not state_hash:
        return

    if contract_key not in contract_states:
        contract_states[contract_key] = {}

    # Only update if this is newer than what we have
    existing = contract_states[contract_key].get(peer_id)
    if existing and existing["timestamp"] >= timestamp:
        return

    contract_states[contract_key][peer_id] = {
        "hash": state_hash,
        "timestamp": timestamp,
        "event_type": event_type,
    }


# Operation statistics
op_stats = {
    "put": {"requests": 0, "successes": 0, "latencies": []},
    "get": {"requests": 0, "successes": 0, "not_found": 0, "latencies": []},
    "update": {"requests": 0, "successes": 0, "broadcasts": 0, "latencies": []},
    "subscribe": {"requests": 0, "successes": 0},
}

# Peer lifecycle tracking
# peer_id -> {version, arch, os, os_version, is_gateway, startup_time, shutdown_time, graceful}
peer_lifecycle = {}

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


def track_transaction(tx_id, event_type, timestamp, peer_id, contract_key=None, body_type=None):
    """Track an event as part of a transaction for timeline lanes.

    All events with a valid transaction ID are tracked. Events are grouped by
    transaction ID to show related events together in the timeline.
    """
    if not tx_id or tx_id == "00000000000000000000000000":
        return  # Skip null transaction IDs

    # Use body_type for more specific event type if available (especially for connect events)
    display_event_type = body_type if body_type else event_type

    # Determine operation type from event type prefix
    op_type = None
    is_start = False
    is_end = False
    status = None

    # Derive op_type from event_type prefix
    if event_type.startswith("put_"):
        op_type = "put"
        if event_type == "put_request":
            is_start = True
        elif event_type == "put_success":
            is_end = True
            status = "success"
    elif event_type.startswith("get_"):
        op_type = "get"
        if event_type == "get_request":
            is_start = True
        elif event_type == "get_success":
            is_end = True
            status = "success"
        elif event_type == "get_not_found":
            is_end = True
            status = "not_found"
    elif event_type.startswith("update_"):
        op_type = "update"
        if event_type == "update_request":
            is_start = True
        elif event_type == "update_success":
            is_end = True
            status = "success"
    elif event_type.startswith("subscribe"):
        op_type = "subscribe"
        if event_type == "subscribe_request":
            is_start = True
        elif event_type == "subscribed":
            is_end = True
            status = "success"
    elif event_type.startswith("connect"):
        op_type = "connect"
        if event_type == "connect_request_sent":
            is_start = True
        elif event_type == "connect_connected":
            is_end = True
            status = "success"
    elif event_type == "disconnect":
        op_type = "disconnect"
        is_start = True
        is_end = True
        status = "complete"
    elif "broadcast" in event_type:
        op_type = "broadcast"
    else:
        # For any other event type, use the prefix before underscore as op_type
        parts = event_type.split("_")
        op_type = parts[0] if parts else "other"

    # Create or update transaction - always create if we have a valid tx_id
    if tx_id not in transactions:
        transactions[tx_id] = {
            "op": op_type or "unknown",
            "contract": contract_key,
            "events": [],
            "start_ns": timestamp,
            "end_ns": None,
            "status": "pending" if is_start and not is_end else "complete",
        }
        transaction_order.append(tx_id)
        prune_old_transactions()

    tx = transactions[tx_id]

    # Add event to transaction (use display_event_type for more specific types)
    tx["events"].append({
        "timestamp": timestamp,
        "event_type": display_event_type,
        "peer_id": peer_id,
    })

    # Update operation type if we now have a more specific one
    if op_type and tx["op"] == "unknown":
        tx["op"] = op_type

    # Update start time if this event is earlier
    if timestamp < tx["start_ns"]:
        tx["start_ns"] = timestamp

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

    # Get body type for more specific event types (especially for connect events)
    # event_type from attrs is generic ("connect"), body type is specific ("start_connection", "connected", "finished")
    body_type = body.get("type", "")

    # Track operation statistics
    tx_id = body.get("id") or attrs.get("transaction_id")  # Transaction ID for correlating request/success

    # Extract state hashes (from PR #2492)
    state_hash = body.get("state_hash")
    state_hash_before = body.get("state_hash_before")
    state_hash_after = body.get("state_hash_after")

    # Get contract key for state tracking
    contract_key = body.get("contract_key") or body.get("key")

    # Get peer_id for state tracking
    event_peer_id = attrs.get("peer_id") or ""
    if not event_peer_id:
        this_peer_str = body.get("this_peer", "")
        if this_peer_str:
            pid, _, _ = parse_peer_string(this_peer_str)
            if pid:
                event_peer_id = pid

    # Update contract state on relevant events
    if contract_key and event_peer_id:
        if event_type == "put_success" and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type)
        elif event_type == "get_success" and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type)
        elif event_type == "update_success" and state_hash_after:
            update_contract_state(contract_key, event_peer_id, state_hash_after, timestamp, event_type)
        elif event_type in ("broadcast_emitted", "update_broadcast_emitted") and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type)
        elif event_type == "broadcast_received" and state_hash:
            update_contract_state(contract_key, event_peer_id, state_hash, timestamp, event_type)

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

    # Handle new subscription tree telemetry events (v0.1.70+)
    # Each event is reported by a specific peer - we track state per (contract, peer)
    # Get the reporting peer's ID from attrs or body
    reporting_peer = attrs.get("peer_id") or ""
    if not reporting_peer:
        # Try to extract from this_peer if available
        this_peer_str = body.get("this_peer", "")
        if this_peer_str:
            pid, _, _ = parse_peer_string(this_peer_str)
            if pid:
                reporting_peer = pid

    def get_peer_state(contract_key, peer_id):
        """Get or create state for a (contract, peer) pair."""
        if contract_key not in seeding_state:
            seeding_state[contract_key] = {}
        if peer_id not in seeding_state[contract_key]:
            seeding_state[contract_key][peer_id] = {
                "is_seeding": False,
                "upstream": None,
                "downstream": [],
                "downstream_count": 0,
            }
        return seeding_state[contract_key][peer_id]

    if event_type == "seeding_started":
        # Local client started subscribing to a contract
        contract_key = body.get("key") or body.get("contract_key")
        if contract_key and reporting_peer:
            state = get_peer_state(contract_key, reporting_peer)
            state["is_seeding"] = True

    elif event_type == "seeding_stopped":
        # Local client stopped subscribing (last client unsubscribed)
        contract_key = body.get("key") or body.get("contract_key")
        reason = body.get("reason", "Unknown")
        if contract_key and reporting_peer and contract_key in seeding_state:
            if reporting_peer in seeding_state[contract_key]:
                state = seeding_state[contract_key][reporting_peer]
                state["is_seeding"] = False
                state["stopped_reason"] = reason

    elif event_type == "downstream_added":
        # A downstream peer subscribed through us
        contract_key = body.get("key") or body.get("contract_key")
        subscriber = body.get("subscriber")
        downstream_count = body.get("downstream_count", 0)
        if contract_key and reporting_peer:
            state = get_peer_state(contract_key, reporting_peer)
            state["downstream_count"] = downstream_count
            if subscriber and subscriber not in state["downstream"]:
                state["downstream"].append(subscriber)

    elif event_type == "downstream_removed":
        # A downstream peer unsubscribed
        contract_key = body.get("key") or body.get("contract_key")
        subscriber = body.get("subscriber")
        downstream_count = body.get("downstream_count", 0)
        reason = body.get("reason", "Unknown")
        if contract_key and reporting_peer and contract_key in seeding_state:
            if reporting_peer in seeding_state[contract_key]:
                state = seeding_state[contract_key][reporting_peer]
                state["downstream_count"] = downstream_count
                if subscriber and subscriber in state["downstream"]:
                    state["downstream"].remove(subscriber)

    elif event_type == "upstream_set":
        # We subscribed to an upstream peer for this contract
        contract_key = body.get("key") or body.get("contract_key")
        upstream = body.get("upstream")
        if contract_key and reporting_peer:
            state = get_peer_state(contract_key, reporting_peer)
            state["upstream"] = upstream

    elif event_type == "unsubscribed":
        # We unsubscribed from a contract (could be voluntary or upstream disconnected)
        contract_key = body.get("key") or body.get("contract_key")
        reason = body.get("reason", "Unknown")
        upstream = body.get("upstream")
        if contract_key and reporting_peer and contract_key in seeding_state:
            if reporting_peer in seeding_state[contract_key]:
                state = seeding_state[contract_key][reporting_peer]
                state["upstream"] = None
                state["unsubscribed_reason"] = reason

    elif event_type == "subscription_state":
        # Full snapshot of subscription state for a contract
        contract_key = body.get("key") or body.get("contract_key")
        if contract_key and reporting_peer:
            if contract_key not in seeding_state:
                seeding_state[contract_key] = {}
            seeding_state[contract_key][reporting_peer] = {
                "is_seeding": body.get("is_seeding", False),
                "upstream": body.get("upstream"),
                "downstream": body.get("downstream", []),
                "downstream_count": body.get("downstream_count", 0),
            }

    elif event_type == "peer_startup":
        # Track peer startup with version/arch/OS info
        peer_id = attrs.get("peer_id", "")
        if peer_id:
            peer_lifecycle[peer_id] = {
                "version": body.get("version", "unknown"),
                "arch": body.get("arch", "unknown"),
                "os": body.get("os", "unknown"),
                "os_version": body.get("os_version"),
                "is_gateway": body.get("is_gateway", False),
                "startup_time": timestamp,
                "shutdown_time": None,
                "graceful": None,
            }
    elif event_type == "peer_shutdown":
        # Track peer shutdown
        peer_id = attrs.get("peer_id", "")
        if peer_id and peer_id in peer_lifecycle:
            peer_lifecycle[peer_id]["shutdown_time"] = timestamp
            peer_lifecycle[peer_id]["graceful"] = body.get("graceful", False)
            peer_lifecycle[peer_id]["shutdown_reason"] = body.get("reason")

    # Extract peer info
    this_peer_id, this_ip, this_loc = parse_peer_string(body.get("this_peer", ""))
    other_peer_id, other_ip, other_loc = None, None, None

    # Check various fields for other peer
    for field in ["connected_peer", "target", "requester", "subscriber", "upstream"]:
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
                    "connections": set(),
                    "peer_id": peer_id,  # Store telemetry peer_id for contract_states matching
                }
                updated_peers.append(ip)
            else:
                peers[ip]["location"] = loc
                peers[ip]["last_seen"] = timestamp
                if peer_id:
                    peers[ip]["peer_id"] = peer_id  # Update peer_id if available

            # Track peer presence for historical reconstruction
            if ip not in peer_presence:
                peer_presence[ip] = {
                    "id": anonymize_ip(ip),
                    "ip_hash": ip_hash(ip),
                    "location": loc,
                    "first_seen": timestamp
                }

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
    # For connect events, use specific body_type (start_connection, connected, finished) instead of generic "connect"
    display_event_type = body_type if (event_type == "connect" and body_type) else event_type
    event = {
        "type": "event",
        "timestamp": timestamp,
        "event_type": display_event_type,
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

    # Include state hashes if present (from PR #2492)
    if state_hash:
        event["state_hash"] = state_hash
    if state_hash_before:
        event["state_hash_before"] = state_hash_before
    if state_hash_after:
        event["state_hash_after"] = state_hash_after

    # Include transaction ID for timeline lanes
    if tx_id and tx_id != "00000000000000000000000000":
        event["tx_id"] = tx_id
        # Track this event as part of the transaction (pass body_type for specific connect events)
        track_transaction(tx_id, event_type, timestamp, event["peer_id"], contract_key, body_type)

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
    """Get subscription tree data for all contracts.

    Returns per-peer subscription state so the UI can show which peer
    has which role (seeding, upstream, downstream) in the subscription tree.
    """
    result = {}

    # Get all contract keys from both sources
    all_keys = set(subscriptions.keys()) | set(seeding_state.keys())

    for contract_key in all_keys:
        # Get broadcast tree data (from broadcast_emitted events)
        sub_data = subscriptions.get(contract_key, {"subscribers": set(), "tree": {}})
        tree = {k: list(v) for k, v in sub_data["tree"].items()}

        # Get seeding/subscription state per peer (from new v0.1.70 events)
        # seeding_state[contract_key] is now {peer_id -> state}
        peer_states = seeding_state.get(contract_key, {})

        # Compute aggregate stats across all peers for this contract
        total_downstream = 0
        any_seeding = False
        peers_with_data = []

        for peer_id, state in peer_states.items():
            if state.get("is_seeding"):
                any_seeding = True
            total_downstream += state.get("downstream_count", 0)
            peers_with_data.append({
                "peer_id": peer_id,
                "is_seeding": state.get("is_seeding", False),
                "upstream": state.get("upstream"),
                "downstream": state.get("downstream", []),
                "downstream_count": state.get("downstream_count", 0),
            })

        # Only include contracts with actual data
        if tree or sub_data["subscribers"] or peer_states:
            result[contract_key] = {
                "subscribers": list(sub_data["subscribers"]),
                "tree": tree,
                "short_key": contract_key[:12] + "...",
                # Per-peer state (new structure)
                "peer_states": peers_with_data,
                # Aggregate stats for quick display
                "total_downstream": total_downstream,
                "any_seeding": any_seeding,
                "peer_count": len(peer_states),
            }
    return result


def get_network_state():
    """Get current network state for new clients."""
    import time
    now_ns = time.time_ns()
    # Consider peers stale if not seen in last 5 minutes
    STALE_THRESHOLD_NS = 5 * 60 * 1_000_000_000

    # Filter to only recently active peers
    active_peer_ips = set()
    active_peer_ids = set()  # Track telemetry peer_ids for contract_states filtering
    peer_list = []
    for ip, data in peers.items():
        if is_public_ip(ip):
            last_seen = data.get("last_seen", 0)
            if now_ns - last_seen < STALE_THRESHOLD_NS:
                active_peer_ips.add(ip)
                # Collect telemetry peer_id for contract_states matching
                if data.get("peer_id"):
                    active_peer_ids.add(data["peer_id"])
                peer_list.append({
                    "id": data["id"],
                    "ip_hash": data.get("ip_hash", ip_hash(ip)),
                    "location": data["location"],
                    "peer_id": data.get("peer_id"),  # Include for frontend reference
                })

    # Only include connections between active peers
    conn_list = []
    for conn in connections:
        ips = list(conn)
        if len(ips) == 2 and ips[0] in active_peer_ips and ips[1] in active_peer_ips:
            conn_list.append([anonymize_ip(ips[0]), anonymize_ip(ips[1])])

    # Get active peers from lifecycle data (those with startup but no shutdown)
    active_peers = {
        pid: data for pid, data in peer_lifecycle.items()
        if data.get("shutdown_time") is None
    }

    # Aggregate version stats
    version_counts = {}
    for data in active_peers.values():
        v = data.get("version", "unknown")
        version_counts[v] = version_counts.get(v, 0) + 1

    # Filter contract_states to only include currently active peers (from topology)
    filtered_contract_states = {}
    for contract_key, peer_states in contract_states.items():
        filtered_peers = {
            peer_id: state
            for peer_id, state in peer_states.items()
            if peer_id in active_peer_ids
        }
        if filtered_peers:
            filtered_contract_states[contract_key] = filtered_peers

    return {
        "type": "state",
        "peers": peer_list,
        "connections": conn_list,
        "subscriptions": get_subscription_trees(),
        "contract_states": filtered_contract_states,
        "op_stats": get_operation_stats(),
        "peer_lifecycle": {
            "active_count": len(active_peers),
            "gateway_count": sum(1 for d in active_peers.values() if d.get("is_gateway")),
            "versions": version_counts,
            "peers": list(active_peers.values())[:50],  # Limit to last 50 for display
        },
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
    # Sort events by timestamp for proper timeline display
    sorted_events = sorted(event_history, key=lambda e: e["timestamp"])

    # Sort peer presence by first_seen for historical reconstruction
    sorted_presence = sorted(peer_presence.values(), key=lambda p: p["first_seen"])

    return {
        "type": "history",
        "events": sorted_events,
        "transactions": get_transactions_list(),
        "peer_presence": sorted_presence,
        "time_range": {
            "start": sorted_events[0]["timestamp"] if sorted_events else 0,
            "end": sorted_events[-1]["timestamp"] if sorted_events else 0,
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

# Store client IPs from X-Forwarded-For headers (keyed by connection id)
client_real_ips = {}


async def process_request(connection, request):
    """Capture X-Forwarded-For header before WebSocket handshake."""
    # Store the real client IP for later use in handle_client
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if forwarded_for:
        real_ip = forwarded_for.split(",")[0].strip()
        client_real_ips[id(connection)] = real_ip
    return None  # Continue with normal WebSocket handling


async def handle_client(websocket):
    """Handle a WebSocket client connection."""
    clients.add(websocket)

    # Get client IP - check stored X-Forwarded-For first, then fall back to remote_address
    client_ip = client_real_ips.pop(id(websocket), None)
    if not client_ip and websocket.remote_address:
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

    # Start WebSocket server with compression enabled
    # permessage-deflate provides ~40x compression for JSON data
    print(f"Starting WebSocket server on port {WS_PORT}...")
    async with websockets.serve(
        handle_client,
        "0.0.0.0",
        WS_PORT,
        compression="deflate",  # Enable per-message deflate compression
        max_size=50 * 1024 * 1024,  # 50MB max message size for large history
        process_request=process_request,  # Capture X-Forwarded-For headers
    ):
        # Start log tailer
        await tail_log()


if __name__ == "__main__":
    asyncio.run(main())
