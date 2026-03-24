/**
 * Utility functions for the Freenet Dashboard
 * Pure functions with no external dependencies
 */

// Event type classification
export function getEventClass(eventType) {
    if (!eventType) return 'other';
    if (eventType.includes('connect') || eventType === 'start_connection' || eventType === 'finished') return 'connect';
    if (eventType.includes('put')) return 'put';
    if (eventType.includes('get')) return 'get';
    if (eventType.includes('update') || eventType.includes('broadcast')) return 'update';
    if (eventType.includes('subscrib')) return 'subscribe';
    if (eventType.includes('transfer')) return 'transfer';
    return 'other';
}

// User-friendly labels for event types
export function getEventLabel(eventType) {
    const labels = {
        'start_connection': 'connecting',
        'connected': 'connected',
        'connect_rejected': 'conn reject',
        'finished': 'conn done',
        'put_request': 'put req',
        'put_success': 'put ok',
        'get_request': 'get req',
        'get_success': 'get ok',
        'get_not_found': 'get 404',
        'get_failure': 'get fail',
        'update_request': 'update req',
        'update_success': 'update ok',
        'update_failure': 'update fail',
        'subscribe_request': 'sub req',
        'subscribe_success': 'sub ok',
        'subscribe_not_found': 'sub 404',
        'subscribed': 'subscribed',
        'broadcast_emitted': 'broadcast',
        'broadcast_applied': 'applied',
    };
    return labels[eventType] || eventType;
}

// Convert state hash to deterministic HSL color
export function hashToColor(hash) {
    if (!hash) return null;
    const hue = parseInt(hash.substring(0, 6), 16) % 360;
    return {
        fill: `hsl(${hue}, 70%, 50%)`,
        glow: `hsla(${hue}, 70%, 50%, 0.3)`
    };
}

// Base58 decoding (Bitcoin style)
const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

export function decodeBase58(str) {
    const bytes = [];
    for (let i = 0; i < str.length; i++) {
        const c = str[i];
        const charIndex = BASE58_ALPHABET.indexOf(c);
        if (charIndex === -1) continue;

        let carry = charIndex;
        for (let j = 0; j < bytes.length; j++) {
            carry += bytes[j] * 58;
            bytes[j] = carry & 0xff;
            carry >>= 8;
        }
        while (carry > 0) {
            bytes.push(carry & 0xff);
            carry >>= 8;
        }
    }
    for (let i = 0; i < str.length && str[i] === '1'; i++) {
        bytes.push(0);
    }
    return bytes.reverse();
}

// Convert contract key to ring location (matches Rust implementation)
export function contractKeyToLocation(contractKey) {
    if (!contractKey) return null;
    const bytes = decodeBase58(contractKey);
    if (bytes.length === 0) return null;

    let value = 0.0;
    let divisor = 256.0;
    for (const byte of bytes) {
        value += byte / divisor;
        divisor *= 256.0;
    }
    return Math.min(Math.max(value, 0.0), 1.0);
}

// Time formatting
export function formatRelativeTime(tsNano) {
    if (!tsNano) return null;
    const now = Date.now();
    const then = tsNano / 1_000_000;
    const diffMs = now - then;

    if (diffMs < 60000) return 'just now';
    if (diffMs < 3600000) return Math.floor(diffMs / 60000) + 'm ago';
    if (diffMs < 86400000) return Math.floor(diffMs / 3600000) + 'h ago';
    return Math.floor(diffMs / 86400000) + 'd ago';
}

export function formatTime(tsNano) {
    return new Date(tsNano / 1_000_000).toLocaleTimeString();
}

export function formatDate(tsNano) {
    return new Date(tsNano / 1_000_000).toLocaleDateString(undefined, {
        month: 'short', day: 'numeric'
    });
}

export function formatLatency(ms) {
    if (ms === null || ms === undefined) return '-';
    if (ms < 1000) return Math.round(ms) + 'ms';
    return (ms / 1000).toFixed(1) + 's';
}

// Rate classification for success rates
export function getRateClass(rate) {
    if (rate === null || rate === undefined) return '';
    if (rate >= 90) return 'good';
    if (rate >= 50) return 'warn';
    return 'bad';
}

// Contract activity index: Map<contractKey, {lastUpdate, totalUpdates, timestamps: number[]}>
// Populated incrementally to avoid O(contracts * events) scanning on every render.
const activityIndex = new Map();

/**
 * Index a single event for the contract activity cache.
 * Call this whenever an event is added to state.allEvents.
 * @param {Object} event - The event object
 */
export function indexEventForActivity(event) {
    if (event.event_type !== 'update_success' && event.event_type !== 'put_success') return;
    const key = event.contract_full;
    if (!key) return;

    let entry = activityIndex.get(key);
    if (!entry) {
        entry = { lastUpdate: null, totalUpdates: 0, timestamps: [] };
        activityIndex.set(key, entry);
    }
    entry.totalUpdates++;
    entry.timestamps.push(event.timestamp);
    if (entry.lastUpdate === null || event.timestamp > entry.lastUpdate) {
        entry.lastUpdate = event.timestamp;
    }
}

/**
 * Clear the activity index. Call when events are pruned/trimmed
 * (e.g., when allEvents is reset or old events are spliced out).
 */
export function clearActivityIndex() {
    activityIndex.clear();
}

/**
 * Rebuild the activity index from a full array of events.
 * Use after bulk-loading history.
 * @param {Array} allEvents - All events to index
 */
export function rebuildActivityIndex(allEvents) {
    activityIndex.clear();
    for (const event of allEvents) {
        indexEventForActivity(event);
    }
}

// Calculate contract activity stats from the pre-built index
export function getContractActivity(contractKey, _allEvents) {
    const entry = activityIndex.get(contractKey);
    if (!entry || entry.totalUpdates === 0) {
        return { lastUpdate: null, totalUpdates: 0, recentUpdates: 0 };
    }

    // Count recent updates (within 1 hour)
    const oneHourAgo = Date.now() * 1_000_000 - (60 * 60 * 1000 * 1_000_000);
    let recentUpdates = 0;
    // Timestamps are in insertion order (roughly chronological), scan from end
    for (let i = entry.timestamps.length - 1; i >= 0; i--) {
        if (entry.timestamps[i] > oneHourAgo) {
            recentUpdates++;
        } else {
            break;
        }
    }

    return { lastUpdate: entry.lastUpdate, totalUpdates: entry.totalUpdates, recentUpdates };
}
