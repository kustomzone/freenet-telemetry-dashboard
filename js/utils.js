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
    return 'other';
}

// User-friendly labels for event types
export function getEventLabel(eventType) {
    const labels = {
        'start_connection': 'connecting',
        'connected': 'connected',
        'finished': 'conn done',
        'put_request': 'put req',
        'put_success': 'put ok',
        'get_request': 'get req',
        'get_success': 'get ok',
        'get_not_found': 'get 404',
        'update_request': 'update req',
        'update_success': 'update ok',
        'subscribe_request': 'sub req',
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

// Calculate contract activity stats from events
export function getContractActivity(contractKey, allEvents) {
    // Only count update_success/put_success - these represent actual state changes
    // Don't count broadcast_received which is just propagation (one change = many receives)
    const contractEvents = allEvents.filter(e =>
        e.contract_full === contractKey &&
        (e.event_type === 'update_success' || e.event_type === 'put_success')
    );

    if (contractEvents.length === 0) {
        return { lastUpdate: null, totalUpdates: 0, recentUpdates: 0 };
    }

    // Sort by timestamp descending to get latest
    contractEvents.sort((a, b) => b.timestamp - a.timestamp);
    const lastUpdate = contractEvents[0].timestamp;
    const totalUpdates = contractEvents.length;

    // Count recent updates (within 1 hour)
    const oneHourAgo = Date.now() * 1_000_000 - (60 * 60 * 1000 * 1_000_000);
    const recentUpdates = contractEvents.filter(e => e.timestamp > oneHourAgo).length;

    return { lastUpdate, totalUpdates, recentUpdates };
}
