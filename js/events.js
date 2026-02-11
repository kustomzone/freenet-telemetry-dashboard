/**
 * Events module for Freenet Dashboard
 * Handles event selection, filtering, display, and URL state
 */

import { state } from './state.js';
import { getEventClass, getEventLabel, formatTime } from './utils.js';

// URL state tracking
let urlLoaded = false;

/**
 * Select an event and optionally highlight related peers
 * @param {Object} event - Event object to select
 * @param {Function} goToTime - Callback to navigate to event time
 * @param {Function} goLive - Callback to return to live mode
 * @param {Function} updateView - Callback to refresh the view
 */
export function selectEvent(event, goToTime, goLive, updateView) {
    state.highlightedPeers.clear();

    // Toggle: clicking same event deselects it
    if (state.selectedEvent === event) {
        state.selectedEvent = null;
        state.selectedContract = null;
        goLive();
        return;
    }

    state.selectedEvent = event;

    if (event) {
        // Move playhead to event time
        goToTime(event.timestamp);

        // Highlight the event's peer
        if (event.peer_id) {
            state.highlightedPeers.add(event.peer_id);
        }

        // Also highlight connection peers
        if (event.connection) {
            state.highlightedPeers.add(event.connection[0]);
            state.highlightedPeers.add(event.connection[1]);
        }

        // If event has a contract with subscription tree, select it
        if (event.contract_full && state.contractData[event.contract_full]) {
            state.selectedContract = event.contract_full;
        } else {
            state.selectedContract = null;
        }
    }

    updateView();
}

/**
 * Select a peer to filter events
 * @param {string} peerId - Peer ID to select
 * @param {Function} updateView - Callback to refresh the view
 * @param {Function} updateURL - Callback to update URL state
 */
export function selectPeer(peerId, updateView, updateURL) {
    if (state.selectedPeerId === peerId) {
        state.selectedPeerId = null;
    } else {
        state.selectedPeerId = peerId;
    }
    updateFilterBar();
    updateView();
    updateURL();
}

/**
 * Toggle peer filter
 */
export function togglePeerFilter(peerId, updateView, updateURL) {
    if (state.selectedPeerId === peerId) {
        state.selectedPeerId = null;
    } else {
        state.selectedPeerId = peerId;
    }
    updateFilterBar();
    updateView();
    updateURL();
}

/**
 * Toggle transaction filter
 */
export function toggleTxFilter(txId, updateView, updateURL) {
    if (state.selectedTxId === txId) {
        state.selectedTxId = null;
    } else {
        state.selectedTxId = txId;
    }
    updateFilterBar();
    updateView();
    updateURL();
}

/**
 * Clear peer selection
 */
export function clearPeerSelection(updateView) {
    state.selectedPeerId = null;
    updateView();
}

/**
 * Update the filter bar display
 */
export function updateFilterBar() {
    const chipsContainer = document.getElementById('filter-chips');
    const noFilters = document.getElementById('no-filters');
    const clearAllBtn = document.getElementById('clear-all-btn');

    let chips = [];

    if (state.selectedPeerId) {
        // Show peer name or "My Peer" for own peer, with connection count
        const isYourPeer = state.selectedPeerId === state.yourPeerId;
        const selectedPeerData = state.initialStatePeers?.find(p => p.id === state.selectedPeerId);
        const peerName = selectedPeerData?.ip_hash ? state.peerNames[selectedPeerData.ip_hash] : null;
        let peerLabel = peerName || state.selectedPeerId.substring(0, 12) + '...';
        if (isYourPeer && !peerName) peerLabel = 'My Peer';

        // Count connections for this peer
        let connCount = 0;
        for (const conn of state.initialStateConnections) {
            if (conn[0] === state.selectedPeerId || conn[1] === state.selectedPeerId) connCount++;
        }
        const connInfo = connCount > 0 ? ` (${connCount} connections)` : '';
        chips.push(`<span class="filter-chip peer">${peerLabel}${connInfo}<button class="filter-chip-close" onclick="clearPeerFilter()">×</button></span>`);
    }

    if (state.selectedTxId) {
        chips.push(`<span class="filter-chip tx">Tx: ${state.selectedTxId.substring(0, 8)}...<button class="filter-chip-close" onclick="clearTxFilter()">×</button></span>`);
    }

    if (state.selectedContract && state.contractData[state.selectedContract]) {
        const shortKey = state.contractData[state.selectedContract].short_key;
        chips.push(`<span class="filter-chip contract">Contract: ${shortKey}<button class="filter-chip-close" onclick="clearContractFilter()">×</button></span>`);
    }

    chipsContainer.innerHTML = chips.join('');

    const hasFilters = chips.length > 0 || state.filterText;
    noFilters.style.display = hasFilters ? 'none' : 'inline';
    clearAllBtn.style.display = hasFilters ? 'inline-block' : 'none';
}

/**
 * Clear peer filter
 */
export function clearPeerFilter(updateView, updateURL) {
    state.selectedPeerId = null;
    updateFilterBar();
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Clear transaction filter
 */
export function clearTxFilter(updateView, updateURL) {
    state.selectedTxId = null;
    updateFilterBar();
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Clear contract filter
 */
export function clearContractFilter(updateView, updateURL) {
    state.selectedContract = null;
    updateFilterBar();
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Clear all filters
 */
export function clearAllFilters(updateView, updateURL) {
    state.selectedPeerId = null;
    state.selectedTxId = null;
    state.selectedContract = null;
    state.filterText = '';
    updateFilterBar();
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Handle event click in the events panel
 */
export function handleEventClick(idx, callbacks) {
    if (state.displayedEvents && state.displayedEvents[idx]) {
        selectEvent(state.displayedEvents[idx], callbacks.goToTime, callbacks.goLive, callbacks.updateView);
    }
}

/**
 * Handle event hover for peer highlighting
 */
export function handleEventHover(idx, updateView) {
    if (idx === null) {
        state.hoveredEvent = null;
    } else if (state.displayedEvents && state.displayedEvents[idx]) {
        state.hoveredEvent = state.displayedEvents[idx];
    }
    updateView();
}

/**
 * Render the events panel
 * @param {Array} nearbyEvents - Events to display
 */
export function renderEventsPanel(nearbyEvents) {
    const eventsPanel = document.getElementById('events-panel');

    // Update events title based on filtering
    const eventsTitle = document.getElementById('events-title');
    if (state.selectedContract && state.contractData[state.selectedContract]) {
        eventsTitle.textContent = `Events for ${state.contractData[state.selectedContract].short_key}`;
    } else if (state.selectedPeerId) {
        eventsTitle.textContent = `Events for ${state.selectedPeerId.substring(0, 12)}...`;
    } else {
        eventsTitle.textContent = 'Events';
    }

    if (nearbyEvents.length === 0) {
        state.displayedEvents = [];
        const emptyMsg = state.selectedContract
            ? 'No subscription events in this time range'
            : 'No events in this time range';
        eventsPanel.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">&#8709;</div>
                <div>${emptyMsg}</div>
            </div>
        `;
        return;
    }

    // Add sticky header row
    const headerHtml = `
        <div class="events-header">
            <span class="header-time">Time</span>
            <span class="header-type">Type</span>
            <span class="header-peers">Peers (from -> to)</span>
            <span class="header-tx">Tx</span>
        </div>
    `;

    const eventsHtml = nearbyEvents.map((e, idx) => {
        const isSelected = state.selectedEvent &&
            state.selectedEvent.timestamp === e.timestamp &&
            state.selectedEvent.peer_id === e.peer_id;
        const classes = ['event-item'];
        if (isSelected) classes.push('selected');

        // Build peer display: from_peer -> to_peer or just peer_id
        let peersHtml = '';
        const fromPeer = e.from_peer || e.peer_id;
        const toPeer = e.to_peer;
        const fromActive = state.selectedPeerId === fromPeer ? ' active' : '';
        const toActive = toPeer && state.selectedPeerId === toPeer ? ' active' : '';

        if (toPeer && toPeer !== fromPeer) {
            peersHtml = `
                <span class="event-filter-link${fromActive}" onclick="event.stopPropagation(); togglePeerFilter('${fromPeer}')">${fromPeer.substring(0, 12)}</span>
                <span class="event-arrow">-></span>
                <span class="event-filter-link${toActive}" onclick="event.stopPropagation(); togglePeerFilter('${toPeer}')">${toPeer.substring(0, 12)}</span>
            `;
        } else {
            peersHtml = `<span class="event-filter-link${fromActive}" onclick="event.stopPropagation(); togglePeerFilter('${fromPeer}')">${fromPeer.substring(0, 12)}</span>`;
        }

        // Transaction ID (shortened)
        const txActive = e.tx_id && state.selectedTxId === e.tx_id ? ' active' : '';
        const txHtml = e.tx_id ? `<span class="event-tx event-filter-link${txActive}" onclick="event.stopPropagation(); toggleTxFilter('${e.tx_id}')">${e.tx_id.substring(0, 8)}</span>` : '';

        // State hash display
        let stateHashHtml = '';
        if (e.state_hash_before && e.state_hash_after) {
            stateHashHtml = `<span class="state-hash">[${e.state_hash_before.substring(0, 4)}->${e.state_hash_after.substring(0, 4)}]</span>`;
        } else if (e.state_hash) {
            stateHashHtml = `<span class="state-hash">[${e.state_hash.substring(0, 4)}]</span>`;
        }

        return `
            <div class="${classes.join(' ')}" data-event-idx="${idx}" onclick="handleEventClick(${idx})" onmouseenter="handleEventHover(${idx})" onmouseleave="handleEventHover(null)">
                <span class="event-time">${e.time_str}</span>
                <span class="event-badge ${getEventClass(e.event_type)}">${getEventLabel(e.event_type)}</span>
                <div class="event-peers">${peersHtml}</div>
                ${stateHashHtml}
                ${txHtml}
            </div>
        `;
    }).reverse().join('');

    eventsPanel.innerHTML = headerHtml + eventsHtml;
    state.displayedEvents = nearbyEvents;
}

/**
 * Filter events based on current state
 * @returns {Array} Filtered events
 */
export function filterEvents() {
    // Performance: Use binary search to find time window instead of scanning all events.
    // Events are roughly time-ordered (appended as they arrive).
    const targetStart = state.currentTime - state.timeWindowNs;
    const targetEnd = state.currentTime + state.timeWindowNs;

    // Find start index using binary search on timestamps
    let lo = 0, hi = state.allEvents.length;
    while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (state.allEvents[mid].timestamp < targetStart) lo = mid + 1;
        else hi = mid;
    }

    // Collect matching events from the time window (scan forward from start index)
    const results = [];
    for (let i = lo; i < state.allEvents.length && results.length < 200; i++) {
        const e = state.allEvents[i];
        if (e.timestamp > targetEnd) break;

        // Apply all filters in a single pass
        if (state.selectedPeerId) {
            if (e.peer_id !== state.selectedPeerId &&
                e.from_peer !== state.selectedPeerId &&
                e.to_peer !== state.selectedPeerId &&
                !(e.connection && (e.connection[0] === state.selectedPeerId || e.connection[1] === state.selectedPeerId))) {
                continue;
            }
        }
        if (state.selectedTxId && e.tx_id !== state.selectedTxId) continue;
        if (state.selectedContract && e.contract_full !== state.selectedContract) continue;
        if (state.filterText) {
            const filter = state.filterText.toLowerCase();
            if (!(e.event_type && e.event_type.toLowerCase().includes(filter)) &&
                !(e.peer_id && e.peer_id.toLowerCase().includes(filter)) &&
                !(e.contract && e.contract.toLowerCase().includes(filter))) {
                continue;
            }
        }
        results.push(e);
    }

    return results.slice(-30);
}

/**
 * Update URL with current state
 */
export function updateURL() {
    if (!urlLoaded) return;

    const params = new URLSearchParams();

    if (state.selectedContract) {
        params.set('contract', state.selectedContract.substring(0, 16));
    }
    if (state.selectedPeerId) {
        params.set('peer', state.selectedPeerId.substring(0, 16));
    }
    if (state.selectedTxId) {
        params.set('tx', state.selectedTxId.substring(0, 12));
    }
    if (state.activeTab !== 'events') {
        params.set('tab', state.activeTab);
    }
    if (!state.isLive && state.currentTime) {
        params.set('time', new Date(state.currentTime / 1_000_000).toISOString());
    }

    const queryString = params.toString();
    const newUrl = queryString ? `?${queryString}` : window.location.pathname;
    history.replaceState(null, '', newUrl);
}

/**
 * Load state from URL
 * @param {Function} switchTab - Callback to switch tabs
 * @param {Function} updateView - Callback to refresh view
 */
export function loadFromURL(switchTab, updateView) {
    const params = new URLSearchParams(window.location.search);

    // Restore contract selection
    const contractParam = params.get('contract');
    if (contractParam && state.contractData) {
        const match = Object.keys(state.contractData).find(k => k.startsWith(contractParam));
        if (match) {
            state.selectedContract = match;
            console.log('Restored contract from URL:', match.substring(0, 16));
        }
    }

    // Restore peer selection
    const peerParam = params.get('peer');
    if (peerParam && state.initialStatePeers) {
        const match = state.initialStatePeers.find(p => p.id && p.id.startsWith(peerParam));
        if (match) {
            state.selectedPeerId = match.id;
            console.log('Restored peer from URL:', match.id.substring(0, 16));
        }
    }

    // Restore transaction filter
    const txParam = params.get('tx');
    if (txParam && state.allTransactions) {
        const match = state.allTransactions.find(t => t.tx_id && t.tx_id.startsWith(txParam));
        if (match) {
            state.selectedTxId = match.tx_id;
            console.log('Restored tx from URL:', match.tx_id.substring(0, 12));
        }
    }

    // Restore tab
    const tabParam = params.get('tab');
    if (tabParam && ['events', 'contracts', 'transactions', 'peers'].includes(tabParam)) {
        switchTab(tabParam);
    }

    // Restore time
    const timeParam = params.get('time');
    if (timeParam) {
        try {
            const timestamp = new Date(timeParam).getTime() * 1_000_000;
            if (!isNaN(timestamp) && timestamp > 0) {
                state.currentTime = timestamp;
                state.isLive = false;
                const liveBtn = document.getElementById('live-btn');
                if (liveBtn) liveBtn.classList.remove('active');
                console.log('Restored time from URL:', timeParam);
            }
        } catch (e) {
            console.warn('Invalid time in URL:', timeParam);
        }
    }

    urlLoaded = true;
    updateFilterBar();
    updateView();
}

/**
 * Mark URL as loaded (call after initial data load)
 */
export function markURLLoaded() {
    urlLoaded = true;
}

/**
 * Check if URL has been loaded
 */
export function isURLLoaded() {
    return urlLoaded;
}

/**
 * Track a transaction from an incoming event
 */
export function trackTransactionFromEvent(event) {
    const txId = event.tx_id;
    if (!txId || txId === '00000000000000000000000000') return;

    const eventType = event.event_type || '';
    const timestamp = event.timestamp;

    // Determine operation type and status
    let op = 'other';
    let isStart = false;
    let isEnd = false;
    let status = null;

    if (eventType.startsWith('put_')) {
        op = 'put';
        if (eventType === 'put_request') isStart = true;
        else if (eventType === 'put_success') { isEnd = true; status = 'success'; }
    } else if (eventType.startsWith('get_')) {
        op = 'get';
        if (eventType === 'get_request') isStart = true;
        else if (eventType === 'get_success') { isEnd = true; status = 'success'; }
        else if (eventType === 'get_not_found') { isEnd = true; status = 'not_found'; }
    } else if (eventType.startsWith('update_')) {
        op = 'update';
        if (eventType === 'update_request') isStart = true;
        else if (eventType === 'update_success') { isEnd = true; status = 'success'; }
    } else if (eventType.startsWith('subscribe')) {
        op = 'subscribe';
        if (eventType === 'subscribe_request') isStart = true;
        else if (eventType === 'subscribed') { isEnd = true; status = 'success'; }
    } else if (eventType.includes('connect')) {
        op = 'connect';
        if (eventType === 'connect_request_sent') isStart = true;
        else if (eventType === 'connect_connected') { isEnd = true; status = 'success'; }
    } else if (eventType === 'disconnect') {
        op = 'disconnect';
        isStart = true; isEnd = true; status = 'complete';
    }

    // Check if transaction already exists
    if (state.transactionMap.has(txId)) {
        const idx = state.transactionMap.get(txId);
        const tx = state.allTransactions[idx];

        // Add event to transaction
        tx.events.push({
            event_type: eventType,
            timestamp: timestamp,
            peer_id: event.peer_id
        });
        tx.event_count = tx.events.length;

        // Update end time and status
        if (timestamp > tx.end_ns) {
            tx.end_ns = timestamp;
        }
        if (isEnd && status) {
            tx.status = status;
            tx.duration_ms = (tx.end_ns - tx.start_ns) / 1_000_000;
        }
    } else {
        // Create new transaction
        const newTx = {
            tx_id: txId,
            op: op,
            contract: event.contract_full ? event.contract_full.substring(0, 12) + '...' : null,
            contract_full: event.contract_full || null,
            start_ns: timestamp,
            end_ns: timestamp,
            duration_ms: null,
            status: (isStart && !isEnd) ? 'pending' : (status || 'complete'),
            event_count: 1,
            events: [{
                event_type: eventType,
                timestamp: timestamp,
                peer_id: event.peer_id
            }]
        };
        state.transactionMap.set(txId, state.allTransactions.length);
        state.allTransactions.push(newTx);

        // Prune old transactions to prevent unbounded memory growth
        const MAX_TRANSACTIONS = 5000;
        if (state.allTransactions.length > MAX_TRANSACTIONS * 1.1) {
            const removeCount = state.allTransactions.length - MAX_TRANSACTIONS;
            const removed = state.allTransactions.splice(0, removeCount);
            removed.forEach(tx => state.transactionMap.delete(tx.tx_id));
            state.transactionMap.clear();
            state.allTransactions.forEach((tx, idx) => state.transactionMap.set(tx.tx_id, idx));
        }
    }
}
