/**
 * Freenet Dashboard - Main Application Entry Point
 * Imports all modules and initializes the application
 */

// Import modules
import { state, SVG_SIZE, SVG_WIDTH, CENTER, RADIUS } from './state.js';
import { getEventClass, getEventLabel, formatTime } from './utils.js';
import { updateRingSVG, getSubscriptionTreeInfo } from './topology.js';
import {
    renderRuler, renderDetailTimeline, renderTimeline,
    updatePlayhead, updateWindowLabel, setupTimeline,
    goLive as timelineGoLive, goToTime as timelineGoToTime,
    addEventMarker
} from './timeline.js';
import {
    selectEvent, selectPeer, togglePeerFilter, toggleTxFilter,
    clearPeerSelection, updateFilterBar, clearPeerFilter,
    clearTxFilter, clearContractFilter, clearAllFilters as eventsClearAllFilters,
    handleEventClick as eventsHandleEventClick, handleEventHover as eventsHandleEventHover,
    renderEventsPanel, filterEvents, updateURL, loadFromURL, markURLLoaded,
    isURLLoaded, trackTransactionFromEvent
} from './events.js';
import {
    selectContract as contractsSelectContract, renderContractsList,
    showTransactionDetail, closeTransactionDetail,
    switchTab as contractsSwitchTab, updateContractDropdown
} from './contracts.js';
import {
    connect, showPeerNamingPrompt, closePeerNamingPrompt,
    reconstructStateAtTime
} from './websocket.js';
import { initTransferChart, addTransferEvents, addTransferEvent, renderTransferChart } from './transfers.js';

// ============================================================================
// Main Application Functions
// ============================================================================

/**
 * Main view update function - coordinates all UI updates
 */
function updateView() {
    let peers = new Map();
    let connections = new Set();

    // For live view, use direct state data
    // For historical view, reconstruct from events
    if (state.isLive && state.initialStatePeers.length > 0) {
        for (const p of state.initialStatePeers) {
            peers.set(p.id, {
                location: p.location,
                ip_hash: p.ip_hash,
                peer_id: p.peer_id
            });
        }
        for (const conn of state.initialStateConnections) {
            const key = [conn[0], conn[1]].sort().join('|');
            connections.add(key);
        }
    } else {
        const reconstructed = reconstructStateAtTime(state.currentTime);
        peers = reconstructed.peers;
        connections = reconstructed.connections;
    }

    // Get subscriber peer IDs for highlighting
    let subscriberPeerIds = new Set();
    if (state.selectedContract && state.contractData[state.selectedContract]) {
        const subData = state.contractData[state.selectedContract];
        if (subData.subscribers) {
            subData.subscribers.forEach(id => subscriberPeerIds.add(id));
        }
        if (subData.peer_states) {
            subData.peer_states.forEach(ps => {
                for (const [topoId, topoPeer] of peers) {
                    if (topoPeer.peer_id === ps.peer_id) {
                        subscriberPeerIds.add(topoId);
                        break;
                    }
                }
            });
        }
    }

    // Update ring visualization
    updateRingSVG(peers, connections, subscriberPeerIds, {
        selectPeer: (peerId) => selectPeer(peerId, updateView, updateURL),
        goToTime: goToTime
    });

    // Update stats
    document.getElementById('peer-count').textContent = peers.size;
    document.getElementById('connection-count').textContent = connections.size;

    // Update topology subtitle
    const topoSubtitle = document.querySelector('.panel-subtitle');
    if (topoSubtitle) {
        if (state.selectedContract && state.contractData[state.selectedContract]) {
            const subData = state.contractData[state.selectedContract];
            const peerStates = subData.peer_states || [];
            const totalSubs = peerStates.length;
            const visibleSubs = [...subscriberPeerIds].filter(id => peers.has(id)).length;

            const treeInfo = getSubscriptionTreeInfo(state.selectedContract, peers, connections);
            const proximityCount = treeInfo.proximityLinks.length;

            if (totalSubs === 0) {
                topoSubtitle.textContent = 'No peers subscribed to this contract.';
            } else {
                let parts = [`${visibleSubs} subscribed (pink)`];
                const linkParts = [];
                if (treeInfo.nodes > 0) linkParts.push('pink = subscription');
                if (proximityCount > 0) linkParts.push('cyan = proximity');
                if (linkParts.length > 0) parts.push(`Links: ${linkParts.join(', ')}`);
                topoSubtitle.textContent = parts.join(' . ');
            }
        } else {
            topoSubtitle.textContent = 'Peers arranged by their network location (0.0-1.0). Click a peer to filter events.';
        }
    }

    // Filter and render events
    const nearbyEvents = filterEvents();
    renderEventsPanel(nearbyEvents);

    // Update event count
    document.getElementById('event-count').textContent = state.allEvents.filter(e => e.timestamp <= state.currentTime).length;

    // Update playhead
    updatePlayhead();

    // Update contracts list if that tab is active
    if (state.activeTab === 'contracts') {
        renderContractsList();
    }
}

/**
 * Go to live mode
 */
function goLive() {
    timelineGoLive(updateView, updateURL);
}

/**
 * Go to specific time (historical mode)
 */
function goToTime(time) {
    timelineGoToTime(time, updateView, updateURL);
}

/**
 * Switch between tabs
 */
function switchTab(tabName) {
    contractsSwitchTab(tabName, updateURL);
}

/**
 * Select a contract
 */
function selectContract(contractKey) {
    contractsSelectContract(contractKey, updateView, updateURL);
}

/**
 * Clear all filters
 */
function clearAllFilters() {
    eventsClearAllFilters(updateView, updateURL);
}

/**
 * Handle event click in events panel
 */
function handleEventClick(idx) {
    eventsHandleEventClick(idx, {
        goToTime: goToTime,
        goLive: goLive,
        updateView: updateView
    });
}

/**
 * Handle event hover
 */
function handleEventHover(idx) {
    eventsHandleEventHover(idx, updateView);
}

// ============================================================================
// Global Window Bindings (for onclick handlers in HTML)
// ============================================================================

window.goLive = goLive;
window.goToTime = goToTime;
window.switchTab = switchTab;
window.selectContract = selectContract;
window.clearAllFilters = clearAllFilters;
window.handleEventClick = handleEventClick;
window.handleEventHover = handleEventHover;
window.togglePeerFilter = (peerId) => togglePeerFilter(peerId, updateView, updateURL);
window.toggleTxFilter = (txId) => toggleTxFilter(txId, updateView, updateURL);
window.clearPeerFilter = () => clearPeerFilter(updateView, updateURL);
window.clearTxFilter = () => clearTxFilter(updateView, updateURL);
window.clearContractFilter = () => clearContractFilter(updateView, updateURL);
window.closeTransactionDetail = () => closeTransactionDetail(updateView);
window.showTransactionDetail = (tx) => showTransactionDetail(tx, switchTab, updateView, updateURL);

// ============================================================================
// Initialization
// ============================================================================

// Setup timeline interactions
setupTimeline({
    goToTime: goToTime,
    goLive: goLive,
    updateView: updateView,
    renderContractsList: renderContractsList
});

// Connect to WebSocket
connect({
    updateView: updateView,
    renderTimeline: renderTimeline,
    renderRuler: renderRuler,
    showPeerNamingPrompt: () => showPeerNamingPrompt(),
    updateContractDropdown: updateContractDropdown,
    trackTransactionFromEvent: trackTransactionFromEvent,
    addEventMarker: addEventMarker,
    loadFromURL: () => {
        if (!isURLLoaded()) {
            loadFromURL(switchTab, updateView);
        }
    }
});

// Mark URL as loaded after initial setup
markURLLoaded();

// Initialize transfer chart
initTransferChart();

console.log('Freenet Dashboard initialized (modular)');
