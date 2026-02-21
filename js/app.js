/**
 * Freenet Dashboard - Main Application Entry Point
 * Imports all modules and initializes the application
 */

// Import modules
import { state, SVG_SIZE, SVG_WIDTH, CENTER, RADIUS } from './state.js';
import { getEventClass, getEventLabel, formatTime } from './utils.js';
import { updateRingSVG } from './topology.js';
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
import { updateContractTree, getTreeStats, resetContractTree } from './contract-tree.js';

// ============================================================================
// Main Application Functions
// ============================================================================

// rAF-based throttle: updateView runs at most once per animation frame
let _updateViewScheduled = false;

function scheduleUpdateView() {
    if (_updateViewScheduled) return;
    _updateViewScheduled = true;
    requestAnimationFrame(() => {
        _updateViewScheduled = false;
        _updateViewImpl();
    });
}

/**
 * Main view update function - coordinates all UI updates
 */
function _updateViewImpl() {
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
            // Build O(1) lookup map: peer_id → topoId (avoids O(n²) nested loop)
            const peerIdToTopoId = new Map();
            for (const [topoId, topoPeer] of peers) {
                if (topoPeer.peer_id) peerIdToTopoId.set(topoPeer.peer_id, topoId);
            }
            subData.peer_states.forEach(ps => {
                const topoId = peerIdToTopoId.get(ps.peer_id);
                if (topoId) subscriberPeerIds.add(topoId);
            });
        }
    }

    // Panel swap: show tree when contract selected, ring otherwise
    const ringContainer = document.getElementById('ring-container');
    const treeContainer = document.getElementById('tree-container');
    const ringLegend = document.getElementById('ring-legend');
    const treeLegend = document.getElementById('tree-legend');
    const topoPanelTitle = document.querySelector('.panel-title');
    const overlayIds = ['dist-chart-container', 'transfer-chart-container', 'transfer-backdrop', 'transfer-tooltip'];

    if (state.selectedContract && state.contractData[state.selectedContract]) {
        // Show contract tree, hide ring + overlays
        ringContainer.style.display = 'none';
        treeContainer.style.display = 'flex';
        if (ringLegend) ringLegend.style.display = 'none';
        if (treeLegend) treeLegend.style.display = 'flex';
        overlayIds.forEach(id => { const el = document.getElementById(id); if (el) el.style.display = 'none'; });
        const shortKey = state.contractData[state.selectedContract].short_key || state.selectedContract.substring(0, 12);
        if (topoPanelTitle) topoPanelTitle.textContent = 'Contract Topology: ' + shortKey;

        updateContractTree(treeContainer, peers, subscriberPeerIds, {
            selectPeer: (id) => selectPeer(id, updateView, updateURL),
            goToTime: goToTime
        });
    } else {
        // Show ring, hide tree
        ringContainer.style.display = '';
        treeContainer.style.display = 'none';
        if (ringLegend) ringLegend.style.display = 'flex';
        if (treeLegend) treeLegend.style.display = 'none';
        overlayIds.forEach(id => { const el = document.getElementById(id); if (el) el.style.display = ''; });
        if (topoPanelTitle) topoPanelTitle.textContent = 'Network Topology';
        resetContractTree();

        updateRingSVG(peers, connections, subscriberPeerIds, {
            selectPeer: (peerId) => selectPeer(peerId, updateView, updateURL),
            goToTime: goToTime
        });
    }

    // Update stats
    document.getElementById('peer-count').textContent = peers.size;
    document.getElementById('connection-count').textContent = connections.size;

    // Update topology subtitle
    const topoSubtitle = document.querySelector('.panel-subtitle');
    if (topoSubtitle) {
        if (state.selectedContract && state.contractData[state.selectedContract]) {
            const treeStats = getTreeStats(state.selectedContract, peers);
            if (treeStats.nodeCount === 0) {
                topoSubtitle.textContent = 'No subscription tree data for this contract.';
            } else if (treeStats.isFlat) {
                topoSubtitle.textContent = `${treeStats.nodeCount} peers with state (no tree structure available)`;
            } else {
                let parts = [`${treeStats.nodeCount} nodes`, `depth ${treeStats.depth}`];
                if (treeStats.segments > 1) parts.push(`${treeStats.segments} segments (disconnected)`);
                topoSubtitle.textContent = parts.join(' \u00b7 ');
            }
        } else {
            topoSubtitle.textContent = 'Peers arranged by their network location (0.0-1.0). Click a peer to filter events.';
        }
    }

    // Filter and render events
    const nearbyEvents = filterEvents();
    renderEventsPanel(nearbyEvents);

    // Update event count (use total length in live mode to avoid O(n) scan every frame)
    document.getElementById('event-count').textContent = state.isLive
        ? state.allEvents.length
        : state.allEvents.filter(e => e.timestamp <= state.currentTime).length;

    // Update timeline markers (has internal cache key, no-ops when unchanged)
    renderTimeline();

    // Update playhead
    updatePlayhead();

    // Update contracts list if that tab is active
    if (state.activeTab === 'contracts') {
        renderContractsList();
    }
}

// updateView: throttled version used by all callbacks and event handlers
const updateView = scheduleUpdateView;

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

// Find My Peer: select the user's own peer and show the button
function findMyPeer() {
    if (state.youArePeer && state.yourPeerId) {
        selectPeer(state.yourPeerId, updateView, updateURL);
    }
}
window.findMyPeer = findMyPeer;

// Show the Find My Peer button when we know the user is a peer
function showFindMyPeerButton() {
    const btn = document.getElementById("find-my-peer-btn");
    if (btn && state.youArePeer) {
        btn.style.display = "flex";
    }
}

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
        // Show the Find My Peer button if user is a peer
        showFindMyPeerButton();
        // Auto-select user's peer on first load (if no peer already selected from URL)
        if (!state.selectedPeerId && state.youArePeer && state.yourPeerId) {
            selectPeer(state.yourPeerId, updateView, updateURL);
        }
    }
});

// Mark URL as loaded after initial setup
markURLLoaded();

// Initialize transfer chart
initTransferChart();

console.log('Freenet Dashboard initialized (modular)');
