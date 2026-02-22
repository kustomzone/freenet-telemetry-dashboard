/**
 * Centralized state management for Freenet Dashboard
 * All mutable state is contained in the `state` object
 */

// Layout constants
export const SVG_SIZE = 450;
export const SVG_WIDTH = SVG_SIZE;  // No extra width needed
export const CENTER = SVG_SIZE / 2;
export const RADIUS = 195;  // Larger ring now that dist chart is an overlay

// Main state object - mutable
export const state = {
    // Event/time state
    allEvents: [],
    timeRange: { start: 0, end: 0 },
    currentTime: Date.now() * 1_000_000,
    timeWindowNs: 5 * 60 * 1_000_000_000, // 5 minutes

    // WebSocket
    ws: null,
    reconnectTimeout: null,

    // UI interaction
    isDragging: false,
    filterText: '',
    selectedEvent: null,
    hoveredEvent: null,
    highlightedPeers: new Set(),
    selectedPeerId: null,
    selectedTxId: null,
    activeTab: 'contracts',

    // Peer identity
    gatewayPeerId: null,
    yourPeerId: null,
    yourIpHash: null,
    youArePeer: false,
    yourName: null,
    peerNames: {},

    // Contracts
    contractData: {},
    contractStates: {},
    propagationData: {},
    selectedContract: null,
    contractSearchText: '',

    // Operations
    opStats: null,

    // Display
    displayedEvents: [],

    // Transactions
    allTransactions: [],
    transactionMap: new Map(),
    selectedTransaction: null,

    // Network state from server
    initialStatePeers: [],
    initialStateConnections: [],
    peerLifecycle: null,
    peerPresence: []
};

// Helper to clear all filters
export function clearAllFilters() {
    state.selectedPeerId = null;
    state.selectedTxId = null;
    state.selectedContract = null;
    state.highlightedPeers = new Set();
}

