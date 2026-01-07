/**
 * Contracts module for Freenet Dashboard
 * Handles contract selection, display, and state management
 */

import { state } from './state.js';
import { hashToColor, formatRelativeTime, getContractActivity } from './utils.js';

/**
 * Select a contract to filter events and show subscription tree
 * @param {string} contractKey - Contract key to select
 * @param {Function} updateView - Callback to refresh view
 * @param {Function} updateURL - Callback to update URL state
 */
export function selectContract(contractKey, updateView, updateURL) {
    // Toggle selection
    if (state.selectedContract === contractKey) {
        state.selectedContract = null;
    } else {
        state.selectedContract = contractKey;
    }

    renderContractsList();

    // Import updateFilterBar dynamically to avoid circular dependency
    const filterChips = document.getElementById('filter-chips');
    const noFilters = document.getElementById('no-filters');
    const clearAllBtn = document.getElementById('clear-all-btn');

    let chips = [];
    if (state.selectedPeerId) {
        chips.push(`<span class="filter-chip peer">Peer: ${state.selectedPeerId.substring(0, 12)}...<button class="filter-chip-close" onclick="clearPeerFilter()">×</button></span>`);
    }
    if (state.selectedTxId) {
        chips.push(`<span class="filter-chip tx">Tx: ${state.selectedTxId.substring(0, 8)}...<button class="filter-chip-close" onclick="clearTxFilter()">×</button></span>`);
    }
    if (state.selectedContract && state.contractData[state.selectedContract]) {
        const shortKey = state.contractData[state.selectedContract].short_key;
        chips.push(`<span class="filter-chip contract">Contract: ${shortKey}<button class="filter-chip-close" onclick="clearContractFilter()">×</button></span>`);
    }
    filterChips.innerHTML = chips.join('');
    const hasFilters = chips.length > 0 || state.filterText;
    noFilters.style.display = hasFilters ? 'none' : 'inline';
    clearAllBtn.style.display = hasFilters ? 'inline-block' : 'none';

    updateView();
    updateURL();
}

/**
 * Render the contracts list in the contracts tab
 */
export function renderContractsList() {
    const list = document.getElementById('contracts-list');
    const allContracts = Object.keys(state.contractData);

    // Filter by search text
    let filteredContracts = allContracts;
    if (state.contractSearchText) {
        const searchLower = state.contractSearchText.toLowerCase();
        filteredContracts = allContracts.filter(key =>
            key.toLowerCase().startsWith(searchLower)
        );
    }

    // Update tab count
    const countLabel = document.getElementById('contract-tab-count');
    if (filteredContracts.length === allContracts.length) {
        countLabel.textContent = allContracts.length;
    } else {
        countLabel.textContent = `${filteredContracts.length}/${allContracts.length}`;
    }

    if (filteredContracts.length === 0) {
        list.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">&#128230;</div>
                <div>${state.contractSearchText ? 'No matching contracts' : 'No contracts tracked'}</div>
            </div>
        `;
        return;
    }

    // Sort contracts by activity
    const sortedContracts = filteredContracts.sort((a, b) => {
        const aData = state.contractData[a];
        const bData = state.contractData[b];
        const aActivity = getContractActivity(a, state.allEvents);
        const bActivity = getContractActivity(b, state.allEvents);
        const aScore = (aActivity.recentUpdates * 5) + (aActivity.totalUpdates) +
            (aData.any_seeding ? 10 : 0) + (aData.total_downstream || 0);
        const bScore = (bActivity.recentUpdates * 5) + (bActivity.totalUpdates) +
            (bData.any_seeding ? 10 : 0) + (bData.total_downstream || 0);
        return bScore - aScore;
    });

    list.innerHTML = sortedContracts.map(key => {
        const data = state.contractData[key];
        const isSelected = state.selectedContract === key;
        const peerStates = data.peer_states || [];

        // Get state info from contractStates
        const states = state.contractStates[key] || {};
        const peerStateHashes = Object.entries(states);
        const uniqueHashes = new Set(peerStateHashes.map(([_, s]) => s.hash));
        const isDiverged = uniqueHashes.size > 1;
        const latestHash = peerStateHashes.length > 0
            ? peerStateHashes.sort((a, b) => b[1].timestamp - a[1].timestamp)[0][1].hash
            : null;

        // Count seeding peers
        const seedingPeers = peerStates.filter(p => p.is_seeding).length;

        // Get activity stats
        const activity = getContractActivity(key, state.allEvents);

        // Build stats display
        let statsRow1 = [];
        let statsRow2 = [];

        // Row 1: Sync status and state hash
        if (peerStateHashes.length > 0) {
            if (isDiverged) {
                const swatches = [...uniqueHashes].slice(0, 6).map(hash => {
                    const color = hashToColor(hash);
                    return `<span class="state-swatch" style="background:${color.fill}" title="${hash.substring(0,8)}"></span>`;
                }).join('');
                const extra = uniqueHashes.size > 6 ? `+${uniqueHashes.size - 6}` : '';
                statsRow1.push(`<span class="sync-indicator diverged" title="Peers have different states - may indicate sync issue">&#9888; ${uniqueHashes.size} states ${swatches}${extra}</span>`);
            } else {
                statsRow1.push(`<span class="sync-indicator synced" title="All peers have the same state">&#10003; Synced</span>`);
            }
        }

        if (latestHash) {
            const shortHash = latestHash.substring(0, 6);
            statsRow1.push(`<span class="state-hash" title="Full state hash: ${latestHash}">[${shortHash}]</span>`);
        }

        // Row 2: Network stats
        const totalSubscribed = peerStates.length;

        if (totalSubscribed > 0) {
            const subTooltip = `${totalSubscribed} peer${totalSubscribed > 1 ? 's' : ''} subscribed to this contract`;
            statsRow2.push(`<span class="contract-stat" title="${subTooltip}"><span class="contract-stat-icon subscribe">&#9733;</span> ${totalSubscribed} subscribed</span>`);
        }

        if (seedingPeers > 0) {
            const tooltip = seedingPeers === 1
                ? 'Peer hosting original contract data'
                : `${seedingPeers} peers hosting original contract data`;
            statsRow2.push(`<span class="contract-stat" title="${tooltip}"><span class="contract-stat-icon seeding">&#9679;</span> ${seedingPeers} seeder${seedingPeers > 1 ? 's' : ''}</span>`);
        }

        // Activity stats
        if (activity.recentUpdates > 0) {
            const lastUpdateStr = formatRelativeTime(activity.lastUpdate);
            statsRow2.push(`<span class="contract-stat activity" title="Updates received in the last hour">&#9889; ${activity.recentUpdates} updates/hr</span>`);
            if (lastUpdateStr) {
                statsRow2.push(`<span class="contract-stat last-update" title="Last update received">&#128337; ${lastUpdateStr}</span>`);
            }
        }

        const allStats = statsRow1.concat(statsRow2);

        return `
            <div class="contract-item ${isSelected ? 'selected' : ''}" onclick="selectContract('${key}')">
                <div class="contract-key">${data.short_key}</div>
                <div class="contract-stats">${allStats.join('') || '<span style="color:var(--text-muted)">No activity</span>'}</div>
            </div>
        `;
    }).join('');
}

/**
 * Show transaction detail popup
 * @param {Object} tx - Transaction object
 * @param {Function} switchTab - Callback to switch tabs
 * @param {Function} updateView - Callback to refresh view
 * @param {Function} updateURL - Callback to update URL state
 */
export function showTransactionDetail(tx, switchTab, updateView, updateURL) {
    // Toggle: clicking same transaction clears the filter
    if (state.selectedTxId === tx.tx_id) {
        state.selectedTxId = null;
        state.selectedTransaction = null;
        state.highlightedPeers.clear();
    } else {
        state.selectedTransaction = tx;
        state.selectedTxId = tx.tx_id;
        // Highlight all peers involved
        state.highlightedPeers.clear();
        const events = tx.events || [];
        events.forEach(evt => {
            if (evt.peer_id) state.highlightedPeers.add(evt.peer_id);
            if (evt.from_peer) state.highlightedPeers.add(evt.from_peer);
            if (evt.to_peer) state.highlightedPeers.add(evt.to_peer);
        });
        switchTab('events');
    }

    // Update filter bar
    const filterChips = document.getElementById('filter-chips');
    const noFilters = document.getElementById('no-filters');
    const clearAllBtn = document.getElementById('clear-all-btn');

    let chips = [];
    if (state.selectedPeerId) {
        chips.push(`<span class="filter-chip peer">Peer: ${state.selectedPeerId.substring(0, 12)}...<button class="filter-chip-close" onclick="clearPeerFilter()">×</button></span>`);
    }
    if (state.selectedTxId) {
        chips.push(`<span class="filter-chip tx">Tx: ${state.selectedTxId.substring(0, 8)}...<button class="filter-chip-close" onclick="clearTxFilter()">×</button></span>`);
    }
    if (state.selectedContract && state.contractData[state.selectedContract]) {
        const shortKey = state.contractData[state.selectedContract].short_key;
        chips.push(`<span class="filter-chip contract">Contract: ${shortKey}<button class="filter-chip-close" onclick="clearContractFilter()">×</button></span>`);
    }
    filterChips.innerHTML = chips.join('');
    const hasFilters = chips.length > 0 || state.filterText;
    noFilters.style.display = hasFilters ? 'none' : 'inline';
    clearAllBtn.style.display = hasFilters ? 'inline-block' : 'none';

    updateView();
    updateURL();
}

/**
 * Close transaction detail
 * @param {Function} updateView - Callback to refresh view
 */
export function closeTransactionDetail(updateView) {
    state.selectedTransaction = null;
    state.selectedTxId = null;
    document.getElementById('tx-detail-container').classList.remove('visible');

    // Update filter bar
    const filterChips = document.getElementById('filter-chips');
    const noFilters = document.getElementById('no-filters');
    const clearAllBtn = document.getElementById('clear-all-btn');

    let chips = [];
    if (state.selectedPeerId) {
        chips.push(`<span class="filter-chip peer">Peer: ${state.selectedPeerId.substring(0, 12)}...<button class="filter-chip-close" onclick="clearPeerFilter()">×</button></span>`);
    }
    if (state.selectedContract && state.contractData[state.selectedContract]) {
        const shortKey = state.contractData[state.selectedContract].short_key;
        chips.push(`<span class="filter-chip contract">Contract: ${shortKey}<button class="filter-chip-close" onclick="clearContractFilter()">×</button></span>`);
    }
    filterChips.innerHTML = chips.join('');
    const hasFilters = chips.length > 0 || state.filterText;
    noFilters.style.display = hasFilters ? 'none' : 'inline';
    clearAllBtn.style.display = hasFilters ? 'inline-block' : 'none';

    updateView();
}

/**
 * Switch between tabs
 * @param {string} tabName - Tab name to switch to
 * @param {Function} updateURL - Callback to update URL state
 */
export function switchTab(tabName, updateURL) {
    state.activeTab = tabName;

    // Update tab buttons
    document.querySelectorAll('.panel-tab').forEach(tab => {
        tab.classList.toggle('active', tab.id === `tab-${tabName}`);
    });

    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `tab-content-${tabName}`);
    });

    // Render contracts if switching to that tab
    if (tabName === 'contracts') {
        renderContractsList();
    }

    if (updateURL) updateURL();
}

/**
 * Update contract dropdown (deprecated - kept for compatibility)
 */
export function updateContractDropdown() {
    // Contract dropdown has been replaced by Contracts tab
    // This function is kept for compatibility but is now a no-op
}
