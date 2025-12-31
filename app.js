        // State
        const allEvents = [];
        let timeRange = { start: 0, end: 0 };
        let isLive = true;
        let currentTime = Date.now() * 1_000_000;
        let ws = null;
        let reconnectTimeout = null;
        let isDragging = false;
        let filterText = '';
        let selectedEvent = null;
        let highlightedPeers = new Set();
        let selectedPeerId = null;  // For filtering events by peer
        let selectedTxId = null;    // For filtering events by transaction
        let gatewayPeerId = null;   // Gateway peer ID
        let yourPeerId = null;      // User's own peer ID
        let yourIpHash = null;      // User's IP hash
        let subscriptionData = {};  // contract_key -> subscription data
        let selectedContract = null; // Currently selected contract for subscription view
        let opStats = null;  // Operation statistics
        let displayedEvents = [];  // Events currently shown in the events panel
        let allTransactions = [];  // Transactions for timeline lanes
        let selectedTransaction = null;  // Currently selected transaction for detail view
        let initialStatePeers = [];  // Peers from initial state message
        let initialStateConnections = [];  // Connections from initial state message
        let peerLifecycle = null;  // Peer lifecycle data (version, arch, OS)
        let peerPresence = [];  // Peer presence timeline for historical reconstruction

        const SVG_SIZE = 450;
        const CENTER = SVG_SIZE / 2;
        const RADIUS = 175;

        function getEventClass(eventType) {
            if (!eventType) return 'other';
            // Handle specific connect event types
            if (eventType.includes('connect') || eventType === 'start_connection' || eventType === 'finished') return 'connect';
            if (eventType.includes('put')) return 'put';
            if (eventType.includes('get')) return 'get';
            if (eventType.includes('update')) return 'update';
            if (eventType.includes('subscrib')) return 'subscribe';
            return 'other';
        }

        function getEventLabel(eventType) {
            // Return user-friendly labels for event types
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
            };
            return labels[eventType] || eventType;
        }

        function locationToXY(location) {
            const angle = location * 2 * Math.PI - Math.PI / 2;
            return { x: CENTER + RADIUS * Math.cos(angle), y: CENTER + RADIUS * Math.sin(angle) };
        }

        function formatTime(tsNano) {
            return new Date(tsNano / 1_000_000).toLocaleTimeString();
        }

        function formatDate(tsNano) {
            return new Date(tsNano / 1_000_000).toLocaleDateString(undefined, {
                month: 'short', day: 'numeric'
            });
        }

        function renderRuler() {
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

            for (let ms = startMs; ms <= endMs; ms += tickInterval) {
                const pos = ((ms * 1_000_000) - timeRange.start) / duration;
                if (pos < 0 || pos > 1) continue;

                const tick = document.createElement('div');
                tick.className = 'timeline-tick' + ((ms % (tickInterval * 2) === 0) ? ' major' : '');
                tick.style.position = 'absolute';
                tick.style.left = `${pos * 100}%`;
                tick.style.transform = 'translateX(-50%)';

                const time = new Date(ms);
                tick.textContent = time.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});

                ruler.appendChild(tick);
            }
        }

        function showTransactionDetail(tx) {
            // Toggle: clicking same transaction clears the filter
            if (selectedTxId === tx.tx_id) {
                selectedTxId = null;
                selectedTransaction = null;
            } else {
                selectedTransaction = tx;
                selectedTxId = tx.tx_id;
            }
            updateFilterBar();
            updateView();  // Refresh events list with filter
            return;  // Events box now shows transaction details, no popup needed

            const container = document.getElementById('tx-detail-container');
            const titleEl = document.getElementById('tx-detail-title');
            const ganttEl = document.getElementById('tx-detail-gantt');

            // Update title with op badge
            const opClass = tx.op || 'other';
            titleEl.innerHTML = `<span class="op-badge ${opClass}">${(tx.op || 'unknown').toUpperCase()}</span>` +
                `<span>Transaction ${tx.tx_id.substring(0, 8)}...</span>`;

            // Build summary
            const duration = tx.duration_ms ? `${tx.duration_ms.toFixed(1)}ms` : 'pending';
            const events = tx.events || [];
            let summaryHtml = `<div class="tx-detail-summary">
                <span>Duration: <strong>${duration}</strong></span>
                <span>Events: <strong>${events.length}</strong></span>
                <span>Status: <strong>${tx.status || 'unknown'}</strong></span>
                ${tx.contract ? `<span>Contract: <strong>${tx.contract}</strong></span>` : ''}
            </div>`;

            // Build Gantt chart of events
            if (events.length === 0) {
                ganttEl.innerHTML = summaryHtml + '<div style="color:var(--text-muted);padding:12px;">No event details available</div>';
            } else {
                // Sort events by timestamp
                const sortedEvents = [...events].sort((a, b) => a.timestamp - b.timestamp);
                const txStart = sortedEvents[0].timestamp;
                const txEnd = sortedEvents[sortedEvents.length - 1].timestamp;
                const txDuration = Math.max(txEnd - txStart, 1); // Avoid division by zero

                let eventsHtml = '';
                // Use transaction's operation type for dot color
                const opType = tx.op || 'other';
                sortedEvents.forEach((evt, idx) => {
                    const relativeTime = (evt.timestamp - txStart) / 1_000_000; // Convert to ms

                    // Determine dot color: use op type, but mark failures red
                    let dotClass = opType;
                    if (evt.event_type.includes('fail') || evt.event_type.includes('error')) {
                        dotClass = 'failed';
                    }

                    // Create descriptive label based on operation type and event
                    let eventLabel = evt.event_type.replace(/_/g, ' ');
                    if (opType === 'connect') {
                        if (idx === 0 && !evt.peer_id) {
                            eventLabel = 'Initiated';
                        } else if (evt.peer_id) {
                            eventLabel = `→ ${evt.peer_id.substring(0, 12)}`;
                        }
                    } else if (opType === 'put') {
                        if (evt.event_type === 'put_request') eventLabel = 'Request sent';
                        else if (evt.event_type === 'put_success') eventLabel = 'Stored ✓';
                        else if (evt.event_type.includes('fail')) eventLabel = 'Failed ✗';
                    } else if (opType === 'get') {
                        if (evt.event_type === 'get_request') eventLabel = 'Request sent';
                        else if (evt.event_type === 'get_success') eventLabel = 'Retrieved ✓';
                        else if (evt.event_type === 'get_not_found') eventLabel = 'Not found';
                        else if (evt.event_type.includes('fail')) eventLabel = 'Failed ✗';
                    } else if (opType === 'update') {
                        if (evt.event_type === 'update_request') eventLabel = 'Update sent';
                        else if (evt.event_type === 'update_success') eventLabel = 'Updated ✓';
                        else if (evt.event_type.includes('broadcast')) eventLabel = 'Broadcast';
                    } else if (opType === 'subscribe') {
                        if (evt.event_type === 'subscribe_request') eventLabel = 'Subscribe sent';
                        else if (evt.event_type === 'subscribed') eventLabel = 'Subscribed ✓';
                    }

                    // For non-connect ops, show peer if available
                    const showPeer = opType !== 'connect' && evt.peer_id;
                    const peerInfo = showPeer ? `<span class="tx-event-peer">${evt.peer_id.substring(0, 12)}</span>` : '';

                    eventsHtml += `<div class="tx-event-row">
                        <span class="tx-event-time">+${relativeTime.toFixed(1)}ms</span>
                        <span class="tx-event-dot ${dotClass}"></span>
                        <span class="tx-event-type">${eventLabel}</span>
                        ${peerInfo}
                    </div>`;
                });

                ganttEl.innerHTML = summaryHtml + eventsHtml;
            }

            container.classList.add('visible');
        }

        function closeTransactionDetail() {
            selectedTransaction = null;
            selectedTxId = null;
            document.getElementById('tx-detail-container').classList.remove('visible');
            updateFilterBar();
            updateView();
        }

        function renderDetailTimeline() {
            const windowSize = timeWindowNs * 2;
            let windowStart = currentTime - timeWindowNs;
            let windowEnd = currentTime + timeWindowNs;
            if (windowStart < timeRange.start) { windowStart = timeRange.start; windowEnd = Math.min(timeRange.end, timeRange.start + windowSize); }
            if (windowEnd > timeRange.end) { windowEnd = timeRange.end; windowStart = Math.max(timeRange.start, timeRange.end - windowSize); }
            const windowDuration = windowEnd - windowStart;
            if (windowDuration <= 0) return;

            // Update range label
            const fmt = { hour: '2-digit', minute: '2-digit', second: '2-digit' };
            document.getElementById('detail-range').textContent =
                `${new Date(windowStart / 1_000_000).toLocaleTimeString([], fmt)} to ${new Date(windowEnd / 1_000_000).toLocaleTimeString([], fmt)}`;

            // Render ruler
            const detailRuler = document.getElementById('detail-ruler');
            detailRuler.innerHTML = '';
            const durationSec = windowDuration / 1_000_000_000;
            let tickIntervalMs = durationSec <= 60 ? 10000 : durationSec <= 300 ? 30000 : durationSec <= 600 ? 60000 : 120000;
            const startMs = Math.ceil((windowStart / 1_000_000) / tickIntervalMs) * tickIntervalMs;
            for (let ms = startMs; ms <= windowEnd / 1_000_000; ms += tickIntervalMs) {
                const pos = ((ms * 1_000_000) - windowStart) / windowDuration;
                if (pos < 0 || pos > 1) continue;
                const tick = document.createElement('div');
                tick.className = 'timeline-tick';
                tick.style.cssText = `position:absolute;left:${pos*100}%;transform:translateX(-50%)`;
                tick.textContent = new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
                detailRuler.appendChild(tick);
            }

            // Filter transactions with events in window
            const windowTx = allTransactions.filter(tx =>
                tx.end_ns >= windowStart && tx.start_ns <= windowEnd && tx.events && tx.events.length > 0
            );

            // Pack transactions into lanes (each tx gets its own lane row)
            const lanes = [];
            windowTx.forEach(tx => {
                let laneIdx = lanes.findIndex(lane => lane[lane.length - 1].end_ns + 100_000_000 < tx.start_ns); // 100ms gap
                if (laneIdx === -1) { laneIdx = lanes.length; lanes.push([]); }
                lanes[laneIdx].push(tx);
            });

            const lanesContainer = document.getElementById('timeline-lanes');
            lanesContainer.innerHTML = '';
            const laneHeight = 20, maxLanes = 4;

            lanes.slice(0, maxLanes).forEach((lane, laneIdx) => {
                const laneDiv = document.createElement('div');
                laneDiv.className = 'tx-lane';
                laneDiv.style.top = `${laneIdx * laneHeight}px`;

                lane.forEach(tx => {
                    const events = tx.events || [];
                    if (events.length === 0) return;

                    // Calculate transaction position and width
                    const txStart = Math.max(windowStart, tx.start_ns);
                    const txEnd = Math.min(windowEnd, tx.end_ns || tx.start_ns);
                    const startPos = (txStart - windowStart) / windowDuration;
                    const endPos = (txEnd - windowStart) / windowDuration;

                    // Minimum width for visibility
                    const minWidthPercent = 0.5;
                    let widthPercent = (endPos - startPos) * 100;
                    if (widthPercent < minWidthPercent) widthPercent = minWidthPercent;

                    // Create pill-shaped transaction bar
                    const txBar = document.createElement('div');
                    const opClass = tx.op || 'other';
                    const statusClass = tx.status === 'pending' ? ' pending' : tx.status === 'failed' ? ' failed' : '';
                    txBar.className = `tx-bar ${opClass}${statusClass}`;
                    txBar.style.cssText = `left:${startPos * 100}%;width:${widthPercent}%;top:1px;`;

                    const duration = tx.duration_ms ? `${tx.duration_ms.toFixed(1)}ms` : 'pending';
                    txBar.title = `${tx.op}: ${tx.contract || 'no contract'}\nDuration: ${duration}\nEvents: ${events.length}`;
                    txBar.onclick = (e) => { e.stopPropagation(); showTransactionDetail(tx); };
                    laneDiv.appendChild(txBar);
                });
                lanesContainer.appendChild(laneDiv);
            });

            if (lanes.length > maxLanes) {
                const overflow = document.createElement('div');
                overflow.style.cssText = 'position:absolute;bottom:0;right:0;font-size:10px;color:var(--text-muted)';
                overflow.textContent = `+${lanes.length - maxLanes} more lanes`;
                lanesContainer.appendChild(overflow);
            }
        }

        function selectEvent(event) {
            highlightedPeers.clear();

            // Toggle: clicking same event deselects it
            if (selectedEvent === event) {
                selectedEvent = null;
                goLive();  // Return to live view when deselecting
                return;
            }

            selectedEvent = event;

            if (event) {
                // Move playhead to event time
                goToTime(event.timestamp);

                // Highlight the event's peer
                if (event.peer_id) {
                    highlightedPeers.add(event.peer_id);
                }

                // Also highlight connection peers
                if (event.connection) {
                    highlightedPeers.add(event.connection[0]);
                    highlightedPeers.add(event.connection[1]);
                }
            }

            updateView();
        }

        function selectPeer(peerId) {
            if (selectedPeerId === peerId) {
                // Clicking same peer clears selection
                selectedPeerId = null;
            } else {
                selectedPeerId = peerId;
            }
            updateFilterBar();
            updateView();
        }

        function togglePeerFilter(peerId) {
            if (selectedPeerId === peerId) {
                selectedPeerId = null;
            } else {
                selectedPeerId = peerId;
            }
            updateFilterBar();
            updateView();
        }

        function toggleTxFilter(txId) {
            if (selectedTxId === txId) {
                selectedTxId = null;
            } else {
                selectedTxId = txId;
            }
            updateFilterBar();
            updateView();
        }

        function clearPeerSelection() {
            selectedPeerId = null;
            document.getElementById('filter-input').placeholder = 'Filter by tx...';
            updateView();
        }

        let contractDropdownOpen = false;

        function toggleContractDropdown() {
            contractDropdownOpen = !contractDropdownOpen;
            document.getElementById('contract-menu').classList.toggle('open', contractDropdownOpen);
        }

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('#contract-dropdown') && contractDropdownOpen) {
                contractDropdownOpen = false;
                document.getElementById('contract-menu').classList.remove('open');
            }
        });

        function selectContract(contractKey) {
            // Toggle selection if clicking the same contract
            if (selectedContract === contractKey) {
                selectedContract = null;
            } else {
                selectedContract = contractKey || null;
            }

            // Close dropdown
            contractDropdownOpen = false;
            document.getElementById('contract-menu').classList.remove('open');

            // Update filter bar
            updateFilterBar();
            updateView();
        }

        function updateFilterBar() {
            const chipsContainer = document.getElementById('filter-chips');
            const noFilters = document.getElementById('no-filters');
            const clearAllBtn = document.getElementById('clear-all-btn');

            let chips = [];

            if (selectedPeerId) {
                chips.push(`<span class="filter-chip peer">Peer: ${selectedPeerId.substring(0, 12)}...<button class="filter-chip-close" onclick="clearPeerFilter()">×</button></span>`);
            }

            if (selectedTxId) {
                chips.push(`<span class="filter-chip tx">Tx: ${selectedTxId.substring(0, 8)}...<button class="filter-chip-close" onclick="clearTxFilter()">×</button></span>`);
            }

            if (selectedContract && subscriptionData[selectedContract]) {
                const shortKey = subscriptionData[selectedContract].short_key;
                chips.push(`<span class="filter-chip contract">Contract: ${shortKey}<button class="filter-chip-close" onclick="clearContractFilter()">×</button></span>`);
            }

            chipsContainer.innerHTML = chips.join('');

            const hasFilters = chips.length > 0 || filterText;
            noFilters.style.display = hasFilters ? 'none' : 'inline';
            clearAllBtn.style.display = hasFilters ? 'inline-block' : 'none';
        }

        function clearPeerFilter() {
            selectedPeerId = null;
            updateFilterBar();
            updateView();
        }

        function clearTxFilter() {
            selectedTxId = null;
            updateFilterBar();
            updateView();
        }

        function clearContractFilter() {
            selectedContract = null;
            updateFilterBar();
            updateView();
        }

        function clearAllFilters() {
            selectedPeerId = null;
            selectedTxId = null;
            selectedContract = null;
            filterText = '';
            document.getElementById('filter-input').value = '';
            updateFilterBar();
            updateView();
        }

        function formatLatency(ms) {
            if (ms === null || ms === undefined) return '-';
            if (ms < 1000) return Math.round(ms) + 'ms';
            return (ms / 1000).toFixed(1) + 's';
        }

        function getRateClass(rate) {
            if (rate === null || rate === undefined) return '';
            if (rate >= 90) return 'good';
            if (rate >= 50) return 'warn';
            return 'bad';
        }

        function updateOpStats() {
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
        }

        function updatePeerLifecycleStats() {
            if (!peerLifecycle) return;

            // Log version distribution for debugging
            console.log('Version distribution:', peerLifecycle.versions);

            // Could add UI elements to display this info later
            // For now, the data is available in the peerLifecycle global
        }

        function updateContractDropdown() {
            const menu = document.getElementById('contract-menu');
            const btnLabel = document.getElementById('contract-btn-label');
            const contracts = Object.keys(subscriptionData);

            if (contracts.length === 0) {
                menu.innerHTML = '<div class="contract-dropdown-item" style="color: var(--text-muted);">No contracts</div>';
                btnLabel.textContent = 'Contracts (0)';
                return;
            }

            btnLabel.textContent = `Contracts (${contracts.length})`;

            menu.innerHTML = contracts.map(key => {
                const data = subscriptionData[key];
                const treeSize = Object.keys(data.tree || {}).length;

                return `
                    <div class="contract-dropdown-item" onclick="selectContract('${key}')">
                        <div class="contract-dropdown-key">${data.short_key}</div>
                        <div class="contract-dropdown-stats">${data.subscribers.length} subscribers &middot; ${treeSize} paths</div>
                    </div>
                `;
            }).join('');
        }

        function reconstructStateAtTime(targetTime) {
            const peers = new Map();
            const connections = new Set();

            // Activity window: ±5 minutes around target time
            const ACTIVITY_WINDOW_NS = 5 * 60 * 1_000_000_000;
            const windowStart = targetTime - ACTIVITY_WINDOW_NS;
            const windowEnd = targetTime + ACTIVITY_WINDOW_NS;

            // Find peers active near target time (have events within window)
            // Also build peer info map from peer_presence for location data
            const peerInfo = new Map();
            for (const p of peerPresence) {
                peerInfo.set(p.id, { location: p.location, ip_hash: p.ip_hash });
            }

            // Scan events to find active peers and connections
            for (const event of allEvents) {
                // Check if event is within activity window
                const inWindow = event.timestamp >= windowStart && event.timestamp <= windowEnd;

                if (inWindow && event.peer_id) {
                    const info = peerInfo.get(event.peer_id);
                    if (info) {
                        peers.set(event.peer_id, info);
                    } else if (event.location !== undefined) {
                        peers.set(event.peer_id, {
                            location: event.location,
                            ip_hash: event.peer_ip_hash
                        });
                    }
                }

                // Build connections from events up to target time
                if (event.timestamp <= targetTime && event.connection) {
                    const key = [event.connection[0], event.connection[1]].sort().join('|');
                    connections.add(key);
                }

                // Stop scanning once past the window
                if (event.timestamp > windowEnd) break;
            }

            return { peers, connections };
        }

        function updateRingSVG(peers, connections, subscriberPeerIds = new Set()) {
            const container = document.getElementById('ring-container');
            const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            svg.setAttribute('viewBox', `0 0 ${SVG_SIZE} ${SVG_SIZE}`);
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
            [0.6, 0.3].forEach((scale, i) => {
                const inner = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                inner.setAttribute('cx', CENTER);
                inner.setAttribute('cy', CENTER);
                inner.setAttribute('r', RADIUS * scale);
                inner.setAttribute('fill', 'none');
                inner.setAttribute('stroke', 'rgba(255,255,255,0.03)');
                inner.setAttribute('stroke-width', '1');
                inner.setAttribute('stroke-dasharray', '4,8');
                svg.appendChild(inner);
            });

            // Location markers
            [0, 0.25, 0.5, 0.75].forEach(loc => {
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
            });

            // Draw connections
            connections.forEach(connKey => {
                const [id1, id2] = connKey.split('|');
                const peer1 = peers.get(id1);
                const peer2 = peers.get(id2);
                if (peer1 && peer2) {
                    const pos1 = locationToXY(peer1.location);
                    const pos2 = locationToXY(peer2.location);
                    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                    line.setAttribute('x1', pos1.x);
                    line.setAttribute('y1', pos1.y);
                    line.setAttribute('x2', pos2.x);
                    line.setAttribute('y2', pos2.y);
                    line.setAttribute('class', 'connection-line animated');
                    svg.appendChild(line);
                }
            });

            // Draw peers
            peers.forEach((peer, id) => {
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

                if (isGateway) {
                    fillColor = '#f59e0b';  // Amber for gateway
                    glowColor = 'rgba(245, 158, 11, 0.3)';
                    label = 'GW';
                } else if (isYou) {
                    fillColor = '#10b981';  // Emerald for you
                    glowColor = 'rgba(16, 185, 129, 0.3)';
                    label = 'YOU';
                } else if (isSubscriber) {
                    fillColor = '#f472b6';  // Pink for subscriber
                    glowColor = 'rgba(244, 114, 182, 0.3)';
                }

                // Override colors for selection states
                if (isEventSelected) {
                    fillColor = '#f87171';
                    glowColor = 'rgba(248, 113, 113, 0.3)';
                } else if (isPeerSelected) {
                    fillColor = '#7ecfef';
                    glowColor = 'rgba(126, 207, 239, 0.4)';
                } else if (isHighlighted) {
                    fillColor = '#fbbf24';
                    glowColor = 'rgba(251, 191, 36, 0.3)';
                }

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
                title.textContent = `${id}${peerType}\n#${peer.ip_hash || ''}\nLocation: ${peer.location.toFixed(4)}\nClick to filter events`;
                clickTarget.appendChild(title);
                svg.appendChild(circle);

                // Add label for gateway/you
                if (label && peers.size <= 15) {
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
                }

                // Label
                if (peers.size <= 12) {
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
                    text.textContent = `#${peer.ip_hash || id.substring(5, 11)}`;
                    svg.appendChild(text);
                }
            });

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
            if (!selectedContract && displayedEvents && displayedEvents.length > 0) {
                displayedEvents.forEach((event, idx) => {
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
                    arrow.setAttribute('class', `message-flow-arrow ${eventClass} animated`);
                    arrow.setAttribute('style', `opacity: ${isSelected ? 1 : 0.6 - idx * 0.05}`);
                    svg.appendChild(arrow);
                });
            }

            // Draw subscription tree arrows if a contract is selected
            if (selectedContract && subscriptionData[selectedContract]) {
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
                Object.entries(tree).forEach(([fromId, toIds]) => {
                    const fromPeer = peers.get(fromId);
                    if (!fromPeer) return;

                    toIds.forEach(toId => {
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
                    });
                });
            }

            container.innerHTML = '';
            container.appendChild(svg);
        }

        function renderTimeline() {
            const container = document.getElementById('timeline-events');
            container.innerHTML = '';

            if (allEvents.length === 0 || timeRange.end <= timeRange.start) return;

            const duration = timeRange.end - timeRange.start;

            // Define lanes: each operation type gets a row
            const lanes = {
                put: { color: 'var(--color-put)', row: 0, label: 'PUT' },
                get: { color: 'var(--color-get)', row: 1, label: 'GET' },
                update: { color: 'var(--color-update)', row: 2, label: 'UPD' },
                subscribe: { color: 'var(--color-subscribe)', row: 3, label: 'SUB' },
                connect: { color: 'var(--color-connect)', row: 4, label: 'CONN' },
            };
            const laneHeight = 13;
            const laneGap = 2;
            const totalHeight = Object.keys(lanes).length * (laneHeight + laneGap);

            // Create lane backgrounds and labels
            Object.entries(lanes).forEach(([type, lane]) => {
                const bg = document.createElement('div');
                bg.className = 'timeline-lane-bg';
                bg.style.cssText = `position:absolute;left:0;right:0;top:${lane.row * (laneHeight + laneGap)}px;height:${laneHeight}px;background:rgba(255,255,255,0.02);border-radius:2px;`;
                container.appendChild(bg);
            });

            // Group events into time buckets per lane
            const buckets = {};
            allEvents.forEach(event => {
                const bucket = Math.round((event.timestamp - timeRange.start) / duration * 400);
                const type = event.event_type;

                // Determine which lane
                let laneType = 'connect';
                if (type.includes('put')) laneType = 'put';
                else if (type.includes('get')) laneType = 'get';
                else if (type.includes('update')) laneType = 'update';
                else if (type.includes('subscrib')) laneType = 'subscribe';
                else if (!type.includes('connect')) return; // Skip other events

                const key = `${bucket}-${laneType}`;
                if (!buckets[key]) buckets[key] = { bucket, laneType, events: [], hasResponse: false };
                buckets[key].events.push(event);

                // Track if this bucket has a response/success
                if (type.includes('success') || type.includes('subscribed')) {
                    buckets[key].hasResponse = true;
                }
            });

            // Render markers
            Object.values(buckets).forEach(data => {
                const posPercent = data.bucket / 4; // 400 buckets -> 100%
                if (posPercent < 0 || posPercent > 100) return;

                const lane = lanes[data.laneType];
                const marker = document.createElement('div');

                // Brighter if has response/success, dimmer if just request
                const opacity = data.hasResponse ? 1 : 0.5;
                const width = Math.min(6, 2 + data.events.length);

                marker.className = 'timeline-lane-marker';
                marker.style.cssText = `
                    position:absolute;
                    left:${posPercent}%;
                    top:${lane.row * (laneHeight + laneGap)}px;
                    width:${width}px;
                    height:${laneHeight}px;
                    background:${lane.color};
                    opacity:${opacity};
                    border-radius:1px;
                    cursor:pointer;
                    transform:translateX(-50%);
                `;

                const evtTypes = [...new Set(data.events.map(e => e.event_type))].join(', ');
                marker.title = `${data.events.length} ${data.laneType} event(s)\n${evtTypes}`;
                marker.onclick = (e) => {
                    e.stopPropagation();
                    goToTime(data.events[0].timestamp);
                };
                container.appendChild(marker);
            });

            document.getElementById('timeline-start').textContent = formatTime(timeRange.start);
        }

        // Time window for events display (default 5 minutes each side = 10 min total)
        let timeWindowNs = 5 * 60 * 1_000_000_000;
        const MIN_TIME_WINDOW_NS = 1 * 60 * 1_000_000_000;  // 1 minute minimum
        const MAX_TIME_WINDOW_NS = 60 * 60 * 1_000_000_000; // 60 minutes maximum
        const MIN_PLAYHEAD_WIDTH_PX = 40;

        function updatePlayhead() {
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
            playhead.style.left = `calc(${leftPos}% + 16px)`;
            playhead.style.width = `${windowWidthPercent}%`;

            document.getElementById('playhead-time').textContent = formatTime(currentTime);
            document.getElementById('playhead-date').textContent = formatDate(currentTime);
            document.getElementById('time-display').textContent = formatTime(currentTime).split(' ')[0];

            // Update topology time label
            const topoLabel = document.getElementById('topology-time-label');
            if (isLive) {
                topoLabel.textContent = 'Live';
                topoLabel.classList.add('live');
            } else {
                const timeStr = new Date(currentTime / 1_000_000).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
                topoLabel.textContent = `at ${timeStr}`;
                topoLabel.classList.remove('live');
            }

            renderDetailTimeline();
        }

        function updateView() {
            let peers = new Map();
            let connections = new Set();

            // For live view, use direct state data (most accurate)
            // For historical view (time scrubbing), reconstruct from events
            if (isLive && initialStatePeers.length > 0) {
                // Live mode: use current network state directly
                for (const p of initialStatePeers) {
                    peers.set(p.id, {
                        location: p.location,
                        ip_hash: p.ip_hash
                    });
                }
                for (const conn of initialStateConnections) {
                    const key = [conn[0], conn[1]].sort().join('|');
                    connections.add(key);
                }
            } else {
                // Historical mode: reconstruct from events
                const reconstructed = reconstructStateAtTime(currentTime);
                peers = reconstructed.peers;
                connections = reconstructed.connections;
            }

            // Get subscription subscribers for highlighting (if contract selected)
            let subscriberPeerIds = new Set();
            if (selectedContract && subscriptionData[selectedContract]) {
                subscriberPeerIds = new Set(subscriptionData[selectedContract].subscribers);
            }

            updateRingSVG(peers, connections, subscriberPeerIds);

            document.getElementById('peer-count').textContent = peers.size;
            document.getElementById('connection-count').textContent = connections.size;

            // Update topology subtitle
            const topoSubtitle = document.querySelector('.panel-subtitle');
            if (topoSubtitle) {
                if (selectedContract && subscriptionData[selectedContract]) {
                    const subData = subscriptionData[selectedContract];
                    const visibleSubs = [...subscriberPeerIds].filter(id => peers.has(id)).length;
                    topoSubtitle.textContent = `${visibleSubs}/${subData.subscribers.length} subscribers visible. Pink arrows show broadcast tree.`;
                } else {
                    topoSubtitle.textContent = 'Peers arranged by their network location (0.0-1.0). Click a peer to filter events.';
                }
            }

            // Filter events within the time window
            let nearbyEvents = allEvents.filter(e =>
                Math.abs(e.timestamp - currentTime) < timeWindowNs
            );

            // Filter by selected peer (check peer_id, from_peer, to_peer, and connection)
            if (selectedPeerId) {
                nearbyEvents = nearbyEvents.filter(e =>
                    e.peer_id === selectedPeerId ||
                    e.from_peer === selectedPeerId ||
                    e.to_peer === selectedPeerId ||
                    (e.connection && (e.connection[0] === selectedPeerId || e.connection[1] === selectedPeerId))
                );
            }

            // Filter by selected transaction
            if (selectedTxId) {
                nearbyEvents = nearbyEvents.filter(e => e.tx_id === selectedTxId);
            }

            // Filter by text input
            if (filterText) {
                const filter = filterText.toLowerCase();
                nearbyEvents = nearbyEvents.filter(e =>
                    (e.event_type && e.event_type.toLowerCase().includes(filter)) ||
                    (e.peer_id && e.peer_id.toLowerCase().includes(filter)) ||
                    (e.contract && e.contract.toLowerCase().includes(filter))
                );
            }

            // Filter by selected contract (show only subscribe/update events for this contract)
            if (selectedContract) {
                nearbyEvents = nearbyEvents.filter(e =>
                    e.contract_full === selectedContract &&
                    (e.event_type.includes('subscribe') || e.event_type.includes('update') || e.event_type.includes('broadcast'))
                );
            }

            nearbyEvents = nearbyEvents.slice(-30);

            // Update events title based on filtering
            const eventsTitle = document.getElementById('events-title');
            if (selectedContract && subscriptionData[selectedContract]) {
                eventsTitle.textContent = `Events for ${subscriptionData[selectedContract].short_key}`;
            } else if (selectedPeerId) {
                eventsTitle.textContent = `Events for ${selectedPeerId.substring(0, 12)}...`;
            } else {
                eventsTitle.textContent = 'Events';
            }

            const eventsPanel = document.getElementById('events-panel');
            if (nearbyEvents.length === 0) {
                displayedEvents = [];
                const emptyMsg = selectedContract
                    ? 'No subscription events in this time range'
                    : 'No events in this time range';
                eventsPanel.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-state-icon">&#8709;</div>
                        <div>${emptyMsg}</div>
                    </div>
                `;
            } else {
                // Add sticky header row
                const headerHtml = `
                    <div class="events-header">
                        <span class="header-time">Time</span>
                        <span class="header-type">Type</span>
                        <span class="header-peers">Peers (from → to)</span>
                        <span class="header-tx">Tx</span>
                    </div>
                `;

                const eventsHtml = nearbyEvents.map((e, idx) => {
                    const isSelected = selectedEvent && selectedEvent.timestamp === e.timestamp && selectedEvent.peer_id === e.peer_id;
                    const classes = ['event-item'];
                    if (isSelected) classes.push('selected');

                    // Build peer display: from_peer → to_peer or just peer_id
                    let peersHtml = '';
                    const fromPeer = e.from_peer || e.peer_id;
                    const toPeer = e.to_peer;
                    const fromActive = selectedPeerId === fromPeer ? ' active' : '';
                    const toActive = toPeer && selectedPeerId === toPeer ? ' active' : '';

                    if (toPeer && toPeer !== fromPeer) {
                        peersHtml = `
                            <span class="event-filter-link${fromActive}" onclick="event.stopPropagation(); togglePeerFilter('${fromPeer}')">${fromPeer.substring(0, 12)}</span>
                            <span class="event-arrow">→</span>
                            <span class="event-filter-link${toActive}" onclick="event.stopPropagation(); togglePeerFilter('${toPeer}')">${toPeer.substring(0, 12)}</span>
                        `;
                    } else {
                        peersHtml = `<span class="event-filter-link${fromActive}" onclick="event.stopPropagation(); togglePeerFilter('${fromPeer}')">${fromPeer.substring(0, 12)}</span>`;
                    }

                    // Transaction ID (shortened)
                    const txActive = e.tx_id && selectedTxId === e.tx_id ? ' active' : '';
                    const txHtml = e.tx_id ? `<span class="event-tx event-filter-link${txActive}" onclick="event.stopPropagation(); toggleTxFilter('${e.tx_id}')">${e.tx_id.substring(0, 8)}</span>` : '';

                    return `
                        <div class="${classes.join(' ')}" data-event-idx="${idx}" onclick="handleEventClick(${idx})">
                            <span class="event-time">${e.time_str}</span>
                            <span class="event-badge ${getEventClass(e.event_type)}">${getEventLabel(e.event_type)}</span>
                            <div class="event-peers">${peersHtml}</div>
                            ${txHtml}
                        </div>
                    `;
                }).reverse().join('');

                eventsPanel.innerHTML = headerHtml + eventsHtml;

                // Store events for click handler and topology visualization
                displayedEvents = nearbyEvents;
            }

            document.getElementById('event-count').textContent = allEvents.filter(e => e.timestamp <= currentTime).length;
            updatePlayhead();
        }

        function handleEventClick(idx) {
            if (displayedEvents && displayedEvents[idx]) {
                selectEvent(displayedEvents[idx]);
            }
        }

        function goLive() {
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
        }

        function goToTime(time) {
            isLive = false;
            currentTime = time;
            document.getElementById('mode-button').className = 'timeline-mode historical';
            document.getElementById('mode-button').textContent = 'HISTORICAL';
            document.getElementById('status-dot').className = 'status-dot historical';
            document.getElementById('status-text').textContent = 'Time Travel';
            document.getElementById('events-title').textContent = 'Events at ' + formatTime(time);
            updateView();
        }

        function updateWindowLabel() {
            const totalMinutes = Math.round(timeWindowNs * 2 / 60_000_000_000);
            let label;
            if (totalMinutes >= 60) {
                label = `${Math.round(totalMinutes / 60)} hr`;
            } else {
                label = `${totalMinutes} min`;
            }
            document.getElementById('window-label').textContent = label;
        }

        function setupTimeline() {
            const timeline = document.getElementById('timeline');

            function getTimeFromX(clientX) {
                const rect = timeline.getBoundingClientRect();
                const pos = Math.max(0, Math.min(1, (clientX - rect.left - 16) / (rect.width - 32)));
                return timeRange.start + pos * (timeRange.end - timeRange.start);
            }

            // Click on timeline background to jump
            let justDragged = false;
            timeline.addEventListener('click', (e) => {
                if (justDragged) { justDragged = false; return; }
                if (e.target.closest('.timeline-playhead')) return;
                if (e.target.classList.contains('timeline-marker')) return;
                goToTime(getTimeFromX(e.clientX));
            });

            // Drag states
            let dragMode = null, dragStartX = 0, dragStartLeft = 0, dragStartRight = 0;
            const playhead = document.getElementById('playhead');
            const resizeLeft = document.getElementById('resize-left');
            const resizeRight = document.getElementById('resize-right');

            function getWindowEdges() {
                const windowSize = timeWindowNs * 2;
                let left = currentTime - timeWindowNs, right = currentTime + timeWindowNs;
                if (left < timeRange.start) { left = timeRange.start; right = Math.min(timeRange.end, timeRange.start + windowSize); }
                if (right > timeRange.end) { right = timeRange.end; left = Math.max(timeRange.start, timeRange.end - windowSize); }
                return { left, right };
            }

            resizeLeft.addEventListener('mousedown', (e) => {
                dragMode = 'resize-left'; dragStartX = e.clientX;
                const edges = getWindowEdges(); dragStartLeft = edges.left; dragStartRight = edges.right;
                e.preventDefault(); e.stopPropagation();
            });

            resizeRight.addEventListener('mousedown', (e) => {
                dragMode = 'resize-right'; dragStartX = e.clientX;
                const edges = getWindowEdges(); dragStartLeft = edges.left; dragStartRight = edges.right;
                e.preventDefault(); e.stopPropagation();
            });

            playhead.addEventListener('mousedown', (e) => {
                if (e.target === resizeLeft || e.target === resizeRight) return;
                dragMode = 'move'; dragStartX = e.clientX;
                const edges = getWindowEdges(); dragStartLeft = edges.left; dragStartRight = edges.right;
                playhead.style.cursor = 'grabbing';
                e.preventDefault(); e.stopPropagation();
            });

            playhead.addEventListener('click', (e) => e.stopPropagation());

            document.addEventListener('mousemove', (e) => {
                if (!dragMode) return;
                const rect = timeline.getBoundingClientRect();
                const timelineWidth = rect.width - 32;
                const duration = timeRange.end - timeRange.start;
                const deltaX = e.clientX - dragStartX;
                const deltaNs = (deltaX / timelineWidth) * duration;

                if (dragMode === 'move') {
                    const windowSize = dragStartRight - dragStartLeft;
                    let newLeft = dragStartLeft + deltaNs, newRight = dragStartRight + deltaNs;
                    if (newLeft < timeRange.start) { newLeft = timeRange.start; newRight = timeRange.start + windowSize; }
                    if (newRight > timeRange.end) { newRight = timeRange.end; newLeft = timeRange.end - windowSize; }
                    currentTime = (newLeft + newRight) / 2;
                    isLive = false; updatePlayhead(); updateView();
                } else if (dragMode === 'resize-left') {
                    const newLeft = dragStartLeft + deltaNs;
                    const newHalfWindow = (dragStartRight - newLeft) / 2;
                    if (newHalfWindow >= MIN_TIME_WINDOW_NS && newHalfWindow <= MAX_TIME_WINDOW_NS && newLeft >= timeRange.start) {
                        timeWindowNs = newHalfWindow; currentTime = (newLeft + dragStartRight) / 2;
                        isLive = false; updateWindowLabel(); updatePlayhead(); updateView();
                    }
                } else if (dragMode === 'resize-right') {
                    const newRight = dragStartRight + deltaNs;
                    const newHalfWindow = (newRight - dragStartLeft) / 2;
                    if (newHalfWindow >= MIN_TIME_WINDOW_NS && newHalfWindow <= MAX_TIME_WINDOW_NS && newRight <= timeRange.end) {
                        timeWindowNs = newHalfWindow; currentTime = (dragStartLeft + newRight) / 2;
                        isLive = false; updateWindowLabel(); updatePlayhead(); updateView();
                    }
                }
            });

            document.addEventListener('mouseup', () => {
                if (dragMode) {
                    justDragged = true;
                    if (dragMode === 'move') playhead.style.cursor = 'grab';
                    dragMode = null;
                    setTimeout(() => { justDragged = false; }, 100);
                }
            });

            // Initialize window label
            updateWindowLabel();

            // Keyboard shortcuts
            document.addEventListener('keydown', (e) => {
                if (e.target.tagName === 'INPUT') return;

                const step = (timeRange.end - timeRange.start) / 100;

                if (e.key === 'ArrowLeft') {
                    goToTime(Math.max(timeRange.start, currentTime - step));
                } else if (e.key === 'ArrowRight') {
                    goToTime(Math.min(timeRange.end, currentTime + step));
                } else if (e.key === 'l' || e.key === 'L') {
                    goLive();
                }
            });

            // Filter input
            document.getElementById('filter-input').addEventListener('input', (e) => {
                filterText = e.target.value;
                updateView();
            });
        }

        function handleMessage(data) {
            if (data.type === 'state') {
                console.log('Received initial state');

                // Extract gateway and user identification
                if (data.gateway_peer_id) {
                    gatewayPeerId = data.gateway_peer_id;
                    console.log('Gateway:', gatewayPeerId);
                }
                if (data.your_peer_id) {
                    yourPeerId = data.your_peer_id;
                    yourIpHash = data.your_ip_hash;
                    console.log('You:', yourPeerId, '#' + yourIpHash);

                    // Update legend (if elements exist)
                    const legendYou = document.getElementById('legend-you');
                    const yourHash = document.getElementById('your-hash');
                    if (legendYou) legendYou.style.display = 'flex';
                    if (yourHash) yourHash.textContent = '#' + yourIpHash;
                }

                // Show legend
                const topoLegend = document.getElementById('topology-legend');
                if (topoLegend) topoLegend.style.display = 'flex';

                // Store subscription data
                if (data.subscriptions) {
                    subscriptionData = data.subscriptions;
                    updateContractDropdown();
                    console.log('Subscriptions:', Object.keys(subscriptionData).length, 'contracts');
                }

                // Store and display operation stats
                if (data.op_stats) {
                    opStats = data.op_stats;
                    updateOpStats();
                    console.log('Op stats loaded');
                }

                // Store peer and connection data from state
                if (data.peers) {
                    initialStatePeers = data.peers;
                    console.log('Peers from state:', initialStatePeers.length);
                }
                if (data.connections) {
                    initialStateConnections = data.connections;
                    console.log('Connections from state:', initialStateConnections.length);
                }

                // Store peer lifecycle data
                if (data.peer_lifecycle) {
                    peerLifecycle = data.peer_lifecycle;
                    console.log('Peer lifecycle:', peerLifecycle.active_count, 'active,',
                                peerLifecycle.gateway_count, 'gateways');
                    updatePeerLifecycleStats();
                }

                // Trigger initial view update if we have peers
                if (initialStatePeers.length > 0) {
                    updateView();
                }

            } else if (data.type === 'history') {
                allEvents.length = 0;
                allEvents.push(...data.events);
                timeRange = data.time_range;
                timeRange.end = Date.now() * 1_000_000;
                currentTime = timeRange.end;

                if (data.transactions) {
                    allTransactions = data.transactions;
                    console.log(`Loaded ${allTransactions.length} transactions`);
                }

                // Store peer presence for historical reconstruction
                if (data.peer_presence) {
                    peerPresence = data.peer_presence;
                    console.log(`Loaded ${peerPresence.length} peer presence records`);
                }

                console.log(`Loaded ${allEvents.length} events`);

                renderTimeline();
                renderRuler();
                updateView();

            } else if (data.type === 'event') {
                allEvents.push(data);
                timeRange.end = data.timestamp;

                if (timeRange.end > timeRange.start) {
                    const container = document.getElementById('timeline-events');
                    const duration = timeRange.end - timeRange.start;
                    const pos = (data.timestamp - timeRange.start) / duration;

                    const marker = document.createElement('div');
                    marker.className = `timeline-marker ${getEventClass(data.event_type)}`;
                    marker.style.left = `${pos * 100}%`;
                    marker.style.height = '20px';
                    marker.style.top = '30px';
                    marker.title = `${data.time_str} - ${data.event_type}`;
                    container.appendChild(marker);
                }

                if (isLive) {
                    currentTime = data.timestamp;
                    updateView();
                }
            }
        }

        function connect() {
            // Use same host/port as the page, proxied through Caddy
            const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${wsProtocol}//${window.location.host}/ws`;
            ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                document.getElementById('status-dot').className = 'status-dot live';
                document.getElementById('status-text').textContent = 'Live';
                if (reconnectTimeout) { clearTimeout(reconnectTimeout); reconnectTimeout = null; }
            };

            ws.onmessage = (event) => {
                try { handleMessage(JSON.parse(event.data)); }
                catch (e) { console.error('Parse error:', e); }
            };

            ws.onclose = () => {
                document.getElementById('status-dot').className = 'status-dot disconnected';
                document.getElementById('status-text').textContent = 'Reconnecting...';
                reconnectTimeout = setTimeout(connect, 3000);
            };

            ws.onerror = () => ws.close();
        }

        // Initialize
        setupTimeline();
        connect();
