/**
 * Timeline module for Freenet Dashboard
 * Handles timeline rendering, playhead, and time navigation
 */

import { state, SVG_SIZE } from './state.js';
import { formatTime, formatDate, getEventClass } from './utils.js';

// Time window constants
export const MIN_TIME_WINDOW_NS = 1 * 60 * 1_000_000_000;  // 1 minute minimum
export const MAX_TIME_WINDOW_NS = 60 * 60 * 1_000_000_000; // 60 minutes maximum
const MIN_PLAYHEAD_WIDTH_PX = 40;

/**
 * Render the main timeline ruler with time ticks
 */
export function renderRuler() {
    const ruler = document.getElementById('timeline-ruler');
    ruler.innerHTML = '';

    if (state.timeRange.end <= state.timeRange.start) return;

    const duration = state.timeRange.end - state.timeRange.start;
    const durationMs = duration / 1_000_000;
    const durationMin = durationMs / 60000;

    // Determine appropriate tick interval
    let tickInterval;
    if (durationMin <= 10) tickInterval = 60000;       // 1 min
    else if (durationMin <= 30) tickInterval = 300000;  // 5 min
    else if (durationMin <= 60) tickInterval = 600000;  // 10 min
    else if (durationMin <= 120) tickInterval = 900000; // 15 min
    else tickInterval = 1800000;                        // 30 min

    const startMs = Math.ceil((state.timeRange.start / 1_000_000) / tickInterval) * tickInterval;
    const endMs = state.timeRange.end / 1_000_000;

    for (let ms = startMs; ms <= endMs; ms += tickInterval) {
        const pos = ((ms * 1_000_000) - state.timeRange.start) / duration;
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

/**
 * Render the detail timeline showing transactions in the current window
 */
let lastDetailKey = null;
export function renderDetailTimeline() {
    // Skip rebuild if window hasn't moved significantly (within 1% of window)
    const cacheKey = `${Math.round(state.currentTime / (state.timeWindowNs * 0.02))}-${state.timeWindowNs}-${state.allTransactions.length}`;
    if (cacheKey === lastDetailKey) return;
    lastDetailKey = cacheKey;

    const windowSize = state.timeWindowNs * 2;
    let windowStart = state.currentTime - state.timeWindowNs;
    let windowEnd = state.currentTime + state.timeWindowNs;
    if (windowStart < state.timeRange.start) {
        windowStart = state.timeRange.start;
        windowEnd = Math.min(state.timeRange.end, state.timeRange.start + windowSize);
    }
    if (windowEnd > state.timeRange.end) {
        windowEnd = state.timeRange.end;
        windowStart = Math.max(state.timeRange.start, state.timeRange.end - windowSize);
    }
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
    const windowTx = state.allTransactions.filter(tx =>
        tx.end_ns >= windowStart && tx.start_ns <= windowEnd && tx.events && tx.events.length > 0
    );

    // Organize by operation type to match main timeline lanes: PUT, GET, UPD, SUB, CONN
    const opOrder = ['put', 'get', 'update', 'subscribe', 'connect'];
    const lanesByOp = { put: [], get: [], update: [], subscribe: [], connect: [], other: [] };
    windowTx.forEach(tx => {
        const op = tx.op || 'other';
        const targetLane = lanesByOp[op] || lanesByOp.other;
        targetLane.push(tx);
    });

    const lanesContainer = document.getElementById('timeline-lanes');
    lanesContainer.innerHTML = '';
    const laneHeight = 18;
    let currentTop = 0;

    opOrder.forEach(op => {
        const txList = lanesByOp[op];
        if (txList.length === 0) return;

        const laneDiv = document.createElement('div');
        laneDiv.className = 'tx-lane';
        laneDiv.style.top = `${currentTop}px`;
        currentTop += laneHeight;

        txList.forEach(tx => {
            const events = tx.events || [];
            if (events.length === 0) return;

            // Calculate transaction position and width
            const txStart = Math.max(windowStart, tx.start_ns);
            const txEnd = Math.min(windowEnd, tx.end_ns || tx.start_ns);
            const startPos = (txStart - windowStart) / windowDuration;
            const endPos = (txEnd - windowStart) / windowDuration;

            const opClass = tx.op || 'other';
            const statusClass = tx.status === 'pending' ? ' pending' : tx.status === 'failed' ? ' failed' : '';
            const duration = tx.duration_ms ? `${tx.duration_ms.toFixed(1)}ms` : 'pending';
            const tooltip = `${tx.op}: ${tx.contract || 'no contract'}\nDuration: ${duration}\nEvents: ${events.length}`;

            // Check if this transaction matches the selected contract
            const dimClass = state.selectedContract && tx.contract_full !== state.selectedContract ? ' dimmed' : '';

            // Create container for the transaction
            const txContainer = document.createElement('div');
            txContainer.className = `tx-container ${opClass}${statusClass}${dimClass}`;
            txContainer.style.cssText = `left:${startPos * 100}%;width:${(endPos - startPos) * 100}%;`;
            txContainer.title = tooltip;
            txContainer.dataset.txId = tx.tx_id;

            // Thin line showing transaction duration
            const txLine = document.createElement('div');
            txLine.className = `tx-line ${opClass}`;
            txContainer.appendChild(txLine);

            // Event pills positioned along the transaction
            events.forEach(evt => {
                const evtTime = evt.timestamp;
                if (!evtTime || evtTime < windowStart || evtTime > windowEnd) return;

                // Position within the transaction container (0-100%)
                const txDuration = txEnd - txStart;
                const evtPos = txDuration > 0 ? ((evtTime - txStart) / txDuration) * 100 : 50;

                const pill = document.createElement('div');
                pill.className = `tx-pill ${opClass}`;
                pill.style.left = `${Math.max(0, Math.min(100, evtPos))}%`;
                txContainer.appendChild(pill);
            });

            laneDiv.appendChild(txContainer);
        });
        lanesContainer.appendChild(laneDiv);
    });
}

/**
 * Render the main timeline with event markers
 */
let lastTimelineKey = null;
export function renderTimeline() {
    // Skip if nothing changed (time range, event count, or selected contract)
    const timelineKey = `${state.timeRange.start}-${state.timeRange.end}-${state.allEvents.length}-${state.selectedContract}`;
    if (timelineKey === lastTimelineKey) return;
    lastTimelineKey = timelineKey;

    const container = document.getElementById('timeline-events');
    container.innerHTML = '';

    if (state.allEvents.length === 0 || state.timeRange.end <= state.timeRange.start) return;

    const duration = state.timeRange.end - state.timeRange.start;

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

    // Create lane backgrounds
    Object.entries(lanes).forEach(([type, lane]) => {
        const bg = document.createElement('div');
        bg.className = 'timeline-lane-bg';
        bg.style.cssText = `position:absolute;left:0;right:0;top:${lane.row * (laneHeight + laneGap)}px;height:${laneHeight}px;background:rgba(255,255,255,0.02);border-radius:2px;`;
        container.appendChild(bg);
    });

    // Group events into time buckets per lane
    const buckets = {};
    state.allEvents.forEach(event => {
        const bucket = Math.round((event.timestamp - state.timeRange.start) / duration * 400);
        const type = event.event_type;

        // Determine which lane
        let laneType = 'connect';
        if (type.includes('put')) laneType = 'put';
        else if (type.includes('get')) laneType = 'get';
        else if (type.includes('update') || type.includes('broadcast')) laneType = 'update';
        else if (type.includes('subscrib')) laneType = 'subscribe';
        else if (!type.includes('connect')) return; // Skip other events

        const key = `${bucket}-${laneType}`;
        if (!buckets[key]) buckets[key] = { bucket, laneType, events: [], hasResponse: false, matchesSelectedContract: false };
        buckets[key].events.push(event);

        // Track if this bucket has a response/success
        if (type.includes('success') || type.includes('subscribed')) {
            buckets[key].hasResponse = true;
        }

        // Track if any event matches the selected contract
        if (state.selectedContract && event.contract_full === state.selectedContract) {
            buckets[key].matchesSelectedContract = true;
        }
    });

    // Render markers
    Object.values(buckets).forEach(data => {
        const posPercent = data.bucket / 4; // 400 buckets -> 100%
        if (posPercent < 0 || posPercent > 100) return;

        const lane = lanes[data.laneType];
        const marker = document.createElement('div');

        // Brighter if has response/success, dimmer if just request
        // Further dim if a contract is selected and this bucket doesn't match
        let opacity = data.hasResponse ? 1 : 0.5;
        if (state.selectedContract && !data.matchesSelectedContract) {
            opacity *= 0.15; // Dim non-matching events
        }
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
        marker.dataset.timestamp = data.events[0].timestamp;
        container.appendChild(marker);
    });

    document.getElementById('timeline-start').textContent = formatTime(state.timeRange.start);
}

/**
 * Update the playhead position and time displays
 */
export function updatePlayhead() {
    if (state.timeRange.end <= state.timeRange.start) return;

    const duration = state.timeRange.end - state.timeRange.start;
    const timeline = document.getElementById('timeline');
    // Events area: left: 50px, right: 16px, so width = timeline.offsetWidth - 66
    const eventsAreaWidth = timeline.offsetWidth - 66;

    // Calculate window width as percentage of time range
    const windowDuration = state.timeWindowNs * 2; // total window size
    let windowWidthPercent = (windowDuration / duration) * 100;

    // Ensure minimum width
    const minWidthPercent = (MIN_PLAYHEAD_WIDTH_PX / eventsAreaWidth) * 100;
    windowWidthPercent = Math.max(windowWidthPercent, minWidthPercent);

    // Cap at 100%
    windowWidthPercent = Math.min(windowWidthPercent, 100);

    // Calculate center position
    const centerPos = (state.currentTime - state.timeRange.start) / duration;
    const clampedCenter = Math.min(Math.max(centerPos, 0), 1);

    // Calculate left edge (center - half width)
    let leftPos = clampedCenter * 100 - windowWidthPercent / 2;
    leftPos = Math.max(0, Math.min(leftPos, 100 - windowWidthPercent));

    // Convert to pixels relative to the events area (which starts at 50px)
    const playhead = document.getElementById('playhead');
    const leftPx = (leftPos / 100) * eventsAreaWidth + 50;
    const widthPx = (windowWidthPercent / 100) * eventsAreaWidth;
    playhead.style.left = `${leftPx}px`;
    playhead.style.width = `${widthPx}px`;

    // Calculate where currentTime actually falls within the (possibly clamped) window
    // This ensures the center line points to currentTime, not just the geometric center
    const windowSize = state.timeWindowNs * 2;
    let windowStart = state.currentTime - state.timeWindowNs;
    let windowEnd = state.currentTime + state.timeWindowNs;
    if (windowStart < state.timeRange.start) {
        windowStart = state.timeRange.start;
        windowEnd = Math.min(state.timeRange.end, state.timeRange.start + windowSize);
    }
    if (windowEnd > state.timeRange.end) {
        windowEnd = state.timeRange.end;
        windowStart = Math.max(state.timeRange.start, state.timeRange.end - windowSize);
    }
    const actualWindowDuration = windowEnd - windowStart;
    const currentTimeInWindow = actualWindowDuration > 0 ? (state.currentTime - windowStart) / actualWindowDuration : 0.5;
    playhead.style.setProperty('--center-line-position', `${currentTimeInWindow * 100}%`);

    document.getElementById('playhead-time').textContent = formatTime(state.currentTime);
    document.getElementById('playhead-date').textContent = formatDate(state.currentTime);
    document.getElementById('time-display').textContent = formatTime(state.currentTime).split(' ')[0];

    // Update topology time label
    const topoLabel = document.getElementById('topology-time-label');
    if (state.isLive) {
        topoLabel.textContent = 'Live';
        topoLabel.classList.add('live');
    } else {
        const timeStr = new Date(state.currentTime / 1_000_000).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
        topoLabel.textContent = `at ${timeStr}`;
        topoLabel.classList.remove('live');
    }

    renderDetailTimeline();
}

/**
 * Update the time window label display
 */
export function updateWindowLabel() {
    const totalMinutes = Math.round(state.timeWindowNs * 2 / 60_000_000_000);
    let label;
    if (totalMinutes >= 60) {
        label = `${Math.round(totalMinutes / 60)} hr`;
    } else {
        label = `${totalMinutes} min`;
    }
    document.getElementById('window-label').textContent = label;
}

/**
 * Go to live mode
 * @param {Function} updateView - Callback to update the view
 * @param {Function} updateURL - Callback to update the URL
 */
export function goLive(updateView, updateURL) {
    state.isLive = true;
    state.currentTime = Date.now() * 1_000_000;
    state.selectedEvent = null;
    state.selectedPeerId = null;
    state.selectedContract = null;
    state.highlightedPeers.clear();
    document.getElementById('mode-button').className = 'timeline-mode live';
    document.getElementById('mode-button').textContent = 'LIVE';
    document.getElementById('status-dot').className = 'status-dot live';
    document.getElementById('status-text').textContent = 'Live';
    document.getElementById('events-title').textContent = 'Events';
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Go to a specific time (historical mode)
 * @param {number} time - Timestamp in nanoseconds
 * @param {Function} updateView - Callback to update the view
 * @param {Function} updateURL - Callback to update the URL
 */
export function goToTime(time, updateView, updateURL) {
    state.isLive = false;
    state.currentTime = time;
    document.getElementById('mode-button').className = 'timeline-mode historical';
    document.getElementById('mode-button').textContent = 'HISTORICAL';
    document.getElementById('status-dot').className = 'status-dot historical';
    document.getElementById('status-text').textContent = 'Time Travel';
    document.getElementById('events-title').textContent = 'Events at ' + formatTime(time);
    if (updateView) updateView();
    if (updateURL) updateURL();
}

/**
 * Setup timeline interaction (drag, resize, click, keyboard)
 * @param {Object} callbacks - Object with callback functions
 * @param {Function} callbacks.goToTime - Function to navigate to a time
 * @param {Function} callbacks.goLive - Function to go to live mode
 * @param {Function} callbacks.updateView - Function to update the view
 * @param {Function} callbacks.renderContractsList - Function to render contracts list
 */
export function setupTimeline(callbacks) {
    const timeline = document.getElementById('timeline');

    function getTimeFromX(clientX) {
        const rect = timeline.getBoundingClientRect();
        // Events area starts at 50px from left, width = rect.width - 66
        const pos = Math.max(0, Math.min(1, (clientX - rect.left - 50) / (rect.width - 66)));
        return state.timeRange.start + pos * (state.timeRange.end - state.timeRange.start);
    }

    // Click on timeline background to jump
    let justDragged = false;
    timeline.addEventListener('click', (e) => {
        if (justDragged) { justDragged = false; return; }
        if (e.target.closest('.timeline-playhead')) return;
        if (e.target.classList.contains('timeline-marker')) return;
        callbacks.goToTime(getTimeFromX(e.clientX));
    });

    // Drag states
    let dragMode = null, dragStartX = 0, dragStartLeft = 0, dragStartRight = 0;
    const playhead = document.getElementById('playhead');
    const resizeLeft = document.getElementById('resize-left');
    const resizeRight = document.getElementById('resize-right');

    function getWindowEdges() {
        const windowSize = state.timeWindowNs * 2;
        let left = state.currentTime - state.timeWindowNs;
        let right = state.currentTime + state.timeWindowNs;
        if (left < state.timeRange.start) {
            left = state.timeRange.start;
            right = Math.min(state.timeRange.end, state.timeRange.start + windowSize);
        }
        if (right > state.timeRange.end) {
            right = state.timeRange.end;
            left = Math.max(state.timeRange.start, state.timeRange.end - windowSize);
        }
        return { left, right };
    }

    // Helper to get clientX from mouse or touch event
    function getClientX(e) {
        return e.touches ? e.touches[0].clientX : e.clientX;
    }

    function startResizeLeft(e) {
        dragMode = 'resize-left'; dragStartX = getClientX(e);
        const edges = getWindowEdges(); dragStartLeft = edges.left; dragStartRight = edges.right;
        e.preventDefault(); e.stopPropagation();
    }
    resizeLeft.addEventListener('mousedown', startResizeLeft);
    resizeLeft.addEventListener('touchstart', startResizeLeft, { passive: false });

    function startResizeRight(e) {
        dragMode = 'resize-right'; dragStartX = getClientX(e);
        const edges = getWindowEdges(); dragStartLeft = edges.left; dragStartRight = edges.right;
        e.preventDefault(); e.stopPropagation();
    }
    resizeRight.addEventListener('mousedown', startResizeRight);
    resizeRight.addEventListener('touchstart', startResizeRight, { passive: false });

    function startMove(e) {
        if (e.target === resizeLeft || e.target === resizeRight) return;
        dragMode = 'move'; dragStartX = getClientX(e);
        const edges = getWindowEdges(); dragStartLeft = edges.left; dragStartRight = edges.right;
        playhead.style.cursor = 'grabbing';
        e.preventDefault(); e.stopPropagation();
    }
    playhead.addEventListener('mousedown', startMove);
    playhead.addEventListener('touchstart', startMove, { passive: false });

    playhead.addEventListener('click', (e) => e.stopPropagation());

    function handleDragMove(e) {
        if (!dragMode) return;
        // Prevent scroll during drag on touch devices
        if (e.cancelable) e.preventDefault();
        const clientX = getClientX(e);
        const rect = timeline.getBoundingClientRect();
        // Events area: left: 50px, right: 16px, so width = rect.width - 66
        const eventsAreaWidth = rect.width - 66;
        const duration = state.timeRange.end - state.timeRange.start;
        const deltaX = clientX - dragStartX;
        const deltaNs = (deltaX / eventsAreaWidth) * duration;

        if (dragMode === 'move') {
            const windowSize = dragStartRight - dragStartLeft;
            let newLeft = dragStartLeft + deltaNs, newRight = dragStartRight + deltaNs;
            if (newLeft < state.timeRange.start) {
                newLeft = state.timeRange.start;
                newRight = state.timeRange.start + windowSize;
            }
            if (newRight > state.timeRange.end) {
                newRight = state.timeRange.end;
                newLeft = state.timeRange.end - windowSize;
            }
            state.currentTime = (newLeft + newRight) / 2;
            state.isLive = false;
            updatePlayhead();
            callbacks.updateView();
        } else if (dragMode === 'resize-left') {
            const newLeft = dragStartLeft + deltaNs;
            const newHalfWindow = (dragStartRight - newLeft) / 2;
            if (newHalfWindow >= MIN_TIME_WINDOW_NS && newHalfWindow <= MAX_TIME_WINDOW_NS && newLeft >= state.timeRange.start) {
                state.timeWindowNs = newHalfWindow;
                state.currentTime = (newLeft + dragStartRight) / 2;
                state.isLive = false;
                updateWindowLabel();
                updatePlayhead();
                callbacks.updateView();
            }
        } else if (dragMode === 'resize-right') {
            const newRight = dragStartRight + deltaNs;
            const newHalfWindow = (newRight - dragStartLeft) / 2;
            if (newHalfWindow >= MIN_TIME_WINDOW_NS && newHalfWindow <= MAX_TIME_WINDOW_NS && newRight <= state.timeRange.end) {
                state.timeWindowNs = newHalfWindow;
                state.currentTime = (dragStartLeft + newRight) / 2;
                state.isLive = false;
                updateWindowLabel();
                updatePlayhead();
                callbacks.updateView();
            }
        }
    }
    document.addEventListener('mousemove', handleDragMove);
    document.addEventListener('touchmove', handleDragMove, { passive: false });

    function handleDragEnd() {
        if (dragMode) {
            justDragged = true;
            if (dragMode === 'move') playhead.style.cursor = 'grab';
            dragMode = null;
            setTimeout(() => { justDragged = false; }, 100);
        }
    }
    document.addEventListener('mouseup', handleDragEnd);
    document.addEventListener('touchend', handleDragEnd);
    document.addEventListener('touchcancel', handleDragEnd);

    // Initialize window label
    updateWindowLabel();

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        if (e.target.tagName === 'INPUT') return;

        const step = (state.timeRange.end - state.timeRange.start) / 100;

        if (e.key === 'ArrowLeft') {
            callbacks.goToTime(Math.max(state.timeRange.start, state.currentTime - step));
        } else if (e.key === 'ArrowRight') {
            callbacks.goToTime(Math.min(state.timeRange.end, state.currentTime + step));
        } else if (e.key === 'l' || e.key === 'L') {
            callbacks.goLive();
        }
    });

    // Contract search input
    const contractSearch = document.getElementById('contract-search');
    if (contractSearch) {
        contractSearch.addEventListener('input', (e) => {
            state.contractSearchText = e.target.value;
            if (callbacks.renderContractsList) callbacks.renderContractsList();
        });
    }
}

/**
 * Add an event marker to the timeline (for live events)
 * @param {Object} event - Event data
 */
export function addEventMarker(event) {
    if (state.timeRange.end <= state.timeRange.start) return;

    const container = document.getElementById('timeline-events');
    const duration = state.timeRange.end - state.timeRange.start;
    const pos = (event.timestamp - state.timeRange.start) / duration;

    const marker = document.createElement('div');
    marker.className = `timeline-marker ${getEventClass(event.event_type)}`;
    marker.style.left = `${pos * 100}%`;
    marker.style.height = '20px';
    marker.style.top = '30px';
    marker.title = `${event.time_str} - ${event.event_type}`;
    container.appendChild(marker);
}
