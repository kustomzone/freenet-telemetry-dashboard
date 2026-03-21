/**
 * Timeline module for Freenet Dashboard
 * Exponential-scale canvas timeline with hover tooltips
 */

import { state } from './state.js';
import { formatTime, formatDate, getEventClass, getEventLabel } from './utils.js';

// Time window constants (kept for compatibility with other modules)
export const MIN_TIME_WINDOW_NS = 1 * 60 * 1_000_000_000;
export const MAX_TIME_WINDOW_NS = 60 * 60 * 1_000_000_000;

const K = 6; // exponential scale factor: last ~5min fills ~50% of width

// Lane configuration (order matches left-side labels)
const LANE_ROWS = { connect: 0, get: 1, put: 2, subscribe: 3, update: 4 };
const NUM_LANES = 5;
const LANE_GAP = 2;

// Lane colors resolved from CSS variables (cached)
let laneColors = null;

function resolveLaneColors() {
    const style = getComputedStyle(document.documentElement);
    laneColors = {
        put:       style.getPropertyValue('--color-put').trim(),
        get:       style.getPropertyValue('--color-get').trim(),
        update:    style.getPropertyValue('--color-update').trim(),
        subscribe: style.getPropertyValue('--color-subscribe').trim(),
        connect:   style.getPropertyValue('--color-connect').trim(),
    };
}

/**
 * Map a timestamp to an x-coordinate on the canvas.
 * Recent events (near tNow) get more screen space.
 */
export function timeToX(timestamp, tNow, totalDurationNs, width) {
    const age = tNow - timestamp;
    if (age <= 0) return width;
    if (totalDurationNs <= 0) return width;
    const normalizedAge = Math.min(age / totalDurationNs, 1);
    return width * (1 - Math.log1p(K * normalizedAge) / Math.log1p(K));
}

/**
 * Inverse: map an x-coordinate back to a timestamp.
 */
export function xToTime(x, tNow, totalDurationNs, width) {
    if (width <= 0) return tNow;
    const normalizedX = Math.max(0, Math.min(1, x / width));
    const normalizedAge = Math.expm1((1 - normalizedX) * Math.log1p(K)) / K;
    return tNow - normalizedAge * totalDurationNs;
}

/**
 * Classify an event type into a lane name.
 */
function eventToLane(eventType) {
    if (!eventType) return null;
    if (eventType.includes('put')) return 'put';
    if (eventType.includes('get')) return 'get';
    if (eventType.includes('update') || eventType.includes('broadcast')) return 'update';
    if (eventType.includes('subscrib')) return 'subscribe';
    if (eventType.includes('connect')) return 'connect';
    return null;
}

/**
 * Check if an event is "completed" (response/success).
 */
function isCompleted(eventType) {
    if (!eventType) return false;
    return eventType.includes('success') || eventType.includes('subscribed') || eventType.includes('connected');
}

/**
 * Check if an event matches the current contract/peer filters.
 */
function eventMatchesFilters(event) {
    if (state.selectedContract && event.contract_full !== state.selectedContract) return false;
    if (state.selectedPeerId) {
        if (event.peer_id !== state.selectedPeerId &&
            event.from_peer !== state.selectedPeerId &&
            event.to_peer !== state.selectedPeerId &&
            !(event.connection && (event.connection[0] === state.selectedPeerId || event.connection[1] === state.selectedPeerId))) {
            return false;
        }
    }
    return true;
}

// Cache key to avoid unnecessary canvas redraws
let lastCanvasKey = null;

/**
 * Render the exponential timeline on the canvas.
 */
export function renderExponentialTimeline() {
    const canvas = document.getElementById('timeline-canvas');
    if (!canvas) return;

    const tNow = Date.now() * 1_000_000;
    state.currentTime = tNow;
    const totalDurationNs = tNow - state.timeRange.start;

    // Cache check — include replay range and drag state so highlight redraws
    const replayKey = state.replayRange ? `${state.replayRange.startNs}-${state.replayRange.endNs}` : 'none';
    const dragKey = isDragging ? `${dragStartX}-${dragCurrentX}` : '';
    const cacheKey = `${tNow}-${state.timeRange.start}-${state.allEvents.length}-${state.selectedContract}-${state.selectedPeerId}-${canvas.clientWidth}-${canvas.clientHeight}-${replayKey}-${dragKey}`;
    if (cacheKey === lastCanvasKey) return;
    lastCanvasKey = cacheKey;

    if (!laneColors) resolveLaneColors();

    // Set canvas resolution to match display size (HiDPI aware)
    const dpr = window.devicePixelRatio || 1;
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, width, height);

    if (state.allEvents.length === 0 || totalDurationNs <= 0) return;

    const laneHeight = (height - (NUM_LANES - 1) * LANE_GAP) / NUM_LANES;

    // Draw lane backgrounds (theme-aware)
    const isLightMode = document.documentElement.getAttribute('data-theme') === 'light';
    for (let i = 0; i < NUM_LANES; i++) {
        const y = i * (laneHeight + LANE_GAP);
        ctx.fillStyle = isLightMode ? 'rgba(0, 0, 0, 0.02)' : 'rgba(255, 255, 255, 0.02)';
        ctx.fillRect(0, y, width, laneHeight);
    }

    const hasFilter = !!(state.selectedContract || state.selectedPeerId);

    // Draw event bars (normal compositing to preserve lane colors)
    for (const event of state.allEvents) {
        const lane = eventToLane(event.event_type);
        if (lane === null) continue;

        const x = timeToX(event.timestamp, tNow, totalDurationNs, width);
        if (x < -3 || x > width + 3) continue;

        const y = LANE_ROWS[lane] * (laneHeight + LANE_GAP);

        let opacity = isCompleted(event.event_type) ? 0.9 : 0.5;
        if (hasFilter && !eventMatchesFilters(event)) {
            opacity *= 0.15;
        }

        ctx.fillStyle = laneColors[lane];
        ctx.globalAlpha = opacity;
        ctx.fillRect(x - 1.5, y, 3, laneHeight);
    }

    ctx.globalAlpha = 1;

    // Draw time ticks
    drawTicks(ctx, tNow, totalDurationNs, width, height);

    // Draw replay range highlight
    drawReplayHighlight(ctx, width, height, tNow, totalDurationNs);

}

/**
 * Draw relative time tick labels on the canvas.
 */
function drawTicks(ctx, tNow, totalDurationNs, width, height) {
    const ticks = [
        { label: 'now', ageNs: 0 },
        { label: '1m',  ageNs: 1 * 60e9 },
        { label: '5m',  ageNs: 5 * 60e9 },
        { label: '15m', ageNs: 15 * 60e9 },
        { label: '30m', ageNs: 30 * 60e9 },
        { label: '1h',  ageNs: 60 * 60e9 },
        { label: '2h',  ageNs: 2 * 60 * 60e9 },
    ];

    ctx.font = '10px "JetBrains Mono", monospace';
    ctx.textAlign = 'center';

    for (const tick of ticks) {
        if (tick.ageNs > totalDurationNs) continue;
        const x = timeToX(tNow - tick.ageNs, tNow, totalDurationNs, width);
        if (x < 15 || x > width - 15) continue;

        // Tick mark (theme-aware: white for dark mode, dark for light mode)
        const isLight = document.documentElement.getAttribute('data-theme') === 'light';
        ctx.strokeStyle = isLight ? 'rgba(0, 0, 0, 0.2)' : 'rgba(255, 255, 255, 0.15)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, 6);
        ctx.stroke();

        // Label
        ctx.fillStyle = isLight ? 'rgba(0, 0, 0, 0.45)' : 'rgba(255, 255, 255, 0.35)';
        ctx.fillText(tick.label, x, height - 2);
    }
}

// ============================================================================
// Canvas Interaction (hover, click)
// ============================================================================

/**
 * Hit-test: find the nearest event to a canvas position.
 */
function hitTest(canvasX, canvasY, canvas) {
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    const laneHeight = (height - (NUM_LANES - 1) * LANE_GAP) / NUM_LANES;

    // Determine which lane was hit
    let hitLane = -1;
    for (let i = 0; i < NUM_LANES; i++) {
        const laneTop = i * (laneHeight + LANE_GAP);
        if (canvasY >= laneTop && canvasY < laneTop + laneHeight) {
            hitLane = i;
            break;
        }
    }

    const tNow = state.currentTime;
    const totalDurationNs = tNow - state.timeRange.start;
    if (totalDurationNs <= 0) return null;

    const timestamp = xToTime(canvasX, tNow, totalDurationNs, width);

    // Time range corresponding to ±5 pixels
    const tLeft = xToTime(canvasX - 5, tNow, totalDurationNs, width);
    const tRight = xToTime(canvasX + 5, tNow, totalDurationNs, width);
    const pixelTimeRange = Math.abs(tLeft - tRight);

    const events = state.allEvents;
    if (events.length === 0) return null;

    // Binary search for start of time window
    const searchStart = timestamp - pixelTimeRange;
    let lo = 0, hi = events.length;
    while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (events[mid].timestamp < searchStart) lo = mid + 1;
        else hi = mid;
    }

    // Scan for nearest matching event
    let bestEvent = null;
    let bestDist = Infinity;

    for (let i = lo; i < events.length; i++) {
        const e = events[i];
        if (e.timestamp > timestamp + pixelTimeRange) break;

        const lane = eventToLane(e.event_type);
        if (lane === null) continue;
        if (hitLane >= 0 && LANE_ROWS[lane] !== hitLane) continue;

        const dist = Math.abs(e.timestamp - timestamp);
        if (dist < bestDist) {
            bestDist = dist;
            bestEvent = e;
        }
    }

    return bestEvent;
}

/**
 * Show tooltip near the cursor for a hovered event.
 */
function showTooltip(event, clientX, clientY) {
    const tooltip = document.getElementById('timeline-tooltip');
    if (!tooltip) return;

    const time = new Date(event.timestamp / 1_000_000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const eventLabel = getEventLabel(event.event_type);
    const eventClass = getEventClass(event.event_type);

    let html = `<div class="tooltip-type ${eventClass}">${eventLabel}</div>`;
    html += `<div class="tooltip-time">${time}</div>`;
    if (event.from_peer || event.peer_id) {
        html += `<div class="tooltip-peer">Peer: ${(event.from_peer || event.peer_id).substring(0, 12)}</div>`;
    }
    if (event.to_peer) {
        html += `<div class="tooltip-peer">To: ${event.to_peer.substring(0, 12)}</div>`;
    }
    if (event.contract) {
        html += `<div class="tooltip-contract">Contract: ${event.contract}</div>`;
    }
    if (event.tx_id) {
        html += `<div class="tooltip-tx">Tx: ${event.tx_id.substring(0, 8)}...</div>`;
    }

    tooltip.innerHTML = html;
    tooltip.style.display = 'block';

    // Position near cursor, keeping on screen
    const rect = tooltip.getBoundingClientRect();
    let left = clientX + 12;
    let top = clientY - rect.height - 8;
    if (left + rect.width > window.innerWidth) left = clientX - rect.width - 12;
    if (top < 0) top = clientY + 16;
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
}

function hideTooltip() {
    const tooltip = document.getElementById('timeline-tooltip');
    if (tooltip) tooltip.style.display = 'none';
}

// ============================================================================
// Replay range: drag-to-select a time window for looping particle animation
// ============================================================================

// Drag state (module-scoped, not in global state since it's transient UI)
let dragStartX = null;   // canvas X where drag started
let dragCurrentX = null;  // canvas X of current drag position
let isDragging = false;
let suppressNextClick = false; // eat the click event that follows a successful drag

/**
 * Collect message flows for a time range from transaction data.
 * Returns [{fromPeer, toPeer, eventType, offsetMs}] where offsetMs is
 * the relative time from range start (for staggered replay).
 */
export function collectFlowsForRange(startNs, endNs) {
    const events = state.allEvents;
    if (events.length === 0) { console.log('[flows] no events'); return []; }

    // Debug: check event timestamp range
    console.log(`[flows] searching ${startNs} - ${endNs}`);
    console.log(`[flows] events[0].ts=${events[0].timestamp}, events[last].ts=${events[events.length-1].timestamp}`);
    console.log(`[flows] total events: ${events.length}, total txns: ${state.allTransactions.length}`);

    // Binary search for start
    let lo = 0, hi = events.length;
    while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (events[mid].timestamp < startNs) lo = mid + 1;
        else hi = mid;
    }

    console.log(`[flows] binary search lo=${lo}`);

    // Collect events in the range, respecting filters
    const windowEvents = [];
    let skippedByFilter = 0;
    for (let i = lo; i < events.length; i++) {
        const e = events[i];
        if (e.timestamp > endNs) break;
        if (!eventMatchesFilters(e)) { skippedByFilter++; continue; }
        windowEvents.push(e);
    }

    console.log(`[flows] windowEvents: ${windowEvents.length}, skippedByFilter: ${skippedByFilter}`);
    if (windowEvents.length > 0) {
        const withTx = windowEvents.filter(e => e.tx_id).length;
        console.log(`[flows] with tx_id: ${withTx}, sample:`, windowEvents[0]);
    }

    // Build message flows from transactions
    const flows = [];
    const seenTx = new Set();
    const rangeMs = (endNs - startNs) / 1_000_000;

    let dbgNoTx = 0, dbgNotFound = 0, dbgTooFew = 0, dbgSinglePeer = 0, dbgMultiPeer = 0;

    for (const evt of windowEvents) {
        // Direct from_peer/to_peer (rare but ideal)
        if (evt.from_peer && evt.to_peer && evt.from_peer !== evt.to_peer) {
            flows.push({
                fromPeer: evt.from_peer, toPeer: evt.to_peer,
                eventType: evt.event_type,
                offsetMs: (evt.timestamp - startNs) / 1_000_000
            });
            continue;
        }

        // Infer from transaction: consecutive events on different peers
        if (evt.tx_id && !seenTx.has(evt.tx_id)) {
            seenTx.add(evt.tx_id);
            const txIdx = state.transactionMap.get(evt.tx_id);
            if (txIdx === undefined) { dbgNotFound++; continue; }
            const tx = state.allTransactions[txIdx];
            if (!tx || !tx.events || tx.events.length < 2) { dbgTooFew++; continue; }

            const sorted = [...tx.events].sort((a, b) => a.timestamp - b.timestamp);
            const uniquePeers = new Set(sorted.map(e => e.peer_id).filter(Boolean));
            if (uniquePeers.size < 2) { dbgSinglePeer++; continue; }
            dbgMultiPeer++;

            for (let j = 1; j < sorted.length; j++) {
                if (sorted[j].peer_id && sorted[j - 1].peer_id &&
                    sorted[j].peer_id !== sorted[j - 1].peer_id) {
                    // Use the midpoint timestamp for timing
                    const midTs = (sorted[j - 1].timestamp + sorted[j].timestamp) / 2;
                    flows.push({
                        fromPeer: sorted[j - 1].peer_id,
                        toPeer: sorted[j].peer_id,
                        eventType: sorted[j].event_type || tx.op,
                        offsetMs: Math.max(0, (midTs - startNs) / 1_000_000)
                    });
                }
            }
        } else if (!evt.tx_id) {
            dbgNoTx++;
        }
    }

    console.log(`[flows] txns: ${seenTx.size} unique, notFound=${dbgNotFound}, tooFew=${dbgTooFew}, singlePeer=${dbgSinglePeer}, multiPeer=${dbgMultiPeer}, noTxId=${dbgNoTx}`);

    // Debug: sample a tx_id from events and check the map
    if (dbgNotFound > 0) {
        const sampleEvt = windowEvents.find(e => e.tx_id);
        if (sampleEvt) {
            console.log(`[flows] sample evt tx_id: "${sampleEvt.tx_id}" (type: ${typeof sampleEvt.tx_id})`);
            const mapKeys = [...state.transactionMap.keys()].slice(0, 3);
            console.log(`[flows] transactionMap sample keys:`, mapKeys);
            console.log(`[flows] map.has(sample): ${state.transactionMap.has(sampleEvt.tx_id)}`);
            // Try exact string match
            const sampleKey = mapKeys[0];
            if (sampleKey) console.log(`[flows] key type: ${typeof sampleKey}, key[0]: "${sampleKey}"`);
        }
    }

    return flows;
}

/**
 * Draw the replay selection highlight on the timeline canvas.
 * Called from renderExponentialTimeline after event bars are drawn.
 */
function drawReplayHighlight(ctx, width, height, tNow, totalDurationNs) {
    const range = state.replayRange;
    const isLight = document.documentElement.getAttribute('data-theme') === 'light';

    // Active drag in progress (not yet committed)
    if (isDragging && dragStartX !== null && dragCurrentX !== null) {
        const x1 = Math.min(dragStartX, dragCurrentX);
        const x2 = Math.max(dragStartX, dragCurrentX);
        ctx.fillStyle = isLight ? 'rgba(0, 127, 255, 0.15)' : 'rgba(0, 180, 255, 0.12)';
        ctx.fillRect(x1, 0, x2 - x1, height);
        ctx.strokeStyle = isLight ? 'rgba(0, 127, 255, 0.5)' : 'rgba(0, 180, 255, 0.4)';
        ctx.lineWidth = 1;
        ctx.strokeRect(x1, 0, x2 - x1, height);
        return;
    }

    // Committed replay range
    if (!range) return;
    const x1 = timeToX(range.startNs, tNow, totalDurationNs, width);
    const x2 = timeToX(range.endNs, tNow, totalDurationNs, width);
    const left = Math.min(x1, x2);
    const right = Math.max(x1, x2);

    // Dim everything outside the range
    ctx.fillStyle = isLight ? 'rgba(0, 0, 0, 0.08)' : 'rgba(0, 0, 0, 0.3)';
    ctx.fillRect(0, 0, left, height);
    ctx.fillRect(right, 0, width - right, height);

    // Highlight border
    ctx.strokeStyle = isLight ? 'rgba(0, 127, 255, 0.6)' : 'rgba(0, 180, 255, 0.5)';
    ctx.lineWidth = 1.5;
    ctx.strokeRect(left, 0, right - left, height);

    // "REPLAY" label
    const centerX = (left + right) / 2;
    ctx.font = '9px "JetBrains Mono", monospace';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.fillStyle = isLight ? 'rgba(0, 127, 255, 0.7)' : 'rgba(0, 180, 255, 0.6)';
    ctx.fillText('▶ REPLAY', centerX, 2);
}

// ============================================================================
// Public API
// ============================================================================

/**
 * Update time displays and render the canvas timeline.
 * (Simplified: no DOM playhead positioning.)
 */
export function updatePlayhead() {
    if (state.timeRange.end <= state.timeRange.start) return;

    document.getElementById('playhead-time').textContent = formatTime(state.currentTime);
    document.getElementById('playhead-date').textContent = formatDate(state.currentTime);
    document.getElementById('time-display').textContent = formatTime(state.currentTime).split(' ')[0];

    // Update topology time label — always live
    const topoLabel = document.getElementById('topology-time-label');
    topoLabel.textContent = 'Live';
    topoLabel.classList.add('live');

    renderExponentialTimeline();
}

/**
 * No-op stubs — kept as exports so existing callers don't break.
 * The exponential canvas replaces renderTimeline, renderRuler, renderDetailTimeline.
 */
export function renderTimeline() { renderExponentialTimeline(); }
export function renderRuler() { /* ticks are drawn on canvas */ }
export function renderDetailTimeline() { /* removed */ }
export function updateWindowLabel() { /* no playhead window */ }
export function addEventMarker() { /* no-op */ }


/**
 * Setup timeline interaction (canvas hover/click, keyboard shortcuts).
 */
export function setupTimeline(callbacks) {
    const canvas = document.getElementById('timeline-canvas');
    if (!canvas) return;

    // --- Canvas hover for tooltips + event visualization ---
    canvas.addEventListener('mousemove', (e) => {
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;

        // If dragging a replay range, update drag position
        if (isDragging) {
            dragCurrentX = x;
            canvas.style.cursor = 'col-resize';
            hideTooltip();
            lastCanvasKey = null; // force redraw for highlight
            renderExponentialTimeline();
            return;
        }

        const event = hitTest(x, y, canvas);
        if (event) {
            canvas.style.cursor = 'pointer';
            showTooltip(event, e.clientX, e.clientY);
            state.hoveredEvent = event;
            if (callbacks.onEventHover) callbacks.onEventHover(event);
        } else {
            canvas.style.cursor = 'crosshair';
            hideTooltip();
            if (state.hoveredEvent) {
                state.hoveredEvent = null;
                if (callbacks.onEventHover) callbacks.onEventHover(null);
            }
        }
    });

    canvas.addEventListener('mouseleave', () => {
        hideTooltip();
        canvas.style.cursor = 'crosshair';
        if (state.hoveredEvent) {
            state.hoveredEvent = null;
            if (callbacks.onEventHover) callbacks.onEventHover(null);
        }
        // Cancel drag if mouse leaves
        if (isDragging) {
            isDragging = false;
            dragStartX = null;
            dragCurrentX = null;
            lastCanvasKey = null;
            renderExponentialTimeline();
        }
    });

    // --- Drag to select replay range ---
    canvas.addEventListener('mousedown', (e) => {
        // Only left button, and only if not clicking an event
        if (e.button !== 0) return;
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;

        // If clicking on an event, let the click handler deal with it
        const event = hitTest(x, y, canvas);
        if (event) return;

        isDragging = true;
        dragStartX = x;
        dragCurrentX = x;
        e.preventDefault(); // prevent text selection
    });

    canvas.addEventListener('mouseup', (e) => {
        if (!isDragging) return;
        isDragging = false;

        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const width = canvas.clientWidth;
        const tNow = state.currentTime;
        const totalDurationNs = tNow - state.timeRange.start;

        // Convert pixel range to time range
        const t1 = xToTime(dragStartX, tNow, totalDurationNs, width);
        const t2 = xToTime(x, tNow, totalDurationNs, width);
        const startNs = Math.min(t1, t2);
        const endNs = Math.max(t1, t2);

        dragStartX = null;
        dragCurrentX = null;

        // Minimum 2px drag to distinguish from click
        if (Math.abs(t1 - t2) < 1_000_000_000) { // < 1 second
            // Too small — treat as click to clear
            state.replayRange = null;
            if (callbacks.onReplayRange) callbacks.onReplayRange(null);
            lastCanvasKey = null;
            renderExponentialTimeline();
            return;
        }

        state.replayRange = { startNs, endNs };
        suppressNextClick = true; // prevent the click event from immediately clearing it
        lastCanvasKey = null;
        renderExponentialTimeline();
        if (callbacks.onReplayRange) callbacks.onReplayRange({ startNs, endNs });
    });

    // --- Canvas click: select event or clear replay ---
    canvas.addEventListener('click', (e) => {
        if (suppressNextClick) {
            suppressNextClick = false;
            return;
        }
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const event = hitTest(x, y, canvas);

        if (event) {
            if (callbacks.selectEvent) {
                callbacks.selectEvent(event);
            }
        } else if (state.replayRange) {
            // Click on background clears replay range
            state.replayRange = null;
            if (callbacks.onReplayRange) callbacks.onReplayRange(null);
            lastCanvasKey = null;
            renderExponentialTimeline();
        } else {
            // Click on background: clear event selection
            if (callbacks.selectEvent) {
                callbacks.selectEvent(null);
            }
        }
    });

    // Escape clears replay range
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && state.replayRange) {
            state.replayRange = null;
            if (callbacks.onReplayRange) callbacks.onReplayRange(null);
            lastCanvasKey = null;
            renderExponentialTimeline();
        }
    });

    // --- Contract search input ---
    const contractSearch = document.getElementById('contract-search');
    if (contractSearch) {
        contractSearch.addEventListener('input', (e) => {
            state.contractSearchText = e.target.value;
            if (callbacks.renderContractsList) callbacks.renderContractsList();
        });
    }
}
