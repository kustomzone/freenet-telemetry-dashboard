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

    // Cache check — include playhead position (rounded to reduce redraws to ~20/sec)
    const replayKey = state.replayRange ? `${state.replayRange.startNs}-${state.replayRange.endNs}` : 'none';
    const dragKey = isDragging ? `${dragStartX}-${dragCurrentX}` : '';
    const pauseKey = state.replayPaused ? 'p' : '';
    // During replay, redraw ~30 times/sec for smooth playhead sweep
    const playheadKey = state.replayPlayheadMs > 0 ? (performance.now() / 33 | 0) : '';
    const cacheKey = `${tNow}-${state.timeRange.start}-${state.allEvents.length}-${state.selectedContract}-${state.selectedPeerId}-${canvas.clientWidth}-${canvas.clientHeight}-${replayKey}-${dragKey}-${pauseKey}-${playheadKey}`;
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

    // Position tick labels as DOM elements below the canvas (not on it)
    const tickContainer = document.getElementById('timeline-ticks');
    if (!tickContainer) return;

    // Build tick HTML only if positions changed
    let html = '';
    for (const tick of ticks) {
        if (tick.ageNs > totalDurationNs) continue;
        const x = timeToX(tNow - tick.ageNs, tNow, totalDurationNs, width);
        if (x < 10 || x > width - 10) continue;
        const pct = (x / width * 100).toFixed(2);
        html += `<span class="timeline-tick" style="left:${pct}%">${tick.label}</span>`;
    }
    tickContainer.innerHTML = html;
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

    // Playhead line — uses timeToX for correct logarithmic positioning
    if (state.replayPlayheadMs > 0) {
        const playheadNs = state.replayPlayheadMs * 1_000_000;
        const px = timeToX(playheadNs, tNow, totalDurationNs, width);
        if (px >= 0 && px <= width) {
            ctx.beginPath();
            ctx.moveTo(px, 0);
            ctx.lineTo(px, height);
            ctx.strokeStyle = isLight ? 'rgba(0, 0, 0, 0.8)' : 'rgba(255, 255, 255, 0.95)';
            ctx.lineWidth = 2;
            ctx.stroke();
        }
    }

    // PAUSED label in the selection region
    if (state.replayPaused) {
        const centerX = (left + right) / 2;
        ctx.font = 'bold 11px "JetBrains Mono", monospace';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        ctx.fillStyle = isLight ? 'rgba(0, 127, 255, 0.8)' : 'rgba(0, 180, 255, 0.8)';
        ctx.fillText('PAUSED', centerX, 2);
    }
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
        if (isDragging) return; // handled by document-level listener

        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;

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
        if (!isDragging) {
            canvas.style.cursor = 'crosshair';
        }
        if (state.hoveredEvent) {
            state.hoveredEvent = null;
            if (callbacks.onEventHover) callbacks.onEventHover(null);
        }
    });

    // --- Drag to select replay range ---
    // mousedown on canvas starts the drag
    canvas.addEventListener('mousedown', (e) => {
        if (e.button !== 0) return;
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;

        const event = hitTest(x, y, canvas);
        if (event) return;

        isDragging = true;
        dragStartX = x;
        dragCurrentX = x;
        e.preventDefault();
    });

    // mousemove and mouseup on document so dragging works outside the canvas
    document.addEventListener('mousemove', (e) => {
        if (!isDragging) return;
        const rect = canvas.getBoundingClientRect();
        // Clamp x to canvas bounds
        const x = Math.max(0, Math.min(canvas.clientWidth, e.clientX - rect.left));
        dragCurrentX = x;
        canvas.style.cursor = 'col-resize';
        lastCanvasKey = null;
        renderExponentialTimeline();
    });

    document.addEventListener('mouseup', (e) => {
        if (!isDragging) return;
        isDragging = false;

        const rect = canvas.getBoundingClientRect();
        // Clamp to canvas bounds
        const x = Math.max(0, Math.min(canvas.clientWidth, e.clientX - rect.left));
        const width = canvas.clientWidth;
        const tNow = state.currentTime;
        const totalDurationNs = tNow - state.timeRange.start;

        const t1 = xToTime(dragStartX, tNow, totalDurationNs, width);
        const t2 = xToTime(x, tNow, totalDurationNs, width);
        const startNs = Math.min(t1, t2);
        const endNs = Math.max(t1, t2);

        dragStartX = null;
        dragCurrentX = null;
        canvas.style.cursor = 'crosshair';

        if (Math.abs(t1 - t2) < 1_000_000_000) {
            state.replayRange = null;
            if (callbacks.onReplayRange) callbacks.onReplayRange(null);
            lastCanvasKey = null;
            renderExponentialTimeline();
            return;
        }

        state.replayRange = { startNs, endNs };
        suppressNextClick = true;
        lastCanvasKey = null;
        renderExponentialTimeline();
        if (callbacks.onReplayRange) callbacks.onReplayRange({ startNs, endNs });
    });

    // --- Canvas click: select event, or reset replay to full range ---
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
        } else {
            // Click on empty space: reset replay to full range
            if (callbacks.onReplayRange) callbacks.onReplayRange(null);
            lastCanvasKey = null;
            renderExponentialTimeline();
        }
    });

    // --- Double-click: stop replay entirely ---
    canvas.addEventListener('dblclick', (e) => {
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        if (hitTest(x, y, canvas)) return; // don't interfere with event clicks

        state.replayRange = null;
        if (callbacks.onStopReplay) callbacks.onStopReplay();
        lastCanvasKey = null;
        renderExponentialTimeline();
    });

    // Escape stops replay, Space pauses/resumes
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && state.replayRange) {
            state.replayRange = null;
            if (callbacks.onStopReplay) callbacks.onStopReplay();
            lastCanvasKey = null;
            renderExponentialTimeline();
        }
        if (e.key === ' ' && state.replayRange) {
            e.preventDefault(); // don't scroll the page
            if (callbacks.onTogglePause) callbacks.onTogglePause();
            lastCanvasKey = null;
            renderExponentialTimeline();
        }
    });

    // Scroll wheel on timeline adjusts replay speed
    canvas.addEventListener('wheel', (e) => {
        if (!state.replayRange) return;
        e.preventDefault();
        const factor = e.deltaY < 0 ? 1.25 : 0.8; // scroll up = faster
        if (callbacks.onSpeedChange) callbacks.onSpeedChange(factor);
    }, { passive: false });

    // --- Contract search input ---
    const contractSearch = document.getElementById('contract-search');
    if (contractSearch) {
        contractSearch.addEventListener('input', (e) => {
            state.contractSearchText = e.target.value;
            if (callbacks.renderContractsList) callbacks.renderContractsList();
        });
    }
}
