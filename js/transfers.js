/**
 * Transfer Histogram Module
 * Shows distribution of transfer speeds
 */

// Transfer events storage
let transferEvents = [];

// Canvas and context
let canvas = null;
let ctx = null;
let initialized = false;

// Chart dimensions
let chartWidth = 0;
let chartHeight = 0;
const PADDING = { top: 2, right: 2, bottom: 2, left: 2 };

// Speed buckets (log scale)
const BUCKET_COUNT = 10;
const MIN_SPEED = 100 * 1024;         // 100 KB/s
const MAX_SPEED = 500 * 1024 * 1024;  // 500 MB/s

// Track zoom state
let isZoomed = false;

// Cached bucket data
let buckets = [];
let maxBucketCount = 0;

/**
 * Initialize the transfer chart
 */
export function initTransferChart() {
    if (initialized) return;

    canvas = document.getElementById('transfer-canvas');
    if (!canvas) {
        console.log('Transfer chart: canvas not found');
        return;
    }

    const container = document.getElementById('transfer-chart');
    if (!container) {
        console.log('Transfer chart: container not found');
        return;
    }

    ctx = canvas.getContext('2d');
    initialized = true;
    console.log('Transfer histogram: initialized');

    requestAnimationFrame(() => {
        setupCanvasSize();
        renderTransferChart();
    });

    // Set up tooltip on hover
    canvas.addEventListener('mousemove', handleMouseMove);
    canvas.addEventListener('mouseleave', hideTooltip);

    // Set up zoom toggle on click
    const overlayContainer = document.getElementById('transfer-chart-container');
    if (overlayContainer) {
        overlayContainer.addEventListener('click', toggleZoom);
    }

    setupBackdropClose();
}

/**
 * Toggle zoom state
 */
function toggleZoom(e) {
    if (e.target.closest('.transfer-tooltip')) return;

    const overlayContainer = document.getElementById('transfer-chart-container');
    const backdrop = document.getElementById('transfer-backdrop');
    const zoomHint = document.getElementById('zoom-hint');
    if (!overlayContainer) return;

    isZoomed = !isZoomed;
    overlayContainer.classList.toggle('zoomed', isZoomed);
    overlayContainer.title = isZoomed ? 'Click to close' : 'Click to zoom';
    if (backdrop) backdrop.classList.toggle('visible', isZoomed);
    if (zoomHint) zoomHint.textContent = isZoomed ? 'click to close' : 'click to zoom';

    setupCanvasSize();
    renderTransferChart();
}

/**
 * Close zoom when clicking backdrop
 */
function setupBackdropClose() {
    const backdrop = document.getElementById('transfer-backdrop');
    if (backdrop) {
        backdrop.addEventListener('click', () => {
            if (isZoomed) {
                const overlayContainer = document.getElementById('transfer-chart-container');
                const zoomHint = document.getElementById('zoom-hint');
                isZoomed = false;
                if (overlayContainer) {
                    overlayContainer.classList.remove('zoomed');
                    overlayContainer.title = 'Click to zoom';
                }
                backdrop.classList.remove('visible');
                if (zoomHint) zoomHint.textContent = 'click to zoom';
                setupCanvasSize();
                renderTransferChart();
            }
        });
    }
}

/**
 * Set up canvas size from container
 */
function setupCanvasSize() {
    const container = document.getElementById('transfer-chart');
    if (!container || !canvas) return;

    const width = container.offsetWidth || 100;
    const height = container.offsetHeight || 40;
    const dpr = window.devicePixelRatio || 1;

    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    chartWidth = width - PADDING.left - PADDING.right;
    chartHeight = height - PADDING.top - PADDING.bottom;
}

/**
 * Get bucket index for a speed value (log scale)
 */
function getBucketIndex(speed) {
    if (speed <= MIN_SPEED) return 0;
    if (speed >= MAX_SPEED) return BUCKET_COUNT - 1;

    const logMin = Math.log10(MIN_SPEED);
    const logMax = Math.log10(MAX_SPEED);
    const logSpeed = Math.log10(speed);

    const ratio = (logSpeed - logMin) / (logMax - logMin);
    return Math.floor(ratio * BUCKET_COUNT);
}

/**
 * Get speed range for a bucket
 */
function getBucketSpeedRange(index) {
    const logMin = Math.log10(MIN_SPEED);
    const logMax = Math.log10(MAX_SPEED);
    const logRange = logMax - logMin;

    const lowLog = logMin + (index / BUCKET_COUNT) * logRange;
    const highLog = logMin + ((index + 1) / BUCKET_COUNT) * logRange;

    return {
        low: Math.pow(10, lowLog),
        high: Math.pow(10, highLog)
    };
}

/**
 * Calculate bucket counts from transfer events
 */
function calculateBuckets() {
    buckets = new Array(BUCKET_COUNT).fill(0);
    maxBucketCount = 0;

    for (const transfer of transferEvents) {
        const speed = transfer.throughput_bps || 0;
        if (speed > 0) {
            const index = getBucketIndex(speed);
            buckets[index]++;
            maxBucketCount = Math.max(maxBucketCount, buckets[index]);
        }
    }
}

/**
 * Format speed for display
 */
function formatSpeed(bytesPerSec) {
    if (bytesPerSec >= 1024 * 1024 * 1024) return (bytesPerSec / (1024 * 1024 * 1024)).toFixed(0) + 'G/s';
    if (bytesPerSec >= 1024 * 1024) return (bytesPerSec / (1024 * 1024)).toFixed(0) + 'M/s';
    if (bytesPerSec >= 1024) return (bytesPerSec / 1024).toFixed(0) + 'K/s';
    return bytesPerSec + 'B/s';
}

/**
 * Render the histogram
 */
export function renderTransferChart() {
    if (!ctx || !canvas || chartWidth <= 0 || chartHeight <= 0) return;

    calculateBuckets();

    const width = canvas.width / (window.devicePixelRatio || 1);
    const height = canvas.height / (window.devicePixelRatio || 1);

    // Clear canvas
    ctx.clearRect(0, 0, width, height);

    if (maxBucketCount === 0) return;

    // Draw bars with adaptive widths based on density
    const baseBarWidth = chartWidth / BUCKET_COUNT;
    const minBarWidth = isZoomed ? 4 : 2;
    const maxBarWidthRatio = 0.8;

    for (let i = 0; i < BUCKET_COUNT; i++) {
        const count = buckets[i];
        if (count === 0) continue;

        // Height based on count
        const barHeight = (count / maxBucketCount) * chartHeight;

        // Width proportional to density
        const densityRatio = count / maxBucketCount;
        const barActualWidth = minBarWidth + densityRatio * (baseBarWidth * maxBarWidthRatio - minBarWidth);
        const barOffset = (baseBarWidth - barActualWidth) / 2;

        const x = PADDING.left + i * baseBarWidth + barOffset;
        const y = PADDING.top + chartHeight - barHeight;

        // Gradient - brighter for denser buckets
        const hue = 200 + (i / BUCKET_COUNT) * 15;
        const saturation = 40 + densityRatio * 30;
        const lightness = 35 + (i / BUCKET_COUNT) * 10 + densityRatio * 10;

        const gradient = ctx.createLinearGradient(x, y, x, y + barHeight);
        gradient.addColorStop(0, `hsl(${hue}, ${saturation}%, ${lightness + 15}%)`);
        gradient.addColorStop(1, `hsl(${hue}, ${saturation}%, ${lightness - 5}%)`);
        ctx.fillStyle = gradient;

        ctx.fillRect(x, y, barActualWidth, barHeight);
    }

    // Draw axis labels when zoomed
    if (isZoomed) {
        ctx.fillStyle = 'rgba(255, 255, 255, 0.5)';
        ctx.font = '10px monospace';
        ctx.textAlign = 'center';

        // Draw a few speed labels
        const labelIndices = [0, Math.floor(BUCKET_COUNT / 2), BUCKET_COUNT - 1];
        for (const i of labelIndices) {
            const range = getBucketSpeedRange(i);
            const x = PADDING.left + (i + 0.5) * barWidth;
            ctx.fillText(formatSpeed(range.low), x, height - 2);
        }
    }
}

/**
 * Handle mouse move for tooltip
 */
function handleMouseMove(e) {
    if (!canvas || !isZoomed) {
        hideTooltip();
        return;
    }

    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const barWidth = chartWidth / BUCKET_COUNT;
    const bucketIndex = Math.floor((x - PADDING.left) / barWidth);

    if (bucketIndex >= 0 && bucketIndex < BUCKET_COUNT && buckets[bucketIndex] > 0) {
        const range = getBucketSpeedRange(bucketIndex);
        showTooltip(e, bucketIndex, range, buckets[bucketIndex]);
    } else {
        hideTooltip();
    }
}

/**
 * Show tooltip for a bucket
 */
function showTooltip(e, index, range, count) {
    const tooltip = document.getElementById('transfer-tooltip');
    if (!tooltip) return;

    const html = `<div><strong>${count}</strong> transfers</div>
        <div>${formatSpeed(range.low)} - ${formatSpeed(range.high)}</div>`;

    tooltip.innerHTML = html;
    tooltip.style.display = 'block';

    // Position tooltip above cursor (fixed positioning uses clientX/Y)
    const tooltipWidth = 150;
    const tooltipHeight = 60;

    let left = e.clientX + 10;
    let top = e.clientY - tooltipHeight - 10;

    // Keep on screen
    if (left + tooltipWidth > window.innerWidth) {
        left = e.clientX - tooltipWidth - 10;
    }
    if (top < 10) {
        top = e.clientY + 20;
    }

    tooltip.style.left = left + 'px';
    tooltip.style.top = top + 'px';
}

/**
 * Hide tooltip
 */
function hideTooltip() {
    const tooltip = document.getElementById('transfer-tooltip');
    if (tooltip) tooltip.style.display = 'none';
}

/**
 * Add transfer events from server
 */
export function addTransferEvents(events) {
    if (!Array.isArray(events)) return;

    transferEvents = transferEvents.concat(events);

    if (transferEvents.length > 500) {
        transferEvents = transferEvents.slice(-500);
    }

    renderTransferChart();
}

/**
 * Add a single transfer event
 */
export function addTransferEvent(event) {
    transferEvents.push(event);

    if (transferEvents.length > 500) {
        transferEvents.shift();
    }

    renderTransferChart();
}

/**
 * Get current transfer events
 */
export function getTransferEvents() {
    return transferEvents;
}
