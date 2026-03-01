/**
 * Performance Metrics Time Series Chart
 * Uses Chart.js with EMA smoothing for clean trend visualization.
 */

import { state } from './state.js';

let chart = null;
let chartCanvas = null;

const COLORS = {
    put: '#fbbf24',
    get: '#34d399',
    update: '#a78bfa',
    peers: '#7ecfef',
    grid: 'rgba(48, 54, 61, 0.3)',
    text: '#8b949e',
    textMuted: '#484f58',
    versionLine: 'rgba(244, 114, 182, 0.25)',
    versionText: '#f472b6',
};

/**
 * Exponential moving average.
 * Handles nulls (gaps) gracefully — resets the EMA after a gap.
 * @param {Array<number|null>} data - raw values
 * @param {number} alpha - smoothing factor (0..1). Higher = less smoothing.
 * @returns {Array<number|null>} smoothed values
 */
function ema(data, alpha = 0.35) {
    const result = new Array(data.length);
    let prev = null;
    for (let i = 0; i < data.length; i++) {
        const v = data[i];
        if (v == null) {
            result[i] = null;
            prev = null;  // reset after gap
        } else if (prev == null) {
            result[i] = v;  // first value after gap: use raw
            prev = v;
        } else {
            prev = alpha * v + (1 - alpha) * prev;
            result[i] = Math.round(prev * 10) / 10;
        }
    }
    return result;
}

export function initMetricsChart(container) {
    if (chart) {
        chart.destroy();
        chart = null;
    }

    container.innerHTML = '';
    chartCanvas = document.createElement('canvas');
    chartCanvas.id = 'metrics-canvas';
    container.appendChild(chartCanvas);

    const data = state.metricsTimeseries;
    if (!data || !data.series || data.series.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">&#128200;</div><div>Collecting performance data...</div><div style="color:var(--text-muted);font-size:0.85em;margin-top:4px">Metrics appear after a few minutes of activity</div></div>';
        return;
    }

    const series = data.series;
    const versions = data.versions || [];

    const labels = series.map(p => new Date(p.t / 1_000_000));

    // Raw rates for tooltips
    const rawGet = series.map(p => p.get_rate);
    const rawPut = series.map(p => p.put_rate);
    const rawUpd = series.map(p => p.upd_rate);

    // EMA-smoothed rates for display
    const smoothGet = ema(rawGet);
    const smoothPut = ema(rawPut);
    const smoothUpd = ema(rawUpd);

    // Version annotations — positioned at the top of the chart
    const versionAnnotations = {};
    versions.forEach(([tsNs, ver], i) => {
        versionAnnotations['version' + i] = {
            type: 'line',
            xMin: new Date(tsNs / 1_000_000),
            xMax: new Date(tsNs / 1_000_000),
            borderColor: COLORS.versionLine,
            borderWidth: 1,
            borderDash: [3, 3],
            label: {
                display: true,
                content: ver,
                position: 'end',          // top of the chart
                yAdjust: -2,
                backgroundColor: 'rgba(6, 8, 12, 0.8)',
                color: COLORS.versionText,
                font: { size: 8, family: "'JetBrains Mono', monospace" },
                padding: { top: 1, bottom: 1, left: 3, right: 3 },
            }
        };
    });

    chart = new Chart(chartCanvas, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'GET',
                    data: smoothGet,
                    borderColor: COLORS.get,
                    backgroundColor: COLORS.get + '18',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.35,
                    fill: true,
                    yAxisID: 'y',
                    spanGaps: false,
                    order: 2,
                },
                {
                    label: 'PUT',
                    data: smoothPut,
                    borderColor: COLORS.put,
                    backgroundColor: COLORS.put + '12',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.35,
                    fill: true,
                    yAxisID: 'y',
                    spanGaps: false,
                    order: 3,
                },
                {
                    label: 'UPDATE',
                    data: smoothUpd,
                    borderColor: COLORS.update,
                    backgroundColor: COLORS.update + '12',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.35,
                    fill: true,
                    yAxisID: 'y',
                    spanGaps: false,
                    order: 4,
                },
                {
                    label: 'Peers',
                    data: series.map(p => p.peers || null),
                    borderColor: COLORS.peers + '70',
                    borderWidth: 1.5,
                    borderDash: [6, 3],
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    pointHitRadius: 12,
                    tension: 0.4,
                    fill: false,
                    yAxisID: 'y2',
                    spanGaps: true,
                    order: 1,
                },
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    align: 'start',
                    labels: {
                        color: COLORS.text,
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                        boxWidth: 16,
                        boxHeight: 2,
                        padding: 14,
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(13, 17, 23, 0.95)',
                    borderColor: 'rgba(48, 54, 61, 0.6)',
                    borderWidth: 1,
                    titleFont: { family: "'JetBrains Mono', monospace", size: 11 },
                    bodyFont: { family: "'JetBrains Mono', monospace", size: 10 },
                    titleColor: '#e6edf3',
                    bodyColor: '#8b949e',
                    padding: 10,
                    displayColors: true,
                    boxWidth: 8,
                    boxHeight: 8,
                    callbacks: {
                        title: function(items) {
                            if (!items.length) return '';
                            const date = new Date(items[0].parsed.x);
                            return date.toLocaleString([], {
                                month: 'short', day: 'numeric',
                                hour: '2-digit', minute: '2-digit'
                            });
                        },
                        label: function(item) {
                            const idx = item.dataIndex;
                            const s = series[idx];
                            if (!s) return '';
                            if (item.dataset.yAxisID === 'y2') {
                                return ` Peers: ${item.raw != null ? item.raw : '-'}`;
                            }
                            // Show raw (unsmoothed) value + sample count
                            const lbl = item.dataset.label;
                            let raw, count;
                            if (lbl === 'GET') { raw = rawGet[idx]; count = s.get_n; }
                            else if (lbl === 'PUT') { raw = rawPut[idx]; count = s.put_n; }
                            else if (lbl === 'UPDATE') { raw = rawUpd[idx]; count = s.upd_n; }
                            if (raw == null) return ` ${lbl}: insufficient data`;
                            return ` ${lbl}: ${raw}% (${count} ops)`;
                        }
                    }
                },
                annotation: {
                    annotations: versionAnnotations
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        tooltipFormat: 'MMM d, HH:mm',
                        displayFormats: { hour: 'HH:mm', day: 'MMM d' }
                    },
                    grid: { color: COLORS.grid, drawBorder: false },
                    ticks: {
                        color: COLORS.textMuted,
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                        maxRotation: 0,
                        maxTicksLimit: 12,
                    },
                    border: { display: false },
                },
                y: {
                    position: 'left',
                    min: 0,
                    max: 100,
                    title: {
                        display: true,
                        text: 'success %',
                        color: COLORS.textMuted,
                        font: { size: 9, family: "'JetBrains Mono', monospace" },
                    },
                    grid: { color: COLORS.grid, drawBorder: false },
                    ticks: {
                        color: COLORS.textMuted,
                        font: { size: 9, family: "'JetBrains Mono', monospace" },
                        callback: v => v + '%',
                        stepSize: 25,
                    },
                    border: { display: false },
                },
                y2: {
                    position: 'right',
                    min: 0,
                    title: {
                        display: true,
                        text: 'peers',
                        color: COLORS.peers + '80',
                        font: { size: 9, family: "'JetBrains Mono', monospace" },
                    },
                    grid: { drawOnChartArea: false },
                    ticks: {
                        color: COLORS.peers + '80',
                        font: { size: 9, family: "'JetBrains Mono', monospace" },
                    },
                    border: { display: false },
                }
            }
        },
    });

    // Store raw data on chart for updates
    chart._rawGet = rawGet;
    chart._rawPut = rawPut;
    chart._rawUpd = rawUpd;
    chart._series = series;
}

export function updateMetricsChart() {
    if (!chart || !state.metricsTimeseries) return;

    const data = state.metricsTimeseries;
    if (!data.series || data.series.length === 0) return;

    const series = data.series;
    const labels = series.map(p => new Date(p.t / 1_000_000));

    const rawGet = series.map(p => p.get_rate);
    const rawPut = series.map(p => p.put_rate);
    const rawUpd = series.map(p => p.upd_rate);

    chart.data.labels = labels;
    chart.data.datasets[0].data = ema(rawGet);
    chart.data.datasets[1].data = ema(rawPut);
    chart.data.datasets[2].data = ema(rawUpd);
    chart.data.datasets[3].data = series.map(p => p.peers || null);

    chart._rawGet = rawGet;
    chart._rawPut = rawPut;
    chart._rawUpd = rawUpd;
    chart._series = series;

    chart.update('none');
}

export function destroyMetricsChart() {
    if (chart) {
        chart.destroy();
        chart = null;
    }
}
