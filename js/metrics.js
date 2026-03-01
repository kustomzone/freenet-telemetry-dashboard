/**
 * Performance Metrics Time Series Chart
 * Uses Chart.js to render operation success rates, peer count, and release markers.
 */

import { state } from './state.js';

let chart = null;
let chartCanvas = null;

// Colors matching the dashboard palette
const COLORS = {
    put: '#fbbf24',       // --color-put
    get: '#34d399',       // --color-get
    update: '#a78bfa',    // --color-update
    peers: '#7ecfef',     // --color-connect
    grid: 'rgba(48, 54, 61, 0.3)',
    text: '#8b949e',
    textMuted: '#484f58',
    versionLine: 'rgba(244, 114, 182, 0.35)',
    versionText: '#f472b6',
};

/**
 * Initialize the metrics chart in the given container element.
 */
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

    // Parse timestamps (nanoseconds -> Date)
    const labels = series.map(p => new Date(p.t / 1_000_000));

    // Build version annotation lines
    const versionAnnotations = {};
    versions.forEach(([tsNs, ver], i) => {
        versionAnnotations['version' + i] = {
            type: 'line',
            xMin: new Date(tsNs / 1_000_000),
            xMax: new Date(tsNs / 1_000_000),
            borderColor: COLORS.versionLine,
            borderWidth: 1,
            borderDash: [4, 4],
            label: {
                display: true,
                content: ver,
                position: 'start',
                backgroundColor: 'rgba(13, 17, 23, 0.85)',
                color: COLORS.versionText,
                font: { size: 9, family: "'JetBrains Mono', monospace" },
                padding: { top: 2, bottom: 2, left: 3, right: 3 },
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
                    data: series.map(p => p.get_rate),
                    borderColor: COLORS.get,
                    backgroundColor: COLORS.get + '20',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.4,
                    fill: true,
                    yAxisID: 'y',
                    spanGaps: false,  // show gaps where data is missing
                    order: 2,
                },
                {
                    label: 'PUT',
                    data: series.map(p => p.put_rate),
                    borderColor: COLORS.put,
                    backgroundColor: COLORS.put + '15',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.4,
                    fill: true,
                    yAxisID: 'y',
                    spanGaps: false,
                    order: 3,
                },
                {
                    label: 'UPDATE',
                    data: series.map(p => p.upd_rate),
                    borderColor: COLORS.update,
                    backgroundColor: COLORS.update + '15',
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    pointHitRadius: 12,
                    tension: 0.4,
                    fill: true,
                    yAxisID: 'y',
                    spanGaps: false,
                    order: 4,
                },
                {
                    label: 'Peers',
                    data: series.map(p => p.peers || null),
                    borderColor: COLORS.peers + '60',
                    backgroundColor: COLORS.peers + '08',
                    borderWidth: 1.5,
                    borderDash: [6, 3],
                    pointRadius: 0,
                    pointHoverRadius: 3,
                    pointHitRadius: 12,
                    tension: 0.4,
                    fill: true,
                    yAxisID: 'y2',
                    spanGaps: true,
                    order: 1,  // draw behind success rates
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
                        usePointStyle: false,
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
                            const val = item.raw;
                            let count = 0;
                            const lbl = item.dataset.label;
                            if (lbl === 'GET') count = s.get_n;
                            else if (lbl === 'PUT') count = s.put_n;
                            else if (lbl === 'UPDATE') count = s.upd_n;
                            if (val == null) return ` ${lbl}: insufficient data`;
                            return ` ${lbl}: ${val}% (${count} ops)`;
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
                        displayFormats: {
                            hour: 'HH:mm',
                            day: 'MMM d',
                        }
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
}

/**
 * Update chart with new data (called when state.metricsTimeseries changes).
 */
export function updateMetricsChart() {
    if (!chart || !state.metricsTimeseries) return;

    const data = state.metricsTimeseries;
    if (!data.series || data.series.length === 0) return;

    const series = data.series;
    const labels = series.map(p => new Date(p.t / 1_000_000));

    chart.data.labels = labels;
    chart.data.datasets[0].data = series.map(p => p.get_rate);
    chart.data.datasets[1].data = series.map(p => p.put_rate);
    chart.data.datasets[2].data = series.map(p => p.upd_rate);
    chart.data.datasets[3].data = series.map(p => p.peers || null);

    chart.update('none');
}

/**
 * Destroy the chart (when switching tabs).
 */
export function destroyMetricsChart() {
    if (chart) {
        chart.destroy();
        chart = null;
    }
}
