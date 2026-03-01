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
    grid: 'rgba(48, 54, 61, 0.4)',
    text: '#8b949e',
    versionLine: 'rgba(244, 114, 182, 0.6)',  // --color-subscribe
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
            borderWidth: 1.5,
            borderDash: [6, 4],
            label: {
                display: true,
                content: 'v' + ver,
                position: 'start',
                backgroundColor: 'rgba(13, 17, 23, 0.9)',
                color: COLORS.versionText,
                font: { size: 10, family: "'JetBrains Mono', monospace" },
                padding: { top: 2, bottom: 2, left: 4, right: 4 },
            }
        };
    });

    chart = new Chart(chartCanvas, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'GET success %',
                    data: series.map(p => p.get_rate),
                    borderColor: COLORS.get,
                    backgroundColor: COLORS.get + '18',
                    borderWidth: 1.5,
                    pointRadius: 0,
                    pointHitRadius: 8,
                    tension: 0.3,
                    fill: false,
                    yAxisID: 'y',
                    spanGaps: true,
                },
                {
                    label: 'PUT success %',
                    data: series.map(p => p.put_rate),
                    borderColor: COLORS.put,
                    backgroundColor: COLORS.put + '18',
                    borderWidth: 1.5,
                    pointRadius: 0,
                    pointHitRadius: 8,
                    tension: 0.3,
                    fill: false,
                    yAxisID: 'y',
                    spanGaps: true,
                },
                {
                    label: 'UPDATE success %',
                    data: series.map(p => p.upd_rate),
                    borderColor: COLORS.update,
                    backgroundColor: COLORS.update + '18',
                    borderWidth: 1.5,
                    pointRadius: 0,
                    pointHitRadius: 8,
                    tension: 0.3,
                    fill: false,
                    yAxisID: 'y',
                    spanGaps: true,
                },
                {
                    label: 'Peers',
                    data: series.map(p => p.peers || null),
                    borderColor: COLORS.peers,
                    backgroundColor: COLORS.peers + '10',
                    borderWidth: 1,
                    borderDash: [4, 3],
                    pointRadius: 0,
                    pointHitRadius: 8,
                    tension: 0.3,
                    fill: true,
                    yAxisID: 'y2',
                    spanGaps: true,
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
                        font: { size: 11, family: "'JetBrains Mono', monospace" },
                        boxWidth: 12,
                        boxHeight: 2,
                        padding: 12,
                        usePointStyle: false,
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(13, 17, 23, 0.95)',
                    borderColor: 'rgba(48, 54, 61, 0.6)',
                    borderWidth: 1,
                    titleFont: { family: "'JetBrains Mono', monospace", size: 11 },
                    bodyFont: { family: "'JetBrains Mono', monospace", size: 11 },
                    titleColor: '#e6edf3',
                    bodyColor: '#8b949e',
                    padding: 10,
                    callbacks: {
                        title: function(items) {
                            if (!items.length) return '';
                            const d = items[0].parsed.x;
                            const date = new Date(d);
                            return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                        },
                        label: function(item) {
                            const idx = item.dataIndex;
                            const s = series[idx];
                            if (!s) return item.dataset.label + ': -';
                            if (item.dataset.yAxisID === 'y2') {
                                return `  ${item.dataset.label}: ${item.formattedValue}`;
                            }
                            // Show success rate + count
                            const val = item.raw;
                            let count = 0;
                            if (item.dataset.label.startsWith('GET')) count = s.get_n;
                            else if (item.dataset.label.startsWith('PUT')) count = s.put_n;
                            else if (item.dataset.label.startsWith('UPDATE')) count = s.upd_n;
                            return val != null
                                ? `  ${item.dataset.label}: ${val}% (n=${count})`
                                : `  ${item.dataset.label}: -`;
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
                        tooltipFormat: 'HH:mm',
                        displayFormats: {
                            minute: 'HH:mm',
                            hour: 'HH:mm',
                        }
                    },
                    grid: { color: COLORS.grid, drawBorder: false },
                    ticks: {
                        color: COLORS.text,
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                        maxRotation: 0,
                    },
                    border: { display: false },
                },
                y: {
                    position: 'left',
                    min: 0,
                    max: 100,
                    title: {
                        display: true,
                        text: 'Success %',
                        color: COLORS.text,
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                    },
                    grid: { color: COLORS.grid, drawBorder: false },
                    ticks: {
                        color: COLORS.text,
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                        callback: v => v + '%',
                    },
                    border: { display: false },
                },
                y2: {
                    position: 'right',
                    min: 0,
                    title: {
                        display: true,
                        text: 'Peers',
                        color: COLORS.peers,
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                    },
                    grid: { drawOnChartArea: false },
                    ticks: {
                        color: COLORS.peers,
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                    },
                    border: { display: false },
                }
            }
        },
        plugins: [
            // Custom plugin: draw version markers even without annotation plugin
        ]
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

    chart.update('none'); // no animation on data refresh
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
