/**
 * Version Rollout Chart
 * Shows peer count per version over time as a line chart.
 */

import { state } from './state.js';

let chart = null;
let chartCanvas = null;

// Distinct colors for version lines (top versions get most distinct colors)
const VERSION_COLORS = [
    '#34d399', // green
    '#fbbf24', // amber
    '#a78bfa', // purple
    '#f472b6', // pink
    '#7ecfef', // cyan
    '#fb923c', // orange
    '#60a5fa', // blue
    '#f87171', // red
    '#a3e635', // lime
    '#e879f9', // fuchsia
];

const CHART_COLORS = {
    grid: 'rgba(48, 54, 61, 0.3)',
    text: '#8b949e',
    textMuted: '#484f58',
    other: '#484f58',
};

// Max versions to show as individual lines (rest grouped as "other")
const MAX_VERSIONS = 8;

/**
 * Filter versions: keep top N by peak peer count, group rest as "other".
 */
function filterVersions(series, versions) {
    if (versions.length <= MAX_VERSIONS) {
        return { versions, hasOther: false };
    }

    // Find peak count for each version across all buckets
    const peaks = {};
    for (const v of versions) {
        peaks[v] = 0;
        for (const bucket of series) {
            peaks[v] = Math.max(peaks[v], bucket[v] || 0);
        }
    }

    // Sort by peak count descending, keep top N
    const sorted = [...versions].sort((a, b) => peaks[b] - peaks[a]);
    const kept = sorted.slice(0, MAX_VERSIONS);
    const grouped = new Set(sorted.slice(MAX_VERSIONS));

    return { versions: kept, grouped, hasOther: grouped.size > 0 };
}

function buildDatasets(series, versions) {
    const { versions: kept, grouped, hasOther } = filterVersions(series, versions);

    const datasets = kept.map((version, i) => ({
        label: version,
        data: series.map(p => p[version] || 0),
        borderColor: VERSION_COLORS[i % VERSION_COLORS.length],
        backgroundColor: VERSION_COLORS[i % VERSION_COLORS.length] + '18',
        borderWidth: 2,
        pointRadius: 0,
        pointHoverRadius: 4,
        pointHitRadius: 12,
        tension: 0.35,
        fill: false,
        spanGaps: true,
    }));

    if (hasOther) {
        datasets.push({
            label: `other (${grouped.size})`,
            data: series.map(p => {
                let sum = 0;
                for (const v of grouped) sum += (p[v] || 0);
                return sum;
            }),
            borderColor: CHART_COLORS.other,
            backgroundColor: CHART_COLORS.other + '18',
            borderWidth: 1.5,
            borderDash: [4, 3],
            pointRadius: 0,
            pointHoverRadius: 3,
            pointHitRadius: 10,
            tension: 0.35,
            fill: false,
            spanGaps: true,
        });
    }

    return datasets;
}

export function initVersionsChart(container) {
    if (chart) {
        chart.destroy();
        chart = null;
    }

    container.innerHTML = '';
    chartCanvas = document.createElement('canvas');
    chartCanvas.id = 'versions-canvas';
    container.appendChild(chartCanvas);

    const data = state.versionRollout;
    if (!data || !data.series || data.series.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">&#128230;</div><div>No version data available yet</div><div style="color:var(--text-muted);font-size:0.85em;margin-top:4px">Version data appears as peers connect</div></div>';
        return;
    }

    const series = data.series;
    const versions = data.versions || [];

    if (versions.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">&#128230;</div><div>No version data available yet</div></div>';
        return;
    }

    const labels = series.map(p => new Date(p.t / 1_000_000));
    const datasets = buildDatasets(series, versions);

    chart = new Chart(chartCanvas, {
        type: 'line',
        data: { labels, datasets },
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
                        color: CHART_COLORS.text,
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                        boxWidth: 16,
                        boxHeight: 2,
                        padding: 12,
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
                    // Only show versions with non-zero counts in tooltip
                    filter: function(item) {
                        return item.parsed.y > 0;
                    },
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
                            const count = item.parsed.y;
                            return ` ${item.dataset.label}: ${count} peer${count !== 1 ? 's' : ''}`;
                        }
                    }
                },
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        tooltipFormat: 'MMM d, HH:mm',
                        displayFormats: { hour: 'HH:mm', day: 'MMM d' }
                    },
                    grid: { color: CHART_COLORS.grid, drawBorder: false },
                    ticks: {
                        color: CHART_COLORS.textMuted,
                        font: { size: 10, family: "'JetBrains Mono', monospace" },
                        maxRotation: 0,
                        maxTicksLimit: 12,
                    },
                    border: { display: false },
                },
                y: {
                    position: 'left',
                    min: 0,
                    title: {
                        display: true,
                        text: 'peers',
                        color: CHART_COLORS.textMuted,
                        font: { size: 9, family: "'JetBrains Mono', monospace" },
                    },
                    grid: { color: CHART_COLORS.grid, drawBorder: false },
                    ticks: {
                        color: CHART_COLORS.textMuted,
                        font: { size: 9, family: "'JetBrains Mono', monospace" },
                        precision: 0,
                    },
                    border: { display: false },
                },
            }
        },
    });
}

export function updateVersionsChart() {
    if (!chart || !state.versionRollout) return;

    const data = state.versionRollout;
    if (!data.series || data.series.length === 0) return;

    const series = data.series;
    const versions = data.versions || [];
    const labels = series.map(p => new Date(p.t / 1_000_000));

    chart.data.labels = labels;
    chart.data.datasets = buildDatasets(series, versions);
    chart.update('none');
}

export function destroyVersionsChart() {
    if (chart) {
        chart.destroy();
        chart = null;
    }
}
