/**
 * Topology visualization for Freenet Dashboard
 * Handles ring rendering and peer visualization
 */

import { state, SVG_SIZE, SVG_WIDTH, CENTER, RADIUS } from './state.js';
import { hashToColor, contractKeyToLocation, getEventClass } from './utils.js';

// Convert ring location (0-1) to SVG coordinates
export function locationToXY(location) {
    const angle = location * 2 * Math.PI - Math.PI / 2;
    return { x: CENTER + RADIUS * Math.cos(angle), y: CENTER + RADIUS * Math.sin(angle) };
}

// Calculate contract activity stats from events
export function getContractActivity(contractKey) {
    const contractEvents = state.allEvents.filter(e =>
        e.contract_full === contractKey &&
        (e.event_type === 'update_success' || e.event_type === 'update_broadcast_received' ||
         e.event_type === 'put_success' || e.event_type === 'put_broadcast_received')
    );

    if (contractEvents.length === 0) {
        return { lastUpdate: null, totalUpdates: 0, recentUpdates: 0 };
    }

    contractEvents.sort((a, b) => b.timestamp - a.timestamp);
    const lastUpdate = contractEvents[0].timestamp;
    const totalUpdates = contractEvents.length;

    const oneHourAgo = Date.now() * 1_000_000 - (60 * 60 * 1000 * 1_000_000);
    const recentUpdates = contractEvents.filter(e => e.timestamp > oneHourAgo).length;

    return { lastUpdate, totalUpdates, recentUpdates };
}

// Compute proximity links: connected peers that both have the contract
export function computeProximityLinks(contractKey, peers, connections) {
    if (!contractKey || !state.contractData[contractKey]) return [];

    const subData = state.contractData[contractKey];
    const peerStates = subData.peer_states || [];

    const peersWithContract = new Set();
    peerStates.forEach(ps => {
        if (ps.peer_id) peersWithContract.add(ps.peer_id);
    });

    const subscriptionEdges = new Set();
    const tree = subData.tree || {};
    Object.entries(tree).forEach(([fromId, toIds]) => {
        toIds.forEach(toId => {
            subscriptionEdges.add(`${fromId}|${toId}`);
            subscriptionEdges.add(`${toId}|${fromId}`);
        });
    });

    const proximityLinks = [];
    connections.forEach(connKey => {
        const [id1, id2] = connKey.split('|');
        const peer1 = peers.get(id1);
        const peer2 = peers.get(id2);
        if (!peer1 || !peer2) return;

        const p1HasContract = peer1.peer_id && peersWithContract.has(peer1.peer_id);
        const p2HasContract = peer2.peer_id && peersWithContract.has(peer2.peer_id);

        if (p1HasContract && p2HasContract) {
            const edgeKey1 = `${peer1.peer_id}|${peer2.peer_id}`;
            const edgeKey2 = `${peer2.peer_id}|${peer1.peer_id}`;

            if (!subscriptionEdges.has(edgeKey1) && !subscriptionEdges.has(edgeKey2)) {
                proximityLinks.push({ from: id1, to: id2, fromPeerId: peer1.peer_id, toPeerId: peer2.peer_id });
            }
        }
    });

    return proximityLinks;
}

// Check if subscription tree is connected using BFS
export function checkTreeConnectivity(contractKey, peers) {
    if (!contractKey || !state.contractData[contractKey]) {
        return { connected: true, segments: 1, nodes: 0 };
    }

    const subData = state.contractData[contractKey];
    const peerStates = subData.peer_states || [];
    const tree = subData.tree || {};

    if (peerStates.length === 0) {
        return { connected: true, segments: 0, nodes: 0 };
    }

    const adjacency = new Map();
    const allNodes = new Set();

    peerStates.forEach(ps => {
        if (ps.peer_id) {
            allNodes.add(ps.peer_id);
            if (!adjacency.has(ps.peer_id)) adjacency.set(ps.peer_id, new Set());
        }
    });

    Object.entries(tree).forEach(([fromId, toIds]) => {
        if (!adjacency.has(fromId)) adjacency.set(fromId, new Set());
        toIds.forEach(toId => {
            if (!adjacency.has(toId)) adjacency.set(toId, new Set());
            adjacency.get(fromId).add(toId);
            adjacency.get(toId).add(fromId);
            allNodes.add(fromId);
            allNodes.add(toId);
        });
    });

    if (allNodes.size === 0) {
        return { connected: true, segments: 0, nodes: 0 };
    }

    const visited = new Set();
    let segments = 0;
    const segmentSizes = [];

    for (const startNode of allNodes) {
        if (visited.has(startNode)) continue;

        segments++;
        let segmentSize = 0;
        const queue = [startNode];

        while (queue.length > 0) {
            const node = queue.shift();
            if (visited.has(node)) continue;
            visited.add(node);
            segmentSize++;

            const neighbors = adjacency.get(node) || new Set();
            for (const neighbor of neighbors) {
                if (!visited.has(neighbor)) {
                    queue.push(neighbor);
                }
            }
        }
        segmentSizes.push(segmentSize);
    }

    return {
        connected: segments <= 1,
        segments: segments,
        nodes: allNodes.size,
        segmentSizes: segmentSizes
    };
}

// Get comprehensive subscription tree info
export function getSubscriptionTreeInfo(contractKey, peers, connections) {
    const connectivity = checkTreeConnectivity(contractKey, peers);
    const proximityLinks = computeProximityLinks(contractKey, peers, connections);

    let bridged = false;
    if (!connectivity.connected && proximityLinks.length > 0) {
        bridged = true;
    }

    return {
        ...connectivity,
        proximityLinks: proximityLinks,
        bridgedByProximity: bridged
    };
}

// Main ring SVG rendering function
export function updateRingSVG(peers, connections, subscriberPeerIds = new Set(), callbacks = {}) {
    const { selectPeer, showPeerNamingPrompt } = callbacks;
    const container = document.getElementById('ring-container');
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', `0 0 ${SVG_WIDTH} ${SVG_SIZE}`);
    svg.setAttribute('width', SVG_WIDTH);
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
    [0.6, 0.3].forEach((scale) => {
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

    // Draw contract location indicator when selected
    if (state.selectedContract) {
        const contractLocation = contractKeyToLocation(state.selectedContract);
        if (contractLocation !== null) {
            const angle = contractLocation * 2 * Math.PI - Math.PI / 2;
            const ringX = CENTER + RADIUS * Math.cos(angle);
            const ringY = CENTER + RADIUS * Math.sin(angle);
            const diamond = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
            const size = 7;
            diamond.setAttribute('points', `${ringX},${ringY-size} ${ringX+size},${ringY} ${ringX},${ringY+size} ${ringX-size},${ringY}`);
            diamond.setAttribute('fill', '#f472b6');
            diamond.setAttribute('stroke', '#fff');
            diamond.setAttribute('stroke-width', '1.5');
            diamond.setAttribute('style', 'cursor: pointer;');

            const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
            const shortKey = state.selectedContract.substring(0, 12) + '...';
            title.textContent = `Location of contract ${shortKey}\n@ ${contractLocation.toFixed(4)}`;
            diamond.appendChild(title);
            svg.appendChild(diamond);
        }
    }

    // Draw connections and collect distances for mini-chart
    const connectionDistances = [];
    connections.forEach(connKey => {
        const [id1, id2] = connKey.split('|');
        const peer1 = peers.get(id1);
        const peer2 = peers.get(id2);
        if (peer1 && peer2) {
            const pos1 = locationToXY(peer1.location);
            const pos2 = locationToXY(peer2.location);

            const rawDist = Math.abs(peer1.location - peer2.location);
            const distance = Math.min(rawDist, 1 - rawDist);
            connectionDistances.push(distance);

            const lineGroup = document.createElementNS("http://www.w3.org/2000/svg", "g");
            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            line.setAttribute('x1', pos1.x);
            line.setAttribute('y1', pos1.y);
            line.setAttribute('x2', pos2.x);
            line.setAttribute('y2', pos2.y);
            line.setAttribute('class', 'connection-line animated');

            const connTitle = document.createElementNS('http://www.w3.org/2000/svg', 'title');
            connTitle.textContent = 'Network connection: ' + id1.substring(0,8) + ' ↔ ' + id2.substring(0,8) + ' (dist: ' + distance.toFixed(3) + ')';
            line.appendChild(connTitle);

            const hitArea = document.createElementNS("http://www.w3.org/2000/svg", "line");
            hitArea.setAttribute("x1", pos1.x);
            hitArea.setAttribute("y1", pos1.y);
            hitArea.setAttribute("x2", pos2.x);
            hitArea.setAttribute("y2", pos2.y);
            hitArea.setAttribute("stroke", "transparent");
            hitArea.setAttribute("stroke-width", "12");
            hitArea.appendChild(connTitle.cloneNode(true));

            lineGroup.appendChild(line);
            lineGroup.appendChild(hitArea);
            svg.appendChild(lineGroup);
        }
    });

    // Draw distance distribution mini-chart
    if (connectionDistances.length > 0) {
        drawDistanceChart(svg, connectionDistances);
    }

    // Draw highlighted connection for hovered event
    if (state.hoveredEvent) {
        const fromPeer = state.hoveredEvent.from_peer || state.hoveredEvent.peer_id;
        const toPeer = state.hoveredEvent.to_peer;
        if (fromPeer && toPeer && fromPeer !== toPeer) {
            let fromPos = null, toPos = null;
            peers.forEach((peer, id) => {
                if (id === fromPeer || peer.peer_id === fromPeer) {
                    fromPos = locationToXY(peer.location);
                }
                if (id === toPeer || peer.peer_id === toPeer) {
                    toPos = locationToXY(peer.location);
                }
            });
            if (fromPos && toPos) {
                const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
                line.setAttribute('x1', fromPos.x);
                line.setAttribute('y1', fromPos.y);
                line.setAttribute('x2', toPos.x);
                line.setAttribute('y2', toPos.y);
                line.setAttribute('stroke', '#fbbf24');
                line.setAttribute('stroke-width', '3');
                line.setAttribute('stroke-opacity', '0.8');
                svg.appendChild(line);
            }
        }
    }

    // Draw peers
    drawPeers(svg, peers, subscriberPeerIds, callbacks);

    // Draw center stats
    drawCenterStats(svg, peers, subscriberPeerIds);

    // Draw message flow arrows
    if (!state.selectedContract && state.displayedEvents && state.displayedEvents.length > 0) {
        drawMessageFlowArrows(svg, peers);
    }

    // Draw subscription/proximity links when contract selected
    if (state.selectedContract && state.contractData[state.selectedContract]) {
        drawSubscriptionLinks(svg, peers, connections);
    }

    container.innerHTML = '';
    container.appendChild(svg);
}

// Helper: Draw distance distribution chart
function drawDistanceChart(svg, connectionDistances) {
    const chartW = 55;
    const chartH = 180;
    const chartX = SVG_SIZE + 15;
    const chartY = (SVG_SIZE - chartH) / 2;
    const plotW = 35;
    const plotX = chartX + 16;

    const chartBg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    chartBg.setAttribute('x', chartX);
    chartBg.setAttribute('y', chartY);
    chartBg.setAttribute('width', chartW);
    chartBg.setAttribute('height', chartH);
    chartBg.setAttribute('rx', '4');
    chartBg.setAttribute('fill', 'rgba(15, 20, 25, 0.85)');
    chartBg.setAttribute('stroke', 'rgba(255,255,255,0.1)');
    chartBg.setAttribute('stroke-width', '1');

    const chartTooltip = document.createElementNS('http://www.w3.org/2000/svg', 'title');
    chartTooltip.textContent = 'Connection distance distribution. Dots show actual peer connections. Curve shows expected 1/d small-world distribution.';
    chartBg.appendChild(chartTooltip);
    svg.appendChild(chartBg);

    const chartTitle = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    chartTitle.setAttribute('x', chartX + chartW / 2);
    chartTitle.setAttribute('y', chartY - 4);
    chartTitle.setAttribute('fill', '#8b949e');
    chartTitle.setAttribute('font-size', '9');
    chartTitle.setAttribute('font-family', 'JetBrains Mono, monospace');
    chartTitle.setAttribute('text-anchor', 'middle');
    chartTitle.textContent = 'dist';
    svg.appendChild(chartTitle);

    const avgDist = connectionDistances.reduce((a, b) => a + b, 0) / connectionDistances.length;
    const statsText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    statsText.setAttribute('x', chartX + chartW / 2);
    statsText.setAttribute('y', chartY + chartH + 12);
    statsText.setAttribute('fill', '#6e7681');
    statsText.setAttribute('font-size', '8');
    statsText.setAttribute('font-family', 'JetBrains Mono, monospace');
    statsText.setAttribute('text-anchor', 'middle');
    statsText.textContent = 'avg=' + avgDist.toFixed(2);
    svg.appendChild(statsText);

    // Draw 1/d curve
    const curvePoints = [];
    const minDist = 0.02;
    for (let i = 0; i <= 30; i++) {
        const d = minDist + (0.5 - minDist) * (i / 30);
        const y = chartY + 4 + (d / 0.5) * (chartH - 8);
        const density = 1 / d;
        const maxDensity = 1 / minDist;
        const x = plotX + (density / maxDensity) * (plotW - 4);
        curvePoints.push((i === 0 ? 'M' : 'L') + ' ' + x + ' ' + y);
    }
    const curve = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    curve.setAttribute('d', curvePoints.join(' '));
    curve.setAttribute('fill', 'none');
    curve.setAttribute('stroke', 'rgba(0, 212, 170, 0.25)');
    curve.setAttribute('stroke-width', '1.5');
    svg.appendChild(curve);

    // Axis
    const axisLine = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    axisLine.setAttribute('x1', plotX);
    axisLine.setAttribute('y1', chartY + 4);
    axisLine.setAttribute('x2', plotX);
    axisLine.setAttribute('y2', chartY + chartH - 4);
    axisLine.setAttribute('stroke', 'rgba(255,255,255,0.2)');
    axisLine.setAttribute('stroke-width', '1');
    svg.appendChild(axisLine);

    ['0', '.25', '.5'].forEach((label, i) => {
        const ly = chartY + 6 + (i / 2) * (chartH - 12);
        const axisLabel = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        axisLabel.setAttribute('x', chartX + 4);
        axisLabel.setAttribute('y', ly + 3);
        axisLabel.setAttribute('fill', '#484f58');
        axisLabel.setAttribute('font-size', '8');
        axisLabel.setAttribute('font-family', 'JetBrains Mono, monospace');
        axisLabel.textContent = label;
        svg.appendChild(axisLabel);
    });

    // Dots
    const sortedDists = [...connectionDistances].sort((a, b) => a - b);
    const processedBins = new Map();
    const dotRadius = 2.5;
    const binSize = 0.02;

    sortedDists.forEach(d => {
        const binKey = Math.floor(d / binSize);
        const countInBin = processedBins.get(binKey) || 0;
        processedBins.set(binKey, countInBin + 1);

        const y = chartY + 4 + (d / 0.5) * (chartH - 8);
        const stackOffset = countInBin * (dotRadius * 2 + 1);
        const x = plotX + dotRadius + 2 + stackOffset;

        const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        dot.setAttribute('cx', Math.min(x, chartX + chartW - dotRadius - 2));
        dot.setAttribute('cy', y);
        dot.setAttribute('r', dotRadius);
        dot.setAttribute('fill', '#a78bfa');
        dot.setAttribute('opacity', '0.85');

        const dotTitle = document.createElementNS('http://www.w3.org/2000/svg', 'title');
        dotTitle.textContent = 'Distance: ' + d.toFixed(3);
        dot.appendChild(dotTitle);
        svg.appendChild(dot);
    });
}

// Helper: Draw peers on the ring
function drawPeers(svg, peers, subscriberPeerIds, callbacks) {
    const { selectPeer, showPeerNamingPrompt } = callbacks;

    peers.forEach((peer, id) => {
        const pos = locationToXY(peer.location);
        const isHighlighted = state.highlightedPeers.has(id) || state.highlightedPeers.has(peer.peer_id);
        const isEventSelected = state.selectedEvent && (state.selectedEvent.peer_id === id || state.selectedEvent.peer_id === peer.peer_id);
        const isPeerSelected = state.selectedPeerId === id;
        const isEventHovered = state.hoveredEvent && (
            (state.hoveredEvent.from_peer === id || state.hoveredEvent.from_peer === peer.peer_id) ||
            (state.hoveredEvent.to_peer === id || state.hoveredEvent.to_peer === peer.peer_id) ||
            (state.hoveredEvent.peer_id === id || state.hoveredEvent.peer_id === peer.peer_id)
        );

        const lifecyclePeer = state.peerLifecycle?.peers?.find(p => p.peer_id === peer.peer_id);
        const isGateway = peer.is_gateway || lifecyclePeer?.is_gateway || id === state.gatewayPeerId;
        const isYou = id === state.yourPeerId;
        const isSubscriber = subscriberPeerIds.has(id);

        let fillColor = '#007FFF';
        let glowColor = 'rgba(0, 127, 255, 0.2)';
        let label = '';
        let peerStateHash = null;
        let isNonSubscriber = false;

        if (state.selectedContract) {
            if (isSubscriber) {
                if (state.contractStates[state.selectedContract] && peer.peer_id) {
                    const peerState = state.contractStates[state.selectedContract][peer.peer_id];
                    if (peerState && peerState.hash) {
                        peerStateHash = peerState.hash;
                    }
                }
                fillColor = '#f472b6';
                glowColor = 'rgba(244, 114, 182, 0.3)';
            } else {
                isNonSubscriber = true;
                fillColor = '#3a4550';
                glowColor = 'rgba(58, 69, 80, 0.15)';
            }
        }

        const peerName = peer.ip_hash ? state.peerNames[peer.ip_hash] : null;

        if (isGateway) {
            if (!state.selectedContract || isSubscriber) {
                fillColor = '#f59e0b';
                glowColor = 'rgba(245, 158, 11, 0.3)';
            } else if (isNonSubscriber) {
                fillColor = '#6b5a30';
                glowColor = 'rgba(107, 90, 48, 0.2)';
            }
            label = peerName || 'GW';
        } else if (isYou) {
            if (!state.selectedContract || isSubscriber) {
                fillColor = '#10b981';
                glowColor = 'rgba(16, 185, 129, 0.3)';
            } else if (isNonSubscriber) {
                fillColor = '#2d5a4a';
                glowColor = 'rgba(45, 90, 74, 0.2)';
            }
            label = peerName || 'YOU';
        } else if (peerName) {
            label = peerName;
        } else if (isSubscriber && !state.selectedContract) {
            fillColor = '#f472b6';
            glowColor = 'rgba(244, 114, 182, 0.3)';
        }

        if (isEventSelected) {
            fillColor = '#f87171';
            glowColor = 'rgba(248, 113, 113, 0.3)';
        } else if (isPeerSelected) {
            fillColor = '#7ecfef';
            glowColor = 'rgba(126, 207, 239, 0.4)';
        } else if (isEventHovered) {
            fillColor = '#fbbf24';
            glowColor = 'rgba(251, 191, 36, 0.5)';
        } else if (isHighlighted) {
            fillColor = '#fbbf24';
            glowColor = 'rgba(251, 191, 36, 0.3)';
        }

        const nodeSize = (isEventHovered || isHighlighted || isPeerSelected || isGateway || isYou || isSubscriber) ? 5 : 4;
        const glowSize = (isEventHovered || isHighlighted || isPeerSelected || isGateway || isYou) ? 9 : 7;

        // Glow
        const glow = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        glow.setAttribute('cx', pos.x);
        glow.setAttribute('cy', pos.y);
        glow.setAttribute('r', glowSize);
        glow.setAttribute('fill', glowColor);
        svg.appendChild(glow);

        // Click target
        const clickTarget = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        clickTarget.setAttribute('cx', pos.x);
        clickTarget.setAttribute('cy', pos.y);
        clickTarget.setAttribute('r', '20');
        clickTarget.setAttribute('fill', 'transparent');
        clickTarget.setAttribute('style', 'cursor: pointer;');
        clickTarget.onclick = () => {
            if (isYou && state.youArePeer && !peerName && showPeerNamingPrompt) {
                showPeerNamingPrompt();
            } else if (selectPeer) {
                selectPeer(id);
            }
        };
        svg.appendChild(clickTarget);

        // Main circle
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('cx', pos.x);
        circle.setAttribute('cy', pos.y);
        circle.setAttribute('r', nodeSize);
        circle.setAttribute('fill', fillColor);
        circle.setAttribute('class', 'peer-node');
        if (!isNonSubscriber) {
            circle.setAttribute('filter', 'url(#glow)');
        }
        circle.setAttribute('style', 'pointer-events: none;');

        if (isNonSubscriber) {
            circle.setAttribute('stroke', '#2a2f35');
            circle.setAttribute('stroke-width', '1');
        }

        // Tooltip
        const peerType = isGateway ? ' (Gateway)' : isYou ? ' (You)' : '';
        const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
        const peerIdentifier = peerName || (peer.ip_hash ? `#${peer.ip_hash}` : '');
        let tooltipText = `${id}${peerType}\n${peerIdentifier}\nLocation: ${peer.location.toFixed(4)}`;

        if (state.peerLifecycle && state.peerLifecycle.peers) {
            let lifecycleData = null;
            if (peer.peer_id) {
                lifecycleData = state.peerLifecycle.peers.find(p => p.peer_id === peer.peer_id);
            }
            if (!lifecycleData) {
                const topoPeer = state.initialStatePeers.find(p => p.id === id);
                if (topoPeer && topoPeer.peer_id) {
                    lifecycleData = state.peerLifecycle.peers.find(p => p.peer_id === topoPeer.peer_id);
                }
            }
            if (lifecycleData) {
                if (lifecycleData.version) tooltipText += `\nVersion: ${lifecycleData.version}`;
                if (lifecycleData.os) {
                    let osInfo = lifecycleData.os;
                    if (lifecycleData.arch) osInfo += ` (${lifecycleData.arch})`;
                    tooltipText += `\nOS: ${osInfo}`;
                }
            }
        }

        if (peerStateHash) {
            tooltipText += `\nState: [${peerStateHash.substring(0, 8)}]`;
        }
        tooltipText += '\nClick to filter events';
        title.textContent = tooltipText;
        clickTarget.appendChild(title);
        svg.appendChild(circle);

        // State indicator
        if (peerStateHash && state.selectedContract) {
            const stateColors = hashToColor(peerStateHash);
            const squareSize = nodeSize * 0.7;

            const borderRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
            borderRect.setAttribute('x', pos.x - squareSize/2 - 1);
            borderRect.setAttribute('y', pos.y - squareSize/2 - 1);
            borderRect.setAttribute('width', squareSize + 2);
            borderRect.setAttribute('height', squareSize + 2);
            borderRect.setAttribute('fill', 'white');
            borderRect.setAttribute('rx', '2');
            borderRect.setAttribute('style', 'pointer-events: none;');
            svg.appendChild(borderRect);

            const stateRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
            stateRect.setAttribute('x', pos.x - squareSize/2);
            stateRect.setAttribute('y', pos.y - squareSize/2);
            stateRect.setAttribute('width', squareSize);
            stateRect.setAttribute('height', squareSize);
            stateRect.setAttribute('fill', stateColors.fill);
            stateRect.setAttribute('rx', '1');
            stateRect.setAttribute('style', 'pointer-events: none;');
            svg.appendChild(stateRect);
        }

        // Labels
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
            if (isYou && state.youArePeer && peerName && showPeerNamingPrompt) {
                labelText.setAttribute('style', 'cursor: pointer; text-decoration: underline; text-decoration-style: dotted;');
                labelText.onclick = (e) => { e.stopPropagation(); showPeerNamingPrompt(); };
            }
            svg.appendChild(labelText);
        }

        // Inside-ring label
        const hasOutsideLabel = label && peers.size <= 15;
        if (peers.size <= 12 && !hasOutsideLabel) {
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
            text.textContent = peerName || `#${peer.ip_hash || id.substring(5, 11)}`;
            if (isYou && state.youArePeer && peerName && showPeerNamingPrompt) {
                text.setAttribute('style', 'cursor: pointer; text-decoration: underline; text-decoration-style: dotted;');
                text.onclick = (e) => { e.stopPropagation(); showPeerNamingPrompt(); };
            }
            svg.appendChild(text);
        }

        // Location label (outside ring)
        const fixedMarkers = [0, 0.25, 0.5, 0.75];
        const minDistance = 0.03;
        const nearFixedMarker = fixedMarkers.some(m =>
            Math.abs(peer.location - m) < minDistance ||
            Math.abs(peer.location - m + 1) < minDistance ||
            Math.abs(peer.location - m - 1) < minDistance
        );
        const hasSpecialLabel = label && peers.size <= 15;

        if (!nearFixedMarker && !hasSpecialLabel && peers.size <= 20) {
            const angle = peer.location * 2 * Math.PI - Math.PI / 2;
            const outerRadius = RADIUS + 25;
            const ox = CENTER + outerRadius * Math.cos(angle);
            const oy = CENTER + outerRadius * Math.sin(angle);

            const locText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            locText.setAttribute('x', ox);
            locText.setAttribute('y', oy);
            locText.setAttribute('fill', isNonSubscriber ? '#3a3f47' : '#00d4aa');
            locText.setAttribute('font-size', '10');
            locText.setAttribute('font-family', 'JetBrains Mono, monospace');
            locText.setAttribute('text-anchor', 'middle');
            locText.setAttribute('dominant-baseline', 'middle');
            locText.textContent = peer.location.toFixed(2);
            svg.appendChild(locText);
        }
    });
}

// Helper: Draw center stats
function drawCenterStats(svg, peers, subscriberPeerIds) {
    const centerGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');

    const visibleSubscribers = state.selectedContract ? [...subscriberPeerIds].filter(id => peers.has(id)).length : 0;
    const displayCount = state.selectedContract ? visibleSubscribers : peers.size;
    const displayLabel = state.selectedContract
        ? (visibleSubscribers > 0 ? 'SUBSCRIBERS' : 'PEERS')
        : 'PEERS';

    const countText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    countText.setAttribute('x', CENTER);
    countText.setAttribute('y', CENTER - 8);
    countText.setAttribute('fill', state.selectedContract && visibleSubscribers === 0 ? '#484f58' : '#00d4aa');
    countText.setAttribute('font-size', '36');
    countText.setAttribute('font-family', 'JetBrains Mono, monospace');
    countText.setAttribute('font-weight', '300');
    countText.setAttribute('text-anchor', 'middle');
    countText.textContent = displayCount;
    centerGroup.appendChild(countText);

    const labelText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    labelText.setAttribute('x', CENTER);
    labelText.setAttribute('y', CENTER + 18);
    labelText.setAttribute('fill', '#484f58');
    labelText.setAttribute('font-size', '11');
    labelText.setAttribute('font-family', 'JetBrains Mono, monospace');
    labelText.setAttribute('text-anchor', 'middle');
    labelText.textContent = displayLabel;
    centerGroup.appendChild(labelText);

    svg.appendChild(centerGroup);
}

// Helper: Draw message flow arrows
function drawMessageFlowArrows(svg, peers) {
    state.displayedEvents.forEach((event, idx) => {
        if (!event.from_peer || !event.to_peer) return;
        if (event.from_peer === event.to_peer) return;

        const fromPeer = peers.get(event.from_peer);
        const toPeer = peers.get(event.to_peer);

        const fromLoc = fromPeer?.location ?? event.from_location;
        const toLoc = toPeer?.location ?? event.to_location;

        if (fromLoc === null || fromLoc === undefined || toLoc === null || toLoc === undefined) return;

        const fromPos = locationToXY(fromLoc);
        const toPos = locationToXY(toLoc);

        const dx = toPos.x - fromPos.x;
        const dy = toPos.y - fromPos.y;
        const dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < 20) return;

        const offsetStart = 15 / dist;
        const offsetEnd = 22 / dist;

        const x1 = fromPos.x + dx * offsetStart;
        const y1 = fromPos.y + dy * offsetStart;
        const x2 = fromPos.x + dx * (1 - offsetEnd);
        const y2 = fromPos.y + dy * (1 - offsetEnd);

        const eventClass = getEventClass(event.event_type);
        const isSelected = state.selectedEvent && state.selectedEvent.timestamp === event.timestamp;

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

// Helper: Draw subscription and proximity links
function drawSubscriptionLinks(svg, peers, connections) {
    const subData = state.contractData[state.selectedContract];
    const tree = subData.tree || {};

    // Proximity links
    const proximityLinks = computeProximityLinks(state.selectedContract, peers, connections);
    proximityLinks.forEach(link => {
        const fromPeer = peers.get(link.from);
        const toPeer = peers.get(link.to);
        if (!fromPeer || !toPeer) return;

        const fromPos = locationToXY(fromPeer.location);
        const toPos = locationToXY(toPeer.location);

        const dx = toPos.x - fromPos.x;
        const dy = toPos.y - fromPos.y;
        const dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < 1) return;
        const offset = 12 / dist;

        const lineGroup = document.createElementNS("http://www.w3.org/2000/svg", "g");
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute('x1', fromPos.x + dx * offset);
        line.setAttribute('y1', fromPos.y + dy * offset);
        line.setAttribute('x2', toPos.x - dx * offset);
        line.setAttribute('y2', toPos.y - dy * offset);
        line.setAttribute('class', 'proximity-link');
        line.setAttribute('stroke', '#22d3ee');
        line.setAttribute('stroke-width', '2');
        line.setAttribute('stroke-dasharray', '4,3');
        line.setAttribute('opacity', '0.7');

        const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
        title.textContent = 'Proximity link: connected peers share updates directly';
        line.appendChild(title);

        const hitArea = document.createElementNS("http://www.w3.org/2000/svg", "line");
        hitArea.setAttribute("x1", fromPos.x + dx * offset);
        hitArea.setAttribute("y1", fromPos.y + dy * offset);
        hitArea.setAttribute("x2", toPos.x - dx * offset);
        hitArea.setAttribute("y2", toPos.y - dy * offset);
        hitArea.setAttribute("stroke", "transparent");
        hitArea.setAttribute("stroke-width", "12");
        hitArea.appendChild(title.cloneNode(true));

        lineGroup.appendChild(line);
        lineGroup.appendChild(hitArea);
        svg.appendChild(lineGroup);
    });

    // Subscription tree links
    Object.entries(tree).forEach(([fromId, toIds]) => {
        const fromPeer = peers.get(fromId);
        if (!fromPeer) return;

        toIds.forEach(toId => {
            const toPeer = peers.get(toId);
            if (!toPeer) return;

            const fromPos = locationToXY(fromPeer.location);
            const toPos = locationToXY(toPeer.location);

            const dx = toPos.x - fromPos.x;
            const dy = toPos.y - fromPos.y;
            const dist = Math.sqrt(dx*dx + dy*dy);
            if (dist < 1) return;
            const offset = 12 / dist;

            const lineGroup = document.createElementNS("http://www.w3.org/2000/svg", "g");
            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            line.setAttribute('x1', fromPos.x + dx * offset);
            line.setAttribute('y1', fromPos.y + dy * offset);
            line.setAttribute('x2', toPos.x - dx * offset);
            line.setAttribute('y2', toPos.y - dy * offset);
            line.setAttribute('class', 'subscription-link');
            line.setAttribute('stroke', '#f472b6');
            line.setAttribute('stroke-width', '2.5');
            line.setAttribute('opacity', '0.9');

            const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
            title.textContent = 'Subscription link: updates flow through this connection';
            line.appendChild(title);

            const hitArea = document.createElementNS("http://www.w3.org/2000/svg", "line");
            hitArea.setAttribute("x1", fromPos.x + dx * offset);
            hitArea.setAttribute("y1", fromPos.y + dy * offset);
            hitArea.setAttribute("x2", toPos.x - dx * offset);
            hitArea.setAttribute("y2", toPos.y - dy * offset);
            hitArea.setAttribute("stroke", "transparent");
            hitArea.setAttribute("stroke-width", "12");
            hitArea.appendChild(title.cloneNode(true));

            lineGroup.appendChild(line);
            lineGroup.appendChild(hitArea);
            svg.appendChild(lineGroup);
        });
    });
}
