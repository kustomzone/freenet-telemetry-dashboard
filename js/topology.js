/**
 * Topology visualization for Freenet Dashboard
 * Handles ring rendering and peer visualization
 *
 * Hybrid approach: SVG for static ring decorations (few elements),
 * HTML5 Canvas for dynamic peer/connection rendering (scales to 2000+ peers).
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

// ============================================================================
// Canvas overlay state - persists across renders for event handling
// ============================================================================

// Hit-test data built each frame so canvas mouse events can find peers
let canvasHitTargets = []; // [{x, y, radius, id, peer, isYou, peerName, tooltipText}]
let lastCallbacks = {};    // callbacks from most recent updateRingSVG call
let peerCanvasEl = null;   // the reusable <canvas> element
let tooltipEl = null;      // the reusable tooltip <div>
let canvasEventsInstalled = false;
let hoveredPeerTarget = null; // currently hovered hit target (for cursor)
// Connection dash animation
let connectionAnimOffset = 0;
let connectionAnimFrame = null;

// Tree edge animation state
let treeAnimCanvas = null;
let treeAnimFrame = null;
let treeAnimEdges = []; // [{from, to, cp1, cp2}]
let treeAnimT = 0;

// ============================================================================
// Ring particle replay system
// Loops a set of message flows as animated particles along spline paths
// ============================================================================

// Active particles currently animating
const ringParticles = [];       // [{fromPos, toPos, cp, color, startTime, duration}]
const PARTICLE_DURATION = 800;  // ms per particle travel

// Replay loop state
let replayFlows = [];           // [{fromPos, toPos, cp, color, offsetMs}] pre-resolved
let replayLoopDuration = 0;     // base loop duration in ms (before speed multiplier)
let replayLoopStart = 0;        // performance.now() when current loop cycle began
let replayFrame = null;         // rAF handle
let replaySpeed = 1.0;          // 0.25x to 4x
let _scheduleRedraw = null;

export function setParticleRedrawCallback(cb) {
    _scheduleRedraw = cb;
}

/**
 * Compute the quadratic bezier control point for a connection between two
 * ring locations, matching the curve used by drawConnectionsCanvas.
 */
function connectionControlPoint(fromLoc, toLoc) {
    const a1 = fromLoc * 2 * Math.PI - Math.PI / 2;
    const a2 = toLoc * 2 * Math.PI - Math.PI / 2;
    let angleDiff = a2 - a1;
    while (angleDiff > Math.PI) angleDiff -= 2 * Math.PI;
    while (angleDiff < -Math.PI) angleDiff += 2 * Math.PI;
    const midAngle = a1 + angleDiff / 2;
    const angularDist = Math.abs(angleDiff) / Math.PI;
    const pullFrac = 0.3 + angularDist * 0.5;
    const cpRadius = RADIUS * (1 - pullFrac);
    return { x: CENTER + cpRadius * Math.cos(midAngle), y: CENTER + cpRadius * Math.sin(midAngle) };
}

/**
 * Evaluate a point on a quadratic bezier at parameter t (0..1).
 */
function quadBezierAt(from, cp, to, t) {
    const u = 1 - t;
    return {
        x: u * u * from.x + 2 * u * t * cp.x + t * t * to.x,
        y: u * u * from.y + 2 * u * t * cp.y + t * t * to.y
    };
}

/**
 * Start looping replay of message flows.
 * flows: [{fromPeer, toPeer, eventType, offsetMs}] from collectFlowsForRange
 * peers: current peer Map from updateView
 */
export function startReplay(flows, peers) {
    stopReplay();
    if (flows.length === 0) return;

    // Resolve peer positions and build pre-resolved flow list
    replayFlows = [];
    for (const flow of flows) {
        let fromLoc = null, toLoc = null, fromPos = null, toPos = null;
        peers.forEach((peer, id) => {
            if (id === flow.fromPeer || peer.peer_id === flow.fromPeer) {
                fromLoc = peer.location;
                fromPos = locationToXY(peer.location);
            }
            if (id === flow.toPeer || peer.peer_id === flow.toPeer) {
                toLoc = peer.location;
                toPos = locationToXY(peer.location);
            }
        });
        if (!fromPos || !toPos || fromLoc === null || toLoc === null) continue;
        const dx = toPos.x - fromPos.x, dy = toPos.y - fromPos.y;
        if (dx * dx + dy * dy < 100) continue;

        const eventClass = getEventClass(flow.eventType);
        const color = EVENT_LINE_COLORS[eventClass] || EVENT_LINE_COLORS.other;
        const cp = connectionControlPoint(fromLoc, toLoc);

        replayFlows.push({ fromPos, toPos, cp, color, offsetMs: flow.offsetMs });
    }

    if (replayFlows.length === 0) return;

    // Compute loop duration: compress real time range into a readable replay speed.
    // The replay takes 3-8 seconds depending on the number of flows,
    // plus PARTICLE_DURATION so the last particle finishes before the loop restarts.
    const maxOffset = Math.max(...replayFlows.map(f => f.offsetMs));
    // Compress to 3-8s replay window
    const compressedDuration = Math.min(8000, Math.max(3000, maxOffset * 0.5));
    replayLoopDuration = compressedDuration + PARTICLE_DURATION + 500; // +500ms pause between loops

    // Normalize offsets to compressed duration
    if (maxOffset > 0) {
        for (const f of replayFlows) {
            f.normalizedOffset = (f.offsetMs / maxOffset) * compressedDuration;
        }
    } else {
        // All at the same time — stagger slightly
        replayFlows.forEach((f, i) => {
            f.normalizedOffset = (i / replayFlows.length) * Math.min(2000, compressedDuration);
        });
    }

    replayLoopStart = performance.now();
    startReplayLoop();
}

export function stopReplay() {
    if (replayFrame) {
        cancelAnimationFrame(replayFrame);
        replayFrame = null;
    }
    replayFlows = [];
    ringParticles.length = 0;
    state.replayProgress = -1;
}

export function isReplaying() {
    return replayFlows.length > 0;
}

/**
 * Adjust replay speed. factor is multiplied into current speed.
 * Returns the new speed for display.
 */
export function adjustReplaySpeed(factor) {
    replaySpeed = Math.max(0.25, Math.min(4, replaySpeed * factor));
    return replaySpeed;
}

export function getReplaySpeed() {
    return replaySpeed;
}


function startReplayLoop() {
    if (replayFrame) return;

    function step() {
        replayFrame = requestAnimationFrame(step);

        if (replayFlows.length === 0) {
            cancelAnimationFrame(replayFrame);
            replayFrame = null;
            return;
        }

        const now = performance.now();
        const effectiveDuration = replayLoopDuration / replaySpeed;
        const elapsed = now - replayLoopStart;

        // Loop: restart when the cycle completes
        if (elapsed >= effectiveDuration) {
            replayLoopStart = now;
            ringParticles.length = 0;
        }

        const cycleTime = now - replayLoopStart;

        // Update progress for timeline playhead
        state.replayProgress = Math.min(1, cycleTime / effectiveDuration);

        // Scale offsets by speed
        const spawnWindow = 50 / replaySpeed;
        for (const flow of replayFlows) {
            const scaledOffset = flow.normalizedOffset / replaySpeed;
            if (cycleTime >= scaledOffset && cycleTime < scaledOffset + spawnWindow) {
                if (!flow._lastSpawn || (now - flow._lastSpawn) > effectiveDuration * 0.9) {
                    flow._lastSpawn = now;
                    ringParticles.push({
                        fromPos: flow.fromPos, toPos: flow.toPos,
                        cp: flow.cp, color: flow.color,
                        startTime: now, duration: PARTICLE_DURATION / Math.sqrt(replaySpeed)
                    });
                }
            }
        }

        // Request redraw
        if (_scheduleRedraw) _scheduleRedraw();
    }

    replayFrame = requestAnimationFrame(step);
}

function drawRingParticles(ctx) {
    if (ringParticles.length === 0) return;
    const now = performance.now();

    ctx.save();
    for (let i = ringParticles.length - 1; i >= 0; i--) {
        const p = ringParticles[i];
        const elapsed = now - p.startTime;
        if (elapsed > p.duration) {
            ringParticles.splice(i, 1);
            continue;
        }

        const t = elapsed / p.duration;
        const eased = 1 - (1 - t) * (1 - t); // ease-out
        const pt = quadBezierAt(p.fromPos, p.cp, p.toPos, eased);
        const alpha = 1 - t * 0.5;

        // Trail along the spline
        ctx.beginPath();
        ctx.strokeStyle = p.color;
        ctx.lineWidth = 2;
        ctx.globalAlpha = alpha * 0.3;
        const TRAIL_STEPS = 8;
        ctx.moveTo(p.fromPos.x, p.fromPos.y);
        for (let s = 1; s <= TRAIL_STEPS; s++) {
            const st = eased * (s / TRAIL_STEPS);
            const sp = quadBezierAt(p.fromPos, p.cp, p.toPos, st);
            ctx.lineTo(sp.x, sp.y);
        }
        ctx.stroke();

        // Outer glow
        ctx.beginPath();
        ctx.arc(pt.x, pt.y, 12, 0, Math.PI * 2);
        ctx.fillStyle = p.color;
        ctx.globalAlpha = alpha * 0.15;
        ctx.fill();

        // Core dot
        ctx.beginPath();
        ctx.arc(pt.x, pt.y, 5, 0, Math.PI * 2);
        ctx.fillStyle = p.color;
        ctx.globalAlpha = alpha;
        ctx.fill();

        // Bright center
        ctx.beginPath();
        ctx.arc(pt.x, pt.y, 2, 0, Math.PI * 2);
        ctx.fillStyle = '#ffffff';
        ctx.globalAlpha = alpha * 0.9;
        ctx.fill();
    }
    ctx.globalAlpha = 1;
    ctx.restore();
}


function getOrCreateTreeAnimCanvas(container) {
    if (treeAnimCanvas && treeAnimCanvas.parentNode === container) return treeAnimCanvas;
    if (treeAnimCanvas) treeAnimCanvas.remove();
    treeAnimCanvas = document.createElement('canvas');
    treeAnimCanvas.id = 'tree-anim-canvas';
    treeAnimCanvas.style.position = 'absolute';
    treeAnimCanvas.style.top = '0';
    treeAnimCanvas.style.left = '50%';
    treeAnimCanvas.style.transform = 'translateX(-50%)';
    treeAnimCanvas.style.zIndex = '2';
    treeAnimCanvas.style.pointerEvents = 'none';
    container.appendChild(treeAnimCanvas);
    return treeAnimCanvas;
}

function startTreeEdgeAnimation(container) {
    if (treeAnimEdges.length === 0) { stopTreeEdgeAnimation(); return; }
    const canvas = getOrCreateTreeAnimCanvas(container);
    const dpr = window.devicePixelRatio || 1;
    const displaySize = parseInt(canvas.style.width) || SVG_SIZE;
    const scale = displaySize / SVG_SIZE;

    let lastFrameTime = 0;
    const FRAME_INTERVAL = 50; // ~20fps — plenty for dash animation

    function step(now) {
        treeAnimFrame = requestAnimationFrame(step);
        if (now - lastFrameTime < FRAME_INTERVAL) return;
        lastFrameTime = now;

        treeAnimT = (treeAnimT + 1) % 20; // dash period = 20
        const ctx = canvas.getContext('2d');
        ctx.setTransform(dpr * scale, 0, 0, dpr * scale, 0, 0);
        ctx.clearRect(0, 0, SVG_WIDTH, SVG_SIZE);

        ctx.lineCap = 'round';
        ctx.setLineDash([4, 8]);
        ctx.lineDashOffset = -treeAnimT;

        const selPeer = state.selectedPeerId;
        const hasSelection = !!selPeer;

        // Three tiers: dim (unrelated), normal (no selection), highlighted (selected peer's edges)
        // Batch into paths by tier to minimize draw calls
        const dimPath = new Path2D();
        const normalPath = new Path2D();
        const highlightPath = new Path2D();

        for (const edge of treeAnimEdges) {
            const touches = hasSelection && edge.peerIds.includes(selPeer);
            const target = hasSelection
                ? (touches ? highlightPath : dimPath)
                : normalPath;
            target.moveTo(edge.from.x, edge.from.y);
            target.bezierCurveTo(edge.cp1.x, edge.cp1.y, edge.cp2.x, edge.cp2.y, edge.to.x, edge.to.y);
        }

        if (hasSelection) {
            // Dim edges
            ctx.lineWidth = 1;
            ctx.strokeStyle = 'rgba(126, 207, 239, 0.1)';
            ctx.stroke(dimPath);
            // Highlighted edges
            ctx.lineWidth = 2.5;
            ctx.strokeStyle = 'rgba(126, 207, 239, 0.9)';
            ctx.stroke(highlightPath);
        } else {
            ctx.lineWidth = treeAnimEdges.length > 80 ? 1 : 2;
            ctx.strokeStyle = treeAnimEdges.length > 80
                ? 'rgba(126, 207, 239, 0.2)'
                : 'rgba(126, 207, 239, 0.7)';
            ctx.stroke(normalPath);
        }

        ctx.setLineDash([]);
    }
    if (!treeAnimFrame) treeAnimFrame = requestAnimationFrame(step);
}

function stopTreeEdgeAnimation() {
    if (treeAnimFrame) {
        cancelAnimationFrame(treeAnimFrame);
        treeAnimFrame = null;
    }
    treeAnimEdges = [];
    if (treeAnimCanvas) {
        const ctx = treeAnimCanvas.getContext('2d');
        if (ctx) ctx.clearRect(0, 0, treeAnimCanvas.width, treeAnimCanvas.height);
    }
}

// ============================================================================
// Main ring SVG rendering function (hybrid: SVG decorations + Canvas peers)
// ============================================================================

// Cached static SVG element (ring, defs, markers, location text - never changes)
let _cachedStaticSvg = null;

function getOrCreateStaticSvg() {
    if (_cachedStaticSvg) return _cachedStaticSvg;

    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', `0 0 ${SVG_WIDTH} ${SVG_SIZE}`);
    svg.style.position = 'absolute';
    svg.style.top = '0';
    svg.style.left = '50%';
    svg.style.transform = 'translateX(-50%)';
    svg.style.pointerEvents = 'none';

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
            <stop offset="0%" style="stop-color:var(--bg-primary);stop-opacity:1" />
            <stop offset="100%" style="stop-color:var(--bg-secondary);stop-opacity:1" />
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

    // Dynamic content group - cleared and rebuilt each frame (only a few elements)
    const dynamicGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    dynamicGroup.setAttribute('id', 'svg-dynamic');
    svg.appendChild(dynamicGroup);

    _cachedStaticSvg = svg;
    return svg;
}

// ============================================================================
// Radial tree overlay helpers (contract topology on ring)
// ============================================================================

/**
 * Build animated dashed-line edges for the entire subscription tree.
 * All peers stay at their normal ring positions; edges arc through the interior.
 * Dashes animate outward from the contract location: the peer closer to the
 * contract on the ring is always "from", the peer further away is "to".
 */
function drawTreeEdgesOnRing(ctx, treeData, peers) {
    treeAnimEdges = [];

    // Count edges to scale line weight
    let edgeCount = 0;
    for (const kids of treeData.children.values()) edgeCount += kids.length;
    const isLarge = edgeCount > 80;

    // Ring distance helper (shortest arc on 0..1 ring)
    const contractLoc = contractKeyToLocation(state.selectedContract);
    function ringDist(loc) {
        if (contractLoc === null) return 0;
        const d = Math.abs(loc - contractLoc);
        return Math.min(d, 1 - d);
    }

    for (const [parentId, kids] of treeData.children) {
        const parentPeer = peers.get(parentId);
        if (!parentPeer) continue;

        for (const kidId of kids) {
            const kidPeer = peers.get(kidId);
            if (!kidPeer) continue;

            // Orient edge so "from" is closer to contract location on the ring
            const parentDist = ringDist(parentPeer.location);
            const kidDist = ringDist(kidPeer.location);
            const [nearPeer, farPeer] = parentDist <= kidDist
                ? [parentPeer, kidPeer]
                : [kidPeer, parentPeer];

            const fromPos = locationToXY(nearPeer.location);
            const toPos = locationToXY(farPeer.location);
            const fromAngle = nearPeer.location * 2 * Math.PI - Math.PI / 2;
            const toAngle = farPeer.location * 2 * Math.PI - Math.PI / 2;

            // Quadratic bezier with control point pulled toward center.
            // Pull strength proportional to angular distance.
            let angleDiff = toAngle - fromAngle;
            while (angleDiff > Math.PI) angleDiff -= 2 * Math.PI;
            while (angleDiff < -Math.PI) angleDiff += 2 * Math.PI;

            const midAngle = fromAngle + angleDiff / 2;
            const angularDist = Math.abs(angleDiff) / Math.PI; // 0..1
            const pullFrac = 0.3 + angularDist * 0.5;
            const cpRadius = RADIUS * (1 - pullFrac);
            const cp = {
                x: CENTER + cpRadius * Math.cos(midAngle),
                y: CENTER + cpRadius * Math.sin(midAngle)
            };

            treeAnimEdges.push({
                from: fromPos, to: toPos,
                cp1: cp, cp2: cp,
                isPrimary: !isLarge,
                peerIds: [parentId, kidId]
            });
        }
    }

    if (treeAnimEdges.length === 0) { stopTreeEdgeAnimation(); return; }
}

export function updateRingSVG(peers, connections, subscriberPeerIds = new Set(), callbacks = {}, treeData = null) {
    lastCallbacks = callbacks;
    const container = document.getElementById('ring-container');

    // --- SVG: cached static + dynamic overlay ---
    const svg = getOrCreateStaticSvg();
    const dynamicGroup = svg.getElementById('svg-dynamic');
    // Clear only the dynamic content (contract diamond, center stats, message arrows)
    dynamicGroup.innerHTML = '';

    // Draw contract location indicator when selected
    if (state.selectedContract) {
        const contractLocation = contractKeyToLocation(state.selectedContract);
        if (contractLocation !== null) {
            const angle = contractLocation * 2 * Math.PI - Math.PI / 2;
            const cosA = Math.cos(angle), sinA = Math.sin(angle);
            const ringX = CENTER + RADIUS * cosA;
            const ringY = CENTER + RADIUS * sinA;

            // Bullseye target outside the ring
            const markerR = RADIUS + 22;
            const mx = CENTER + markerR * cosA;
            const my = CENTER + markerR * sinA;

            // Thin line from ring edge to marker
            const tick = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            tick.setAttribute('x1', ringX);
            tick.setAttribute('y1', ringY);
            tick.setAttribute('x2', CENTER + (markerR - 10) * cosA);
            tick.setAttribute('y2', CENTER + (markerR - 10) * sinA);
            tick.setAttribute('stroke', '#7ecfef');
            tick.setAttribute('stroke-width', '1');
            tick.setAttribute('stroke-opacity', '0.6');
            dynamicGroup.appendChild(tick);

            const diamond = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            diamond.setAttribute('x', mx);
            diamond.setAttribute('y', my);
            diamond.setAttribute('text-anchor', 'middle');
            diamond.setAttribute('dominant-baseline', 'central');
            diamond.setAttribute('font-size', '26');
            diamond.setAttribute('fill', '#7ecfef');
            diamond.setAttribute('style', 'pointer-events: none;');
            diamond.textContent = '\u25CE';
            dynamicGroup.appendChild(diamond);
        }
    }

    // Center stats (SVG text - few elements)
    drawCenterStats(dynamicGroup, peers, subscriberPeerIds);

    // Message flow arrows (SVG - uses marker defs, typically <20 elements)
    if (!state.selectedContract && state.displayedEvents && state.displayedEvents.length > 0) {
        drawMessageFlowArrows(dynamicGroup, peers);
    }

    // --- Canvas: dynamic peer/connection rendering ---
    // Compute tree overlay data if tree data provided
    let treeOverlay = null;
    if (treeData && treeData.allNodes.size > 0) {
        treeOverlay = { treeData };
    }

    // Compute connection distances (needed for dist chart regardless of drawing)
    if (!treeOverlay) {
        const connectionDistances = [];
        connections.forEach(connKey => {
            const [id1, id2] = connKey.split('|');
            const peer1 = peers.get(id1);
            const peer2 = peers.get(id2);
            if (peer1 && peer2) {
                const rawDist = Math.abs(peer1.location - peer2.location);
                connectionDistances.push(Math.min(rawDist, 1 - rawDist));
            }
        });
        if (connectionDistances.length > 0) {
            drawDistanceChartOverlay(connectionDistances);
        }
    }

    // Ensure canvas element exists and is sized to fit container (square)
    const canvas = getOrCreatePeerCanvas(container);
    const dpr = window.devicePixelRatio || 1;
    const displaySize = Math.min(container.offsetWidth, container.offsetHeight) || SVG_SIZE;
    const targetPx = Math.round(displaySize * dpr);
    if (canvas.width !== targetPx || canvas.height !== targetPx) {
        canvas.width = targetPx;
        canvas.height = targetPx;
        canvas.style.width = displaySize + 'px';
        canvas.style.height = displaySize + 'px';
    }

    // Also update SVG to match
    svg.style.width = displaySize + 'px';
    svg.style.height = displaySize + 'px';

    const ctx = canvas.getContext('2d');
    // Scale from logical SVG_SIZE coordinate space to actual display size
    const scale = displaySize / SVG_SIZE;
    ctx.setTransform(dpr * scale, 0, 0, dpr * scale, 0, 0);
    ctx.clearRect(0, 0, SVG_WIDTH, SVG_SIZE);

    if (treeOverlay) {
        // Contract selected: show connections between hosting peers (faded),
        // then overlay animated subscription tree edges
        drawConnectionsCanvas(ctx, peers, connections, subscriberPeerIds);
        drawTreeEdgesOnRing(ctx, treeOverlay.treeData, peers);

        // Set up animation overlay canvas (same size as peer canvas)
        const animCanvas = getOrCreateTreeAnimCanvas(container);
        if (animCanvas.width !== targetPx || animCanvas.height !== targetPx) {
            animCanvas.width = targetPx;
            animCanvas.height = targetPx;
            animCanvas.style.width = displaySize + 'px';
            animCanvas.style.height = displaySize + 'px';
        }
        startTreeEdgeAnimation(container);
    } else {
        // Normal ring: connections + arrows
        stopTreeEdgeAnimation();
        drawConnectionsCanvas(ctx, peers, connections, subscriberPeerIds);
        drawSelectedTransactionArrows(ctx, peers);
        // Suppress static hover line when particles are animating — particles show the flow
        if (ringParticles.length === 0) {
            drawHoveredEventLine(ctx, peers);
        }
    }

    // Draw peers on canvas and build hit-test array
    drawPeersCanvas(ctx, peers, connections, subscriberPeerIds, callbacks, treeOverlay);

    // Draw any active ring particles (from timeline scrubbing)
    drawRingParticles(ctx);

    // Install mouse events once
    installCanvasEvents(canvas, container);

    // Assemble into container: canvas behind SVG (only on first call)
    const emptyState = container.querySelector('.empty-state');
    if (emptyState) emptyState.remove();
    if (!svg.parentNode) {
        container.appendChild(svg);
    }
}

// ============================================================================
// Canvas element management
// ============================================================================

function getOrCreatePeerCanvas(container) {
    if (peerCanvasEl && peerCanvasEl.parentNode === container) {
        return peerCanvasEl;
    }
    // Remove any stale canvas
    if (peerCanvasEl) peerCanvasEl.remove();

    peerCanvasEl = document.createElement('canvas');
    peerCanvasEl.id = 'peer-canvas';
    peerCanvasEl.style.position = 'absolute';
    peerCanvasEl.style.top = '0';
    peerCanvasEl.style.left = '50%';
    peerCanvasEl.style.transform = 'translateX(-50%)';
    peerCanvasEl.style.zIndex = '1'; // above svg (which has no pointer-events)
    container.style.position = 'relative';
    container.appendChild(peerCanvasEl);
    return peerCanvasEl;
}

function getOrCreateTooltip(container) {
    if (tooltipEl && tooltipEl.parentNode === container) {
        return tooltipEl;
    }
    if (tooltipEl) tooltipEl.remove();

    tooltipEl = document.createElement('div');
    tooltipEl.id = 'peer-canvas-tooltip';
    tooltipEl.style.cssText = `
        position: absolute;
        pointer-events: none;
        background: rgba(13, 17, 23, 0.95);
        color: #e6edf3;
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
        padding: 6px 10px;
        border-radius: 6px;
        border: 1px solid rgba(255,255,255,0.1);
        white-space: pre-line;
        z-index: 100;
        display: none;
        max-width: 300px;
        line-height: 1.4;
    `;
    container.appendChild(tooltipEl);
    return tooltipEl;
}

// ============================================================================
// Canvas mouse event handling
// ============================================================================

function installCanvasEvents(canvas, container) {
    if (canvasEventsInstalled) return;
    canvasEventsInstalled = true;

    // Click anywhere in the topology panel (outside a peer) clears selection
    const panel = container.closest('.panel');
    if (panel) {
        panel.addEventListener('click', (e) => {
            // Only clear if click wasn't handled by canvas or other interactive elements
            if (e.target.closest('canvas') || e.target.closest('a') || e.target.closest('button')) return;
            if (state.selectedPeerId) {
                const { selectPeer } = lastCallbacks;
                if (selectPeer) selectPeer(state.selectedPeerId); // toggle off
            }
        });
    }

    const tooltip = getOrCreateTooltip(container);

    canvas.addEventListener('mousemove', (e) => {
        const rect = canvas.getBoundingClientRect();
        const scaleX = SVG_WIDTH / rect.width;
        const scaleY = SVG_SIZE / rect.height;
        const mx = (e.clientX - rect.left) * scaleX;
        const my = (e.clientY - rect.top) * scaleY;

        const hit = findHitTarget(mx, my);
        if (hit) {
            canvas.style.cursor = 'pointer';
            hoveredPeerTarget = hit;
            tooltip.style.display = 'block';
            tooltip.textContent = hit.tooltipText;
            // Position tooltip relative to container
            const containerRect = container.getBoundingClientRect();
            let tipX = e.clientX - containerRect.left + 12;
            let tipY = e.clientY - containerRect.top - 10;
            // Keep tooltip in view
            const tipW = tooltip.offsetWidth;
            const tipH = tooltip.offsetHeight;
            if (tipX + tipW > containerRect.width) tipX = tipX - tipW - 24;
            if (tipY + tipH > containerRect.height) tipY = containerRect.height - tipH - 4;
            if (tipY < 0) tipY = 4;
            tooltip.style.left = tipX + 'px';
            tooltip.style.top = tipY + 'px';
        } else {
            canvas.style.cursor = '';
            hoveredPeerTarget = null;
            tooltip.style.display = 'none';
        }
    });

    canvas.addEventListener('mouseleave', () => {
        canvas.style.cursor = '';
        hoveredPeerTarget = null;
        tooltip.style.display = 'none';
    });

    canvas.addEventListener('click', (e) => {
        const rect = canvas.getBoundingClientRect();
        const scaleX = SVG_WIDTH / rect.width;
        const scaleY = SVG_SIZE / rect.height;
        const mx = (e.clientX - rect.left) * scaleX;
        const my = (e.clientY - rect.top) * scaleY;

        const hit = findHitTarget(mx, my);
        if (hit) {
            const { selectPeer, showPeerNamingPrompt } = lastCallbacks;
            if (hit.isYou && state.youArePeer && !hit.peerName && showPeerNamingPrompt) {
                showPeerNamingPrompt();
            } else if (selectPeer) {
                selectPeer(hit.id);
            }
        } else if (state.selectedPeerId) {
            // Click on empty space clears peer selection
            const { selectPeer } = lastCallbacks;
            if (selectPeer) selectPeer(state.selectedPeerId); // toggle off
        }
    });
}

function findHitTarget(mx, my) {
    // Search in reverse order so top-drawn peers are found first
    const HIT_RADIUS = 20; // same as old SVG click targets
    let closest = null;
    let closestDist = HIT_RADIUS;

    for (let i = canvasHitTargets.length - 1; i >= 0; i--) {
        const t = canvasHitTargets[i];
        const dx = mx - t.x;
        const dy = my - t.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < closestDist) {
            closest = t;
            closestDist = dist;
        }
    }
    return closest;
}

// ============================================================================
// Canvas: draw connections
// ============================================================================

function drawConnectionsCanvas(ctx, peers, connections, subscriberPeerIds = new Set()) {
    const contractSelected = state.selectedContract && subscriberPeerIds.size > 0;
    const CONN_HIDE_THRESHOLD = 50;
    const CONN_ANIM_THRESHOLD = 30;
    // When contract selected, show all connections between hosting peers
    // (the contract filter already restricts to hosting peers only)
    const hostingPeerCount = contractSelected ? subscriberPeerIds.size : peers.size;
    const showAllConnections = contractSelected || hostingPeerCount <= CONN_HIDE_THRESHOLD;
    const animateConnections = !contractSelected && hostingPeerCount <= CONN_ANIM_THRESHOLD;
    const focusPeerId = state.selectedPeerId || null;

    // Determine connection color based on focused peer type
    let focusConnColor = '0, 127, 255'; // default blue
    if (focusPeerId) {
        const isYou = focusPeerId === state.yourPeerId;
        const focusPeerData = peers.get(focusPeerId);
        const lifecyclePeer = focusPeerData?.peer_id
            ? state.peerLifecycle?.peers?.find(p => p.peer_id === focusPeerData.peer_id)
            : undefined;
        const isGateway = focusPeerData?.is_gateway || lifecyclePeer?.is_gateway || focusPeerId === state.gatewayPeerId;
        if (isYou) {
            focusConnColor = '16, 185, 129'; // emerald
        } else if (isGateway) {
            focusConnColor = '245, 158, 11'; // amber
        }
    }

    // Collect lines to draw
    const lines = [];
    connections.forEach(connKey => {
        const [id1, id2] = connKey.split('|');
        const peer1 = peers.get(id1);
        const peer2 = peers.get(id2);
        if (!peer1 || !peer2) return;

        // Contract filter: only show connections to/from the selected peer
        // (between hosting peers). The subscription tree edges are drawn
        // separately by the tree animation overlay.
        if (contractSelected) {
            const bothHost = subscriberPeerIds.has(id1) && subscriberPeerIds.has(id2);
            const isFocusConn = focusPeerId && (id1 === focusPeerId || id2 === focusPeerId);
            if (!bothHost || !isFocusConn) return;
        }

        const isFocusConn = focusPeerId && (id1 === focusPeerId || id2 === focusPeerId);
        if (!showAllConnections && !isFocusConn) return;

        const pos1 = locationToXY(peer1.location);
        const pos2 = locationToXY(peer2.location);

        let opacity, color;
        if (contractSelected) {
            // Contract mode: only showing selected peer's connections
            opacity = 0.5;
            color = focusConnColor;
        } else {
            opacity = (isFocusConn && !showAllConnections) ? 0.6 : 0.3;
            color = isFocusConn ? focusConnColor : '0, 127, 255';
        }

        // Curved connections for focused peer, straight for background
        let cp = null;
        if (isFocusConn) {
            const a1 = peer1.location * 2 * Math.PI - Math.PI / 2;
            const a2 = peer2.location * 2 * Math.PI - Math.PI / 2;
            let angleDiff = a2 - a1;
            while (angleDiff > Math.PI) angleDiff -= 2 * Math.PI;
            while (angleDiff < -Math.PI) angleDiff += 2 * Math.PI;
            const midAngle = a1 + angleDiff / 2;
            const angularDist = Math.abs(angleDiff) / Math.PI;
            const pullFrac = 0.3 + angularDist * 0.5;
            const cpRadius = RADIUS * (1 - pullFrac);
            cp = { x: CENTER + cpRadius * Math.cos(midAngle), y: CENTER + cpRadius * Math.sin(midAngle) };
        }
        lines.push({ x1: pos1.x, y1: pos1.y, x2: pos2.x, y2: pos2.y, opacity, color, cp });
    });

    if (lines.length === 0) return;

    ctx.lineCap = 'round';

    // Split into background (straight) and focused (curved/animated)
    const bgLines = [];
    const focusLines = [];
    for (const l of lines) {
        (l.cp ? focusLines : bgLines).push(l);
    }

    // Background connections: straight, solid or small-network animated
    if (bgLines.length > 0) {
        ctx.lineWidth = 1.5;
        if (animateConnections) {
            ctx.setLineDash([8, 4]);
            ctx.lineDashOffset = -connectionAnimOffset;
            startConnectionAnimation();
        } else {
            ctx.setLineDash([]);
        }
        const byStyle = new Map();
        for (const l of bgLines) {
            const key = `rgba(${l.color}, ${l.opacity})`;
            if (!byStyle.has(key)) byStyle.set(key, []);
            byStyle.get(key).push(l);
        }
        for (const [style, batch] of byStyle) {
            ctx.strokeStyle = style;
            ctx.beginPath();
            for (const l of batch) {
                ctx.moveTo(l.x1, l.y1);
                ctx.lineTo(l.x2, l.y2);
            }
            ctx.stroke();
        }
        ctx.setLineDash([]);
    }

    // Focused connections: curved, static
    if (focusLines.length > 0) {
        ctx.lineWidth = 1.5;
        ctx.setLineDash([]);
        const byStyle = new Map();
        for (const l of focusLines) {
            const key = `rgba(${l.color}, ${l.opacity})`;
            if (!byStyle.has(key)) byStyle.set(key, []);
            byStyle.get(key).push(l);
        }
        for (const [style, batch] of byStyle) {
            ctx.strokeStyle = style;
            ctx.beginPath();
            for (const l of batch) {
                ctx.moveTo(l.x1, l.y1);
                ctx.quadraticCurveTo(l.cp.x, l.cp.y, l.x2, l.y2);
            }
            ctx.stroke();
        }
    }

    ctx.lineDashOffset = 0;
}

function startConnectionAnimation() {
    // Only start one animation loop
    if (connectionAnimFrame) return;
    const step = () => {
        connectionAnimOffset = (connectionAnimOffset + 0.5) % 24;
        connectionAnimFrame = requestAnimationFrame(step);
    };
    connectionAnimFrame = requestAnimationFrame(step);
}

// ============================================================================
// Canvas: draw hovered event highlight line
// ============================================================================

// Event type colors for hovered event line (matches CSS vars)
const EVENT_LINE_COLORS = {
    connect:   '#7ecfef',
    put:       '#fbbf24',
    get:       '#34d399',
    update:    '#a78bfa',
    subscribe: '#f472b6',
    other:     '#9ca3af'
};

function drawHoveredEventLine(ctx, peers) {
    if (!state.hoveredEvent) return;
    const fromPeer = state.hoveredEvent.from_peer || state.hoveredEvent.peer_id;
    const toPeer = state.hoveredEvent.to_peer;
    if (!fromPeer) return;

    const eventClass = getEventClass(state.hoveredEvent.event_type);
    const color = EVENT_LINE_COLORS[eventClass] || EVENT_LINE_COLORS.other;
    const eventType = state.hoveredEvent.event_type;

    // Find peer positions
    let fromPos = null, toPos = null;
    peers.forEach((peer, id) => {
        if (id === fromPeer || peer.peer_id === fromPeer) {
            fromPos = locationToXY(peer.location);
        }
        if (toPeer && (id === toPeer || peer.peer_id === toPeer)) {
            toPos = locationToXY(peer.location);
        }
    });
    if (!fromPos) return;

    ctx.save();

    const hasTwoPeers = toPeer && fromPeer !== toPeer && toPos;

    if (hasTwoPeers) {
        // --- Two-peer arrow ---
        const dx = toPos.x - fromPos.x;
        const dy = toPos.y - fromPos.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 10) { ctx.restore(); return; }

        const offsetStart = 12 / dist;
        const offsetEnd = 12 / dist;
        const x1 = fromPos.x + dx * offsetStart;
        const y1 = fromPos.y + dy * offsetStart;
        const x2 = fromPos.x + dx * (1 - offsetEnd);
        const y2 = fromPos.y + dy * (1 - offsetEnd);

        // Line
        ctx.strokeStyle = color;
        ctx.lineWidth = 3;
        ctx.lineCap = 'round';
        ctx.globalAlpha = 0.85;
        ctx.beginPath();
        ctx.moveTo(x1, y1);
        ctx.lineTo(x2, y2);
        ctx.stroke();

        // Arrowhead
        const angle = Math.atan2(dy, dx);
        const arrowLen = 10;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.moveTo(x2, y2);
        ctx.lineTo(x2 - arrowLen * Math.cos(angle - Math.PI / 6), y2 - arrowLen * Math.sin(angle - Math.PI / 6));
        ctx.lineTo(x2 - arrowLen * Math.cos(angle + Math.PI / 6), y2 - arrowLen * Math.sin(angle + Math.PI / 6));
        ctx.closePath();
        ctx.fill();

        // Label at midpoint
        if (eventType) {
            drawEventLabel(ctx, (x1 + x2) / 2, (y1 + y2) / 2, eventType, color);
        }
    } else {
        // --- Single-peer highlight: pulsing ring + label ---
        const px = fromPos.x;
        const py = fromPos.y;

        // Outer glow
        ctx.globalAlpha = 0.3;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(px, py, 18, 0, Math.PI * 2);
        ctx.fill();

        // Ring
        ctx.globalAlpha = 0.85;
        ctx.strokeStyle = color;
        ctx.lineWidth = 2.5;
        ctx.beginPath();
        ctx.arc(px, py, 12, 0, Math.PI * 2);
        ctx.stroke();

        // Label above the peer
        if (eventType) {
            drawEventLabel(ctx, px, py - 22, eventType, color);
        }
    }

    ctx.restore();
}

/**
 * Draw arrows for all events in the selected transaction.
 *
 * Infers message flow from the transaction's events:
 * - Finds the "origin" peer (the one with a request/emitted event)
 * - Finds "receiver" peers (those with received/applied/success events)
 * - Draws arrows from origin → each receiver
 * - Falls back to per-event from_peer/to_peer when they differ
 */
function drawSelectedTransactionArrows(ctx, peers) {
    const txEvents = state.selectedTxEvents;
    if (!txEvents || txEvents.length === 0) return;

    // Build peer position lookup (anon_id and peer_id both map to position)
    const peerPos = new Map();
    peers.forEach((peer, id) => {
        const pos = locationToXY(peer.location);
        peerPos.set(id, pos);
        if (peer.peer_id) peerPos.set(peer.peer_id, pos);
    });

    const sorted = [...txEvents].sort((a, b) => a.timestamp - b.timestamp);

    // Classify events: find origin peer and receiver peers
    // "request" or "emitted" events → origin; "received"/"applied"/"success" → receiver
    const REQUEST_PATTERNS = ['_request', '_emitted', 'broadcast_emitted'];
    const RECEIVE_PATTERNS = ['_received', '_applied', '_success', '_not_found', 'subscribed', 'connected'];

    let originPeer = null;
    const receiverPeers = new Map(); // peerId → {eventType, color}
    const explicitArrows = new Map(); // "from|to" → {fromPos, toPos, eventType, color}

    // First pass: find explicit two-peer arrows and classify peers
    for (const evt of sorted) {
        const peerId = evt.peer_id;
        const fromPeer = evt.from_peer;
        const toPeer = evt.to_peer;
        const et = evt.event_type || '';
        const eventClass = getEventClass(et);
        const color = EVENT_LINE_COLORS[eventClass] || EVENT_LINE_COLORS.other;

        // Check for explicit two-peer events (from_peer ≠ to_peer)
        if (fromPeer && toPeer && fromPeer !== toPeer && peerPos.get(fromPeer) && peerPos.get(toPeer)) {
            const key = fromPeer + '|' + toPeer;
            explicitArrows.set(key, {
                fromPos: peerPos.get(fromPeer), toPos: peerPos.get(toPeer),
                eventType: et, color
            });
            continue;
        }

        // Classify by event type pattern
        const isOrigin = REQUEST_PATTERNS.some(p => et.includes(p));
        const isReceiver = RECEIVE_PATTERNS.some(p => et.includes(p));

        if (isOrigin && peerId && peerPos.get(peerId)) {
            originPeer = { id: peerId, eventType: et, color };
        } else if (isReceiver && peerId && peerPos.get(peerId)) {
            // Don't overwrite origin as a receiver
            if (!originPeer || originPeer.id !== peerId) {
                receiverPeers.set(peerId, { eventType: et, color });
            }
        }
    }

    // If no explicit origin found, use the earliest event's peer
    if (!originPeer && sorted.length > 0) {
        const first = sorted[0];
        const pid = first.peer_id;
        if (pid && peerPos.get(pid)) {
            const ec = getEventClass(first.event_type);
            originPeer = { id: pid, eventType: first.event_type, color: EVENT_LINE_COLORS[ec] || EVENT_LINE_COLORS.other };
        }
    }

    // Build inferred arrows: origin → each receiver
    const arrowMap = new Map(explicitArrows);
    if (originPeer) {
        const originPos = peerPos.get(originPeer.id);
        for (const [recvId, info] of receiverPeers) {
            if (recvId === originPeer.id) continue;
            const recvPos = peerPos.get(recvId);
            if (!recvPos) continue;
            const key = originPeer.id + '|' + recvId;
            if (!arrowMap.has(key)) {
                arrowMap.set(key, {
                    fromPos: originPos, toPos: recvPos,
                    eventType: info.eventType, color: info.color
                });
            }
        }
    }

    ctx.save();

    // Glow on origin peer
    if (originPeer) {
        const pos = peerPos.get(originPeer.id);
        if (pos) {
            ctx.globalAlpha = 0.3;
            ctx.fillStyle = originPeer.color;
            ctx.beginPath();
            ctx.arc(pos.x, pos.y, 18, 0, Math.PI * 2);
            ctx.fill();
            ctx.globalAlpha = 0.8;
            ctx.strokeStyle = originPeer.color;
            ctx.lineWidth = 2.5;
            ctx.beginPath();
            ctx.arc(pos.x, pos.y, 12, 0, Math.PI * 2);
            ctx.stroke();
        }
    }

    // If no arrows to draw (single-peer transaction), glow all involved peers
    if (arrowMap.size === 0) {
        for (const evt of sorted) {
            const pid = evt.peer_id;
            if (!pid) continue;
            const pos = peerPos.get(pid);
            if (!pos) continue;
            const ec = getEventClass(evt.event_type);
            const color = EVENT_LINE_COLORS[ec] || EVENT_LINE_COLORS.other;
            ctx.globalAlpha = 0.25;
            ctx.fillStyle = color;
            ctx.beginPath();
            ctx.arc(pos.x, pos.y, 16, 0, Math.PI * 2);
            ctx.fill();
            ctx.globalAlpha = 0.7;
            ctx.strokeStyle = color;
            ctx.lineWidth = 2;
            ctx.beginPath();
            ctx.arc(pos.x, pos.y, 11, 0, Math.PI * 2);
            ctx.stroke();
        }
        ctx.restore();
        return;
    }

    // Draw arrows
    for (const { fromPos, toPos, eventType, color } of arrowMap.values()) {
        const dx = toPos.x - fromPos.x;
        const dy = toPos.y - fromPos.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 10) continue;

        const offsetStart = 12 / dist;
        const offsetEnd = 12 / dist;
        const x1 = fromPos.x + dx * offsetStart;
        const y1 = fromPos.y + dy * offsetStart;
        const x2 = fromPos.x + dx * (1 - offsetEnd);
        const y2 = fromPos.y + dy * (1 - offsetEnd);

        // Line
        ctx.strokeStyle = color;
        ctx.lineWidth = 2.5;
        ctx.lineCap = 'round';
        ctx.globalAlpha = 0.75;
        ctx.beginPath();
        ctx.moveTo(x1, y1);
        ctx.lineTo(x2, y2);
        ctx.stroke();

        // Arrowhead
        const angle = Math.atan2(dy, dx);
        const arrowLen = 9;
        ctx.fillStyle = color;
        ctx.globalAlpha = 0.85;
        ctx.beginPath();
        ctx.moveTo(x2, y2);
        ctx.lineTo(x2 - arrowLen * Math.cos(angle - Math.PI / 6), y2 - arrowLen * Math.sin(angle - Math.PI / 6));
        ctx.lineTo(x2 - arrowLen * Math.cos(angle + Math.PI / 6), y2 - arrowLen * Math.sin(angle + Math.PI / 6));
        ctx.closePath();
        ctx.fill();

        // Label at midpoint
        drawEventLabel(ctx, (x1 + x2) / 2, (y1 + y2) / 2, eventType, color);
    }

    ctx.restore();
}

/** Draw a labeled pill for an event type at position (mx, my). */
function drawEventLabel(ctx, mx, my, eventType, color) {
    const label = eventType.replace(/_/g, ' ');
    ctx.font = '9px "JetBrains Mono", monospace';
    const metrics = ctx.measureText(label);
    const padX = 5, padY = 3;
    const boxW = metrics.width + padX * 2;
    const boxH = 13 + padY * 2;

    const bx = mx - boxW / 2;
    const by = my - boxH / 2;
    ctx.globalAlpha = 0.85;
    ctx.fillStyle = 'rgba(13, 17, 23, 0.9)';
    ctx.beginPath();
    ctx.roundRect(bx, by, boxW, boxH, 4);
    ctx.fill();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.roundRect(bx, by, boxW, boxH, 4);
    ctx.stroke();

    ctx.globalAlpha = 1;
    ctx.fillStyle = color;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(label, mx, my);
}

// ============================================================================
// Canvas: draw subscription / proximity links
// ============================================================================

function drawSubscriptionLinksCanvas(ctx, peers, connections) {
    const subData = state.contractData[state.selectedContract];
    const tree = subData.tree || {};

    // Proximity links (dashed cyan)
    const proximityLinks = computeProximityLinks(state.selectedContract, peers, connections);
    if (proximityLinks.length > 0) {
        ctx.save();
        ctx.strokeStyle = 'rgba(34, 211, 238, 0.7)';
        ctx.lineWidth = 2;
        ctx.setLineDash([4, 3]);
        ctx.lineCap = 'round';
        ctx.beginPath();
        for (const link of proximityLinks) {
            const fromPeer = peers.get(link.from);
            const toPeer = peers.get(link.to);
            if (!fromPeer || !toPeer) continue;
            const fromPos = locationToXY(fromPeer.location);
            const toPos = locationToXY(toPeer.location);
            const dx = toPos.x - fromPos.x;
            const dy = toPos.y - fromPos.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            if (dist < 1) continue;
            const offset = 12 / dist;
            ctx.moveTo(fromPos.x + dx * offset, fromPos.y + dy * offset);
            ctx.lineTo(toPos.x - dx * offset, toPos.y - dy * offset);
        }
        ctx.stroke();
        ctx.restore();
    }

    // Subscription tree links (solid pink)
    const treeLines = [];
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
            const dist = Math.sqrt(dx * dx + dy * dy);
            if (dist < 1) return;
            const offset = 12 / dist;
            treeLines.push({
                x1: fromPos.x + dx * offset, y1: fromPos.y + dy * offset,
                x2: toPos.x - dx * offset, y2: toPos.y - dy * offset
            });
        });
    });

    if (treeLines.length > 0) {
        ctx.save();
        ctx.strokeStyle = 'rgba(244, 114, 182, 0.9)';
        ctx.lineWidth = 2.5;
        ctx.lineCap = 'round';
        ctx.setLineDash([]);
        ctx.beginPath();
        for (const l of treeLines) {
            ctx.moveTo(l.x1, l.y1);
            ctx.lineTo(l.x2, l.y2);
        }
        ctx.stroke();
        ctx.restore();
    }
}

// ============================================================================
// Canvas: draw peers (replaces SVG drawPeers -- the main scalability win)
// ============================================================================

function drawPeersCanvas(ctx, peers, connections, subscriberPeerIds, callbacks, treeOverlay = null) {
    const { showPeerNamingPrompt } = callbacks;
    const isLargeNetwork = peers.size > 50;
    const isVeryLargeNetwork = peers.size > 500;
    const showGlow = !isLargeNetwork;
    const showLabels = peers.size <= 15;
    const showLocationLabels = peers.size <= 20;
    const showInsideLabels = peers.size <= 12;

    // Build set of peers connected to the selected peer (for smart label display)
    const connectedToSelected = new Set();
    if (state.selectedPeerId) {
        connections.forEach(connKey => {
            const [id1, id2] = connKey.split('|');
            if (id1 === state.selectedPeerId) connectedToSelected.add(id2);
            if (id2 === state.selectedPeerId) connectedToSelected.add(id1);
        });
    }

    // Reset hit targets
    canvasHitTargets = [];

    // Pre-build O(1) lookup maps (avoid O(n²) .find() inside per-peer loop)
    const lifecycleByPeerId = new Map();
    if (state.peerLifecycle?.peers) {
        for (const p of state.peerLifecycle.peers) {
            if (p.peer_id) lifecycleByPeerId.set(p.peer_id, p);
        }
    }
    const initialPeerById = new Map();
    for (const p of state.initialStatePeers) {
        initialPeerById.set(p.id, p);
    }

    // Pre-compute all peer rendering data
    const peerRenderData = [];

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

        const lifecyclePeer = peer.peer_id ? lifecycleByPeerId.get(peer.peer_id) : undefined;
        const isGateway = peer.is_gateway || lifecyclePeer?.is_gateway || id === state.gatewayPeerId;
        const isYou = id === state.yourPeerId;
        const isSubscriber = subscriberPeerIds.has(id);

        let fillColor = '#007FFF';
        let glowColor = 'rgba(0, 127, 255, 0.2)';
        let label = '';
        let peerStateHash = null;
        let isNonSubscriber = false;

        if (state.selectedContract && !isSubscriber) {
            isNonSubscriber = true;
            fillColor = '#3a4550';
            glowColor = 'rgba(58, 69, 80, 0.15)';
        }

        const peerName = peer.ip_hash ? state.peerNames[peer.ip_hash] : null;

        if (isGateway) {
            if (!isNonSubscriber) {
                fillColor = '#f59e0b';
                glowColor = 'rgba(245, 158, 11, 0.3)';
            }
            label = peerName || 'GW';
        } else if (isYou) {
            if (!isNonSubscriber) {
                fillColor = '#10b981';
                glowColor = 'rgba(16, 185, 129, 0.3)';
            }
            label = peerName || 'YOU';
        } else if (peerName) {
            label = peerName;
        }

        if (isEventSelected) {
            fillColor = '#f87171';
            glowColor = 'rgba(248, 113, 113, 0.3)';
        } else if (isPeerSelected && !isYou) {
            fillColor = '#7ecfef';
            glowColor = 'rgba(126, 207, 239, 0.4)';
        } else if (isEventHovered) {
            fillColor = '#fbbf24';
            glowColor = 'rgba(251, 191, 36, 0.5)';
        } else if (isHighlighted) {
            fillColor = '#fbbf24';
            glowColor = 'rgba(251, 191, 36, 0.3)';
        }

        const isSpecial = isEventHovered || isHighlighted || isPeerSelected || isGateway || isYou;
        let nodeSize, glowSize;
        if (treeOverlay) {
            // Tree overlay: subscribers normal, non-subscribers nearly invisible
            if (isNonSubscriber) {
                nodeSize = 1;
                glowSize = 0;
                fillColor = 'rgba(58, 69, 80, 0.15)';
            } else if (isSpecial) {
                nodeSize = 6;
                glowSize = 10;
            } else {
                nodeSize = 4;
                glowSize = 7;
            }
        } else if (isVeryLargeNetwork) {
            nodeSize = isSpecial ? 3 : 2;
            glowSize = 0; // no glow for very large networks
        } else if (isLargeNetwork) {
            nodeSize = (isSpecial || isSubscriber) ? 4 : 3;
            glowSize = 0; // no glow for large networks
        } else {
            nodeSize = (isSpecial || isSubscriber) ? 5 : 4;
            glowSize = isSpecial ? 9 : 7;
        }

        // Build tooltip text
        const peerType = isGateway ? ' (Gateway)' : isYou ? ' (You)' : '';
        const peerIdentifier = peerName || (peer.ip_hash ? `#${peer.ip_hash}` : '');
        let tooltipText = `${id}${peerType}\n${peerIdentifier}\nLocation: ${peer.location.toFixed(4)}`;

        if (lifecycleByPeerId.size > 0) {
            let lifecycleData = peer.peer_id ? lifecycleByPeerId.get(peer.peer_id) : undefined;
            if (!lifecycleData) {
                const topoPeer = initialPeerById.get(id);
                if (topoPeer?.peer_id) {
                    lifecycleData = lifecycleByPeerId.get(topoPeer.peer_id);
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

        peerRenderData.push({
            pos, id, peer, fillColor, glowColor, glowSize, nodeSize,
            isNonSubscriber, isSpecial, isYou, isGateway, isSubscriber,
            label, peerName, peerStateHash, tooltipText,
            isHighlighted, isEventSelected, isPeerSelected, isEventHovered
        });
    });

    // --- Pass 1: Draw all glows (batched) ---
    if (showGlow) {
        for (const d of peerRenderData) {
            if (d.glowSize <= 0) continue;
            ctx.fillStyle = d.glowColor;
            ctx.beginPath();
            ctx.arc(d.pos.x, d.pos.y, d.glowSize, 0, Math.PI * 2);
            ctx.fill();
        }
    }

    // --- Pass 2: Draw all main circles ---
    // For non-subscriber dots with stroke, draw separately
    // For normal dots, batch by fill color
    const colorBatches = new Map();
    const strokeDots = [];

    for (const d of peerRenderData) {
        if (d.isNonSubscriber) {
            strokeDots.push(d);
        } else {
            if (!colorBatches.has(d.fillColor)) colorBatches.set(d.fillColor, []);
            colorBatches.get(d.fillColor).push(d);
        }
    }

    // Draw normal (non-stroke) dots batched by color
    for (const [color, batch] of colorBatches) {
        ctx.fillStyle = color;
        ctx.beginPath();
        for (const d of batch) {
            ctx.moveTo(d.pos.x + d.nodeSize, d.pos.y);
            ctx.arc(d.pos.x, d.pos.y, d.nodeSize, 0, Math.PI * 2);
        }
        ctx.fill();
    }

    // Draw non-subscriber dots (with stroke border)
    for (const d of strokeDots) {
        ctx.fillStyle = d.fillColor;
        ctx.strokeStyle = '#2a2f35';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.arc(d.pos.x, d.pos.y, d.nodeSize, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
    }

    // --- Pass 3: State indicators (small colored squares for contract state) ---
    // Skip when tree overlay is active — uniform peer styling is cleaner
    for (const d of peerRenderData) {
        if (treeOverlay) break;
        if (!d.peerStateHash || !state.selectedContract) continue;
        const stateColors = hashToColor(d.peerStateHash);
        const squareSize = d.nodeSize * 0.7;

        // White border
        ctx.fillStyle = 'white';
        const bx = d.pos.x - squareSize / 2 - 1;
        const by = d.pos.y - squareSize / 2 - 1;
        ctx.fillRect(bx, by, squareSize + 2, squareSize + 2);

        // Colored square
        ctx.fillStyle = stateColors.fill;
        ctx.fillRect(d.pos.x - squareSize / 2, d.pos.y - squareSize / 2, squareSize, squareSize);
    }

    // --- Pass 3.5: Selection ring (visible ring around selected peer) ---
    for (const d of peerRenderData) {
        if (!d.isPeerSelected) continue;
        ctx.strokeStyle = d.isYou ? '#10b981' : '#7ecfef';
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.arc(d.pos.x, d.pos.y, d.nodeSize + 4, 0, Math.PI * 2);
        ctx.stroke();
        // Outer glow ring
        ctx.strokeStyle = d.isYou ? 'rgba(16, 185, 129, 0.3)' : 'rgba(126, 207, 239, 0.3)';
        ctx.lineWidth = 3;
        ctx.beginPath();
        ctx.arc(d.pos.x, d.pos.y, d.nodeSize + 7, 0, Math.PI * 2);
        ctx.stroke();
    }

    // --- Pass 4: Labels (radial, collision-aware) ---
    const MIN_LABEL_ANGLE_GAP = 0.08; // ~29 degrees between labels
    const MAX_LABELS = 12;
    const usedLabelSlots = [];

    // Collect label candidates with priority
    const labelCandidates = [];
    for (const d of peerRenderData) {
        const labelContent = d.label || (showInsideLabels ? (d.peerName || `#${d.peer.ip_hash || d.id.substring(5, 11)}`) : null);
        if (!labelContent) continue;

        const isConnected = connectedToSelected.has(d.id);
        const shouldShow = (
            d.isGateway || d.isYou ||
            d.isPeerSelected ||
            isConnected ||
            (peers.size <= 30 && d.label) ||
            showInsideLabels
        );
        if (!shouldShow) continue;

        const priority = (d.isGateway || d.isYou || d.isPeerSelected) ? 3 : isConnected ? 1 : 2;
        labelCandidates.push({ ...d, labelContent, priority, isConnected });
    }

    // Sort by priority (highest first) so important labels win collision slots
    labelCandidates.sort((a, b) => b.priority - a.priority);

    let labelCount = 0;
    for (const lbl of labelCandidates) {
        if (labelCount >= MAX_LABELS) break;

        // Check collision with already-placed labels
        const loc = lbl.peer.location;
        const tooClose = usedLabelSlots.some(usedLoc => {
            const dist = Math.abs(loc - usedLoc);
            return Math.min(dist, 1 - dist) < MIN_LABEL_ANGLE_GAP;
        });
        if (tooClose) continue;
        usedLabelSlots.push(loc);
        labelCount++;

        // Draw radial label
        const angle = loc * 2 * Math.PI - Math.PI / 2;
        let labelRadius = RADIUS + 18;
        const lx = CENTER + labelRadius * Math.cos(angle);
        const ly = CENTER + labelRadius * Math.sin(angle);
        const onLeft = Math.cos(angle) < 0;
        const rotation = onLeft ? angle + Math.PI : angle;

        ctx.save();
        ctx.translate(lx, ly);
        ctx.rotate(rotation);
        ctx.font = (lbl.isGateway || lbl.isYou || lbl.isPeerSelected)
            ? 'bold 10px "JetBrains Mono", monospace'
            : '500 9px "JetBrains Mono", monospace';
        ctx.fillStyle = lbl.fillColor;
        ctx.globalAlpha = lbl.isConnected ? 0.8 : 1;
        ctx.textAlign = onLeft ? 'end' : 'start';
        ctx.textBaseline = 'middle';
        ctx.fillText(lbl.labelContent, 0, 0);
        ctx.restore();
        ctx.globalAlpha = 1;
    }

    // --- Build hit targets for mouse events ---
    for (const d of peerRenderData) {
        // Skip non-subscribers when contract is selected — they're not interactive
        if (treeOverlay && d.isNonSubscriber) continue;
        canvasHitTargets.push({
            x: d.pos.x,
            y: d.pos.y,
            radius: Math.max(d.nodeSize, 5), // minimum hit radius for tiny dots
            id: d.id,
            peer: d.peer,
            isYou: d.isYou,
            peerName: d.peerName,
            tooltipText: d.tooltipText
        });
    }

    // Contract location marker hit target
    if (state.selectedContract) {
        const contractLoc = contractKeyToLocation(state.selectedContract);
        if (contractLoc !== null) {
            const angle = contractLoc * 2 * Math.PI - Math.PI / 2;
            const markerR = RADIUS + 22;
            const shortKey = state.selectedContract.substring(0, 12) + '...';
            canvasHitTargets.push({
                x: CENTER + markerR * Math.cos(angle),
                y: CENTER + markerR * Math.sin(angle),
                radius: 14,
                id: '__contract_location__',
                peer: null,
                isYou: false,
                peerName: null,
                tooltipText: `Contract location: ${shortKey}\nRing position: ${contractLoc.toFixed(4)}`
            });
        }
    }
}

// ============================================================================
// SVG helpers (center stats, message flow arrows -- few elements, stay in SVG)
// ============================================================================

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

// Helper: Draw message flow arrows (stays in SVG for marker-end support)
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
        arrow.setAttribute('style', `opacity: ${isSelected ? 1 : 0.6 - idx * 0.05}; pointer-events: none;`);
        svg.appendChild(arrow);
    });
}

// ============================================================================
// Distance chart (unchanged -- already uses canvas)
// ============================================================================

// Distance chart state
let distChartZoomed = false;
let distChartInitialized = false;
let lastConnectionDistances = [];

// Helper: Set up dist chart click handler
function setupDistChartZoom() {
    if (distChartInitialized) return;

    const container = document.getElementById('dist-chart-container');
    const backdrop = document.getElementById('transfer-backdrop');

    if (container) {
        container.addEventListener('click', (e) => {
            e.stopPropagation(); // Prevent backdrop click when clicking chart
            distChartZoomed = !distChartZoomed;
            container.classList.toggle('zoomed', distChartZoomed);
            container.title = distChartZoomed ? 'Click to close' : 'Connection distance distribution';
            if (backdrop) backdrop.classList.toggle('visible', distChartZoomed);

            // Re-render immediately
            if (lastConnectionDistances.length > 0) {
                renderDistChart(lastConnectionDistances);
            }
        });

        // Close on backdrop click
        if (backdrop) {
            backdrop.addEventListener('click', () => {
                if (distChartZoomed) {
                    distChartZoomed = false;
                    container.classList.remove('zoomed');
                    container.title = 'Connection distance distribution';
                    backdrop.classList.remove('visible');
                    if (lastConnectionDistances.length > 0) {
                        renderDistChart(lastConnectionDistances);
                    }
                }
            });
        }

        distChartInitialized = true;
    }
}

// Helper: Draw distance distribution chart into HTML overlay
let _lastDistCount = -1;
function drawDistanceChartOverlay(connectionDistances) {
    setupDistChartZoom();
    lastConnectionDistances = connectionDistances;
    // Skip re-render if connection count hasn't changed (distances are stable)
    if (connectionDistances.length === _lastDistCount) return;
    _lastDistCount = connectionDistances.length;
    renderDistChart(connectionDistances);
}

function renderDistChart(connectionDistances) {
    const container = document.getElementById('dist-chart');
    if (!container) return;

    const width = container.offsetWidth || 50;
    const height = container.offsetHeight || 80;

    // Create or reuse canvas
    let canvas = container.querySelector('canvas');
    if (!canvas) {
        canvas = document.createElement('canvas');
        canvas.style.width = '100%';
        canvas.style.height = '100%';
        container.appendChild(canvas);
    }

    const dpr = window.devicePixelRatio || 1;
    const targetW = Math.round(width * dpr);
    const targetH = Math.round(height * dpr);
    if (canvas.width !== targetW || canvas.height !== targetH) {
        canvas.width = targetW;
        canvas.height = targetH;
    }

    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    if (connectionDistances.length === 0) return;

    // Sort ascending — shortest at bottom, longest at top
    const sorted = [...connectionDistances].sort((a, b) => a - b);
    const n = sorted.length;
    const maxDist = sorted[n - 1] || 0.5;

    const pad = distChartZoomed ? 20 : 2;
    const plotW = width - pad * 2;
    const plotH = height - pad * 2;

    // Build line path: x = connection index, y = distance (inverted: 0 at bottom)
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
        const x = pad + (i / (n - 1 || 1)) * plotW;
        const y = pad + plotH - (sorted[i] / maxDist) * plotH;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }

    // Fill under the curve (down to baseline)
    const gradient = ctx.createLinearGradient(0, pad, 0, pad + plotH);
    gradient.addColorStop(0, 'hsla(265, 70%, 65%, 0.35)');
    gradient.addColorStop(1, 'hsla(265, 60%, 45%, 0.08)');
    ctx.lineTo(pad + plotW, pad + plotH);
    ctx.lineTo(pad, pad + plotH);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();

    // Stroke the line
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
        const x = pad + (i / (n - 1 || 1)) * plotW;
        const y = pad + plotH - (sorted[i] / maxDist) * plotH;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = 'hsla(265, 70%, 65%, 0.9)';
    ctx.lineWidth = distChartZoomed ? 2 : 1.5;
    ctx.lineJoin = 'round';
    ctx.stroke();

    // Median line and labels when zoomed
    if (distChartZoomed && n >= 3) {
        const medianDist = sorted[Math.floor(n / 2)];
        const medianY = pad + plotH - (medianDist / maxDist) * plotH;

        ctx.setLineDash([4, 4]);
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.25)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(pad, medianY);
        ctx.lineTo(pad + plotW, medianY);
        ctx.stroke();
        ctx.setLineDash([]);

        // Median label
        ctx.font = '9px "JetBrains Mono", monospace';
        ctx.fillStyle = 'rgba(255, 255, 255, 0.4)';
        ctx.textAlign = 'right';
        ctx.textBaseline = 'bottom';
        ctx.fillText('median ' + medianDist.toFixed(3), pad + plotW, medianY - 3);
    }

    // Axis labels when zoomed
    if (distChartZoomed) {
        ctx.font = '10px "JetBrains Mono", monospace';
        ctx.fillStyle = 'rgba(255,255,255,0.4)';

        // Bottom-left: min distance
        ctx.textAlign = 'left';
        ctx.textBaseline = 'bottom';
        ctx.fillText(sorted[0].toFixed(3), pad, pad + plotH + 14);

        // Top-left: max distance
        ctx.textBaseline = 'top';
        ctx.fillText(maxDist.toFixed(3), pad, pad - 14);

        // Top-right: count
        ctx.textAlign = 'right';
        ctx.textBaseline = 'top';
        ctx.fillText(`${n} conns`, pad + plotW, pad - 14);
    }
}
