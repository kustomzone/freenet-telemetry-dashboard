/**
 * Contract Tree Visualization
 * Renders a top-down hierarchical tree when a contract is selected,
 * showing the subscription/broadcast tree structure with animated messages.
 */

import { state } from './state.js';
import { hashToColor, getEventClass, contractKeyToLocation } from './utils.js';

// ============================================================================
// Module state
// ============================================================================

let treeCanvasEl = null;
let treeTooltipEl = null;
let treeHitTargets = [];
let treeEventsInstalled = false;
let treeCallbacks = {};
let activeAnims = [];
let animFrameId = null;

// Cache to avoid re-layout when data hasn't changed
let lastTreeKey = null;
let lastLayout = null;
let lastTreeData = null;

// ============================================================================
// Tree building
// ============================================================================

/**
 * Build tree structure from contract peer_states, broadcast tree, or
 * inferred from subscribers + network connections.
 * Returns { roots[], children: Map<id, id[]>, allNodes: Set, parentOf: Map }
 */
function buildTree(contractKey, peers, connections) {
    const subData = state.contractData[contractKey];
    if (!subData) return { roots: [], children: new Map(), allNodes: new Set(), parentOf: new Map() };

    const peerStates = subData.peer_states || [];
    const broadcastTree = subData.tree || {};

    // Build peer_id <-> anonId mapping
    const peerIdToAnonId = new Map();
    for (const [anonId, p] of peers) {
        if (p.peer_id) peerIdToAnonId.set(p.peer_id, anonId);
    }

    const children = new Map();  // parent -> [children]
    const parentOf = new Map();  // child -> parent
    const allNodes = new Set();

    if (peerStates.length > 0) {
        // Primary: use peer_states upstream/downstream
        for (const ps of peerStates) {
            const nodeId = peerIdToAnonId.get(ps.peer_id) || ps.peer_id;
            allNodes.add(nodeId);
            if (!children.has(nodeId)) children.set(nodeId, []);

            if (ps.upstream) {
                const parentId = peerIdToAnonId.get(ps.upstream) || ps.upstream;
                allNodes.add(parentId);
                if (!children.has(parentId)) children.set(parentId, []);
                if (!children.get(parentId).includes(nodeId)) {
                    children.get(parentId).push(nodeId);
                }
                parentOf.set(nodeId, parentId);
            }

            if (ps.downstream) {
                for (const ds of ps.downstream) {
                    const childId = peerIdToAnonId.get(ds) || ds;
                    allNodes.add(childId);
                    if (!children.has(childId)) children.set(childId, []);
                    if (!children.get(nodeId).includes(childId)) {
                        children.get(nodeId).push(childId);
                    }
                    if (!parentOf.has(childId)) {
                        parentOf.set(childId, nodeId);
                    }
                }
            }
        }
    } else if (Object.keys(broadcastTree).length > 0) {
        // Fallback 1: use broadcast tree
        for (const [fromId, toIds] of Object.entries(broadcastTree)) {
            const parentId = peerIdToAnonId.get(fromId) || fromId;
            allNodes.add(parentId);
            if (!children.has(parentId)) children.set(parentId, []);

            for (const toId of toIds) {
                const childId = peerIdToAnonId.get(toId) || toId;
                allNodes.add(childId);
                if (!children.has(childId)) children.set(childId, []);
                if (!children.get(parentId).includes(childId)) {
                    children.get(parentId).push(childId);
                }
                if (!parentOf.has(childId)) {
                    parentOf.set(childId, parentId);
                }
            }
        }
    } else {
        // Fallback 2: use subscribers list (anonymized IPs = topology peer IDs)
        // plus contractStates peer_ids, and infer tree from network connections
        const subscribers = subData.subscribers || [];
        for (const subId of subscribers) {
            if (peers.has(subId)) {
                allNodes.add(subId);
                if (!children.has(subId)) children.set(subId, []);
            }
        }
        // Also add contractStates peers (may have peer_id not in subscribers)
        const csData = state.contractStates[contractKey] || {};
        for (const peerId of Object.keys(csData)) {
            const nodeId = peerIdToAnonId.get(peerId) || peerId;
            if (peers.has(nodeId) && !allNodes.has(nodeId)) {
                allNodes.add(nodeId);
                if (!children.has(nodeId)) children.set(nodeId, []);
            }
        }

        // Infer tree edges from network connections between subscribers
        if (allNodes.size > 1 && connections) {
            inferTreeFromConnections(allNodes, children, parentOf, peers, contractKey, connections);
        }
    }

    // Identify roots: nodes with no parent
    const roots = [];
    for (const nodeId of allNodes) {
        if (!parentOf.has(nodeId)) {
            roots.push(nodeId);
        }
    }

    // Sort roots by proximity to contract location (closest = "true root")
    const contractLoc = contractKeyToLocation(contractKey);
    roots.sort((a, b) => {
        const peerA = peers.get(a);
        const peerB = peers.get(b);
        // Seeding peers first (when peer_states available)
        const psA = peerStates.find(ps => (peerIdToAnonId.get(ps.peer_id) || ps.peer_id) === a);
        const psB = peerStates.find(ps => (peerIdToAnonId.get(ps.peer_id) || ps.peer_id) === b);
        const seedA = psA?.is_seeding ? 1 : 0;
        const seedB = psB?.is_seeding ? 1 : 0;
        if (seedA !== seedB) return seedB - seedA;
        // Then by proximity to contract location
        if (contractLoc !== null && peerA && peerB) {
            const distA = Math.min(Math.abs(peerA.location - contractLoc), 1 - Math.abs(peerA.location - contractLoc));
            const distB = Math.min(Math.abs(peerB.location - contractLoc), 1 - Math.abs(peerB.location - contractLoc));
            return distA - distB;
        }
        return 0;
    });

    return { roots, children, allNodes, parentOf };
}

/**
 * Build a spanning tree from network connections between subscriber nodes.
 * Uses BFS from the node closest to contract location (approximate root).
 * This gives a reasonable tree approximation when no explicit tree data exists.
 */
function inferTreeFromConnections(allNodes, children, parentOf, peers, contractKey, connections) {
    // Build adjacency among subscriber nodes only
    const adj = new Map();
    for (const nodeId of allNodes) {
        adj.set(nodeId, []);
    }
    connections.forEach(connKey => {
        const [id1, id2] = connKey.split('|');
        if (allNodes.has(id1) && allNodes.has(id2)) {
            adj.get(id1).push(id2);
            adj.get(id2).push(id1);
        }
    });

    // Find the best root: node closest to contract location
    const contractLoc = contractKeyToLocation(contractKey);
    let bestRoot = [...allNodes][0];
    if (contractLoc !== null) {
        let bestDist = Infinity;
        for (const nodeId of allNodes) {
            const peer = peers.get(nodeId);
            if (!peer) continue;
            const dist = Math.min(
                Math.abs(peer.location - contractLoc),
                1 - Math.abs(peer.location - contractLoc)
            );
            if (dist < bestDist) {
                bestDist = dist;
                bestRoot = nodeId;
            }
        }
    }

    // BFS from root to build spanning tree
    const visited = new Set();
    const queue = [bestRoot];
    visited.add(bestRoot);

    while (queue.length > 0) {
        const node = queue.shift();
        const neighbors = adj.get(node) || [];
        for (const neighbor of neighbors) {
            if (visited.has(neighbor)) continue;
            visited.add(neighbor);
            // Add tree edge: node -> neighbor
            if (!children.get(node).includes(neighbor)) {
                children.get(node).push(neighbor);
            }
            parentOf.set(neighbor, node);
            queue.push(neighbor);
        }
    }
}

// ============================================================================
// Connectivity segmentation
// ============================================================================

function findSegments(roots, children, allNodes) {
    const visited = new Set();
    const segments = [];

    // BFS from each root to find its connected segment
    for (const root of roots) {
        if (visited.has(root)) continue;
        const segment = [];
        const queue = [root];
        while (queue.length > 0) {
            const node = queue.shift();
            if (visited.has(node)) continue;
            visited.add(node);
            segment.push(node);
            const kids = children.get(node) || [];
            for (const kid of kids) {
                if (!visited.has(kid)) queue.push(kid);
            }
        }
        segments.push(segment);
    }

    // Pick up any orphans not reachable from roots
    for (const node of allNodes) {
        if (!visited.has(node)) {
            visited.add(node);
            segments.push([node]);
        }
    }

    // Sort segments: largest first
    segments.sort((a, b) => b.length - a.length);
    return segments;
}

// ============================================================================
// Layout
// ============================================================================

/**
 * Arrange flat nodes (no edges) in a grid layout.
 */
function layoutFlat(allNodes, canvasWidth, canvasHeight) {
    const layout = new Map();
    const nodes = [...allNodes];
    const PADDING_X = 60;
    const PADDING_TOP = 60;
    const SPACING_X = 80;
    const SPACING_Y = 70;

    const cols = Math.max(1, Math.floor((canvasWidth - 2 * PADDING_X) / SPACING_X));
    const rows = Math.ceil(nodes.length / cols);

    // Center the grid
    const gridWidth = Math.min(nodes.length, cols) * SPACING_X;
    const startX = (canvasWidth - gridWidth) / 2 + SPACING_X / 2;

    for (let i = 0; i < nodes.length; i++) {
        const col = i % cols;
        const row = Math.floor(i / cols);
        layout.set(nodes[i], {
            x: startX + col * SPACING_X,
            y: PADDING_TOP + row * SPACING_Y,
            depth: 0,
            segmentIndex: 0
        });
    }

    return {
        layout,
        segments: [{ nodes: allNodes, segmentIndex: 0, roots: nodes, depth: new Map(), maxDepth: 0, subtreeWidth: new Map(), totalWidth: nodes.length }],
        overallMaxDepth: 0,
        isFlat: true
    };
}

/**
 * Assign (x, y) positions using BFS layering with subtree width allocation.
 * Returns Map<nodeId, {x, y, depth, segmentIndex}>
 */
function layoutTree(roots, children, allNodes, canvasWidth, canvasHeight) {
    // Detect "flat" mode: all nodes are roots with no children (no tree structure)
    const hasEdges = [...children.values()].some(kids => kids.length > 0);
    if (!hasEdges && allNodes.size > 0) {
        return layoutFlat(allNodes, canvasWidth, canvasHeight);
    }

    const segments = findSegments(roots, children, allNodes);
    const layout = new Map();

    const PADDING_X = 30;
    const PADDING_TOP = 30;
    const PADDING_BOTTOM = 20;
    const SEGMENT_GAP = 30;

    // First pass: compute max depth for each segment and subtree widths
    const segmentLayouts = [];

    for (let si = 0; si < segments.length; si++) {
        const segNodes = new Set(segments[si]);

        // Segment roots = nodes in segment with no parent in the same segment
        const actualRoots = segments[si].filter(n => {
            for (const [parent, kids] of children) {
                if (kids.includes(n) && segNodes.has(parent)) return false;
            }
            return true;
        });

        // BFS to assign depths
        const depth = new Map();
        const queue = [];
        for (const r of actualRoots) {
            depth.set(r, 0);
            queue.push(r);
        }
        let maxDepth = 0;
        while (queue.length > 0) {
            const node = queue.shift();
            const d = depth.get(node);
            maxDepth = Math.max(maxDepth, d);
            const kids = (children.get(node) || []).filter(k => segNodes.has(k));
            for (const kid of kids) {
                if (!depth.has(kid)) {
                    depth.set(kid, d + 1);
                    queue.push(kid);
                }
            }
        }

        // Bottom-up subtree widths
        const subtreeWidth = new Map();
        function computeWidth(node) {
            const kids = (children.get(node) || []).filter(k => segNodes.has(k) && depth.has(k) && depth.get(k) > depth.get(node));
            if (kids.length === 0) {
                subtreeWidth.set(node, 1);
                return 1;
            }
            let w = 0;
            for (const kid of kids) {
                w += computeWidth(kid);
            }
            subtreeWidth.set(node, w);
            return w;
        }
        for (const r of actualRoots) {
            computeWidth(r);
        }

        let totalWidth = 0;
        for (const r of actualRoots) {
            totalWidth += subtreeWidth.get(r) || 1;
        }

        segmentLayouts.push({
            roots: actualRoots,
            nodes: segNodes,
            depth: depth,
            maxDepth,
            subtreeWidth,
            totalWidth,
            segmentIndex: si
        });
    }

    // Compute overall max depth for consistent layer height
    const overallMaxDepth = Math.max(1, ...segmentLayouts.map(s => s.maxDepth));

    // Adaptive spacing: scale to fit canvas
    const totalWidthUnits = segmentLayouts.reduce((sum, s) => sum + s.totalWidth, 0);
    const availableWidth = canvasWidth - 2 * PADDING_X - Math.max(0, segments.length - 1) * SEGMENT_GAP;
    const availableHeight = canvasHeight - PADDING_TOP - PADDING_BOTTOM;

    // Clamp unit width and layer height so tree fits
    const unitWidth = Math.max(8, Math.min(50, availableWidth / Math.max(1, totalWidthUnits)));
    const layerH = Math.max(20, Math.min(55, availableHeight / (overallMaxDepth + 1)));

    // Center the tree horizontally
    const totalTreeWidth = totalWidthUnits * unitWidth + Math.max(0, segments.length - 1) * SEGMENT_GAP;
    const offsetX = PADDING_X + Math.max(0, (availableWidth - totalTreeWidth + Math.max(0, segments.length - 1) * SEGMENT_GAP) / 2);

    // Assign positions
    let segmentX = offsetX;
    for (const seg of segmentLayouts) {
        const segWidth = seg.totalWidth * unitWidth;

        function assignPositions(node, xStart, xEnd) {
            const d = seg.depth.get(node);
            const x = (xStart + xEnd) / 2;
            const y = PADDING_TOP + d * layerH;
            layout.set(node, { x, y, depth: d, segmentIndex: seg.segmentIndex });

            const kids = (children.get(node) || []).filter(k =>
                seg.nodes.has(k) && seg.depth.has(k) && seg.depth.get(k) > d
            );
            if (kids.length === 0) return;

            let cx = xStart;
            for (const kid of kids) {
                const kidW = (seg.subtreeWidth.get(kid) || 1) * unitWidth;
                assignPositions(kid, cx, cx + kidW);
                cx += kidW;
            }
        }

        let rx = segmentX;
        for (const root of seg.roots) {
            const rootW = (seg.subtreeWidth.get(root) || 1) * unitWidth;
            assignPositions(root, rx, rx + rootW);
            rx += rootW;
        }
        segmentX += segWidth + SEGMENT_GAP;
    }

    return { layout, segments: segmentLayouts, overallMaxDepth };
}

// ============================================================================
// Canvas rendering
// ============================================================================

const EVENT_COLORS = {
    connect: '#7ecfef',
    put: '#fbbf24',
    get: '#34d399',
    update: '#a78bfa',
    subscribe: '#f472b6',
    other: '#9ca3af'
};

function drawContractTree(ctx, layoutData, treeData, peers, subscriberPeerIds, canvasWidth, canvasHeight) {
    const { layout, segments } = layoutData;
    const { children, parentOf, roots } = treeData;

    treeHitTargets = [];

    // Build lookup maps
    const lifecycleByPeerId = new Map();
    if (state.peerLifecycle?.peers) {
        for (const p of state.peerLifecycle.peers) {
            if (p.peer_id) lifecycleByPeerId.set(p.peer_id, p);
        }
    }

    // Pass 1: Edges
    ctx.save();
    for (const [nodeId, pos] of layout) {
        const kids = children.get(nodeId) || [];
        for (const kid of kids) {
            const kidPos = layout.get(kid);
            if (!kidPos) continue;

            ctx.beginPath();
            ctx.moveTo(pos.x, pos.y);
            // Slight curve for readability
            const midY = (pos.y + kidPos.y) / 2;
            ctx.bezierCurveTo(pos.x, midY, kidPos.x, midY, kidPos.x, kidPos.y);
            ctx.strokeStyle = 'rgba(126, 207, 239, 0.35)';
            ctx.lineWidth = 1.5;
            ctx.stroke();
        }
    }
    ctx.restore();

    // Pass 2: Message animations
    const now = performance.now();
    ctx.save();
    for (let i = activeAnims.length - 1; i >= 0; i--) {
        const anim = activeAnims[i];
        const elapsed = now - anim.startTime;
        const duration = 800;
        if (elapsed > duration) {
            activeAnims.splice(i, 1);
            continue;
        }
        const t = elapsed / duration;
        const fromPos = layout.get(anim.fromId);
        const toPos = layout.get(anim.toId);
        if (!fromPos || !toPos) continue;

        const x = fromPos.x + (toPos.x - fromPos.x) * t;
        const y = fromPos.y + (toPos.y - fromPos.y) * t;
        const alpha = 1 - t * 0.5;

        ctx.beginPath();
        ctx.arc(x, y, 4, 0, Math.PI * 2);
        ctx.fillStyle = anim.color;
        ctx.globalAlpha = alpha;
        ctx.fill();

        // Glow
        ctx.beginPath();
        ctx.arc(x, y, 8, 0, Math.PI * 2);
        ctx.fillStyle = anim.color;
        ctx.globalAlpha = alpha * 0.3;
        ctx.fill();
    }
    ctx.globalAlpha = 1;
    ctx.restore();

    // Pass 3: Nodes
    // Scale node size based on node count
    const nodeCount = layout.size;
    const NODE_RADIUS = nodeCount > 60 ? 4 : nodeCount > 30 ? 6 : 8;
    const SHOW_LABELS = nodeCount <= 40;
    const peerStates = state.contractData[state.selectedContract]?.peer_states || [];

    for (const [nodeId, pos] of layout) {
        const peer = peers.get(nodeId);
        const peerId = peer?.peer_id;
        const lifecyclePeer = peerId ? lifecycleByPeerId.get(peerId) : undefined;
        const isGateway = peer?.is_gateway || lifecyclePeer?.is_gateway || nodeId === state.gatewayPeerId;
        const isYou = nodeId === state.yourPeerId;
        const isRoot = roots.includes(nodeId) || !parentOf.has(nodeId);
        const isPeerSelected = state.selectedPeerId === nodeId;
        const ps = peerStates.find(p => (peer?.peer_id && p.peer_id === peer.peer_id));
        const isSeeding = ps?.is_seeding;

        // Determine color
        let fillColor = '#f472b6'; // default coral/pink for subscriber
        let glowColor = 'rgba(244, 114, 182, 0.3)';

        if (isGateway) {
            fillColor = '#f59e0b';
            glowColor = 'rgba(245, 158, 11, 0.4)';
        } else if (isYou) {
            fillColor = '#10b981';
            glowColor = 'rgba(16, 185, 129, 0.4)';
        }

        if (isPeerSelected) {
            fillColor = '#7ecfef';
            glowColor = 'rgba(126, 207, 239, 0.5)';
        }

        // State hash convergence tinting
        let stateHash = null;
        if (state.contractStates[state.selectedContract] && peerId) {
            const peerState = state.contractStates[state.selectedContract][peerId];
            if (peerState?.hash) stateHash = peerState.hash;
        }

        // Glow (skip for very small nodes)
        if (NODE_RADIUS >= 6) {
            ctx.save();
            ctx.beginPath();
            ctx.arc(pos.x, pos.y, NODE_RADIUS + 3, 0, Math.PI * 2);
            ctx.fillStyle = glowColor;
            ctx.fill();
            ctx.restore();
        }

        // Main circle
        ctx.beginPath();
        ctx.arc(pos.x, pos.y, NODE_RADIUS, 0, Math.PI * 2);
        ctx.fillStyle = fillColor;
        ctx.fill();

        // State hash indicator (small colored square)
        if (stateHash) {
            const hc = hashToColor(stateHash);
            if (hc) {
                ctx.save();
                const sq = 5;
                ctx.fillStyle = hc.fill;
                ctx.fillRect(pos.x - sq / 2, pos.y - sq / 2, sq, sq);
                ctx.strokeStyle = '#fff';
                ctx.lineWidth = 0.5;
                ctx.strokeRect(pos.x - sq / 2, pos.y - sq / 2, sq, sq);
                ctx.restore();
            }
        }

        // Root star marker (only show if tree has actual edges, otherwise all are roots)
        const treeHasEdges = parentOf.size > 0;
        if (isRoot && treeHasEdges && (isSeeding || !parentOf.has(nodeId))) {
            ctx.save();
            ctx.font = '10px sans-serif';
            ctx.fillStyle = '#fbbf24';
            ctx.textAlign = 'center';
            ctx.fillText('\u2605', pos.x, pos.y - NODE_RADIUS - 4);
            ctx.restore();
        }

        // Selection ring
        if (isPeerSelected) {
            ctx.save();
            ctx.beginPath();
            ctx.arc(pos.x, pos.y, NODE_RADIUS + 2, 0, Math.PI * 2);
            ctx.strokeStyle = '#fff';
            ctx.lineWidth = 2;
            ctx.stroke();
            ctx.restore();
        }

        // Pass 4: Label (only for small trees or special nodes)
        const peerName = peer?.ip_hash ? state.peerNames[peer.ip_hash] : null;
        const isSpecial = isYou || isGateway || isPeerSelected || peerName;
        if (SHOW_LABELS || isSpecial) {
            let labelText = '';
            if (isYou) labelText = peerName || 'YOU';
            else if (isGateway) labelText = peerName || 'GW';
            else if (peerName) labelText = peerName;
            else if (SHOW_LABELS) labelText = nodeId.substring(0, 6);

            if (labelText) {
                ctx.save();
                ctx.font = `${NODE_RADIUS >= 6 ? 9 : 7}px "JetBrains Mono", monospace`;
                ctx.fillStyle = 'rgba(230, 237, 243, 0.7)';
                ctx.textAlign = 'center';
                ctx.fillText(labelText, pos.x, pos.y + NODE_RADIUS + 10);
                ctx.restore();
            }
        }

        // Build hit target
        const peerType = isGateway ? ' (Gateway)' : isYou ? ' (You)' : '';
        const peerIdentifier = peerName || (peer?.ip_hash ? `#${peer.ip_hash}` : '');
        let tooltipText = `${nodeId}${peerType}\n${peerIdentifier}`;
        if (peer?.location !== undefined) tooltipText += `\nLocation: ${peer.location.toFixed(4)}`;
        if (isSeeding) tooltipText += '\nSeeding (root)';
        if (ps?.downstream_count) tooltipText += `\nDownstream: ${ps.downstream_count}`;
        if (stateHash) tooltipText += `\nState: ${stateHash.substring(0, 8)}...`;

        treeHitTargets.push({
            x: pos.x, y: pos.y, radius: NODE_RADIUS + 4,
            id: nodeId, peer, isYou, peerName, tooltipText
        });
    }

    // Pass 5: Disconnected segment indicators
    if (segments.length > 1) {
        ctx.save();
        for (let si = 1; si < segments.length; si++) {
            const seg = segments[si];
            // Find bounding box of this segment's nodes
            let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
            for (const nodeId of seg.nodes) {
                const pos = layout.get(nodeId);
                if (!pos) continue;
                minX = Math.min(minX, pos.x);
                maxX = Math.max(maxX, pos.x);
                minY = Math.min(minY, pos.y);
                maxY = Math.max(maxY, pos.y);
            }
            if (minX === Infinity) continue;

            const pad = 20;
            ctx.setLineDash([4, 4]);
            ctx.strokeStyle = 'rgba(248, 113, 113, 0.5)';
            ctx.lineWidth = 1;
            ctx.strokeRect(minX - pad, minY - pad, maxX - minX + 2 * pad, maxY - minY + 2 * pad);
            ctx.setLineDash([]);

            // "disconnected" label
            ctx.font = '8px "JetBrains Mono", monospace';
            ctx.fillStyle = 'rgba(248, 113, 113, 0.6)';
            ctx.textAlign = 'center';
            ctx.fillText('disconnected', (minX + maxX) / 2, minY - pad - 4);
        }
        ctx.restore();
    }

    // Schedule next frame if animations are active
    if (activeAnims.length > 0 && !animFrameId) {
        scheduleAnimFrame(ctx, layoutData, treeData, peers, subscriberPeerIds, canvasWidth, canvasHeight);
    }
}

function scheduleAnimFrame(ctx, layoutData, treeData, peers, subscriberPeerIds, canvasWidth, canvasHeight) {
    animFrameId = requestAnimationFrame(() => {
        animFrameId = null;
        if (activeAnims.length === 0) return;

        const dpr = window.devicePixelRatio || 1;
        const canvas = treeCanvasEl;
        if (!canvas) return;

        const scale = canvasWidth / canvasWidth; // identity for tree (1:1 logical)
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, canvasWidth, canvasHeight);
        drawContractTree(ctx, layoutData, treeData, peers, subscriberPeerIds, canvasWidth, canvasHeight);
    });
}

// ============================================================================
// Canvas & event management
// ============================================================================

function getOrCreateTreeCanvas(container) {
    if (treeCanvasEl && treeCanvasEl.parentNode === container) {
        return treeCanvasEl;
    }
    if (treeCanvasEl) treeCanvasEl.remove();

    treeCanvasEl = document.createElement('canvas');
    treeCanvasEl.id = 'tree-canvas';
    treeCanvasEl.style.maxWidth = '100%';
    container.style.position = 'relative';
    container.appendChild(treeCanvasEl);
    return treeCanvasEl;
}

function getOrCreateTreeTooltip(container) {
    if (treeTooltipEl && treeTooltipEl.parentNode === container) {
        return treeTooltipEl;
    }
    if (treeTooltipEl) treeTooltipEl.remove();

    treeTooltipEl = document.createElement('div');
    treeTooltipEl.id = 'tree-canvas-tooltip';
    treeTooltipEl.style.cssText = `
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
    container.appendChild(treeTooltipEl);
    return treeTooltipEl;
}

function findTreeHitTarget(mx, my) {
    const HIT_RADIUS = 16;
    let closest = null;
    let closestDist = HIT_RADIUS;

    for (let i = treeHitTargets.length - 1; i >= 0; i--) {
        const t = treeHitTargets[i];
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

function installTreeEvents(canvas, container) {
    if (treeEventsInstalled) return;
    treeEventsInstalled = true;

    const tooltip = getOrCreateTreeTooltip(container);

    canvas.addEventListener('mousemove', (e) => {
        const rect = canvas.getBoundingClientRect();
        const dpr = window.devicePixelRatio || 1;
        const scaleX = (canvas.width / dpr) / rect.width;
        const scaleY = (canvas.height / dpr) / rect.height;
        const mx = (e.clientX - rect.left) * scaleX;
        const my = (e.clientY - rect.top) * scaleY;

        const hit = findTreeHitTarget(mx, my);
        if (hit) {
            canvas.style.cursor = 'pointer';
            tooltip.style.display = 'block';
            tooltip.textContent = hit.tooltipText;
            const containerRect = container.getBoundingClientRect();
            let tipX = e.clientX - containerRect.left + 12;
            let tipY = e.clientY - containerRect.top - 10;
            const tipW = tooltip.offsetWidth;
            const tipH = tooltip.offsetHeight;
            if (tipX + tipW > containerRect.width) tipX = tipX - tipW - 24;
            if (tipY + tipH > containerRect.height) tipY = containerRect.height - tipH - 4;
            if (tipY < 0) tipY = 4;
            tooltip.style.left = tipX + 'px';
            tooltip.style.top = tipY + 'px';
        } else {
            canvas.style.cursor = '';
            tooltip.style.display = 'none';
        }
    });

    canvas.addEventListener('mouseleave', () => {
        canvas.style.cursor = '';
        tooltip.style.display = 'none';
    });

    canvas.addEventListener('click', (e) => {
        const rect = canvas.getBoundingClientRect();
        const dpr = window.devicePixelRatio || 1;
        const scaleX = (canvas.width / dpr) / rect.width;
        const scaleY = (canvas.height / dpr) / rect.height;
        const mx = (e.clientX - rect.left) * scaleX;
        const my = (e.clientY - rect.top) * scaleY;

        const hit = findTreeHitTarget(mx, my);
        if (hit) {
            const { selectPeer } = treeCallbacks;
            if (selectPeer) selectPeer(hit.id);
        } else if (state.selectedPeerId) {
            const { selectPeer } = treeCallbacks;
            if (selectPeer) selectPeer(state.selectedPeerId); // toggle off
        }
    });
}

// ============================================================================
// Message animation trigger (called from event processing)
// ============================================================================

export function triggerTreeMessageAnim(fromId, toId, eventType) {
    const color = EVENT_COLORS[getEventClass(eventType)] || EVENT_COLORS.other;
    activeAnims.push({
        fromId, toId,
        color,
        startTime: performance.now()
    });
}

// ============================================================================
// Main entry point
// ============================================================================

export function updateContractTree(container, peers, connections, subscriberPeerIds, callbacks) {
    treeCallbacks = callbacks;

    const contractKey = state.selectedContract;
    if (!contractKey || !state.contractData[contractKey]) return;

    // Compute a cache key from contract data to avoid unnecessary re-layouts
    const subData = state.contractData[contractKey];
    const peerStates = subData.peer_states || [];
    const subs = subData.subscribers || [];
    const cacheKey = `${contractKey}:${peerStates.length}:${Object.keys(subData.tree || {}).length}:${subs.length}:${peers.size}`;

    const canvas = getOrCreateTreeCanvas(container);
    const dpr = window.devicePixelRatio || 1;
    const displayWidth = container.offsetWidth || 600;
    const minHeight = 480;

    // Build tree and layout (using min height for initial layout)
    let treeData, layoutData;
    if (cacheKey === lastTreeKey && lastLayout && lastTreeData) {
        treeData = lastTreeData;
        layoutData = lastLayout;
    } else {
        treeData = buildTree(contractKey, peers, connections);
        layoutData = layoutTree(treeData.roots, treeData.children, treeData.allNodes, displayWidth, minHeight);
        lastTreeKey = cacheKey;
        lastLayout = layoutData;
        lastTreeData = treeData;
    }

    if (treeData.allNodes.size === 0) {
        canvas.width = Math.round(displayWidth * dpr);
        canvas.height = Math.round(minHeight * dpr);
        canvas.style.width = displayWidth + 'px';
        canvas.style.height = minHeight + 'px';
        const ctx = canvas.getContext('2d');
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, displayWidth, minHeight);
        ctx.font = '14px "Space Grotesk", sans-serif';
        ctx.fillStyle = 'rgba(230, 237, 243, 0.5)';
        ctx.textAlign = 'center';
        ctx.fillText('No subscription tree data for this contract', displayWidth / 2, minHeight / 2);
        return;
    }

    // Compute actual needed height from layout positions
    let maxY = 0;
    for (const pos of layoutData.layout.values()) {
        maxY = Math.max(maxY, pos.y);
    }
    const displayHeight = Math.max(minHeight, maxY + 50);

    // If needed height exceeds min, re-layout with correct height
    if (displayHeight > minHeight && cacheKey !== lastTreeKey + ':resized') {
        layoutData = layoutTree(treeData.roots, treeData.children, treeData.allNodes, displayWidth, displayHeight);
        lastLayout = layoutData;
        // Recompute maxY after relayout
        maxY = 0;
        for (const pos of layoutData.layout.values()) {
            maxY = Math.max(maxY, pos.y);
        }
    }

    const finalHeight = Math.max(minHeight, maxY + 50);
    const targetW = Math.round(displayWidth * dpr);
    const targetH = Math.round(finalHeight * dpr);
    if (canvas.width !== targetW || canvas.height !== targetH) {
        canvas.width = targetW;
        canvas.height = targetH;
        canvas.style.width = displayWidth + 'px';
        canvas.style.height = finalHeight + 'px';
    }

    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, displayWidth, finalHeight);

    drawContractTree(ctx, layoutData, treeData, peers, subscriberPeerIds, displayWidth, finalHeight);
    installTreeEvents(canvas, container);

    // Remove empty state if present
    const emptyState = container.querySelector('.empty-state');
    if (emptyState) emptyState.remove();
}

/**
 * Get tree stats for subtitle display.
 */
export function getTreeStats(contractKey, peers, connections) {
    if (!contractKey || !state.contractData[contractKey]) {
        return { nodeCount: 0, depth: 0, segments: 0, isFlat: false };
    }
    const treeData = buildTree(contractKey, peers, connections);
    if (treeData.allNodes.size === 0) {
        return { nodeCount: 0, depth: 0, segments: 0, isFlat: false };
    }
    const layoutData = layoutTree(treeData.roots, treeData.children, treeData.allNodes, 600, 480);
    return {
        nodeCount: treeData.allNodes.size,
        depth: layoutData.overallMaxDepth + 1,
        segments: layoutData.segments.length,
        isFlat: !!layoutData.isFlat
    };
}

/**
 * Reset module state (call when switching away from tree view).
 */
export function resetContractTree() {
    activeAnims = [];
    if (animFrameId) {
        cancelAnimationFrame(animFrameId);
        animFrameId = null;
    }
    lastTreeKey = null;
    lastLayout = null;
    lastTreeData = null;
}
