// Node Visualizer for TunnelMesh Dashboard
// Shows mesh topology with selected node centered

// Import utilities from TM modules
const { extractRegion } = TM.utils;
const { formatBytesCompact, formatLatencyCompact } = TM.format;

// =============================================================================
// Constants and Types
// =============================================================================

const NodeType = {
    STANDARD: 'standard',
    WG_CONCENTRATOR: 'wg_concentrator',
    EXIT_NODE: 'exit_node',
};

const CARD_WIDTH = 210;
const CARD_HEIGHT = 80;
const TAB_HEIGHT = 22;
const ROW_SPACING = 110; // Vertical spacing between spread nodes
const CONNECTION_DOT_RADIUS = 5;
const MAX_VISIBLE_NODES = 3; // Show max 3 nodes per column, then "+ N more"

// Content dimension bounds (content size scales with canvas, within these limits)
const MIN_CONTENT_WIDTH = 1200;
const MAX_CONTENT_WIDTH = 2200;
const MIN_CONTENT_HEIGHT = 350;
const MAX_CONTENT_HEIGHT = 500;

// Helper to read CSS variables with fallback
function getCSSVar(name, fallback) {
    if (typeof getComputedStyle === 'undefined') return fallback;
    const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return value || fallback;
}

// Colors matching dashboard theme - read from CSS variables for consistency
// Uses CSS variables from :root in style.css with fallbacks for compatibility
const COLORS = {
    get background() {
        return getCSSVar('--color-bg-primary', '#0d1117');
    },
    get cardFill() {
        return getCSSVar('--color-bg-tertiary', '#21262d');
    },
    cardStroke: '#484f58', // Light grey stroke for all nodes
    get text() {
        return getCSSVar('--color-text-primary', '#e6edf3');
    },
    get textDim() {
        return getCSSVar('--color-text-secondary', '#8b949e');
    },
    get connection() {
        return getCSSVar('--color-border-primary', '#30363d');
    },
    get connectionHighlight() {
        return getCSSVar('--color-accent-blue', '#58a6ff');
    },
    exitConnection: '#f0a500', // Golden color for exit path connections
    // Transport type badge colors
    get transportSSH() {
        return getCSSVar('--color-accent-green', '#3fb950');
    },
    get transportUDP() {
        return getCSSVar('--color-accent-blue', '#58a6ff');
    },
    get transportRelay() {
        return getCSSVar('--color-accent-orange', '#d29922');
    },
};

// =============================================================================
// VisualizerNode Class
// =============================================================================

class VisualizerNode {
    constructor(peer, domainSuffix, nodeType = NodeType.STANDARD) {
        // Identity from peer data
        this.id = peer.name;
        this.name = peer.name;
        this.meshIP = peer.mesh_ip;
        this.online = peer.online;
        this.connectable = peer.connectable;
        this.behindNAT = peer.behind_nat;
        this.activeTunnels = peer.stats?.active_tunnels ?? 0;
        this.coordinatorRTTMs = peer.coordinator_rtt_ms ?? 0;
        this.version = peer.version || '';
        this.nodeType = nodeType;

        // Throughput stats
        this.bytesSentRate = peer.bytes_sent_rate || 0;
        this.bytesReceivedRate = peer.bytes_received_rate || 0;

        // Location info (region/city) - use shortest available
        this.region = extractRegion(peer);

        // Build DNS name with truncation
        const fullDns = peer.name + (domainSuffix || '');
        this.dnsName = fullDns.length > 25 ? `${fullDns.substring(0, 22)}...` : fullDns;

        // Exit node info
        this.exitNode = peer.exit_node || ''; // Name of peer this node uses as exit
        this.allowsExitTraffic = peer.allows_exit_traffic || false; // Whether this node can be exit
        this.exitClients = peer.exit_clients || []; // Clients using this as exit

        // Connection types (peer -> transport type)
        this.connections = peer.connections || {};

        // Layout positions
        this.x = 0;
        this.y = 0;
        this.targetX = 0;
        this.targetY = 0;

        // Visual state
        this.selected = false;
        this.hovered = false;
        this.stackIndex = 0;
        this.stackSize = 1;
        this.visible = true; // Whether to render this node
    }

    get category() {
        return `${this.nodeType}_${this.connectable}_${this.online}`;
    }
}

// =============================================================================
// Core Logic Functions
// =============================================================================

// =============================================================================
// Visual Slot - represents a node's position on one side (left or right)
// =============================================================================

class VisualSlot {
    constructor(node, side) {
        this.node = node;
        this.side = side; // 'left', 'center', or 'right'
        this.x = 0;
        this.y = 0;
        this.startX = 0;
        this.startY = 0;
        this.targetX = 0;
        this.targetY = 0;
        this.visible = true;
    }

    get id() {
        return `${this.node.id}_${this.side}`;
    }
}

// =============================================================================
// Layout Algorithm
// Selected node centered, connected peers distributed evenly on both sides
// =============================================================================

function calculateLayout(nodes, selectedId, canvasWidth, canvasHeight, stackInfo, slots) {
    const centerX = canvasWidth / 2;
    const centerY = canvasHeight / 2;

    // Dynamic column spacing based on canvas width (adjust to fit)
    // Increased spacing to move nodes further from center
    const columnSpacing = Math.min(500, (canvasWidth - CARD_WIDTH * 3) / 2);

    // Reset stack info
    stackInfo.left = { total: 0, hidden: 0 };
    stackInfo.right = { total: 0, hidden: 0 };
    stackInfo.columnSpacing = columnSpacing;

    // Clear old slots
    slots.length = 0;

    if (!selectedId || !nodes.has(selectedId)) {
        return;
    }

    const selectedNode = nodes.get(selectedId);

    // Create center slot for selected node
    const centerSlot = new VisualSlot(selectedNode, 'center');
    centerSlot.targetX = centerX;
    centerSlot.targetY = centerY;
    slots.push(centerSlot);

    // Collect all connected (online) peers
    const connected = [];

    for (const [id, node] of nodes) {
        if (id === selectedId) continue;
        if (!node.online) continue;
        connected.push(node);
    }

    // Sort: exit nodes first (selected's exit node, then other exit-capable nodes), then by name
    connected.sort((a, b) => {
        // Selected's exit node goes first
        if (selectedNode.exitNode) {
            if (a.name === selectedNode.exitNode) return -1;
            if (b.name === selectedNode.exitNode) return 1;
        }
        // Exit-capable nodes come next
        if (a.allowsExitTraffic && !b.allowsExitTraffic) return -1;
        if (!a.allowsExitTraffic && b.allowsExitTraffic) return 1;
        // Then sort by name
        return a.name.localeCompare(b.name);
    });

    // Split connected nodes evenly between right and left sides (index 0 on right)
    const midpoint = Math.ceil(connected.length / 2);
    const rightNodes = connected.slice(0, midpoint);
    const leftNodes = connected.slice(midpoint);

    // Create slots for each column
    layoutColumn(leftNodes, centerX - columnSpacing, centerY, stackInfo.left, slots, 'left');
    layoutColumn(rightNodes, centerX + columnSpacing, centerY, stackInfo.right, slots, 'right');
}

function layoutColumn(nodes, centerX, centerY, stackInfo, slots, side) {
    if (nodes.length === 0) return;

    stackInfo.total = nodes.length;

    const visibleCount = Math.min(nodes.length, MAX_VISIBLE_NODES);
    stackInfo.hidden = nodes.length - visibleCount;

    const totalHeight = (visibleCount - 1) * ROW_SPACING;
    let currentY = centerY - totalHeight / 2;

    // Elliptical offset - nodes at top/bottom curve inward
    const ellipseWidth = 40; // Max horizontal offset for ellipse effect

    for (let i = 0; i < visibleCount; i++) {
        const node = nodes[i];
        const slot = new VisualSlot(node, side);

        // Calculate elliptical X offset based on Y distance from center
        // Nodes at centerY get max offset, nodes away from center curve inward
        const yDistFromCenter = Math.abs(currentY - centerY);
        const maxYDist = totalHeight / 2 || 1;
        const normalizedDist = yDistFromCenter / maxYDist; // 0 at center, 1 at edges
        const ellipseOffset = ellipseWidth * (1 - normalizedDist * normalizedDist);

        // Apply offset away from center (left side goes more left, right side goes more right)
        const offsetDir = side === 'left' ? -1 : 1;
        slot.targetX = centerX + offsetDir * ellipseOffset;
        slot.targetY = currentY;
        slots.push(slot);
        currentY += ROW_SPACING;
    }
}

// =============================================================================
// NodeVisualizer Class
// =============================================================================

class NodeVisualizer {
    constructor(canvas) {
        this.canvas = canvas;
        this.ctx = canvas.getContext('2d');

        // Data
        this.nodes = new Map();
        this.slots = []; // Visual slots for rendering
        this.selectedNodeId = null;
        this.hoveredSlotId = null;
        this.hoveredArrow = null; // 'left' or 'right'
        this.navArrows = null; // Arrow positions for hit testing
        this.domainSuffix = '.tunnelmesh';
        this.coordNodeName = null; // Name of coord node if enabled as peer

        // Stack info for "+ N more" labels
        this.stackInfo = {
            left: { total: 0, hidden: 0 },
            right: { total: 0, hidden: 0 },
        };

        // Animation
        this.animating = false;
        this.animationStart = 0;
        this.animationDuration = 400; // ms
        this.skipNextAnimation = false;

        // Pan state
        this.panX = 0;
        this.panY = 0;
        this.isDragging = false;
        this.dragStartX = 0;
        this.dragStartY = 0;
        this.panStartX = 0;
        this.panStartY = 0;

        // Dynamic content dimensions (calculated from canvas size)
        this.contentWidth = MIN_CONTENT_WIDTH;
        this.contentHeight = MIN_CONTENT_HEIGHT;

        // Callbacks
        this.onNodeSelected = null;

        // Bound event handlers (stored for cleanup)
        this._boundResize = () => this.resize();

        // Setup
        this.setupInteraction();
        this.resize();

        // Initial render
        this.render();
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    setDomainSuffix(suffix) {
        this.domainSuffix = suffix;
    }

    setCoordNodeName(name) {
        this.coordNodeName = name;
    }

    syncNodes(peers, wgConcentratorName = null) {
        const existingIds = new Set(this.nodes.keys());
        const newIds = new Set(peers.map((p) => p.name));

        // Remove nodes that no longer exist
        for (const id of existingIds) {
            if (!newIds.has(id)) {
                this.nodes.delete(id);
            }
        }

        // Update or add nodes
        for (const peer of peers) {
            const nodeType = peer.name === wgConcentratorName ? NodeType.WG_CONCENTRATOR : NodeType.STANDARD;

            if (this.nodes.has(peer.name)) {
                // Update existing node
                const node = this.nodes.get(peer.name);
                node.online = peer.online;
                node.connectable = peer.connectable;
                node.behindNAT = peer.behind_nat;
                node.activeTunnels = peer.stats?.active_tunnels ?? 0;
                node.coordinatorRTTMs = peer.coordinator_rtt_ms ?? 0;
                node.version = peer.version || '';
                node.nodeType = nodeType;
                node.bytesSentRate = peer.bytes_sent_rate || 0;
                node.bytesReceivedRate = peer.bytes_received_rate || 0;
                node.region = extractRegion(peer);
                // Exit node info
                node.exitNode = peer.exit_node || '';
                node.allowsExitTraffic = peer.allows_exit_traffic || false;
                node.exitClients = peer.exit_clients || [];
                // Connection types
                node.connections = peer.connections || {};
            } else {
                // Add new node
                const node = new VisualizerNode(peer, this.domainSuffix, nodeType);
                this.nodes.set(peer.name, node);
            }
        }

        // Auto-select node if none selected
        // Priority: 'server-node' > coord node > first alphabetically
        if (!this.selectedNodeId && this.nodes.size > 0) {
            let nodeToSelect = null;

            // 1. Prefer 'server-node'
            if (this.nodes.has('server-node')) {
                nodeToSelect = 'server-node';
            }
            // 2. Try coord node (if set)
            else if (this.coordNodeName && this.nodes.has(this.coordNodeName)) {
                nodeToSelect = this.coordNodeName;
            }
            // 3. Fallback to first alphabetically
            else {
                const sortedIds = Array.from(this.nodes.keys()).sort();
                nodeToSelect = sortedIds[0];
            }

            this.selectNode(nodeToSelect);
        } else {
            // Recalculate layout and animate to new positions
            this.recalculateLayout();
            if (this.skipNextAnimation) {
                this.skipNextAnimation = false;
                // Snap slots to final positions (no animation)
                for (const slot of this.slots) {
                    slot.x = slot.targetX;
                    slot.y = slot.targetY;
                }
                this.render();
            } else {
                this.startAnimation();
            }
        }
    }

    selectNode(nodeId) {
        // Deselect previous
        if (this.selectedNodeId && this.nodes.has(this.selectedNodeId)) {
            this.nodes.get(this.selectedNodeId).selected = false;
        }

        this.selectedNodeId = nodeId;

        // Select new
        if (nodeId && this.nodes.has(nodeId)) {
            this.nodes.get(nodeId).selected = true;
        }

        // Reset pan to center when selection changes
        this.panX = 0;
        this.panY = 0;

        this.recalculateLayout();
        this.startAnimation();

        // Notify callback
        if (this.onNodeSelected) {
            this.onNodeSelected(nodeId);
        }
    }

    // Set selection without triggering callback (for external sync)
    setSelection(nodeId) {
        if (this.selectedNodeId === nodeId) return;

        // Deselect previous
        if (this.selectedNodeId && this.nodes.has(this.selectedNodeId)) {
            this.nodes.get(this.selectedNodeId).selected = false;
        }

        this.selectedNodeId = nodeId;

        // Select new
        if (nodeId && this.nodes.has(nodeId)) {
            this.nodes.get(nodeId).selected = true;
        }

        // Reset pan to center when selection changes
        this.panX = 0;
        this.panY = 0;

        this.recalculateLayout();
        this.startAnimation();
    }

    resize() {
        const rect = this.canvas.parentElement.getBoundingClientRect();
        const dpr = window.devicePixelRatio || 1;

        this.canvas.width = rect.width * dpr;
        this.canvas.height = rect.height * dpr;
        this.canvas.style.width = `${rect.width}px`;
        this.canvas.style.height = `${rect.height}px`;

        // Reset transform before scaling to avoid cumulative scaling on tab switches
        this.ctx.setTransform(1, 0, 0, 1, 0, 0);
        this.ctx.scale(dpr, dpr);

        // Calculate content dimensions based on canvas size (with min/max bounds)
        this.contentWidth = Math.max(MIN_CONTENT_WIDTH, Math.min(MAX_CONTENT_WIDTH, rect.width * 0.95));
        this.contentHeight = Math.max(MIN_CONTENT_HEIGHT, Math.min(MAX_CONTENT_HEIGHT, rect.height * 0.9));

        this.recalculateLayout();
        this.render();
    }

    // -------------------------------------------------------------------------
    // Layout
    // -------------------------------------------------------------------------

    recalculateLayout() {
        const oldSlots = new Map(this.slots.map((s) => [s.id, { x: s.x, y: s.y }]));

        // Use dynamic content size for layout
        calculateLayout(
            this.nodes,
            this.selectedNodeId,
            this.contentWidth,
            this.contentHeight,
            this.stackInfo,
            this.slots,
        );

        // Preserve old positions for animation, or initialize to target
        for (const slot of this.slots) {
            const old = oldSlots.get(slot.id);
            if (old) {
                slot.startX = old.x;
                slot.startY = old.y;
                slot.x = old.x;
                slot.y = old.y;
            } else {
                // New slot - start at target (no animation)
                slot.startX = slot.targetX;
                slot.startY = slot.targetY;
                slot.x = slot.targetX;
                slot.y = slot.targetY;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Animation
    // -------------------------------------------------------------------------

    startAnimation() {
        // Capture start positions
        for (const slot of this.slots) {
            slot.startX = slot.x;
            slot.startY = slot.y;
        }

        this.animationStart = performance.now();
        if (!this.animating) {
            this.animating = true;
            this.animate();
        }
    }

    animate() {
        const elapsed = performance.now() - this.animationStart;
        const progress = Math.min(elapsed / this.animationDuration, 1);

        // Ease out cubic
        const t = 1 - (1 - progress) ** 3;

        // Interpolate positions
        for (const slot of this.slots) {
            slot.x = slot.startX + (slot.targetX - slot.startX) * t;
            slot.y = slot.startY + (slot.targetY - slot.startY) * t;
        }

        this.render();

        if (progress < 1) {
            requestAnimationFrame(() => this.animate());
        } else {
            this.animating = false;
        }
    }

    // -------------------------------------------------------------------------
    // Interaction (with pan support)
    // -------------------------------------------------------------------------

    setupInteraction() {
        this.canvas.addEventListener('mousedown', (e) => this.onMouseDown(e));
        this.canvas.addEventListener('mousemove', (e) => this.onMouseMove(e));
        this.canvas.addEventListener('mouseup', (e) => this.onMouseUp(e));
        this.canvas.addEventListener('mouseleave', () => this.onMouseLeave());

        // Resize - use bound handler for cleanup
        window.addEventListener('resize', this._boundResize);
    }

    // Clean up event listeners to prevent memory leaks
    destroy() {
        window.removeEventListener('resize', this._boundResize);
    }

    // Convert screen coordinates to content coordinates (accounting for pan and centering)
    screenToContent(screenX, screenY) {
        const rect = this.canvas.getBoundingClientRect();
        const offsetX = (rect.width - this.contentWidth) / 2 + this.panX;
        const offsetY = (rect.height - this.contentHeight) / 2 + this.panY;
        return {
            x: screenX - offsetX,
            y: screenY - offsetY,
        };
    }

    // Constrain pan to bounds (allow panning up to 2x content size)
    clampPan() {
        const maxPanX = this.contentWidth / 2;
        const maxPanY = this.contentHeight / 2;
        this.panX = Math.max(-maxPanX, Math.min(maxPanX, this.panX));
        this.panY = Math.max(-maxPanY, Math.min(maxPanY, this.panY));
    }

    onMouseDown(e) {
        const rect = this.canvas.getBoundingClientRect();
        const screenX = e.clientX - rect.left;
        const screenY = e.clientY - rect.top;

        // Check if clicking on interactive element first
        const content = this.screenToContent(screenX, screenY);
        const arrow = this.hitTestNavArrows(content.x, content.y);
        const slot = this.hitTestSlots(content.x, content.y);

        if (arrow) {
            // Handle arrow click immediately
            if (arrow === 'left') {
                this.navigatePrev();
            } else if (arrow === 'right') {
                this.navigateNext();
            }
            return;
        }

        if (slot && slot.node.id !== this.selectedNodeId) {
            // Handle slot click immediately
            this.selectNode(slot.node.id);
            return;
        }

        // Start drag for panning
        this.isDragging = true;
        this.dragStartX = e.clientX;
        this.dragStartY = e.clientY;
        this.panStartX = this.panX;
        this.panStartY = this.panY;
        this.canvas.style.cursor = 'grabbing';
    }

    onMouseMove(e) {
        const rect = this.canvas.getBoundingClientRect();
        const screenX = e.clientX - rect.left;
        const screenY = e.clientY - rect.top;

        if (this.isDragging) {
            // Update pan offset
            this.panX = this.panStartX + (e.clientX - this.dragStartX);
            this.panY = this.panStartY + (e.clientY - this.dragStartY);
            this.clampPan();
            this.render();
            return;
        }

        const content = this.screenToContent(screenX, screenY);

        // Check arrows first
        const arrow = this.hitTestNavArrows(content.x, content.y);
        if (arrow !== this.hoveredArrow) {
            this.hoveredArrow = arrow;
            if (arrow) {
                this.canvas.style.cursor = 'pointer';
                this.render();
                return;
            }
        }

        // Update slot hover
        const slot = this.hitTestSlots(content.x, content.y);
        const newHoveredId = slot ? slot.id : null;

        if (newHoveredId !== this.hoveredSlotId || !arrow) {
            this.hoveredSlotId = newHoveredId;
            this.canvas.style.cursor = newHoveredId || arrow ? 'pointer' : 'grab';
            this.render();
        }
    }

    onMouseUp(e) {
        if (this.isDragging) {
            // Check if it was a click (minimal movement)
            const dx = Math.abs(e.clientX - this.dragStartX);
            const dy = Math.abs(e.clientY - this.dragStartY);
            const wasClick = dx < 5 && dy < 5;

            this.isDragging = false;
            this.canvas.style.cursor = 'grab';

            if (wasClick) {
                // Treat as click
                const rect = this.canvas.getBoundingClientRect();
                const screenX = e.clientX - rect.left;
                const screenY = e.clientY - rect.top;
                const content = this.screenToContent(screenX, screenY);

                const arrow = this.hitTestNavArrows(content.x, content.y);
                if (arrow === 'left') {
                    this.navigatePrev();
                    return;
                }
                if (arrow === 'right') {
                    this.navigateNext();
                    return;
                }

                const slot = this.hitTestSlots(content.x, content.y);
                if (slot && slot.node.id !== this.selectedNodeId) {
                    this.selectNode(slot.node.id);
                }
            }
        }
    }

    onMouseLeave() {
        this.isDragging = false;
        this.hoveredSlotId = null;
        this.hoveredArrow = null;
        this.canvas.style.cursor = 'default';
        this.render();
    }

    hitTestSlots(contentX, contentY) {
        // Check slots in reverse order (topmost first)
        // contentX/contentY are already in content coordinates
        for (let i = this.slots.length - 1; i >= 0; i--) {
            const slot = this.slots[i];
            const halfWidth = CARD_WIDTH / 2;
            const halfHeight = CARD_HEIGHT / 2;

            if (
                contentX >= slot.x - halfWidth &&
                contentX <= slot.x + halfWidth &&
                contentY >= slot.y - halfHeight - TAB_HEIGHT &&
                contentY <= slot.y + halfHeight
            ) {
                return slot;
            }
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // Rendering
    // -------------------------------------------------------------------------

    render() {
        const ctx = this.ctx;
        const rect = this.canvas.getBoundingClientRect();
        const width = rect.width;
        const height = rect.height;

        // Clear
        ctx.fillStyle = COLORS.background;
        ctx.fillRect(0, 0, width, height);

        if (this.nodes.size === 0) {
            ctx.fillStyle = COLORS.textDim;
            ctx.font = '14px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
            ctx.textAlign = 'center';
            ctx.fillText('No peers connected', width / 2, height / 2);
            return;
        }

        if (!this.selectedNodeId) {
            ctx.fillStyle = COLORS.textDim;
            ctx.font = '14px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
            ctx.textAlign = 'center';
            ctx.fillText('Select a peer to view connections', width / 2, height / 2);
            return;
        }

        // Apply pan offset - center content on canvas then apply pan
        ctx.save();
        const offsetX = (width - this.contentWidth) / 2 + this.panX;
        const offsetY = (height - this.contentHeight) / 2 + this.panY;
        ctx.translate(offsetX, offsetY);

        // Draw connections first (behind nodes)
        this.renderConnections(ctx);

        // Draw slots (center slot last so it's on top)
        const sortedSlots = [...this.slots].sort((a, b) => {
            if (a.side === 'center') return 1;
            if (b.side === 'center') return -1;
            return 0;
        });

        for (const slot of sortedSlots) {
            const isHovered = slot.id === this.hoveredSlotId;
            this.renderSlot(ctx, slot, isHovered);
        }

        // Draw "+ N more" labels
        this.renderStackLabels(ctx, this.contentWidth, this.contentHeight);

        // Draw navigation arrows below center node
        this.renderNavArrows(ctx);

        ctx.restore();
    }

    renderNavArrows(ctx) {
        const centerSlot = this.slots.find((s) => s.side === 'center');
        if (!centerSlot || this.nodes.size <= 1) return;

        const arrowY = centerSlot.y + CARD_HEIGHT / 2 + 30;
        const arrowSpacing = 40;

        ctx.font = '36px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';

        // Left arrow
        ctx.fillStyle = this.hoveredArrow === 'left' ? COLORS.text : COLORS.textDim;
        ctx.textAlign = 'center';
        ctx.fillText('◀', centerSlot.x - arrowSpacing, arrowY);

        // Right arrow
        ctx.fillStyle = this.hoveredArrow === 'right' ? COLORS.text : COLORS.textDim;
        ctx.fillText('▶', centerSlot.x + arrowSpacing, arrowY);

        // Store arrow positions for hit testing
        this.navArrows = {
            y: arrowY,
            leftX: centerSlot.x - arrowSpacing,
            rightX: centerSlot.x + arrowSpacing,
        };
    }

    hitTestNavArrows(contentX, contentY) {
        if (!this.navArrows || this.nodes.size <= 1) return null;

        const hitRadius = 24;
        const { y, leftX, rightX } = this.navArrows;

        if (Math.abs(contentX - leftX) < hitRadius && Math.abs(contentY - y) < hitRadius) {
            return 'left';
        }
        if (Math.abs(contentX - rightX) < hitRadius && Math.abs(contentY - y) < hitRadius) {
            return 'right';
        }
        return null;
    }

    getSortedNodeIds() {
        return Array.from(this.nodes.keys()).sort();
    }

    navigatePrev() {
        const ids = this.getSortedNodeIds();
        const currentIndex = ids.indexOf(this.selectedNodeId);
        const prevIndex = (currentIndex - 1 + ids.length) % ids.length;
        this.selectNode(ids[prevIndex]);
    }

    navigateNext() {
        const ids = this.getSortedNodeIds();
        const currentIndex = ids.indexOf(this.selectedNodeId);
        const nextIndex = (currentIndex + 1) % ids.length;
        this.selectNode(ids[nextIndex]);
    }

    renderStackLabels(ctx, width, height) {
        const centerSlot = this.slots.find((s) => s.side === 'center');
        if (!centerSlot) return;

        const columnSpacing = this.stackInfo.columnSpacing || 280;

        ctx.font = '12px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
        ctx.fillStyle = COLORS.textDim;

        // Left side label
        if (this.stackInfo.left.hidden > 0) {
            ctx.textAlign = 'center';
            const labelX = centerSlot.x - columnSpacing;
            const labelY = height - 20;
            ctx.fillText(`+ ${this.stackInfo.left.hidden} more`, labelX, labelY);
        }

        // Right side label
        if (this.stackInfo.right.hidden > 0) {
            ctx.textAlign = 'center';
            const labelX = centerSlot.x + columnSpacing;
            const labelY = height - 20;
            ctx.fillText(`+ ${this.stackInfo.right.hidden} more`, labelX, labelY);
        }
    }

    renderConnections(ctx) {
        const centerSlot = this.slots.find((s) => s.side === 'center');
        if (!centerSlot) return;

        for (const slot of this.slots) {
            if (slot.side === 'center') continue;

            const isLeft = slot.side === 'left';

            // Check if this is an exit path connection
            // Exit path is golden when this node is the center node's exit node
            const centerNode = centerSlot.node;
            const otherNode = slot.node;
            const isExitPath = centerNode.exitNode === otherNode.name;

            ctx.beginPath();
            ctx.strokeStyle = isExitPath ? COLORS.exitConnection : COLORS.connectionHighlight;
            ctx.lineWidth = isExitPath ? 3 : 2; // Thicker for exit path

            const startX = isLeft ? slot.x + CARD_WIDTH / 2 : slot.x - CARD_WIDTH / 2;
            const startY = slot.y;
            const endX = isLeft ? centerSlot.x - CARD_WIDTH / 2 : centerSlot.x + CARD_WIDTH / 2;
            const endY = centerSlot.y;

            const midX = (startX + endX) / 2;

            ctx.moveTo(startX, startY);
            ctx.bezierCurveTo(midX, startY, midX, endY, endX, endY);
            ctx.stroke();

            // Connection dots - use same color as line
            const dotColor = isExitPath ? COLORS.exitConnection : COLORS.connectionHighlight;
            ctx.fillStyle = dotColor;
            ctx.beginPath();
            ctx.arc(startX, startY, CONNECTION_DOT_RADIUS, 0, Math.PI * 2);
            ctx.fill();
            ctx.beginPath();
            ctx.arc(endX, endY, CONNECTION_DOT_RADIUS, 0, Math.PI * 2);
            ctx.fill();
        }
    }

    renderSlot(ctx, slot, isHovered) {
        const node = slot.node;
        const x = slot.x - CARD_WIDTH / 2;
        const y = slot.y - CARD_HEIGHT / 2;

        const tabText = node.name;
        ctx.font = 'bold 11px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
        const tabTextWidth = ctx.measureText(tabText).width;

        // Calculate EXIT badge width if needed
        let exitBadgeWidth = 0;
        if (node.allowsExitTraffic) {
            ctx.font = 'bold 8px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
            const exitTextWidth = ctx.measureText('EXIT').width;
            exitBadgeWidth = exitTextWidth + 6 + 6; // padding + gap before badge
        }

        const tabWidth = tabTextWidth + 14 + exitBadgeWidth;
        const tabX = x;
        const tabY = y - TAB_HEIGHT;

        // Draw combined tab + card shape
        ctx.fillStyle = isHovered ? '#2d333b' : COLORS.cardFill;
        ctx.strokeStyle = isHovered ? '#58a6ff' : COLORS.cardStroke;
        ctx.lineWidth = isHovered ? 2 : 1;

        ctx.beginPath();
        ctx.moveTo(tabX, y);
        ctx.lineTo(tabX, tabY + 4);
        ctx.arcTo(tabX, tabY, tabX + 4, tabY, 4);
        ctx.lineTo(tabX + tabWidth - 4, tabY);
        ctx.arcTo(tabX + tabWidth, tabY, tabX + tabWidth, tabY + 4, 4);
        ctx.lineTo(tabX + tabWidth, y);
        ctx.lineTo(x + CARD_WIDTH - 6, y);
        ctx.arcTo(x + CARD_WIDTH, y, x + CARD_WIDTH, y + 6, 6);
        ctx.lineTo(x + CARD_WIDTH, y + CARD_HEIGHT - 6);
        ctx.arcTo(x + CARD_WIDTH, y + CARD_HEIGHT, x + CARD_WIDTH - 6, y + CARD_HEIGHT, 6);
        ctx.lineTo(x + 6, y + CARD_HEIGHT);
        ctx.arcTo(x, y + CARD_HEIGHT, x, y + CARD_HEIGHT - 6, 6);
        ctx.lineTo(x, y);
        ctx.closePath();
        ctx.fill();
        ctx.stroke();

        // Tab text
        ctx.font = 'bold 11px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
        ctx.fillStyle = COLORS.text;
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillText(tabText, tabX + 7, tabY + TAB_HEIGHT / 2);

        // Exit badge next to title in tab (for nodes that allow exit traffic)
        if (node.allowsExitTraffic) {
            const badgeX = tabX + 7 + tabTextWidth + 6; // after text + gap
            const badgeY = tabY + (TAB_HEIGHT - 12) / 2; // vertically centered (badge height = 12)
            this.drawExitBadge(ctx, badgeX, badgeY);
        }

        // Content
        const contentX = x + 10;
        const contentY = y + 14;
        const lineHeight = 16;

        ctx.fillStyle = COLORS.text;
        ctx.font = '12px monospace';
        ctx.textAlign = 'left';
        // Show mesh IP with tunnel count in brackets
        const tunnelCount = node.connections ? Object.keys(node.connections).length : 0;
        const ipText = tunnelCount > 0 ? `${node.meshIP} (${tunnelCount})` : node.meshIP;
        ctx.fillText(ipText, contentX, contentY + 4);

        if (node.version) {
            ctx.fillStyle = COLORS.textDim;
            ctx.font = '10px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
            ctx.textAlign = 'right';
            ctx.fillText(node.version, x + CARD_WIDTH - 10, contentY + 4);
        }

        ctx.fillStyle = COLORS.textDim;
        ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
        ctx.textAlign = 'left';
        const latencyText = formatLatencyCompact(node.coordinatorRTTMs);
        ctx.fillText(latencyText, contentX, contentY + lineHeight + 6);

        // Throughput (right side of line 2)
        const throughputText = `↑${formatBytesCompact(node.bytesSentRate)} ↓${formatBytesCompact(node.bytesReceivedRate)}`;
        ctx.textAlign = 'right';
        ctx.fillText(throughputText, x + CARD_WIDTH - 10, contentY + lineHeight + 6);

        // Region (bottom left corner of card)
        if (node.region) {
            ctx.fillStyle = COLORS.textDim;
            ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
            ctx.textAlign = 'left';
            ctx.fillText(node.region, contentX, contentY + lineHeight * 2 + 8);
        }

        // Transport type badge (show connection type to selected node)
        const selectedNode = this.selectedNodeId ? this.nodes.get(this.selectedNodeId) : null;
        if (selectedNode && slot.side !== 'center') {
            // Get transport type from this node's connections to the selected node
            // or from selected node's connections to this node
            const transportType = node.connections[selectedNode.name] || selectedNode.connections[node.name];
            if (transportType) {
                this.drawTransportBadge(ctx, x + CARD_WIDTH - 10, contentY + lineHeight * 2 + 8, transportType);
            }
        }
    }

    drawTransportBadge(ctx, x, y, transportType) {
        const type = transportType.toLowerCase();
        let color;
        switch (type) {
            case 'ssh':
                color = COLORS.transportSSH;
                break;
            case 'udp':
                color = COLORS.transportUDP;
                break;
            case 'relay':
                color = COLORS.transportRelay;
                break;
            default:
                color = COLORS.textDim;
        }

        ctx.font = 'bold 9px monospace';
        ctx.textAlign = 'right';
        ctx.fillStyle = color;
        ctx.fillText(type.toUpperCase(), x, y);
    }

    drawExitBadge(ctx, x, y) {
        const text = 'EXIT';
        ctx.font = 'bold 8px -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';
        const textWidth = ctx.measureText(text).width;

        // Draw badge background (square corners)
        const paddingH = 3; // horizontal padding
        const _paddingV = 2; // vertical padding
        const badgeWidth = textWidth + paddingH * 2;
        const badgeHeight = 12;
        const badgeX = x;
        const badgeY = y;

        ctx.fillStyle = 'rgba(240, 165, 0, 0.2)';
        ctx.strokeStyle = 'rgba(240, 165, 0, 0.5)';
        ctx.lineWidth = 1;

        // Square corners - use fillRect/strokeRect instead of roundRect
        ctx.fillRect(badgeX, badgeY, badgeWidth, badgeHeight);
        ctx.strokeRect(badgeX, badgeY, badgeWidth, badgeHeight);

        // Draw text centered in badge
        ctx.fillStyle = COLORS.exitConnection;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(text, badgeX + badgeWidth / 2, badgeY + badgeHeight / 2);
    }
}

// Export for use in app.js
window.NodeVisualizer = NodeVisualizer;
window.NodeType = NodeType;
