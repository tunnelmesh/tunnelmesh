// NodeMap - Displays peer locations on a canvas map with dark theme
// Location data comes from peer.location object which contains:
// - latitude, longitude: coordinates
// - source: "manual" (user configured) or "ip" (IP geolocation)
// - accuracy: meters (~0 for manual, ~50000 for IP)
// - city, region, country: location details

const WORLD_PX = 4096; // world-map.png is 4096×4096 (@2x retina tiles, zoom 3)

// Lazy-load the world map image (shared across all instances)
const _worldImg = new Image();
_worldImg.src = 'img/world-map.png';

// Web Mercator helper
function _mercY(latDeg) {
    return Math.log(Math.tan(Math.PI / 4 + (latDeg * Math.PI) / 360));
}

const _MERC_BOTTOM = _mercY(-85.051);
const _MERC_TOP = _mercY(85.051);
const _MERC_RANGE = _MERC_TOP - _MERC_BOTTOM;

class NodeMap {
    constructor(containerId) {
        this._containerId = containerId;
        this._canvas = null;
        this._ctx = null;
        this._dpr = 1;
        this._ro = null;
        this._initialized = false;

        // Data model (same as before)
        this._markers = new Map(); // peerName → { lat, lng, color, peer, loc }
        this._connections = new Map(); // key → { pts[], isExitPath }
        this._selectedPeer = null;
        this._selectedPeerData = null;
        this._onlinePeersWithLocation = new Map(); // peerName → { lat, lng, exitNode }
        this._onPageShow = null;

        // Viewport state
        this._vp = { cx: WORLD_PX / 2, cy: WORLD_PX / 2, scale: 1, cw: 0, ch: 0 };
    }

    // -------------------------------------------------------------------------
    // Public API (same interface as Leaflet version)
    // -------------------------------------------------------------------------

    init() {
        if (this._initialized) return;

        const container = document.getElementById(this._containerId);
        if (!container) {
            console.error('Map container not found:', this._containerId);
            return;
        }

        this._canvas = document.createElement('canvas');
        this._ctx = this._canvas.getContext('2d');
        this._dpr = window.devicePixelRatio || 1;
        container.appendChild(this._canvas);

        // Click handler for node selection
        this._canvas.addEventListener('click', (e) => this._onClick(e));

        // ResizeObserver keeps canvas sized to container
        this._ro = new ResizeObserver(() => this._onResize());
        this._ro.observe(container);
        this._onResize();

        // Render once image is ready (may already be cached)
        _worldImg.onload = () => this._render();
        if (_worldImg.complete) this._render();

        // Re-render after bfcache restore (canvas is cleared and ctx transform is reset)
        this._onPageShow = (e) => {
            if (e.persisted) this._onResize();
        };
        window.addEventListener('pageshow', this._onPageShow);

        this._initialized = true;
    }

    updatePeers(peers) {
        if (!this._initialized) this.init();

        const seenPeers = new Set();
        let hasLocations = false;

        // Clear online peers tracking for this update
        this._onlinePeersWithLocation.clear();

        // Group peers by location to detect co-located nodes
        const locationGroups = new Map();
        peers.forEach((peer) => {
            if (!peer.location || !peer.location.source) return;
            const key = `${peer.location.latitude},${peer.location.longitude}`;
            if (!locationGroups.has(key)) locationGroups.set(key, []);
            locationGroups.get(key).push(peer);
        });

        // Calculate offsets for co-located nodes (arrange in a circle)
        const peerOffsets = new Map();
        locationGroups.forEach((groupPeers) => {
            if (groupPeers.length > 1) {
                const offsetRadius = 0.45;
                groupPeers.forEach((peer, i) => {
                    const angle = (2 * Math.PI * i) / groupPeers.length;
                    peerOffsets.set(peer.name, {
                        latOffset: offsetRadius * Math.sin(angle),
                        lngOffset: offsetRadius * Math.cos(angle),
                    });
                });
            }
        });

        peers.forEach((peer) => {
            if (!peer.location || !peer.location.source) {
                this._markers.delete(peer.name);
                return;
            }

            hasLocations = true;
            seenPeers.add(peer.name);

            const loc = peer.location;
            let lat = loc.latitude;
            let lng = loc.longitude;

            const offset = peerOffsets.get(peer.name);
            if (offset) {
                lat += offset.latOffset;
                lng += offset.lngOffset;
            }

            if (peer.online) {
                this._onlinePeersWithLocation.set(peer.name, {
                    lat,
                    lng,
                    exitNode: peer.exit_node || '',
                });
            }

            if (peer.name === this._selectedPeer) {
                this._selectedPeerData = peer;
            }

            let color;
            if (peer.name === this._selectedPeer) {
                color = '#58a6ff';
            } else if (!peer.online) {
                color = '#6b7280';
            } else {
                color = '#3fb950';
            }

            this._markers.set(peer.name, { lat, lng, color, peer, loc });
        });

        // Remove markers for peers no longer present
        for (const name of [...this._markers.keys()]) {
            if (!seenPeers.has(name)) this._markers.delete(name);
        }

        this._updateConnections();

        // Show/hide map section
        const mapSection = document.getElementById('map-section');
        if (mapSection) {
            const wasHidden = mapSection.style.display === 'none';
            mapSection.style.display = hasLocations ? 'block' : 'none';
            if (wasHidden && hasLocations) {
                setTimeout(() => this._onResize(), 100);
            }
        }

        this._computeViewport();
        this._render();
    }

    setSelectedPeer(peerName) {
        const prev = this._selectedPeer;
        this._selectedPeer = peerName;

        // Recolour previous selection
        if (prev && this._markers.has(prev)) {
            const entry = this._markers.get(prev);
            entry.color = entry.peer?.online ? '#3fb950' : '#6b7280';
        }

        // Colour new selection
        if (peerName && this._markers.has(peerName)) {
            this._markers.get(peerName).color = '#58a6ff';
            this._selectedPeerData = this._markers.get(peerName).peer;
        } else {
            this._selectedPeerData = null;
        }

        this._updateConnections();
        this._render();
    }

    refresh() {
        this._onResize();
    }

    invalidateSize() {
        this._onResize();
    }

    destroy() {
        if (this._ro) {
            this._ro.disconnect();
            this._ro = null;
        }
        if (this._onPageShow) {
            window.removeEventListener('pageshow', this._onPageShow);
            this._onPageShow = null;
        }
        if (this._canvas?.parentNode) {
            this._canvas.parentNode.removeChild(this._canvas);
        }
        this._canvas = null;
        this._ctx = null;
        this._markers.clear();
        this._connections.clear();
        this._onlinePeersWithLocation.clear();
        this._initialized = false;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    _latLngToWorld(lat, lng) {
        const x = ((lng + 180) / 360) * WORLD_PX;
        const y = (1 - (_mercY(lat) - _MERC_BOTTOM) / _MERC_RANGE) * WORLD_PX;
        return [x, y];
    }

    _worldToCanvas(wx, wy) {
        const { cx, cy, scale, cw, ch } = this._vp;
        return [cw / 2 + (wx - cx) * scale, ch / 2 + (wy - cy) * scale];
    }

    _computeViewport() {
        if (!this._canvas) return;
        const cw = this._canvas.width / this._dpr;
        const ch = this._canvas.height / this._dpr;

        const pts = [...this._markers.values()].map((m) => this._latLngToWorld(m.lat, m.lng));

        let cx, cy, scale;

        if (pts.length === 0) {
            cx = WORLD_PX / 2;
            cy = WORLD_PX / 2;
            scale = ch / WORLD_PX;
        } else if (pts.length === 1) {
            [cx, cy] = pts[0];
            scale = Math.min(cw, ch) / 10;
        } else {
            const xs = pts.map((p) => p[0]);
            const ys = pts.map((p) => p[1]);
            const minX = Math.min(...xs),
                maxX = Math.max(...xs);
            const minY = Math.min(...ys),
                maxY = Math.max(...ys);
            cx = (minX + maxX) / 2;
            cy = (minY + maxY) / 2;
            const spanX = maxX - minX || 1;
            const spanY = maxY - minY || 1;
            scale = Math.min((cw - 100) / spanX, (ch - 100) / spanY);
        }

        scale = Math.min(scale, 64); // cap at zoom-10 equivalent (2^(10-3)*256/4096)
        scale = Math.max(scale, ch / WORLD_PX); // don't zoom out past world view

        this._vp = { cx, cy, scale, cw, ch };
    }

    _onResize() {
        if (!this._canvas) return;
        const container = this._canvas.parentElement;
        if (!container) return;
        const w = container.offsetWidth || 0;
        const h = container.offsetHeight || 0;
        if (w === 0 || h === 0) return;

        this._canvas.width = Math.round(w * this._dpr);
        this._canvas.height = Math.round(h * this._dpr);
        this._canvas.style.cssText = `width:${w}px;height:${h}px;display:block;`;
        this._ctx.setTransform(1, 0, 0, 1, 0, 0);
        this._ctx.scale(this._dpr, this._dpr);

        this._computeViewport();
        this._render();
    }

    _onClick(e) {
        if (!this._canvas) return;
        const rect = this._canvas.getBoundingClientRect();
        const mx = e.clientX - rect.left;
        const my = e.clientY - rect.top;
        let nearest = null,
            best = 15;

        for (const [name, entry] of this._markers) {
            const [wx, wy] = this._latLngToWorld(entry.lat, entry.lng);
            const [cx2, cy2] = this._worldToCanvas(wx, wy);
            const d = Math.hypot(mx - cx2, my - cy2);
            if (d < best) {
                nearest = name;
                best = d;
            }
        }

        if (nearest && typeof selectNode === 'function') {
            selectNode(nearest);
        }
    }

    // -------------------------------------------------------------------------
    // Connection helpers (reuse Leaflet version logic, adapted for canvas)
    // -------------------------------------------------------------------------

    _updateConnections() {
        const expected = new Set();

        if (this._selectedPeer && this._onlinePeersWithLocation.has(this._selectedPeer)) {
            const selectedLoc = this._onlinePeersWithLocation.get(this._selectedPeer);
            const selectedExitNode = this._selectedPeerData?.exit_node || '';

            this._onlinePeersWithLocation.forEach((loc, peerName) => {
                if (peerName === this._selectedPeer) return;
                if (!selectedLoc || !loc) return;
                if (Number.isNaN(selectedLoc.lat) || Number.isNaN(selectedLoc.lng)) return;
                if (Number.isNaN(loc.lat) || Number.isNaN(loc.lng)) return;

                const key = `${this._selectedPeer}-${peerName}`;
                const isExitPath = selectedExitNode === peerName || loc.exitNode === this._selectedPeer;
                const pts = this.calculateCurvePoints(selectedLoc.lat, selectedLoc.lng, loc.lat, loc.lng);

                expected.add(key);
                this._connections.set(key, { pts, isExitPath });
            });
        }

        for (const key of [...this._connections.keys()]) {
            if (!expected.has(key)) this._connections.delete(key);
        }
    }

    // Calculate bezier curve points between two locations (unchanged from Leaflet version)
    calculateCurvePoints(lat1, lng1, lat2, lng2, numPoints = 20) {
        const dx = lng2 - lng1;
        const dy = lat2 - lat1;
        const distance = Math.sqrt(dx * dx + dy * dy);

        if (distance < 0.0001) return [];

        const midLat = (lat1 + lat2) / 2;
        const midLng = (lng1 + lng2) / 2;
        const curveAmount = distance * 0.15;
        const perpX = (-dy / distance) * curveAmount;
        const perpY = (dx / distance) * curveAmount;
        const ctrlLat = midLat + perpY;
        const ctrlLng = midLng + perpX;

        const trimDistance = 0.5;
        const trimT = Math.min(trimDistance / distance, 0.4);
        const startT = trimT;
        const endT = 1 - trimT;

        if (startT >= endT) return [];

        const points = [];
        for (let i = 0; i <= numPoints; i++) {
            const t = startT + (i / numPoints) * (endT - startT);
            const t1 = 1 - t;
            points.push([
                t1 * t1 * lat1 + 2 * t1 * t * ctrlLat + t * t * lat2,
                t1 * t1 * lng1 + 2 * t1 * t * ctrlLng + t * t * lng2,
            ]);
        }
        return points;
    }

    // -------------------------------------------------------------------------
    // Render pipeline
    // -------------------------------------------------------------------------

    _render() {
        const ctx = this._ctx;
        if (!ctx) return;

        const { cx, cy, scale, cw, ch } = this._vp;

        // 1. Dark background
        ctx.fillStyle = '#0d0e11';
        ctx.fillRect(0, 0, cw, ch);

        // 2. World map image
        if (_worldImg.complete && _worldImg.naturalWidth) {
            const imgX = cw / 2 - cx * scale;
            const imgY = ch / 2 - cy * scale;
            ctx.drawImage(_worldImg, imgX, imgY, WORLD_PX * scale, WORLD_PX * scale);
        }

        // 3. Connection curves
        for (const [, conn] of this._connections) {
            if (!conn.pts || conn.pts.length < 2) continue;
            const canvasPts = conn.pts.map(([lat, lng]) => {
                const [wx, wy] = this._latLngToWorld(lat, lng);
                return this._worldToCanvas(wx, wy);
            });

            ctx.beginPath();
            ctx.moveTo(canvasPts[0][0], canvasPts[0][1]);
            for (let i = 1; i < canvasPts.length; i++) {
                ctx.lineTo(canvasPts[i][0], canvasPts[i][1]);
            }

            if (conn.isExitPath) {
                ctx.strokeStyle = '#f0a500';
                ctx.lineWidth = 3;
                ctx.setLineDash([]);
            } else {
                ctx.strokeStyle = '#58a6ff';
                ctx.lineWidth = 2;
                ctx.setLineDash([6, 4]);
            }
            ctx.stroke();
            ctx.setLineDash([]);
        }

        // 4. Accuracy circle for selected IP-geolocated peer
        if (this._selectedPeer && this._markers.has(this._selectedPeer)) {
            const entry = this._markers.get(this._selectedPeer);
            const { loc, peer } = entry;
            const shouldShowCircle =
                loc && loc.source === 'ip' && loc.accuracy && loc.accuracy > 1000 && peer && peer.online;

            if (shouldShowCircle) {
                const [wx, wy] = this._latLngToWorld(loc.latitude, loc.longitude);
                const [px, py] = this._worldToCanvas(wx, wy);
                const radiusDeg = loc.accuracy / 111320;
                const radiusWpx = (radiusDeg / 180) * WORLD_PX;
                const radiusCpx = radiusWpx * scale;

                ctx.beginPath();
                ctx.arc(px, py, radiusCpx, 0, Math.PI * 2);
                ctx.strokeStyle = 'rgba(88,166,255,0.3)';
                ctx.lineWidth = 1;
                ctx.setLineDash([5, 5]);
                ctx.stroke();
                ctx.fillStyle = 'rgba(88,166,255,0.05)';
                ctx.fill();
                ctx.setLineDash([]);
            }
        }

        // 5. Node circles
        for (const [name, entry] of this._markers) {
            const [wx, wy] = this._latLngToWorld(entry.lat, entry.lng);
            const [px, py] = this._worldToCanvas(wx, wy);
            const r = 8;

            ctx.globalAlpha = 0.8;
            ctx.beginPath();
            ctx.arc(px, py, r, 0, Math.PI * 2);
            ctx.fillStyle = entry.color;
            ctx.strokeStyle = entry.color;
            ctx.lineWidth = 2;
            ctx.fill();
            ctx.stroke();
            ctx.globalAlpha = 1;

            // Label
            ctx.font = '11px "JetBrains Mono", ui-monospace, monospace';
            ctx.fillStyle = 'rgba(230,237,243,0.85)';
            ctx.textBaseline = 'middle';
            // Place label to the right normally, left for eastern nodes
            const [wx180] = this._latLngToWorld(entry.lat, 0);
            const [px0] = this._worldToCanvas(wx180, 0);
            if (px > px0 + cw * 0.25) {
                ctx.textAlign = 'right';
                ctx.fillText(name, px - r - 4, py + r + 8);
            } else {
                ctx.textAlign = 'left';
                ctx.fillText(name, px + r + 4, py + r + 8);
            }
        }

        ctx.textAlign = 'left';
        ctx.textBaseline = 'alphabetic';
    }
}

// Expose to window for use in app.js
window.NodeMap = NodeMap;
