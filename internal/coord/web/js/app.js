// Import constants from TM.utils (loaded via lib/utils.js)
const { POLL_INTERVAL_MS, SSE_RETRY_DELAY_MS, MAX_SSE_RETRIES, ROWS_PER_PAGE, TOAST_DURATION_MS, TOAST_FADE_MS } =
    TM.utils.CONSTANTS;

// Import utilities from TM modules
const { escapeHtml } = TM.utils;
const { formatBytes, formatRate, formatLatency, formatLastSeen, formatExpiry } = TM.format;
const { createPaginationController } = TM.pagination;
const { createSparklineSVG } = TM.table;
const { createModalController } = TM.modal;

// Panel refresh lists - defines which panels to refresh for each tab
const PANELS_MESH_TAB = ['peers', 'logs', 'tracing', 'alerts', 'filter']; // Mesh tab panels (visualizer/map loaded via fetchData)
const PANELS_APP_TAB = ['s3', 'shares', 'docker']; // App tab panels
const PANELS_DATA_TAB = ['peers-mgmt', 'groups', 'bindings', 'dns']; // Data tab panels

// Toggle collapsible section
function toggleSection(header) {
    header.classList.toggle('collapsed');
    const content = header.nextElementSibling;
    if (content?.classList.contains('collapsible-content')) {
        const wasCollapsed = content.classList.contains('collapsed');
        content.classList.toggle('collapsed');

        // If we just expanded the section, refresh any maps inside
        if (wasCollapsed) {
            requestAnimationFrame(() => {
                const section = header.closest('section');
                if (section && section.id === 'map-section' && state.nodeMap) {
                    state.nodeMap.refresh();
                }
            });
        }
    }
}
window.toggleSection = toggleSection;

// Cached DOM elements (populated on DOMContentLoaded)
const dom = {};

// Use event bus from TM.events module
const events = TM.events;

// Dashboard state - track history per peer
const state = {
    peerHistory: {}, // { peerName: { throughputTx: [], throughputRx: [], packetsTx: [], packetsRx: [] } }
    eventSource: null, // Store EventSource for cleanup
    selectedNodeId: null, // Centralized selection state
    // Visualizer state
    visualizer: null,
    // Map state
    nodeMap: null,
    locationsEnabled: false, // Server-side locations feature flag
    domainSuffix: '.tunnelmesh',
    // Pagination state
    peersVisibleCount: ROWS_PER_PAGE,
    dnsVisibleCount: ROWS_PER_PAGE,
    peersMgmtVisibleCount: ROWS_PER_PAGE,
    groupsVisibleCount: ROWS_PER_PAGE,
    sharesVisibleCount: ROWS_PER_PAGE,
    bindingsVisibleCount: ROWS_PER_PAGE,
    currentPeers: [], // Store current peers data for pagination
    currentDnsRecords: [], // Store current DNS records for pagination
    currentPeersMgmt: [],
    currentGroups: [],
    currentShares: [],
    currentBindings: [],
    // Alerts state
    alertsEnabled: false,
    peerAlerts: {}, // { peerName: { warning: count, critical: count, page: count } }
    // Loki logs state
    lokiEnabled: false,
    // Tempo tracing state
    tempoEnabled: false,
    tempoDatasourceUid: null,
};

// Initialize DOM cache - call once on DOMContentLoaded
function initDOMCache() {
    // Header elements
    dom.uptime = document.getElementById('uptime');
    dom.peerCount = document.getElementById('peer-count');
    dom.serverVersion = document.getElementById('server-version');

    // Peers table elements
    dom.peersBody = document.getElementById('peers-body');
    dom.noPeers = document.getElementById('no-peers');

    // DNS table elements
    dom.dnsBody = document.getElementById('dns-body');
    dom.noDns = document.getElementById('no-dns');

    // Filter elements
    dom.filterSection = document.getElementById('filter-section');
    dom.filterPeerSelect = document.getElementById('filter-peer');
    dom.filterRulesBody = document.getElementById('filter-rules-body');
    dom.noFilterRules = document.getElementById('no-filter-rules');
    dom.filterModal = document.getElementById('filter-modal');
    dom.filterInfo = document.getElementById('filter-info');
    dom.filterDefaultPolicy = document.getElementById('filter-default-policy');
    dom.filterTempWarning = document.getElementById('filter-temp-warning');
    dom.addFilterRuleBtn = document.getElementById('add-filter-rule-btn');

    // Visualizer
    dom.visualizerCanvas = document.getElementById('visualizer-canvas');

    // Toast container
    dom.toastContainer = document.getElementById('toast-container');

    // Logs resize handle
    dom.logsContainer = document.getElementById('logs-link');
    dom.logsResizeHandle = document.getElementById('logs-resize-handle');

    // S3 resize handle
    dom.s3Content = document.getElementById('s3-content');
    dom.s3ResizeHandle = document.getElementById('s3-resize-handle');
}

// Toast notification system
function showToast(message, type = 'error', duration = TOAST_DURATION_MS) {
    if (!dom.toastContainer) return;

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    dom.toastContainer.appendChild(toast);

    // Auto-dismiss after duration
    setTimeout(() => {
        toast.classList.add('fade-out');
        setTimeout(() => toast.remove(), TOAST_FADE_MS);
    }, duration);
}

// Fetch and update dashboard
async function fetchData() {
    try {
        const resp = await fetch('/api/overview');
        if (resp.status === 401) {
            // Browser will show Basic Auth prompt on page load
            // If we get 401 during polling, credentials were rejected
            showAuthError();
            return;
        }
        if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}`);
        }
        hideAuthError();
        const data = await resp.json();
        updateDashboard(data);
    } catch (err) {
        console.error('Failed to fetch data:', err);
    }
}

// Setup Server-Sent Events for real-time updates
let sseRetryCount = 0;
let _heartbeatDebounce = null;

function setupSSE() {
    // Check if EventSource is supported
    if (typeof EventSource === 'undefined') {
        console.log('SSE not supported, falling back to polling');
        startPolling();
        return;
    }

    // Close existing connection if any
    if (state.eventSource) {
        state.eventSource.close();
    }

    // EventSource with credentials for authenticated endpoints
    state.eventSource = new EventSource('/api/events', { withCredentials: true });

    state.eventSource.addEventListener('connected', () => {
        console.log('SSE connected - dashboard will update in real-time');
        sseRetryCount = 0; // Reset retry count on successful connection
    });

    state.eventSource.addEventListener('heartbeat', () => {
        // Debounce: collapse any burst of queued heartbeats (e.g. after tab returns) into one fetch
        clearTimeout(_heartbeatDebounce);
        _heartbeatDebounce = setTimeout(fetchData, 150);
    });

    state.eventSource.onerror = (err) => {
        console.error('SSE error:', err);
        state.eventSource.close();
        state.eventSource = null;

        sseRetryCount++;
        if (sseRetryCount <= MAX_SSE_RETRIES) {
            // Retry SSE connection after a delay
            console.log(`SSE reconnecting (attempt ${sseRetryCount}/${MAX_SSE_RETRIES})...`);
            setTimeout(setupSSE, SSE_RETRY_DELAY_MS * sseRetryCount);
        } else {
            // Fall back to polling after max retries
            console.log('SSE failed, falling back to polling');
            startPolling();
        }
    };
}

function startPolling() {
    // Fallback polling (matches heartbeat interval)
    console.log(`Starting polling mode (every ${POLL_INTERVAL_MS / 1000} seconds)`);
    setInterval(() => fetchData(), POLL_INTERVAL_MS);
}

// Cleanup on page unload to prevent memory leaks
function cleanup() {
    if (state.eventSource) {
        state.eventSource.close();
        state.eventSource = null;
    }
    if (state.visualizer) {
        state.visualizer.destroy();
    }
    if (state.nodeMap) {
        state.nodeMap.destroy();
    }
}

function showAuthError() {
    let banner = document.getElementById('auth-error-banner');
    if (!banner) {
        banner = document.createElement('div');
        banner.id = 'auth-error-banner';
        banner.style.cssText =
            'background:#d32f2f;color:white;padding:12px 20px;text-align:center;position:fixed;top:0;left:0;right:0;z-index:1000;';
        banner.innerHTML =
            'Authentication required. <a href="/admin/" style="color:white;text-decoration:underline;">Click here to login</a>';
        document.body.prepend(banner);
    }
}

function hideAuthError() {
    const banner = document.getElementById('auth-error-banner');
    if (banner) {
        banner.remove();
    }
}

function updateDashboard(data) {
    // Update header stats using cached DOM elements
    if (dom.uptime) dom.uptime.textContent = data.server_uptime;
    if (dom.peerCount) dom.peerCount.textContent = `${data.online_peers}/${data.total_peers}`;

    // Update footer version
    if (dom.serverVersion && data.server_version) {
        dom.serverVersion.textContent = data.server_version;
    }

    // Domain suffix is hardcoded (aliases .tm/.mesh redirect to .tunnelmesh)
    state.domainSuffix = '.tunnelmesh';

    // Track if locations feature is enabled
    state.locationsEnabled = data.locations_enabled || false;

    // Update visualizer with peer data
    if (state.visualizer) {
        state.visualizer.setDomainSuffix(state.domainSuffix);
        state.visualizer.syncNodes(data.peers, null);
        state.visualizer.render();
    }

    // Update map with peer locations (only if locations feature is enabled)
    const mapSection = document.getElementById('map-section');
    if (state.locationsEnabled) {
        if (state.nodeMap) {
            state.nodeMap.updatePeers(data.peers);
        }
    } else if (mapSection) {
        // Hide map section entirely when locations is disabled
        mapSection.style.display = 'none';
    }

    // Update history for each peer
    data.peers.forEach((peer) => {
        if (!state.peerHistory[peer.name]) {
            state.peerHistory[peer.name] = {
                throughputTx: [],
                throughputRx: [],
                packetsTx: [],
                packetsRx: [],
            };
        }
        const history = state.peerHistory[peer.name];

        // Add new data points from current rates
        history.throughputTx.push(peer.bytes_sent_rate || 0);
        history.throughputRx.push(peer.bytes_received_rate || 0);
        history.packetsTx.push(peer.packets_sent_rate || 0);
        history.packetsRx.push(peer.packets_received_rate || 0);

        // Trim to max history using slice (O(n) instead of O(n) shift per element)
        const maxPoints = state.maxHistoryPoints;
        if (history.throughputTx.length > maxPoints) {
            const excess = history.throughputTx.length - maxPoints;
            history.throughputTx = history.throughputTx.slice(excess);
            history.throughputRx = history.throughputRx.slice(excess);
            history.packetsTx = history.packetsTx.slice(excess);
            history.packetsRx = history.packetsRx.slice(excess);
        }
    });

    // Clean up history for removed peers
    const currentPeers = new Set(data.peers.map((p) => p.name));
    Object.keys(state.peerHistory).forEach((name) => {
        if (!currentPeers.has(name)) {
            delete state.peerHistory[name];
        }
    });

    // Sort peers: exit nodes first, then online, then by name
    const sortedPeers = [...data.peers].sort((a, b) => {
        // Exit-capable nodes (allows_exit_traffic) come first
        if (a.allows_exit_traffic && !b.allows_exit_traffic) return -1;
        if (!a.allows_exit_traffic && b.allows_exit_traffic) return 1;
        // Online nodes come before offline
        if (a.online && !b.online) return -1;
        if (!a.online && b.online) return 1;
        // Then sort by name
        return a.name.localeCompare(b.name);
    });

    // Store peers data for pagination
    state.currentPeers = sortedPeers;

    // Render peers table with pagination
    renderPeersTable();

    // Show filter section and populate peer dropdown if we have peers
    if (sortedPeers.length > 0 && dom.filterSection) {
        dom.filterSection.style.display = 'block';
        populateFilterPeerSelect(sortedPeers);
    }
}

// Pagination controllers
const peersPagination = createPaginationController({
    pageSize: ROWS_PER_PAGE,
    getItems: () => state.currentPeers,
    getVisibleCount: () => state.peersVisibleCount,
    setVisibleCount: (n) => {
        state.peersVisibleCount = n;
    },
    onRender: () => renderPeersTable(),
});

const dnsPagination = createPaginationController({
    pageSize: ROWS_PER_PAGE,
    getItems: () => state.currentDnsRecords || [],
    getVisibleCount: () => state.dnsVisibleCount,
    setVisibleCount: (n) => {
        state.dnsVisibleCount = n;
    },
    onRender: () => renderDnsTable(),
});

const peersMgmtPagination = createPaginationController({
    pageSize: ROWS_PER_PAGE,
    getItems: () => state.currentPeersMgmt,
    getVisibleCount: () => state.peersMgmtVisibleCount,
    setVisibleCount: (n) => {
        state.peersMgmtVisibleCount = n;
    },
    onRender: () => renderPeersMgmtTable(),
});

const groupsPagination = createPaginationController({
    pageSize: ROWS_PER_PAGE,
    getItems: () => state.currentGroups,
    getVisibleCount: () => state.groupsVisibleCount,
    setVisibleCount: (n) => {
        state.groupsVisibleCount = n;
    },
    onRender: () => renderGroupsTable(),
});

const sharesPagination = createPaginationController({
    pageSize: ROWS_PER_PAGE,
    getItems: () => state.currentShares,
    getVisibleCount: () => state.sharesVisibleCount,
    setVisibleCount: (n) => {
        state.sharesVisibleCount = n;
    },
    onRender: () => renderSharesTable(),
});

const bindingsPagination = createPaginationController({
    pageSize: ROWS_PER_PAGE,
    getItems: () => state.currentBindings,
    getVisibleCount: () => state.bindingsVisibleCount,
    setVisibleCount: (n) => {
        state.bindingsVisibleCount = n;
    },
    onRender: () => renderBindingsTable(),
});

// Helper to update pagination UI for a section
// Can be called with (prefix, controller) or (prefix, { total, shown, hasMore, canShowLess })
function updateSectionPagination(prefix, controllerOrState) {
    const uiState =
        typeof controllerOrState.getUIState === 'function' ? controllerOrState.getUIState() : controllerOrState;
    const paginationEl = document.getElementById(`${prefix}-pagination`);
    if (!paginationEl) return;
    if (uiState.isEmpty || uiState.total === 0) {
        paginationEl.style.display = 'none';
        return;
    }
    paginationEl.style.display = uiState.hasMore || uiState.canShowLess ? 'block' : 'none';
    document.getElementById(`${prefix}-show-more`).style.display = uiState.hasMore ? 'inline' : 'none';
    document.getElementById(`${prefix}-show-less`).style.display = uiState.canShowLess ? 'inline' : 'none';
    document.getElementById(`${prefix}-shown-count`).textContent = uiState.shown;
    document.getElementById(`${prefix}-total-count`).textContent = uiState.total;
}
window.updateSectionPagination = updateSectionPagination;

// Expose pagination functions for HTML onclick handlers
window.showMorePeers = () => peersPagination.showMore();
window.showLessPeers = () => peersPagination.showLess();
window.showMoreDns = () => dnsPagination.showMore();
window.showLessDns = () => dnsPagination.showLess();
window.showMorePeersMgmt = () => peersMgmtPagination.showMore();
window.showLessPeersMgmt = () => peersMgmtPagination.showLess();
window.showMoreGroups = () => groupsPagination.showMore();
window.showLessGroups = () => groupsPagination.showLess();
window.showMoreShares = () => sharesPagination.showMore();
window.showLessShares = () => sharesPagination.showLess();
window.showMoreBindings = () => bindingsPagination.showMore();
window.showLessBindings = () => bindingsPagination.showLess();

// Modal controllers (initialized with element IDs, resolved lazily)
const filterModal = createModalController('filter-modal', {
    onClose: () => {
        // Clear form
        const port = document.getElementById('filter-rule-port');
        if (port) port.value = '';
        const sourcePeer = document.getElementById('filter-rule-source-peer');
        if (sourcePeer) sourcePeer.value = '';
        const destPeer = document.getElementById('filter-rule-dest-peer');
        if (destPeer) destPeer.value = '__all__';
    },
});

function renderPeersTable() {
    const peers = state.currentPeers;
    const uiState = peersPagination.getUIState();

    if (peers.length === 0) {
        if (dom.peersBody) dom.peersBody.innerHTML = '';
        if (dom.noPeers) dom.noPeers.style.display = 'block';
        document.getElementById('peers-pagination').style.display = 'none';
        return;
    }

    if (dom.noPeers) dom.noPeers.style.display = 'none';
    const visiblePeers = peersPagination.getVisibleItems();
    if (!dom.peersBody) return;
    dom.peersBody.innerHTML = visiblePeers
        .map((peer) => {
            const history = state.peerHistory[peer.name] || {
                throughputTx: [],
                throughputRx: [],
                packetsTx: [],
                packetsRx: [],
            };
            const peerNameEscaped = escapeHtml(peer.name);
            // Alert badge - show if peer has any firing alerts
            const peerAlert = state.peerAlerts[peer.name];
            const hasAlert = peerAlert && (peerAlert.warning > 0 || peerAlert.critical > 0 || peerAlert.page > 0);
            const alertSeverity = peerAlert?.page > 0 ? 'page' : peerAlert?.critical > 0 ? 'critical' : 'warning';
            const alertBadge = hasAlert
                ? `<span class="status-badge alert-icon" title="${alertSeverity}">⚠</span>`
                : '';
            const exitBadge = peer.allows_exit_traffic ? '<span class="status-badge exit">EXIT</span>' : '';
            const exitVia = peer.exit_node ? `<span class="exit-via">via ${escapeHtml(peer.exit_node)}</span>` : '';
            // Tunnel count from connections map
            const tunnelCount = peer.connections ? Object.keys(peer.connections).length : 0;
            const tunnelSuffix = tunnelCount > 0 ? ` <span class="tunnel-count">(${tunnelCount})</span>` : '';
            return `
        <tr>
            <td><strong>${peerNameEscaped}</strong>${alertBadge}${exitBadge}${exitVia}</td>
            <td><code>${peer.mesh_ip}</code>${tunnelSuffix}</td>
            <td>${formatLatency(peer.coordinator_rtt_ms)}</td>
            <td class="sparkline-cell">
                <a class="sparkline-link" href="/grafana/d/tunnelmesh-overview/tunnelmesh-overview?orgId=1&viewPanel=7">
                ${createSparklineSVG(history.throughputTx, history.throughputRx)}
                <div class="rate-values">
                    <span class="tx">${formatBytes(peer.bytes_sent_rate)}/s</span>
                    <span class="rx">${formatBytes(peer.bytes_received_rate)}/s</span>
                </div>
                </a>
            </td>
            <td class="sparkline-cell">
                <a class="sparkline-link" href="/grafana/d/tunnelmesh-overview/tunnelmesh-overview?orgId=1&viewPanel=8">
                ${createSparklineSVG(history.packetsTx, history.packetsRx)}
                <div class="rate-values">
                    <span class="tx">${formatRate(peer.packets_sent_rate)}</span>
                    <span class="rx">${formatRate(peer.packets_received_rate)}</span>
                </div>
                </a>
            </td>
            <td><code>${escapeHtml(peer.version || '-')}</code></td>
            <td><span class="status-badge ${peer.online ? 'online' : 'offline'}">${peer.online ? 'Online' : 'Offline'}</span></td>
        </tr>
    `;
        })
        .join('');

    // Update pagination UI using controller state
    const peersPaginationEl = document.getElementById('peers-pagination');
    if (peersPaginationEl) {
        peersPaginationEl.style.display = uiState.isEmpty ? 'none' : 'flex';
        document.getElementById('peers-show-more').style.display = uiState.hasMore ? 'inline' : 'none';
        document.getElementById('peers-show-less').style.display = uiState.canShowLess ? 'inline' : 'none';
        document.getElementById('peers-shown-count').textContent = uiState.shown;
        document.getElementById('peers-total-count').textContent = uiState.total;
    }
}

// Fetch DNS records from API
async function fetchDnsRecords() {
    try {
        const resp = await fetch('/api/dns');
        if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}`);
        }
        const data = await resp.json();

        // Sort records by hostname
        state.currentDnsRecords = (data.records || []).sort((a, b) => a.hostname.localeCompare(b.hostname));

        renderDnsTable();
    } catch (err) {
        console.error('Failed to fetch DNS records:', err);
        state.currentDnsRecords = [];
        renderDnsTable();
    }
}

function renderDnsTable() {
    const records = state.currentDnsRecords || [];

    if (records.length === 0) {
        if (dom.dnsBody) dom.dnsBody.innerHTML = '';
        if (dom.noDns) dom.noDns.style.display = 'block';
        document.getElementById('dns-pagination').style.display = 'none';
        return;
    }

    if (dom.noDns) dom.noDns.style.display = 'none';
    const visibleRecords = dnsPagination.getVisibleItems();
    if (!dom.dnsBody) return;
    dom.dnsBody.innerHTML = visibleRecords
        .map(
            (record) => `
        <tr>
            <td><code>${escapeHtml(record.hostname)}</code></td>
            <td><code>${escapeHtml(record.mesh_ip)}</code></td>
        </tr>
    `,
        )
        .join('');

    // Update pagination UI using controller state
    const dnsUIState = dnsPagination.getUIState();
    const dnsPaginationEl = document.getElementById('dns-pagination');
    if (dnsPaginationEl) {
        dnsPaginationEl.style.display = dnsUIState.isEmpty ? 'none' : 'flex';
        document.getElementById('dns-show-more').style.display = dnsUIState.hasMore ? 'inline' : 'none';
        document.getElementById('dns-show-less').style.display = dnsUIState.canShowLess ? 'inline' : 'none';
        document.getElementById('dns-shown-count').textContent = dnsUIState.shown;
        document.getElementById('dns-total-count').textContent = dnsUIState.total;
    }
}

// Prometheus alerts polling
const PROMETHEUS_MAX_RETRIES = 10; // ~5 min; avoids console spam in deployments without Prometheus
let prometheusRetryTimer = null;
let prometheusRetries = 0;
async function checkPrometheusAvailable() {
    // If already enabled, just refresh alerts instead of re-checking
    if (state.alertsEnabled) {
        fetchAlerts();
        return;
    }

    // Cancel any pending retry; we're running the check right now
    if (prometheusRetryTimer) {
        clearTimeout(prometheusRetryTimer);
        prometheusRetryTimer = null;
    }

    try {
        // Use alerts endpoint directly - if it works, Prometheus is available
        const resp = await fetch('/prometheus/api/v1/alerts');
        if (resp.ok) {
            prometheusRetries = 0;
            state.alertsEnabled = true;
            // Process the initial response
            const data = await resp.json();
            processAlertData(data);
            // Start polling
            setInterval(fetchAlerts, POLL_INTERVAL_MS);
        } else if (prometheusRetries < PROMETHEUS_MAX_RETRIES) {
            prometheusRetries++;
            console.warn(`Prometheus check: alerts endpoint returned HTTP ${resp.status} (Prometheus not ready?)`);
            prometheusRetryTimer = setTimeout(checkPrometheusAvailable, 30000);
        }
    } catch (err) {
        if (prometheusRetries < PROMETHEUS_MAX_RETRIES) {
            prometheusRetries++;
            console.warn('Prometheus check failed:', err.message);
            prometheusRetryTimer = setTimeout(checkPrometheusAvailable, 30000);
        }
    }
}

// Loki logs
let lokiRetryTimer = null;
async function checkLokiAvailable() {
    // If already enabled, just refresh logs instead of re-checking
    if (state.lokiEnabled) {
        fetchLogs();
        return;
    }

    // Cancel any pending retry; we're running the check right now
    if (lokiRetryTimer) {
        clearTimeout(lokiRetryTimer);
        lokiRetryTimer = null;
    }

    try {
        // First get the Loki datasource info from Grafana
        const dsResp = await fetch('/grafana/api/datasources/name/Loki');
        if (!dsResp.ok) {
            console.warn(`Loki check: datasource API returned HTTP ${dsResp.status} (Grafana not ready?)`);
            lokiRetryTimer = setTimeout(checkLokiAvailable, 30000);
            return;
        }
        const dsInfo = await dsResp.json();
        state.lokiDatasourceId = dsInfo.id;
        state.lokiDatasourceUid = dsInfo.uid;

        // Verify Loki is reachable via the datasource health endpoint.
        // The /resources/ proxy is broken for Loki in Grafana 12; /health is reliable.
        const healthResp = await fetch(`/grafana/api/datasources/uid/${dsInfo.uid}/health`);
        const healthData = healthResp.ok ? await healthResp.json() : null;
        if (healthResp.ok && healthData?.status === 'OK') {
            state.lokiEnabled = true;
            document.getElementById('logs-section').style.display = 'block';
            updateLokiExploreLink();
            fetchLogs();
            setInterval(fetchLogs, POLL_INTERVAL_MS);
        } else {
            console.warn(
                `Loki check: health endpoint returned HTTP ${healthResp.status} status=${healthData?.status} (Loki not ready?)`,
            );
            lokiRetryTimer = setTimeout(checkLokiAvailable, 30000);
        }
    } catch (err) {
        console.warn('Loki check failed:', err.message);
        lokiRetryTimer = setTimeout(checkLokiAvailable, 30000);
    }
}

function updateLokiExploreLink() {
    const logsLink = document.getElementById('logs-link');
    if (!logsLink) return;

    // Build Grafana 10+ explore URL with Loki query (panes= format, schemaVersion=1)
    const now = Date.now();
    const from = now - 3600000; // 1 hour ago
    const dsUid = state.lokiDatasourceUid || 'loki';
    const panesParam = encodeURIComponent(
        JSON.stringify({
            abc: {
                datasource: dsUid,
                queries: [{ refId: 'A', expr: '{job="tunnelmesh"}', queryType: 'range' }],
                range: { from: String(from), to: String(now) },
            },
        }),
    );
    logsLink.href = `/grafana/explore?schemaVersion=1&panes=${panesParam}&orgId=1`;
}

async function fetchLogs() {
    if (!state.lokiEnabled || !state.lokiDatasourceUid) return;

    try {
        const peerCount = Math.max(state.currentPeers.length, 1);
        const limit = 25 * peerCount;
        const now = Date.now(); // ms
        const oneHourAgo = now - 3600000; // ms

        // Use Grafana's unified query endpoint — the /resources/ proxy is broken
        // for Loki in Grafana 12. Response is in DataFrame format.
        const resp = await fetch('/grafana/api/ds/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                queries: [
                    {
                        refId: 'A',
                        expr: '{job="tunnelmesh"}',
                        queryType: 'range',
                        maxLines: limit,
                        direction: 'backward',
                        datasource: { uid: state.lokiDatasourceUid, type: 'loki' },
                    },
                ],
                from: String(oneHourAgo),
                to: String(now),
            }),
        });
        if (!resp.ok) {
            console.error('Loki ds/query failed:', resp.status, await resp.text().catch(() => ''));
            return;
        }

        const data = await resp.json();
        displayLogs(data);
    } catch (err) {
        console.error('Failed to fetch logs:', err);
    }
}

function displayLogs(data) {
    const logsContent = document.getElementById('logs-content');
    const noLogs = document.getElementById('no-logs');
    if (!logsContent) return;

    // Parse Grafana DataFrame format returned by /api/ds/query.
    // Each frame is one log stream; fields are: labels(0), Time(1), Line(2), tsNs(3)...
    const frames = data.results?.A?.frames || [];
    const allEntries = [];

    for (const frame of frames) {
        const fields = frame.schema?.fields || [];
        const values = frame.data?.values || [];
        const timeIdx = fields.findIndex((f) => f.name === 'Time');
        const lineIdx = fields.findIndex((f) => f.name === 'Line');
        const labelsIdx = fields.findIndex((f) => f.name === 'labels');
        if (timeIdx === -1 || lineIdx === -1) continue;

        const times = values[timeIdx] || [];
        const lines = values[lineIdx] || [];
        const labelsList = labelsIdx >= 0 ? values[labelsIdx] : [];
        for (let i = 0; i < lines.length; i++) {
            allEntries.push({
                timestamp: times[i], // already in ms
                line: lines[i],
                labels: labelsList[i] || {},
            });
        }
    }

    // Sort by timestamp descending (newest first)
    allEntries.sort((a, b) => b.timestamp - a.timestamp);

    if (allEntries.length === 0) {
        logsContent.innerHTML = '';
        if (noLogs) noLogs.style.display = 'block';
        return;
    }

    if (noLogs) noLogs.style.display = 'none';

    const html = allEntries
        .map((entry) => {
            const date = new Date(entry.timestamp);
            const timeStr = date.toISOString().replace('T', ' ').substring(0, 23);

            // Parse the log line - try JSON first
            let level = 'info';
            let message = entry.line;

            try {
                const parsed = JSON.parse(entry.line);
                level = (parsed.level || 'info').toLowerCase();
                message = formatLogJson(parsed);
            } catch {
                // Not JSON, check for level prefix
                const levelMatch = entry.line.match(/^(DEBUG|INFO|WARN|ERROR)/i);
                if (levelMatch) {
                    level = levelMatch[1].toLowerCase();
                    message = escapeHtml(entry.line.substring(levelMatch[0].length).trim());
                } else {
                    message = escapeHtml(entry.line);
                }
            }

            return `<div class="log-entry">
            <span class="log-timestamp">${timeStr}</span>
            <span class="log-level ${level}">${level.toUpperCase()}</span>
            <span class="log-message">${message}</span>
        </div>`;
        })
        .join('');

    logsContent.innerHTML = html;
}

function formatLogJson(obj) {
    const parts = [];
    for (const [key, value] of Object.entries(obj)) {
        if (key === 'level' || key === 'ts' || key === 'time') continue;
        const valueStr =
            typeof value === 'string'
                ? `<span class="log-string">"${escapeHtml(value)}"</span>`
                : `<span class="log-value">${escapeHtml(String(value))}</span>`;
        parts.push(`<span class="log-key">${escapeHtml(key)}</span>:${valueStr}`);
    }
    return `{${parts.join(',')}}`;
}

// Tempo distributed tracing
let tempoRetryTimer = null;
async function checkTempoAvailable() {
    // If already enabled, just refresh traces instead of re-checking
    if (state.tempoEnabled) {
        fetchTraces();
        return;
    }

    // Cancel any pending retry; we're running the check right now
    if (tempoRetryTimer) {
        clearTimeout(tempoRetryTimer);
        tempoRetryTimer = null;
    }

    try {
        // First get the Tempo datasource info from Grafana
        const dsResp = await fetch('/grafana/api/datasources/name/Tempo');
        if (!dsResp.ok) {
            console.warn(`Tempo check: datasource API returned HTTP ${dsResp.status} (Grafana not ready?)`);
            tempoRetryTimer = setTimeout(checkTempoAvailable, 30000);
            return;
        }
        const dsInfo = await dsResp.json();
        state.tempoDatasourceUid = dsInfo.uid;

        // Verify Tempo is reachable via the datasource health endpoint
        const healthResp = await fetch(`/grafana/api/datasources/uid/${dsInfo.uid}/health`);
        const healthData = healthResp.ok ? await healthResp.json() : null;
        if (healthResp.ok && healthData?.status === 'OK') {
            state.tempoEnabled = true;
            updateTempoExploreLink();
            fetchTraces();
            setInterval(fetchTraces, POLL_INTERVAL_MS);
        } else {
            console.warn(
                `Tempo check: health endpoint returned HTTP ${healthResp.status} status=${healthData?.status} (Tempo not ready?)`,
            );
            tempoRetryTimer = setTimeout(checkTempoAvailable, 30000);
        }
    } catch (err) {
        console.warn('Tempo check failed:', err.message);
        tempoRetryTimer = setTimeout(checkTempoAvailable, 30000);
    }
}

function updateTempoExploreLink() {
    const tracesLink = document.getElementById('traces-link');
    if (!tracesLink) return;

    // Build Grafana 10+ explore URL with Tempo TraceQL query
    const now = Date.now();
    const from = now - 3600000; // 1 hour ago
    const dsUid = state.tempoDatasourceUid || 'tempo';
    const panesParam = encodeURIComponent(
        JSON.stringify({
            abc: {
                datasource: dsUid,
                queries: [{ refId: 'A', query: '{}', queryType: 'traceql' }],
                range: { from: String(from), to: String(now) },
            },
        }),
    );
    tracesLink.href = `/grafana/explore?schemaVersion=1&panes=${panesParam}&orgId=1`;
}

async function fetchTraces() {
    if (!state.tempoEnabled || !state.tempoDatasourceUid) return;

    try {
        const now = Date.now(); // ms
        const oneHourAgo = now - 3600000; // ms

        // Use Grafana's unified query endpoint — the /resources/ proxy returns 404
        // for Tempo in Grafana 12 (same issue as Loki). Response is DataFrame format.
        const resp = await fetch('/grafana/api/ds/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                queries: [
                    {
                        refId: 'A',
                        datasource: { uid: state.tempoDatasourceUid, type: 'tempo' },
                        queryType: 'traceql',
                        query: '{}',
                        limit: 20,
                        tableType: 'traces',
                    },
                ],
                from: String(oneHourAgo),
                to: String(now),
            }),
        });
        if (!resp.ok) {
            console.error('Tempo ds/query failed:', resp.status, await resp.text().catch(() => ''));
            return;
        }

        const data = await resp.json();
        displayTraces(data);
    } catch (err) {
        console.error('Failed to fetch traces:', err);
    }
}

function displayTraces(data) {
    const tracesContent = document.getElementById('traces-content');
    const noTraces = document.getElementById('no-traces');
    if (!tracesContent) return;

    // Parse Grafana DataFrame format from /api/ds/query for Tempo TraceQL queries.
    // The Traces frame has fields: traceID(0), startTime(1,ms), traceService(2),
    // traceName(3), traceDuration(4,ms nullable).
    const frames = data.results?.A?.frames || [];
    const allTraces = [];

    for (const frame of frames) {
        const fields = frame.schema?.fields || [];
        const values = frame.data?.values || [];

        const traceIdIdx = fields.findIndex((f) => f.name === 'traceID');
        const startTimeIdx = fields.findIndex((f) => f.name === 'startTime');
        const serviceIdx = fields.findIndex((f) => f.name === 'traceService');
        const nameIdx = fields.findIndex((f) => f.name === 'traceName');
        const durationIdx = fields.findIndex((f) => f.name === 'traceDuration');
        if (traceIdIdx === -1 || startTimeIdx === -1) continue;

        const traceIds = values[traceIdIdx] || [];
        for (let i = 0; i < traceIds.length; i++) {
            // Tempo search bug (issue #5395): the search API serialises trace IDs
            // as plain integers, stripping a leading zero hex digit and returning a
            // 31-char string. Left-padding to 32 chars restores the actual stored ID
            // so the trace-by-ID lookup succeeds.
            const rawID = values[traceIdIdx][i] || '';
            allTraces.push({
                traceID: rawID.padStart(32, '0'),
                startTime: values[startTimeIdx]?.[i] || 0, // ms since epoch
                service: serviceIdx >= 0 ? values[serviceIdx]?.[i] || '' : '',
                name: nameIdx >= 0 ? values[nameIdx]?.[i] || '' : '',
                durationMs: durationIdx >= 0 ? values[durationIdx]?.[i] : null,
            });
        }
    }

    if (allTraces.length === 0) {
        tracesContent.innerHTML = '';
        if (noTraces) noTraces.style.display = 'block';
        return;
    }

    if (noTraces) noTraces.style.display = 'none';

    const now = Date.now();
    const html = allTraces
        .map((trace) => {
            const ageMs = now - trace.startTime;
            let ageStr;
            if (ageMs < 60000) {
                ageStr = `${Math.round(ageMs / 1000)}s ago`;
            } else if (ageMs < 3600000) {
                ageStr = `${Math.round(ageMs / 60000)}m ago`;
            } else {
                ageStr = `${Math.round(ageMs / 3600000)}h ago`;
            }

            const durationStr =
                trace.durationMs == null
                    ? '—'
                    : trace.durationMs >= 1000
                      ? `${(trace.durationMs / 1000).toFixed(1)}s`
                      : `${Math.round(trace.durationMs)}ms`;

            const dsUid = state.tempoDatasourceUid || 'tempo';
            const traceUrl = `/grafana/explore?schemaVersion=1&panes=${encodeURIComponent(
                JSON.stringify({
                    abc: {
                        datasource: dsUid,
                        queries: [{ refId: 'A', query: trace.traceID, queryType: 'traceql' }],
                        range: { from: 'now-1h', to: 'now' },
                    },
                }),
            )}&orgId=1`;

            const shortId = escapeHtml(trace.traceID ? trace.traceID.substring(0, 8) : '—');
            const a = (content) => `<a class="trace-link" href="${traceUrl}">${content}</a>`;

            return `<tr class="trace-row">
                <td class="log-timestamp">${a(escapeHtml(ageStr))}</td>
                <td class="trace-id">${a(shortId)}</td>
                <td class="trace-op">${a(escapeHtml(trace.name || '—'))}</td>
                <td>${a(escapeHtml(trace.service || '—'))}</td>
                <td class="trace-duration">${a(escapeHtml(durationStr))}</td>
            </tr>`;
        })
        .join('');

    tracesContent.innerHTML = html;
}

async function fetchAlerts() {
    if (!state.alertsEnabled) return;

    try {
        const resp = await fetch('/prometheus/api/v1/alerts');
        if (!resp.ok) {
            return;
        }

        const data = await resp.json();
        processAlertData(data);
    } catch (err) {
        console.error('Failed to fetch alerts:', err);
    }
}

function processAlertData(data) {
    const alerts = data.data?.alerts || [];

    // Count alerts by severity
    const counts = { warning: 0, critical: 0, page: 0 };
    // Track alerts per peer
    const peerAlerts = {};

    for (const alert of alerts) {
        if (alert.state === 'firing') {
            const severity = alert.labels?.severity || 'warning';

            // Count alert by severity
            if (Object.hasOwn(counts, severity)) {
                counts[severity]++;
            }

            // Extract peer name from instance label (format: name.tunnelmesh:port or mesh_ip:port)
            const instance = alert.labels?.instance || '';
            const peerMatch = instance.match(/^([^.:]+)/);
            if (peerMatch) {
                const peerName = peerMatch[1];
                if (!peerAlerts[peerName]) {
                    peerAlerts[peerName] = { warning: 0, critical: 0, page: 0 };
                }
                if (Object.hasOwn(peerAlerts[peerName], severity)) {
                    peerAlerts[peerName][severity]++;
                }
            }
        }
    }

    state.peerAlerts = peerAlerts;
    updateAlertTiles(counts);
    // Re-render peers table to show alert badges
    renderPeersTable();
}

function updateAlertTiles(counts) {
    for (const severity of ['warning', 'critical', 'page']) {
        const countEl = document.getElementById(`alert-count-${severity}`);
        const tileEl = document.getElementById(`alert-tile-${severity}`);

        if (countEl) countEl.textContent = counts[severity];

        if (tileEl) {
            if (counts[severity] > 0) {
                tileEl.classList.add('active');
            } else {
                tileEl.classList.remove('active');
            }
        }
    }
}

// ============= PACKET FILTER FUNCTIONS =============

// Populate the peer dropdown for filter rules
function populateFilterPeerSelect(peers) {
    if (!dom.filterPeerSelect) return;

    // Build new peer list for comparison - skip rebuild if unchanged
    const newPeerList = peers.map((p) => p.name).join(',');
    const currentOptions = Array.from(dom.filterPeerSelect.options);
    const currentPeerList = currentOptions.map((o) => o.value).join(',');

    if (newPeerList === currentPeerList) {
        // Peer list unchanged, skip rebuild to avoid disrupting user interaction
        return;
    }

    const currentValue = dom.filterPeerSelect.value;
    dom.filterPeerSelect.innerHTML = '';

    peers.forEach((peer) => {
        const opt = document.createElement('option');
        opt.value = peer.name;
        opt.textContent = peer.name;
        dom.filterPeerSelect.appendChild(opt);
    });

    // Restore selection if it still exists, otherwise use global selection or first peer
    if (currentValue && peers.some((p) => p.name === currentValue)) {
        dom.filterPeerSelect.value = currentValue;
    } else if (state.selectedNodeId && peers.some((p) => p.name === state.selectedNodeId)) {
        dom.filterPeerSelect.value = state.selectedNodeId;
        loadFilterRules();
    } else if (peers.length > 0 && !dom.filterPeerSelect.value) {
        dom.filterPeerSelect.value = peers[0].name;
        loadFilterRules();
    }
}

// Load filter rules for selected peer
async function loadFilterRules() {
    const peerName = dom.filterPeerSelect?.value;

    if (!peerName) {
        if (dom.filterRulesBody) dom.filterRulesBody.innerHTML = '';
        if (dom.noFilterRules) dom.noFilterRules.style.display = 'block';
        if (dom.filterInfo) dom.filterInfo.style.display = 'none';
        return;
    }

    try {
        const resp = await fetch(`/api/filter/rules?peer=${encodeURIComponent(peerName)}`);
        if (!resp.ok) {
            showToast('Failed to load filter rules', 'error');
            return;
        }

        const data = await resp.json();
        renderFilterRules(data);
    } catch (err) {
        console.error('Failed to load filter rules:', err);
        showToast('Failed to load filter rules', 'error');
    }
}
window.loadFilterRules = loadFilterRules;

// Render filter rules table
function renderFilterRules(data) {
    if (!dom.filterRulesBody) return;

    // Show default policy
    if (dom.filterInfo) {
        dom.filterInfo.style.display = 'block';
    }
    if (dom.filterDefaultPolicy) {
        const policyText = data.default_deny ? 'deny' : 'allow';
        const modeText = data.default_deny ? 'allowlist mode' : 'blocklist mode';
        dom.filterDefaultPolicy.innerHTML = `<span class="action-badge ${policyText}">${policyText}</span> <span class="text-muted">(${modeText})</span>`;
    }

    if (!data.rules || data.rules.length === 0) {
        dom.filterRulesBody.innerHTML = '';
        if (dom.filterTempWarning) dom.filterTempWarning.style.display = 'none';
        if (dom.noFilterRules) {
            dom.noFilterRules.style.display = 'block';
            // Show error message if peer query failed
            if (data.error) {
                dom.noFilterRules.innerHTML = `<span class="text-warning">⚠ ${data.error}</span>`;
            } else {
                dom.noFilterRules.textContent = 'No filter rules configured for this peer.';
            }
        }
        return;
    }

    if (dom.noFilterRules) dom.noFilterRules.style.display = 'none';

    // Check for temporary rules and show warning if any exist (but not if S3 is enabled)
    const hasTemporaryRules = data.rules.some((r) => r.source === 'temporary');
    const shouldShowWarning = hasTemporaryRules && !data.s3_enabled;
    if (dom.filterTempWarning) {
        dom.filterTempWarning.style.display = shouldShowWarning ? 'block' : 'none';
    }

    // Sort rules: coordinator first, then config, temporary, service
    const sourceOrder = { coordinator: 0, config: 1, temporary: 2, service: 3 };
    const sortedRules = [...data.rules].sort((a, b) => {
        const orderA = sourceOrder[a.source] ?? 99;
        const orderB = sourceOrder[b.source] ?? 99;
        if (orderA !== orderB) return orderA - orderB;
        // Within same source, sort by port
        return a.port - b.port;
    });

    dom.filterRulesBody.innerHTML = sortedRules
        .map((rule) => {
            const sourcePeerDisplay = rule.source_peer ? rule.source_peer : '<span class="text-muted">Any</span>';
            const sourcePeerEscaped = rule.source_peer || '';
            const expiresDisplay =
                rule.expires === 0
                    ? '<span class="text-muted">Permanent</span>'
                    : TM.format.formatExpiry(new Date(rule.expires * 1000));
            return `
        <tr>
            <td>${rule.port}</td>
            <td>${rule.protocol.toUpperCase()}</td>
            <td><span class="action-badge ${rule.action}">${rule.action}</span></td>
            <td>${sourcePeerDisplay}</td>
            <td>${rule.source}</td>
            <td>${expiresDisplay}</td>
            <td>
                ${
                    rule.source === 'temporary'
                        ? `<button class="btn-danger" onclick="removeFilterRule('${data.peer}', ${rule.port}, '${rule.protocol}', '${sourcePeerEscaped}')">Remove</button>`
                        : '<span class="text-muted">-</span>'
                }
            </td>
        </tr>
    `;
        })
        .join('');
}

// Populate the source peer dropdown in the filter modal
function populateSourcePeerSelect() {
    const select = document.getElementById('filter-rule-source-peer');
    if (!select) return;

    select.innerHTML = '<option value="">Any peer (global rule)</option>';

    // Add all known peers
    const peers = state.currentPeers || [];
    const sortedPeers = [...peers].sort((a, b) => a.name.localeCompare(b.name));
    sortedPeers.forEach((peer) => {
        const opt = document.createElement('option');
        opt.value = peer.name;
        opt.textContent = `${peer.name} (${peer.mesh_ip})`;
        select.appendChild(opt);
    });
}

// Populate the destination peer dropdown in the filter modal
function populateDestPeerSelect() {
    const select = document.getElementById('filter-rule-dest-peer');
    if (!select) return;

    select.innerHTML = '<option value="__all__">All peers</option>';

    // Add all known peers
    const peers = state.currentPeers || [];
    const sortedPeers = [...peers].sort((a, b) => a.name.localeCompare(b.name));
    sortedPeers.forEach((peer) => {
        const opt = document.createElement('option');
        opt.value = peer.name;
        opt.textContent = `${peer.name} (${peer.mesh_ip})`;
        select.appendChild(opt);
    });
}

// Open filter rule modal
function openFilterModal() {
    // Populate peer dropdowns
    populateSourcePeerSelect();
    populateDestPeerSelect();
    filterModal.open();
}
window.openFilterModal = openFilterModal;

// Close filter rule modal
function closeFilterModal() {
    filterModal.close();
}
window.closeFilterModal = closeFilterModal;

// Add a filter rule
async function addFilterRule() {
    const destPeer = document.getElementById('filter-rule-dest-peer')?.value || '__all__';
    const port = parseInt(document.getElementById('filter-rule-port')?.value, 10);
    const protocol = document.getElementById('filter-rule-protocol')?.value;
    const action = document.getElementById('filter-rule-action')?.value;
    const sourcePeer = document.getElementById('filter-rule-source-peer')?.value || '';
    const ttl = parseInt(document.getElementById('filter-rule-ttl')?.value || '0', 10);

    if (!port || !protocol || !action) {
        showToast('Please fill in all fields', 'warning');
        return;
    }

    if (port < 1 || port > 65535) {
        showToast('Port must be between 1 and 65535', 'warning');
        return;
    }

    // Prevent self-targeting: a peer can't filter traffic from itself
    if (sourcePeer && destPeer !== '__all__' && sourcePeer === destPeer) {
        showToast('A peer cannot filter traffic from itself', 'warning');
        return;
    }

    // Confirm before pushing to all peers
    if (destPeer === '__all__') {
        const sourceDesc = sourcePeer ? ` from ${sourcePeer}` : '';
        if (!confirm(`Push "${action} ${port}/${protocol.toUpperCase()}${sourceDesc}" to ALL connected peers?`)) {
            return;
        }
    }

    try {
        const resp = await fetch('/api/filter/rules', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ peer: destPeer, port, protocol, action, source_peer: sourcePeer, ttl }),
        });

        if (!resp.ok) {
            const err = await resp.json();
            showToast(`Failed to add rule: ${err.message || 'Unknown error'}`, 'error');
            return;
        }

        const sourceDesc = sourcePeer ? ` from ${sourcePeer}` : '';
        const destDesc = destPeer === '__all__' ? 'all peers' : destPeer;
        showToast(
            `Filter rule sent to ${destDesc}: ${action} ${port}/${protocol.toUpperCase()}${sourceDesc}`,
            'success',
        );
        closeFilterModal();
        loadFilterRules();
        events.emit('panelDataChanged', 'filter');
    } catch (err) {
        console.error('Failed to add filter rule:', err);
        showToast('Failed to add filter rule', 'error');
    }
}
window.addFilterRule = addFilterRule;

// Remove a filter rule
async function removeFilterRule(peerName, port, protocol, sourcePeer) {
    const sourceDesc = sourcePeer ? ` from ${sourcePeer}` : '';
    if (!confirm(`Remove filter rule for port ${port}/${protocol.toUpperCase()}${sourceDesc}?`)) {
        return;
    }

    try {
        const resp = await fetch('/api/filter/rules', {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ peer: peerName, port, protocol, source_peer: sourcePeer || '' }),
        });

        if (!resp.ok) {
            showToast('Failed to remove rule', 'error');
            return;
        }

        showToast('Filter rule removed', 'success');
        loadFilterRules();
        events.emit('panelDataChanged', 'filter');
    } catch (err) {
        console.error('Failed to remove filter rule:', err);
        showToast('Failed to remove filter rule', 'error');
    }
}
window.removeFilterRule = removeFilterRule;

// ============= END PACKET FILTER FUNCTIONS =============

// Central function to select a node - emits event for all listeners
// Exposed globally so it can be called from HTML onclick handlers
function selectNode(nodeId) {
    state.selectedNodeId = nodeId;
    events.emit('nodeSelected', nodeId);
}
window.selectNode = selectNode;

// Initialize visualizer
function initVisualizer() {
    const canvas = document.getElementById('visualizer-canvas');
    if (!canvas) return;

    state.visualizer = new NodeVisualizer(canvas);
    state.visualizer.setDomainSuffix(state.domainSuffix);

    // Wire up visualizer selection to central event system
    state.visualizer.onNodeSelected = (nodeId) => {
        selectNode(nodeId);
    };

    // Subscribe visualizer to selection events (for external selection changes)
    events.on('nodeSelected', (nodeId) => {
        if (state.visualizer) {
            state.visualizer.setSelection(nodeId);
        }
    });
}

// Initialize map
function initMap() {
    const container = document.getElementById('node-map');
    if (!container) return;

    state.nodeMap = new NodeMap('node-map');
    // Map is initialized lazily when first peer with location is added

    // Subscribe map to selection events
    events.on('nodeSelected', (nodeId) => {
        if (state.nodeMap) {
            state.nodeMap.setSelectedPeer(nodeId);
        }
    });
}

// Initialize filter panel
function initFilterPanel() {
    // Subscribe filter to selection events
    events.on('nodeSelected', (nodeId) => {
        if (dom.filterPeerSelect && nodeId) {
            // Check if the peer exists in the dropdown
            const options = Array.from(dom.filterPeerSelect.options);
            if (options.some((opt) => opt.value === nodeId)) {
                dom.filterPeerSelect.value = nodeId;
                loadFilterRules();
            }
        }
    });
}

// Handle filter peer selection change - emit global selection event
function onFilterPeerChange() {
    const peerName = dom.filterPeerSelect?.value;
    if (peerName) {
        selectNode(peerName);
    }
    loadFilterRules();
}
window.onFilterPeerChange = onFilterPeerChange;

// Register built-in panels with the panel system
function registerBuiltinPanels() {
    if (!TM.panel) return;

    const panels = [
        {
            id: 'visualizer',
            sectionId: 'visualizer-section',
            tab: 'mesh',
            title: 'Network Topology',
            category: 'network',
            collapsible: true,
            sortOrder: 10,
        },
        {
            id: 'map',
            sectionId: 'map-section',
            tab: 'mesh',
            title: 'Node Locations',
            category: 'network',
            collapsible: true,
            sortOrder: 20,
        },
        {
            id: 'alerts',
            sectionId: 'alerts-section',
            tab: 'mesh',
            title: 'Active Alerts',
            category: 'monitoring',
            collapsible: false,
            sortOrder: 35,
        },
        {
            id: 'peers',
            sectionId: 'peers-section',
            tab: 'mesh',
            title: 'Connected Peers',
            category: 'network',
            collapsible: true,
            sortOrder: 40,
        },
        {
            id: 'logs',
            sectionId: 'logs-section',
            tab: 'mesh',
            title: 'Peer Logs',
            category: 'monitoring',
            collapsible: true,
            resizable: true,
            sortOrder: 50,
        },
        {
            id: 'tracing',
            sectionId: 'traces-section',
            tab: 'mesh',
            title: 'Recent Traces',
            category: 'monitoring',
            collapsible: true,
            sortOrder: 60,
        },
        {
            id: 'filter',
            sectionId: 'filter-section',
            tab: 'mesh',
            title: 'Packet Filter',
            category: 'admin',
            hasActionButton: true,
            sortOrder: 70,
        },
        { id: 'dns', sectionId: 'dns-section', tab: 'data', title: 'DNS Records', category: 'data', sortOrder: 80 },
        {
            id: 's3',
            sectionId: 's3-section',
            tab: 'app',
            title: 'Object Viewer',
            category: 'storage',
            resizable: true,
            sortOrder: 10,
        },
        {
            id: 'shares',
            sectionId: 'shares-section',
            tab: 'app',
            title: 'Shares',
            category: 'storage',
            hasActionButton: true,
            sortOrder: 20,
        },
        {
            id: 'peers-mgmt',
            sectionId: 'peers-mgmt-section',
            tab: 'data',
            title: 'Peers',
            category: 'admin',
            sortOrder: 30,
        },
        {
            id: 'groups',
            sectionId: 'groups-section',
            tab: 'data',
            title: 'Groups',
            category: 'admin',
            hasActionButton: true,
            sortOrder: 40,
        },
        {
            id: 'bindings',
            sectionId: 'bindings-section',
            tab: 'data',
            title: 'Role Bindings',
            category: 'admin',
            hasActionButton: true,
            sortOrder: 50,
        },
        {
            id: 'docker',
            sectionId: 'docker-section',
            tab: 'app',
            title: 'Docker Containers',
            category: 'admin',
            hasActionButton: true,
            sortOrder: 30,
        },
    ];

    panels.forEach((p) => {
        try {
            TM.panel.registerPanel(p);
        } catch (err) {
            console.warn(`Failed to register panel ${p.id}:`, err);
        }
    });
}

// Load external panels from API
async function loadExternalPanels() {
    if (!TM.panel) return;

    try {
        const resp = await fetch('/api/panels?external=true');
        if (resp.ok) {
            const data = await resp.json();
            if (data.panels) {
                for (const p of data.panels) {
                    TM.panel.registerExternalPanel(p);
                }
            }
        }
    } catch (err) {
        console.warn('Failed to load external panels:', err);
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', async () => {
    // Cache DOM elements first
    initDOMCache();

    // Register panels and initialize panel system
    registerBuiltinPanels();
    await loadExternalPanels();
    if (TM.panel) {
        await TM.panel.init();
    }

    // Initialize tab navigation
    initTabs();

    // Initialize refresh coordinator
    initRefreshCoordinator();

    // Initialize visualizer
    initVisualizer();

    // Initialize map (for geolocation display)
    initMap();

    // Initialize filter panel
    initFilterPanel();

    // Setup real-time updates via SSE
    setupSSE();

    // Initial data fetch
    fetchData();

    // Check if Prometheus is available for alerts
    checkPrometheusAvailable();

    // Check if Loki is available for logs
    checkLokiAvailable();

    // Check if Tempo is available for distributed traces
    checkTempoAvailable();

    // Load peer management (peers, groups, shares)
    checkPeerManagement();

    // Add filter rule button handler
    if (dom.addFilterRuleBtn) {
        dom.addFilterRuleBtn.addEventListener('click', openFilterModal);
    }

    // Setup modal background click handlers
    filterModal.setupBackgroundClose();

    // Setup background click for group modal
    const groupModal = document.getElementById('group-modal');
    if (groupModal) {
        groupModal.addEventListener('click', (e) => {
            if (e.target === groupModal) closeGroupModal();
        });
    }

    // Setup background click for share modal
    const shareModal = document.getElementById('share-modal');
    if (shareModal) {
        shareModal.addEventListener('click', (e) => {
            if (e.target === shareModal) closeShareModal();
        });
    }

    // Setup background click for binding modal
    const bindingModal = document.getElementById('binding-modal');
    if (bindingModal) {
        bindingModal.addEventListener('click', (e) => {
            if (e.target === bindingModal) closeBindingModal();
        });
    }

    // Setup background click for manage groups modal
    const manageGroupsModal = document.getElementById('manage-groups-modal');
    if (manageGroupsModal) {
        manageGroupsModal.addEventListener('click', (e) => {
            if (e.target === manageGroupsModal) closeManageGroupsModal();
        });
    }

    // Panel resize handles
    if (dom.logsResizeHandle && dom.logsContainer) {
        initPanelResize(dom.logsResizeHandle, dom.logsContainer);
    }
    if (dom.s3ResizeHandle && dom.s3Content) {
        initPanelResize(dom.s3ResizeHandle, dom.s3Content);
    }

    // Trigger visualizer resize after DOM is fully rendered
    // This ensures the visualizer displays correctly on initial page load
    requestAnimationFrame(() => {
        if (state.visualizer) {
            state.visualizer.resize();
        }
    });

    // Apply initial tab filtering to hide panels not in active tab
    // This must happen BEFORE refresh triggers to avoid showing all panels briefly
    const activeTab = document.querySelector('#main-tabs .tab.active');
    if (activeTab) {
        const tabName = activeTab.dataset.tab;
        // Apply tab filtering immediately
        switchTab(tabName);
    }

    // Trigger initial refresh for the active tab
    // This ensures panels show their content on page load, not just after tab switch
    if (activeTab && TM.refresh) {
        const tabName = activeTab.dataset.tab;
        if (tabName === 'app') {
            TM.refresh.triggerMultiple(PANELS_APP_TAB, { cascade: false });
        } else if (tabName === 'data') {
            TM.refresh.triggerMultiple(PANELS_DATA_TAB, { cascade: false });
        } else if (tabName === 'mesh') {
            TM.refresh.triggerMultiple(PANELS_MESH_TAB, { cascade: false });
        }
    }
});

// Initialize panel resize functionality (shared by logs and S3 panels)
function initPanelResize(handle, container) {
    const MIN_HEIGHT = 100;
    const MAX_HEIGHT = 800;

    let isResizing = false;
    let startY = 0;
    let startHeight = 0;

    handle.addEventListener('mousedown', (e) => {
        isResizing = true;
        startY = e.clientY;
        startHeight = container.offsetHeight;
        document.body.style.cursor = 'ns-resize';
        document.body.style.userSelect = 'none';
        e.preventDefault();
    });

    document.addEventListener('mousemove', (e) => {
        if (!isResizing) return;
        const delta = e.clientY - startY;
        const newHeight = Math.min(MAX_HEIGHT, Math.max(MIN_HEIGHT, startHeight + delta));
        container.style.height = `${newHeight}px`;
    });

    document.addEventListener('mouseup', () => {
        if (isResizing) {
            isResizing = false;
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        }
    });

    // Touch support for mobile
    handle.addEventListener(
        'touchstart',
        (e) => {
            isResizing = true;
            startY = e.touches[0].clientY;
            startHeight = container.offsetHeight;
            e.preventDefault();
        },
        { passive: false },
    );

    document.addEventListener(
        'touchmove',
        (e) => {
            if (!isResizing) return;
            const delta = e.touches[0].clientY - startY;
            const newHeight = Math.min(MAX_HEIGHT, Math.max(MIN_HEIGHT, startHeight + delta));
            container.style.height = `${newHeight}px`;
        },
        { passive: true },
    );

    document.addEventListener('touchend', () => {
        isResizing = false;
    });
}

// --- Users, Groups, and Shares Management ---

// Check if S3/peer management is enabled and show sections
async function checkPeerManagement() {
    // Check if S3/peer management is enabled
    // Panel visibility is managed by the panel system, not here
    // Data loading is handled by refresh coordinator when data tab is accessed
    try {
        const resp = await fetch('/api/users');
        if (!resp.ok) {
            // S3 not enabled - panels will be hidden by panel system
        }
    } catch (_err) {
        // Peer management not enabled
    }
}

async function fetchPeersMgmt() {
    try {
        const resp = await fetch('/api/users');
        if (resp.ok) {
            const peers = await resp.json();
            state.currentPeersMgmt = peers || [];
            renderPeersMgmtTable();
        }
    } catch (err) {
        console.error('Failed to fetch peers:', err);
    }
}

function renderPeersMgmtTable() {
    const tbody = document.getElementById('peers-mgmt-body');
    const noPeers = document.getElementById('no-peers-mgmt');
    const peers = state.currentPeersMgmt;

    if (!peers || peers.length === 0) {
        tbody.innerHTML = '';
        noPeers.style.display = 'block';
        document.getElementById('peers-mgmt-pagination').style.display = 'none';
        return;
    }

    noPeers.style.display = 'none';
    const visiblePeers = peersMgmtPagination.getVisibleItems();
    tbody.innerHTML = visiblePeers
        .map((p) => {
            const groups = (p.groups || []).filter((g) => g !== 'everyone');
            const groupsDisplay = groups.length > 0 ? groups.map((g) => escapeHtml(g)).join(', ') : '-';
            return `
        <tr>
            <td>${escapeHtml(p.name || '-')}</td>
            <td>${groupsDisplay}</td>
            <td>${p.last_seen ? formatLastSeen(p.last_seen) : '-'}</td>
            <td>${p.is_service ? 'Never' : p.expires_at ? formatExpiry(p.expires_at) : '-'}</td>
            <td><span class="status-badge ${p.expired ? 'expired' : 'active'}">${p.expired ? 'Expired' : 'Active'}</span></td>
            <td><button class="btn-small btn-primary" onclick="openManageGroupsModal('${escapeHtml(p.id)}')">Groups</button></td>
        </tr>
    `;
        })
        .join('');

    updateSectionPagination('peers-mgmt', peersMgmtPagination);
}

// Alias for backward compat
function _updatePeersMgmtTable(peers) {
    state.currentPeersMgmt = peers || [];
    renderPeersMgmtTable();
}

async function fetchGroups() {
    try {
        const resp = await fetch('/api/groups');
        if (resp.ok) {
            const groups = await resp.json();
            state.currentGroups = groups || [];
            renderGroupsTable();
        }
    } catch (err) {
        console.error('Failed to fetch groups:', err);
    }
}

function renderGroupsTable() {
    const tbody = document.getElementById('groups-body');
    const noGroups = document.getElementById('no-groups');
    const groups = state.currentGroups;

    if (!groups || groups.length === 0) {
        tbody.innerHTML = '';
        noGroups.style.display = 'block';
        document.getElementById('groups-pagination').style.display = 'none';
        return;
    }

    noGroups.style.display = 'none';
    const visibleGroups = groupsPagination.getVisibleItems();
    tbody.innerHTML = visibleGroups
        .map(
            (g) => `
        <tr>
            <td><strong>${escapeHtml(g.name)}</strong></td>
            <td>${escapeHtml(g.description || '-')}</td>
            <td>${g.members ? g.members.length : 0}</td>
            <td>
                ${g.builtin ? '<span class="text-muted" title="Built-in group">Protected</span>' : `<button class="btn-small btn-danger" onclick="deleteGroup('${escapeHtml(g.name)}')">Delete</button>`}
            </td>
        </tr>
    `,
        )
        .join('');

    updateSectionPagination('groups', groupsPagination);
}

function _updateGroupsTable(groups) {
    state.currentGroups = groups || [];
    renderGroupsTable();
}

function openGroupModal() {
    document.getElementById('group-modal').style.display = 'flex';
    document.getElementById('group-name').value = '';
    document.getElementById('group-description').value = '';
    document.getElementById('group-name').focus();
}
window.openGroupModal = openGroupModal;

function closeGroupModal() {
    document.getElementById('group-modal').style.display = 'none';
}
window.closeGroupModal = closeGroupModal;

async function createGroup() {
    const name = document.getElementById('group-name').value.trim();
    const description = document.getElementById('group-description').value.trim();

    if (!name) {
        showToast('Group name is required', 'error');
        return;
    }

    try {
        const resp = await fetch('/api/groups', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, description }),
        });

        if (resp.ok) {
            showToast(`Group "${name}" created`, 'success');
            closeGroupModal();
            TM.refresh.trigger('groups');
            events.emit('panelDataChanged', 'groups');
        } else {
            const data = await resp.json();
            showToast(data.error || 'Failed to create group', 'error');
        }
    } catch (err) {
        showToast(`Failed to create group: ${err.message}`, 'error');
    }
}
window.createGroup = createGroup;

async function deleteGroup(name) {
    if (!confirm(`Delete group "${name}"?`)) return;

    try {
        const resp = await fetch(`/api/groups/${name}`, { method: 'DELETE' });
        if (resp.ok) {
            showToast(`Group "${name}" deleted`, 'success');
            TM.refresh.trigger('groups');
            events.emit('panelDataChanged', 'groups');
        } else {
            const data = await resp.json();
            showToast(data.error || 'Failed to delete group', 'error');
        }
    } catch (err) {
        showToast(`Failed to delete group: ${err.message}`, 'error');
    }
}
window.deleteGroup = deleteGroup;

// =====================
// Peer Group Management
// =====================

let _managingPeer = null; // { id, name, groups }

function openManageGroupsModal(peerId) {
    const peer = state.currentPeersMgmt.find((p) => p.id === peerId);
    if (!peer) return;
    _managingPeer = { id: peer.id, name: peer.name, groups: [...(peer.groups || [])] };
    document.getElementById('manage-groups-title').textContent = `Groups — ${peer.name}`;
    _renderManageGroupsList();
    _populateManageGroupsDropdown();
    document.getElementById('manage-groups-modal').style.display = 'flex';
}
window.openManageGroupsModal = openManageGroupsModal;

function closeManageGroupsModal() {
    document.getElementById('manage-groups-modal').style.display = 'none';
    _managingPeer = null;
}
window.closeManageGroupsModal = closeManageGroupsModal;

function _renderManageGroupsList() {
    const el = document.getElementById('manage-groups-list');
    const groups = _managingPeer.groups || [];
    if (groups.length === 0) {
        el.innerHTML = '<p class="text-muted manage-groups-empty">No groups.</p>';
        return;
    }
    const builtinNames = new Set((state.currentGroups || []).filter((g) => g.builtin).map((g) => g.name));
    el.innerHTML = groups
        .map((g) => {
            const isBuiltin = builtinNames.has(g);
            return `<div class="manage-group-row">
            <span>${escapeHtml(g)}</span>
            ${isBuiltin ? '<span class="text-muted manage-group-builtin">built-in</span>' : `<button class="btn-small btn-danger" onclick="removePeerFromGroup('${escapeHtml(g)}')">Remove</button>`}
        </div>`;
        })
        .join('');
}

function _populateManageGroupsDropdown() {
    const select = document.getElementById('manage-groups-select');
    const current = new Set(_managingPeer.groups || []);
    const available = (state.currentGroups || []).filter((g) => !current.has(g.name) && !g.builtin);
    select.innerHTML =
        available.length === 0
            ? '<option value="">No groups available</option>'
            : available.map((g) => `<option value="${escapeHtml(g.name)}">${escapeHtml(g.name)}</option>`).join('');
}

async function removePeerFromGroup(groupName) {
    if (!_managingPeer) return;
    try {
        const resp = await fetch(
            `/api/groups/${encodeURIComponent(groupName)}/members/${encodeURIComponent(_managingPeer.id)}`,
            { method: 'DELETE' },
        );
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        _managingPeer.groups = _managingPeer.groups.filter((g) => g !== groupName);
        _renderManageGroupsList();
        _populateManageGroupsDropdown();
        TM.refresh.trigger('peers-mgmt');
        showToast(`Removed from "${groupName}"`, 'success');
    } catch (err) {
        showToast(`Failed to remove from group: ${err.message}`, 'error');
    }
}
window.removePeerFromGroup = removePeerFromGroup;

async function addPeerToSelectedGroup() {
    if (!_managingPeer) return;
    const groupName = document.getElementById('manage-groups-select').value;
    if (!groupName) return;
    try {
        const resp = await fetch(`/api/groups/${encodeURIComponent(groupName)}/members`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user_id: _managingPeer.id }),
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        _managingPeer.groups.push(groupName);
        _renderManageGroupsList();
        _populateManageGroupsDropdown();
        TM.refresh.trigger('peers-mgmt');
        showToast(`Added to "${groupName}"`, 'success');
    } catch (err) {
        showToast(`Failed to add to group: ${err.message}`, 'error');
    }
}
window.addPeerToSelectedGroup = addPeerToSelectedGroup;

async function fetchShares() {
    try {
        const resp = await fetch('/api/shares');
        if (resp.ok) {
            const shares = await resp.json();
            state.currentShares = shares || [];
            renderSharesTable();
        }
    } catch (err) {
        console.error('Failed to fetch shares:', err);
    }
}

function renderSharesTable() {
    const tbody = document.getElementById('shares-body');
    const noShares = document.getElementById('no-shares');
    const shares = state.currentShares;

    if (!shares || shares.length === 0) {
        tbody.innerHTML = '';
        noShares.style.display = 'block';
        document.getElementById('shares-pagination').style.display = 'none';
        return;
    }

    noShares.style.display = 'none';
    const visibleShares = sharesPagination.getVisibleItems();
    tbody.innerHTML = visibleShares
        .map(
            (s) => `
        <tr>
            <td><strong>${escapeHtml(s.name)}</strong></td>
            <td>${escapeHtml(s.description || '-')}</td>
            <td>${escapeHtml(s.owner_name || s.owner || '-')}</td>
            <td>${s.quota_bytes ? formatBytes(s.quota_bytes) : 'Unlimited'}</td>
            <td>${s.created_at ? new Date(s.created_at).toLocaleDateString() : '-'}</td>
            <td>${s.expires_at ? formatExpiry(s.expires_at) : 'Never'}</td>
            <td>
                <button class="btn-small btn-danger" onclick="deleteShare('${escapeHtml(s.name)}')">Delete</button>
            </td>
        </tr>
    `,
        )
        .join('');

    updateSectionPagination('shares', sharesPagination);
}

function _updateSharesTable(shares) {
    state.currentShares = shares || [];
    renderSharesTable();
}

function openShareModal() {
    document.getElementById('share-modal').style.display = 'flex';
    document.getElementById('share-name').value = '';
    document.getElementById('share-description').value = '';
    document.getElementById('share-quota').value = '';
    document.getElementById('share-replication').value = '2'; // Default to 2
    document.getElementById('share-expires').value = '';
    document.getElementById('share-guest-read').checked = true;
    document.getElementById('share-name').focus();
}
window.openShareModal = openShareModal;

function closeShareModal() {
    document.getElementById('share-modal').style.display = 'none';
}
window.closeShareModal = closeShareModal;

async function createShare() {
    const name = document.getElementById('share-name').value.trim();
    const description = document.getElementById('share-description').value.trim();
    const quotaValue = document.getElementById('share-quota').value.trim();
    const expiresValue = document.getElementById('share-expires').value;
    const guestRead = document.getElementById('share-guest-read').checked;
    const replicationFactor = parseInt(document.getElementById('share-replication').value, 10);
    // Default to 100 MB if empty, but allow explicit 0 for unlimited
    const quotaMB = quotaValue === '' ? 100 : parseInt(quotaValue, 10) || 0;

    if (!name) {
        showToast('Share name is required', 'error');
        return;
    }

    // Convert MB to bytes (0 means unlimited)
    const quota_bytes = quotaMB > 0 ? quotaMB * 1024 * 1024 : 0;

    // Build request body
    const body = {
        name,
        description,
        quota_bytes,
        guest_read: guestRead,
        replication_factor: replicationFactor,
    };
    if (expiresValue) {
        body.expires_at = expiresValue; // Send as YYYY-MM-DD, backend will parse
    }

    try {
        const resp = await fetch('/api/shares', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });

        if (resp.ok) {
            showToast(`File share "${name}" created`, 'success');
            closeShareModal();
            // Trigger refresh for shares panel (will cascade to bindings and S3)
            TM.refresh.trigger('shares');
            events.emit('panelDataChanged', 'shares');
        } else {
            const data = await resp.json();
            showToast(data.error || 'Failed to create share', 'error');
        }
    } catch (err) {
        showToast(`Failed to create share: ${err.message}`, 'error');
    }
}
window.createShare = createShare;

async function deleteShare(name) {
    if (!confirm(`Delete file share "${name}"? All files will be deleted.`)) return;

    try {
        const resp = await fetch(`/api/shares/${name}`, { method: 'DELETE' });
        if (resp.ok) {
            showToast(`File share "${name}" deleted`, 'success');
            // Trigger refresh for shares panel (will cascade to bindings and S3)
            TM.refresh.trigger('shares');
            events.emit('panelDataChanged', 'shares');
        } else {
            const data = await resp.json();
            showToast(data.error || 'Failed to delete share', 'error');
        }
    } catch (err) {
        showToast(`Failed to delete share: ${err.message}`, 'error');
    }
}
window.deleteShare = deleteShare;

// =====================
// Refresh Coordinator
// =====================

function initRefreshCoordinator() {
    if (!TM.refresh) {
        console.warn('TM.refresh not loaded, panel refresh coordination disabled');
        return;
    }

    // Register all panel refresh functions
    TM.refresh.register('peers', () => fetchData(false));
    TM.refresh.register('logs', checkLokiAvailable);
    TM.refresh.register('tracing', checkTempoAvailable);
    TM.refresh.register('alerts', fetchAlerts);
    TM.refresh.register('filter', loadFilterRules);
    TM.refresh.register('peers-mgmt', fetchPeersMgmt);
    TM.refresh.register('groups', fetchGroups);
    TM.refresh.register('shares', fetchShares);
    TM.refresh.register('bindings', fetchBindings);
    TM.refresh.register('dns', fetchDnsRecords);
    TM.refresh.register('docker', loadDockerContainers);

    // Register S3 explorer refresh (init if needed, then refresh)
    // Use promise chain instead of async/await to avoid blocking
    TM.refresh.register('s3', () => {
        initS3Explorer()
            .then(() => {
                if (TM.s3explorer) {
                    TM.s3explorer.refresh();
                }
            })
            .catch((err) => {
                console.error('Failed to initialize S3 explorer:', err);
            });
    });

    // Define dependencies: when X changes, also refresh Y
    // When shares change, refresh bindings (share creation/deletion modifies bindings)
    // and S3 explorer (shares create S3 buckets)
    TM.refresh.addDependency('shares', ['bindings', 's3']);

    // When bindings change, refresh S3 explorer (permissions might have changed)
    TM.refresh.addDependency('bindings', ['s3']);

    // When peers-mgmt (users) changes, refresh groups and bindings (user changes affect group/role assignments)
    // Note: Removed groups → peers-mgmt to avoid circular dependency
    TM.refresh.addDependency('peers-mgmt', ['groups', 'bindings']);
}

// Tab Navigation
// =====================

function initTabs() {
    const tabs = document.querySelectorAll('#main-tabs .tab');
    tabs.forEach((tab) => {
        tab.addEventListener('click', () => {
            const tabName = tab.dataset.tab;
            switchTab(tabName);
        });
    });

    // Restore tab from URL hash on initial load
    const hash = window.location.hash.slice(1);
    const params = new URLSearchParams(hash);
    const tabName = params.get('tab');

    if (tabName && ['app', 'data', 'mesh'].includes(tabName)) {
        switchTab(tabName, { skipHistory: true });
    }

    // Handle browser back/forward navigation
    window.addEventListener('popstate', () => {
        const popHash = window.location.hash.slice(1);
        const popParams = new URLSearchParams(popHash);
        const popTab = popParams.get('tab');
        if (popTab && ['app', 'data', 'mesh'].includes(popTab)) {
            switchTab(popTab, { skipHistory: true });
        }
    });
}

/**
 * Update browser history with tab change
 */
function updateTabHistory(tabName) {
    // Parse current hash to preserve S3 state
    const hash = window.location.hash.slice(1);
    const params = new URLSearchParams(hash);

    // Update tab parameter
    params.set('tab', tabName);

    const newHash = params.toString();
    const url = newHash ? `#${newHash}` : '#';

    window.history.pushState(null, '', url);
}

function switchTab(tabName, options = {}) {
    // Update tab buttons
    document.querySelectorAll('#main-tabs .tab').forEach((t) => {
        t.classList.toggle('active', t.dataset.tab === tabName);
    });

    // Show/hide sections based on their data-tab attribute
    // Panel system controls base visibility (permissions), we just add tab filtering
    // Sections can belong to multiple tabs via space-separated values (e.g., data-tab="app data")
    document.querySelectorAll('section[data-tab]').forEach((section) => {
        const tabs = section.dataset.tab.split(' ');
        const belongsToTab = tabs.includes(tabName);
        // Only show if: (1) belongs to active tab, AND (2) panel system hasn't hidden it
        if (belongsToTab) {
            // Remove tab-based hiding - panel system controls final visibility
            section.classList.remove('tab-hidden');
        } else {
            // Hide because it belongs to a different tab
            section.classList.add('tab-hidden');
        }
    });

    // Handle tab-specific initialization
    if (tabName === 'mesh') {
        // Snap visualizer to current positions on return — no animation replay
        if (state.visualizer) {
            state.visualizer.skipNextAnimation = true;
        }
        // Refresh mesh-tab panels (peers/logs/alerts/filter) on tab switch.
        // 'logs' calls checkLokiAvailable which retries if Grafana wasn't ready on page load.
        if (TM.refresh) {
            TM.refresh.triggerMultiple(PANELS_MESH_TAB, { cascade: false });
        }
        // Defer visualizer and map resize until after DOM updates to get correct dimensions
        requestAnimationFrame(() => {
            if (state.visualizer) {
                state.visualizer.resize();
            }
            if (state.nodeMap) {
                // Use refresh() instead of just invalidateSize() to handle both
                // tile rendering and centering when returning to tab
                state.nodeMap.refresh();
            }
        });
    } else if (tabName === 'data') {
        // Refresh all data panels without cascading (we're already listing all of them)
        TM.refresh.triggerMultiple(PANELS_DATA_TAB, { cascade: false });
    } else if (tabName === 'app') {
        // Refresh app tab panels
        if (TM.refresh) {
            TM.refresh.triggerMultiple(PANELS_APP_TAB, { cascade: false });
        }
    }

    // Update browser history (unless restoring from history)
    if (!options.skipHistory) {
        updateTabHistory(tabName);
    }
}
window.switchTab = switchTab;

// =====================
// Role Bindings
// =====================

async function fetchBindings() {
    try {
        const resp = await fetch('/api/bindings');
        if (!resp.ok) return;
        const bindings = await resp.json();
        // Sort by user/group name
        (bindings || []).sort((a, b) => {
            const aName = (a.user_id || a.group_name || '').toLowerCase();
            const bName = (b.user_id || b.group_name || '').toLowerCase();
            return aName.localeCompare(bName);
        });
        state.currentBindings = bindings || [];
        renderBindingsTable();
    } catch (err) {
        console.error('Failed to fetch bindings:', err);
    }
}

function renderBindingsTable() {
    const tbody = document.getElementById('bindings-body');
    const emptyState = document.getElementById('no-bindings');
    const bindings = state.currentBindings;

    if (!bindings || bindings.length === 0) {
        tbody.innerHTML = '';
        emptyState.style.display = 'block';
        document.getElementById('bindings-pagination').style.display = 'none';
        return;
    }

    emptyState.style.display = 'none';
    const visibleBindings = bindingsPagination.getVisibleItems();
    tbody.innerHTML = visibleBindings
        .map(
            (b) => `
        <tr>
            <td>${escapeHtml(b.user_id || b.group_name || '-')}</td>
            <td>${escapeHtml(b.role_name)}</td>
            <td>${escapeHtml(b.bucket_scope || 'All')}</td>
            <td>${escapeHtml(b.object_prefix || '-')}</td>
            <td>${escapeHtml(b.panel_scope || '-')}</td>
            <td>${b.created_at ? TM.format.formatRelativeTime(b.created_at) : '-'}</td>
            <td>
                ${
                    b.protected
                        ? '<span class="text-muted" title="File share owner binding">Protected</span>'
                        : `<button class="btn-small btn-danger" onclick="deleteBinding('${escapeHtml(b.name)}')">Delete</button>`
                }
            </td>
        </tr>
    `,
        )
        .join('');

    updateSectionPagination('bindings', bindingsPagination);
}

// Alias for backward compat
function _renderBindings(bindings) {
    state.currentBindings = bindings || [];
    renderBindingsTable();
}

function openBindingModal() {
    document.getElementById('binding-modal').style.display = 'flex';
    document.getElementById('binding-bucket').value = '';
    document.getElementById('binding-prefix').value = '';

    // Populate peer dropdown
    populateBindingPeers();
}
window.openBindingModal = openBindingModal;

async function populateBindingPeers() {
    const select = document.getElementById('binding-peer');
    select.innerHTML = '<option value="">Select a peer...</option>';

    try {
        const resp = await fetch('/api/users');
        if (resp.ok) {
            const peers = await resp.json();
            peers.forEach((p) => {
                const opt = document.createElement('option');
                opt.value = p.id;
                opt.textContent = p.name || p.id;
                select.appendChild(opt);
            });
        }
    } catch (err) {
        console.error('Failed to fetch peers for binding:', err);
    }
}

function closeBindingModal() {
    document.getElementById('binding-modal').style.display = 'none';
}
window.closeBindingModal = closeBindingModal;

async function createBinding() {
    const peerId = document.getElementById('binding-peer').value;
    const roleName = document.getElementById('binding-role').value;
    const bucketScope = document.getElementById('binding-bucket').value.trim();
    const objectPrefix = document.getElementById('binding-prefix').value.trim();

    if (!peerId) {
        showToast('Peer is required', 'error');
        return;
    }

    try {
        const resp = await fetch('/api/bindings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                user_id: peerId,
                role_name: roleName,
                bucket_scope: bucketScope,
                object_prefix: objectPrefix,
            }),
        });

        if (resp.ok) {
            showToast('Role binding created', 'success');
            closeBindingModal();
            TM.refresh.trigger('bindings');
            events.emit('panelDataChanged', 'bindings');
        } else {
            const data = await resp.json();
            showToast(data.error || 'Failed to create binding', 'error');
        }
    } catch (err) {
        showToast(`Failed to create binding: ${err.message}`, 'error');
    }
}
window.createBinding = createBinding;

// ============================
// Bucket Creation Modal
// ============================

function openBucketModal() {
    document.getElementById('bucket-modal').style.display = 'flex';
    document.getElementById('bucket-name').value = '';
    document.getElementById('bucket-replication').value = '2'; // Default
    document.getElementById('bucket-quota').value = '';
    document.getElementById('bucket-name').focus();
}
window.openBucketModal = openBucketModal;

function closeBucketModal() {
    document.getElementById('bucket-modal').style.display = 'none';
}
window.closeBucketModal = closeBucketModal;

async function createBucket() {
    const name = document.getElementById('bucket-name').value.trim();
    const replicationFactor = parseInt(document.getElementById('bucket-replication').value, 10);
    const quotaMB = parseInt(document.getElementById('bucket-quota').value, 10) || 0;

    // Validate bucket name
    if (!name) {
        showToast('Bucket name is required', 'error');
        return;
    }

    // DNS-safe validation (alphanumeric + hyphens)
    if (!/^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/.test(name)) {
        showToast('Invalid bucket name. Use lowercase letters, numbers, and hyphens (1-63 chars)', 'error');
        return;
    }

    try {
        const response = await fetch('/api/s3/buckets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: name,
                replication_factor: replicationFactor,
                quota_bytes: quotaMB > 0 ? quotaMB * 1024 * 1024 : 0,
            }),
        });

        if (!response.ok) {
            const error = await response.text();
            throw new Error(error || 'Failed to create bucket');
        }

        showToast(`Bucket "${name}" created successfully`, 'success');
        closeBucketModal();

        // Refresh S3 explorer to show new bucket
        if (window.TM && TM.refresh) {
            TM.refresh.trigger('s3');
        }
    } catch (err) {
        console.error('Create bucket error:', err);
        showToast(`Failed to create bucket: ${err.message}`, 'error');
    }
}
window.createBucket = createBucket;

// ============================
// Bucket Properties Modal (Admin-Only)
// ============================

async function openBucketPropertiesModal(bucketName) {
    // Fetch full bucket metadata
    try {
        const response = await fetch(`/api/s3/buckets/${encodeURIComponent(bucketName)}`);
        if (!response.ok) throw new Error('Failed to fetch bucket metadata');

        const metadata = await response.json();

        // Populate modal fields
        document.getElementById('prop-bucket-name').value = metadata.name;
        document.getElementById('prop-bucket-owner').value = metadata.owner || 'N/A';
        document.getElementById('prop-bucket-created').value = new Date(metadata.created_at).toLocaleString();
        document.getElementById('prop-bucket-replication').value = metadata.replication_factor || 2;

        // Store bucket name for save operation
        document.getElementById('bucket-properties-modal').dataset.bucketName = metadata.name;

        // Show modal
        document.getElementById('bucket-properties-modal').style.display = 'flex';
    } catch (err) {
        console.error('Failed to load bucket properties:', err);
        showToast('Failed to load bucket properties', 'error');
    }
}
window.openBucketPropertiesModal = openBucketPropertiesModal;

function closeBucketPropertiesModal() {
    document.getElementById('bucket-properties-modal').style.display = 'none';
}
window.closeBucketPropertiesModal = closeBucketPropertiesModal;

async function saveBucketProperties() {
    const bucketName = document.getElementById('bucket-properties-modal').dataset.bucketName;
    const replicationFactor = parseInt(document.getElementById('prop-bucket-replication').value, 10);

    try {
        const response = await fetch(`/api/s3/buckets/${encodeURIComponent(bucketName)}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                replication_factor: replicationFactor,
            }),
        });

        if (!response.ok) {
            const error = await response.text();
            throw new Error(error || 'Failed to update bucket properties');
        }

        showToast('Bucket properties updated successfully', 'success');
        closeBucketPropertiesModal();

        // Refresh S3 explorer
        if (window.TM && TM.refresh) {
            TM.refresh.trigger('s3');
        }
    } catch (err) {
        console.error('Save bucket properties error:', err);
        showToast(`Failed to save: ${err.message}`, 'error');
    }
}
window.saveBucketProperties = saveBucketProperties;

async function deleteBinding(name) {
    if (!confirm('Delete this role binding?')) return;

    try {
        const resp = await fetch(`/api/bindings/${name}`, { method: 'DELETE' });
        if (resp.ok) {
            showToast('Role binding deleted', 'success');
            TM.refresh.trigger('bindings');
            events.emit('panelDataChanged', 'bindings');
        } else {
            const data = await resp.json();
            showToast(data.error || 'Failed to delete binding', 'error');
        }
    } catch (err) {
        showToast(`Failed to delete binding: ${err.message}`, 'error');
    }
}
window.deleteBinding = deleteBinding;

// Cleanup on page unload to prevent memory leaks
window.addEventListener('beforeunload', cleanup);

// =====================
// S3 Explorer Integration
// =====================

let s3ExplorerInitialized = false;

async function initS3Explorer() {
    if (s3ExplorerInitialized) return;

    // Check if S3 is available
    try {
        const resp = await fetch('/api/s3/buckets');
        if (resp.ok) {
            document.getElementById('s3-section').style.display = 'block';
            if (typeof TM !== 'undefined' && TM.s3explorer) {
                await TM.s3explorer.init();
                s3ExplorerInitialized = true;
            }
        }
    } catch (err) {
        // S3 not available, section stays hidden
        console.log('S3 explorer not available:', err.message);
    }
}

// S3 Explorer global handlers
function s3NewFile() {
    if (TM.s3explorer) TM.s3explorer.openNewModal();
}
window.s3NewFile = s3NewFile;

function s3Upload() {
    if (TM.s3explorer) TM.s3explorer.openUploadDialog();
}
window.s3Upload = s3Upload;

function s3Save() {
    if (TM.s3explorer) TM.s3explorer.saveFile();
}
window.s3Save = s3Save;

function s3Download() {
    if (TM.s3explorer) TM.s3explorer.downloadFile();
}
window.s3Download = s3Download;

function s3Delete() {
    if (TM.s3explorer) TM.s3explorer.deleteFile();
}
window.s3Delete = s3Delete;

function s3ToggleEditorMode() {
    if (TM.s3explorer) TM.s3explorer.toggleEditorMode();
}
window.s3ToggleEditorMode = s3ToggleEditorMode;

function s3CloseFile() {
    if (TM.s3explorer) TM.s3explorer.closeFile();
}
window.s3CloseFile = s3CloseFile;

function s3CreateFile() {
    if (TM.s3explorer) TM.s3explorer.createFile();
}
window.s3CreateFile = s3CreateFile;

function s3CreateFolder() {
    if (TM.s3explorer) TM.s3explorer.createFolder();
}
window.s3CreateFolder = s3CreateFolder;

function closeS3Modal(modalId) {
    if (TM.s3explorer) TM.s3explorer.closeModal(modalId);
}
window.closeS3Modal = closeS3Modal;

function s3HandleFileSelect(event) {
    if (TM.s3explorer) TM.s3explorer.handleFileSelect(event);
}
window.s3HandleFileSelect = s3HandleFileSelect;

function s3ShowMore() {
    if (TM.s3explorer) TM.s3explorer.showMore();
}
window.s3ShowMore = s3ShowMore;

function s3ShowLess() {
    if (TM.s3explorer) TM.s3explorer.showLess();
}
window.s3ShowLess = s3ShowLess;

function s3Rename() {
    if (TM.s3explorer) TM.s3explorer.renameSelected();
}
window.s3Rename = s3Rename;

function s3DownloadSelected() {
    if (TM.s3explorer) TM.s3explorer.downloadSelected();
}
window.s3DownloadSelected = s3DownloadSelected;

function s3DeleteSelected() {
    if (TM.s3explorer) TM.s3explorer.deleteSelected();
}
window.s3DeleteSelected = s3DeleteSelected;

function s3UndeleteSelected() {
    if (TM.s3explorer) TM.s3explorer.undeleteSelected();
}
window.s3UndeleteSelected = s3UndeleteSelected;

function s3ToggleAutosave(enabled) {
    if (TM.s3explorer) TM.s3explorer.setAutosave(enabled);
}
window.s3ToggleAutosave = s3ToggleAutosave;

function s3ToggleFullscreen() {
    if (TM.s3explorer) TM.s3explorer.toggleFullscreen();
}
window.s3ToggleFullscreen = s3ToggleFullscreen;

function s3ToggleView() {
    if (TM.s3explorer) TM.s3explorer.toggleView();
}
window.s3ToggleView = s3ToggleView;
