// TunnelMesh S3 Explorer
// GitHub-style single-pane file browser

(function (root, factory) {
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = factory();
    } else {
        root.TM = root.TM || {};
        root.TM.s3explorer = factory();
    }
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';

    // =========================================================================
    // Constants
    // =========================================================================

    const PAGE_SIZE = 7;
    const DATASHEET_PAGE_SIZE = 50; // Minimum rows per page in datasheet view (actual size adapts to viewport)
    const DEFAULT_COLUMN_WIDTH = 150; // Default column width in pixels

    // =========================================================================
    // State
    // =========================================================================

    const state = {
        buckets: [],
        currentBucket: null,
        currentPath: '', // Current folder prefix
        currentFile: null, // { bucket, key, content, contentType }
        isDirty: false,
        originalContent: '',
        // Pagination
        currentItems: [], // All items in current view
        visibleCount: PAGE_SIZE,
        // Selection
        selectedItems: new Set(), // Set of item keys/names
        // Permissions
        writable: true, // Whether current bucket is writable
        // Editor options
        autosave: false,
        autosaveTimer: null,
        isFullscreen: false,
        // Quota info
        quota: null,
        // View mode (defaults to 'icon' - user can toggle to 'list' view manually)
        viewMode: 'icon', // 'list' or 'icon'
        // Editor mode
        editorMode: 'source', // 'source' or 'wysiwyg' or 'datasheet' or 'treeview'
        // Datasheet-specific state
        datasheetData: null, // Parsed JSON array for datasheet view
        datasheetSchema: null, // { columns: [{ key, type, width }] }
        datasheetPage: 1, // Current page number
        datasheetPageSize: DATASHEET_PAGE_SIZE, // Rows per page
        // Tree view state
        treeviewData: null, // Parsed JSON object/array for tree view
        treeviewCollapsed: new Set(), // Set of collapsed paths (e.g., 'data.items[0]')
    };

    // Text file extensions
    const TEXT_EXTENSIONS = new Set([
        'txt',
        'md',
        'json',
        'yaml',
        'yml',
        'js',
        'ts',
        'jsx',
        'tsx',
        'go',
        'py',
        'rb',
        'rs',
        'css',
        'scss',
        'html',
        'htm',
        'xml',
        'sh',
        'bash',
        'zsh',
        'conf',
        'ini',
        'env',
        'toml',
        'cfg',
        'sql',
        'graphql',
        'proto',
        'tf',
        'hcl',
        'makefile',
        'gitignore',
        'dockerignore',
        'log',
        'csv',
    ]);

    const IMAGE_EXTENSIONS = new Set(['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'ico']);

    // =========================================================================
    // Helpers
    // =========================================================================

    function escapeHtml(str) {
        if (!str) return '';
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // Escape string for use in JavaScript string literals within HTML attributes
    // Prevents XSS when building onclick handlers with user-controlled data
    function escapeJsString(str) {
        if (!str) return '';
        return str
            .replace(/\\/g, '\\\\') // Escape backslashes first
            .replace(/'/g, "\\'") // Escape single quotes
            .replace(/"/g, '\\"') // Escape double quotes
            .replace(/\n/g, '\\n') // Escape newlines
            .replace(/\r/g, '\\r'); // Escape carriage returns
    }

    function formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return `${parseFloat((bytes / k ** i).toFixed(1))} ${sizes[i]}`;
    }

    function formatDate(isoDate) {
        if (!isoDate) return '-';
        const d = new Date(isoDate);
        const now = new Date();
        const diffMs = now - d;
        const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

        if (diffDays === 0) return 'Today';
        if (diffDays === 1) return 'Yesterday';
        if (diffDays > 0 && diffDays < 7) return `${diffDays} days ago`;

        return d.toLocaleDateString();
    }

    // Format date for version history - shows relative time + readable timestamp
    // Uses shared TM.format utilities for DRY code
    function formatVersionDate(isoDate) {
        if (!isoDate) return '-';

        // Use shared format utilities if available
        let relative, timestamp;
        if (typeof TM !== 'undefined' && TM.format) {
            relative = TM.format.formatRelativeTime(isoDate);
            timestamp = TM.format.formatDateTime(isoDate);
        } else {
            // Fallback for standalone use
            const d = new Date(isoDate);
            relative = d.toLocaleDateString();
            timestamp = d.toLocaleString();
        }

        return `<span class="s3-version-relative">${relative}</span><span class="s3-version-timestamp">${timestamp}</span>`;
    }

    // Use formatExpiry from TM.format utility
    function formatExpiry(isoDate) {
        if (!isoDate) return '-';
        // Delegate to shared utility (handles null check internally too)
        if (typeof TM !== 'undefined' && TM.format && TM.format.formatExpiry) {
            return TM.format.formatExpiry(isoDate);
        }
        // Fallback if utility not loaded
        return new Date(isoDate).toLocaleDateString();
    }

    function getExtension(filename) {
        const parts = filename.split('.');
        if (parts.length < 2) return '';
        return parts[parts.length - 1].toLowerCase();
    }

    /**
     * Determines if a markdown file should open in WYSIWYG (preview) mode
     * @param {string} ext - File extension
     * @param {string} content - File content
     * @returns {boolean} - True if should use WYSIWYG mode
     */
    function shouldUseWysiwygMode(ext, content) {
        // Only markdown files can use wysiwyg mode
        if (ext !== 'md') return false;
        // Empty files should open in source mode (editable)
        if (!content || content.trim().length === 0) return false;
        // Non-empty markdown files open in preview mode
        return true;
    }

    /**
     * Detect JSON type (object vs array)
     * @param {string} content - JSON string content
     * @returns {{isObject: boolean, isArray: boolean, parsed: any}} - Detection result
     */
    function detectJsonType(content) {
        try {
            const parsed = JSON.parse(content);
            return {
                isObject: typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed),
                isArray: Array.isArray(parsed),
                parsed,
            };
        } catch (_e) {
            return { isObject: false, isArray: false, parsed: null };
        }
    }

    function _isTextFile(filename) {
        const ext = getExtension(filename);
        const name = filename.toLowerCase();
        return TEXT_EXTENSIONS.has(ext) || name === 'dockerfile' || name === 'makefile' || name.startsWith('.');
    }

    function isImageFile(filename) {
        return IMAGE_EXTENSIONS.has(getExtension(filename));
    }

    function getContentType(filename) {
        const ext = getExtension(filename);
        const types = {
            json: 'application/json',
            yaml: 'text/yaml',
            yml: 'text/yaml',
            md: 'text/markdown',
            js: 'application/javascript',
            html: 'text/html',
            css: 'text/css',
            xml: 'application/xml',
        };
        return types[ext] || 'text/plain';
    }

    function showToast(message, type = 'info') {
        if (typeof window.showToast === 'function') {
            window.showToast(message, type);
        } else {
            console.log(`[${type}] ${message}`);
        }
    }

    // Icon rendering utilities
    function getItemIcon(item) {
        if (item.isBucket) {
            const isShare = item.name.startsWith('fs+');
            return isShare ? 'share' : 'bucket';
        }
        if (item.isFolder) return 'folder';
        return 'file';
    }

    function getItemDisplayName(item) {
        if (item.isBucket && item.name.startsWith('fs+')) {
            return item.name.substring(3); // Strip "fs+" prefix for shares
        }
        return item.name;
    }

    function getIconSVG(iconType) {
        const svgs = {
            file: '<svg class="s3-large-icon" width="64" height="64" viewBox="0 0 24 24" fill="currentColor"><path d="M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/></svg>',
            folder: '<svg class="s3-large-icon" width="64" height="64" viewBox="0 0 24 24" fill="currentColor"><path d="M10 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2h-8l-2-2z"/></svg>',
            bucket: '<svg class="s3-large-icon" width="64" height="64" viewBox="0 0 24 24" fill="currentColor"><path d="M18.06 23h-12c-.72 0-1.34-.5-1.47-1.2L2 6.2C1.87 5.5 2.42 5 3.14 5h17.72c.72 0 1.27.5 1.14 1.2l-2.59 15.6c-.13.7-.75 1.2-1.47 1.2zM9 9v6h2V9h2V7H7v2h2z"/></svg>',
            share: '<svg class="s3-large-icon" width="64" height="64" viewBox="0 0 24 24" fill="currentColor"><path d="M18 16.08c-.76 0-1.44.3-1.96.77L8.91 12.7c.05-.23.09-.46.09-.7s-.04-.47-.09-.7l7.05-4.11c.54.5 1.25.81 2.04.81 1.66 0 3-1.34 3-3s-1.34-3-3-3-3 1.34-3 3c0 .24.04.47.09.7L8.04 9.81C7.5 9.31 6.79 9 6 9c-1.66 0-3 1.34-3 3s1.34 3 3 3c.79 0 1.5-.31 2.04-.81l7.12 4.16c-.05.21-.08.43-.08.65 0 1.61 1.31 2.92 2.92 2.92 1.61 0 2.92-1.31 2.92-2.92s-1.31-2.92-2.92-2.92z"/></svg>',
        };
        return svgs[iconType] || svgs.file;
    }

    function buildItemMetadata(item) {
        const parts = [];

        // Size/quota
        if (item.size !== null && item.size !== undefined && !item.isFolder) {
            parts.push(formatBytes(item.size));
        } else if (item.quota) {
            parts.push(`${formatBytes(item.size || 0)} / ${formatBytes(item.quota)}`);
        }

        // Date
        if (item.lastModified) {
            parts.push(formatDate(item.lastModified));
        }

        return parts.join(' • ');
    }

    function buildOnclickHandler(item) {
        if (item.isBucket) {
            return `TM.s3explorer.navigateTo('${escapeJsString(item.name)}', '')`;
        }
        if (item.isFolder) {
            return `TM.s3explorer.navigateTo('${escapeJsString(state.currentBucket)}', '${escapeJsString(item.key)}')`;
        }
        return `TM.s3explorer.openFile('${escapeJsString(state.currentBucket)}', '${escapeJsString(item.key)}')`;
    }

    // =========================================================================
    // Datasheet Helper Functions
    // =========================================================================

    /**
     * Detect if JSON content should be displayed as a datasheet
     * @param {string} content - JSON string content
     * @returns {{isDatasheet: boolean, data: Array|null}}
     */
    function detectDatasheetMode(content) {
        try {
            const parsed = JSON.parse(content);
            if (!Array.isArray(parsed)) {
                return {
                    isDatasheet: false,
                    data: null,
                    reason: 'Not a JSON array (found object or primitive)',
                };
            }
            if (parsed.length === 0) {
                return {
                    isDatasheet: false,
                    data: null,
                    reason: 'Empty array',
                };
            }
            // Check if all elements are objects (not arrays or primitives)
            const allObjects = parsed.every(
                (item) => typeof item === 'object' && item !== null && !Array.isArray(item),
            );
            if (!allObjects) {
                return {
                    isDatasheet: false,
                    data: null,
                    reason: 'Array contains non-object elements (primitives or nested arrays)',
                };
            }
            return { isDatasheet: true, data: parsed, reason: null };
        } catch (e) {
            return {
                isDatasheet: false,
                data: null,
                reason: `Invalid JSON: ${e.message}`,
            };
        }
    }

    /**
     * Infer schema from JSON array data
     * @param {Array} data - Array of objects
     * @returns {{columns: Array<{key: string, type: string, width: number}>}}
     */
    function inferSchema(data) {
        if (!data || data.length === 0) {
            return { columns: [] };
        }

        // Collect all unique keys across all objects
        const allKeys = new Set();
        data.forEach((obj) => {
            Object.keys(obj).forEach((k) => {
                allKeys.add(k);
            });
        });

        // Infer type for each column
        const columns = Array.from(allKeys).map((key) => {
            const values = data.map((obj) => obj[key]).filter((v) => v != null);
            let type = 'string'; // default

            if (values.length > 0) {
                if (values.every((v) => typeof v === 'number')) {
                    type = 'number';
                } else if (values.every((v) => typeof v === 'boolean')) {
                    type = 'boolean';
                } else if (values.every((v) => Array.isArray(v))) {
                    type = 'nested-array';
                } else if (values.every((v) => typeof v === 'object' && !Array.isArray(v))) {
                    type = 'nested-object';
                } else if (values.every((v) => typeof v === 'string' && /^https?:\/\//.test(v))) {
                    type = 'url';
                } else if (values.every((v) => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(v))) {
                    type = 'date';
                }
            }

            return { key, type, width: DEFAULT_COLUMN_WIDTH };
        });

        return { columns };
    }

    /**
     * Render cell content based on type
     * @param {any} value - Cell value
     * @param {string} type - Column type
     * @param {number} rowIdx - Row index
     * @param {string} colKey - Column key
     * @returns {string} HTML string
     */
    function renderCell(value, type, rowIdx, colKey) {
        if (value === null || value === undefined) {
            return '<span class="s3-ds-null">null</span>';
        }

        switch (type) {
            case 'number':
                return `<span class="s3-ds-number">${TM.format ? TM.format.formatNumber(value) : value.toLocaleString()}</span>`;
            case 'boolean':
                return value
                    ? '<span class="s3-ds-badge s3-ds-badge-true">true</span>'
                    : '<span class="s3-ds-badge s3-ds-badge-false">false</span>';
            case 'date':
                return TM.format?.formatDateTime ? TM.format.formatDateTime(value) : new Date(value).toLocaleString();
            case 'url':
                return `<a href="${encodeURI(value)}" target="_blank" rel="noopener noreferrer">${escapeHtml(value)}</a>`;
            case 'nested-array':
                return `<span class="s3-ds-nested" data-row-idx="${rowIdx}" data-col-key="${escapeHtml(colKey)}" title="Click to view nested array">
                    <span class="s3-ds-nested-text">${Array.isArray(value) ? value.length : 0} items</span>
                    <svg class="s3-ds-nested-icon" width="14" height="14" viewBox="0 0 14 14" fill="none">
                        <circle cx="7" cy="7" r="5.5" stroke="currentColor" stroke-width="1.5" opacity="0.5"/>
                        <circle cx="7" cy="7" r="2" fill="currentColor" opacity="0.5"/>
                    </svg>
                </span>`;
            case 'nested-object':
                return `<span class="s3-ds-nested" data-row-idx="${rowIdx}" data-col-key="${escapeHtml(colKey)}" title="Click to view nested object">
                    <span class="s3-ds-nested-text">{...}</span>
                    <svg class="s3-ds-nested-icon" width="14" height="14" viewBox="0 0 14 14" fill="none">
                        <circle cx="7" cy="7" r="5.5" stroke="currentColor" stroke-width="1.5" opacity="0.5"/>
                        <circle cx="7" cy="7" r="2" fill="currentColor" opacity="0.5"/>
                    </svg>
                </span>`;
            default:
                return escapeHtml(String(value));
        }
    }

    /**
     * Calculate dynamic page size based on available viewport height
     * @returns {number} Number of rows that fit in the viewport
     */
    function calculateDatasheetPageSize() {
        const container = document.getElementById('s3-datasheet-container');
        if (!container) return DATASHEET_PAGE_SIZE;

        // Get available height for the table
        const containerHeight = container.clientHeight;

        // Estimate row height (padding + line height + border)
        // Using 36px as a reasonable estimate based on CSS (0.5rem padding = 8px * 2 + ~20px line height)
        const estimatedRowHeight = 36;

        // Reserve space for header (estimate ~40px)
        const headerHeight = 40;

        // Calculate how many rows fit
        const availableHeight = containerHeight - headerHeight;
        const rowsFit = Math.floor(availableHeight / estimatedRowHeight);

        // Return at least minimum page size, at most the calculated rows
        return Math.max(DATASHEET_PAGE_SIZE, rowsFit);
    }

    /**
     * Render datasheet view
     */
    function renderDatasheet() {
        const container = document.getElementById('s3-datasheet');
        if (!container || !state.datasheetData || !state.datasheetSchema) return;

        // Calculate dynamic page size based on viewport
        state.datasheetPageSize = calculateDatasheetPageSize();

        const data = state.datasheetData;
        const schema = state.datasheetSchema;
        const page = state.datasheetPage;
        const pageSize = state.datasheetPageSize;

        // Calculate pagination
        const totalRows = data.length;
        const totalPages = Math.ceil(totalRows / pageSize);
        const startRow = (page - 1) * pageSize;
        const endRow = Math.min(startRow + pageSize, totalRows);
        const visibleRows = data.slice(startRow, endRow);

        // Update toolbar info
        const rowCountEl = document.getElementById('s3-ds-row-count');
        const colCountEl = document.getElementById('s3-ds-col-count');
        const pageCurrentEl = document.getElementById('s3-ds-page-current');
        const pageTotalEl = document.getElementById('s3-ds-page-total');

        if (rowCountEl) rowCountEl.textContent = totalRows;
        if (colCountEl) colCountEl.textContent = schema.columns.length;
        if (pageCurrentEl) pageCurrentEl.textContent = page;
        if (pageTotalEl) pageTotalEl.textContent = totalPages;

        // Render table
        const table = document.getElementById('s3-datasheet-table');
        if (!table) return;

        // Render header
        const thead = document.getElementById('s3-ds-thead');
        if (thead) {
            const headerRow = document.createElement('tr');
            schema.columns.forEach((col) => {
                const th = document.createElement('th');
                th.textContent = col.key;
                th.title = `Type: ${col.type}`;
                headerRow.appendChild(th);
            });
            thead.innerHTML = '';
            thead.appendChild(headerRow);
        }

        // Render body
        const tbody = document.getElementById('s3-ds-tbody');
        if (tbody) {
            tbody.innerHTML = '';
            visibleRows.forEach((row, idx) => {
                const tr = document.createElement('tr');
                const actualRowIdx = startRow + idx;

                schema.columns.forEach((col) => {
                    const td = document.createElement('td');
                    const value = row[col.key];
                    td.innerHTML = renderCell(value, col.type, actualRowIdx, col.key);
                    td.className = `s3-ds-cell s3-ds-type-${col.type}`;
                    tr.appendChild(td);
                });
                tbody.appendChild(tr);
            });
        }

        // Render aggregations footer
        renderAggregations();

        // Update pagination buttons
        const prevBtn = document.getElementById('s3-ds-prev');
        const nextBtn = document.getElementById('s3-ds-next');
        if (prevBtn) prevBtn.disabled = page === 1;
        if (nextBtn) nextBtn.disabled = page === totalPages;
    }

    /**
     * Render aggregation footer
     */
    function renderAggregations() {
        const tfoot = document.getElementById('s3-ds-tfoot');
        if (!tfoot || !state.datasheetData || !state.datasheetSchema) return;

        const { data, schema } = { data: state.datasheetData, schema: state.datasheetSchema };

        const tr = document.createElement('tr');
        tr.className = 's3-ds-agg-row';

        schema.columns.forEach((col) => {
            const td = document.createElement('td');
            td.className = 's3-ds-agg-cell';

            if (col.type === 'number') {
                const values = data.map((row) => row[col.key]).filter((v) => typeof v === 'number');

                if (values.length > 0) {
                    const sum = values.reduce((a, b) => a + b, 0);
                    const avg = sum / values.length;
                    const formatter = TM.format?.formatNumber || ((n) => n.toLocaleString());

                    td.innerHTML = `
                        <div class="s3-ds-agg">
                            <span title="Sum">Σ ${formatter(sum)}</span>
                            <span title="Average">μ ${formatter(parseFloat(avg.toFixed(2)))}</span>
                            <span title="Count">n=${values.length}</span>
                        </div>
                    `;
                }
            }

            tr.appendChild(td);
        });

        tfoot.innerHTML = '';
        tfoot.appendChild(tr);
    }

    /**
     * Navigate to next page
     */
    function nextPage() {
        if (!state.datasheetData) return;
        const totalPages = Math.ceil(state.datasheetData.length / state.datasheetPageSize);
        if (state.datasheetPage < totalPages) {
            state.datasheetPage++;
            renderDatasheet();
        }
    }

    /**
     * Navigate to previous page
     */
    function prevPage() {
        if (state.datasheetPage > 1) {
            state.datasheetPage--;
            renderDatasheet();
        }
    }

    /**
     * Render tree view for JSON objects
     */
    function renderTreeView() {
        const container = document.getElementById('s3-treeview');
        if (!container || !state.treeviewData) return;

        container.innerHTML = renderTreeNode(state.treeviewData, '', 'root');
    }

    /**
     * Render a single tree node recursively
     * @param {any} value - The value to render
     * @param {string} path - The path to this node (e.g., 'data.items[0].name')
     * @param {string} key - The key/index for this node
     * @returns {string} - HTML string
     */
    function renderTreeNode(value, path, key) {
        const fullPath = path ? `${path}.${key}` : key;
        const isCollapsed = state.treeviewCollapsed.has(fullPath);

        if (value === null) {
            return `<div class="tree-item"><span class="tree-key">${escapeHtml(key)}:</span> <span class="tree-null">null</span></div>`;
        }

        if (value === undefined) {
            return `<div class="tree-item"><span class="tree-key">${escapeHtml(key)}:</span> <span class="tree-undefined">undefined</span></div>`;
        }

        const type = typeof value;

        if (type === 'boolean') {
            return `<div class="tree-item"><span class="tree-key">${escapeHtml(key)}:</span> <span class="tree-boolean">${value}</span></div>`;
        }

        if (type === 'number') {
            return `<div class="tree-item"><span class="tree-key">${escapeHtml(key)}:</span> <span class="tree-number">${value}</span></div>`;
        }

        if (type === 'string') {
            return `<div class="tree-item"><span class="tree-key">${escapeHtml(key)}:</span> <span class="tree-string">"${escapeHtml(value)}"</span></div>`;
        }

        if (Array.isArray(value)) {
            const toggleIcon = isCollapsed ? '▶' : '▼';
            const arrayLength = value.length;

            // Check if array can be displayed as datasheet (all elements are objects)
            const canShowDatasheet =
                arrayLength > 0 &&
                value.every((item) => typeof item === 'object' && item !== null && !Array.isArray(item));

            let html = `<div class="tree-item tree-expandable">
                <span class="tree-toggle" data-tree-path="${escapeHtml(fullPath)}">${toggleIcon}</span>
                <span class="tree-key">${escapeHtml(key)}:</span>
                <span class="tree-bracket">[</span><span class="tree-count">${arrayLength} items</span><span class="tree-bracket">]</span>`;

            // Add datasheet link if applicable
            if (canShowDatasheet) {
                html += ` <a href="#" class="tree-datasheet-link" data-tree-path="${escapeHtml(fullPath)}" data-tree-array="true">(datasheet)</a>`;
            }

            html += '</div>';

            if (!isCollapsed) {
                html += '<div class="tree-children">';
                value.forEach((item, index) => {
                    html += renderTreeNode(item, fullPath, `[${index}]`);
                });
                html += '</div>';
            }

            return html;
        }

        if (type === 'object') {
            const toggleIcon = isCollapsed ? '▶' : '▼';
            const keys = Object.keys(value);
            const keyCount = keys.length;

            let html = `<div class="tree-item tree-expandable">
                <span class="tree-toggle" data-tree-path="${escapeHtml(fullPath)}">${toggleIcon}</span>
                <span class="tree-key">${escapeHtml(key)}:</span>
                <span class="tree-bracket">{</span><span class="tree-count">${keyCount} keys</span><span class="tree-bracket">}</span>
            </div>`;

            if (!isCollapsed) {
                html += '<div class="tree-children">';
                keys.forEach((k) => {
                    html += renderTreeNode(value[k], fullPath, k);
                });
                html += '</div>';
            }

            return html;
        }

        return `<div class="tree-item"><span class="tree-key">${escapeHtml(key)}:</span> <span class="tree-unknown">${escapeHtml(String(value))}</span></div>`;
    }

    /**
     * Toggle expand/collapse state of a tree node
     * @param {string} path - The path to toggle
     */
    function toggleTreeNode(path) {
        if (state.treeviewCollapsed.has(path)) {
            state.treeviewCollapsed.delete(path);
        } else {
            state.treeviewCollapsed.add(path);
        }
        renderTreeView();
    }

    /**
     * Open modal to view nested data
     * @param {number} rowIdx - Row index
     * @param {string} colKey - Column key
     */
    function openNestedModal(rowIdx, colKey) {
        if (!state.datasheetData) return;

        // Validate row index bounds
        if (rowIdx < 0 || rowIdx >= state.datasheetData.length) {
            console.error(`Invalid row index: ${rowIdx}, data length: ${state.datasheetData.length}`);
            return;
        }

        // Validate row and column key exist
        const row = state.datasheetData[rowIdx];
        if (!row || !(colKey in row)) {
            console.error(`Invalid column key: ${colKey} for row ${rowIdx}`);
            return;
        }

        const value = row[colKey];
        const modal = document.getElementById('s3-nested-modal');
        const body = document.getElementById('s3-nested-body');
        const title = document.getElementById('s3-nested-title');

        if (!modal || !body) return;

        // Set title
        if (title) {
            title.textContent = `Nested Data: ${colKey} [Row ${rowIdx + 1}]`;
        }

        // If nested array of objects → Render sub-datasheet
        if (Array.isArray(value) && value.length > 0) {
            const detect = detectDatasheetMode(JSON.stringify(value));
            if (detect.isDatasheet) {
                // Render as mini datasheet table
                const schema = inferSchema(value);
                let html = '<table class="s3-datasheet-table s3-nested-table"><thead><tr>';
                schema.columns.forEach((col) => {
                    html += `<th>${escapeHtml(col.key)}</th>`;
                });
                html += '</tr></thead><tbody>';
                value.forEach((row, idx) => {
                    html += '<tr>';
                    schema.columns.forEach((col) => {
                        html += `<td>${renderCell(row[col.key], col.type, idx, col.key)}</td>`;
                    });
                    html += '</tr>';
                });
                html += '</tbody></table>';
                body.innerHTML = html;
            } else {
                // Show as formatted JSON
                body.innerHTML = `<pre class="s3-nested-json">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;
            }
        } else {
            // Show as formatted JSON
            body.innerHTML = `<pre class="s3-nested-json">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;
        }

        modal.style.display = 'flex';
    }

    /**
     * Close nested modal
     */
    function closeNestedModal() {
        const modal = document.getElementById('s3-nested-modal');
        if (modal) {
            modal.style.display = 'none';
        }
    }

    // =========================================================================
    // API Functions
    // =========================================================================

    async function fetchBuckets() {
        try {
            const resp = await fetch('/api/s3/buckets');
            if (!resp.ok) return { buckets: [], quota: null };
            const data = await resp.json();
            // Store quota info in state
            state.quota = data.quota || null;
            state.volume = data.volume || null;
            state.storage = data.storage || null;
            return data.buckets || [];
        } catch (err) {
            console.error('Failed to fetch buckets:', err);
            return [];
        }
    }

    async function fetchObjects(bucket, prefix = '') {
        try {
            const params = new URLSearchParams({ prefix, delimiter: '/' });
            const resp = await fetch(`/api/s3/buckets/${encodeURIComponent(bucket)}/objects?${params}`);
            if (!resp.ok) return [];
            return await resp.json();
        } catch (err) {
            console.error('Failed to fetch objects:', err);
            return [];
        }
    }

    async function fetchRecycledObjects(bucket, prefix = '') {
        try {
            const params = new URLSearchParams();
            if (prefix) params.set('prefix', prefix);
            const qs = params.toString();
            const url = `/api/s3/buckets/${encodeURIComponent(bucket)}/recyclebin${qs ? `?${qs}` : ''}`;
            const resp = await fetch(url);
            if (!resp.ok) return [];
            const entries = await resp.json();
            if (!Array.isArray(entries)) return [];
            // Server-side prefix filter returns all entries under the prefix;
            // apply delimiter='/' logic client-side to show only direct children.
            return entries.filter((obj) => {
                if (!obj.key.startsWith(prefix)) return false;
                const remainder = obj.key.slice(prefix.length);
                return !remainder.includes('/');
            });
        } catch (err) {
            console.error('Failed to fetch recycled objects:', err);
            return [];
        }
    }

    async function _getObject(bucket, key) {
        const resp = await fetch(`/api/s3/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`);
        if (!resp.ok) throw new Error(`Failed to get object: ${resp.status}`);
        return {
            content: await resp.text(),
            contentType: resp.headers.get('Content-Type'),
            size: parseInt(resp.headers.get('Content-Length'), 10) || 0,
        };
    }

    async function putObject(bucket, key, content, contentType) {
        const resp = await fetch(`/api/s3/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`, {
            method: 'PUT',
            headers: { 'Content-Type': contentType },
            body: content,
        });
        if (!resp.ok) throw new Error(`Failed to save: ${resp.status}`);
        return true;
    }

    async function deleteObject(bucket, key) {
        const resp = await fetch(`/api/s3/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`, {
            method: 'DELETE',
        });
        return resp.ok || resp.status === 204;
    }

    async function restoreRecycledObject(bucket, key) {
        const resp = await fetch(
            `/api/s3/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}/undelete`,
            {
                method: 'POST',
            },
        );
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ error: `HTTP ${resp.status}` }));
            throw new Error(err.error || `Failed to restore: ${resp.status}`);
        }
        return true;
    }

    async function fetchVersions(bucket, key) {
        try {
            const resp = await fetch(
                `/api/s3/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}/versions`,
            );
            if (!resp.ok) return [];
            return await resp.json();
        } catch (err) {
            console.error('Failed to fetch versions:', err);
            return [];
        }
    }

    async function restoreVersion(bucket, key, versionId) {
        const resp = await fetch(
            `/api/s3/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}/restore`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ version_id: versionId }),
            },
        );
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            throw new Error(err.error || `Failed to restore: ${resp.status}`);
        }
        return await resp.json();
    }

    // =========================================================================
    // Rendering
    // =========================================================================

    /* istanbul ignore next */
    function renderBreadcrumb() {
        const container = document.getElementById('s3-breadcrumb');
        if (!container) return;

        let html = '';

        // Root (buckets view)
        if (!state.currentBucket) {
            html = '<span class="s3-breadcrumb-item current">All</span>';
        } else {
            html = '<span class="s3-breadcrumb-item" onclick="TM.s3explorer.navigateTo(null, \'\')">All</span>';
            html += '<span class="s3-breadcrumb-sep">/</span>';

            // Bucket
            if (!state.currentPath && !state.currentFile) {
                html += `<span class="s3-breadcrumb-item current">${escapeHtml(state.currentBucket)}</span>`;
            } else {
                html += `<span class="s3-breadcrumb-item" onclick="TM.s3explorer.navigateTo('${escapeJsString(state.currentBucket)}', '')">${escapeHtml(state.currentBucket)}</span>`;
            }

            // Path parts
            if (state.currentPath || state.currentFile) {
                const path = state.currentFile ? state.currentFile.key : state.currentPath;
                const parts = path.split('/').filter((p) => p);
                let accum = '';

                for (let i = 0; i < parts.length; i++) {
                    const part = parts[i];
                    accum += `${part}/`;
                    html += '<span class="s3-breadcrumb-sep">/</span>';

                    const isLast = i === parts.length - 1;
                    const isFile = state.currentFile && isLast;

                    if (isLast) {
                        html += `<span class="s3-breadcrumb-item current">${escapeHtml(isFile ? part : part)}</span>`;
                    } else {
                        html += `<span class="s3-breadcrumb-item" onclick="TM.s3explorer.navigateTo('${escapeJsString(state.currentBucket)}', '${escapeJsString(accum)}')">${escapeHtml(part)}</span>`;
                    }
                }
            }

            // Add [LINK] shortcut for file share buckets
            if (state.currentBucket.startsWith('fs+')) {
                const shareName = state.currentBucket.substring(3);
                const idx = shareName.indexOf('_');
                if (idx > 0) {
                    const peerName = shareName.substring(0, idx);
                    const share = shareName.substring(idx + 1);
                    const sitePath = `/peers/${encodeURIComponent(peerName)}/${encodeURIComponent(share)}/${state.currentPath || ''}`;
                    html += `<a href="${sitePath}" target="_blank" class="s3-breadcrumb-link" title="Open in web view"><sub>LINK</sub></a>`;
                }
            }
        }

        container.innerHTML = html;
    }

    /* istanbul ignore next */
    function renderVolumeBar() {
        let bar = document.getElementById('s3-volume-bar');
        // Only show when viewing bucket list (not inside a bucket)
        if (state.currentBucket || !state.volume) {
            if (bar) bar.style.display = 'none';
            return;
        }
        const v = state.volume;
        const pct = v.total_bytes > 0 ? ((v.used_bytes / v.total_bytes) * 100).toFixed(1) : 0;
        const color = pct >= 95 ? 'var(--color-danger)' : pct >= 80 ? 'var(--color-warning)' : 'var(--color-success)';
        if (!bar) {
            bar = document.createElement('div');
            bar.id = 's3-volume-bar';
            const table = document.getElementById('s3-files');
            if (table) table.parentNode.insertBefore(bar, table);
            else return;
        }
        bar.style.display = 'flex';
        let html =
            `<div class="s3-volume-row">` +
            `<span>Volume</span>` +
            `<div class="s3-volume-track">` +
            `<div class="s3-volume-fill" style="width:${Math.min(pct, 100)}%;background:${color}"></div></div>` +
            `<span>${pct}% used</span>` +
            `<span style="color:var(--color-text-secondary)">${formatBytes(v.used_bytes)} / ${formatBytes(v.total_bytes)} (${formatBytes(v.available_bytes)} free)</span>` +
            `</div>`;
        // Show dedup storage info when available
        if (state.storage && state.storage.physical_bytes > 0) {
            const s = state.storage;
            const ratio = s.dedup_ratio.toFixed(1);
            html +=
                `<div class="s3-volume-detail">` +
                `Storage: ${formatBytes(s.physical_bytes)} on disk` +
                (s.logical_bytes !== s.physical_bytes
                    ? ` (${formatBytes(s.logical_bytes)} logical, ${ratio}x dedup)`
                    : '') +
                `</div>`;
        }
        bar.innerHTML = html;
    }

    /* istanbul ignore next */
    async function renderFileListing(resetPagination = true) {
        const tbody = document.getElementById('s3-files-body');
        const table = document.getElementById('s3-files');
        const iconGrid = document.getElementById('s3-icons');
        const browser = document.getElementById('s3-browser');
        const viewer = document.getElementById('s3-viewer');
        const preview = document.getElementById('s3-preview');
        const empty = document.getElementById('s3-empty');
        const browseActions = document.getElementById('s3-browse-actions');
        const paginationEl = document.getElementById('s3-pagination');

        if (!tbody) return;

        // Hide viewer/preview, show browser
        if (viewer) viewer.style.display = 'none';
        if (preview) preview.style.display = 'none';
        if (browser) browser.style.display = 'block';

        // Clear current file
        state.currentFile = null;
        state.isDirty = false;

        // Reset pagination and selection when navigating to new folder
        if (resetPagination) {
            state.visibleCount = PAGE_SIZE;
            state.selectedItems.clear();
            updateSelectionUI();
        }

        let items = [];

        if (!state.currentBucket) {
            // Show buckets as folders
            const buckets = await fetchBuckets();

            // Show "New Bucket" button when viewing bucket list
            if (browseActions) {
                // Remove existing New Bucket button if present
                const existingBtn = browseActions.querySelector('.s3-new-bucket-btn');
                if (existingBtn) existingBtn.remove();

                // Add New Bucket button
                const newBucketBtn = document.createElement('button');
                newBucketBtn.className = 's3-btn s3-new-bucket-btn';
                newBucketBtn.title = 'Create new bucket';
                newBucketBtn.onclick = () => window.openBucketModal();
                newBucketBtn.innerHTML = `
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z"/>
                    </svg>
                    <span>New Bucket</span>
                `;
                browseActions.insertBefore(newBucketBtn, browseActions.firstChild);
            }

            state.buckets = buckets;
            items = buckets.map((b) => ({
                name: b.name,
                isFolder: true,
                isBucket: true,
                size: b.used_bytes || 0,
                quota: b.quota_bytes || 0,
                replication_factor: b.replication_factor || 2,
                lastModified: b.created_at,
                expires: null,
                writable: b.writable,
            }));
        } else {
            // Remove "New Bucket" button when inside a bucket
            if (browseActions) {
                const existingBtn = browseActions.querySelector('.s3-new-bucket-btn');
                if (existingBtn) existingBtn.remove();
            }

            // Show objects in current path, including recycled objects
            const [objects, recycled] = await Promise.all([
                fetchObjects(state.currentBucket, state.currentPath),
                fetchRecycledObjects(state.currentBucket, state.currentPath),
            ]);

            // Build set of live object keys to deduplicate recycled entries
            const liveKeys = new Set(objects.filter((o) => !o.is_prefix).map((o) => o.key));

            // Merge recycled objects that don't have a live counterpart
            const merged = [...objects];
            for (const rb of recycled) {
                if (!liveKeys.has(rb.key)) {
                    merged.push(rb);
                }
            }

            // Sort: folders first, then files
            merged.sort((a, b) => {
                if (a.is_prefix && !b.is_prefix) return -1;
                if (!a.is_prefix && b.is_prefix) return 1;
                return a.key.localeCompare(b.key);
            });

            items = merged
                .map((obj) => {
                    if (obj.is_prefix) {
                        const name = obj.key.replace(state.currentPath, '').replace(/\/$/, '');
                        return {
                            name,
                            isFolder: true,
                            key: obj.key,
                            size: null,
                            quota: null,
                            lastModified: null,
                            expires: null,
                        };
                    } else {
                        const name = obj.key.replace(state.currentPath, '');
                        return {
                            name,
                            isFolder: false,
                            key: obj.key,
                            size: obj.size,
                            quota: null,
                            owner: obj.owner,
                            lastModified: obj.last_modified,
                            expires: obj.expires,
                            deletedAt: obj.deleted_at,
                        };
                    }
                })
                .filter((item) => item.name && item.name !== '.folder');
        }

        // Store items for pagination
        state.currentItems = items;

        // Toggle quota column visibility (only show for bucket list, not files)
        const quotaCol = document.querySelector('.s3-col-quota');
        if (quotaCol) {
            quotaCol.style.display = state.currentBucket ? 'none' : '';
        }

        // Toggle replication column visibility (only show for bucket list, not files)
        const replicationCol = document.querySelector('.s3-col-replication');
        if (replicationCol) {
            replicationCol.style.display = state.currentBucket ? 'none' : '';
        }

        // Toggle owner column visibility (only show for files within bucket, not bucket list)
        const ownerCol = document.querySelector('.s3-col-owner');
        if (ownerCol) {
            ownerCol.style.display = state.currentBucket ? '' : 'none';
        }

        // Toggle actions column visibility (only show for bucket list if admin)
        const actionsCol = document.querySelector('.s3-col-actions');
        const isAdmin = window.TM?.panel?.isAdmin();
        if (actionsCol) {
            actionsCol.style.display = !state.currentBucket && isAdmin ? '' : 'none';
        }

        // Render volume capacity bar when viewing bucket list
        renderVolumeBar();

        // Always render breadcrumb for navigation
        renderBreadcrumb();

        if (items.length === 0) {
            tbody.innerHTML = '';
            if (iconGrid) iconGrid.innerHTML = '';
            if (empty) empty.style.display = 'block';
            if (paginationEl) paginationEl.style.display = 'none';
            return;
        }

        if (empty) empty.style.display = 'none';

        // Icon view shows all items (no pagination), list view uses pagination
        const visibleItems = state.viewMode === 'icon' ? items : items.slice(0, state.visibleCount);

        // Render based on view mode
        if (state.viewMode === 'icon') {
            // Hide table, show icon grid
            if (table) table.style.display = 'none';
            if (iconGrid) {
                iconGrid.style.display = 'grid';
                renderIconGrid(visibleItems);
            }
        } else {
            // Show table, hide icon grid
            if (table) table.style.display = 'table';
            if (iconGrid) iconGrid.style.display = 'none';

            tbody.innerHTML = visibleItems
                .map((item, index) => {
                    const icon = item.isFolder
                        ? '<svg class="s3-icon s3-icon-folder" width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M10 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2h-8l-2-2z"/></svg>'
                        : '<svg class="s3-icon s3-icon-file" width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M14 2H6c-1.1 0-1.99.9-1.99 2L4 20c0 1.1.89 2 1.99 2H18c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/></svg>';
                    const isDeleted = Boolean(item.deletedAt);
                    const nameClass = item.isFolder ? 's3-name s3-name-folder' : 's3-name';
                    const onclick = item.isBucket
                        ? `TM.s3explorer.navigateTo('${escapeJsString(item.name)}', '')`
                        : item.isFolder
                          ? `TM.s3explorer.navigateTo('${escapeJsString(state.currentBucket)}', '${escapeJsString(item.key)}')`
                          : `TM.s3explorer.openFile('${escapeJsString(state.currentBucket)}', '${escapeJsString(item.key)}')`;
                    const itemId = item.key || item.name;
                    const isSelected = state.selectedItems.has(itemId);
                    let rowClass = isSelected ? 's3-selected' : '';
                    if (isDeleted) rowClass += ' s3-recycled';
                    // Show checkboxes for files/folders (not buckets), disabled for read-only or deleted
                    const checkbox = item.isBucket
                        ? ''
                        : `<input type="checkbox" class="s3-checkbox" data-item-id="${escapeHtml(itemId)}" ${isSelected ? 'checked' : ''} ${state.writable && !isDeleted ? '' : 'disabled'} onclick="event.stopPropagation(); TM.s3explorer.toggleSelection('${escapeJsString(itemId)}')" />`;
                    const deletedBadge = isDeleted ? '<span class="s3-badge s3-badge-deleted">Deleted</span>' : '';

                    // Only show quota column for bucket list (not when inside a bucket)
                    const quotaCell = state.currentBucket
                        ? ''
                        : `<td>${item.quota ? formatBytes(item.quota) : '-'}</td>`;
                    // Only show replication column for bucket list (not when inside a bucket)
                    const replication_factor = item.replication_factor || 2;
                    const replicationCell = state.currentBucket ? '' : `<td>${replication_factor}x</td>`;
                    // Only show owner column when inside a bucket (not for bucket list)
                    const ownerCell = state.currentBucket ? `<td>${escapeHtml(item.owner || '-')}</td>` : '';
                    // Actions column: show properties button for buckets (admin-only)
                    const actionsCell =
                        !state.currentBucket && item.isBucket && isAdmin
                            ? `<td><button class="s3-btn s3-btn-sm" onclick="event.stopPropagation(); window.openBucketPropertiesModal('${escapeJsString(item.name)}')" title="Properties"><svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M19.14,12.94c0.04-0.3,0.06-0.61,0.06-0.94c0-0.32-0.02-0.64-0.07-0.94l2.03-1.58c0.18-0.14,0.23-0.41,0.12-0.61 l-1.92-3.32c-0.12-0.22-0.37-0.29-0.59-0.22l-2.39,0.96c-0.5-0.38-1.03-0.7-1.62-0.94L14.4,2.81c-0.04-0.24-0.24-0.41-0.48-0.41 h-3.84c-0.24,0-0.43,0.17-0.47,0.41L9.25,5.35C8.66,5.59,8.12,5.92,7.63,6.29L5.24,5.33c-0.22-0.08-0.47,0-0.59,0.22L2.74,8.87 C2.62,9.08,2.66,9.34,2.86,9.48l2.03,1.58C4.84,11.36,4.8,11.69,4.8,12s0.02,0.64,0.07,0.94l-2.03,1.58 c-0.18,0.14-0.23,0.41-0.12,0.61l1.92,3.32c0.12,0.22,0.37,0.29,0.59,0.22l2.39-0.96c0.5,0.38,1.03,0.7,1.62,0.94l0.36,2.54 c0.05,0.24,0.24,0.41,0.48,0.41h3.84c0.24,0,0.44-0.17,0.47-0.41l0.36-2.54c0.59-0.24,1.13-0.56,1.62-0.94l2.39,0.96 c0.22,0.08,0.47,0,0.59-0.22l1.92-3.32c0.12-0.22,0.07-0.47-0.12-0.61L19.14,12.94z M12,15.6c-1.98,0-3.6-1.62-3.6-3.6 s1.62-3.6,3.6-3.6s3.6,1.62,3.6,3.6S13.98,15.6,12,15.6z"/></svg></button></td>`
                            : !state.currentBucket && isAdmin
                              ? '<td></td>'
                              : '';
                    return `
                <tr class="${rowClass}" onclick="${onclick}">
                    <td>${checkbox}</td>
                    <td><div class="s3-item-name">${icon}<span class="${nameClass}">${escapeHtml(item.name)}</span>${deletedBadge}</div></td>
                    <td>${item.size !== null ? formatBytes(item.size) : '-'}</td>
                    ${quotaCell}
                    ${replicationCell}
                    ${ownerCell}
                    <td>${formatDate(item.lastModified)}</td>
                    <td>${formatExpiry(item.expires)}</td>
                    ${actionsCell}
                </tr>
            `;
                })
                .join('');
        }

        // Update pagination UI (only for list view, hide in icon view)
        if (state.viewMode === 'icon') {
            // Hide pagination in icon view (show all with scrolling)
            if (paginationEl) paginationEl.style.display = 'none';
        } else {
            // Show pagination in list view
            const total = state.currentItems.length;
            const shown = Math.min(state.visibleCount, total);
            if (typeof window.updateSectionPagination === 'function') {
                window.updateSectionPagination('s3', {
                    total,
                    shown,
                    hasMore: total > state.visibleCount,
                    canShowLess: state.visibleCount > PAGE_SIZE,
                });
            }
        }

        updateToolbar();
    }

    /* istanbul ignore next */
    function renderIconGrid(items) {
        const iconGrid = document.getElementById('s3-icons');
        if (!iconGrid) return;

        try {
            iconGrid.innerHTML = items
                .map((item) => {
                    // Defensive checks for malformed data
                    if (!item || typeof item !== 'object') {
                        console.warn('Skipping malformed item in icon grid:', item);
                        return '';
                    }

                    const iconType = getItemIcon(item);
                    const displayName = getItemDisplayName(item);
                    const iconSVG = getIconSVG(iconType);
                    const metaHint = buildItemMetadata(item);

                    const isDeleted = Boolean(item.deletedAt);
                    const itemId = item.key || item.name;
                    const isSelected = state.selectedItems.has(itemId);

                    // Store navigation data in data attributes (safe from XSS)
                    const dataAttrs = [
                        `data-item-id="${escapeHtml(itemId)}"`,
                        `data-is-bucket="${item.isBucket ? 'true' : 'false'}"`,
                        `data-is-folder="${item.isFolder ? 'true' : 'false'}"`,
                        item.isBucket ? `data-bucket-name="${escapeHtml(item.name)}"` : '',
                        item.key ? `data-item-key="${escapeHtml(item.key)}"` : '',
                    ]
                        .filter(Boolean)
                        .join(' ');

                    // Checkbox (not for buckets)
                    const checkbox = item.isBucket
                        ? ''
                        : `<input type="checkbox" class="s3-icon-checkbox"
                        data-item-id="${escapeHtml(itemId)}"
                        ${isSelected ? 'checked' : ''}
                        ${state.writable && !isDeleted ? '' : 'disabled'} />`;

                    // Deleted badge
                    const deletedBadge = isDeleted ? '<span class="s3-badge s3-badge-deleted">Deleted</span>' : '';

                    return `
                <div class="s3-icon-item ${isSelected ? 's3-selected' : ''} ${isDeleted ? 's3-recycled' : ''}"
                     ${dataAttrs}>
                    ${checkbox}
                    ${deletedBadge}
                    ${iconSVG}
                    <div class="s3-icon-label">${escapeHtml(displayName)}</div>
                    ${metaHint ? `<div class="s3-icon-meta">${metaHint}</div>` : ''}
                </div>
            `;
                })
                .join('');
        } catch (err) {
            console.error('Error rendering icon grid:', err);
            if (iconGrid) {
                iconGrid.innerHTML = '<div class="empty-state">Error rendering files. Please refresh.</div>';
            }
        }
    }

    function showMore() {
        state.visibleCount += PAGE_SIZE;
        renderFileListing(false);
    }

    function showLess() {
        state.visibleCount = PAGE_SIZE;
        renderFileListing(false);
    }

    function isBinaryContent(content) {
        // Check for null bytes or high ratio of non-printable characters
        const sample = content.slice(0, 8192);
        let nonPrintable = 0;
        for (let i = 0; i < sample.length; i++) {
            const code = sample.charCodeAt(i);
            if (code === 0) return true; // Null byte = definitely binary
            if (code < 32 && code !== 9 && code !== 10 && code !== 13) {
                nonPrintable++;
            }
        }
        return nonPrintable / sample.length > 0.1; // >10% non-printable = binary
    }

    async function openFile(bucket, key, options = {}) {
        const browser = document.getElementById('s3-browser');
        const viewer = document.getElementById('s3-viewer');
        const preview = document.getElementById('s3-preview');
        const editor = document.getElementById('s3-editor');
        const saveBtn = document.getElementById('s3-save-btn');
        const deleteBtn = document.getElementById('s3-delete-btn');
        const readonlyBadge = document.getElementById('s3-readonly-badge');

        const fileName = key.split('/').pop();
        // Check if file is in recycle bin from cached items
        const item = state.currentItems.find((i) => i.key === key);
        const isDeleted = item?.deletedAt;
        // Calculate when recycled file will be purged (deletedAt + 90 days)
        const recycleExpiry = isDeleted
            ? new Date(new Date(item.deletedAt).getTime() + 90 * 24 * 60 * 60 * 1000).toISOString()
            : null;
        const isReadOnly = !state.writable || isDeleted;

        // Build content URL — recyclebin endpoint for deleted files, objects endpoint for live files
        const contentBase = isDeleted
            ? `/api/s3/buckets/${encodeURIComponent(bucket)}/recyclebin/${encodeURIComponent(key)}`
            : `/api/s3/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`;

        // Clear selection when opening file
        state.selectedItems.clear();
        updateSelectionUI();

        state.currentFile = { bucket, key };
        state.isDirty = false;

        // Hide browser, show viewer
        if (browser) browser.style.display = 'none';

        renderBreadcrumb();

        // Handle images specially (display inline)
        if (isImageFile(fileName)) {
            if (viewer) viewer.style.display = 'none';
            if (preview) {
                preview.style.display = 'flex';
                preview.innerHTML = `<img src="${contentBase}" alt="${escapeHtml(fileName)}">`;
            }
            if (saveBtn) saveBtn.style.display = isReadOnly ? 'none' : 'inline-flex';
            if (deleteBtn) deleteBtn.style.display = isReadOnly ? 'none' : 'inline-flex';
            updateToolbar();
            return;
        }

        // Try to open all other files as text
        try {
            const resp = await fetch(contentBase);
            if (!resp.ok) throw new Error(`Failed to get object: ${resp.status}`);
            const content = await resp.text();

            // Check if content is binary
            if (isBinaryContent(content)) {
                throw new Error('Binary file - not for human eyes');
            }

            // Auto-format JSON files for better readability
            let displayContent = content;
            const ext = getExtension(fileName);
            if (ext === 'json') {
                try {
                    const parsed = JSON.parse(content);
                    displayContent = JSON.stringify(parsed, null, 2);
                } catch (_e) {
                    // If JSON parsing fails, show original content
                    displayContent = content;
                }
            }

            state.originalContent = displayContent;

            // Detect JSON type and choose appropriate viewer mode
            if (ext === 'json') {
                const jsonType = detectJsonType(displayContent);
                if (jsonType.isArray) {
                    // JSON arrays → datasheet mode (table view)
                    const datasheetDetect = detectDatasheetMode(displayContent);
                    if (datasheetDetect.isDatasheet) {
                        state.datasheetData = datasheetDetect.data;
                        state.datasheetSchema = inferSchema(datasheetDetect.data);
                        state.datasheetPage = 1;
                        state.editorMode = 'datasheet';
                        state.treeviewData = null;
                    } else {
                        state.editorMode = 'source';
                        state.datasheetData = null;
                        state.datasheetSchema = null;
                        state.treeviewData = null;
                    }
                } else if (jsonType.isObject) {
                    // JSON objects → tree view mode
                    state.treeviewData = jsonType.parsed;
                    state.treeviewCollapsed = new Set();
                    state.editorMode = 'treeview';
                    state.datasheetData = null;
                    state.datasheetSchema = null;
                } else {
                    // Invalid JSON → source mode
                    state.editorMode = 'source';
                    state.datasheetData = null;
                    state.datasheetSchema = null;
                    state.treeviewData = null;
                }
            }
            // Auto-switch to WYSIWYG mode for non-empty markdown files
            else if (shouldUseWysiwygMode(ext, displayContent)) {
                state.editorMode = 'wysiwyg';
                // Enable autosave for markdown files (unless read-only)
                if (!isReadOnly) {
                    state.autosave = true;
                }
            } else {
                state.editorMode = 'source';
            }

            if (viewer) viewer.style.display = 'block';
            if (preview) preview.style.display = 'none';
            if (saveBtn) saveBtn.style.display = isReadOnly ? 'none' : 'inline-flex';

            // Update delete/undelete button
            if (deleteBtn) {
                if (isDeleted) {
                    deleteBtn.innerHTML =
                        '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12.5 8c-2.65 0-5.05.99-6.9 2.6L2 7v9h9l-3.62-3.62c1.39-1.16 3.16-1.88 5.12-1.88 3.54 0 6.55 2.31 7.6 5.5l2.37-.78C21.08 11.03 17.15 8 12.5 8z"/></svg><span>Undelete</span>';
                    deleteBtn.className = 's3-btn';
                    deleteBtn.title = 'Restore this deleted file';
                    deleteBtn.onclick = () => undeleteFile();
                    deleteBtn.style.display = 'inline-flex';
                } else if (isReadOnly) {
                    deleteBtn.style.display = 'none';
                } else {
                    deleteBtn.innerHTML =
                        '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg><span>Delete</span>';
                    deleteBtn.className = 's3-btn s3-btn-danger';
                    deleteBtn.title = 'Delete file';
                    deleteBtn.onclick = () => deleteFile();
                    deleteBtn.style.display = 'inline-flex';
                }
            }

            // Update readonly badge
            if (readonlyBadge) {
                if (isDeleted && recycleExpiry) {
                    const expiryText = TM.format?.formatExpiry ? TM.format.formatExpiry(recycleExpiry) : 'soon';
                    readonlyBadge.textContent = `READ-ONLY - this deleted file will be permanently removed ${expiryText}`;
                    readonlyBadge.style.display = 'block';
                    readonlyBadge.classList.add('s3-readonly-recycled');
                } else if (isReadOnly) {
                    readonlyBadge.textContent = 'READ-ONLY';
                    readonlyBadge.style.display = 'block';
                    readonlyBadge.classList.remove('s3-readonly-recycled');
                } else {
                    readonlyBadge.style.display = 'none';
                    readonlyBadge.classList.remove('s3-readonly-recycled');
                }
            }

            // Update autosave UI
            const autosaveCheckbox = document.getElementById('s3-autosave');
            const autosaveLabel = autosaveCheckbox?.parentElement;
            if (autosaveCheckbox) {
                autosaveCheckbox.checked = state.autosave;
            }
            if (autosaveLabel) {
                autosaveLabel.style.display = isReadOnly ? 'none' : '';
            }

            // Render in appropriate mode
            if (state.editorMode === 'datasheet') {
                // Render datasheet view
                renderDatasheet();
                updateEditorUI('datasheet');
            } else if (state.editorMode === 'treeview') {
                // Render tree view
                renderTreeView();
                updateEditorUI('treeview');
            } else if (state.editorMode === 'wysiwyg') {
                const wysiwyg = document.getElementById('s3-wysiwyg');
                if (wysiwyg) {
                    if (TM.markdown) {
                        // Render markdown as read-only preview
                        wysiwyg.innerHTML = TM.markdown.renderMarkdown(state.originalContent);
                    } else {
                        console.error('TM.markdown not loaded - falling back to source mode');
                        state.editorMode = 'source';
                    }
                }
                if (editor) {
                    editor.value = state.originalContent;
                    editor.readOnly = isReadOnly;
                }

                if (state.editorMode === 'wysiwyg') {
                    updateEditorUI('wysiwyg');
                } else {
                    updateEditorUI('source');
                    updateLineNumbers();
                }
            } else {
                if (editor) {
                    editor.value = state.originalContent;
                    editor.readOnly = isReadOnly;
                }
                updateEditorUI('source');
                updateLineNumbers();
            }

            updateToolbar();

            // Focus editor at position 0 when in source mode
            if (state.editorMode === 'source' && editor && !isReadOnly) {
                editor.focus();
                editor.setSelectionRange(0, 0);
            }

            // Update browser history (unless restoring from history)
            if (!options.skipHistory) {
                updateBrowserHistory();
            }
        } catch (err) {
            showToast(`Failed to load file: ${err.message}`, 'error');
            closeFile();
        }
    }

    async function closeFile() {
        if (state.isDirty) {
            const fileName = state.currentFile.key.split('/').pop();
            if (confirm(`Save changes to "${fileName}" before closing?`)) {
                await saveFile();
            }
        }

        state.currentFile = null;
        state.isDirty = false;
        renderFileListing();

        // Update browser history to reflect closed file
        updateBrowserHistory();
    }

    /* istanbul ignore next */
    function updateLineNumbers() {
        const editor = document.getElementById('s3-editor');
        const lineNumbers = document.getElementById('s3-line-numbers');
        if (!editor || !lineNumbers) return;

        const lines = editor.value.split('\n').length;
        let html = '';
        for (let i = 1; i <= lines; i++) {
            html += `<div>${i}</div>`;
        }
        lineNumbers.innerHTML = html;
    }

    function onEditorInput() {
        const editor = document.getElementById('s3-editor');

        // Check dirty state
        state.isDirty = editor.value !== state.originalContent;

        updateLineNumbers();
        updateSaveButton();

        // Autosave with debounce
        if (state.autosave && state.isDirty) {
            if (state.autosaveTimer) clearTimeout(state.autosaveTimer);
            state.autosaveTimer = setTimeout(() => {
                saveFile();
            }, 1500);
        }
    }

    function setAutosave(enabled) {
        state.autosave = enabled;
        if (!enabled && state.autosaveTimer) {
            clearTimeout(state.autosaveTimer);
            state.autosaveTimer = null;
        }
    }

    function toggleFullscreen() {
        const section = document.getElementById('s3-section');
        const btn = document.getElementById('s3-fullscreen-btn');
        if (!section) return;

        state.isFullscreen = !state.isFullscreen;
        section.classList.toggle('s3-fullscreen', state.isFullscreen);

        // Update button icon
        if (btn) {
            btn.innerHTML = state.isFullscreen
                ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M5 16h3v3h2v-5H5v2zm3-8H5v2h5V5H8v3zm6 11h2v-3h3v-2h-5v5zm2-11V5h-2v5h5V8h-3z"/></svg>'
                : '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z"/></svg>';
        }
    }

    function toggleView() {
        state.viewMode = state.viewMode === 'list' ? 'icon' : 'list';
        updateViewToggleButton();
        renderFileListing(false);
    }

    /* istanbul ignore next */
    function updateViewToggleButton() {
        const btn = document.getElementById('s3-view-toggle-btn');
        if (!btn) return;

        // Hide view toggle when viewing a file, show when browsing
        if (state.currentFile) {
            btn.style.display = 'none';
            return;
        }
        btn.style.display = 'inline-flex';

        const listIcon =
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M4 14h4v-4H4v4zm0 5h4v-4H4v4zM4 9h4V5H4v4zm5 5h12v-4H9v4zm0 5h12v-4H9v4zM9 5v4h12V5H9z"/></svg>';
        const gridIcon =
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 13h8v8H3v-8zm0-10h8v8H3V3zm10 0h8v8h-8V3zm0 10h8v8h-8v-8z"/></svg>';

        btn.innerHTML = state.viewMode === 'list' ? gridIcon : listIcon;
        btn.title = state.viewMode === 'list' ? 'Switch to icon view' : 'Switch to list view';
    }

    // =========================================================================
    // Editor Mode Toggle (Source / WYSIWYG)
    // =========================================================================

    function toggleEditorMode() {
        if (!state.currentFile) return;

        const editor = document.getElementById('s3-editor');
        const wysiwyg = document.getElementById('s3-wysiwyg');
        const ext = getExtension(state.currentFile.key);

        // For JSON files, allow toggling between source/datasheet/treeview modes
        if (ext === 'json') {
            if (state.editorMode === 'datasheet' || state.editorMode === 'treeview') {
                // Switch from datasheet/treeview to source
                state.editorMode = 'source';
                if (editor) {
                    editor.value = state.originalContent;
                    // Update line numbers when switching back to source
                    updateLineNumbers();
                }
            } else {
                // Switch from source to appropriate view mode based on JSON type
                const jsonType = detectJsonType(editor.value);
                if (jsonType.isArray) {
                    // JSON arrays → datasheet mode
                    const detect = detectDatasheetMode(editor.value);
                    if (detect.isDatasheet) {
                        state.editorMode = 'datasheet';
                        state.datasheetData = detect.data;

                        // Cache schema inference: only recompute if content changed
                        // or if we don't have a cached schema
                        const contentUnchanged = editor.value === state.originalContent;
                        if (!state.datasheetSchema || !contentUnchanged) {
                            state.datasheetSchema = inferSchema(detect.data);
                        }

                        state.datasheetPage = 1;
                        state.treeviewData = null;
                        renderDatasheet();
                    } else {
                        const reason = detect.reason || 'must be a JSON array of objects';
                        showToast(`Cannot render as datasheet: ${reason}`, 'error');
                        return;
                    }
                } else if (jsonType.isObject) {
                    // JSON objects → tree view mode
                    state.editorMode = 'treeview';
                    state.treeviewData = jsonType.parsed;
                    state.treeviewCollapsed = new Set();
                    state.datasheetData = null;
                    state.datasheetSchema = null;
                    renderTreeView();
                } else {
                    showToast('Invalid JSON - cannot visualize', 'error');
                    return;
                }
            }
        }
        // For markdown files with WYSIWYG mode
        else if (ext === 'md') {
            // Toggle between source and wysiwyg
            state.editorMode = state.editorMode === 'source' ? 'wysiwyg' : 'source';

            // Render preview from current editor content
            if (state.editorMode === 'wysiwyg' && wysiwyg && editor && TM.markdown) {
                wysiwyg.innerHTML = TM.markdown.renderMarkdown(editor.value);
            }
        }

        // Update UI
        updateEditorUI(state.editorMode);
        updateModeToggleButton();
    }

    /* istanbul ignore next */
    function updateEditorUI(mode) {
        const editor = document.getElementById('s3-editor');
        const wysiwyg = document.getElementById('s3-wysiwyg');
        const datasheet = document.getElementById('s3-datasheet');
        const treeview = document.getElementById('s3-treeview');
        const editorWrap = document.querySelector('.s3-editor-wrap');
        const saveBtn = document.getElementById('s3-save-btn');
        const autosaveLabel = document.querySelector('.s3-autosave-label');

        if (!editor || !wysiwyg) return;

        if (mode === 'datasheet') {
            editor.style.display = 'none';
            wysiwyg.style.display = 'none';
            if (treeview) treeview.style.display = 'none';
            if (datasheet) datasheet.style.display = 'flex';
            if (editorWrap) editorWrap.classList.add('datasheet-mode');
            if (editorWrap) editorWrap.classList.remove('treeview-mode');
            // Hide save and autosave in datasheet mode (view-only)
            if (saveBtn) saveBtn.style.display = 'none';
            if (autosaveLabel) autosaveLabel.style.display = 'none';
        } else if (mode === 'treeview') {
            editor.style.display = 'none';
            wysiwyg.style.display = 'none';
            if (datasheet) datasheet.style.display = 'none';
            if (treeview) treeview.style.display = 'block';
            if (editorWrap) editorWrap.classList.add('treeview-mode');
            if (editorWrap) editorWrap.classList.remove('datasheet-mode');
            // Hide save and autosave in tree view mode (view-only)
            if (saveBtn) saveBtn.style.display = 'none';
            if (autosaveLabel) autosaveLabel.style.display = 'none';
        } else if (mode === 'wysiwyg') {
            editor.style.display = 'none';
            wysiwyg.style.display = 'block';
            if (datasheet) datasheet.style.display = 'none';
            if (treeview) treeview.style.display = 'none';
            if (editorWrap) editorWrap.classList.add('wysiwyg-mode');
            if (editorWrap) editorWrap.classList.remove('datasheet-mode');
            if (editorWrap) editorWrap.classList.remove('treeview-mode');
            // Hide save and autosave in preview mode (read-only)
            if (saveBtn) saveBtn.style.display = 'none';
            if (autosaveLabel) autosaveLabel.style.display = 'none';
        } else {
            editor.style.display = 'block';
            wysiwyg.style.display = 'none';
            if (datasheet) datasheet.style.display = 'none';
            if (treeview) treeview.style.display = 'none';
            if (editorWrap) editorWrap.classList.remove('wysiwyg-mode');
            if (editorWrap) editorWrap.classList.remove('datasheet-mode');
            if (editorWrap) editorWrap.classList.remove('treeview-mode');
            // Show save and autosave in source mode (unless read-only)
            if (saveBtn && !state.writable) {
                saveBtn.style.display = 'none';
            } else if (saveBtn) {
                saveBtn.style.display = 'inline-flex';
            }
            if (autosaveLabel && !state.writable) {
                autosaveLabel.style.display = 'none';
            } else if (autosaveLabel) {
                autosaveLabel.style.display = '';
            }
        }
    }

    /* istanbul ignore next */
    function updateModeToggleButton() {
        const btn = document.getElementById('s3-mode-toggle-btn');
        if (!btn) return;

        const ext = state.currentFile ? getExtension(state.currentFile.key) : '';

        // For JSON files, show appropriate toggle based on JSON type
        if (ext === 'json') {
            btn.style.display = 'inline-flex'; // Make sure button is visible
            if (state.editorMode === 'datasheet' || state.editorMode === 'treeview') {
                // Currently in visual mode, show "Source" button
                btn.title = 'Switch to source mode';
                btn.innerHTML =
                    '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M14.06 9l.94.94L5.92 19H5v-.92L14.06 9m3.6-6c-.25 0-.51.1-.7.29l-1.83 1.83 3.75 3.75 1.83-1.83c.39-.39.39-1.04 0-1.41l-2.34-2.34c-.2-.2-.45-.29-.71-.29zm-3.6 3.19L3 17.25V21h3.75L17.81 9.94l-3.75-3.75z"/></svg><span id="s3-mode-label">Source</span>';
            } else {
                // Currently in source mode, determine button based on JSON type
                const editor = document.getElementById('s3-editor');
                if (editor?.value) {
                    const jsonType = detectJsonType(editor.value);
                    if (jsonType.isArray) {
                        // JSON array → show "Datasheet" button
                        btn.title = 'Switch to datasheet view';
                        btn.innerHTML =
                            '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M3 3v18h18V3H3zm16 16H5V5h14v14zm-2-12H7v2h10V7zm0 4H7v2h10v-2zm0 4H7v2h10v-2z"/></svg><span id="s3-mode-label">Datasheet</span>';
                    } else if (jsonType.isObject) {
                        // JSON object → show "Tree View" button
                        btn.title = 'Switch to tree view';
                        btn.innerHTML =
                            '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2l-5.5 9h11L12 2zm0 3.84L13.93 9h-3.87L12 5.84zM17.5 13c-2.49 0-4.5 2.01-4.5 4.5s2.01 4.5 4.5 4.5 4.5-2.01 4.5-4.5-2.01-4.5-4.5-4.5zm0 7c-1.38 0-2.5-1.12-2.5-2.5s1.12-2.5 2.5-2.5 2.5 1.12 2.5 2.5-1.12 2.5-2.5 2.5zM3 21.5h8v-8H3v8zm2-6h4v4H5v-4z"/></svg><span id="s3-mode-label">Tree View</span>';
                    } else {
                        // Invalid JSON, hide button
                        btn.style.display = 'none';
                    }
                } else {
                    btn.style.display = 'none';
                }
            }
        }
        // For markdown files with WYSIWYG mode
        else if (ext === 'md') {
            btn.style.display = 'inline-flex'; // Make sure button is visible
            if (state.editorMode === 'wysiwyg') {
                // Currently in preview mode, show "Source" button with edit icon
                btn.title = 'Switch to source mode';
                btn.innerHTML =
                    '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M14.06 9l.94.94L5.92 19H5v-.92L14.06 9m3.6-6c-.25 0-.51.1-.7.29l-1.83 1.83 3.75 3.75 1.83-1.83c.39-.39.39-1.04 0-1.41l-2.34-2.34c-.2-.2-.45-.29-.71-.29zm-3.6 3.19L3 17.25V21h3.75L17.81 9.94l-3.75-3.75z"/></svg><span id="s3-mode-label">Source</span>';
            } else {
                // Currently in source mode, show "Preview" button with magnifying glass icon
                btn.title = 'Switch to preview mode';
                btn.innerHTML =
                    '<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><path d="M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z"/></svg><span id="s3-mode-label">Preview</span>';
            }
        }
        // For other files, hide the toggle button
        else {
            btn.style.display = 'none';
        }
    }

    /**
     * Central toolbar FSM — single source of truth for all toolbar visibility.
     *
     * States:
     *   viewing   — a file is open (state.currentFile !== null)
     *   selecting — browsing with ≥1 item selected
     *   browsing  — browsing with no selection
     *
     * Call this after every state transition: navigate, open, close, toggle selection.
     */
    /* istanbul ignore next */
    function updateToolbar() {
        const browseActions = document.getElementById('s3-browse-actions');
        const selectionActions = document.getElementById('s3-selection-actions');
        const fileActions = document.getElementById('s3-file-actions');
        const viewToggleBtn = document.getElementById('s3-view-toggle-btn');
        const closeBtn = document.getElementById('s3-close-btn');
        const renameBtn = document.getElementById('s3-rename-btn');
        const countEl = document.getElementById('s3-selection-count');

        const isViewing = state.currentFile !== null;
        const selCount = state.selectedItems.size;

        if (isViewing) {
            // FILE state: show file actions only
            if (browseActions) browseActions.style.display = 'none';
            if (selectionActions) selectionActions.style.display = 'none';
            if (fileActions) fileActions.style.display = 'flex';
            if (viewToggleBtn) viewToggleBtn.style.display = 'none';
        } else if (selCount > 0) {
            // BROWSING + SELECTION state
            if (browseActions) browseActions.style.display = 'none';
            if (selectionActions) selectionActions.style.display = 'flex';
            if (fileActions) fileActions.style.display = 'none';
            if (viewToggleBtn) viewToggleBtn.style.display = 'inline-flex';
            if (renameBtn) renameBtn.style.display = selCount === 1 ? 'inline-flex' : 'none';
            if (countEl) countEl.textContent = `${selCount} selected`;
        } else {
            // BROWSING + NO SELECTION state
            if (browseActions) browseActions.style.display = state.writable ? 'flex' : 'none';
            if (selectionActions) selectionActions.style.display = 'none';
            if (fileActions) fileActions.style.display = 'none';
            if (viewToggleBtn) viewToggleBtn.style.display = 'inline-flex';
        }

        // Close button enabled only when a file is open
        if (closeBtn) closeBtn.disabled = !state.currentFile;

        // View toggle icon — only needs updating when browsing (hidden otherwise)
        if (!isViewing) updateViewToggleButton();

        // Mode toggle button — only relevant when viewing a file
        updateModeToggleButton();
    }

    /* istanbul ignore next */
    function updateSaveButton() {
        const saveBtn = document.getElementById('s3-save-btn');
        if (!saveBtn) return;

        if (state.isDirty) {
            saveBtn.classList.add('dirty');
        } else {
            saveBtn.classList.remove('dirty');
        }
    }

    function syncScroll() {
        const editor = document.getElementById('s3-editor');
        const lineNumbers = document.getElementById('s3-line-numbers');
        if (editor && lineNumbers) {
            lineNumbers.scrollTop = editor.scrollTop;
        }
    }

    // =========================================================================
    // Browser History Management
    // =========================================================================

    /**
     * Parse URL hash parameters
     * Example: #tab=data&s3_bucket=mybucket&s3_path=folder/
     */
    function parseHashParams() {
        const hash = window.location.hash.slice(1); // Remove '#'
        const params = {};
        if (!hash) return params;

        hash.split('&').forEach((param) => {
            const [key, value] = param.split('=');
            if (key && value !== undefined) {
                params[key] = decodeURIComponent(value);
            }
        });
        return params;
    }

    /**
     * Build URL hash from current state
     */
    function buildHashFromState(tabName) {
        const params = new URLSearchParams();

        // Always include tab
        if (tabName) {
            params.set('tab', tabName);
        }

        // Include S3 state if navigating in S3
        if (state.currentFile) {
            // Viewing a file
            params.set('s3_bucket', state.currentFile.bucket);
            params.set('s3_file', state.currentFile.key);
        } else if (state.currentBucket) {
            // Browsing a bucket/folder
            params.set('s3_bucket', state.currentBucket);
            if (state.currentPath) {
                params.set('s3_path', state.currentPath);
            }
        }

        return params.toString();
    }

    /**
     * Update browser history with current S3 state
     * @param {boolean} replace - Use replaceState instead of pushState
     */
    function updateBrowserHistory(replace = false) {
        // Get current tab from DOM
        const activeTab = document.querySelector('#main-tabs .tab.active');
        const tabName = activeTab ? activeTab.dataset.tab : 'app';

        const hash = buildHashFromState(tabName);
        const url = hash ? `#${hash}` : '#';

        if (replace) {
            window.history.replaceState(null, '', url);
        } else {
            window.history.pushState(null, '', url);
        }
    }

    /**
     * Restore S3 state from URL hash parameters
     */
    async function restoreFromHistory() {
        const params = parseHashParams();

        // Restore tab first (if present)
        if (params.tab && typeof window.switchTab === 'function') {
            window.switchTab(params.tab, { skipHistory: true });
        }

        // Restore S3 state
        if (params.s3_bucket) {
            if (params.s3_file) {
                // Restore file view - but only if not already viewing this file
                if (state.currentFile?.bucket !== params.s3_bucket || state.currentFile?.key !== params.s3_file) {
                    await openFile(params.s3_bucket, params.s3_file, { skipHistory: true });
                }
            } else {
                // Restore bucket/folder view - but only if not already in this location
                const path = params.s3_path || '';
                if (state.currentBucket !== params.s3_bucket || state.currentPath !== path || state.currentFile) {
                    await navigateTo(params.s3_bucket, path, { skipHistory: true });
                }
            }
        } else {
            // No S3 context in URL - return to bucket list if needed
            if (state.currentFile || state.currentBucket) {
                state.currentFile = null;
                state.currentBucket = null;
                state.currentPath = '';
                state.isDirty = false;
                await renderFileListing();
            }
        }
    }

    /**
     * Handle browser back/forward button
     */
    async function handlePopState() {
        await restoreFromHistory();
    }

    // =========================================================================
    // Navigation
    // =========================================================================

    async function navigateTo(bucket, path, options = {}) {
        if (state.isDirty) {
            const fileName = state.currentFile.key.split('/').pop();
            if (!confirm(`Save changes to "${fileName}" before navigating away?`)) {
                return; // Cancel navigation
            }
            await saveFile();
        }

        state.currentBucket = bucket;
        state.currentPath = path;
        state.currentFile = null;
        state.isDirty = false;

        // Look up writable state from bucket info (fetch if not cached)
        if (bucket && state.buckets.length === 0) {
            state.buckets = await fetchBuckets();
        }
        const bucketInfo = state.buckets.find((b) => b.name === bucket);
        state.writable = bucketInfo ? bucketInfo.writable : true;

        await renderFileListing();

        // Update browser history (unless restoring from history)
        if (!options.skipHistory) {
            updateBrowserHistory();
        }
    }

    // =========================================================================
    // File Operations
    // =========================================================================

    async function saveFile() {
        if (!state.currentFile || !state.isDirty) return;

        const editor = document.getElementById('s3-editor');
        const fileName = state.currentFile.key.split('/').pop();

        try {
            // Save from source editor
            const content = editor.value;

            await putObject(state.currentFile.bucket, state.currentFile.key, content, getContentType(fileName));

            // Update original content to match what we just saved
            state.originalContent = content;
            state.isDirty = false;
            updateSaveButton();

            showToast('File saved', 'success');
        } catch (err) {
            showToast(`Failed to save: ${err.message}`, 'error');
        }
    }

    function downloadFile() {
        if (!state.currentFile) return;

        const url = `/api/s3/buckets/${encodeURIComponent(state.currentFile.bucket)}/objects/${encodeURIComponent(state.currentFile.key)}`;
        const a = document.createElement('a');
        a.href = url;
        a.download = state.currentFile.key.split('/').pop();
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }

    async function deleteFile() {
        if (!state.currentFile) return;

        const fileName = state.currentFile.key.split('/').pop();
        if (!confirm(`Delete "${fileName}"?`)) return;

        try {
            await deleteObject(state.currentFile.bucket, state.currentFile.key);
            showToast('File deleted', 'success');
            state.currentFile = null;
            state.isDirty = false;
            await renderFileListing();
        } catch (err) {
            showToast(`Failed to delete: ${err.message}`, 'error');
        }
    }

    async function undeleteFile() {
        if (!state.currentFile) return;

        const { bucket, key } = state.currentFile;
        const fileName = key.split('/').pop();
        if (!confirm(`Restore "${fileName}"?`)) return;

        try {
            await restoreRecycledObject(bucket, key);
            showToast('File restored', 'success');
            // Refresh to show the file as restored
            await renderFileListing();
            // Reopen the file to update the UI
            await openFile(bucket, key);
        } catch (err) {
            showToast(`Failed to restore: ${err.message}`, 'error');
        }
    }

    // =========================================================================
    // New File/Folder
    // =========================================================================

    function openNewModal() {
        if (!state.currentBucket) {
            showToast('Navigate into a bucket first', 'warning');
            return;
        }
        const modal = document.getElementById('s3-new-modal');
        const input = document.getElementById('s3-new-name');
        if (modal) modal.style.display = 'flex';
        if (input) {
            input.value = '';
            input.focus();
        }
    }

    async function createFile() {
        const input = document.getElementById('s3-new-name');
        const name = input?.value.trim();

        if (!name) {
            showToast('Please enter a name', 'warning');
            return;
        }

        const key = state.currentPath + name;

        try {
            await putObject(state.currentBucket, key, '', getContentType(name));
            closeModal('s3-new-modal');
            showToast('File created', 'success');
            await openFile(state.currentBucket, key);
        } catch (err) {
            showToast(`Failed to create file: ${err.message}`, 'error');
        }
    }

    async function createFolder() {
        const input = document.getElementById('s3-new-name');
        const name = input?.value.trim();

        if (!name) {
            showToast('Please enter a name', 'warning');
            return;
        }

        const key = `${state.currentPath + name}/.folder`;

        try {
            await putObject(state.currentBucket, key, '', 'text/plain');
            closeModal('s3-new-modal');
            showToast('Folder created', 'success');
            await navigateTo(state.currentBucket, `${state.currentPath + name}/`);
        } catch (err) {
            showToast(`Failed to create folder: ${err.message}`, 'error');
        }
    }

    function closeModal(modalId) {
        const modal = document.getElementById(modalId);
        if (modal) modal.style.display = 'none';
    }

    // =========================================================================
    // Version History
    // =========================================================================

    async function showVersionHistory() {
        if (!state.currentFile) {
            showToast('No file selected', 'warning');
            return;
        }

        const { bucket, key } = state.currentFile;
        const versions = await fetchVersions(bucket, key);

        if (versions.length === 0) {
            showToast('No version history available', 'info');
            return;
        }

        // Create or get modal - uses standard modal classes for consistency
        let modal = document.getElementById('s3-version-modal');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 's3-version-modal';
            modal.className = 'modal';
            modal.innerHTML = `
                <div class="modal-content modal-wide">
                    <div class="modal-header">
                        <h3>Version History</h3>
                        <button class="modal-close" onclick="TM.s3explorer.closeModal('s3-version-modal')">&times;</button>
                    </div>
                    <div class="modal-body modal-body-table">
                        <table class="s3-version-table">
                            <thead>
                                <tr>
                                    <th>File</th>
                                    <th class="text-right">Size</th>
                                    <th class="text-right">Date</th>
                                    <th class="text-center">Actions</th>
                                </tr>
                            </thead>
                            <tbody id="s3-version-list"></tbody>
                        </table>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
        }

        // Populate version list
        const tbody = document.getElementById('s3-version-list');
        const canRestore = state.writable; // Can only restore if bucket is writable

        tbody.innerHTML = versions
            .map((v) => {
                const currentBadge = v.is_current ? '<span class="s3-badge s3-badge-current">Current</span>' : '';
                // Square icon buttons with tooltips
                const downloadBtn = `<button class="btn-icon s3-version-btn" title="Download this version" onclick="TM.s3explorer.downloadVersion('${escapeJsString(v.version_id)}')">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z"/>
                    </svg>
                </button>`;

                let restoreBtn = '';
                if (!v.is_current) {
                    if (canRestore) {
                        restoreBtn = `<button class="btn-icon s3-version-btn" title="Restore this version" onclick="TM.s3explorer.restoreVersionAndRefresh('${escapeJsString(v.version_id)}')">
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M13 3c-4.97 0-9 4.03-9 9H1l3.89 3.89.07.14L9 12H6c0-3.87 3.13-7 7-7s7 3.13 7 7-3.13 7-7 7c-1.93 0-3.68-.79-4.94-2.06l-1.42 1.42C8.27 19.99 10.51 21 13 21c4.97 0 9-4.03 9-9s-4.03-9-9-9zm-1 5v5l4.28 2.54.72-1.21-3.5-2.08V8H12z"/>
                            </svg>
                        </button>`;
                    } else {
                        restoreBtn = `<button class="btn-icon s3-version-btn" title="Cannot restore (read-only)" disabled>
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                                <path d="M13 3c-4.97 0-9 4.03-9 9H1l3.89 3.89.07.14L9 12H6c0-3.87 3.13-7 7-7s7 3.13 7 7-3.13 7-7 7c-1.93 0-3.68-.79-4.94-2.06l-1.42 1.42C8.27 19.99 10.51 21 13 21c4.97 0 9-4.03 9-9s-4.03-9-9-9zm-1 5v5l4.28 2.54.72-1.21-3.5-2.08V8H12z"/>
                            </svg>
                        </button>`;
                    }
                }

                const fileName = key.split('/').pop();
                return `
                    <tr class="${v.is_current ? 's3-version-current' : ''}">
                        <td>
                            <div class="s3-version-filename">${escapeHtml(fileName)} ${currentBadge}</div>
                            <div class="s3-version-id">${escapeHtml(v.version_id)}</div>
                        </td>
                        <td class="text-right">${formatBytes(v.size)}</td>
                        <td class="text-right">${formatVersionDate(v.last_modified)}</td>
                        <td class="text-center">
                            <div class="btn-group">
                                ${downloadBtn}
                                ${restoreBtn}
                            </div>
                        </td>
                    </tr>
                `;
            })
            .join('');

        modal.style.display = 'flex';
    }

    async function restoreVersionAndRefresh(versionId) {
        if (!state.currentFile) return;

        const { bucket, key } = state.currentFile;

        try {
            await restoreVersion(bucket, key, versionId);
            showToast('Version restored', 'success');
            closeModal('s3-version-modal');

            // Reload the file content
            await openFile(bucket, key);
        } catch (err) {
            showToast(`Failed to restore: ${err.message}`, 'error');
        }
    }

    function downloadVersion(versionId) {
        if (!state.currentFile) return;

        const { bucket, key } = state.currentFile;
        const url = `/api/s3/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}?versionId=${encodeURIComponent(versionId)}`;
        const a = document.createElement('a');
        a.href = url;
        a.download = `${key.split('/').pop()}.${versionId.slice(0, 10)}`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }

    // =========================================================================
    // Upload
    // =========================================================================

    function openUploadDialog() {
        if (!state.currentBucket) {
            showToast('Navigate into a bucket first', 'warning');
            return;
        }
        document.getElementById('s3-file-input')?.click();
    }

    async function handleFileSelect(event) {
        const files = event.target.files;
        await uploadFiles(files);
        event.target.value = '';
    }

    async function uploadFiles(files) {
        if (!state.currentBucket) {
            showToast('Navigate into a bucket first', 'warning');
            return;
        }

        for (const file of files) {
            const key = state.currentPath + file.name;

            try {
                const content = await file.arrayBuffer();
                await fetch(
                    `/api/s3/buckets/${encodeURIComponent(state.currentBucket)}/objects/${encodeURIComponent(key)}`,
                    {
                        method: 'PUT',
                        headers: { 'Content-Type': file.type || 'application/octet-stream' },
                        body: content,
                    },
                );
                showToast(`Uploaded ${file.name}`, 'success');
            } catch (_err) {
                showToast(`Failed to upload ${file.name}`, 'error');
            }
        }

        await renderFileListing();
    }

    // =========================================================================
    // Drag and Drop
    // =========================================================================

    /* istanbul ignore next */
    function initDragDrop() {
        const section = document.getElementById('s3-section');
        const dropZone = document.getElementById('s3-drop-zone');

        if (!section || !dropZone) return;

        let dragCounter = 0;

        section.addEventListener('dragenter', (e) => {
            e.preventDefault();
            dragCounter++;
            if (state.currentBucket) {
                dropZone.classList.add('active');
            }
        });

        section.addEventListener('dragleave', (e) => {
            e.preventDefault();
            dragCounter--;
            if (dragCounter === 0) {
                dropZone.classList.remove('active');
            }
        });

        section.addEventListener('dragover', (e) => {
            e.preventDefault();
        });

        section.addEventListener('drop', async (e) => {
            e.preventDefault();
            dragCounter = 0;
            dropZone.classList.remove('active');

            if (!state.currentBucket) {
                showToast('Navigate into a bucket first', 'warning');
                return;
            }

            await uploadFiles(e.dataTransfer.files);
        });
    }

    // =========================================================================
    // Keyboard Shortcuts
    // =========================================================================

    /* istanbul ignore next */
    function initKeyboardShortcuts() {
        document.addEventListener('keydown', (e) => {
            const editor = document.getElementById('s3-editor');
            if (!editor || document.activeElement !== editor) return;

            if ((e.ctrlKey || e.metaKey) && e.key === 's') {
                e.preventDefault();
                saveFile();
            }
        });
    }

    // =========================================================================
    // Selection
    // =========================================================================

    function toggleSelection(itemId) {
        if (state.selectedItems.has(itemId)) {
            state.selectedItems.delete(itemId);
        } else {
            state.selectedItems.add(itemId);
        }
        updateSelectionUI();
        updateRowSelectionVisuals();
    }

    /* istanbul ignore next */
    function updateSelectionUI() {
        updateToolbar();
    }

    function updateRowSelectionVisuals() {
        const rows = document.querySelectorAll('#s3-files-body tr');
        rows.forEach((row) => {
            const checkbox = row.querySelector('.s3-checkbox');
            if (checkbox) {
                const itemId = checkbox.dataset.itemId;
                const isSelected = state.selectedItems.has(itemId);
                checkbox.checked = isSelected;
                row.classList.toggle('s3-selected', isSelected);
            }
        });
    }

    function getSelectedItem() {
        if (state.selectedItems.size !== 1) return null;
        const itemId = [...state.selectedItems][0];
        return state.currentItems.find((item) => (item.key || item.name) === itemId);
    }

    async function renameSelected() {
        const item = getSelectedItem();
        if (!item) return;

        const oldName = item.name;
        const newName = prompt('Enter new name:', oldName);
        if (!newName || newName === oldName) return;

        // Validate name
        if (newName.includes('/')) {
            alert('Name cannot contain /');
            return;
        }

        if (item.isBucket) {
            alert('Buckets cannot be renamed');
            return;
        }

        if (item.isFolder) {
            alert('Folders cannot be renamed (would require renaming all contents)');
            return;
        }

        try {
            const oldKey = item.key || item.name;
            const newKey = state.currentPath + newName;

            // S3 rename = GET + PUT + DELETE (no native copy support)
            // First get the file content
            const getResp = await fetch(
                `/api/s3/buckets/${encodeURIComponent(state.currentBucket)}/objects/${encodeURIComponent(oldKey)}`,
            );
            if (!getResp.ok) {
                throw new Error(`Failed to read file: ${getResp.status}`);
            }
            const content = await getResp.blob();

            // Put to new key
            const putResp = await fetch(
                `/api/s3/buckets/${encodeURIComponent(state.currentBucket)}/objects/${encodeURIComponent(newKey)}`,
                {
                    method: 'PUT',
                    body: content,
                },
            );
            if (!putResp.ok) {
                throw new Error(`Failed to create new file: ${putResp.status}`);
            }

            // Delete old key
            const deleteResp = await fetch(
                `/api/s3/buckets/${encodeURIComponent(state.currentBucket)}/objects/${encodeURIComponent(oldKey)}`,
                {
                    method: 'DELETE',
                },
            );
            if (!deleteResp.ok) {
                throw new Error(`Failed to delete old file: ${deleteResp.status}`);
            }

            state.selectedItems.clear();
            await renderFileListing();
        } catch (err) {
            console.error('Rename failed:', err);
            alert(`Rename failed: ${err.message}`);
        }
    }

    async function deleteSelected() {
        const count = state.selectedItems.size;
        if (count === 0) return;

        const confirmMsg = count === 1 ? 'Delete this item?' : `Delete ${count} items?`;

        if (!confirm(confirmMsg)) return;

        try {
            let deletedCount = 0;
            for (const itemId of state.selectedItems) {
                const item = state.currentItems.find((i) => (i.key || i.name) === itemId);
                if (!item || item.isBucket) continue;

                // Handle folder deletion
                if (item.isFolder) {
                    const folderPrefix = item.key; // Already ends with '/'

                    // Check if folder is empty (only contains .folder marker or nothing)
                    const objects = await fetchObjects(state.currentBucket, folderPrefix);
                    const nonMarkerObjects = objects.filter((obj) => !obj.key.endsWith('/.folder'));

                    if (nonMarkerObjects.length > 0) {
                        showToast(
                            `Cannot delete "${item.name}" - folder contains ${nonMarkerObjects.length} item${nonMarkerObjects.length > 1 ? 's' : ''}`,
                            'error',
                        );
                        continue;
                    }

                    // Delete the .folder marker if it exists
                    const markerKey = `${folderPrefix}.folder`;
                    await deleteObject(state.currentBucket, markerKey);
                    deletedCount++;
                } else {
                    // Regular file deletion
                    const key = item.key || item.name;
                    await deleteObject(state.currentBucket, key);
                    deletedCount++;
                }
            }

            state.selectedItems.clear();
            updateSelectionUI();
            if (deletedCount > 0) {
                showToast(`Deleted ${deletedCount} item${deletedCount > 1 ? 's' : ''}`, 'success');
            }
            await renderFileListing();
        } catch (err) {
            console.error('Delete failed:', err);
            showToast(`Delete failed: ${err.message}`, 'error');
        }
    }

    // =========================================================================
    // Initialization
    // =========================================================================

    /* istanbul ignore next */
    function initIconGridEvents() {
        const iconGrid = document.getElementById('s3-icons');
        if (!iconGrid) return;

        // Use event delegation for icon grid clicks (prevents XSS via onclick)
        iconGrid.addEventListener('click', (e) => {
            // Find the icon item (in case user clicked on child element)
            const iconItem = e.target.closest('.s3-icon-item');
            if (!iconItem) return;

            // Ignore if clicking on checkbox
            if (e.target.classList.contains('s3-icon-checkbox')) return;

            // Get item data from data attributes
            const isBucket = iconItem.dataset.isBucket === 'true';
            const isFolder = iconItem.dataset.isFolder === 'true';
            const bucketName = iconItem.dataset.bucketName;
            const itemKey = iconItem.dataset.itemKey;

            // Navigate based on item type
            if (isBucket) {
                navigateTo(bucketName, '');
            } else if (isFolder) {
                navigateTo(state.currentBucket, itemKey);
            } else {
                openFile(state.currentBucket, itemKey);
            }
        });

        // Handle checkbox clicks with event delegation
        iconGrid.addEventListener('change', (e) => {
            if (e.target.classList.contains('s3-icon-checkbox')) {
                const itemId = e.target.dataset.itemId;
                toggleSelection(itemId);
            }
        });
    }

    /**
     * Initialize datasheet event delegation
     * Handles clicks on nested data cells without inline onclick handlers (XSS prevention)
     */
    function initDatasheetEvents() {
        const datasheetTable = document.getElementById('s3-datasheet-table');
        if (!datasheetTable) return;

        // Use event delegation for nested data clicks (prevents XSS via onclick)
        datasheetTable.addEventListener('click', (e) => {
            // Find the nested data element (in case user clicked on child element)
            const nestedElement = e.target.closest('.s3-ds-nested');
            if (!nestedElement) return;

            // Get row and column data from data attributes
            const rowIdx = Number.parseInt(nestedElement.dataset.rowIdx, 10);
            const colKey = nestedElement.dataset.colKey;

            // Validate inputs before calling openNestedModal
            if (Number.isNaN(rowIdx) || !colKey) return;

            openNestedModal(rowIdx, colKey);
        });
    }

    /**
     * Initialize treeview event delegation
     * Handles clicks on tree toggles and datasheet links without inline onclick handlers
     */
    function initTreeviewEvents() {
        const treeview = document.getElementById('s3-treeview');
        if (!treeview) return;

        // Use event delegation for tree toggle and datasheet link clicks
        treeview.addEventListener('click', (e) => {
            // Handle tree toggle clicks
            const toggleElement = e.target.closest('.tree-toggle');
            if (toggleElement) {
                const path = toggleElement.dataset.treePath;
                if (path) {
                    toggleTreeNode(path);
                }
                return;
            }

            // Handle datasheet link clicks
            const datasheetLink = e.target.closest('.tree-datasheet-link');
            if (datasheetLink) {
                e.preventDefault();
                const path = datasheetLink.dataset.treePath;
                if (path && datasheetLink.dataset.treeArray === 'true') {
                    openArrayAsDatasheet(path);
                }
                return;
            }
        });
    }

    /**
     * Open a tree array as datasheet view
     * @param {string} path - The path to the array in the tree
     */
    function openArrayAsDatasheet(path) {
        if (!state.treeviewData) return;

        // Navigate to the array value using the path
        const pathParts = path.split('.').filter((p) => p && p !== 'root');
        let value = state.treeviewData;

        for (const part of pathParts) {
            if (part.startsWith('[') && part.endsWith(']')) {
                // Array index
                const index = Number.parseInt(part.slice(1, -1), 10);
                value = value[index];
            } else {
                // Object key
                value = value[part];
            }

            if (value === undefined) {
                showToast('Array not found', 'error');
                return;
            }
        }

        // Verify it's an array
        if (!Array.isArray(value)) {
            showToast('Selected item is not an array', 'error');
            return;
        }

        // Check if it can be displayed as datasheet
        const detect = detectDatasheetMode(JSON.stringify(value));
        if (!detect.isDatasheet) {
            const reason = detect.reason || 'must be a JSON array of objects';
            showToast(`Cannot render as datasheet: ${reason}`, 'error');
            return;
        }

        // Switch to datasheet mode
        state.editorMode = 'datasheet';
        state.datasheetData = detect.data;
        state.datasheetSchema = inferSchema(detect.data);
        state.datasheetPage = 1;
        state.treeviewData = null;
        renderDatasheet();
        updateEditorUI('datasheet');
        updateModeToggleButton();
    }

    /* istanbul ignore next */
    async function init() {
        const editor = document.getElementById('s3-editor');
        if (editor) {
            editor.addEventListener('input', onEditorInput);
            editor.addEventListener('scroll', syncScroll);
        }

        initDragDrop();
        initKeyboardShortcuts();
        initIconGridEvents();
        initDatasheetEvents();
        initTreeviewEvents();
        updateToolbar();

        // Listen for browser back/forward navigation
        window.addEventListener('popstate', handlePopState);

        // Listen for panel data changes to refresh S3 explorer
        // (user might be browsing state metadata like filter rules, groups, etc.)
        if (typeof TM !== 'undefined' && TM.events) {
            TM.events.on('panelDataChanged', () => {
                // Only refresh if S3 explorer is visible and has data
                if (state.currentBucket) {
                    renderFileListing();
                }
            });
        }

        // Listen for window resize to recalculate datasheet page size
        if (typeof TM !== 'undefined' && TM.utils && TM.utils.debounce) {
            const handleResize = TM.utils.debounce(() => {
                // Only re-render if currently in datasheet mode
                if (state.editorMode === 'datasheet' && state.datasheetData) {
                    renderDatasheet();
                }
            }, 200); // Debounce by 200ms

            window.addEventListener('resize', handleResize);
        }

        // Restore state from URL hash on initial load
        const params = parseHashParams();
        if (params.s3_bucket || params.s3_file) {
            await restoreFromHistory();
        } else {
            await renderFileListing();
        }
    }

    // =========================================================================
    // Public API
    // =========================================================================

    return {
        init,
        navigateTo,
        openFile,
        closeFile,
        saveFile,
        downloadFile,
        deleteFile,
        undeleteFile,
        openNewModal,
        createFile,
        createFolder,
        closeModal,
        openUploadDialog,
        handleFileSelect,
        refresh: renderFileListing,
        showMore,
        showLess,
        toggleSelection,
        renameSelected,
        deleteSelected,
        setAutosave,
        toggleFullscreen,
        toggleView,
        updateViewToggleButton,
        toggleEditorMode,
        // Datasheet
        nextPage,
        prevPage,
        openNestedModal,
        closeNestedModal,
        // Tree view
        toggleTreeNode,
        // Version history
        showVersionHistory,
        restoreVersionAndRefresh,
        downloadVersion,
        // Test-only exports (prefixed with _test)
        _test: {
            getItemIcon,
            getItemDisplayName,
            getIconSVG,
            buildItemMetadata,
            buildOnclickHandler,
            shouldUseWysiwygMode,
            detectJsonType,
            detectDatasheetMode,
            inferSchema,
        },
    };
});
