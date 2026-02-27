// Tests for s3explorer.js utility functions
import { describe, test, expect } from 'bun:test';
import s3explorer from '../s3explorer.js';

const {
    getItemIcon,
    getItemDisplayName,
    getIconSVG,
    buildItemMetadata,
    buildOnclickHandler,
    shouldUseWysiwygMode,
    detectJsonType,
    detectDatasheetMode,
    inferSchema,
    validateRename,
    classifySelection,
} = s3explorer._test;

describe('getItemIcon', () => {
    test('returns "share" for bucket with fs+ prefix', () => {
        const item = { isBucket: true, name: 'fs+myshare' };
        expect(getItemIcon(item)).toBe('share');
    });

    test('returns "bucket" for regular bucket', () => {
        const item = { isBucket: true, name: 'mybucket' };
        expect(getItemIcon(item)).toBe('bucket');
    });

    test('returns "folder" for folder', () => {
        const item = { isFolder: true, name: 'myfolder' };
        expect(getItemIcon(item)).toBe('folder');
    });

    test('returns "file" for regular file', () => {
        const item = { name: 'myfile.txt' };
        expect(getItemIcon(item)).toBe('file');
    });
});

describe('getItemDisplayName', () => {
    test('strips fs+ prefix from share buckets', () => {
        const item = { isBucket: true, name: 'fs+documents' };
        expect(getItemDisplayName(item)).toBe('documents');
    });

    test('keeps name for regular buckets', () => {
        const item = { isBucket: true, name: 'mybucket' };
        expect(getItemDisplayName(item)).toBe('mybucket');
    });

    test('keeps name for folders', () => {
        const item = { isFolder: true, name: 'myfolder' };
        expect(getItemDisplayName(item)).toBe('myfolder');
    });

    test('keeps name for files', () => {
        const item = { name: 'myfile.txt' };
        expect(getItemDisplayName(item)).toBe('myfile.txt');
    });

    test('handles empty names', () => {
        const item = { name: '' };
        expect(getItemDisplayName(item)).toBe('');
    });
});

describe('getIconSVG', () => {
    test('returns file icon SVG', () => {
        const svg = getIconSVG('file');
        expect(svg).toContain('class="s3-large-icon"');
        expect(svg).toContain('width="64"');
        expect(svg).toContain('height="64"');
        expect(svg).toContain('<path');
    });

    test('returns folder icon SVG', () => {
        const svg = getIconSVG('folder');
        expect(svg).toContain('class="s3-large-icon"');
        expect(svg).toContain('<path');
    });

    test('returns bucket icon SVG', () => {
        const svg = getIconSVG('bucket');
        expect(svg).toContain('class="s3-large-icon"');
        expect(svg).toContain('<path');
    });

    test('returns share icon SVG', () => {
        const svg = getIconSVG('share');
        expect(svg).toContain('class="s3-large-icon"');
        expect(svg).toContain('<path');
    });

    test('falls back to file icon for unknown type', () => {
        const svg = getIconSVG('unknown');
        const fileSvg = getIconSVG('file');
        expect(svg).toBe(fileSvg);
    });
});

describe('buildItemMetadata', () => {
    test('formats file size', () => {
        const item = { size: 1024 };
        const metadata = buildItemMetadata(item);
        expect(metadata).toContain('KB');
    });

    test('formats quota for buckets without size', () => {
        const item = { size: null, quota: 1024 };
        const metadata = buildItemMetadata(item);
        expect(metadata).toContain('/');
        expect(metadata).toContain('KB');
    });

    test('formats date', () => {
        const item = { lastModified: new Date().toISOString() };
        const metadata = buildItemMetadata(item);
        expect(metadata).toContain('Today');
    });

    test('combines size and date with separator', () => {
        const item = {
            size: 1024,
            lastModified: new Date().toISOString(),
        };
        const metadata = buildItemMetadata(item);
        expect(metadata).toContain('•');
        expect(metadata).toContain('KB');
        expect(metadata).toContain('Today');
    });

    test('returns empty string for folders without metadata', () => {
        const item = { isFolder: true, size: null };
        const metadata = buildItemMetadata(item);
        expect(metadata).toBe('');
    });

    test('handles null/undefined values', () => {
        const item = { size: null, lastModified: null };
        const metadata = buildItemMetadata(item);
        expect(metadata).toBe('');
    });
});

describe('buildOnclickHandler', () => {
    test('builds bucket navigation handler', () => {
        const item = { isBucket: true, name: 'mybucket' };
        const handler = buildOnclickHandler(item);
        expect(handler).toContain('TM.s3explorer.navigateTo');
        expect(handler).toContain('mybucket');
        expect(handler).toContain("''");
    });

    test('builds folder navigation handler', () => {
        const item = { isFolder: true, key: 'folder/' };
        const handler = buildOnclickHandler(item);
        expect(handler).toContain('TM.s3explorer.navigateTo');
        expect(handler).toContain('folder/');
    });

    test('builds file open handler', () => {
        const item = { key: 'file.txt' };
        const handler = buildOnclickHandler(item);
        expect(handler).toContain('TM.s3explorer.openFile');
        expect(handler).toContain('file.txt');
    });

    test('escapes special characters in names', () => {
        const item = { isBucket: true, name: "bucket'with'quotes" };
        const handler = buildOnclickHandler(item);
        expect(handler).toContain("\\'");
    });
});

describe('shouldUseWysiwygMode', () => {
    test('returns true for non-empty markdown files', () => {
        expect(shouldUseWysiwygMode('md', '# Hello World')).toBe(true);
        expect(shouldUseWysiwygMode('md', 'Some content')).toBe(true);
        expect(shouldUseWysiwygMode('md', '   \n\n Content with whitespace   ')).toBe(true);
    });

    test('returns false for empty markdown files', () => {
        expect(shouldUseWysiwygMode('md', '')).toBe(false);
        expect(shouldUseWysiwygMode('md', '   ')).toBe(false);
        expect(shouldUseWysiwygMode('md', '\n\n\t  \n')).toBe(false);
    });

    test('returns false for null/undefined content', () => {
        expect(shouldUseWysiwygMode('md', null)).toBe(false);
        expect(shouldUseWysiwygMode('md', undefined)).toBe(false);
    });

    test('returns false for non-markdown files', () => {
        expect(shouldUseWysiwygMode('txt', 'Some content')).toBe(false);
        expect(shouldUseWysiwygMode('json', '{"key": "value"}')).toBe(false);
        expect(shouldUseWysiwygMode('js', 'console.log("hello");')).toBe(false);
        expect(shouldUseWysiwygMode('', 'content')).toBe(false);
    });

    test('returns false for non-markdown files even when empty', () => {
        expect(shouldUseWysiwygMode('txt', '')).toBe(false);
        expect(shouldUseWysiwygMode('json', '')).toBe(false);
    });
});

describe('detectJsonType', () => {
    test('detects JSON objects correctly', () => {
        const result = detectJsonType('{"name": "test", "value": 123}');
        expect(result.isObject).toBe(true);
        expect(result.isArray).toBe(false);
        expect(result.parsed).toEqual({ name: 'test', value: 123 });
    });

    test('detects JSON arrays correctly', () => {
        const result = detectJsonType('[1, 2, 3]');
        expect(result.isObject).toBe(false);
        expect(result.isArray).toBe(true);
        expect(result.parsed).toEqual([1, 2, 3]);
    });

    test('detects nested objects', () => {
        const result = detectJsonType('{"user": {"name": "Alice", "age": 30}}');
        expect(result.isObject).toBe(true);
        expect(result.isArray).toBe(false);
        expect(result.parsed).toEqual({ user: { name: 'Alice', age: 30 } });
    });

    test('detects array of objects', () => {
        const result = detectJsonType('[{"name": "Alice"}, {"name": "Bob"}]');
        expect(result.isObject).toBe(false);
        expect(result.isArray).toBe(true);
        expect(result.parsed).toEqual([{ name: 'Alice' }, { name: 'Bob' }]);
    });

    test('handles empty object', () => {
        const result = detectJsonType('{}');
        expect(result.isObject).toBe(true);
        expect(result.isArray).toBe(false);
        expect(result.parsed).toEqual({});
    });

    test('handles empty array', () => {
        const result = detectJsonType('[]');
        expect(result.isObject).toBe(false);
        expect(result.isArray).toBe(true);
        expect(result.parsed).toEqual([]);
    });

    test('handles invalid JSON', () => {
        const result = detectJsonType('not valid json');
        expect(result.isObject).toBe(false);
        expect(result.isArray).toBe(false);
        expect(result.parsed).toBe(null);
    });

    test('handles null input', () => {
        const result = detectJsonType('null');
        expect(result.isObject).toBe(false);
        expect(result.isArray).toBe(false);
        expect(result.parsed).toBe(null);
    });

    test('handles primitive values', () => {
        expect(detectJsonType('123').isObject).toBe(false);
        expect(detectJsonType('"string"').isObject).toBe(false);
        expect(detectJsonType('true').isObject).toBe(false);
    });

    test('handles complex nested structures', () => {
        const json = JSON.stringify({
            users: [
                { name: 'Alice', age: 30 },
                { name: 'Bob', age: 25 },
            ],
            metadata: {
                created: '2024-01-01',
                version: 1,
            },
        });
        const result = detectJsonType(json);
        expect(result.isObject).toBe(true);
        expect(result.isArray).toBe(false);
        expect(result.parsed.users).toHaveLength(2);
    });
});

describe('detectDatasheetMode', () => {
    test('detects valid JSON array of objects', () => {
        const content = JSON.stringify([
            { name: 'Alice', age: 30 },
            { name: 'Bob', age: 25 },
        ]);
        const result = detectDatasheetMode(content);
        expect(result.isDatasheet).toBe(true);
        expect(result.data).toHaveLength(2);
        expect(result.reason).toBe(null);
    });

    test('rejects invalid JSON with syntax error', () => {
        const content = '{ invalid json }';
        const result = detectDatasheetMode(content);
        expect(result.isDatasheet).toBe(false);
        expect(result.data).toBe(null);
        expect(result.reason).toContain('Invalid JSON');
    });

    test('rejects JSON object (not array)', () => {
        const content = JSON.stringify({ name: 'Alice', age: 30 });
        const result = detectDatasheetMode(content);
        expect(result.isDatasheet).toBe(false);
        expect(result.data).toBe(null);
        expect(result.reason).toContain('Not a JSON array');
    });

    test('rejects JSON primitive (not array)', () => {
        const content = JSON.stringify(123);
        const result = detectDatasheetMode(content);
        expect(result.isDatasheet).toBe(false);
        expect(result.data).toBe(null);
        expect(result.reason).toContain('Not a JSON array');
    });

    test('rejects empty array', () => {
        const content = JSON.stringify([]);
        const result = detectDatasheetMode(content);
        expect(result.isDatasheet).toBe(false);
        expect(result.data).toBe(null);
        expect(result.reason).toContain('Empty array');
    });

    test('rejects array of primitives', () => {
        const content = JSON.stringify([1, 2, 3, 4, 5]);
        const result = detectDatasheetMode(content);
        expect(result.isDatasheet).toBe(false);
        expect(result.data).toBe(null);
        expect(result.reason).toContain('non-object elements');
    });

    test('rejects array of arrays', () => {
        const content = JSON.stringify([
            [1, 2, 3],
            [4, 5, 6],
        ]);
        const result = detectDatasheetMode(content);
        expect(result.isDatasheet).toBe(false);
        expect(result.data).toBe(null);
        expect(result.reason).toContain('non-object elements');
    });

    test('rejects mixed array (objects and primitives)', () => {
        const content = JSON.stringify([{ name: 'Alice' }, 123, 'string']);
        const result = detectDatasheetMode(content);
        expect(result.isDatasheet).toBe(false);
        expect(result.data).toBe(null);
        expect(result.reason).toContain('non-object elements');
    });
});

describe('inferSchema', () => {
    test('returns empty columns for empty data', () => {
        const result = inferSchema([]);
        expect(result.columns).toEqual([]);
    });

    test('infers number type correctly', () => {
        const data = [{ age: 30 }, { age: 25 }, { age: 35 }];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(1);
        expect(result.columns[0].key).toBe('age');
        expect(result.columns[0].type).toBe('number');
    });

    test('infers boolean type correctly', () => {
        const data = [{ active: true }, { active: false }, { active: true }];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(1);
        expect(result.columns[0].key).toBe('active');
        expect(result.columns[0].type).toBe('boolean');
    });

    test('infers string type correctly', () => {
        const data = [{ name: 'Alice' }, { name: 'Bob' }];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(1);
        expect(result.columns[0].key).toBe('name');
        expect(result.columns[0].type).toBe('string');
    });

    test('infers nested-array type correctly', () => {
        const data = [{ items: [1, 2, 3] }, { items: [4, 5] }];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(1);
        expect(result.columns[0].key).toBe('items');
        expect(result.columns[0].type).toBe('nested-array');
    });

    test('infers nested-object type correctly', () => {
        const data = [{ metadata: { created: '2024-01-01' } }, { metadata: { created: '2024-01-02' } }];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(1);
        expect(result.columns[0].key).toBe('metadata');
        expect(result.columns[0].type).toBe('nested-object');
    });

    test('infers URL type correctly', () => {
        const data = [{ website: 'https://example.com' }, { website: 'http://test.com' }];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(1);
        expect(result.columns[0].key).toBe('website');
        expect(result.columns[0].type).toBe('url');
    });

    test('infers date type correctly', () => {
        const data = [{ created: '2024-01-01T10:00:00Z' }, { created: '2024-01-02T12:00:00Z' }];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(1);
        expect(result.columns[0].key).toBe('created');
        expect(result.columns[0].type).toBe('date');
    });

    test('defaults to string for mixed types in same column', () => {
        const data = [{ value: 123 }, { value: 'text' }, { value: true }];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(1);
        expect(result.columns[0].key).toBe('value');
        expect(result.columns[0].type).toBe('string');
    });

    test('handles missing values (null) correctly', () => {
        const data = [{ name: 'Alice' }, { name: null }, { name: 'Bob' }];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(1);
        expect(result.columns[0].key).toBe('name');
        expect(result.columns[0].type).toBe('string');
    });

    test('collects all keys from inconsistent schemas', () => {
        const data = [
            { name: 'Alice', age: 30 },
            { name: 'Bob', email: 'bob@example.com' },
            { age: 25, active: true },
        ];
        const result = inferSchema(data);
        expect(result.columns).toHaveLength(4);
        const keys = result.columns.map((col) => col.key);
        expect(keys).toContain('name');
        expect(keys).toContain('age');
        expect(keys).toContain('email');
        expect(keys).toContain('active');
    });

    test('assigns default column width', () => {
        const data = [{ name: 'Alice' }];
        const result = inferSchema(data);
        expect(result.columns[0].width).toBe(150);
    });
});

// =============================================================================
// validateRename
// =============================================================================

describe('validateRename', () => {
    const liveFile = { name: 'report.txt', key: 'docs/report.txt' };
    const bucket = { name: 'mybucket', isBucket: true };
    const folder = { name: 'myfolder', key: 'myfolder/', isFolder: true };

    test('returns null for a valid rename', () => {
        expect(validateRename('report-v2.txt', liveFile)).toBe(null);
    });

    test('allows extension change', () => {
        expect(validateRename('report.md', liveFile)).toBe(null);
    });

    test('allows renaming to same name (no-op guard is caller responsibility)', () => {
        // validateRename only validates constraints, not whether name changed
        expect(validateRename('report.txt', liveFile)).toBe(null);
    });

    test('returns error for empty name', () => {
        expect(validateRename('', liveFile)).toBeTruthy();
    });

    test('returns error for whitespace-only name', () => {
        expect(validateRename('   ', liveFile)).toBeTruthy();
        expect(validateRename('\t\n', liveFile)).toBeTruthy();
    });

    test('returns error when name contains a slash', () => {
        expect(validateRename('path/file.txt', liveFile)).toBeTruthy();
        expect(validateRename('/leading-slash', liveFile)).toBeTruthy();
        expect(validateRename('trailing/', liveFile)).toBeTruthy();
    });

    test('returns error when trying to rename a bucket', () => {
        expect(validateRename('newname', bucket)).toBeTruthy();
    });

    test('returns error when trying to rename a folder', () => {
        expect(validateRename('newname', folder)).toBeTruthy();
    });

    test('returns a string (not just truthy) for every error case', () => {
        expect(typeof validateRename('', liveFile)).toBe('string');
        expect(typeof validateRename('a/b', liveFile)).toBe('string');
        expect(typeof validateRename('ok', bucket)).toBe('string');
        expect(typeof validateRename('ok', folder)).toBe('string');
    });

    test('valid name does not include slash even at end', () => {
        expect(validateRename('validname/', liveFile)).toBeTruthy();
    });
});

// =============================================================================
// classifySelection
// =============================================================================

describe('classifySelection', () => {
    // Shared fixture: a realistic listing inside a bucket
    const items = [
        { name: 'readme.md', key: 'readme.md' },
        { name: 'data.json', key: 'data.json' },
        { name: 'images/', key: 'images/', isFolder: true },
        { name: 'archive/', key: 'archive/', isFolder: true },
        { name: 'old.txt', key: 'old.txt', deletedAt: '2024-06-01T00:00:00Z' },
        { name: 'draft.md', key: 'draft.md', deletedAt: '2024-07-15T12:00:00Z' },
        { name: 'mybucket', isBucket: true },
    ];

    test('empty selection returns all empty arrays', () => {
        const result = classifySelection(new Set(), items);
        expect(result.allItems).toHaveLength(0);
        expect(result.liveItems).toHaveLength(0);
        expect(result.deletedItems).toHaveLength(0);
        expect(result.liveFiles).toHaveLength(0);
    });

    test('single live file classified correctly', () => {
        const result = classifySelection(new Set(['readme.md']), items);
        expect(result.allItems).toHaveLength(1);
        expect(result.liveItems).toHaveLength(1);
        expect(result.deletedItems).toHaveLength(0);
        expect(result.liveFiles).toHaveLength(1);
    });

    test('single deleted file classified correctly', () => {
        const result = classifySelection(new Set(['old.txt']), items);
        expect(result.allItems).toHaveLength(1);
        expect(result.liveItems).toHaveLength(0);
        expect(result.deletedItems).toHaveLength(1);
        expect(result.liveFiles).toHaveLength(0);
    });

    test('live folder appears in liveItems but not liveFiles', () => {
        const result = classifySelection(new Set(['images/']), items);
        expect(result.liveItems).toHaveLength(1);
        expect(result.liveFiles).toHaveLength(0);
        expect(result.deletedItems).toHaveLength(0);
    });

    test('bucket excluded from liveItems and liveFiles', () => {
        const result = classifySelection(new Set(['mybucket']), items);
        expect(result.allItems).toHaveLength(1);
        expect(result.liveItems).toHaveLength(0);
        expect(result.liveFiles).toHaveLength(0);
    });

    test('mixed live and deleted files classified separately', () => {
        const result = classifySelection(new Set(['readme.md', 'old.txt']), items);
        expect(result.liveItems).toHaveLength(1);
        expect(result.deletedItems).toHaveLength(1);
        expect(result.liveFiles).toHaveLength(1);
    });

    test('multiple deleted items all appear in deletedItems', () => {
        const result = classifySelection(new Set(['old.txt', 'draft.md']), items);
        expect(result.deletedItems).toHaveLength(2);
        expect(result.liveItems).toHaveLength(0);
    });

    test('mixed live files and live folder', () => {
        const result = classifySelection(new Set(['readme.md', 'data.json', 'images/']), items);
        expect(result.liveItems).toHaveLength(3); // 2 files + 1 folder
        expect(result.liveFiles).toHaveLength(2); // only files
    });

    test('unknown item IDs are silently excluded', () => {
        const result = classifySelection(new Set(['nonexistent.txt', 'also-missing']), items);
        expect(result.allItems).toHaveLength(0);
    });

    test('mix of known and unknown IDs — unknowns excluded', () => {
        const result = classifySelection(new Set(['readme.md', 'ghost.txt']), items);
        expect(result.allItems).toHaveLength(1);
        expect(result.liveItems).toHaveLength(1);
    });

    test('all categories correct for comprehensive selection', () => {
        const ids = new Set(['readme.md', 'images/', 'old.txt', 'mybucket']);
        const result = classifySelection(ids, items);
        expect(result.allItems).toHaveLength(4);
        expect(result.liveItems).toHaveLength(2); // readme.md + images/
        expect(result.deletedItems).toHaveLength(1); // old.txt
        expect(result.liveFiles).toHaveLength(1); // readme.md only
    });

    test('items matched by name when key is absent (bucket-level)', () => {
        const bucketItems = [{ name: 'bucket-a', isBucket: true }];
        const result = classifySelection(new Set(['bucket-a']), bucketItems);
        expect(result.allItems).toHaveLength(1);
        // buckets excluded from liveItems
        expect(result.liveItems).toHaveLength(0);
    });

    test('deletedAt with different truthy values all treated as deleted', () => {
        const mixed = [
            { name: 'a.txt', key: 'a.txt', deletedAt: '2024-01-01T00:00:00Z' },
            { name: 'b.txt', key: 'b.txt', deletedAt: 1704067200000 }, // timestamp
        ];
        const result = classifySelection(new Set(['a.txt', 'b.txt']), mixed);
        expect(result.deletedItems).toHaveLength(2);
        expect(result.liveItems).toHaveLength(0);
    });
});
