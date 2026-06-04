#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const BACKEND_ROOT = path.resolve(__dirname, '../../..');
const FRONTEND_ROOT = process.env.CDB_FRONTEND_ROOT || '/Users/gauravgrover/cdb-frontend';
const OUT_DIR = path.join(BACKEND_ROOT, 'docs/aci-assist');
const OUT_MD = path.join(OUT_DIR, 'ACI_CANVAS_INLINE_CONTRACT_INVENTORY_V1.md');
const OUT_JSON = path.join(OUT_DIR, 'aci_canvas_inline_contract_inventory_v1.json');

const SCAN_ROOTS = [
  path.join(BACKEND_ROOT, 'src'),
  path.join(BACKEND_ROOT, 'scripts'),
].filter((dir) => fs.existsSync(dir));

const EXTENSIONS = new Set(['.js', '.jsx', '.cjs', '.mjs', '.ts', '.tsx']);
const SKIP_DIRS = new Set(['node_modules', '.git', 'coverage', 'dist', 'build', '.next']);
const CONTRACT_KEYS = [
  'canvasType',
  'inlineType',
  'displayMode',
  'intent',
  'tool',
  'preferredWidgetType',
];

const normalizeType = (value = '') =>
  String(value || '')
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[\s-]+/g, '_')
    .replace(/_{2,}/g, '_')
    .toLowerCase();

const walkFiles = (dir, files = []) => {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (!SKIP_DIRS.has(entry.name)) walkFiles(path.join(dir, entry.name), files);
      continue;
    }

    const ext = path.extname(entry.name);
    if (EXTENSIONS.has(ext)) files.push(path.join(dir, entry.name));
  }
  return files;
};

const readIfExists = (filePath) => {
  try {
    return fs.readFileSync(filePath, 'utf8');
  } catch (_) {
    return '';
  }
};

const extractObjectKeys = (source = '', objectName = '') => {
  const start = source.indexOf(objectName);
  if (start < 0) return [];
  const open = source.indexOf('{', start);
  if (open < 0) return [];

  let depth = 0;
  let end = open;
  for (; end < source.length; end += 1) {
    if (source[end] === '{') depth += 1;
    if (source[end] === '}') depth -= 1;
    if (depth === 0) break;
  }

  const body = source.slice(open + 1, end);
  const keys = new Set();
  const keyPattern = /(?:^|[\n,])\s*([A-Za-z0-9_]+)\s*:/g;
  let match;
  while ((match = keyPattern.exec(body))) keys.add(normalizeType(match[1]));
  return [...keys];
};

const frontendSupport = () => {
  const registry = readIfExists(
    path.join(FRONTEND_ROOT, 'src/components/aci-assist-v2/canvas/aciV2CanvasRegistry.js'),
  );
  const inlineRenderer = readIfExists(
    path.join(FRONTEND_ROOT, 'src/components/aci-assist-v2/chat/AciV2InlineRenderer.jsx'),
  );
  return {
    canvas: new Set(extractObjectKeys(registry, 'ACI_V2_CANVAS_TYPE_TO_SCREEN')),
    inline: new Set(extractObjectKeys(inlineRenderer, 'INLINE_RENDERERS')),
  };
};

const classify = ({ key = '', value = '' } = {}) => {
  const type = normalizeType(value);
  if (key === 'displayMode') return 'unknown';
  if (type === 'canvas' || type === 'inline') return 'unknown';
  if (/canvas$|_canvas$/.test(type)) return 'canvas';
  if (/inline|summary|card|notice/.test(type)) return 'inline';
  if (/widget/.test(type) || /widget/i.test(key) || key === 'type') return 'widget';
  return 'unknown';
};

const likelyOwner = (relativePath = '') => {
  const toolMatch = relativePath.match(/tools\/(?:newCars\/)?([^/]+)\.tool\./);
  if (toolMatch) return toolMatch[1];
  if (/aiAgent\.responseTools/.test(relativePath)) return 'aiAgent.responseTools';
  if (/aciCoreToLegacyPlan/.test(relativePath)) return 'aciCoreToLegacyPlan.adapter';
  if (/aciCoreLiveBridge/.test(relativePath)) return 'aciCoreLiveBridge';
  if (/smoke|eval|audit/i.test(relativePath)) return 'smoke/eval/audit';
  return relativePath.split('/').slice(0, 3).join('/');
};

const isSupported = ({ kind, type, support }) => {
  if (kind === 'canvas') return support.canvas.has(type);
  if (kind === 'inline') return support.inline.has(type);
  return false;
};

const contractPattern = new RegExp(
  `\\b(${CONTRACT_KEYS.join('|')})\\b\\s*[:=]\\s*["'\`]([^"'\`]+)["'\`]`,
  'g',
);
const widgetTypePattern = /\bwidget\s*\.\s*type\s*[:=]\s*["'`]([^"'`]+)["'`]/g;

const collectInventory = () => {
  const support = frontendSupport();
  const records = new Map();

  for (const root of SCAN_ROOTS) {
    for (const filePath of walkFiles(root)) {
      const source = readIfExists(filePath);
      if (!source) continue;

      const relativePath = path.relative(BACKEND_ROOT, filePath);
      let match;
      while ((match = contractPattern.exec(source))) {
        const key = match[1];
        const rawValue = match[2];
        const type = normalizeType(rawValue);
        if (!type || type.length > 80) continue;
        if (/[${}]/.test(rawValue)) continue;

        const kind = classify({ key, value: rawValue });
        if (kind === 'unknown' && !/_canvas$|_summary$|_card$|_notice$|widget/i.test(type)) {
          continue;
        }

        const id = `${kind}:${type}`;
        const current = records.get(id) || {
          type,
          kind,
          backendFilePaths: new Set(),
          likelyOwningModules: new Set(),
          appearsInPublicOrLiveBridgeSmoke: false,
          frontendSupported: false,
          status: 'unknown',
          keys: new Set(),
        };

        current.backendFilePaths.add(relativePath);
        current.likelyOwningModules.add(likelyOwner(relativePath));
        current.appearsInPublicOrLiveBridgeSmoke =
          current.appearsInPublicOrLiveBridgeSmoke ||
          /smoke|eval/i.test(relativePath);
        current.frontendSupported = current.frontendSupported || isSupported({ kind, type, support });
        current.status = current.frontendSupported ? 'supported' : kind === 'unknown' ? 'unknown' : 'missing';
        current.keys.add(key);
        records.set(id, current);
      }

      while ((match = widgetTypePattern.exec(source))) {
        const key = 'widget.type';
        const rawValue = match[1];
        const type = normalizeType(rawValue);
        if (!type || type.length > 80) continue;

        const id = `widget:${type}`;
        const current = records.get(id) || {
          type,
          kind: 'widget',
          backendFilePaths: new Set(),
          likelyOwningModules: new Set(),
          appearsInPublicOrLiveBridgeSmoke: false,
          frontendSupported: false,
          status: 'unknown',
          keys: new Set(),
        };

        current.backendFilePaths.add(relativePath);
        current.likelyOwningModules.add(likelyOwner(relativePath));
        current.appearsInPublicOrLiveBridgeSmoke =
          current.appearsInPublicOrLiveBridgeSmoke ||
          /smoke|eval/i.test(relativePath);
        current.keys.add(key);
        records.set(id, current);
      }
    }
  }

  return [...records.values()]
    .map((record) => ({
      ...record,
      backendFilePaths: [...record.backendFilePaths].sort(),
      likelyOwningModules: [...record.likelyOwningModules].sort(),
      keys: [...record.keys].sort(),
    }))
    .sort((a, b) => a.kind.localeCompare(b.kind) || a.type.localeCompare(b.type));
};

const toMarkdown = (inventory = []) => {
  const byKind = (kind) => inventory.filter((item) => item.kind === kind);
  const rows = inventory
    .map((item) => [
      item.type,
      item.kind,
      item.status,
      item.frontendSupported ? 'yes' : 'no',
      item.appearsInPublicOrLiveBridgeSmoke ? 'yes' : 'no',
      item.likelyOwningModules.join(', '),
      item.backendFilePaths.slice(0, 6).join('<br>'),
    ]);

  const table = [
    '| Type | Kind | Status | Frontend support | Smoke/eval seen | Likely owner | Backend paths |',
    '| --- | --- | --- | --- | --- | --- | --- |',
    ...rows.map((cols) => `| ${cols.map((col) => String(col || '').replace(/\|/g, '/')).join(' | ')} |`),
  ].join('\n');

  const supported = inventory.filter((item) => item.status === 'supported').length;
  const missing = inventory.filter((item) => item.status === 'missing').length;
  const unknown = inventory.filter((item) => item.status === 'unknown').length;

  return [
    '# ACI Canvas / Inline Contract Inventory V1',
    '',
    `Generated by \`src/scripts/aci-audits/auditAciCanvasInlineContractsV1.cjs\`.`,
    '',
    '## Summary',
    '',
    `- Total contract values: ${inventory.length}`,
    `- Canvas types: ${byKind('canvas').length}`,
    `- Inline types: ${byKind('inline').length}`,
    `- Widget/other types: ${byKind('widget').length}`,
    `- Supported on frontend: ${supported}`,
    `- Missing on frontend: ${missing}`,
    `- Unknown status: ${unknown}`,
    '',
    '## Inventory',
    '',
    table,
    '',
  ].join('\n');
};

const inventory = collectInventory();
fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(OUT_JSON, JSON.stringify({ generatedAt: new Date().toISOString(), inventory }, null, 2));
fs.writeFileSync(OUT_MD, toMarkdown(inventory));

console.log(JSON.stringify({
  status: 'ok',
  count: inventory.length,
  supported: inventory.filter((item) => item.status === 'supported').length,
  missing: inventory.filter((item) => item.status === 'missing').map((item) => item.type),
  output: {
    markdown: path.relative(BACKEND_ROOT, OUT_MD),
    json: path.relative(BACKEND_ROOT, OUT_JSON),
  },
}, null, 2));
