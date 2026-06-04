#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const REPO_ROOT = path.resolve(__dirname, '../../..');

const SCAN_PATHS = [
  'src/services/aciCore/decisionPolicy',
];

const BANNED_PATTERNS = [
  // Vehicle/model/brand names must not appear in generic policy code.
  /\bhyundai\b/i,
  /\bkia\b/i,
  /\bmaruti\b/i,
  /\btata\b/i,
  /\bhonda\b/i,
  /\btoyota\b/i,
  /\bmahindra\b/i,
  /\bcreta\b/i,
  /\bseltos\b/i,
  /\bbaleno\b/i,
  /\bnexon\b/i,
  /\bverna\b/i,
  /\bcity zx\b/i,

  // Market/persona judgement must not be embedded in policy code.
  /\burban premium\b/i,
  /\bmarket leader\b/i,
  /\bmust buy\b/i,
  /\bavoid this\b/i,
  /\bbest choice\b/i,
  /\bbest family\b/i,
  /\bfamily buyer\b/i,
  /\bpoor resale\b/i,
  /\bstrong resale\b/i,
  /\bservice network\b/i,
  /\bdirect rival\b/i,
  /\brival pair\b/i,
  /\bpremium feel\b/i,
  /\benthusiast\b/i,
  /\bchauffeur\b/i,
];

const ALLOWED_FILES = new Set([
  // Keep empty intentionally. If a future file needs exceptions, add it with a reason in this script.
]);

function walk(dir) {
  const absDir = path.join(REPO_ROOT, dir);
  if (!fs.existsSync(absDir)) return [];

  const out = [];
  for (const entry of fs.readdirSync(absDir, { withFileTypes: true })) {
    const abs = path.join(absDir, entry.name);
    if (entry.isDirectory()) {
      out.push(...walk(path.relative(REPO_ROOT, abs)));
    } else if (entry.isFile() && /\.(cjs|js|json|md)$/.test(entry.name)) {
      out.push(path.relative(REPO_ROOT, abs));
    }
  }
  return out;
}

const files = SCAN_PATHS.flatMap(walk).filter((file) => !ALLOWED_FILES.has(file));
const violations = [];

for (const file of files) {
  const abs = path.join(REPO_ROOT, file);
  const lines = fs.readFileSync(abs, 'utf8').split(/\r?\n/);

  lines.forEach((line, index) => {
    for (const pattern of BANNED_PATTERNS) {
      if (pattern.test(line)) {
        violations.push({
          file,
          line: index + 1,
          pattern: String(pattern),
          text: line.trim(),
        });
      }
    }
  });
}

const summary = {
  suite: 'ACI Decision Market-Judgement Isolation Audit v1',
  ok: violations.length === 0,
  scannedPaths: SCAN_PATHS,
  scannedFiles: files,
  bannedPatternCount: BANNED_PATTERNS.length,
  violationCount: violations.length,
  violations,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) {
  process.exit(1);
}
