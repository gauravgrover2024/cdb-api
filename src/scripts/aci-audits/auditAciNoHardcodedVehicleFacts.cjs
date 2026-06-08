#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const ROOT = process.cwd();

const SCAN_ROOTS = [
  "src/services/aciCore",
  "src/services/aiAgent",
];

const EXCLUDED_PARTS = [
  `${path.sep}_deprecated_v1${path.sep}`,
  `${path.sep}_legacy_backup_before_v2${path.sep}`,
  `${path.sep}node_modules${path.sep}`,
];

const BANNED_PATTERNS = [
  {
    id: "explicit_scorpio_n_runtime_branch",
    description: "Runtime must not use explicitScorpioN or Scorpio-N-specific routing branches.",
    regex: /\bexplicitScorpioN\b/g,
  },
  {
    id: "scorpion_to_scorpio_n_hard_alias",
    description: "Ambiguous typo 'scorpion' must not be hard-mapped to Scorpio N.",
    regex: /compact\s*={2,3}\s*["']scorpion["'][\s\S]{0,180}Scorpio\s*N/gi,
  },
  {
    id: "creta_king_runtime_repair",
    description: "Runtime must not special-case Creta King routing.",
    regex: /creta\\s\+king[\s\S]{0,360}model:\s*["']Creta["'][\s\S]{0,180}variant:\s*["']King["']/gi,
  },
  {
    id: "verna_sx_runtime_repair",
    description: "Runtime must not special-case Verna SX routing.",
    regex: /verna\\s\+sx[\s\S]{0,360}model:\s*["']Verna["'][\s\S]{0,180}variant:\s*["']SX["']/gi,
  },
  {
    id: "feature_explorer_model_whitelist",
    description: "Feature explorer routing must not use a hardcoded model whitelist.",
    regex: /modelOnlyExplorer[\s\S]{0,900}\(creta\|verna\|seltos/gi,
  },
  {
    id: "hardcoded_score_model_candidate",
    description: "Score routing must not synthesize a hardcoded model candidate object.",
    fileIncludes: ["src/services/aciCore/integration/aciCoreLiveBridge.service.js"],
    regex: /modelCandidate[\s\S]{0,260}\?[\s\S]{0,520}model:\s*["'][A-Za-z0-9 -]+["'][\s\S]{0,360}confidence:\s*1/gi,
  },
];

const WATCHLIST_PATTERNS = [
  {
    id: "runtime_model_literal",
    description: "Model/variant literal found in runtime. Review if it is not parser/example-only.",
    regex: /\b(model|variant):\s*["'](?:Creta|Seltos|Scorpio N|Scorpio|Verna|Baleno|Venue|i20|SX|HTX|HTE|Alpha|Sportz|King|ZX)["']/g,
    maxExamples: 30,
  },
];

function walk(dir, files = []) {
  if (!fs.existsSync(dir)) return files;

  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    const normalized = full.split(path.sep).join(path.sep);

    if (EXCLUDED_PARTS.some((part) => normalized.includes(part))) continue;

    if (entry.isDirectory()) {
      walk(full, files);
    } else if (/\.(js|cjs|mjs|ts)$/.test(entry.name)) {
      files.push(full);
    }
  }

  return files;
}

function lineForIndex(text, index) {
  return text.slice(0, index).split(/\r?\n/).length;
}

function snippetForIndex(text, index) {
  const start = Math.max(0, index - 120);
  const end = Math.min(text.length, index + 220);
  return text
    .slice(start, end)
    .replace(/\s+/g, " ")
    .trim();
}

function shouldScanPattern(file, pattern) {
  if (!pattern.fileIncludes) return true;
  const rel = path.relative(ROOT, file).split(path.sep).join("/");
  return pattern.fileIncludes.some((needle) => rel.includes(needle));
}

function run() {
  const scanFiles = SCAN_ROOTS.flatMap((root) => walk(path.join(ROOT, root)));

  const bannedFindings = [];
  const watchFindings = [];

  for (const file of scanFiles) {
    const rel = path.relative(ROOT, file).split(path.sep).join("/");
    const text = fs.readFileSync(file, "utf8");

    for (const pattern of BANNED_PATTERNS) {
      if (!shouldScanPattern(file, pattern)) continue;

      pattern.regex.lastIndex = 0;
      let match;
      while ((match = pattern.regex.exec(text))) {
        bannedFindings.push({
          id: pattern.id,
          description: pattern.description,
          file: rel,
          line: lineForIndex(text, match.index),
          snippet: snippetForIndex(text, match.index),
        });

        if (match.index === pattern.regex.lastIndex) pattern.regex.lastIndex += 1;
      }
    }

    for (const pattern of WATCHLIST_PATTERNS) {
      pattern.regex.lastIndex = 0;
      let count = 0;
      let match;
      while ((match = pattern.regex.exec(text))) {
        count += 1;
        if (count <= pattern.maxExamples) {
          watchFindings.push({
            id: pattern.id,
            description: pattern.description,
            file: rel,
            line: lineForIndex(text, match.index),
            snippet: snippetForIndex(text, match.index),
          });
        }

        if (match.index === pattern.regex.lastIndex) pattern.regex.lastIndex += 1;
      }
    }
  }

  const result = {
    suite: "ACI No-Hardcoded Vehicle Facts Audit",
    ok: bannedFindings.length === 0,
    total: 1,
    passed: bannedFindings.length === 0 ? 1 : 0,
    failed: bannedFindings.length === 0 ? 0 : 1,
    failedIds: [...new Set(bannedFindings.map((finding) => finding.id))],
    scannedFiles: scanFiles.length,
    bannedFindingCount: bannedFindings.length,
    watchFindingCount: watchFindings.length,
    bannedFindings,
    watchFindings,
    policy: {
      banned: [
        "No model-specific runtime branches for factual routing.",
        "No hardcoded car/variant candidate object to force a buyer answer.",
        "No hardcoded vehicle typo alias that changes ambiguous meaning into a factual entity.",
        "No hardcoded model whitelist for feature routing.",
      ],
      allowed: [
        "Parser keywords such as sunroof, airbags, mileage, safety.",
        "Generic variant-token formatting such as SX/HTX/ZX.",
        "Docs, deprecated backups, and test fixtures outside scanned runtime paths.",
        "DB-backed aliases and candidate retrieval.",
      ],
    },
  };

  console.log(JSON.stringify(result, null, 2));

  if (!result.ok) {
    process.exit(1);
  }
}

run();
