#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const SCORE_TOOL_PATH = path.join(
  process.cwd(),
  'src/services/aiAgent/tools/newCars/vehicleScoreInsight.tool.js'
);

const HARD_BANNED_PATTERNS = [
  /\bmust buy\b/i,
  /\bbuy this\b/i,
  /\bbuy it\b/i,
  /\bgo for this\b/i,
  /\bbest choice\b/i,
  /\bbest pick\b/i,
  /\bclear winner\b/i,
  /\bwinner\b/i,
  /\brecommended buy\b/i,
  /\bstrongest value pick\b/i,
  /\bstrongest same-family value pick\b/i,
  /\bavoid this\b/i,
  /\bpoor resale\b/i,
  /\bstrong resale\b/i,
  /\bservice network\b/i,
];

const SAFE_CONTEXT_PATTERNS = [
  /\bnot a purchase verdict\b/i,
  /\bnot a final recommendation\b/i,
  /\bdiagnostic module scores only\b/i,
  /\bfinal recommendation needs\b/i,
  /\bfinal buyer recommendations\b/i,
  /\bcanUseForFinalRecommendation\b/i,
];

function getLineNumber(text, index) {
  return text.slice(0, index).split('\n').length;
}

function hasSafeContext(excerpt) {
  return SAFE_CONTEXT_PATTERNS.some((pattern) => pattern.test(excerpt));
}

function main() {
  if (!fs.existsSync(SCORE_TOOL_PATH)) {
    throw new Error(`Missing file: ${SCORE_TOOL_PATH}`);
  }

  const text = fs.readFileSync(SCORE_TOOL_PATH, 'utf8');
  const violations = [];

  for (const pattern of HARD_BANNED_PATTERNS) {
    pattern.lastIndex = 0;
    const regex = new RegExp(pattern.source, `${pattern.flags.includes('i') ? 'i' : ''}g`);
    let match;

    while ((match = regex.exec(text))) {
      const start = Math.max(0, match.index - 160);
      const end = Math.min(text.length, match.index + 220);
      const excerpt = text.slice(start, end).replace(/\s+/g, ' ').trim();

      if (hasSafeContext(excerpt)) continue;

      violations.push({
        file: path.relative(process.cwd(), SCORE_TOOL_PATH),
        line: getLineNumber(text, match.index),
        pattern: String(pattern),
        match: match[0],
        excerpt,
      });
    }
  }

  const summary = {
    suite: 'ACI Score Insight Tool Language Audit v1',
    ok: violations.length === 0,
    scannedFile: path.relative(process.cwd(), SCORE_TOOL_PATH),
    bannedPatternCount: HARD_BANNED_PATTERNS.length,
    violationCount: violations.length,
    violations,
  };

  console.log(JSON.stringify(summary, null, 2));

  if (!summary.ok) {
    process.exit(1);
  }
}

main();
