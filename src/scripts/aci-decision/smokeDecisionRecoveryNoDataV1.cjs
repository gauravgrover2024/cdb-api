#!/usr/bin/env node

const fs = require('fs');

const checks = [];

function read(file) {
  return fs.readFileSync(file, 'utf8');
}

function addCheck(id, ok, details = {}) {
  checks.push({ id, ok: Boolean(ok), ...details });
}

const parser = read('src/services/aciCore/understanding/deterministicMeaningFrame.parser.js');
const understanding = read('src/services/aciCore/understanding/aciUnderstandingEngine.js');
const liveBridge = read('src/services/aciCore/integration/aciCoreLiveBridge.service.js');
const responseTools = read('src/services/aiAgent/aiAgent.responseTools.js');
const scoreTool = read('src/services/aiAgent/tools/newCars/vehicleScoreInsight.tool.js');
const similarTool = read('src/services/aiAgent/tools/newCars/vehicleSimilar.tool.js');

addCheck(
  'generic-car-topic-fallback-removed-parser',
  !parser.includes('What would you like to check about the car?')
);

addCheck(
  'generic-car-topic-fallback-removed-understanding',
  !understanding.includes('What would you like to check about the car?')
);

addCheck(
  'topic-fallback-has-next-actions',
  parser.includes('price, features, on-road price, EMI') &&
    understanding.includes('price, features, on-road price, EMI')
);

addCheck(
  'exact-variant-live-bridge-uses-recovery-language',
  liveBridge.includes('decision_exact_variant_unavailable_recovery') ||
    liveBridge.includes('listed variants, model-level price, and features')
);

addCheck(
  'exact-variant-response-tools-has-safe-recovery',
  responseTools.includes('could not match ${variantResolution.requestedVariant || variant} to a current variant I can verify') &&
    responseTools.includes('I should not calculate EMI from the wrong price')
);

addCheck(
  'score-no-data-has-next-actions',
  scoreTool.includes('I can still help with its available variants') &&
    scoreTool.includes('I can still help with its price, features, variants, similar cars')
);

addCheck(
  'similar-no-result-has-understood-anchor-and-next-actions',
  similarTool.includes('I understood ${anchor.displayName || "this model"}') &&
    similarTool.includes('cheaper step-downs, premium step-ups, or EV/powertrain alternatives')
);

const failed = checks.filter((check) => !check.ok);
const summary = {
  suite: 'ACI Decision Recovery / No-Data Smoke v1',
  ok: failed.length === 0,
  total: checks.length,
  passed: checks.length - failed.length,
  failed: failed.length,
  failedIds: failed.map((check) => check.id),
  checks,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) {
  process.exit(1);
}
