#!/usr/bin/env node

require('dotenv').config();

const assert = require('assert');
const mongoose = require('mongoose');

const QUERY = 'How good is Baleno petrol manual overall?';

const UNSAFE_PATTERNS = [
  /\bmust buy\b/i,
  /\bbuy this\b/i,
  /\bbuy it\b/i,
  /\bgo for this\b/i,
  /\bbest choice\b/i,
  /\bbest pick\b/i,
  /\bclear winner\b/i,
  /\brecommended buy\b/i,
  /\bstrongest value pick\b/i,
  /\bstrongest same-family value pick\b/i,
  /\bavoid this\b/i,
  /\bpoor resale\b/i,
  /\bstrong resale\b/i,
  /\bservice network\b/i,
];

function getMongoUri() {
  return process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
}

function getDb() {
  return mongoose.connection.db;
}

function collectText(value, out = []) {
  if (value == null) return out;
  if (typeof value === 'string') {
    out.push(value);
    return out;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectText(item, out);
    return out;
  }
  if (typeof value === 'object') {
    for (const item of Object.values(value)) collectText(item, out);
  }
  return out;
}

function compact(value, depth = 0) {
  if (value == null) return value;
  if (typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.slice(0, 6).map((item) => compact(item, depth + 1));

  const out = {};
  for (const [key, item] of Object.entries(value).slice(0, 80)) {
    if (depth >= 2 && item && typeof item === 'object') {
      out[key] = Array.isArray(item) ? `[array:${item.length}]` : `[object:${Object.keys(item).length}]`;
    } else {
      out[key] = compact(item, depth + 1);
    }
  }
  return out;
}

function findUnsafeLanguage(output) {
  const textItems = collectText(output);
  const violations = [];

  for (const text of textItems) {
    for (const pattern of UNSAFE_PATTERNS) {
      if (pattern.test(text)) {
        violations.push({
          pattern: String(pattern),
          excerpt: text.slice(0, 240),
        });
      }
    }
  }

  return violations;
}

function extractRows(output = {}) {
  const data = output.data || {};
  const candidates = [
    data.variants,
    output.variants,
    data.rows,
    output.rows,
    data.insights,
    output.insights,
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate)) return candidate;
  }

  if (data.modules || output.modules) return [data.modules ? data : output];

  return [];
}

function hasUsefulScoreData(output = {}) {
  const rows = extractRows(output);
  if (rows.length > 0) return true;

  const data = output.data || {};
  return Boolean(
    data.modules ||
    output.modules ||
    data.scoreProfileKey ||
    output.scoreProfileKey ||
    Number(data.count || output.count || 0) > 0
  );
}

function hasFinalRecommendationEnabled(output = {}) {
  const blob = JSON.stringify(output);
  return /"canUseForFinalRecommendation"\s*:\s*true/.test(blob) ||
    /"finalRecommendationEnabled"\s*:\s*true/.test(blob);
}

function isSuccessOutput(output = {}) {
  return output?.ok === true || output?.status === 'success';
}

async function getRunner() {
  const mod = await import('../../services/aiAgent/tools/newCars/vehicleScoreInsight.tool.js');

  const exportKeys = Object.keys(mod);
  const candidates = [
    ['runVehicleScoreInsightTool', mod.runVehicleScoreInsightTool],
    ['runVehicleScoreProfileTool', mod.runVehicleScoreProfileTool],
    ['default.runVehicleScoreInsightTool', mod.default?.runVehicleScoreInsightTool],
    ['default.runVehicleScoreProfileTool', mod.default?.runVehicleScoreProfileTool],
    ['default', typeof mod.default === 'function' ? mod.default : null],
  ].filter(([, fn]) => typeof fn === 'function');

  if (!candidates.length) {
    throw new Error(`Could not find score insight runner. Export keys: ${exportKeys.join(', ')}`);
  }

  return {
    exportKeys,
    runnerName: candidates[0][0],
    runner: candidates[0][1],
  };
}

async function getBalenoScoreProfileProbe(db) {
  return db.collection('aci_vehicle_variant_score_profile')
    .find(
      {
        modelKey: 'baleno',
        fuelKey: 'petrol',
        transmissionKey: 'manual',
      },
      {
        projection: {
          _id: 0,
          scoreProfileKey: 1,
          variantProfileKey: 1,
          makeKey: 1,
          modelKey: 1,
          variantKey: 1,
          fuelKey: 1,
          transmissionKey: 1,
          variantFullName: 1,
        },
      }
    )
    .limit(8)
    .toArray();
}

async function runAttempt({ runner, db, label, args }) {
  try {
    const output = await runner({ ...args, db });
    return {
      label,
      threw: false,
      ok: isSuccessOutput(output),
      tool: output?.tool || null,
      operation: output?.operation || output?.meta?.operation || output?.data?.operation || null,
      code: output?.code || output?.error?.code || null,
      message: output?.message || output?.error?.message || output?.answer || null,
      answerPreview: String(output?.answer || '').slice(0, 360),
      rowCount: extractRows(output).length,
      hasUsefulScoreData: hasUsefulScoreData(output),
      hasFinalRecommendationEnabled: hasFinalRecommendationEnabled(output),
      unsafeLanguageViolations: findUnsafeLanguage(output),
      outputShape: {
        status: output?.status || null,
        canvasType: output?.canvasType || null,
        inlineType: output?.inlineType || null,
        dataCount: output?.data?.count ?? output?.count ?? null,
        finalRecommendationEnabled: output?.meta?.finalRecommendationEnabled ?? null,
      },
    };
  } catch (error) {
    return {
      label,
      threw: true,
      errorName: error.name,
      errorMessage: error.message,
      stack: String(error.stack || '').split('\\n').slice(0, 8),
    };
  }
}

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  await mongoose.connect(mongoUri);
  const db = getDb();
  const { exportKeys, runnerName, runner } = await getRunner();
  const balenoProfiles = await getBalenoScoreProfileProbe(db);

  const attempts = [];
  const common = {
    userMessage: QUERY,
    context: {},
  };

  attempts.push(await runAttempt({
    runner,
    db,
    label: 'bare_user_message_empty_input',
    args: {
      ...common,
      toolPlan: { tool: 'vehicle_score_insight', input: {} },
    },
  }));

  attempts.push(await runAttempt({
    runner,
    db,
    label: 'input_userMessage',
    args: {
      ...common,
      toolPlan: { tool: 'vehicle_score_insight', input: { userMessage: QUERY } },
    },
  }));

  attempts.push(await runAttempt({
    runner,
    db,
    label: 'input_query',
    args: {
      ...common,
      toolPlan: { tool: 'vehicle_score_insight', input: { query: QUERY } },
    },
  }));

  attempts.push(await runAttempt({
    runner,
    db,
    label: 'explicit_model_operation_from_query',
    args: {
      ...common,
      toolPlan: {
        tool: 'vehicle_score_insight',
        input: {
          operation: 'model_score_insights',
          userMessage: QUERY,
        },
      },
    },
  }));

  if (balenoProfiles[0]) {
    attempts.push(await runAttempt({
      runner,
      db,
      label: 'direct_scoreProfileKey_from_db_probe',
      args: {
        ...common,
        scoreProfileKey: balenoProfiles[0].scoreProfileKey || balenoProfiles[0].variantProfileKey,
        toolPlan: {
          tool: 'vehicle_score_insight',
          input: {
            scoreProfileKey: balenoProfiles[0].scoreProfileKey || balenoProfiles[0].variantProfileKey,
          },
        },
      },
    }));
  }

  const successful = attempts.find((attempt) =>
    attempt.ok &&
    attempt.hasUsefulScoreData &&
    !attempt.hasFinalRecommendationEnabled &&
    attempt.unsafeLanguageViolations?.length === 0
  );

  const summary = {
    suite: 'ACI Score Insight User Query Smoke v1',
    ok: Boolean(successful),
    query: QUERY,
    runnerName,
    exportKeys,
    dbProbe: {
      balenoPetrolManualProfileCount: balenoProfiles.length,
      sampleProfiles: balenoProfiles.slice(0, 5),
    },
    successfulAttemptLabel: successful?.label || null,
    attempts,
  };

  console.log(JSON.stringify(summary, null, 2));

  if (!summary.ok) {
    process.exit(1);
  }
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
