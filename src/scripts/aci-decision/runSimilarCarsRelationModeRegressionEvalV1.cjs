#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const {
  DECISION_MODULES,
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
  ALLOWED_ANSWER_TYPES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const {
  applyDecisionPolicyWithModuleProfile,
} = require('../../services/aciCore/decisionPolicy/aciDecisionModulePolicyProfiles.service.cjs');

const GRAPH_COLLECTION = process.env.ACI_SIMILAR_MODEL_GRAPH_COLLECTION || 'aci_vehicle_similar_model_graph_v1';
const GRAPH_VERSION = 'similar_model_graph_v1';

const DEFAULT_RELATIONS = new Set([
  'direct_rival',
  'platform_twin',
  'nearby_alternative',
  'adjacent_crossover',
]);

const MODE_SPECIFIC_RELATIONS = new Set([
  'cheaper_step_down',
  'premium_step_up',
  'powertrain_shift',
]);

const CHEAPER_ALLOWED_RELATIONS = new Set([
  'cheaper_step_down',
  'direct_rival',
  'platform_twin',
  'nearby_alternative',
  'adjacent_crossover',
]);

function getMongoUri() {
  return process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
}

function getDb() {
  return mongoose.connection.db;
}

function asArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function getRowsFromSimilarOutput(output = {}) {
  return output.similarModels || output.data?.similarModels || output.rows || [];
}

function relationTypes(rows = []) {
  return [...new Set(asArray(rows).map((row) => row.relationType || 'unknown'))].sort();
}

function hasDefaultRelations(rawRows = []) {
  return asArray(rawRows).some((row) => DEFAULT_RELATIONS.has(row.relationType));
}

function buildPolicyWrappedOutput({ rows, mode, traceWarnings = [] }) {
  return applyDecisionPolicyWithModuleProfile({
    module: DECISION_MODULES.SIMILAR_CARS,
    intent: `similar_cars_relation_mode_regression_${mode}`,
    requestedFinalRecommendation: true,
    buyerContext: {
      city: 'city_supported',
      budget: 2000000,
      primaryUseCase: 'primary_use_case_generic',
      familySize: 4,
      fuelPreference: 'fuel_preference_generic',
      transmissionPreference: 'transmission_preference_generic',
      safetyPriority: 'high',
      featurePriority: ['feature_a'],
      shortlistedModels: ['model_a', 'model_b'],
    },
    rows,
    diagnostics: [],
    evidence: {
      evidenceStatus: rows.length > 0 ? EVIDENCE_STATUS.COMPLETE : EVIDENCE_STATUS.MISSING,
      confidence: rows.length > 0 ? CONFIDENCE_LEVELS.HIGH : CONFIDENCE_LEVELS.LOW,
      sourceTransparency: ['similar_relation_mode_regression'],
      missingData: rows.length > 0 ? [] : ['similar_models'],
      usableEvidenceCount: rows.length,
      requiredEvidenceCount: 1,
    },
    provenance: {
      buildVersion: GRAPH_VERSION,
      builtAt: new Date().toISOString(),
      sourceClass: SOURCE_CLASSES.SIMILAR_GRAPH_INFERENCE,
      stalenessDays: 0,
      needsRebuild: false,
    },
    trace: {
      toolRoute: 'similar_relation_mode_regression',
      collectionsUsed: [GRAPH_COLLECTION],
      matchedRows: rows.length,
      candidateCount: rows.length,
      warnings: traceWarnings,
    },
  });
}

async function getCandidateGraphs(db, limit = 80) {
  return db.collection(GRAPH_COLLECTION)
    .find(
      {
        graphVersion: GRAPH_VERSION,
        'anchor.modelKey': { $exists: true, $type: 'string', $ne: '' },
        similarModels: { $exists: true, $type: 'array', $ne: [] },
      },
      {
        projection: {
          _id: 0,
          graphVersion: 1,
          anchor: 1,
          similarModels: 1,
        },
      }
    )
    .sort({ updatedAt: -1, 'anchor.modelKey': 1 })
    .limit(limit)
    .toArray();
}

async function runToolForMode({ runVehicleSimilarTool, db, modelKey, mode }) {
  const messageByMode = {
    default: 'Show similar cars',
    cheaper: 'Show cheaper alternatives',
    premium: 'Show premium alternatives',
    ev: 'Show EV alternatives',
  };

  const output = await runVehicleSimilarTool({
    userMessage: messageByMode[mode],
    toolPlan: {
      tool: 'vehicle_similar',
      input: {
        modelKey,
      },
    },
    context: {},
    db,
  });

  const rows = getRowsFromSimilarOutput(output);

  return {
    mode,
    output,
    rows,
    relationTypes: relationTypes(rows),
    count: rows.length,
    answer: String(output.answer || ''),
    guardrailFinal: output.usageGuardrail?.canUseForFinalRecommendation,
    requestedRelation: output.meta?.requestedRelation,
  };
}

function addFailure(failures, id, details = {}) {
  failures.push({ id, ...details });
}

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) {
    throw new Error('Missing Mongo URI.');
  }

  await mongoose.connect(mongoUri);
  const db = getDb();
  const { runVehicleSimilarTool } = await import('../../services/aiAgent/tools/newCars/vehicleSimilar.tool.js');

  const graphs = await getCandidateGraphs(db);
  const failures = [];
  const inspected = [];

  for (const graph of graphs) {
    const modelKey = graph.anchor?.modelKey;
    const rawRows = asArray(graph.similarModels);
    if (!modelKey || rawRows.length === 0) continue;

    const modeResults = {
      default: await runToolForMode({ runVehicleSimilarTool, db, modelKey, mode: 'default' }),
      cheaper: await runToolForMode({ runVehicleSimilarTool, db, modelKey, mode: 'cheaper' }),
      premium: await runToolForMode({ runVehicleSimilarTool, db, modelKey, mode: 'premium' }),
      ev: await runToolForMode({ runVehicleSimilarTool, db, modelKey, mode: 'ev' }),
    };

    for (const [mode, result] of Object.entries(modeResults)) {
      if (result.guardrailFinal !== false) {
        addFailure(failures, 'guardrail_allows_final_recommendation', {
          mode,
          relationTypes: result.relationTypes,
        });
      }

      const wrapped = buildPolicyWrappedOutput({ rows: result.rows, mode });
      const allowedPolicyAnswerTypes = new Set([
        ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
        ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED,
      ]);

      if (
        wrapped.decisionPolicy?.canUseForFinalRecommendation !== false ||
        !allowedPolicyAnswerTypes.has(wrapped.decisionPolicy?.allowedAnswerType)
      ) {
        addFailure(failures, 'policy_wrapper_allows_final_recommendation', {
          mode,
          relationTypes: result.relationTypes,
          policy: wrapped.decisionPolicy,
        });
      }
    }

    const defaultBadRelations = modeResults.default.rows
      .map((row) => row.relationType)
      .filter((relationType) => MODE_SPECIFIC_RELATIONS.has(relationType));

    if (defaultBadRelations.length > 0) {
      addFailure(failures, 'default_mode_contains_mode_specific_relations', {
        relationTypes: relationTypes(modeResults.default.rows),
      });
    }

    if (!hasDefaultRelations(rawRows) && modeResults.default.count === 0) {
      const answer = modeResults.default.answer;
      if (/not enough similar-car graph data/i.test(answer)) {
        addFailure(failures, 'empty_default_claims_data_missing_when_mode_specific_graph_exists', {
          answerPreview: answer.slice(0, 220),
        });
      }
      if (!/cheaper|premium|ev|powertrain/i.test(answer)) {
        addFailure(failures, 'empty_default_missing_recovery_modes', {
          answerPreview: answer.slice(0, 220),
        });
      }
    }

    const premiumBadRelations = modeResults.premium.rows
      .map((row) => row.relationType)
      .filter((relationType) => relationType !== 'premium_step_up');

    if (premiumBadRelations.length > 0) {
      addFailure(failures, 'premium_mode_contains_non_premium_relations', {
        relationTypes: relationTypes(modeResults.premium.rows),
      });
    }

    const evBadRelations = modeResults.ev.rows
      .map((row) => row.relationType)
      .filter((relationType) => relationType !== 'powertrain_shift');

    if (evBadRelations.length > 0) {
      addFailure(failures, 'ev_mode_contains_non_powertrain_relations', {
        relationTypes: relationTypes(modeResults.ev.rows),
      });
    }

    const cheaperBadRelations = modeResults.cheaper.rows
      .map((row) => row.relationType)
      .filter((relationType) => !CHEAPER_ALLOWED_RELATIONS.has(relationType));

    if (cheaperBadRelations.length > 0) {
      addFailure(failures, 'cheaper_mode_contains_unexpected_relations', {
        relationTypes: relationTypes(modeResults.cheaper.rows),
      });
    }

    inspected.push({
      hasAnchorModelKey: Boolean(modelKey),
      rawRowCount: rawRows.length,
      rawRelationTypes: relationTypes(rawRows),
      modeCounts: {
        default: modeResults.default.count,
        cheaper: modeResults.cheaper.count,
        premium: modeResults.premium.count,
        ev: modeResults.ev.count,
      },
      modeRelationTypes: {
        default: modeResults.default.relationTypes,
        cheaper: modeResults.cheaper.relationTypes,
        premium: modeResults.premium.relationTypes,
        ev: modeResults.ev.relationTypes,
      },
    });
  }

  const summary = {
    suite: 'ACI Similar Cars Relation Mode Regression Eval v1',
    ok: failures.length === 0,
    graphCollection: GRAPH_COLLECTION,
    graphVersion: GRAPH_VERSION,
    inspectedGraphCount: inspected.length,
    failed: failures.length,
    failureIds: [...new Set(failures.map((failure) => failure.id))],
    failures: failures.slice(0, 30),
    aggregate: {
      defaultRowsTotal: inspected.reduce((sum, item) => sum + item.modeCounts.default, 0),
      cheaperRowsTotal: inspected.reduce((sum, item) => sum + item.modeCounts.cheaper, 0),
      premiumRowsTotal: inspected.reduce((sum, item) => sum + item.modeCounts.premium, 0),
      evRowsTotal: inspected.reduce((sum, item) => sum + item.modeCounts.ev, 0),
    },
    inspectedSample: inspected.slice(0, 8),
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
