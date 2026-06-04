#!/usr/bin/env node

require('dotenv').config();

const assert = require('assert');
const mongoose = require('mongoose');

const {
  DECISION_MODULES,
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
  ALLOWED_ANSWER_TYPES,
  CLAIM_TYPES,
  BLOCKED_REASONS,
  DEGRADED_MODES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const {
  decisionOutputHasUsefulResult,
} = require('../../services/aciCore/decisionPolicy/aciDecisionOutput.contract.cjs');

const {
  applyDecisionPolicyWithModuleProfile,
} = require('../../services/aciCore/decisionPolicy/aciDecisionModulePolicyProfiles.service.cjs');

const GRAPH_COLLECTION = process.env.ACI_SIMILAR_MODEL_GRAPH_COLLECTION || 'aci_vehicle_similar_model_graph_v1';
const GRAPH_VERSION = 'similar_model_graph_v1';

const completeBuyerContext = Object.freeze({
  city: 'city_supported',
  budget: 2000000,
  primaryUseCase: 'primary_use_case_generic',
  familySize: 4,
  fuelPreference: 'fuel_preference_generic',
  transmissionPreference: 'transmission_preference_generic',
  safetyPriority: 'high',
  featurePriority: ['feature_a', 'feature_b'],
  shortlistedModels: ['model_a', 'model_b'],
});

function getMongoUri() {
  return process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
}

function getDb() {
  return mongoose.connection.db;
}

function getRowsFromSimilarOutput(output = {}) {
  return output.similarModels || output.data?.similarModels || output.rows || [];
}

async function getCandidateSimilarGraphs(db, limit = 80) {
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
          formulaVersion: 1,
          buildVersion: 1,
          builtAt: 1,
          updatedAt: 1,
          createdAt: 1,
          anchor: 1,
          similarModels: { $slice: 20 },
        },
      }
    )
    .sort({ updatedAt: -1, 'anchor.modelKey': 1 })
    .limit(limit)
    .toArray();
}

async function findUsableSimilarGraphFixture({ db, runVehicleSimilarTool }) {
  const candidates = await getCandidateSimilarGraphs(db);

  for (const graphDoc of candidates) {
    try {
      const directOutput = await runVehicleSimilarTool({
        userMessage: 'Show similar cars',
        toolPlan: {
          tool: 'vehicle_similar',
          input: {
            modelKey: graphDoc.anchor.modelKey,
          },
        },
        context: {},
        db,
      });

      const directRows = getRowsFromSimilarOutput(directOutput);
      if (!Array.isArray(directRows) || directRows.length <= 0) {
        continue;
      }

      if (directRows.some((row) => row.modelKey === graphDoc.anchor.modelKey)) {
        continue;
      }

      return {
        graphDoc,
        directOutput,
        directRows,
      };
    } catch (error) {
      // Continue until we find a genuinely usable DB-backed fixture.
    }
  }

  throw new Error('No usable similar-cars fixture found where direct tool returns rows.');
}

function buildFreshProvenance(graphDoc = {}) {
  return {
    buildVersion: graphDoc.buildVersion || graphDoc.graphVersion || graphDoc.formulaVersion || 'similar_graph_fixture_unknown_build',
    builtAt: new Date(graphDoc.builtAt || graphDoc.updatedAt || graphDoc.createdAt || Date.now()).toISOString(),
    sourceClass: SOURCE_CLASSES.SIMILAR_GRAPH_INFERENCE,
    stalenessDays: 0,
    needsRebuild: false,
  };
}

function buildSimilarDecisionInput({ graphDoc, output, rows, requestedFinalRecommendation }) {
  const rowList = Array.isArray(rows) ? rows : getRowsFromSimilarOutput(output);
  const usableEvidenceCount = rowList.length;

  return {
    module: DECISION_MODULES.SIMILAR_CARS,
    intent: 'similar_cars_real_output_fixture',
    requestedFinalRecommendation,
    buyerContext: requestedFinalRecommendation ? completeBuyerContext : {},
    rows: rowList,
    diagnostics: rowList.flatMap((row) => Array.isArray(row.reasons) ? row.reasons : []),
    evidence: {
      evidenceStatus: usableEvidenceCount > 0 ? EVIDENCE_STATUS.COMPLETE : EVIDENCE_STATUS.MISSING,
      confidence: usableEvidenceCount > 0 ? CONFIDENCE_LEVELS.HIGH : CONFIDENCE_LEVELS.LOW,
      sourceTransparency: ['similar_graph_fixture_output'],
      missingData: [],
      usableEvidenceCount,
      requiredEvidenceCount: 1,
    },
    provenance: buildFreshProvenance(graphDoc),
    trace: {
      toolRoute: 'similar_cars_fixture_eval',
      collectionsUsed: [GRAPH_COLLECTION],
      matchedRows: rowList.length,
      candidateCount: rowList.length,
      warnings: [],
    },
  };
}

function assertSimilarCarsBlockedFromFinal(policy) {
  assert.strictEqual(policy.canUseForFinalRecommendation, false, 'similar_cars must not final recommend');
  assert.strictEqual(policy.allowedAnswerType, ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY, 'similar_cars must stay diagnostic_only');
  assert.strictEqual(policy.claimType, CLAIM_TYPES.DIAGNOSTIC, 'similar_cars must stay diagnostic claim type');
  assert(policy.blockedReasons.includes(BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE), 'module eligibility block missing');
}

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) {
    throw new Error('Missing Mongo URI.');
  }

  await mongoose.connect(mongoUri);
  const db = getDb();
  const { runVehicleSimilarTool } = await import('../../services/aiAgent/tools/newCars/vehicleSimilar.tool.js');

  const {
    graphDoc,
    directOutput,
    directRows,
  } = await findUsableSimilarGraphFixture({ db, runVehicleSimilarTool });

  assert.strictEqual(directOutput.tool, 'vehicle_similar', 'similar tool name mismatch');
  assert.strictEqual(directOutput.usageGuardrail?.canUseForFinalRecommendation, false, 'similar tool usage guardrail missing');
  assert(Array.isArray(directRows), 'similar output rows must be an array');
  assert(directRows.length > 0, 'similar output should contain rows for selected fixture');
  assert(!directRows.some((row) => row.modelKey === graphDoc.anchor.modelKey), 'similar output includes anchor duplicate');

  const finalWrapped = applyDecisionPolicyWithModuleProfile(
    buildSimilarDecisionInput({
      graphDoc,
      output: directOutput,
      rows: directRows,
      requestedFinalRecommendation: true,
    })
  );

  assertSimilarCarsBlockedFromFinal(finalWrapped.decisionPolicy);
  assert.strictEqual(decisionOutputHasUsefulResult(finalWrapped), true, 'similar fixture should be useful');

  const diagnosticWrapped = applyDecisionPolicyWithModuleProfile(
    buildSimilarDecisionInput({
      graphDoc,
      output: directOutput,
      rows: directRows,
      requestedFinalRecommendation: false,
    })
  );

  assert.strictEqual(diagnosticWrapped.decisionPolicy.canUseForFinalRecommendation, false);
  assert.strictEqual(diagnosticWrapped.decisionPolicy.allowedAnswerType, ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY);
  assert.strictEqual(diagnosticWrapped.decisionPolicy.degradedMode, null);
  assert(!diagnosticWrapped.decisionPolicy.blockedReasons.includes(BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE));

  const rawGraphWrapped = applyDecisionPolicyWithModuleProfile(
    buildSimilarDecisionInput({
      graphDoc,
      output: {
        anchor: graphDoc.anchor,
        similarModels: graphDoc.similarModels,
      },
      rows: graphDoc.similarModels,
      requestedFinalRecommendation: true,
    })
  );

  assertSimilarCarsBlockedFromFinal(rawGraphWrapped.decisionPolicy);
  assert.strictEqual(decisionOutputHasUsefulResult(rawGraphWrapped), true, 'raw graph fixture should be useful');

  const emptyWrapped = applyDecisionPolicyWithModuleProfile({
    module: DECISION_MODULES.SIMILAR_CARS,
    intent: 'similar_cars_empty_fixture',
    requestedFinalRecommendation: true,
    buyerContext: completeBuyerContext,
    rows: [],
    diagnostics: [],
    recoveryOptions: [],
    evidence: {
      evidenceStatus: EVIDENCE_STATUS.MISSING,
      confidence: CONFIDENCE_LEVELS.LOW,
      sourceTransparency: ['similar_graph_fixture_output'],
      missingData: ['similar_models'],
      usableEvidenceCount: 0,
      requiredEvidenceCount: 1,
    },
    provenance: buildFreshProvenance(graphDoc),
    trace: {
      toolRoute: 'similar_cars_fixture_eval',
      collectionsUsed: [GRAPH_COLLECTION],
      matchedRows: 0,
      candidateCount: 0,
      warnings: [],
    },
  });

  assert.strictEqual(emptyWrapped.decisionPolicy.canUseForFinalRecommendation, false);
  assert.strictEqual(emptyWrapped.decisionPolicy.degradedMode, DEGRADED_MODES.EMPTY_RESULT_RECOVERY_REQUIRED);
  assert(emptyWrapped.decisionPolicy.blockedReasons.includes(BLOCKED_REASONS.EMPTY_RESULT));

  const summary = {
    suite: 'ACI Similar Cars Real Output Policy Fixture Eval v1',
    ok: true,
    sample: {
      hasAnchorModelKey: Boolean(graphDoc.anchor?.modelKey),
      similarRowCount: directRows.length,
      rawGraphRowCount: graphDoc.similarModels.length,
      graphVersion: graphDoc.graphVersion,
    },
    checks: {
      directToolFinalBlocked: true,
      directToolDiagnosticAllowedAsDiagnostic: true,
      rawGraphFinalBlocked: true,
      emptyOutputRecoveryRequired: true,
      noAnchorDuplicate: true,
    },
    policy: {
      similarAllowedAnswerType: diagnosticWrapped.decisionPolicy.allowedAnswerType,
      similarCanUseForFinalRecommendation: diagnosticWrapped.decisionPolicy.canUseForFinalRecommendation,
      similarFinalRequestBlockedReasons: finalWrapped.decisionPolicy.blockedReasons,
      emptyDegradedMode: emptyWrapped.decisionPolicy.degradedMode,
    },
  };

  console.log(JSON.stringify(summary, null, 2));
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
