#!/usr/bin/env node

require('dotenv').config();

const assert = require('assert');
const mongoose = require('mongoose');

const {
  getVariantScoreInsight,
  getModelScoreInsights,
  getSameFamilyValueInsights,
} = require('../../services/aciCore/scoreProfiles/aciVariantScoreInsight.service.cjs');

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

const SCORE_PROFILE_COLLECTION = 'aci_vehicle_variant_score_profile';

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

async function getCandidateScoreProfiles(db, limit = 80) {
  return db.collection(SCORE_PROFILE_COLLECTION)
    .find(
      {
        variantProfileKey: { $exists: true, $type: 'string', $ne: '' },
        makeKey: { $exists: true, $type: 'string', $ne: '' },
        modelKey: { $exists: true, $type: 'string', $ne: '' },
        fuelKey: { $exists: true, $type: 'string', $ne: '' },
        transmissionKey: { $exists: true, $type: 'string', $ne: '' },
      },
      {
        projection: {
          _id: 0,
          variantProfileKey: 1,
          makeKey: 1,
          modelKey: 1,
          fuelKey: 1,
          transmissionKey: 1,
          buildVersion: 1,
          formulaVersion: 1,
          builtAt: 1,
          updatedAt: 1,
          createdAt: 1,
        },
      }
    )
    .sort({ updatedAt: -1, variantProfileKey: 1 })
    .limit(limit)
    .toArray();
}

async function findUsableScoreInsightFixture(db) {
  const candidates = await getCandidateScoreProfiles(db);

  for (const sample of candidates) {
    try {
      const variantInsight = await getVariantScoreInsight({
        db,
        variantProfileKey: sample.variantProfileKey,
      });

      if (!variantInsight || countUsableModuleScores(variantInsight) <= 0) {
        continue;
      }

      const modelInsights = await getModelScoreInsights({
        db,
        makeKey: sample.makeKey,
        modelKey: sample.modelKey,
        fuelKey: sample.fuelKey,
        transmissionKey: sample.transmissionKey,
        limit: 10,
      });

      const modelRows = Array.isArray(modelInsights?.variants) ? modelInsights.variants : [];
      if (modelRows.length <= 0) {
        continue;
      }

      const sameFamily = await getSameFamilyValueInsights({
        db,
        makeKey: sample.makeKey,
        modelKey: sample.modelKey,
        fuelKey: sample.fuelKey,
        transmissionKey: sample.transmissionKey,
        limit: 10,
      });

      const sameFamilyRows = Array.isArray(sameFamily?.variants) ? sameFamily.variants : [];
      if (sameFamilyRows.length <= 0) {
        continue;
      }

      return {
        sample,
        variantInsight,
        modelInsights,
        modelRows,
        sameFamily,
        sameFamilyRows,
      };
    } catch (error) {
      // Keep looking. This fixture selector must find a genuinely usable DB-backed sample,
      // not assume the first score profile supports every score-insight path.
    }
  }

  throw new Error('No usable score insight fixture found with variant, model and same-family outputs.');
}

function countUsableModuleScores(insight = {}) {
  const modules = insight.modules || {};
  return Object.values(modules).filter((moduleScore) => Number.isFinite(Number(moduleScore?.score))).length;
}

function buildFreshProvenance(sample = {}) {
  return {
    buildVersion: sample.buildVersion || sample.formulaVersion || 'score_profile_fixture_unknown_build',
    builtAt: new Date(sample.builtAt || sample.updatedAt || sample.createdAt || Date.now()).toISOString(),
    sourceClass: SOURCE_CLASSES.INTERNAL_SCORE_PROFILE,
    stalenessDays: 0,
    needsRebuild: false,
  };
}

function buildScoreInsightDecisionInput({ sample, output, rows, requestedFinalRecommendation }) {
  const rowList = Array.isArray(rows) ? rows : [output].filter(Boolean);
  const usableEvidenceCount = rowList.reduce((sum, row) => sum + countUsableModuleScores(row), 0);

  return {
    module: DECISION_MODULES.SCORE_INSIGHT,
    intent: 'score_insight_real_output_fixture',
    requestedFinalRecommendation,
    buyerContext: requestedFinalRecommendation ? completeBuyerContext : {},
    rows: rowList,
    diagnostics: rowList.flatMap((row) => [
      ...(Array.isArray(row.strengths) ? row.strengths : []),
      ...(Array.isArray(row.watchouts) ? row.watchouts : []),
    ]),
    evidence: {
      evidenceStatus: usableEvidenceCount > 0 ? EVIDENCE_STATUS.COMPLETE : EVIDENCE_STATUS.MISSING,
      confidence: usableEvidenceCount > 0 ? CONFIDENCE_LEVELS.HIGH : CONFIDENCE_LEVELS.LOW,
      sourceTransparency: ['score_profile_fixture_output'],
      missingData: [],
      usableEvidenceCount,
      requiredEvidenceCount: usableEvidenceCount > 0 ? usableEvidenceCount : 1,
    },
    provenance: buildFreshProvenance(sample),
    trace: {
      toolRoute: 'score_insight_fixture_eval',
      collectionsUsed: [SCORE_PROFILE_COLLECTION],
      matchedRows: rowList.length,
      candidateCount: rowList.length,
      warnings: [],
    },
  };
}

function assertScoreInsightBlockedFromFinal(policy) {
  assert.strictEqual(policy.canUseForFinalRecommendation, false, 'score_insight must not final recommend');
  assert.strictEqual(policy.allowedAnswerType, ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY, 'score_insight must stay diagnostic_only');
  assert.strictEqual(policy.claimType, CLAIM_TYPES.DIAGNOSTIC, 'score_insight must stay diagnostic claim type');
  assert(policy.blockedReasons.includes(BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE), 'module eligibility block missing');
}

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) {
    throw new Error('Missing Mongo URI.');
  }

  await mongoose.connect(mongoUri);
  const db = getDb();
  const {
    sample,
    variantInsight,
    modelInsights,
    modelRows,
    sameFamily,
    sameFamilyRows,
  } = await findUsableScoreInsightFixture(db);

  assert(variantInsight, 'variant score insight missing');
  assert(variantInsight.usageGuardrail, 'variant usageGuardrail missing');
  assert.strictEqual(
    variantInsight.usageGuardrail.canUseForFinalRecommendation,
    false,
    'existing variant score insight guardrail must block final recommendation'
  );

  const variantFinalWrapped = applyDecisionPolicyWithModuleProfile(
    buildScoreInsightDecisionInput({
      sample,
      output: variantInsight,
      requestedFinalRecommendation: true,
    })
  );

  assertScoreInsightBlockedFromFinal(variantFinalWrapped.decisionPolicy);
  assert.strictEqual(decisionOutputHasUsefulResult(variantFinalWrapped), true, 'variant fixture should be useful');

  const variantDiagnosticWrapped = applyDecisionPolicyWithModuleProfile(
    buildScoreInsightDecisionInput({
      sample,
      output: variantInsight,
      requestedFinalRecommendation: false,
    })
  );

  assert.strictEqual(variantDiagnosticWrapped.decisionPolicy.canUseForFinalRecommendation, false);
  assert.strictEqual(variantDiagnosticWrapped.decisionPolicy.allowedAnswerType, ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY);
  assert.strictEqual(variantDiagnosticWrapped.decisionPolicy.degradedMode, null);
  assert(!variantDiagnosticWrapped.decisionPolicy.blockedReasons.includes(BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE));

  assert(modelRows.length > 0, 'model score insight rows missing');
  assert.strictEqual(modelInsights.usageGuardrail?.canUseForFinalRecommendation, false, 'model score insight guardrail missing');

  const modelWrapped = applyDecisionPolicyWithModuleProfile(
    buildScoreInsightDecisionInput({
      sample,
      output: modelInsights,
      rows: modelRows,
      requestedFinalRecommendation: true,
    })
  );

  assertScoreInsightBlockedFromFinal(modelWrapped.decisionPolicy);
  assert.strictEqual(decisionOutputHasUsefulResult(modelWrapped), true, 'model fixture should be useful');

  assert(sameFamilyRows.length > 0, 'same-family score insight rows missing');
  assert.strictEqual(sameFamily.usageGuardrail?.canUseForFinalRecommendation, false, 'same-family score insight guardrail missing');

  const sameFamilyWrapped = applyDecisionPolicyWithModuleProfile(
    buildScoreInsightDecisionInput({
      sample,
      output: sameFamily,
      rows: sameFamilyRows,
      requestedFinalRecommendation: true,
    })
  );

  assertScoreInsightBlockedFromFinal(sameFamilyWrapped.decisionPolicy);

  const emptyWrapped = applyDecisionPolicyWithModuleProfile({
    module: DECISION_MODULES.SCORE_INSIGHT,
    intent: 'score_insight_empty_fixture',
    requestedFinalRecommendation: true,
    buyerContext: completeBuyerContext,
    rows: [],
    diagnostics: [],
    recoveryOptions: [],
    evidence: {
      evidenceStatus: EVIDENCE_STATUS.MISSING,
      confidence: CONFIDENCE_LEVELS.LOW,
      sourceTransparency: ['score_profile_fixture_output'],
      missingData: ['score_profile_rows'],
      usableEvidenceCount: 0,
      requiredEvidenceCount: 1,
    },
    provenance: buildFreshProvenance(sample),
    trace: {
      toolRoute: 'score_insight_fixture_eval',
      collectionsUsed: [SCORE_PROFILE_COLLECTION],
      matchedRows: 0,
      candidateCount: 0,
      warnings: [],
    },
  });

  assert.strictEqual(emptyWrapped.decisionPolicy.canUseForFinalRecommendation, false);
  assert.strictEqual(emptyWrapped.decisionPolicy.degradedMode, DEGRADED_MODES.EMPTY_RESULT_RECOVERY_REQUIRED);
  assert(emptyWrapped.decisionPolicy.blockedReasons.includes(BLOCKED_REASONS.EMPTY_RESULT));

  const summary = {
    suite: 'ACI Score Insight Real Output Policy Fixture Eval v1',
    ok: true,
    sample: {
      hasVariantProfileKey: Boolean(sample.variantProfileKey),
      hasMakeKey: Boolean(sample.makeKey),
      hasModelKey: Boolean(sample.modelKey),
      hasFuelKey: Boolean(sample.fuelKey),
      hasTransmissionKey: Boolean(sample.transmissionKey),
    },
    checks: {
      variantFinalBlocked: true,
      variantDiagnosticAllowedAsDiagnostic: true,
      modelFinalBlocked: true,
      sameFamilyFinalBlocked: true,
      emptyOutputRecoveryRequired: true,
    },
    policy: {
      scoreInsightAllowedAnswerType: variantDiagnosticWrapped.decisionPolicy.allowedAnswerType,
      scoreInsightCanUseForFinalRecommendation: variantDiagnosticWrapped.decisionPolicy.canUseForFinalRecommendation,
      scoreInsightFinalRequestBlockedReasons: variantFinalWrapped.decisionPolicy.blockedReasons,
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
