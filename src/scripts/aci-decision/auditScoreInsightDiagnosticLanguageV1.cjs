#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const {
  getVariantScoreInsight,
  getModelScoreInsights,
} = require('../../services/aciCore/scoreProfiles/aciVariantScoreInsight.service.cjs');

const SCORE_PROFILE_COLLECTION = 'aci_vehicle_variant_score_profile';

const HARD_BANNED_PATTERNS = [
  /\bmust buy\b/i,
  /\bbuy this\b/i,
  /\bgo for this\b/i,
  /\bbest choice\b/i,
  /\bbest pick\b/i,
  /\bclear winner\b/i,
  /\brecommended buy\b/i,
  /\bstrongest value pick\b/i,
  /\bavoid this\b/i,
  /\bpoor resale\b/i,
  /\bstrong resale\b/i,
  /\bservice network\b/i,
];

const FINAL_RECOMMENDATION_RISK_PATTERNS = [
  /\bmy final recommendation\b/i,
  /\bfinal recommendation\s*:/i,
  /\bfinal recommendation is\b/i,
  /\bas a final recommendation\b/i,
  /\bthis is the final recommendation\b/i,
];

const SAFE_FINAL_RECOMMENDATION_CONTEXT_PATTERNS = [
  /\bnot a final recommendation\b/i,
  /\bnot final recommendation\b/i,
  /\bfinal recommendation needs\b/i,
  /\bfinal recommendation requires\b/i,
  /\bfinal recommendation is blocked\b/i,
  /\bfinal recommendation policy\b/i,
  /\bcannot final recommend\b/i,
  /\bcanUseForFinalRecommendation\b/i,
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

async function getCandidateScoreProfiles(db, limit = 50) {
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
          updatedAt: 1,
        },
      }
    )
    .sort({ updatedAt: -1, variantProfileKey: 1 })
    .limit(limit)
    .toArray();
}

function hasSafeFinalRecommendationContext(text = '') {
  return SAFE_FINAL_RECOMMENDATION_CONTEXT_PATTERNS.some((pattern) => pattern.test(text));
}

function findLanguageViolations({ scope, output }) {
  const textItems = collectText(output);
  const violations = [];

  for (const text of textItems) {
    for (const pattern of HARD_BANNED_PATTERNS) {
      if (pattern.test(text)) {
        violations.push({
          scope,
          category: 'hard_banned_language',
          pattern: String(pattern),
          excerpt: text.slice(0, 260),
        });
      }
    }

    for (const pattern of FINAL_RECOMMENDATION_RISK_PATTERNS) {
      if (pattern.test(text) && !hasSafeFinalRecommendationContext(text)) {
        violations.push({
          scope,
          category: 'unsafe_final_recommendation_language',
          pattern: String(pattern),
          excerpt: text.slice(0, 260),
        });
      }
    }
  }

  return violations;
}

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  await mongoose.connect(mongoUri);
  const db = getDb();

  const samples = await getCandidateScoreProfiles(db);
  const violations = [];
  let variantOutputsChecked = 0;
  let modelOutputsChecked = 0;

  for (const sample of samples) {
    const variantInsight = await getVariantScoreInsight({
      db,
      variantProfileKey: sample.variantProfileKey,
    });

    if (variantInsight) {
      variantOutputsChecked += 1;
      violations.push(...findLanguageViolations({
        scope: 'variant_score_insight',
        output: variantInsight,
      }));
    }

    const modelInsights = await getModelScoreInsights({
      db,
      makeKey: sample.makeKey,
      modelKey: sample.modelKey,
      fuelKey: sample.fuelKey,
      transmissionKey: sample.transmissionKey,
      limit: 8,
    });

    if (modelInsights) {
      modelOutputsChecked += 1;
      violations.push(...findLanguageViolations({
        scope: 'model_score_insight',
        output: modelInsights,
      }));
    }
  }

  const summary = {
    suite: 'ACI Score Insight Diagnostic Language Audit v1',
    ok: violations.length === 0,
    checked: {
      scoreProfileSamples: samples.length,
      variantOutputsChecked,
      modelOutputsChecked,
    },
    hardBannedPatternCount: HARD_BANNED_PATTERNS.length,
    finalRecommendationRiskPatternCount: FINAL_RECOMMENDATION_RISK_PATTERNS.length,
    safeFinalRecommendationContextPatternCount: SAFE_FINAL_RECOMMENDATION_CONTEXT_PATTERNS.length,
    violationCount: violations.length,
    violations: violations.slice(0, 50),
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
