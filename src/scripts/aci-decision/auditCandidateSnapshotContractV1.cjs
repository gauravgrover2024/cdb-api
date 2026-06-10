#!/usr/bin/env node

require('dotenv').config();

const assert = require('assert');
const path = require('path');
const { pathToFileURL } = require('url');
const mongoose = require('mongoose');

const getMongoUri = () =>
  process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const PERSONAL_DATA_PATTERNS = [
  /\bphone(Number)?\b/i,
  /\bmobile(Number)?\b/i,
  /\bwhatsapp(Number)?\b/i,
  /\bemail(Address)?\b/i,
  /\bleadId\b/i,
  /\bcrmLeadId\b/i,
  /\bsessionId\b/i,
  /\buserId\b/i,
  /\bipAddress\b/i,
  /\bdealerSharingConsent\b/i,
  /\bconsent(Status|Id)?\b/i,
];

const FINAL_DECISION_PATTERNS = [
  /\bfinalRecommendation\b/i,
  /\boverallWinner\b/i,
  /\bwinner\b/i,
  /\bmustBuy\b/i,
  /\bbuyThis\b/i,
  /\bcanUseForFinalRecommendation\s*:\s*true\b/i,
  /\bsponsoredInfluenceDetected\s*:\s*true\b/i,
];

const TEST_CASES = [
  {
    id: 'creta_vs_seltos',
    message: 'Creta vs Seltos',
    minModels: 2,
    requiredText: [/creta/i, /seltos/i],
    requiredTaskKeys: ['vehicle_comparison'],
  },
  {
    id: 'grand_vitara_vs_hyryder',
    message: 'Grand Vitara vs Hyryder',
    minModels: 2,
    requiredText: [/grand.*vitara/i, /hyryder/i],
    requiredTaskKeys: ['vehicle_comparison'],
  },
  {
    id: 'creta_sx_price_delhi',
    message: 'Creta SX on-road price Delhi',
    minModels: 1,
    requiredText: [/creta/i, /\bsx\b/i],
    variantResolutionOneOf: ['exact', 'ambiguous', null],
  },
  {
    id: 'creta_sxo_features_inactive_variant_guard',
    message: 'Creta SX(O) features',
    minModels: 1,
    maxVariants: 0,
    requiredText: [/creta/i, /sx/i],
    requiredTaskKeys: ['feature_answer'],
    variantResolutionOneOf: ['exact_unavailable'],
    expectedVariantResolutionReason: 'parenthetical_variant_not_found',
    note:
      'SX(O) exists in historical/raw feature sources but is inactive in vehicle_variant_feature_matrix_v2; default live new-car answers must not expose discontinued variants.',
  },
  {
    id: 'punch_adventure_s_sunroof',
    message: 'Punch Adventure S sunroof',
    minModels: 1,
    requiredText: [/punch/i, /adventure/i],
    minFeatures: 1,
    variantResolutionOneOf: ['exact', 'ambiguous', null],
  },
  {
    id: 'bmw_ix_range',
    message: 'BMW IX range',
    minModels: 1,
    requiredText: [/bmw/i, /\bix\b/i],
  },
  {
    id: 'eqs_range',
    message: 'EQS range',
    minModels: 1,
    requiredText: [/eqs/i],
  },
  {
    id: 'seltos_price_mumbai',
    message: 'Seltos price Mumbai',
    minModels: 1,
    requiredText: [/seltos/i, /mumbai/i],
  },
  {
    id: 'cars_under_20_lakh',
    message: 'cars under 20 lakh',
    minBudgets: 1,
    requiredText: [/2000000|20/i],
  },
  {
    id: 'automatic_suvs_under_20_lakh',
    message: 'automatic SUVs under 20 lakh',
    minBudgets: 1,
    minBodyTypes: 1,
    minTransmissions: 1,
    requiredText: [/automatic/i, /suv/i, /2000000|20/i],
  },
];

const asArray = (value) => (Array.isArray(value) ? value : []);

const normalizeBlob = (value) => JSON.stringify(value || {});

const itemKeyText = (item = {}) =>
  [
    item.rawText,
    item.canonicalKey,
    item.displayName,
    item.type,
    item.source,
    item.metadata?.make,
    item.metadata?.model,
    item.metadata?.variant,
    item.metadata?.fullModel,
    item.metadata?.raw?.make,
    item.metadata?.raw?.model,
    item.metadata?.raw?.variant,
    item.metadata?.raw?.fullModel,
  ]
    .filter(Boolean)
    .join(' ');

const snapshotText = (snapshot = {}) =>
  [
    snapshot.rawMessage,
    snapshot.normalizedMessage,
    ...asArray(snapshot.vehicles?.makes).map(itemKeyText),
    ...asArray(snapshot.vehicles?.models).map(itemKeyText),
    ...asArray(snapshot.vehicles?.variants).map(itemKeyText),
    ...asArray(snapshot.vehicles?.colors).map(itemKeyText),
    ...asArray(snapshot.taxonomy?.features).map(itemKeyText),
    ...asArray(snapshot.taxonomy?.bodyTypes).map(itemKeyText),
    ...asArray(snapshot.taxonomy?.fuelTypes).map(itemKeyText),
    ...asArray(snapshot.taxonomy?.transmissions).map(itemKeyText),
    ...asArray(snapshot.commerce?.budgets).map(itemKeyText),
    ...asArray(snapshot.commerce?.cities).map(itemKeyText),
    ...asArray(snapshot.language?.tasks).map(itemKeyText),
  ]
    .filter(Boolean)
    .join(' ');

const canonicalKeys = (items = []) =>
  asArray(items)
    .map((item) => item?.canonicalKey || item?.displayName || item?.rawText || '')
    .filter(Boolean);

const hasTaskKey = (snapshot, expectedKey) =>
  canonicalKeys(snapshot.language?.tasks).some((key) => key === expectedKey);

const assertNoUnsafeFields = ({ snapshot, id }) => {
  const blob = normalizeBlob(snapshot);

  for (const pattern of PERSONAL_DATA_PATTERNS) {
    assert(!pattern.test(blob), `${id}: personal-data-like field leaked into candidate snapshot: ${pattern}`);
  }

  for (const pattern of FINAL_DECISION_PATTERNS) {
    assert(!pattern.test(blob), `${id}: final-decision/recommendation field leaked into candidate snapshot: ${pattern}`);
  }
};

const auditCandidateItems = ({ items = [], pathLabel, failures }) => {
  asArray(items).forEach((item, index) => {
    if (!item || typeof item !== 'object') {
      failures.push(`${pathLabel}[${index}] is not an object`);
      return;
    }

    if (!item.rawText && !item.canonicalKey && !item.displayName) {
      failures.push(`${pathLabel}[${index}] has no rawText/canonicalKey/displayName`);
    }

    if (!item.type) {
      failures.push(`${pathLabel}[${index}] missing type`);
    }

    if (!item.source) {
      failures.push(`${pathLabel}[${index}] missing source`);
    }

    if (item.confidence !== null && item.confidence !== undefined) {
      const confidence = Number(item.confidence);
      if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) {
        failures.push(`${pathLabel}[${index}] confidence must be 0..1 or null`);
      }
    }
  });
};

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  const schemaModule = await import(pathToFileURL(path.resolve(
    'src/services/aciCore/candidates/aciCandidateSnapshot.schema.js'
  )).href);

  const retrieverModule = await import(pathToFileURL(path.resolve(
    'src/services/aciCore/candidates/aciDbCandidateRetriever.js'
  )).href);

  const {
    CANDIDATE_SNAPSHOT_SCHEMA_VERSION,
    assertCandidateSnapshotShape,
  } = schemaModule;

  const {
    retrieveAciDbCandidates,
    prewarmAciDbCandidateRetrieverCaches,
  } = retrieverModule;

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  if (typeof prewarmAciDbCandidateRetrieverCaches === 'function') {
    await prewarmAciDbCandidateRetrieverCaches({ force: false });
  }

  const results = [];
  const failures = [];

  for (const test of TEST_CASES) {
    const startedAt = Date.now();

    try {
      const snapshot = await retrieveAciDbCandidates({
        rawMessage: test.message,
        normalizedMessage: test.message,
        activeContext: {},
      });

      assertCandidateSnapshotShape(snapshot);

      assert.strictEqual(
        snapshot.schemaVersion,
        CANDIDATE_SNAPSHOT_SCHEMA_VERSION,
        `${test.id}: schema version mismatch`
      );

      assert.strictEqual(
        snapshot.trace?.candidateRetriever,
        'aciDbCandidateRetriever',
        `${test.id}: missing candidateRetriever trace`
      );

      assert(
        snapshot.trace?.candidateRetrieverVersion,
        `${test.id}: missing candidateRetrieverVersion trace`
      );

      assertNoUnsafeFields({ snapshot, id: test.id });

      const candidateFailures = [];

      auditCandidateItems({
        items: snapshot.vehicles?.makes,
        pathLabel: `${test.id}.vehicles.makes`,
        failures: candidateFailures,
      });

      auditCandidateItems({
        items: snapshot.vehicles?.models,
        pathLabel: `${test.id}.vehicles.models`,
        failures: candidateFailures,
      });

      auditCandidateItems({
        items: snapshot.vehicles?.variants,
        pathLabel: `${test.id}.vehicles.variants`,
        failures: candidateFailures,
      });

      auditCandidateItems({
        items: snapshot.taxonomy?.features,
        pathLabel: `${test.id}.taxonomy.features`,
        failures: candidateFailures,
      });

      auditCandidateItems({
        items: snapshot.language?.tasks,
        pathLabel: `${test.id}.language.tasks`,
        failures: candidateFailures,
      });

      if (candidateFailures.length) {
        throw new Error(candidateFailures.join('; '));
      }

      if (test.minModels !== undefined) {
        assert(
          asArray(snapshot.vehicles?.models).length >= test.minModels,
          `${test.id}: expected at least ${test.minModels} model candidate(s), got ${asArray(snapshot.vehicles?.models).length}`
        );
      }

      if (test.minFeatures !== undefined) {
        assert(
          asArray(snapshot.taxonomy?.features).length >= test.minFeatures,
          `${test.id}: expected at least ${test.minFeatures} feature candidate(s), got ${asArray(snapshot.taxonomy?.features).length}`
        );
      }

      if (test.maxVariants !== undefined) {
        assert(
          asArray(snapshot.vehicles?.variants).length <= test.maxVariants,
          `${test.id}: expected at most ${test.maxVariants} variant candidate(s), got ${asArray(snapshot.vehicles?.variants).length}`
        );
      }

      if (test.minBudgets !== undefined) {
        assert(
          asArray(snapshot.commerce?.budgets).length >= test.minBudgets,
          `${test.id}: expected at least ${test.minBudgets} budget candidate(s), got ${asArray(snapshot.commerce?.budgets).length}`
        );
      }

      if (test.minBodyTypes !== undefined) {
        assert(
          asArray(snapshot.taxonomy?.bodyTypes).length >= test.minBodyTypes,
          `${test.id}: expected at least ${test.minBodyTypes} body type candidate(s), got ${asArray(snapshot.taxonomy?.bodyTypes).length}`
        );
      }

      if (test.minTransmissions !== undefined) {
        assert(
          asArray(snapshot.taxonomy?.transmissions).length >= test.minTransmissions,
          `${test.id}: expected at least ${test.minTransmissions} transmission candidate(s), got ${asArray(snapshot.taxonomy?.transmissions).length}`
        );
      }

      const text = snapshotText(snapshot);

      for (const pattern of test.requiredText || []) {
        assert(
          pattern.test(text),
          `${test.id}: expected snapshot text to match ${pattern}; text=${text.slice(0, 500)}`
        );
      }

      for (const taskKey of test.requiredTaskKeys || []) {
        assert(
          hasTaskKey(snapshot, taskKey),
          `${test.id}: expected task key ${taskKey}; actual=${canonicalKeys(snapshot.language?.tasks).join(',')}`
        );
      }

      if (test.variantResolutionOneOf) {
        const status = snapshot.vehicles?.variantResolution?.status || null;
        assert(
          test.variantResolutionOneOf.includes(status),
          `${test.id}: variantResolution status ${status} not in ${test.variantResolutionOneOf.join(',')}`
        );
      }

      if (test.expectedVariantResolutionReason) {
        assert.strictEqual(
          snapshot.vehicles?.variantResolution?.reason || null,
          test.expectedVariantResolutionReason,
          `${test.id}: unexpected variantResolution reason`
        );
      }

      results.push({
        id: test.id,
        ok: true,
        message: test.message,
        durationMs: Date.now() - startedAt,
        counts: snapshot.trace?.counts || null,
        variantResolutionStatus: snapshot.vehicles?.variantResolution?.status || null,
        taskKeys: canonicalKeys(snapshot.language?.tasks),
        modelKeys: canonicalKeys(snapshot.vehicles?.models).slice(0, 8),
        variantKeys: canonicalKeys(snapshot.vehicles?.variants).slice(0, 8),
        featureKeys: canonicalKeys(snapshot.taxonomy?.features).slice(0, 8),
      });
    } catch (error) {
      const failure = {
        id: test.id,
        ok: false,
        message: test.message,
        durationMs: Date.now() - startedAt,
        error: error.message,
      };

      results.push(failure);
      failures.push(failure);
    }
  }

  const summary = {
    suite: 'ACI Candidate Snapshot Contract Audit v1',
    ok: failures.length === 0,
    schemaVersion: CANDIDATE_SNAPSHOT_SCHEMA_VERSION,
    retriever: 'aciDbCandidateRetriever',
    testCount: TEST_CASES.length,
    passed: results.filter((result) => result.ok).length,
    failed: failures.length,
    failures,
    results,
  };

  console.log(JSON.stringify(summary, null, 2));

  if (!summary.ok) process.exit(1);
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
