#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const mongoose = require('mongoose');

const FEATURE_COLLECTION = process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2';
const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';
const TARGET_COLLECTION = process.env.ACI_CRASH_SAFETY_PROFILE_COLLECTION || 'aci_vehicle_crash_safety_profile';

const args = process.argv.slice(2);
const write = args.includes('--write');
const reset = args.includes('--reset');

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const RATING_KEYS = {
  globalAdult: 'global_ncap_safety_rating',
  globalChild: 'global_ncap_child_safety_rating',
  bharatAdult: 'bharat_ncap_safety_rating',
  bharatChild: 'bharat_ncap_child_safety_rating',
};

const now = () => new Date();

const normalizeKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const isUsable = (value) => {
  if (value === null || value === undefined) return false;
  const s = String(value).trim().toLowerCase();
  return Boolean(s) && !['-', 'na', 'n/a', 'not available', 'not tested', 'null', 'undefined'].includes(s);
};

const parseStar = (value) => {
  if (!isUsable(value)) return null;
  const match = String(value).match(/(\d+(?:\.\d+)?)/);
  if (!match) return null;
  const num = Number(match[1]);
  return Number.isFinite(num) ? num : null;
};

const ratingFromFeature = (feature) => {
  if (!feature || !isUsable(feature.value)) return null;

  return {
    displayName: feature.displayName || null,
    value: String(feature.value).trim(),
    stars: parseStar(feature.value),
    available: feature.available ?? null,
    availabilityStatus: feature.availabilityStatus || null,
    conflictStatus: feature.conflictStatus || null,
    synthetic: feature.synthetic ?? null,
  };
};

const profileIndexKeys = (doc) => {
  const makeKeys = [
    doc.makeKey,
    doc.brandKey,
    normalizeKey(doc.make),
    normalizeKey(doc.brand),
  ].filter(Boolean).map(normalizeKey);

  const modelKeys = [
    doc.modelKey,
    normalizeKey(doc.model),
  ].filter(Boolean).map(normalizeKey);

  const variantKeys = [
    doc.variantKey,
    normalizeKey(doc.variant),
    normalizeKey(doc.variantName),
  ].filter(Boolean).map(normalizeKey);

  const keys = [];

  for (const makeKey of makeKeys) {
    for (const modelKey of modelKeys) {
      for (const variantKey of variantKeys) {
        keys.push(`make:${makeKey}__${modelKey}__${variantKey}`);
      }
    }
  }

  for (const modelKey of modelKeys) {
    for (const variantKey of variantKeys) {
      keys.push(`model:${modelKey}__${variantKey}`);
    }
  }

  return [...new Set(keys)];
};

const addMapArray = (map, key, value) => {
  if (!key) return;
  if (!map.has(key)) map.set(key, []);
  map.get(key).push(value);
};

const loadDecisionProfileIndex = async (profilesCol) => {
  const projection = {
    _id: 0,
    variantProfileKey: 1,
    make: 1,
    makeKey: 1,
    model: 1,
    modelKey: 1,
    fullModel: 1,
    variant: 1,
    variantKey: 1,
    variantFullName: 1,
    brandModelKey: 1,
    fuel: 1,
    fuelKey: 1,
    transmission: 1,
    transmissionKey: 1,
    fuelTransmissionFamilyKey: 1,
  };

  const index = new Map();
  const total = await profilesCol.estimatedDocumentCount();
  console.log(`[load] decision profiles=${total}`);

  const cursor = profilesCol.find({}, { projection }).batchSize(500);
  let loaded = 0;

  for await (const profile of cursor) {
    loaded += 1;
    for (const key of profileIndexKeys(profile)) {
      addMapArray(index, key, profile);
    }
  }

  console.log(`[load] decision profiles loaded=${loaded}`);
  return index;
};

const buildFeatureProjection = () => {
  const projection = {
    _id: 1,
    make: 1,
    makeKey: 1,
    brand: 1,
    brandKey: 1,
    model: 1,
    modelKey: 1,
    variant: 1,
    variantName: 1,
    variantKey: 1,
    activePricelistMatched: 1,
  };

  for (const key of Object.values(RATING_KEYS)) {
    projection[`featuresByKey.${key}`] = 1;
  }

  return projection;
};

const buildRatingDoc = ({ profile, featureDoc, ratings, matchKey, matchAmbiguityCount }) => {
  const nowDate = now();

  const sourceFeatureKeys = Object.entries(RATING_KEYS)
    .filter(([_, key]) => Boolean(ratingFromFeature(featureDoc.featuresByKey?.[key])))
    .map(([name, key]) => ({ name, key }));

  const hasGlobal = Boolean(ratings.globalAdult || ratings.globalChild);
  const hasBharat = Boolean(ratings.bharatAdult || ratings.bharatChild);

  return {
    crashSafetyProfileKey: `${profile.variantProfileKey}__internal_feature_matrix_crash_rating`,
    variantProfileKey: profile.variantProfileKey,
    variantFullName: profile.variantFullName,
    make: profile.make,
    makeKey: profile.makeKey,
    model: profile.model,
    modelKey: profile.modelKey,
    fullModel: profile.fullModel,
    brandModelKey: profile.brandModelKey,
    variant: profile.variant,
    variantKey: profile.variantKey,
    fuel: profile.fuel,
    fuelKey: profile.fuelKey,
    transmission: profile.transmission,
    transmissionKey: profile.transmissionKey,
    fuelTransmissionFamilyKey: profile.fuelTransmissionFamilyKey,

    hasCrashRating: hasGlobal || hasBharat,
    globalNcapAdult: ratings.globalAdult,
    globalNcapChild: ratings.globalChild,
    bharatNcapAdult: ratings.bharatAdult,
    bharatNcapChild: ratings.bharatChild,

    sourceCollection: FEATURE_COLLECTION,
    sourceFeatureDocId: featureDoc._id ? String(featureDoc._id) : null,
    sourceFeatureVariant: featureDoc.variant || featureDoc.variantName || null,
    sourceFeatureVariantKey: featureDoc.variantKey || null,
    sourceFeatureModel: featureDoc.model || null,
    sourceFeatureModelKey: featureDoc.modelKey || null,
    sourceFeatureKeys,
    sourceActivePricelistMatched: featureDoc.activePricelistMatched ?? null,

    matchKey,
    matchAmbiguityCount,

    applicabilityScope: 'internal_variant_feature_matrix',
    confidence: 'internal_feature_matrix',
    needsOfficialVerification: true,
    reviewStatus: 'needs_official_applicability_check',
    applicabilityNotes: 'Imported from internal variant feature matrix. Official tested-variant applicability still needs verification before using as final safety judgement.',

    sourceVersion: 'aci_crash_safety_profile_from_feature_matrix_v1_2026_06_01',
    createdAt: nowDate,
    updatedAt: nowDate,
  };
};

const preferDoc = (next, current) => {
  if (!current) return true;

  const nextScore =
    (next.globalNcapAdult ? 2 : 0) +
    (next.globalNcapChild ? 1 : 0) +
    (next.bharatNcapAdult ? 2 : 0) +
    (next.bharatNcapChild ? 1 : 0) +
    (next.sourceActivePricelistMatched === true ? 1 : 0);

  const currentScore =
    (current.globalNcapAdult ? 2 : 0) +
    (current.globalNcapChild ? 1 : 0) +
    (current.bharatNcapAdult ? 2 : 0) +
    (current.bharatNcapChild ? 1 : 0) +
    (current.sourceActivePricelistMatched === true ? 1 : 0);

  return nextScore > currentScore;
};

const safeCreateIndex = async (collection, keys, options = {}) => {
  let indexes = [];
  try {
    indexes = await collection.indexes();
  } catch (error) {
    if (error?.code !== 26 && error?.codeName !== 'NamespaceNotFound') throw error;
  }

  const wanted = JSON.stringify(keys);
  if (indexes.some((idx) => JSON.stringify(idx.key) === wanted)) return;

  const name = options.name || Object.entries(keys).map(([k, v]) => `${k}_${v}`).join('_');
  await collection.createIndex(keys, { ...options, name });
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const featuresCol = db.collection(FEATURE_COLLECTION);
  const profilesCol = db.collection(PROFILE_COLLECTION);
  const targetCol = db.collection(TARGET_COLLECTION);

  console.log(`[mode] ${write ? 'WRITE' : 'DRY_RUN'}, reset=${reset}`);
  console.log(`[source] features=${FEATURE_COLLECTION}, profiles=${PROFILE_COLLECTION}`);
  console.log(`[target] ${TARGET_COLLECTION}`);

  const profileIndex = await loadDecisionProfileIndex(profilesCol);

  const ratingExistsQuery = {
    $or: Object.values(RATING_KEYS).map((key) => ({
      [`featuresByKey.${key}`]: { $exists: true }
    }))
  };

  const cursor = featuresCol.find(ratingExistsQuery, {
    projection: buildFeatureProjection(),
  }).batchSize(500);

  const docsByKey = new Map();
  const unmatchedSamples = [];
  const ambiguousSamples = [];

  let scanned = 0;
  let docsWithUsableRating = 0;
  let matchedFeatureDocs = 0;
  let unmatchedFeatureDocs = 0;
  let ambiguousFeatureDocs = 0;

  for await (const featureDoc of cursor) {
    scanned += 1;

    const ratings = {
      globalAdult: ratingFromFeature(featureDoc.featuresByKey?.[RATING_KEYS.globalAdult]),
      globalChild: ratingFromFeature(featureDoc.featuresByKey?.[RATING_KEYS.globalChild]),
      bharatAdult: ratingFromFeature(featureDoc.featuresByKey?.[RATING_KEYS.bharatAdult]),
      bharatChild: ratingFromFeature(featureDoc.featuresByKey?.[RATING_KEYS.bharatChild]),
    };

    if (!Object.values(ratings).some(Boolean)) continue;
    docsWithUsableRating += 1;

    const keys = profileIndexKeys(featureDoc);
    let candidates = [];
    let matchedKey = null;

    for (const key of keys) {
      const found = profileIndex.get(key) || [];
      if (found.length) {
        candidates = found;
        matchedKey = key;
        break;
      }
    }

    if (!candidates.length) {
      unmatchedFeatureDocs += 1;
      if (unmatchedSamples.length < 40) {
        unmatchedSamples.push({
          make: featureDoc.make,
          model: featureDoc.model,
          modelKey: featureDoc.modelKey,
          variant: featureDoc.variant || featureDoc.variantName,
          variantKey: featureDoc.variantKey,
          activePricelistMatched: featureDoc.activePricelistMatched,
          attemptedKeys: keys.slice(0, 8),
          ratings,
        });
      }
      continue;
    }

    if (candidates.length > 1) {
      ambiguousFeatureDocs += 1;
      if (ambiguousSamples.length < 40) {
        ambiguousSamples.push({
          make: featureDoc.make,
          model: featureDoc.model,
          modelKey: featureDoc.modelKey,
          variant: featureDoc.variant || featureDoc.variantName,
          variantKey: featureDoc.variantKey,
          activePricelistMatched: featureDoc.activePricelistMatched,
          matchedKey,
          candidateCount: candidates.length,
          candidateVariantProfileKeys: candidates.slice(0, 10).map((p) => p.variantProfileKey),
        });
      }
    }

    matchedFeatureDocs += 1;

    for (const profile of candidates) {
      const doc = buildRatingDoc({
        profile,
        featureDoc,
        ratings,
        matchKey: matchedKey,
        matchAmbiguityCount: candidates.length,
      });

      const current = docsByKey.get(doc.crashSafetyProfileKey);
      if (preferDoc(doc, current)) {
        docsByKey.set(doc.crashSafetyProfileKey, doc);
      }
    }
  }

  const docs = [...docsByKey.values()];
  const byProgram = {
    globalAdult: docs.filter((doc) => doc.globalNcapAdult).length,
    globalChild: docs.filter((doc) => doc.globalNcapChild).length,
    bharatAdult: docs.filter((doc) => doc.bharatNcapAdult).length,
    bharatChild: docs.filter((doc) => doc.bharatNcapChild).length,
  };

  let writeResult = null;

  if (write) {
    if (reset) {
      await targetCol.deleteMany({});
      console.log(`[reset] cleared ${TARGET_COLLECTION}`);
    }

    await safeCreateIndex(targetCol, { crashSafetyProfileKey: 1 }, { unique: true, name: 'crash_safety_profile_key_unique' });
    await safeCreateIndex(targetCol, { variantProfileKey: 1 }, { name: 'crash_safety_variant_idx' });
    await safeCreateIndex(targetCol, { brandModelKey: 1 }, { name: 'crash_safety_brand_model_idx' });
    await safeCreateIndex(targetCol, { makeKey: 1, modelKey: 1 }, { name: 'crash_safety_make_model_idx' });
    await safeCreateIndex(targetCol, { needsOfficialVerification: 1, reviewStatus: 1 }, { name: 'crash_safety_review_idx' });

    let upserted = 0;
    let modified = 0;
    let bulk = [];

    for (const doc of docs) {
      const { createdAt, ...setDoc } = doc;

      bulk.push({
        updateOne: {
          filter: { crashSafetyProfileKey: doc.crashSafetyProfileKey },
          update: {
            $set: setDoc,
            $setOnInsert: { createdAt },
          },
          upsert: true,
        },
      });

      if (bulk.length >= 500) {
        const result = await targetCol.bulkWrite(bulk, { ordered: false });
        upserted += result.upsertedCount || 0;
        modified += result.modifiedCount || 0;
        bulk = [];
      }
    }

    if (bulk.length) {
      const result = await targetCol.bulkWrite(bulk, { ordered: false });
      upserted += result.upsertedCount || 0;
      modified += result.modifiedCount || 0;
    }

    writeResult = { upserted, modified };
  }

  console.log(JSON.stringify({
    mode: write ? 'WRITE' : 'DRY_RUN',
    scannedFeatureDocsWithRatingKeys: scanned,
    docsWithUsableRating,
    matchedFeatureDocs,
    unmatchedFeatureDocs,
    ambiguousFeatureDocs,
    builtCrashSafetyProfiles: docs.length,
    byProgram,
    unmatchedSamples,
    ambiguousSamples,
    samples: docs.slice(0, 30).map((doc) => ({
      crashSafetyProfileKey: doc.crashSafetyProfileKey,
      variantFullName: doc.variantFullName,
      sourceFeatureVariant: doc.sourceFeatureVariant,
      sourceActivePricelistMatched: doc.sourceActivePricelistMatched,
      globalNcapAdult: doc.globalNcapAdult?.value || null,
      globalNcapChild: doc.globalNcapChild?.value || null,
      bharatNcapAdult: doc.bharatNcapAdult?.value || null,
      bharatNcapChild: doc.bharatNcapChild?.value || null,
      matchAmbiguityCount: doc.matchAmbiguityCount,
      needsOfficialVerification: doc.needsOfficialVerification,
    })),
    writeResult,
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
