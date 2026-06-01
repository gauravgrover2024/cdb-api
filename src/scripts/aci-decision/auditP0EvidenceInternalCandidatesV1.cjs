#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const mongoose = require('mongoose');

const EVIDENCE_COLLECTION = process.env.ACI_VARIANT_EXTERNAL_EVIDENCE_COLLECTION || 'aci_variant_external_evidence';
const FEATURE_COLLECTION = process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2';
const PRICE_COLLECTION = process.env.ACI_PRICE_ROWS_COLLECTION || 'aci_vehicle_price_rows';
const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const normalizeKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const tokens = (value) =>
  normalizeKey(value)
    .split('_')
    .filter(Boolean)
    .filter((t) => !['the', 'and', 'plus'].includes(t));

const scoreText = (target, candidate) => {
  const a = normalizeKey(target);
  const b = normalizeKey(candidate);

  if (!a || !b) return 0;
  if (a === b) return 100;
  if (b.includes(a) || a.includes(b)) return 82;

  const at = new Set(tokens(a));
  const bt = new Set(tokens(b));

  if (!at.size || !bt.size) return 0;

  let overlap = 0;
  for (const t of at) {
    if (bt.has(t)) overlap += 1;
  }

  const coverage = overlap / Math.max(at.size, bt.size);
  return Math.round(coverage * 70);
};

const candidateScore = (seed, candidate) => {
  const scores = [
    scoreText(seed.variantKey, candidate.variantKey),
    scoreText(seed.variant, candidate.variant),
    scoreText(seed.variantFullName, candidate.variant),
    scoreText(seed.variantFullName, candidate.variantName),
    scoreText(seed.variantFullName, candidate.variantFullName),
  ];

  let score = Math.max(...scores);

  if (candidate.activePricelistMatched === true) score += 8;
  if (normalizeKey(seed.fuelKey) && normalizeKey(candidate.fuelKey) === normalizeKey(seed.fuelKey)) score += 3;
  if (normalizeKey(seed.transmissionKey) && normalizeKey(candidate.transmissionKey) === normalizeKey(seed.transmissionKey)) score += 3;

  return Math.min(score, 110);
};

const compactCandidate = (seed, candidate, source) => ({
  source,
  score: candidateScore(seed, candidate),
  make: candidate.make || candidate.brand || null,
  makeKey: candidate.makeKey || candidate.brandKey || null,
  model: candidate.model || null,
  modelKey: candidate.modelKey || null,
  variant: candidate.variant || candidate.variantName || candidate.variantFullName || null,
  variantKey: candidate.variantKey || null,
  fuel: candidate.fuel || null,
  fuelKey: candidate.fuelKey || null,
  transmission: candidate.transmission || null,
  transmissionKey: candidate.transmissionKey || null,
  gearbox: candidate.gearbox || null,
  activePricelistMatched: candidate.activePricelistMatched ?? null,
  featureKeyCount: Array.isArray(candidate.featureKeys) ? candidate.featureKeys.length : undefined,
  hasFeaturesByKey: Boolean(candidate.featuresByKey),
  citySlug: candidate.citySlug || undefined,
  exShowroomPrice: candidate.exShowroomPrice || undefined,
});

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const evidenceCol = db.collection(EVIDENCE_COLLECTION);
  const featureCol = db.collection(FEATURE_COLLECTION);
  const priceCol = db.collection(PRICE_COLLECTION);
  const profileCol = db.collection(PROFILE_COLLECTION);

  const seeds = await evidenceCol.find({
    priority: 'P0',
    status: 'needs_source',
    evidenceType: { $in: ['feature_matrix', 'transmission_spec'] },
  }, {
    projection: {
      _id: 0,
      evidenceKey: 1,
      gapKey: 1,
      gapType: 1,
      evidenceType: 1,
      variantProfileKey: 1,
      variantFullName: 1,
      make: 1,
      makeKey: 1,
      model: 1,
      modelKey: 1,
      brandModelKey: 1,
      variant: 1,
      variantKey: 1,
      fuel: 1,
      fuelKey: 1,
      transmission: 1,
      transmissionKey: 1,
      fuelTransmissionFamilyKey: 1,
    }
  }).sort({ makeKey: 1, modelKey: 1, variantKey: 1, evidenceType: 1 }).toArray();

  const output = [];

  for (const seed of seeds) {
    const modelKeys = [...new Set([
      normalizeKey(seed.modelKey),
      normalizeKey(seed.model),
    ].filter(Boolean))];

    const makeKeys = [...new Set([
      normalizeKey(seed.makeKey),
      normalizeKey(seed.make),
    ].filter(Boolean))];

    const featureQuery = {
      $and: [
        {
          $or: [
            { modelKey: { $in: modelKeys } },
            { model: seed.model },
          ]
        },
        {
          $or: [
            { makeKey: { $in: makeKeys } },
            { brandKey: { $in: makeKeys } },
            { make: seed.make },
            { brand: seed.make },
          ]
        }
      ]
    };

    const featureDocs = await featureCol.find(featureQuery, {
      projection: {
        _id: 0,
        make: 1,
        makeKey: 1,
        brand: 1,
        brandKey: 1,
        model: 1,
        modelKey: 1,
        variant: 1,
        variantName: 1,
        variantKey: 1,
        fuel: 1,
        fuelKey: 1,
        transmission: 1,
        transmissionKey: 1,
        gearbox: 1,
        activePricelistMatched: 1,
        featureKeys: 1,
        featuresByKey: 1,
      }
    }).limit(200).toArray();

    const priceDocs = await priceCol.find({
      $or: [
        { modelKey: { $in: modelKeys } },
        { model: seed.model },
      ]
    }, {
      projection: {
        _id: 0,
        make: 1,
        makeKey: 1,
        brand: 1,
        brandKey: 1,
        model: 1,
        modelKey: 1,
        variant: 1,
        variantName: 1,
        variantFullName: 1,
        variantKey: 1,
        fuel: 1,
        fuelKey: 1,
        transmission: 1,
        transmissionKey: 1,
        gearbox: 1,
        citySlug: 1,
        exShowroomPrice: 1,
      }
    }).limit(500).toArray();

    const profile = await profileCol.findOne({
      variantProfileKey: seed.variantProfileKey,
    }, {
      projection: {
        _id: 0,
        variantProfileKey: 1,
        variantFullName: 1,
        fuel: 1,
        fuelKey: 1,
        transmission: 1,
        transmissionKey: 1,
        fuelTransmissionFamilyKey: 1,
        gearbox: 1,
        engineCc: 1,
        powerBhp: 1,
        torqueNm: 1,
        dataQuality: 1,
      }
    });

    const featureCandidates = featureDocs
      .map((doc) => compactCandidate(seed, doc, 'vehicle_variant_feature_matrix_v2'))
      .sort((a, b) => b.score - a.score)
      .slice(0, 8);

    const priceCandidates = priceDocs
      .map((doc) => compactCandidate(seed, doc, 'aci_vehicle_price_rows'))
      .sort((a, b) => b.score - a.score)
      .slice(0, 8);

    output.push({
      evidenceKey: seed.evidenceKey,
      evidenceType: seed.evidenceType,
      gapType: seed.gapType,
      variantProfileKey: seed.variantProfileKey,
      variantFullName: seed.variantFullName,
      currentProfile: profile,
      topFeatureCandidates: featureCandidates,
      topPriceCandidates: priceCandidates,
      verdict: {
        hasStrongFeatureCandidate: featureCandidates.some((c) => c.score >= 90),
        hasPossibleFeatureCandidate: featureCandidates.some((c) => c.score >= 70),
        hasStrongPriceCandidate: priceCandidates.some((c) => c.score >= 90),
        hasPossiblePriceCandidate: priceCandidates.some((c) => c.score >= 70),
      }
    });
  }

  const summary = {
    seeds: seeds.length,
    featureMatrixSeeds: seeds.filter((s) => s.evidenceType === 'feature_matrix').length,
    transmissionSeeds: seeds.filter((s) => s.evidenceType === 'transmission_spec').length,
    strongFeatureCandidateSeeds: output.filter((o) => o.verdict.hasStrongFeatureCandidate).length,
    possibleFeatureCandidateSeeds: output.filter((o) => o.verdict.hasPossibleFeatureCandidate).length,
    strongPriceCandidateSeeds: output.filter((o) => o.verdict.hasStrongPriceCandidate).length,
    possiblePriceCandidateSeeds: output.filter((o) => o.verdict.hasPossiblePriceCandidate).length,
  };

  console.log(JSON.stringify({
    summary,
    items: output,
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
