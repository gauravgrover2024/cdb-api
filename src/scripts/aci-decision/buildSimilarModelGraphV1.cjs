#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const SOURCE_COLLECTION = process.env.ACI_MODEL_SUMMARY_COLLECTION || 'aci_vehicle_model_summary';
const TARGET_COLLECTION =
  process.env.ACI_SIMILAR_MODEL_GRAPH_COLLECTION || 'aci_vehicle_similar_model_graph_v1';

const GRAPH_VERSION = 'similar_model_graph_v1';
const FORMULA_VERSION = 'similarity_model_v1_relation_guardrails';
const CITY_PRIORITY = ['gurgaon', 'new-delhi', 'noida'];
const MAX_SIMILAR_MODELS = 16;

const args = process.argv.slice(2);
const WRITE = args.includes('--write');
const RESET = args.includes('--reset');

const cleanText = (value = '') =>
  String(value || '')
    .replace(/\s+/g, ' ')
    .trim();

const cleanKey = (value = '') =>
  String(value || '')
    .trim()
    .toLowerCase();

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const uniqueClean = (items = []) =>
  [...new Set(asArray(items).map(cleanText).filter(Boolean))];

const numberValue = (value) => {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : 0;
};

const midpoint = (min = 0, max = 0) => {
  const low = numberValue(min);
  const high = numberValue(max);
  if (low && high) return (low + high) / 2;
  return low || high || 0;
};

const rangeOverlapRatio = (aMin = 0, aMax = 0, bMin = 0, bMax = 0) => {
  const lowA = numberValue(aMin);
  const highA = numberValue(aMax) || lowA;
  const lowB = numberValue(bMin);
  const highB = numberValue(bMax) || lowB;
  if (!lowA || !highA || !lowB || !highB) return 0;

  const overlap = Math.max(0, Math.min(highA, highB) - Math.max(lowA, lowB));
  const union = Math.max(highA, highB) - Math.min(lowA, lowB);
  if (!union) return lowA === lowB ? 1 : 0;
  return Math.max(0, Math.min(1, overlap / union));
};

const overlapRatio = (left = [], right = []) => {
  const a = new Set(asArray(left).map((item) => cleanKey(item)).filter(Boolean));
  const b = new Set(asArray(right).map((item) => cleanKey(item)).filter(Boolean));
  if (!a.size || !b.size) return 0;
  const intersection = [...a].filter((item) => b.has(item)).length;
  const union = new Set([...a, ...b]).size;
  return union ? intersection / union : 0;
};

const hasFuel = (model = {}, fuelName = '') =>
  asArray(model.fuels).some((fuel) => cleanKey(fuel) === cleanKey(fuelName));

const isPowertrainShift = (anchor = {}, candidate = {}) =>
  hasFuel(anchor, 'Electric') !== hasFuel(candidate, 'Electric');

const hasCrossoverCue = (model = {}) =>
  /\b(suv|crossover|cross|urban cruiser|fronx|exter|punch|kiger|magnite)\b/i.test(
    `${model.displayName || ''} ${model.modelKey || ''} ${model.bodyType || ''} ${model.bodyTypeKey || ''}`,
  );

const priceMidRatio = (anchor = {}, candidate = {}) => {
  const anchorMid = midpoint(anchor.minExShowroomPrice, anchor.maxExShowroomPrice);
  const candidateMid = midpoint(candidate.minExShowroomPrice, candidate.maxExShowroomPrice);
  if (!anchorMid || !candidateMid) return 1;
  return candidateMid / anchorMid;
};

const isPlatformTwin = ({
  anchor = {},
  candidate = {},
  priceOverlap = 0,
  priceMidCloseness = 0,
  fuelOverlap = 0,
  transmissionOverlap = 0,
} = {}) =>
  anchor.makeKey &&
  candidate.makeKey &&
  anchor.makeKey !== candidate.makeKey &&
  anchor.bodyTypeKey &&
  anchor.bodyTypeKey === candidate.bodyTypeKey &&
  priceOverlap >= 0.7 &&
  priceMidCloseness >= 0.9 &&
  fuelOverlap >= 0.5 &&
  transmissionOverlap >= 0.8;

const cityRank = (citySlug = '') => {
  const index = CITY_PRIORITY.indexOf(cleanKey(citySlug));
  return index >= 0 ? CITY_PRIORITY.length - index : 0;
};

const toModelDoc = (doc = {}) => ({
  makeKey: cleanKey(doc.makeKey),
  modelKey: cleanKey(doc.modelKey),
  displayName: cleanText(doc.displayName),
  bodyTypeKey: cleanKey(doc.bodyTypeKey),
  bodyType: cleanText(doc.bodyType),
  minExShowroomPrice: numberValue(doc.minExShowroomPrice),
  maxExShowroomPrice: numberValue(doc.maxExShowroomPrice),
  minOnRoadPrice: numberValue(doc.minOnRoadPrice),
  maxOnRoadPrice: numberValue(doc.maxOnRoadPrice),
  fuels: uniqueClean(doc.fuels),
  transmissions: uniqueClean(doc.transmissions),
  variantCount: numberValue(doc.variantCount),
  variantsPreview: uniqueClean(doc.variantsPreview).slice(0, 8),
  citySlug: cleanKey(doc.citySlug),
});

const pickBestModelRows = (sourceDocs = []) => {
  const grouped = new Map();

  for (const raw of sourceDocs) {
    const doc = toModelDoc(raw);
    if (!doc.makeKey || !doc.modelKey || !doc.displayName) continue;
    if (!doc.minExShowroomPrice && !doc.maxExShowroomPrice) continue;

    const key = `${doc.makeKey}__${doc.modelKey}`;
    const current = grouped.get(key);
    if (!current) {
      grouped.set(key, doc);
      continue;
    }

    const currentScore =
      cityRank(current.citySlug) * 1000 +
      numberValue(current.variantCount) * 10 +
      (current.bodyTypeKey ? 1 : 0);
    const nextScore =
      cityRank(doc.citySlug) * 1000 +
      numberValue(doc.variantCount) * 10 +
      (doc.bodyTypeKey ? 1 : 0);

    if (nextScore > currentScore) grouped.set(key, doc);
  }

  return [...grouped.values()].sort((a, b) =>
    a.displayName.localeCompare(b.displayName),
  );
};

const relationTypeFor = ({
  anchor = {},
  candidate = {},
  priceOverlap = 0,
  priceMidCloseness = 0,
  fuelOverlap = 0,
  transmissionOverlap = 0,
} = {}) => {
  const anchorMid = midpoint(anchor.minExShowroomPrice, anchor.maxExShowroomPrice);
  const candidateMid = midpoint(candidate.minExShowroomPrice, candidate.maxExShowroomPrice);
  const ratio = priceMidRatio(anchor, candidate);
  const sameBody = Boolean(
    anchor.bodyTypeKey &&
      candidate.bodyTypeKey &&
      anchor.bodyTypeKey === candidate.bodyTypeKey,
  );
  const platformTwin = isPlatformTwin({
    anchor,
    candidate,
    priceOverlap,
    priceMidCloseness,
    fuelOverlap,
    transmissionOverlap,
  });

  if (isPowertrainShift(anchor, candidate)) return 'powertrain_shift';
  if (sameBody && hasCrossoverCue(anchor) !== hasCrossoverCue(candidate)) {
    return 'adjacent_crossover';
  }
  if (platformTwin) return 'platform_twin';
  if (sameBody && anchorMid && candidateMid && candidateMid < anchorMid * 0.92 && priceOverlap < 0.6) {
    return 'cheaper_step_down';
  }
  if (anchorMid && candidateMid && candidateMid >= anchorMid * 1.15) {
    return 'premium_step_up';
  }

  if (sameBody && ratio >= 0.65 && ratio <= 1.55 && priceOverlap >= 0.35) {
    return 'direct_rival';
  }

  if (anchorMid && candidateMid) {
    if (candidateMid < anchorMid * 0.88) return 'cheaper_step_down';
    if (candidateMid > anchorMid * 1.15) return 'premium_step_up';
  }

  return 'nearby_alternative';
};

const scoreCandidate = ({ anchor = {}, candidate = {} } = {}) => {
  const sameBody = Boolean(
    anchor.bodyTypeKey &&
      candidate.bodyTypeKey &&
      anchor.bodyTypeKey === candidate.bodyTypeKey,
  );
  const priceOverlap = rangeOverlapRatio(
    anchor.minExShowroomPrice,
    anchor.maxExShowroomPrice,
    candidate.minExShowroomPrice,
    candidate.maxExShowroomPrice,
  );
  const anchorMid = midpoint(anchor.minExShowroomPrice, anchor.maxExShowroomPrice);
  const candidateMid = midpoint(candidate.minExShowroomPrice, candidate.maxExShowroomPrice);
  const priceMidCloseness =
    anchorMid && candidateMid
      ? Math.max(0, 1 - Math.abs(anchorMid - candidateMid) / Math.max(anchorMid, candidateMid))
      : 0;
  const fuelOverlap = overlapRatio(anchor.fuels, candidate.fuels);
  const transmissionOverlap = overlapRatio(anchor.transmissions, candidate.transmissions);
  const variantSignal = Math.min(1, numberValue(candidate.variantCount) / 8);

  const rawScore =
    (sameBody ? 30 : 0) +
    priceOverlap * 30 +
    priceMidCloseness * 20 +
    fuelOverlap * 8 +
    transmissionOverlap * 7 +
    variantSignal * 5;

  const relationType = relationTypeFor({
    anchor,
    candidate,
    priceOverlap,
    priceMidCloseness,
    fuelOverlap,
    transmissionOverlap,
  });
  const reasons = [];

  if (relationType === 'platform_twin') reasons.push('Very similar price band and configuration spread');
  if (relationType === 'adjacent_crossover') reasons.push('Adjacent crossover-style alternative');
  if (relationType === 'powertrain_shift') reasons.push('Electric/non-electric powertrain shift');
  if (sameBody) reasons.push(`Same body type: ${candidate.bodyType || candidate.bodyTypeKey}`);
  if (priceOverlap >= 0.15) reasons.push(`Overlapping ex-showroom price band (${Math.round(priceOverlap * 100)}%)`);
  if (priceMidCloseness >= 0.7) reasons.push('Nearby ex-showroom price midpoint');
  if (fuelOverlap > 0) reasons.push(`Fuel overlap: ${anchor.fuels.filter((fuel) => candidate.fuels.includes(fuel)).join(', ')}`);
  if (transmissionOverlap > 0) {
    reasons.push(`Transmission overlap: ${anchor.transmissions.filter((item) => candidate.transmissions.includes(item)).join(', ')}`);
  }
  if (candidate.variantCount >= 4) reasons.push(`${candidate.variantCount} current variants available`);
  if (relationType === 'cheaper_step_down') reasons.push('Budget step-down from anchor price band');
  if (relationType === 'premium_step_up') reasons.push('Premium step-up from anchor price band');

  return {
    similarityScore: Number(Math.max(0, Math.min(100, rawScore)).toFixed(1)),
    relationType,
    reasons: reasons.slice(0, 6),
  };
};

const diversifySimilarModels = (ranked = [], limit = MAX_SIMILAR_MODELS) => {
  const selected = [];
  const overflow = [];
  const makeCounts = new Map();

  for (const item of ranked) {
    const count = makeCounts.get(item.makeKey) || 0;
    if (count < 2 || selected.length < 5) {
      selected.push(item);
      makeCounts.set(item.makeKey, count + 1);
    } else {
      overflow.push(item);
    }
    if (selected.length >= limit) break;
  }

  for (const item of overflow) {
    if (selected.length >= limit) break;
    if (selected.some((existing) => existing.modelKey === item.modelKey && existing.makeKey === item.makeKey)) {
      continue;
    }
    selected.push(item);
  }

  return selected.slice(0, limit);
};

const buildGraphDoc = ({ anchor = {}, models = [], now = new Date() } = {}) => {
  const ranked = models
    .filter((candidate) =>
      candidate.makeKey !== anchor.makeKey || candidate.modelKey !== anchor.modelKey,
    )
    .map((candidate) => {
      const scored = scoreCandidate({ anchor, candidate });
      return {
        makeKey: candidate.makeKey,
        modelKey: candidate.modelKey,
        displayName: candidate.displayName,
        bodyTypeKey: candidate.bodyTypeKey,
        bodyType: candidate.bodyType,
        minExShowroomPrice: candidate.minExShowroomPrice,
        maxExShowroomPrice: candidate.maxExShowroomPrice,
        fuels: candidate.fuels,
        transmissions: candidate.transmissions,
        similarityScore: scored.similarityScore,
        relationType: scored.relationType,
        reasons: scored.reasons,
      };
    })
    .filter((candidate) => candidate.similarityScore >= 35 && candidate.reasons.length)
    .sort((a, b) => b.similarityScore - a.similarityScore || a.displayName.localeCompare(b.displayName));

  const similarModels = diversifySimilarModels(ranked);

  return {
    graphVersion: GRAPH_VERSION,
    formulaVersion: FORMULA_VERSION,
    sourceCollection: SOURCE_COLLECTION,
    anchor: {
      makeKey: anchor.makeKey,
      modelKey: anchor.modelKey,
      displayName: anchor.displayName,
      bodyTypeKey: anchor.bodyTypeKey,
      bodyType: anchor.bodyType,
      minExShowroomPrice: anchor.minExShowroomPrice,
      maxExShowroomPrice: anchor.maxExShowroomPrice,
      fuels: anchor.fuels,
      transmissions: anchor.transmissions,
    },
    similarModels,
    coverage: {
      sourceModelsConsidered: models.length,
      similarModelCount: similarModels.length,
      zeroSimilar: similarModels.length === 0,
    },
    createdAt: now,
    updatedAt: now,
  };
};

const summarize = ({ sourceDocs = [], models = [], graphDocs = [] } = {}) => {
  const similarCounts = graphDocs.map((doc) => doc.similarModels.length);
  const totalSimilar = similarCounts.reduce((sum, count) => sum + count, 0);
  const zeroSimilarAnchors = graphDocs
    .filter((doc) => doc.similarModels.length === 0)
    .map((doc) => doc.anchor.displayName);
  const balenoGraph = graphDocs.find((doc) => doc.anchor.modelKey === 'baleno');

  return {
    mode: WRITE ? 'WRITE' : 'DRY_RUN',
    reset: RESET,
    sourceCollection: SOURCE_COLLECTION,
    targetCollection: TARGET_COLLECTION,
    graphVersion: GRAPH_VERSION,
    formulaVersion: FORMULA_VERSION,
    sourceRows: sourceDocs.length,
    sourceModels: models.length,
    graphDocs: graphDocs.length,
    averageSimilarCount: graphDocs.length ? Number((totalSimilar / graphDocs.length).toFixed(2)) : 0,
    zeroSimilarAnchorCount: zeroSimilarAnchors.length,
    zeroSimilarAnchors: zeroSimilarAnchors.slice(0, 20),
    sampleBalenoGraph: balenoGraph
      ? {
          anchor: balenoGraph.anchor,
          similarModels: balenoGraph.similarModels.slice(0, 8),
        }
      : null,
  };
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const sourceCol = db.collection(SOURCE_COLLECTION);
  const targetCol = db.collection(TARGET_COLLECTION);
  const now = new Date();

  console.error(`[load] source=${SOURCE_COLLECTION}`);
  const sourceDocs = await sourceCol
    .find(
      {},
      {
        projection: {
          _id: 0,
          makeKey: 1,
          modelKey: 1,
          displayName: 1,
          bodyType: 1,
          bodyTypeKey: 1,
          minExShowroomPrice: 1,
          maxExShowroomPrice: 1,
          minOnRoadPrice: 1,
          maxOnRoadPrice: 1,
          fuels: 1,
          transmissions: 1,
          variantCount: 1,
          variantsPreview: 1,
          citySlug: 1,
        },
      },
    )
    .maxTimeMS(120000)
    .toArray();

  const models = pickBestModelRows(sourceDocs);
  const graphDocs = models.map((anchor) => buildGraphDoc({ anchor, models, now }));

  let writeResult = null;

  if (WRITE) {
    if (RESET) {
      const deleted = await targetCol.deleteMany({});
      console.error(`[write] deleted=${deleted.deletedCount || 0}`);
    }

    if (graphDocs.length) {
      const result = await targetCol.bulkWrite(
        graphDocs.map((doc) => ({
          replaceOne: {
            filter: {
              graphVersion: GRAPH_VERSION,
              'anchor.makeKey': doc.anchor.makeKey,
              'anchor.modelKey': doc.anchor.modelKey,
            },
            replacement: doc,
            upsert: true,
          },
        })),
        { ordered: false, writeConcern: { w: 1 } },
      );

      await targetCol.createIndex({ 'anchor.modelKey': 1 }, { name: 'anchor_model_key_idx' });
      await targetCol.createIndex(
        { 'anchor.makeKey': 1, 'anchor.modelKey': 1 },
        { name: 'anchor_make_model_idx' },
      );
      await targetCol.createIndex({ graphVersion: 1 }, { name: 'graph_version_idx' });

      writeResult = {
        matched: result.matchedCount || 0,
        modified: result.modifiedCount || 0,
        upserted: result.upsertedCount || 0,
      };
    }
  }

  console.log(JSON.stringify({
    ...summarize({ sourceDocs, models, graphDocs }),
    writeResult,
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
