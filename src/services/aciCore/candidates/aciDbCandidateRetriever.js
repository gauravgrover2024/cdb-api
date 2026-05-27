'use strict';

/**
 * ACI DB-backed Candidate Retriever V1
 *
 * Purpose:
 * Raw message + active context → candidate snapshot.
 *
 * This is not the final parser.
 * This only retrieves possible entities/features/filters from DB-backed catalogs/indexes
 * so Gemini/deterministic parsers do not parse blindly.
 *
 * Rules:
 * - No factual car availability is decided here.
 * - No answer is composed here.
 * - No live chat routing is touched here.
 * - Small vocabulary detection is allowed only for language/filter interpretation.
 */

import mongoose from 'mongoose';

import {
  findColorMatches,
  findModelMatches,
  findVariantMatches,
  getVehicleEntityIndex,
} from '../../aiAgent/aiAgent.vehicleEntityIndex.js';

import {
  CANDIDATE_SOURCE_TYPES,
  createCandidateItem,
  createEmptyCandidateSnapshot,
  assertCandidateSnapshotShape,
} from './aciCandidateSnapshot.schema.js';

import {
  normalizeFeatureKey,
} from '../taxonomy/aciCanonicalFeature.schema.js';

const CACHE_TTL_MS = Number(process.env.ACI_CANDIDATE_CACHE_TTL_MS || 5 * 60 * 1000);

let featureCatalogCache = {
  builtAt: 0,
  items: [],
};

let makeCatalogCache = {
  builtAt: 0,
  items: [],
};

let priceVariantCatalogCache = {
  builtAt: 0,
  items: [],
};

const clean = (value = '') =>
  String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const uniqueBy = (items = [], keyFn) => {
  const seen = new Set();
  const output = [];

  for (const item of items) {
    const key = keyFn(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(item);
  }

  return output;
};

const isTokenSubset = (small = [], large = []) => {
  const largeSet = new Set(large);
  return small.every((token) => largeSet.has(token));
};

const getVariantSpecificityTokens = (item = {}) => {
  if (Array.isArray(item.variantTokens) && item.variantTokens.length) {
    return item.variantTokens.filter(Boolean);
  }

  return clean(item.variant || item.fullVariant || item.displayName || '')
    .split(' ')
    .filter(Boolean);
};

const pruneVariantMatchesByTokenSpecificity = (matches = []) => {
  const uniqueMatches = uniqueBy(matches, (item) => `${item.modelKey || ''}:${item.variantKey || item.displayName || ''}`);

  return uniqueMatches.filter((candidate, index) => {
    const candidateTokens = getVariantSpecificityTokens(candidate);
    if (!candidateTokens.length) return true;

    return !uniqueMatches.some((other, otherIndex) => {
      if (index === otherIndex) return false;
      if ((other.modelKey || '') !== (candidate.modelKey || '')) return false;

      const otherTokens = getVariantSpecificityTokens(other);
      if (otherTokens.length <= candidateTokens.length) return false;

      return isTokenSubset(candidateTokens, otherTokens);
    });
  });
};

const hasWord = (normalizedMessage, term) => {
  const cleaned = clean(term);
  if (!cleaned) return false;

  if (cleaned.length <= 2) {
    return normalizedMessage.split(' ').includes(cleaned);
  }

  const boundary = new RegExp(`(^|\\s)${cleaned.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(\\s|$)`);
  return boundary.test(normalizedMessage) || normalizedMessage.includes(cleaned);
};

const extractBudgetCandidates = (message = '') => {
  const candidates = [];
  const text = String(message || '');

  const patterns = [
    {
      relation: 'max',
      regex: /\b(?:under|below|less than|upto|up to|andar|ke andar)\s*₹?\s*(\d+(?:\.\d+)?)\s*(lakh|lakhs|lac|l|cr|crore)?\b/gi,
    },
    {
      relation: 'max',
      regex: /\b₹?\s*(\d+(?:\.\d+)?)\s*(lakh|lakhs|lac|l|cr|crore)\s*(?:ke andar|tak|budget)\b/gi,
    },
    {
      relation: 'min',
      regex: /\b(?:above|over|more than|starting from)\s*₹?\s*(\d+(?:\.\d+)?)\s*(lakh|lakhs|lac|l|cr|crore)?\b/gi,
    },
  ];

  const toAmount = (num, unit = '') => {
    const value = Number(num);
    const normalizedUnit = clean(unit);

    if (!Number.isFinite(value)) return null;
    if (['cr', 'crore'].includes(normalizedUnit)) return Math.round(value * 10000000);
    if (['lakh', 'lakhs', 'lac', 'l'].includes(normalizedUnit)) return Math.round(value * 100000);
    return value;
  };

  for (const pattern of patterns) {
    for (const match of text.matchAll(pattern.regex)) {
      const amount = toAmount(match[1], match[2]);
      if (!amount) continue;

      candidates.push(createCandidateItem({
        rawText: match[0],
        canonicalKey: `${pattern.relation}_budget`,
        displayName: `${pattern.relation} ₹${amount}`,
        type: 'budget',
        source: CANDIDATE_SOURCE_TYPES.USER_TEXT,
        confidence: 0.8,
        metadata: {
          relation: pattern.relation,
          amount,
          currency: 'INR',
        },
      }));
    }
  }

  return candidates;
};

const LANGUAGE_FILTERS = [
  { type: 'fuelType', canonicalKey: 'cng', terms: ['cng'] },
  { type: 'fuelType', canonicalKey: 'petrol', terms: ['petrol'] },
  { type: 'fuelType', canonicalKey: 'diesel', terms: ['diesel'] },
  { type: 'fuelType', canonicalKey: 'electric', terms: ['electric', 'ev'] },
  { type: 'fuelType', canonicalKey: 'hybrid', terms: ['hybrid'] },

  { type: 'transmission', canonicalKey: 'automatic', terms: ['automatic', 'auto', 'amt', 'dct', 'ivt', 'cvt', 'at'] },
  { type: 'transmission', canonicalKey: 'manual', terms: ['manual', 'mt'] },

  { type: 'bodyType', canonicalKey: 'suv', terms: ['suv', 'suvs'] },
  { type: 'bodyType', canonicalKey: 'sedan', terms: ['sedan', 'sedans'] },
  { type: 'bodyType', canonicalKey: 'hatchback', terms: ['hatchback', 'hatchbacks'] },
  { type: 'bodyType', canonicalKey: 'mpv', terms: ['mpv', 'muv', '7 seater', 'seven seater'] },
];

const TASK_HINTS = [
  { task: 'price_lookup', terms: ['price', 'ex showroom', 'ex-showroom', 'kitne ka', 'kitna hoga'] },
  { task: 'on_road_estimate', terms: ['on road', 'on-road', 'total on road', 'kitne ka padega'] },
  { task: 'emi_calculation', terms: ['emi', 'down payment', 'loan'] },
  { task: 'color_lookup', terms: ['color', 'colour', 'black', 'white', 'red', 'dual tone'] },
  { task: 'feature_answer', terms: ['feature', 'sunroof', 'abs', 'adas', 'airbag', 'airbags', 'range', 'mileage'] },
  { task: 'vehicle_comparison', terms: ['compare', 'vs', 'versus', 'better than', 'difference between'] },
  { task: 'vehicle_discovery', terms: ['cars with', 'cars under', 'show cars', 'hyundai cars', 'tata cars'] },
  { task: 'quotation', terms: ['quote', 'quotation', 'best price'] },
  { task: 'offer_lookup', terms: ['offer', 'discount', 'bonus'] },
  { task: 'waiting_period', terms: ['waiting', 'delivery time', 'kitne din', 'immediate delivery'] },
  { task: 'safest_vehicles', terms: ['safest', 'safety rating', 'ncap', 'crash test'] },
];

const extractLanguageFilterCandidates = (message = '') => {
  const normalized = clean(message);
  const candidates = [];

  for (const item of LANGUAGE_FILTERS) {
    const matchedTerm = item.terms.find((term) => hasWord(normalized, term));
    if (!matchedTerm) continue;

    candidates.push(createCandidateItem({
      rawText: matchedTerm,
      canonicalKey: item.canonicalKey,
      displayName: item.canonicalKey,
      type: item.type,
      source: CANDIDATE_SOURCE_TYPES.STATIC_LANGUAGE_OPERATOR,
      confidence: 0.75,
    }));
  }

  return candidates;
};

const ADAS_CHILD_TERMS = [
  'lane keep assist',
  'lane keeping assist',
  'forward collision warning',
  'blind spot monitor',
  'adaptive high beam',
  'lane departure warning',
  'adaptive cruise control',
  'driver attention warning',
  'rear cross traffic alert',
  'automatic emergency braking',
  'aeb',
];

const hasExplicitAdasChildTerm = (normalizedMessage = '') =>
  ADAS_CHILD_TERMS.some((term) => normalizedMessage.includes(clean(term)));

const ADAS_CHILD_CANONICAL_KEYS = new Set([
  'lane_keep_assist',
  'forward_collision_warning',
  'blind_spot_monitor',
  'adaptive_high_beam_assist',
  'lane_departure_warning',
  'adaptive_cruise_control',
  'driver_attention_warning',
  'rear_cross_traffic_alert',
  'automatic_emergency_braking',
]);

const hasAbsToken = (normalizedMessage = '') =>
  /(^|\s)abs(\s|$)/i.test(String(normalizedMessage || ''));

const postProcessFeatureCandidates = ({ candidates = [], normalizedMessage = '' } = {}) => {
  let output = Array.isArray(candidates) ? [...candidates] : [];

  const mentionsAdas = /(^|\s)adas(\s|$)/i.test(normalizedMessage);
  const mentionsExplicitAdasChild = hasExplicitAdasChildTerm(normalizedMessage);

  if (mentionsAdas && !mentionsExplicitAdasChild) {
    output = output.filter((candidate) => (
      candidate.canonicalKey === 'adas_package' ||
      !ADAS_CHILD_CANONICAL_KEYS.has(candidate.canonicalKey)
    ));
  }

  if (hasAbsToken(normalizedMessage) && !output.some((item) => item.canonicalKey === 'anti_lock_braking_system')) {
    output.push(createCandidateItem({
      rawText: 'ABS',
      canonicalKey: 'anti_lock_braking_system',
      displayName: 'Anti-lock Braking System (ABS)',
      type: 'feature',
      source: CANDIDATE_SOURCE_TYPES.STATIC_LANGUAGE_OPERATOR,
      confidence: 0.92,
      metadata: {
        reason: 'common_feature_abbreviation_post_process',
      },
    }));
  }

  return uniqueBy(
    output.sort((a, b) => (b.confidence || 0) - (a.confidence || 0)),
    (item) => item.canonicalKey,
  ).slice(0, 8);
};

const hasComparisonLanguage = (message = '') => {
  const normalized = clean(message);
  return (
    normalized.includes(' vs ') ||
    normalized.includes(' versus ') ||
    normalized.includes(' compare ') ||
    normalized.includes(' compared ') ||
    normalized.includes(' better ') ||
    normalized.includes(' between ') ||
    normalized.includes(' and ')
  );
};

const extractTaskHintCandidates = (message = '') => {
  const normalized = clean(message);
  const candidates = [];

  for (const item of TASK_HINTS) {
    const matchedTerm = item.terms.find((term) => normalized.includes(clean(term)));
    if (!matchedTerm) continue;

    candidates.push(createCandidateItem({
      rawText: matchedTerm,
      canonicalKey: item.task,
      displayName: item.task,
      type: 'task',
      source: CANDIDATE_SOURCE_TYPES.STATIC_LANGUAGE_OPERATOR,
      confidence: 0.65,
    }));
  }

  return candidates;
};

const getDb = () => {
  if (mongoose.connection?.readyState !== 1 || !mongoose.connection?.db) {
    return null;
  }

  return mongoose.connection.db;
};

const loadMakeCatalog = async () => {
  const now = Date.now();

  if (makeCatalogCache.items.length && now - makeCatalogCache.builtAt < CACHE_TTL_MS) {
    return makeCatalogCache.items;
  }

  const db = getDb();
  if (!db) return [];

  const docs = await db.collection('aci_vehicle_model_summary')
    .find({})
    .project({
      _id: 0,
      make: 1,
      brand: 1,
      makeKey: 1,
      citySlug: 1,
    })
    .toArray();

  const items = uniqueBy(
    docs.map((doc) => {
      const displayName = doc.make || doc.brand || doc.makeKey || null;
      const canonicalKey = doc.makeKey || normalizeFeatureKey(displayName || '');
      return {
        canonicalKey,
        displayName,
        aliases: [displayName, doc.makeKey].filter(Boolean),
      };
    }).filter((item) => item.canonicalKey && item.displayName),
    (item) => item.canonicalKey,
  );

  makeCatalogCache = {
    builtAt: now,
    items,
  };

  return items;
};

const findMakeCandidates = async (message = '') => {
  const normalized = clean(message);
  if (!normalized) return [];

  const makes = await loadMakeCatalog();
  const candidates = [];

  for (const make of makes) {
    const matchedAlias = make.aliases.find((alias) => hasWord(normalized, alias));
    if (!matchedAlias) continue;

    candidates.push(createCandidateItem({
      rawText: matchedAlias,
      canonicalKey: make.canonicalKey,
      displayName: make.displayName,
      type: 'make',
      source: CANDIDATE_SOURCE_TYPES.DB,
      confidence: 0.82,
    }));
  }

  return uniqueBy(candidates, (item) => item.canonicalKey).slice(0, 8);
};

const loadFeatureCatalog = async () => {
  const now = Date.now();

  if (featureCatalogCache.items.length && now - featureCatalogCache.builtAt < CACHE_TTL_MS) {
    return featureCatalogCache.items;
  }

  const db = getDb();
  if (!db) return [];

  const docs = await db.collection('vehicle_feature_catalog_v2')
    .find({})
    .project({
      _id: 0,
      canonicalKey: 1,
      key: 1,
      displayName: 1,
      aliases: 1,
      groupKey: 1,
      category: 1,
    })
    .toArray();

  const items = docs.map((doc) => {
    const canonicalKey = normalizeFeatureKey(doc.canonicalKey || doc.key || doc.displayName || '');
    const aliases = [
      doc.displayName,
      doc.canonicalKey,
      doc.key,
      ...(Array.isArray(doc.aliases) ? doc.aliases : []),
    ]
      .filter(Boolean)
      .map((item) => String(item).trim())
      .filter(Boolean);

    return {
      canonicalKey,
      displayName: doc.displayName || doc.canonicalKey || doc.key || canonicalKey,
      aliases: uniqueBy(aliases, (item) => clean(item)),
      groupKey: doc.groupKey || null,
      category: doc.category || null,
    };
  }).filter((item) => item.canonicalKey && item.aliases.length);

  featureCatalogCache = {
    builtAt: now,
    items,
  };

  return items;
};

const scoreFeatureAliasMatch = ({ normalizedMessage, alias = '', feature }) => {
  const aliasClean = clean(alias);
  if (!aliasClean || aliasClean.length < 3) return null;

  if (!hasWord(normalizedMessage, aliasClean)) return null;

  const canonicalKey = feature.canonicalKey || '';
  const canonicalClean = clean(canonicalKey.replace(/_/g, ' '));
  const displayClean = clean(feature.displayName || '');

  let score = 0.72;

  if (normalizedMessage.split(' ').includes(aliasClean)) score += 0.1;
  if (aliasClean === canonicalClean || aliasClean === displayClean) score += 0.08;

  // Avoid broad parent/child feature explosion.
  // If user says only "ADAS", prefer the ADAS package candidate, not every ADAS child feature.
  if (normalizedMessage.includes('adas') && !hasExplicitAdasChildTerm(normalizedMessage)) {
    if (canonicalKey === 'adas_package' || aliasClean === 'adas') score += 0.22;
    else if (
      [
        'lane_keep_assist',
        'forward_collision_warning',
        'blind_spot_monitor',
        'adaptive_high_beam_assist',
        'lane_departure_warning',
        'adaptive_cruise_control',
        'driver_attention_warning',
        'rear_cross_traffic_alert',
      ].includes(canonicalKey)
    ) {
      return null;
    }
  }

  // If user says plain "sunroof", prefer canonical sunroof over voice/panoramic child features
  // unless those child terms are explicitly mentioned.
  if (normalizedMessage.includes('sunroof')) {
    const explicitVoice = normalizedMessage.includes('voice');
    const explicitPanoramic = normalizedMessage.includes('panoramic');

    if (canonicalKey === 'sunroof') score += 0.18;

    if (!explicitVoice && canonicalKey === 'voice_assisted_sunroof') return null;
    if (!explicitPanoramic && canonicalKey === 'panoramic_sunroof') return null;
  }

  return {
    alias,
    score: Math.max(0, Math.min(1, score)),
  };
};


const loadPriceVariantCatalog = async () => {
  const now = Date.now();

  if (priceVariantCatalogCache.items.length && now - priceVariantCatalogCache.builtAt < CACHE_TTL_MS) {
    return priceVariantCatalogCache.items;
  }

  const db = getDb();
  if (!db) return [];

  const docs = await db.collection('aci_vehicle_price_rows')
    .find({})
    .project({
      _id: 0,
      make: 1,
      brand: 1,
      model: 1,
      variant: 1,
      fullModel: 1,
      fullVariant: 1,
      makeKey: 1,
      modelKey: 1,
      variantKey: 1,
      citySlug: 1,
    })
    .toArray();

  const items = docs
    .map((doc) => {
      const modelKey = doc.modelKey || normalizeFeatureKey(doc.fullModel || doc.model || '');
      const variantKey = doc.variantKey || normalizeFeatureKey(doc.fullVariant || doc.variant || '');
      const variantName = doc.variant || doc.fullVariant || '';
      const modelName = doc.model || doc.fullModel || '';
      const fullVariant = doc.fullVariant || [doc.make || doc.brand, doc.model, doc.variant].filter(Boolean).join(' ');

      const makeName = doc.make || doc.brand || null;
      const fullModel = doc.fullModel || [makeName, doc.model].filter(Boolean).join(' ');
      const makeModelTokens = new Set(
        clean([makeName, modelName, fullModel].filter(Boolean).join(' '))
          .split(' ')
          .filter(Boolean),
      );

      const variantTokens = clean(variantName)
        .split(' ')
        .filter((token) => token.length >= 2)
        .filter((token) => !makeModelTokens.has(token));

      const fullVariantTokens = clean(fullVariant)
        .split(' ')
        .filter((token) => token.length >= 2)
        .filter((token) => !makeModelTokens.has(token));

      return {
        make: makeName,
        model: modelName,
        variant: variantName,
        fullModel,
        fullVariant,
        modelKey,
        variantKey,
        displayName: fullVariant,
        citySlug: doc.citySlug || null,
        variantTokens: variantTokens.length ? variantTokens : fullVariantTokens,
      };
    })
    .filter((item) => item.modelKey && item.variantKey && item.variantTokens.length);

  priceVariantCatalogCache = {
    builtAt: now,
    items,
  };

  return items;
};

const findModelScopedVariantCandidates = async ({ message = '', modelCandidates = [] } = {}) => {
  const normalized = clean(message);
  if (!normalized || !Array.isArray(modelCandidates) || !modelCandidates.length) return [];

  const catalog = await loadPriceVariantCatalog();

  const modelKeys = new Set(modelCandidates.map((item) => item.canonicalKey).filter(Boolean));
  const modelNames = new Set(
    modelCandidates
      .flatMap((item) => [
        item.metadata?.model,
        item.metadata?.fullModel,
        item.displayName,
      ])
      .filter(Boolean)
      .map(clean),
  );

  const matches = [];

  for (const item of catalog) {
    const itemModelName = clean(item.model || item.fullModel || '');
    const itemFullModelName = clean(item.fullModel || item.model || '');
    const itemModelKey = item.modelKey || '';

    // Exact model scoping only.
    // Do not allow "Honda City Hybrid" to match "Honda City" just because it includes the phrase.
    // Candidate/model keys are DB-backed, so exact key/name matching is the safe path here.
    const modelMatches = (
      modelKeys.has(itemModelKey) ||
      modelNames.has(itemModelName) ||
      modelNames.has(itemFullModelName)
    );

    if (!modelMatches) continue;

    const variantTokensPresent = item.variantTokens.every((token) => normalized.split(' ').includes(token));
    if (!variantTokensPresent) continue;

    matches.push({
      ...item,
      matchedAlias: item.variant,
      score: 96,
    });
  }

  return pruneVariantMatchesByTokenSpecificity(
    uniqueBy(matches, (item) => `${item.modelKey}:${item.variantKey}`),
  ).slice(0, 12);
};

const findFeatureCandidates = async (message = '') => {
  const normalized = clean(message);
  if (!normalized) return [];

  const words = normalized.split(' ').filter(Boolean);
  const wordSet = new Set(words);

  const mentionsAbs = wordSet.has('abs');
  const mentionsAdas = wordSet.has('adas');
  const mentionsSunroof = wordSet.has('sunroof');
  const mentionsVoice = wordSet.has('voice');
  const mentionsPanoramic = wordSet.has('panoramic');

  const explicitAdasChildTerms = [
    'lane keep assist',
    'lane keeping assist',
    'forward collision warning',
    'blind spot',
    'blind spot monitor',
    'adaptive high beam',
    'lane departure',
    'lane departure warning',
    'adaptive cruise',
    'adaptive cruise control',
    'driver attention',
    'rear cross traffic',
    'automatic emergency braking',
    'traffic sign recognition',
    'speed assist',
    'autonomous parking',
    'parking assist',
    'self parking',
    'aeb',
  ];

  const hasExplicitAdasChild = explicitAdasChildTerms.some((term) =>
    normalized.includes(clean(term)),
  );

  const adasChildIndicators = [
    'lane',
    'collision',
    'blind_spot',
    'blind spot',
    'traffic',
    'adaptive',
    'departure',
    'warning',
    'speed_assist',
    'speed assist',
    'autonomous_parking',
    'autonomous parking',
    'parking_assist',
    'parking assist',
    'self parking',
    'driver_attention',
    'driver attention',
    'cross_traffic',
    'cross traffic',
    'avoidance',
    'emergency_braking',
    'emergency braking',
  ];

  const isAdasChildCandidate = (feature = {}) => {
    const text = clean([
      feature.canonicalKey || '',
      feature.displayName || '',
      feature.groupKey || '',
      feature.category || '',
      ...(Array.isArray(feature.aliases) ? feature.aliases : []),
    ].join(' '));

    return adasChildIndicators.some((indicator) => text.includes(clean(indicator)));
  };

  const catalog = await loadFeatureCatalog();
  const scored = [];

  for (const feature of catalog) {
    const canonicalKey = feature.canonicalKey || '';
    const aliasList = Array.isArray(feature.aliases) ? feature.aliases : [];

    // If user says plain ADAS, keep package-level ADAS only.
    // Do not flood candidates with individual ADAS child features.
    if (mentionsAdas && !hasExplicitAdasChild && canonicalKey !== 'adas_package' && isAdasChildCandidate(feature)) {
      continue;
    }

    // If user says plain sunroof, do not infer voice/panoramic subfeatures.
    if (mentionsSunroof && !mentionsVoice && canonicalKey === 'voice_assisted_sunroof') {
      continue;
    }
    if (mentionsSunroof && !mentionsPanoramic && canonicalKey === 'panoramic_sunroof') {
      continue;
    }

    let best = null;

    for (const alias of aliasList) {
      const aliasClean = clean(alias);
      if (!aliasClean || aliasClean.length < 3) continue;

      if (!hasWord(normalized, aliasClean)) continue;

      let score = 0.72;

      if (wordSet.has(aliasClean)) score += 0.1;
      if (aliasClean === clean(canonicalKey.replace(/_/g, ' '))) score += 0.08;
      if (aliasClean === clean(feature.displayName || '')) score += 0.08;

      if (mentionsAdas && canonicalKey === 'adas_package') score += 0.24;
      if (mentionsSunroof && canonicalKey === 'sunroof') score += 0.18;

      const match = {
        alias,
        score: Math.max(0, Math.min(1, score)),
      };

      if (!best || match.score > best.score) {
        best = match;
      }
    }

    if (!best || best.score < 0.55) continue;

    scored.push(createCandidateItem({
      rawText: best.alias,
      canonicalKey,
      displayName: feature.displayName,
      type: 'feature',
      source: CANDIDATE_SOURCE_TYPES.DB,
      confidence: best.score,
      metadata: {
        groupKey: feature.groupKey,
        category: feature.category,
      },
    }));
  }

  // Force common short automotive feature abbreviations when present.
  // These are language operators, not factual availability.
  if (mentionsAbs && !scored.some((item) => item.canonicalKey === 'anti_lock_braking_system')) {
    scored.push(createCandidateItem({
      rawText: 'ABS',
      canonicalKey: 'anti_lock_braking_system',
      displayName: 'Anti-lock Braking System (ABS)',
      type: 'feature',
      source: CANDIDATE_SOURCE_TYPES.STATIC_LANGUAGE_OPERATOR,
      confidence: 0.95,
      metadata: {
        reason: 'common_feature_abbreviation',
      },
    }));
  }

  if (mentionsAdas && !scored.some((item) => item.canonicalKey === 'adas_package')) {
    scored.push(createCandidateItem({
      rawText: 'ADAS',
      canonicalKey: 'adas_package',
      displayName: 'ADAS',
      type: 'feature',
      source: CANDIDATE_SOURCE_TYPES.STATIC_LANGUAGE_OPERATOR,
      confidence: 0.94,
      metadata: {
        reason: 'common_feature_abbreviation',
      },
    }));
  }

  if (mentionsSunroof && !scored.some((item) => item.canonicalKey === 'sunroof')) {
    scored.push(createCandidateItem({
      rawText: 'sunroof',
      canonicalKey: 'sunroof',
      displayName: 'Sunroof',
      type: 'feature',
      source: CANDIDATE_SOURCE_TYPES.STATIC_LANGUAGE_OPERATOR,
      confidence: 0.93,
      metadata: {
        reason: 'common_feature_name',
      },
    }));
  }

  let output = uniqueBy(
    scored.sort((a, b) => (b.confidence || 0) - (a.confidence || 0)),
    (item) => item.canonicalKey,
  );

  // Final guardrail: if the user only said ADAS generally, keep package-level ADAS
  // and remove all child ADAS capability candidates.
  if (mentionsAdas && !hasExplicitAdasChild) {
    output = output.filter((candidate) => {
      if (candidate.canonicalKey === 'adas_package') return true;
      return !isAdasChildCandidate({
        canonicalKey: candidate.canonicalKey,
        displayName: candidate.displayName,
        groupKey: candidate.metadata?.groupKey,
        category: candidate.metadata?.category,
        aliases: [candidate.rawText],
      });
    });
  }

  return output.slice(0, 8);
};

const safeCall = async (fn, fallback = []) => {
  try {
    const value = await fn();
    return Array.isArray(value) ? value : fallback;
  } catch {
    return fallback;
  }
};

const mapModelMatch = (match = {}) => createCandidateItem({
  rawText: match.matchedAlias || match.model || match.displayName || null,
  canonicalKey: match.modelKey || normalizeFeatureKey(match.fullModel || match.model || match.displayName || ''),
  displayName: match.displayName || match.fullModel || match.model || null,
  type: 'model',
  source: CANDIDATE_SOURCE_TYPES.DB,
  confidence: typeof match.score === 'number' ? Math.min(1, Math.max(0, match.score / 100)) : 0.8,
  metadata: {
    make: match.brand || match.make || null,
    model: match.model || null,
    fullModel: match.fullModel || match.displayName || null,
    raw: match,
  },
});

const filterVariantMatchesByModels = (variantMatches = [], modelCandidates = []) => {
  if (!modelCandidates.length) return variantMatches;

  const modelKeys = new Set(modelCandidates.map((item) => item.canonicalKey).filter(Boolean));
  const modelNames = new Set(
    modelCandidates
      .flatMap((item) => [
        item.metadata?.model,
        item.metadata?.fullModel,
        item.displayName,
      ])
      .filter(Boolean)
      .map(clean),
  );

  const scoped = variantMatches.filter((match) => {
    const matchModelKey = match.modelKey || normalizeFeatureKey(match.fullModel || match.model || '');
    const matchModelName = clean(match.model || match.fullModel || match.displayName || '');

    return (
      modelKeys.has(matchModelKey) ||
      modelNames.has(matchModelName) ||
      Array.from(modelNames).some((modelName) => modelName && matchModelName.includes(modelName))
    );
  });

  return scoped.length ? scoped : [];
};

const mapVariantMatch = (match = {}) => createCandidateItem({
  rawText: match.matchedAlias || match.variant || match.displayName || null,
  canonicalKey: match.variantKey || normalizeFeatureKey(match.fullVariant || match.variant || match.displayName || ''),
  displayName: match.displayName || match.fullVariant || match.variant || null,
  type: 'variant',
  source: CANDIDATE_SOURCE_TYPES.DB,
  confidence: typeof match.score === 'number' ? Math.min(1, Math.max(0, match.score / 100)) : 0.75,
  metadata: {
    make: match.brand || match.make || null,
    model: match.model || null,
    variant: match.variant || null,
    fullVariant: match.fullVariant || match.displayName || null,
    raw: match,
  },
});

const mapColorMatch = (match = {}) => createCandidateItem({
  rawText: match.matchedAlias || match.color || match.name || match.displayName || null,
  canonicalKey: normalizeFeatureKey(match.color || match.name || match.displayName || ''),
  displayName: match.displayName || match.color || match.name || null,
  type: 'color',
  source: CANDIDATE_SOURCE_TYPES.DB,
  confidence: typeof match.score === 'number' ? Math.min(1, Math.max(0, match.score / 100)) : 0.7,
  metadata: {
    make: match.brand || match.make || null,
    model: match.model || null,
    raw: match,
  },
});

async function retrieveAciDbCandidates({
  rawMessage = '',
  normalizedMessage = '',
  activeContext = null,
  limits = {},
} = {}) {
  const startedAt = Date.now();
  const message = rawMessage || normalizedMessage || '';
  const normalized = normalizedMessage || String(message || '').trim().replace(/\s+/g, ' ');

  const snapshot = createEmptyCandidateSnapshot({
    rawMessage: message,
    normalizedMessage: normalized,
    activeContext,
  });

  const index = await getVehicleEntityIndex();

  const makeMatches = await findMakeCandidates(message);
  const modelMatches = await safeCall(() => Promise.resolve(findModelMatches(index, message)), []);
  const rawVariantMatches = await safeCall(() => Promise.resolve(findVariantMatches(index, message)), []);
  const colorMatches = await safeCall(() => Promise.resolve(findColorMatches(index, message)), []);
  const featureMatches = await findFeatureCandidates(message);

  const languageFilters = extractLanguageFilterCandidates(message);
  const taskHints = extractTaskHintCandidates(message);
  const budgetCandidates = extractBudgetCandidates(message);

  snapshot.vehicles.makes = makeMatches.slice(0, limits.makes || 8);

  snapshot.vehicles.models = uniqueBy(modelMatches.map(mapModelMatch), (item) => item.canonicalKey)
    .slice(0, limits.models || 8);

  const explicitVariantMatches = await findModelScopedVariantCandidates({
    message,
    modelCandidates: snapshot.vehicles.models,
  });

  const broadScopedVariantMatches = filterVariantMatchesByModels(
    rawVariantMatches,
    snapshot.vehicles.models,
  );

  // If exact model-scoped variant mentions are found, use them and avoid dumping
  // broad variant candidates into the parser snapshot.
  const scopedVariantMatches = explicitVariantMatches.length
    ? explicitVariantMatches
    : broadScopedVariantMatches;

  snapshot.vehicles.variants = uniqueBy(scopedVariantMatches.map(mapVariantMatch), (item) => item.canonicalKey)
    .slice(0, limits.variants || 12);

  snapshot.vehicles.colors = uniqueBy(colorMatches.map(mapColorMatch), (item) => item.canonicalKey)
    .slice(0, limits.colors || 8);

  snapshot.taxonomy.features = featureMatches.slice(0, limits.features || 20);

  snapshot.taxonomy.fuelTypes = languageFilters
    .filter((item) => item.type === 'fuelType')
    .slice(0, limits.fuelTypes || 6);

  snapshot.taxonomy.transmissions = languageFilters
    .filter((item) => item.type === 'transmission')
    .slice(0, limits.transmissions || 6);

  snapshot.taxonomy.bodyTypes = languageFilters
    .filter((item) => item.type === 'bodyType')
    .slice(0, limits.bodyTypes || 6);

  snapshot.commerce.budgets = budgetCandidates.slice(0, limits.budgets || 4);
  const finalTaskHints = [...taskHints];

  if (snapshot.vehicles.models.length >= 2 && hasComparisonLanguage(message)) {
    finalTaskHints.unshift(createCandidateItem({
      rawText: 'multi-vehicle comparison',
      canonicalKey: 'vehicle_comparison',
      displayName: 'vehicle_comparison',
      type: 'task',
      source: CANDIDATE_SOURCE_TYPES.STATIC_LANGUAGE_OPERATOR,
      confidence: 0.86,
      metadata: {
        reason: 'multiple_models_with_comparison_language',
      },
    }));
  }

  snapshot.language.tasks = uniqueBy(finalTaskHints, (item) => item.canonicalKey).slice(0, limits.tasks || 10);

  snapshot.trace = {
    ...snapshot.trace,
    candidateRetriever: 'aciDbCandidateRetriever',
    candidateRetrieverVersion: '0.1.0',
    durationMs: Date.now() - startedAt,
    cache: {
      featureCatalogSize: featureCatalogCache.items.length,
      featureCatalogAgeMs: featureCatalogCache.builtAt ? Date.now() - featureCatalogCache.builtAt : null,
      makeCatalogSize: makeCatalogCache.items.length,
      makeCatalogAgeMs: makeCatalogCache.builtAt ? Date.now() - makeCatalogCache.builtAt : null,
      priceVariantCatalogSize: priceVariantCatalogCache.items.length,
      priceVariantCatalogAgeMs: priceVariantCatalogCache.builtAt ? Date.now() - priceVariantCatalogCache.builtAt : null,
    },
    counts: {
      makes: snapshot.vehicles.makes.length,
      models: snapshot.vehicles.models.length,
      variants: snapshot.vehicles.variants.length,
      colors: snapshot.vehicles.colors.length,
      explicitModelScopedVariants: typeof explicitVariantMatches !== 'undefined' ? explicitVariantMatches.length : 0,
      broadScopedVariants: typeof broadScopedVariantMatches !== 'undefined' ? broadScopedVariantMatches.length : 0,
      features: snapshot.taxonomy.features.length,
      fuelTypes: snapshot.taxonomy.fuelTypes.length,
      transmissions: snapshot.taxonomy.transmissions.length,
      bodyTypes: snapshot.taxonomy.bodyTypes.length,
      budgets: snapshot.commerce.budgets.length,
      tasks: snapshot.language.tasks.length,
    },
  };

  assertCandidateSnapshotShape(snapshot);

  return snapshot;
}

function clearAciCandidateRetrieverCaches() {
  featureCatalogCache = {
    builtAt: 0,
    items: [],
  };
  makeCatalogCache = {
    builtAt: 0,
    items: [],
  };
  priceVariantCatalogCache = {
    builtAt: 0,
    items: [],
  };
}

export {
  retrieveAciDbCandidates,
  clearAciCandidateRetrieverCaches,
};

export default retrieveAciDbCandidates;
