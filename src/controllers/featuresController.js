// src/controllers/featuresController.js
import asyncHandler from "express-async-handler";
import Vehicle from "../models/Vehicle.js";
import VehicleFeature from "../models/VehicleFeature.js";

// Convert { "Category | Name": "Yes" } → [{category,name,value},...]
const objectToFeaturesArray = (featuresObj) => {
  if (!featuresObj || typeof featuresObj !== "object") return [];
  return Object.entries(featuresObj).map(([fullKey, value]) => {
    const [category, ...nameParts] = fullKey.split(" | ");
    return {
      category: category || "Others",
      name: nameParts.join(" | "),
      value: value || "Not Available",
    };
  });
};

// --- NORMALIZERS (same logic as checkFeaturesJoin.js) ---

const escapeRegex = (value) => String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const normalizeBrandForJoin = (rawBrand) => {
  if (!rawBrand) return "";
  let b = String(rawBrand).trim().toLowerCase();

  b = b.replace(/[-_]+/g, " ");
  b = b.replace(/\s+/g, " ").trim();

  const aliases = {
    mercedes: "mercedes benz",
    "mercedes benz": "mercedes benz",
    benz: "mercedes benz",
    maruti: "maruti suzuki",
    "maruti suzuki": "maruti suzuki",
  };

  return aliases[b] || b;
};

const normalizeModelForJoin = (brand, rawModel) => {
  if (!rawModel) return "";
  let m = String(rawModel).trim().toLowerCase();
  const b = normalizeBrandForJoin(brand);

  // unify separators: "grand-vitara" -> "grand vitara"
  m = m.replace(/[-_]+/g, " ");

  // collapse spaces
  m = m.replace(/\s+/g, " ").trim();

  // Strip brand-leading tokens robustly:
  // "maruti swift" with brand "maruti suzuki" -> "swift"
  // "mercedes benz glc" with brand "mercedes benz" -> "glc"
  const brandTokens = b.split(" ").filter(Boolean);
  const modelTokens = m.split(" ").filter(Boolean);
  let idx = 0;
  while (idx < modelTokens.length && brandTokens.includes(modelTokens[idx])) {
    idx += 1;
  }
  if (idx > 0 && idx < modelTokens.length) {
    m = modelTokens.slice(idx).join(" ");
  }

  return m;
};

const normalizeVariantForJoin = (rawVariant, rawBrand = "", rawModel = "") => {
  if (!rawVariant) return "";
  let v = String(rawVariant).trim().toLowerCase();

  v = v
    .replace(/\+/g, " plus ")
    .replace(/[-_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const brandNorm = normalizeBrandForJoin(rawBrand);
  const modelNorm = normalizeModelForJoin(brandNorm, rawModel);
  const prefixes = [
    `${brandNorm} ${modelNorm}`.trim(),
    modelNorm,
    brandNorm,
    String(rawModel || "").toLowerCase().replace(/[-_]+/g, " ").replace(/\s+/g, " ").trim(),
    String(rawBrand || "").toLowerCase().replace(/[-_]+/g, " ").replace(/\s+/g, " ").trim(),
  ].filter(Boolean);

  // Strip repeatedly until no brand/model prefix remains.
  // Example: "Maruti Baleno Delta" -> "Delta"
  let changed = true;
  while (changed) {
    changed = false;
    for (const prefix of prefixes) {
      if (v.startsWith(`${prefix} `)) {
        v = v.slice(prefix.length).trim();
        changed = true;
      }
    }
  }

  // normalize whitespace so long names line up
  v = v.replace(/\s+/g, " ").trim();

  return v;
};

const trimLeading = (value, prefix) => {
  const source = String(value || "").trim();
  const leader = String(prefix || "").trim();
  if (!source || !leader) return source;
  const escaped = leader.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return source.replace(new RegExp(`^${escaped}\\s*`, "i"), "").trim();
};

const presentMake = (rawBrand) => String(rawBrand || "").trim();
const presentModel = (rawBrand, rawModel) => {
  const make = presentMake(rawBrand);
  const model = String(rawModel || "").trim();
  return trimLeading(model, make) || model;
};
const presentVariant = (rawBrand, rawModel, rawVariant) => {
  const make = presentMake(rawBrand);
  const model = presentModel(rawBrand, rawModel);
  const variant = String(rawVariant || "").trim();
  return (
    trimLeading(variant, rawModel) ||
    trimLeading(variant, `${make} ${model}`.trim()) ||
    trimLeading(variant, make) ||
    variant
  );
};

const compactAlphaNum = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");

const normalizeText = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const dedupeStrings = (items = []) =>
  [...new Set(items.map((value) => String(value || "").trim()).filter(Boolean))];

const canonicalVariantForCompare = (rawBrand, rawModel, rawVariant) => {
  const make = presentMake(rawBrand);
  const model = presentModel(rawBrand, rawModel);
  const normalizedVariant = presentVariant(rawBrand, rawModel, rawVariant);

  const candidates = [
    normalizedVariant,
    trimLeading(normalizedVariant, model),
    trimLeading(normalizedVariant, `${make} ${model}`.trim()),
    trimLeading(normalizedVariant, make),
    rawVariant,
  ].filter(Boolean);

  return compactAlphaNum(candidates[0] || rawVariant || "");
};

const variantsLikelySame = (brand, model, leftVariant, rightVariant) => {
  const left = canonicalVariantForCompare(brand, model, leftVariant);
  const right = canonicalVariantForCompare(brand, model, rightVariant);
  if (!left || !right) return false;
  return left === right || left.includes(right) || right.includes(left);
};

const buildBrandRegexCandidates = (brand) => {
  const raw = String(brand || "").trim().toLowerCase();
  const canonical = normalizeBrandForJoin(raw);
  const firstToken = canonical.split(" ").filter(Boolean)[0] || "";
  return [...new Set([raw, canonical, firstToken].filter(Boolean))];
};

// Simple score so we prefer New-Delhi > Delhi > others when deduping
const scoreCity = (c) => {
  const lc = (c || "").toLowerCase();
  if (lc === "new-delhi" || lc === "new delhi") return 3;
  if (lc === "delhi") return 2;
  return 1;
};

const parseBoolean = (value) => {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value === 1;
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return ["1", "true", "yes", "y"].includes(normalized);
};

const hasDiscontinuedDate = (value) => {
  if (value == null) return false;
  const normalized = String(value).trim().toLowerCase();
  return normalized !== "" && normalized !== "null" && normalized !== "undefined";
};

const isVehicleDiscontinued = (vehicle) =>
  parseBoolean(
    vehicle?.is_discontinued ??
      vehicle?.isDiscontinued ??
      vehicle?.IsDiscontinued,
  ) ||
  hasDiscontinuedDate(vehicle?.discontinued_date ?? vehicle?.discontinuedDate);

const ACTIVE_VARIANT_FILTER = {
  $and: [
    {
      $or: [
        { is_discontinued: { $exists: false } },
        { is_discontinued: false },
        { is_discontinued: 0 },
        { is_discontinued: null },
      ],
    },
    {
      $nor: [
        { isDiscontinued: true },
        { isDiscontinued: 1 },
        { isDiscontinued: "true" },
        { isDiscontinued: "True" },
        { discontinued_date: { $exists: true, $nin: [null, "", "null", "NULL"] } },
        { discontinuedDate: { $exists: true, $nin: [null, "", "null", "NULL"] } },
      ],
    },
  ],
};

// @desc  Get all raw feature details (id = feature doc _id)
// @route GET /api/features/details
// @access Public
export const getFeatureDetails = asyncHandler(async (req, res) => {
  const featureDocs = await VehicleFeature.find({});
  const details = {};
  featureDocs.forEach((f) => {
    details[f._id.toString()] = {
      id: f._id.toString(),
      features: objectToFeaturesArray(f.features),
    };
  });
  res.json(details);
});

// @desc  Get all feature variants (flattened)
// @route GET /api/features/variants
// @access Public
export const getFeatureVariants = asyncHandler(async (req, res) => {
  const featureDocs = await VehicleFeature.find({});
  const variants = featureDocs.map((f) => ({
    id: f._id.toString(),
    make: f.brand,
    model: f.model,
    variant: f.variant,
    fuel: null,
    tags: [],
  }));
  res.json(variants);
});

// @desc  Get one variant's feature details
// @route GET /api/features/variant/:id
// @access Public
export const getFeatureVariantById = asyncHandler(async (req, res) => {
  const f = await VehicleFeature.findById(req.params.id);
  if (!f) {
    res.status(404);
    throw new Error("Variant features not found");
  }
  res.json({
    id: f._id.toString(),
    features: objectToFeaturesArray(f.features),
  });
});

// @desc  Get combined variants with pricing + features
//        One row per brand+model+variant (features are not city specific)
// @route GET /api/features/variants-with-price
// @access Public


export const getFeaturesBySelection = asyncHandler(async (req, res) => {
  const { make, model, variant, vehicleId } = req.query;

  if (!make || !model || !variant) {
    res.status(400);
    throw new Error("Make, model and variant are required");
  }

  let brand = make;
  let rawModel = model;
  let rawVariant = variant;

  if (vehicleId) {
    const vehicle = await Vehicle.findById(vehicleId).lean();
    if (vehicle) {
      brand = vehicle.brand || vehicle.make || make;
      rawModel = vehicle.model || model;
      rawVariant = vehicle.variant || variant;
    }
  }

  const brandKey = normalizeBrandForJoin(brand);
  const modelKey = normalizeModelForJoin(brandKey, rawModel);
  const variantKey = normalizeVariantForJoin(rawVariant, brand, rawModel);
  const joinKey = `${brandKey}|${modelKey}|${variantKey}`;
  const cachedSelection = _bySelectionCache.get(joinKey);
  if (
    cachedSelection &&
    Date.now() - cachedSelection.ts <= BY_SELECTION_CACHE_TTL_MS
  ) {
    return res.json({ success: true, data: cachedSelection.data });
  }

  const findFuzzyMatch = (rows = []) => {
    if (!Array.isArray(rows) || !rows.length) return null;
    const exact = rows.find((row) => row?._joinKey === joinKey);
    if (exact) return exact;

    return rows.find((row) => {
      const rowBrandKey = normalizeBrandForJoin(row?.make || row?.brand);
      const rowModelKey = normalizeModelForJoin(rowBrandKey, row?.model);
      if (rowBrandKey !== brandKey || rowModelKey !== modelKey) return false;
      return variantsLikelySame(brand, rawModel, rawVariant, row?.variant);
    });
  };

  // Serve from in-memory cache when available (avoids DB round-trip).
  // Use stored _joinKey for direct comparison — re-normalizing the presentation-form
  // variant would produce a different key than buildFullJoin's raw-variant-based key.
  const cachedPool =
    _vwpfCacheFull && Array.isArray(_vwpfCacheFull.data)
      ? _vwpfCacheFull.data
      : [];
  if (cachedPool.length) {
    const cached = findFuzzyMatch(cachedPool);
    return res.json({
      success: true,
      data: cached?.features ?? [],
    });
  }

  // Cache miss: try direct exact lookup first (index-friendly), then fuzzy fallback.
  const modelCandidates = dedupeStrings([
    rawModel,
    presentModel(brand, rawModel),
    `${presentMake(brand)} ${presentModel(brand, rawModel)}`.trim(),
  ]);
  const variantCandidates = dedupeStrings([
    rawVariant,
    presentVariant(brand, rawModel, rawVariant),
    `${presentMake(brand)} ${presentVariant(brand, rawModel, rawVariant)}`.trim(),
    `${presentModel(brand, rawModel)} ${presentVariant(brand, rawModel, rawVariant)}`.trim(),
    `${presentMake(brand)} ${presentModel(brand, rawModel)} ${presentVariant(
      brand,
      rawModel,
      rawVariant,
    )}`.trim(),
  ]);
  const quickMatch = await VehicleFeature.findOne({
    brand: { $in: dedupeStrings([brand, ...buildBrandRegexCandidates(brand)]) },
    model: { $in: modelCandidates },
    variant: { $in: variantCandidates },
  })
    .collation({ locale: "en", strength: 2 })
    .lean();
  if (quickMatch) {
    const data = objectToFeaturesArray(quickMatch.features);
    _bySelectionCache.set(joinKey, { ts: Date.now(), data });
    return res.json({ success: true, data });
  }

  // Fallback fuzzy scan (brand-scoped)
  const brandCandidates = buildBrandRegexCandidates(brand);
  const featureDocs = await VehicleFeature.find({
    $or: brandCandidates.map((candidate) => ({
      brand: new RegExp(`^${escapeRegex(candidate)}$`, "i"),
    })),
  }).lean();

  const match = featureDocs.find((f) => {
    const fBrandKey = normalizeBrandForJoin(f.brand);
    const fModelKey = normalizeModelForJoin(fBrandKey, f.model);
    const fVariantKey = normalizeVariantForJoin(f.variant, f.brand, f.model);
    const candidateJoinKey = `${fBrandKey}|${fModelKey}|${fVariantKey}`;
    if (candidateJoinKey === joinKey) return true;
    if (fBrandKey !== brandKey || fModelKey !== modelKey) return false;
    return variantsLikelySame(brand, rawModel, rawVariant, f.variant);
  });

  const data = match ? objectToFeaturesArray(match.features) : [];
  _bySelectionCache.set(joinKey, { ts: Date.now(), data });
  res.json({ success: true, data });
});

// In-memory caches for variants list endpoint:
// - slim cache: fast list payload for initial catalog/EMI loads (no full feature arrays)
// - full cache: includes features arrays (used when UI explicitly requests full rows)
const VWPF_CACHE_TTL_MS = 10 * 60 * 1000; // 10 min
let _vwpfCacheSlim = null; // { data: Array, ts: number }
let _vwpfCacheFull = null; // { data: Array, ts: number }
let _vwpfCacheSlimPending = null;
let _vwpfCacheFullPending = null;
const BY_SELECTION_CACHE_TTL_MS = 30 * 60 * 1000;
const _bySelectionCache = new Map(); // key: joinKey, value: {ts,data}
const SCOPED_CACHE_TTL_MS = 5 * 60 * 1000;
const _vwpfScopedCache = new Map(); // key: scoped filter key, value: {ts,data}

// Extract a single summary value from the raw features object for a given keyword.
// Used to populate card pills (airbags, NCAP, screen size) without sending the
// full features array to the client on initial list load.
const extractFeatureSummary = (featuresObj, keyword) => {
  if (!featuresObj || typeof featuresObj !== "object") return null;
  const lc = keyword.toLowerCase();
  for (const [fullKey, value] of Object.entries(featuresObj)) {
    if (fullKey.toLowerCase().includes(lc) && value && value !== "Not Available") {
      return String(value);
    }
  }
  return null;
};

const extractFeatureSummaryByAnyKeyword = (featuresObj, keywords = []) => {
  if (!featuresObj || typeof featuresObj !== "object") return null;
  const needles = (keywords || [])
    .map((value) => String(value || "").toLowerCase().trim())
    .filter(Boolean);
  if (!needles.length) return null;

  for (const [fullKey, value] of Object.entries(featuresObj)) {
    if (value == null) continue;
    const hay = String(fullKey || "").toLowerCase();
    if (!needles.some((needle) => hay.includes(needle))) continue;
    const normalized = String(value).trim();
    if (!normalized) continue;
    if (["not available", "na", "n/a", "-", "null", "undefined"].includes(normalized.toLowerCase())) {
      continue;
    }
    return normalized;
  }
  return null;
};

const normalizeFuelValue = (rawFuel) => {
  const fuel = String(rawFuel || "").trim().toLowerCase();
  if (!fuel) return null;
  if (fuel.includes("petrol")) return "Petrol";
  if (fuel.includes("diesel")) return "Diesel";
  if (fuel.includes("cng")) return "CNG";
  if (fuel.includes("electric") || fuel === "ev") return "Electric";
  if (fuel.includes("hybrid")) return "Hybrid";
  if (fuel.includes("lpg")) return "LPG";
  return String(rawFuel || "").trim() || null;
};

const detectTransmissionFromText = (rawText) => {
  const text = String(rawText || "").trim().toLowerCase();
  if (!text) return null;

  // Manual bucket
  if (
    /\bmt\b/.test(text) ||
    /\bmanual\b/.test(text)
  ) {
    return "MT";
  }

  // Automatic bucket
  if (
    /\bat\b/.test(text) ||
    /\bautomatic\b/.test(text) ||
    /\bamt\b/.test(text) ||
    /\bcvt\b/.test(text) ||
    /\bdct\b/.test(text) ||
    /\bivt\b/.test(text) ||
    /\btorque\s*converter\b/.test(text)
  ) {
    return "AT";
  }

  return null;
};

const normalizeTransmissionValue = (vehicleDoc, featuresObj = {}) => {
  const directTransmission =
    vehicleDoc?.transmission ||
    vehicleDoc?.transmission_type ||
    vehicleDoc?.gearbox ||
    null;
  const fromDirect = detectTransmissionFromText(directTransmission);
  if (fromDirect) return fromDirect;

  const featureTransmission = extractFeatureSummaryByAnyKeyword(featuresObj, [
    "transmission",
    "gearbox",
  ]);
  const fromFeature = detectTransmissionFromText(featureTransmission);
  if (fromFeature) return fromFeature;

  const variantText = vehicleDoc?.variant || "";
  const fromVariant = detectTransmissionFromText(variantText);
  if (fromVariant) return fromVariant;

  // Practical default for this domain: if not explicitly automatic,
  // treat it as MT so manual variants don't disappear from filtering.
  return "MT";
};

const buildJoinedRows = async ({
  includeFeatures = false,
  vehicleRows = null,
  featureDocs: featureDocsOverride = null,
} = {}) => {
  // 1) Load feature docs (full or scoped set)
  const featureDocs = Array.isArray(featureDocsOverride)
    ? featureDocsOverride
    : await VehicleFeature.find({}).lean();
  const featureIndex = {};
  featureDocs.forEach((f) => {
    const brandKey = normalizeBrandForJoin(f.brand);
    const modelKey = normalizeModelForJoin(brandKey, f.model);
    const variantKey = normalizeVariantForJoin(f.variant, f.brand, f.model);
    featureIndex[`${brandKey}|${modelKey}|${variantKey}`] = f;
  });

  // 2) Load vehicles with only the fields needed for pricing (lean + projection)
  const vehicles = Array.isArray(vehicleRows)
    ? vehicleRows
    : await Vehicle.find({})
        .select(
          "brand make model variant fuel fuel_type transmission transmission_type gearbox city ex_showroom exShowroom total_on_road_with_accessories onRoadPrice is_discontinued isDiscontinued IsDiscontinued discontinued_date discontinuedDate _id",
        )
        .lean();

  // 3) Join + dedupe per brand|model|variant — keep best-city row
  const byKey = new Map();
  vehicles.forEach((v) => {
    const brand = v.brand || v.make;
    const modelRaw = v.model;
    const variantRaw = v.variant;
    if (!brand || !modelRaw || !variantRaw) return;

    const brandKey = normalizeBrandForJoin(brand);
    const modelKey = normalizeModelForJoin(brandKey, modelRaw);
    const variantKey = normalizeVariantForJoin(variantRaw, brand, modelRaw);
    const joinKey = `${brandKey}|${modelKey}|${variantKey}`;

    const f = featureIndex[joinKey];
    if (!f || !f.features || Object.keys(f.features).length === 0) return;

    const currentCity = v.city || "";
    const existing = byKey.get(joinKey);
    const currentIsDiscontinued = isVehicleDiscontinued(v);
    if (!existing || scoreCity(currentCity) > scoreCity(existing.city)) {
      // Pre-compute card pill values so the slim list can render badges without features
      const rawFeatures = f.features || {};
      const featureCount = Object.keys(rawFeatures || {}).length;
      byKey.set(joinKey, {
        id: f._id.toString(),
        _joinKey: joinKey,   // stored so getBySelection can do direct key comparison
        make: presentMake(brand),
        model: presentModel(brand, modelRaw),
        variant: presentVariant(brand, modelRaw, variantRaw),
        fuel: normalizeFuelValue(v.fuel || v.fuel_type || null),
        transmission: normalizeTransmissionValue(v, rawFeatures),
        tags: [],
        exShowroom: v.ex_showroom || v.exShowroom,
        onRoadPrice: v.total_on_road_with_accessories || v.onRoadPrice,
        city: currentCity,
        vehicleId: v._id,
        // Preserve discontinuation truth even when best-city winner is a different row.
        isDiscontinued: currentIsDiscontinued || Boolean(existing?.isDiscontinued),
        // Summary fields — always present even in slim mode (tiny strings, not arrays)
        _airbags: extractFeatureSummary(rawFeatures, "airbag"),
        _ncap:    extractFeatureSummary(rawFeatures, "ncap"),
        _screen:  extractFeatureSummary(rawFeatures, "touchscreen size"),
        featureCount,
        ...(includeFeatures ? { features: objectToFeaturesArray(rawFeatures) } : {}),
      });
    } else if (currentIsDiscontinued && !existing.isDiscontinued) {
      // If another city-row for same make/model/variant is discontinued,
      // keep that truth on the deduped canonical row.
      byKey.set(joinKey, {
        ...existing,
        isDiscontinued: true,
      });
    }
  });

  // 4) Sort: make+model alphabetically, then price ascending within group
  const result = Array.from(byKey.values());
  result.sort((a, b) => {
    const keyA = `${a.make} ${a.model}`;
    const keyB = `${b.make} ${b.model}`;
    if (keyA !== keyB) return keyA.localeCompare(keyB);
    return Number(a.exShowroom || a.onRoadPrice || 0) - Number(b.exShowroom || b.onRoadPrice || 0);
  });

  return result;
};

const buildSlimRowsFast = async () => {
  const vehicles = await Vehicle.find({})
    .select(
      "brand make model variant fuel fuel_type transmission transmission_type gearbox city ex_showroom exShowroom total_on_road_with_accessories onRoadPrice is_discontinued isDiscontinued IsDiscontinued discontinued_date discontinuedDate _id",
    )
    .lean();

  const byKey = new Map();
  vehicles.forEach((v) => {
    const brand = v.brand || v.make;
    const modelRaw = v.model;
    const variantRaw = v.variant;
    if (!brand || !modelRaw || !variantRaw) return;

    const brandKey = normalizeBrandForJoin(brand);
    const modelKey = normalizeModelForJoin(brandKey, modelRaw);
    const variantKey = normalizeVariantForJoin(variantRaw, brand, modelRaw);
    const joinKey = `${brandKey}|${modelKey}|${variantKey}`;

    const currentCity = v.city || "";
    const existing = byKey.get(joinKey);
    const currentIsDiscontinued = isVehicleDiscontinued(v);
    if (!existing || scoreCity(currentCity) > scoreCity(existing.city)) {
      byKey.set(joinKey, {
        id: joinKey,
        _joinKey: joinKey,
        make: presentMake(brand),
        model: presentModel(brand, modelRaw),
        variant: presentVariant(brand, modelRaw, variantRaw),
        fuel: normalizeFuelValue(v.fuel || v.fuel_type || null),
        transmission: normalizeTransmissionValue(v, {}),
        tags: [],
        exShowroom: v.ex_showroom || v.exShowroom,
        onRoadPrice: v.total_on_road_with_accessories || v.onRoadPrice,
        city: currentCity,
        vehicleId: v._id,
        isDiscontinued: currentIsDiscontinued || Boolean(existing?.isDiscontinued),
        _airbags: null,
        _ncap: null,
        _screen: null,
        featureCount: 0,
      });
    } else if (currentIsDiscontinued && !existing.isDiscontinued) {
      byKey.set(joinKey, { ...existing, isDiscontinued: true });
    }
  });

  const result = Array.from(byKey.values());
  result.sort((a, b) => {
    const keyA = `${a.make} ${a.model}`;
    const keyB = `${b.make} ${b.model}`;
    if (keyA !== keyB) return keyA.localeCompare(keyB);
    return (
      Number(a.exShowroom || a.onRoadPrice || 0) -
      Number(b.exShowroom || b.onRoadPrice || 0)
    );
  });
  return result;
};

const getFreshCache = (cache) =>
  cache && Date.now() - cache.ts <= VWPF_CACHE_TTL_MS ? cache.data : null;

const ensureCache = async ({ slim = true } = {}) => {
  if (slim) {
    const fresh = getFreshCache(_vwpfCacheSlim);
    if (fresh) return fresh;
    if (!_vwpfCacheSlimPending) {
      _vwpfCacheSlimPending = buildSlimRowsFast()
        .then((data) => {
          _vwpfCacheSlim = { data, ts: Date.now() };
          return data;
        })
        .finally(() => {
          _vwpfCacheSlimPending = null;
        });
    }
    return _vwpfCacheSlimPending;
  }

  const fresh = getFreshCache(_vwpfCacheFull);
  if (fresh) return fresh;
  if (!_vwpfCacheFullPending) {
    _vwpfCacheFullPending = buildJoinedRows({ includeFeatures: true })
      .then((data) => {
        _vwpfCacheFull = { data, ts: Date.now() };
        return data;
      })
      .finally(() => {
        _vwpfCacheFullPending = null;
      });
  }
  return _vwpfCacheFullPending;
};

const buildScopedRows = async ({
  make = "",
  model = "",
  variant = "",
  fuel = "",
  q = "",
  showDiscontinued = false,
  includeFeatures = false,
} = {}) => {
  const makeNormalized = normalizeText(make);
  const modelNormalized = normalizeText(model);
  const variantNormalized = normalizeText(variant);
  const fuelNormalized = normalizeText(fuel);
  const queryNormalized = normalizeText(q);

  const brandCandidates = dedupeStrings([
    make,
    ...buildBrandRegexCandidates(make),
    normalizeBrandForJoin(make),
  ]);
  const modelCandidates = dedupeStrings([
    model,
    presentModel(make, model),
    `${presentMake(make)} ${presentModel(make, model)}`.trim(),
  ]);
  const variantCandidates = dedupeStrings([
    variant,
    presentVariant(make, model, variant),
    `${presentMake(make)} ${presentVariant(make, model, variant)}`.trim(),
    `${presentModel(make, model)} ${presentVariant(make, model, variant)}`.trim(),
  ]);

  const baseQuery = {};
  if (brandCandidates.length) {
    baseQuery.$or = [
      { brand: { $in: brandCandidates } },
      { make: { $in: brandCandidates } },
    ];
  }
  if (modelCandidates.length) {
    baseQuery.model = { $in: modelCandidates };
  }
  if (variantCandidates.length && variantNormalized) {
    baseQuery.variant = { $in: variantCandidates };
  }
  if (!showDiscontinued) {
    baseQuery.$and = [...(baseQuery.$and || []), ACTIVE_VARIANT_FILTER];
  }

  const vehicleRows = await Vehicle.find(baseQuery)
    .select(
      "brand make model variant fuel fuel_type transmission transmission_type gearbox city ex_showroom exShowroom total_on_road_with_accessories onRoadPrice is_discontinued isDiscontinued IsDiscontinued discontinued_date discontinuedDate _id",
    )
    .collation({ locale: "en", strength: 2 })
    .lean();

  const filteredVehicles = vehicleRows.filter((row) => {
    const rowMake = normalizeText(row?.brand || row?.make || "");
    const rowModel = normalizeText(
      presentModel(row?.brand || row?.make || "", row?.model || ""),
    );
    const rowVariant = normalizeText(
      presentVariant(
        row?.brand || row?.make || "",
        row?.model || "",
        row?.variant || "",
      ),
    );
    const rowFuel = normalizeText(normalizeFuelValue(row?.fuel || row?.fuel_type || ""));
    const hay = normalizeText(
      `${row?.brand || row?.make || ""} ${row?.model || ""} ${row?.variant || ""} ${
        row?.fuel || row?.fuel_type || ""
      }`,
    );

    if (makeNormalized && rowMake !== makeNormalized && !rowMake.includes(makeNormalized)) {
      return false;
    }
    if (modelNormalized && !rowModel.includes(modelNormalized)) return false;
    if (variantNormalized && !rowVariant.includes(variantNormalized)) return false;
    if (fuelNormalized && !rowFuel.includes(fuelNormalized)) return false;
    if (queryNormalized && !hay.includes(queryNormalized)) return false;
    return true;
  });

  const scopedBrandKeys = dedupeStrings(
    filteredVehicles.map((row) => row?.brand || row?.make || ""),
  );
  const featureBrandCandidates = dedupeStrings([
    ...scopedBrandKeys,
    ...buildBrandRegexCandidates(make),
    normalizeBrandForJoin(make),
  ]);
  const featureQuery = {};
  if (featureBrandCandidates.length) {
    featureQuery.brand = { $in: featureBrandCandidates };
  }
  if (modelCandidates.length) {
    featureQuery.model = { $in: modelCandidates };
  }
  if (variantCandidates.length && variantNormalized) {
    featureQuery.variant = { $in: variantCandidates };
  }
  const scopedFeatureDocs = await VehicleFeature.find(featureQuery)
    .collation({ locale: "en", strength: 2 })
    .lean();

  return buildJoinedRows({
    includeFeatures,
    vehicleRows: filteredVehicles,
    featureDocs: scopedFeatureDocs,
  });
};

const buildScopedRowsFromSlim = async ({
  slimRows = [],
  make = "",
  model = "",
  variant = "",
  fuel = "",
  q = "",
  showDiscontinued = false,
} = {}) => {
  const makeNormalized = normalizeText(make);
  const modelNormalized = normalizeText(model);
  const variantNormalized = normalizeText(variant);
  const fuelNormalized = normalizeText(fuel);
  const queryNormalized = normalizeText(q);

  const filteredRows = (Array.isArray(slimRows) ? slimRows : []).filter((row) => {
    const rowMake = normalizeText(row?.make || "");
    const rowModel = normalizeText(row?.model || "");
    const rowVariant = normalizeText(row?.variant || "");
    const rowFuel = normalizeText(row?.fuel || "");
    const hay = normalizeText(
      `${row?.make || ""} ${row?.model || ""} ${row?.variant || ""} ${row?.fuel || ""}`,
    );

    if (!showDiscontinued && Boolean(row?.isDiscontinued)) return false;
    if (makeNormalized && !rowMake.includes(makeNormalized)) return false;
    if (modelNormalized && !rowModel.includes(modelNormalized)) return false;
    if (variantNormalized && !rowVariant.includes(variantNormalized)) return false;
    if (fuelNormalized && !rowFuel.includes(fuelNormalized)) return false;
    if (queryNormalized && !hay.includes(queryNormalized)) return false;
    return true;
  });
  if (!filteredRows.length) return [];

  const modelCandidates = dedupeStrings([
    model,
    presentModel(make, model),
    `${presentMake(make)} ${presentModel(make, model)}`.trim(),
  ]);
  const variantCandidates = dedupeStrings([
    variant,
    presentVariant(make, model, variant),
    `${presentMake(make)} ${presentVariant(make, model, variant)}`.trim(),
    `${presentModel(make, model)} ${presentVariant(make, model, variant)}`.trim(),
  ]);
  const featureBrandCandidates = dedupeStrings([
    make,
    ...buildBrandRegexCandidates(make),
    normalizeBrandForJoin(make),
  ]);

  const featureQuery = {};
  if (featureBrandCandidates.length) {
    featureQuery.brand = { $in: featureBrandCandidates };
  }
  if (modelCandidates.length) {
    featureQuery.model = { $in: modelCandidates };
  }
  if (variantCandidates.length && variantNormalized) {
    featureQuery.variant = { $in: variantCandidates };
  }

  const featureDocs = await VehicleFeature.find(featureQuery)
    .collation({ locale: "en", strength: 2 })
    .lean();
  if (!featureDocs.length) return [];

  const featureIndex = new Map();
  featureDocs.forEach((f) => {
    const b = normalizeBrandForJoin(f.brand);
    const m = normalizeModelForJoin(b, f.model);
    const v = normalizeVariantForJoin(f.variant, f.brand, f.model);
    featureIndex.set(`${b}|${m}|${v}`, f);
  });

  const out = [];
  filteredRows.forEach((row) => {
    const f = featureIndex.get(row?._joinKey);
    if (!f) return;
    const rawFeatures = f.features || {};
    out.push({
      ...row,
      id: f._id?.toString() || row.id,
      _airbags: extractFeatureSummary(rawFeatures, "airbag"),
      _ncap: extractFeatureSummary(rawFeatures, "ncap"),
      _screen: extractFeatureSummary(rawFeatures, "touchscreen size"),
      featureCount: Object.keys(rawFeatures).length,
      features: objectToFeaturesArray(rawFeatures),
    });
  });
  return out;
};

// Warm the cache immediately at module load in the background.
// This means the very first user request is served from cache instead of
// waiting for the expensive join to complete on-demand.
_vwpfCacheSlim = null;
_vwpfCacheFull = null;
setImmediate(() => {
  if (_vwpfCacheSlim) return;
  ensureCache({ slim: true })
    .then((data) => {
      if (!_vwpfCacheSlim) _vwpfCacheSlim = { data, ts: Date.now() };
    })
    .catch((err) => console.warn("[features] background cache warm failed:", err.message));
});

export const getVariantsWithPriceAndFeatures = asyncHandler(
  async (req, res) => {
    const {
      make = "",
      model = "",
      variant = "",
      fuel = "",
      q = "",
      search = "",
      slim = "",
      includeDiscontinued = "0",
    } = req.query;
    const isSlim = slim === "1" || slim === "true";
    const showDiscontinued = parseBoolean(includeDiscontinued);

    const hasScopedFilter = Boolean(
      String(make || "").trim() ||
        String(model || "").trim() ||
        String(variant || "").trim() ||
        String(fuel || "").trim() ||
        String(q || search || "").trim(),
    );

    let result = [];
    if (hasScopedFilter && !isSlim) {
      const scopedKey = JSON.stringify({
        make: String(make || "").trim().toLowerCase(),
        model: String(model || "").trim().toLowerCase(),
        variant: String(variant || "").trim().toLowerCase(),
        fuel: String(fuel || "").trim().toLowerCase(),
        q: String(q || search || "").trim().toLowerCase(),
        includeDiscontinued: Boolean(showDiscontinued),
      });
      const scopedCached = _vwpfScopedCache.get(scopedKey);
      if (scopedCached && Date.now() - scopedCached.ts <= SCOPED_CACHE_TTL_MS) {
        const payload = scopedCached.data.map(({ _joinKey, ...rest }) => rest);
        return res.json({
          success: true,
          count: payload.length,
          data: payload,
          fromCache: true,
        });
      }

      // Fast path: reuse already-cached slim rows for filtering and only fetch feature docs.
      let slimRows = getFreshCache(_vwpfCacheSlim);
      if (!slimRows) {
        slimRows = await ensureCache({ slim: true });
      }
      result = await buildScopedRowsFromSlim({
        slimRows,
        make,
        model,
        variant,
        fuel,
        q: q || search,
        showDiscontinued,
      });
      if (!result.length) {
        result = await buildScopedRows({
          make,
          model,
          variant,
          fuel,
          q: q || search,
          showDiscontinued,
          includeFeatures: !isSlim,
        });
      }
      _vwpfScopedCache.set(scopedKey, { ts: Date.now(), data: result });
    } else {
      result = await ensureCache({ slim: isSlim });
    }

    // Server-side filter on the cached dataset (O(n) — fast in memory)
    const safeMake     = String(make).trim().toLowerCase();
    const safeModel    = String(model).trim().toLowerCase();
    const safeVariant  = String(variant).trim().toLowerCase();
    const safeFuel     = String(fuel).trim().toLowerCase();
    const safeQ        = String(q || search).trim().toLowerCase();

    if (safeMake)    result = result.filter(v => String(v.make || "").toLowerCase() === safeMake);
    if (safeModel)   result = result.filter(v => String(v.model || "").toLowerCase() === safeModel);
    if (safeVariant) result = result.filter(v => String(v.variant || "").toLowerCase() === safeVariant);
    if (safeFuel)    result = result.filter(v => String(v.fuel || "").toLowerCase().includes(safeFuel));
    if (!showDiscontinued) {
      result = result.filter((v) => !Boolean(v.isDiscontinued));
    }
    if (safeQ) {
      result = result.filter(v => {
        const hay = `${v.make} ${v.model} ${v.variant} ${v.fuel || ""}`.toLowerCase();
        return hay.includes(safeQ);
      });
    }

    // Strip internal fields + optionally the full features array from the HTTP response.
    // _joinKey is never sent to the client — it's only used for in-process cache lookups.
    const payload = isSlim
      ? result.map(({ features, _joinKey, ...rest }) => rest)
      : result.map(({ _joinKey, ...rest }) => rest);

    res.json({
      success: true,
      count: payload.length,
      data: payload,
      fromCache: !hasScopedFilter,
    });
  },
);

// Expose a way for other routes to invalidate the cache (e.g. after a vehicle upsert)
export const invalidateVariantsCache = () => {
  _vwpfCacheSlim = null;
  _vwpfCacheFull = null;
  _vwpfScopedCache.clear();
  _bySelectionCache.clear();
};
