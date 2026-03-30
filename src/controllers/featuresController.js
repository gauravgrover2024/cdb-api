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

  // strip brand prefix: "aston martin db12" -> "db12"
  const prefix = b + " ";
  if (m.startsWith(prefix)) {
    m = m.slice(prefix.length);
  }

  // unify separators: "grand-vitara" -> "grand vitara"
  m = m.replace(/[-_]+/g, " ");

  // collapse spaces
  m = m.replace(/\s+/g, " ").trim();

  return m;
};

const normalizeVariantForJoin = (rawVariant) => {
  if (!rawVariant) return "";
  let v = String(rawVariant).trim().toLowerCase();

  // just normalize whitespace so long names line up
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
  const variantKey = normalizeVariantForJoin(rawVariant);
  const joinKey = `${brandKey}|${modelKey}|${variantKey}`;

  // Serve from in-memory cache when available (avoids DB round-trip).
  // Use stored _joinKey for direct comparison — re-normalizing the presentation-form
  // variant would produce a different key than buildFullJoin's raw-variant-based key.
  if (_vwpfCache) {
    const cached = _vwpfCache.data.find((v) => v._joinKey === joinKey);
    return res.json({
      success: true,
      data: cached?.features ?? [],
    });
  }

  // Cache miss: fall back to DB query
  const featureDocs = await VehicleFeature.find({
    brand: new RegExp(`^${escapeRegex(brand)}$`, 'i'),
  }).lean();

  const match = featureDocs.find((f) => {
    const fBrandKey = normalizeBrandForJoin(f.brand);
    const fModelKey = normalizeModelForJoin(fBrandKey, f.model);
    const fVariantKey = normalizeVariantForJoin(f.variant);
    return `${fBrandKey}|${fModelKey}|${fVariantKey}` === joinKey;
  });

  res.json({
    success: true,
    data: match ? objectToFeaturesArray(match.features) : [],
  });
});

// In-memory cache: full unfiltered join result, refreshed every 10 minutes.
// Filtered requests always served from this cache (no extra DB round-trip).
const VWPF_CACHE_TTL_MS = 10 * 60 * 1000; // 10 min
let _vwpfCache = null; // { data: Array, ts: number }

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

const buildFullJoin = async () => {
  // 1) Load all feature docs — brand index makes this scan-free after index creation
  const featureDocs = await VehicleFeature.find({}).lean();
  const featureIndex = {};
  featureDocs.forEach((f) => {
    const brandKey = normalizeBrandForJoin(f.brand);
    const modelKey = normalizeModelForJoin(brandKey, f.model);
    const variantKey = normalizeVariantForJoin(f.variant);
    featureIndex[`${brandKey}|${modelKey}|${variantKey}`] = f;
  });

  // 2) Load vehicles with only the fields needed for pricing (lean + projection)
  const vehicles = await Vehicle.find({}).select(
    "brand make model variant fuel fuel_type transmission transmission_type gearbox city ex_showroom exShowroom total_on_road_with_accessories onRoadPrice is_discontinued isDiscontinued IsDiscontinued discontinued_date discontinuedDate _id",
  ).lean();

  // 3) Join + dedupe per brand|model|variant — keep best-city row
  const byKey = new Map();
  vehicles.forEach((v) => {
    const brand = v.brand || v.make;
    const modelRaw = v.model;
    const variantRaw = v.variant;
    if (!brand || !modelRaw || !variantRaw) return;

    const brandKey = normalizeBrandForJoin(brand);
    const modelKey = normalizeModelForJoin(brandKey, modelRaw);
    const variantKey = normalizeVariantForJoin(variantRaw);
    const joinKey = `${brandKey}|${modelKey}|${variantKey}`;

    const f = featureIndex[joinKey];
    if (!f || !f.features || Object.keys(f.features).length === 0) return;

    const currentCity = v.city || "";
    const existing = byKey.get(joinKey);
    const currentIsDiscontinued = isVehicleDiscontinued(v);
    if (!existing || scoreCity(currentCity) > scoreCity(existing.city)) {
      // Pre-compute card pill values so the slim list can render badges without features
      const rawFeatures = f.features || {};
      const featuresArr = objectToFeaturesArray(rawFeatures);
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
        featureCount: featuresArr.length,
        // Full features array — stripped in slim mode
        features: featuresArr,
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

// Warm the cache immediately at module load in the background.
// This means the very first user request is served from cache instead of
// waiting for the expensive join to complete on-demand.
_vwpfCache = null; // force rebuild so _joinKey is present in every entry
setImmediate(() => {
  if (_vwpfCache) return;
  buildFullJoin()
    .then((data) => {
      if (!_vwpfCache) _vwpfCache = { data, ts: Date.now() };
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

    // Serve from cache if fresh; otherwise rebuild (warm cache in background on first miss)
    const now = Date.now();
    if (!_vwpfCache || now - _vwpfCache.ts > VWPF_CACHE_TTL_MS) {
      _vwpfCache = { data: await buildFullJoin(), ts: now };
    }

    let result = _vwpfCache.data;

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

    res.json({ success: true, count: payload.length, data: payload, fromCache: Boolean(_vwpfCache) });
  },
);

// Expose a way for other routes to invalidate the cache (e.g. after a vehicle upsert)
export const invalidateVariantsCache = () => { _vwpfCache = null; };
