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

const normalizeBrandForJoin = (rawBrand) => {
  if (!rawBrand) return "";
  let b = String(rawBrand).trim().toLowerCase();

  // "aston-martin" -> "aston martin", "land-rover" -> "land rover"
  b = b.replace(/[-_]+/g, " ");

  // collapse spaces
  b = b.replace(/\s+/g, " ").trim();

  return b;
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
export const getVariantsWithPriceAndFeatures = asyncHandler(
  async (req, res) => {
    // 1) Load all features and index by normalized brand|model|variant
    const featureDocs = await VehicleFeature.find({});
    const featureIndex = {};
    featureDocs.forEach((f) => {
      const brandKey = normalizeBrandForJoin(f.brand);
      const modelKey = normalizeModelForJoin(brandKey, f.model);
      const variantKey = normalizeVariantForJoin(f.variant);
      const key = `${brandKey}|${modelKey}|${variantKey}`;
      featureIndex[key] = f;
    });

    // 2) Load all vehicles (all cities)
    const vehicles = await Vehicle.find({});

    // 3) Join and dedupe: one row per brand+model+variant (best city chosen)
    const byKey = new Map();

    vehicles.forEach((v) => {
      const brand = v.brand || v.make;
      const modelRaw = v.model;
      const variantRaw = v.variant;
      const presentBrand = presentMake(brand);
      const presentModelValue = presentModel(brand, modelRaw);
      const presentVariantValue = presentVariant(brand, modelRaw, variantRaw);
      if (!brand || !modelRaw || !variantRaw) return;

      const brandKey = normalizeBrandForJoin(brand);
      const modelKey = normalizeModelForJoin(brandKey, modelRaw);
      const variantKey = normalizeVariantForJoin(variantRaw);

      const joinKey = `${brandKey}|${modelKey}|${variantKey}`;
      const f = featureIndex[joinKey];
      if (!f || !f.features || Object.keys(f.features).length === 0) return;

      const outKey = joinKey; // dedupe key without city
      const currentCity = v.city || "";

      const existing = byKey.get(outKey);
      if (!existing || scoreCity(currentCity) > scoreCity(existing.city)) {
        byKey.set(outKey, {
          id: f._id.toString(), // feature doc id
          make: presentBrand,
          model: presentModelValue,
          variant: presentVariantValue,
          fuel: v.fuel || v.fuel_type || null,
          transmission: "Automatic", // placeholder unless you add real field
          tags: [],
          exShowroom: v.ex_showroom || v.exShowroom,
          onRoadPrice: v.total_on_road_with_accessories || v.onRoadPrice,
          city: currentCity, // best city we found for this variant
          vehicleId: v._id,
          features: objectToFeaturesArray(f.features),
        });
      }
    });

    const result = Array.from(byKey.values());

    // 4) Sort within make+model by price
    result.sort((a, b) => {
      const keyA = `${a.make} ${a.model}`;
      const keyB = `${b.make} ${b.model}`;
      if (keyA !== keyB) return keyA.localeCompare(keyB);
      const priceA = Number(a.exShowroom || a.onRoadPrice || 0);
      const priceB = Number(b.exShowroom || b.onRoadPrice || 0);
      return priceA - priceB;
    });

    res.json({ success: true, count: result.length, data: result });
  },
);
