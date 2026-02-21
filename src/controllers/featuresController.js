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

// Normalize model for vehicle side so "Audi A6" matches features model "A6"
const normalizeModelForJoin = (brand, model) => {
  if (!model) return "";
  const b = (brand || "").trim().toLowerCase(); // "audi"
  const m = String(model).trim().toLowerCase(); // "audi a6"
  const prefix = b + " ";
  return m.startsWith(prefix) ? m.slice(prefix.length) : m; // "a6"
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
    // NOTE: we intentionally ignore city for deduped list,
    // but keep city on the chosen representative vehicle.

    // 1) Load all features and index by brand|model|variant (features side)
    const featureDocs = await VehicleFeature.find({});
    const featureIndex = {};
    featureDocs.forEach((f) => {
      const brand = (f.brand || "").trim().toLowerCase();
      const model = (f.model || "").trim().toLowerCase(); // e.g. "a6"
      const variant = (f.variant || "").trim().toLowerCase();
      const key = `${brand}|${model}|${variant}`;
      featureIndex[key] = f;
    });

    // 2) Load all vehicles (all cities)
    const vehicles = await Vehicle.find({}); // no city filter here

    // 3) Join and dedupe: one row per brand+model+variant (best city chosen)
    const byKey = new Map();

    vehicles.forEach((v) => {
      const brand = v.brand || v.make;
      const modelRaw = v.model;
      const variantRaw = v.variant;
      if (!brand || !modelRaw || !variantRaw) return;

      const brandKey = String(brand).trim().toLowerCase();
      const modelKey = normalizeModelForJoin(brand, modelRaw); // "Audi A6" -> "a6"
      const variantKey = String(variantRaw).trim().toLowerCase();

      const joinKey = `${brandKey}|${modelKey}|${variantKey}`;
      const f = featureIndex[joinKey];
      if (!f || !f.features || Object.keys(f.features).length === 0) return;

      const outKey = joinKey; // dedupe key without city
      const currentCity = v.city || "";

      const existing = byKey.get(outKey);
      if (!existing || scoreCity(currentCity) > scoreCity(existing.city)) {
        byKey.set(outKey, {
          id: f._id.toString(), // feature doc id
          make: brand,
          model: modelRaw,
          variant: variantRaw,
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
