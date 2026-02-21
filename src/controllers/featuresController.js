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
// @route GET /api/features/variants-with-price
// @access Public
export const getVariantsWithPriceAndFeatures = asyncHandler(
  async (req, res) => {
    const { city } = req.query;

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

    // 2) Load vehicles (pricing side)
    const vehicleQuery = {};
    if (city) vehicleQuery.city = city;

    // No Mongo sort to avoid 32MB limit; we sort in JS later
    const vehicles = await Vehicle.find(vehicleQuery);

    // 3) Join vehicles with features
    const result = vehicles
      .map((v) => {
        const brand = v.brand || v.make;
        const modelRaw = v.model;
        const variantRaw = v.variant;

        if (!brand || !modelRaw || !variantRaw) return null;

        const brandKey = String(brand).trim().toLowerCase();
        const modelKey = normalizeModelForJoin(brand, modelRaw); // "Audi A6" -> "a6"
        const variantKey = String(variantRaw).trim().toLowerCase();

        const key = `${brandKey}|${modelKey}|${variantKey}`;
        const f = featureIndex[key];

        if (!f || !f.features || Object.keys(f.features).length === 0) {
          return null; // skip vehicles with no matched features
        }

        return {
          id: f._id.toString(), // feature doc id
          make: brand,
          model: modelRaw,
          variant: variantRaw,
          fuel: v.fuel || v.fuel_type || null,
          transmission: "Automatic", // placeholder, unless you add real field
          tags: [],
          exShowroom: v.ex_showroom || v.exShowroom,
          onRoadPrice: v.total_on_road_with_accessories || v.onRoadPrice,
          city: v.city,
          vehicleId: v._id,
          features: objectToFeaturesArray(f.features),
        };
      })
      .filter(Boolean);

    // 4) Sort within make+model by price (same as earlier)
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
