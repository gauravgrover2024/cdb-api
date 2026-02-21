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

// @desc  Get all raw feature details (id = feature doc _id)
// @route GET /api/features/details
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
export const getFeatureVariants = asyncHandler(async (req, res) => {
  const featureDocs = await VehicleFeature.find({});
  const variants = featureDocs.map((f) => ({
    id: f._id.toString(),
    make: f.brand,
    model: f.model,
    variant: f.variant,
    fuel: null, // not stored here
    tags: [],
  }));
  res.json(variants);
});

// @desc  Get one variant's feature details
// @route GET /api/features/variant/:id
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
export const getVariantsWithPriceAndFeatures = asyncHandler(
  async (req, res) => {
    const { city } = req.query;

    // 1) Load all features
    const featureDocs = await VehicleFeature.find({});
    // index by brand|model|variant
    const featureIndex = {};
    featureDocs.forEach((f) => {
      const key = `${(f.brand || "").trim().toLowerCase()}|${(f.model || "")
        .trim()
        .toLowerCase()}|${(f.variant || "").trim().toLowerCase()}`;
      featureIndex[key] = f;
    });

    // 2) Load vehicles (pricing)
    const vehicleQuery = {};
    if (city) vehicleQuery.city = city;
    const vehicles = await Vehicle.find(vehicleQuery).sort({
      brand: 1,
      make: 1,
      model: 1,
      variant: 1,
    });

    const result = vehicles
      .map((v) => {
        const brand = v.brand || v.make;
        const model = v.model;
        const variant = v.variant;

        if (!brand || !model || !variant) return null;

        const key = `${String(brand).trim().toLowerCase()}|${String(model)
          .trim()
          .toLowerCase()}|${String(variant).trim().toLowerCase()}`;

        const f = featureIndex[key];
        if (!f || !f.features || Object.keys(f.features).length === 0) {
          return null; // skip vehicles with no features
        }

        return {
          id: f._id.toString(),
          make: brand,
          model,
          variant,
          fuel: v.fuel || v.fuel_type || null,
          transmission: "Automatic", // not in DB; placeholder
          tags: [],
          exShowroom: v.ex_showroom || v.exShowroom,
          onRoadPrice: v.total_on_road_with_accessories || v.onRoadPrice,
          city: v.city,
          vehicleId: v._id,
          features: objectToFeaturesArray(f.features),
        };
      })
      .filter(Boolean);

    // 3) Sort within make+model by price
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
