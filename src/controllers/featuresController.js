import asyncHandler from "express-async-handler";
import Vehicle from "../models/Vehicle.js";

// Converts Mongo { "Category | Feature": "Yes" } → frontend array
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

// GET /api/features/details
export const getFeatureDetails = asyncHandler(async (req, res) => {
  const vehicles = await Vehicle.find({ features: { $exists: true, $ne: {} } });
  const details = {};
  vehicles.forEach((v) => {
    details[v._id.toString()] = {
      id: v._id.toString(),
      features: objectToFeaturesArray(v.features),
    };
  });
  res.json(details);
});

// GET /api/features/variants
export const getFeatureVariants = asyncHandler(async (req, res) => {
  const vehicles = await Vehicle.find({ features: { $exists: true, $ne: {} } });
  const variants = vehicles.map((v) => ({
    id: v._id.toString(),
    make: v.make,
    model: v.model,
    variant: v.variant,
    fuel: v.fuel || v.fuel_type,
    tags: [],
  }));
  res.json(variants);
});

// GET /api/features/variant/:id
export const getFeatureVariantById = asyncHandler(async (req, res) => {
  const vehicle = await Vehicle.findById(req.params.id);
  if (!vehicle?.features) {
    res.status(404);
    throw new Error("Variant features not found");
  }
  res.json({
    id: vehicle._id.toString(),
    features: objectToFeaturesArray(vehicle.features),
  });
});

// GET /api/features/variants-with-price (MAIN ENDPOINT)
export const getVariantsWithPriceAndFeatures = asyncHandler(
  async (req, res) => {
    const { city } = req.query;
    const query = { features: { $exists: true, $ne: {} } };
    if (city) query.city = city;

    const vehicles = await Vehicle.find(query).sort({
      make: 1,
      model: 1,
      variant: 1,
    });

    const result = vehicles.map((v) => ({
      id: v._id.toString(),
      make: v.make,
      model: v.model,
      variant: v.variant,
      fuel: v.fuel || v.fuel_type,
      transmission: "Automatic", // fallback
      tags: [],
      exShowroom: v.ex_showroom || v.exShowroom,
      onRoadPrice: v.total_on_road_with_accessories || v.onRoadPrice,
      city: v.city,
      vehicleId: v._id,
      features: objectToFeaturesArray(v.features), // ← Magic conversion
    }));

    // Original sorting
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
