import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const clean = (v = "") => String(v || "").trim();

const compact = (v = "") =>
  clean(v)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const print = (title, value) => {
  console.log("\n==============================");
  console.log(title);
  console.log("==============================");
  console.log(JSON.stringify(value, null, 2));
};

await connectDB();

const db = mongoose.connection.db;
const vehicles = db.collection("vehicles");

const query = {
  $or: [
    { model: /verna/i },
    { modelName: /verna/i },
    { model_name: /verna/i },
    { fullModel: /verna/i },
    { variant: /verna/i },
    { variantName: /verna/i },
    { name: /verna/i },
    { title: /verna/i },
  ],
};

const docs = await vehicles.find(query).limit(80).toArray();

print("Verna docs count sampled", {
  sampled: docs.length,
});

const cityCounts = {};
const modelCounts = {};
const variantSamples = [];

for (const doc of docs) {
  const city = clean(doc.citySlug || doc.city_slug || doc.city || doc.cityName || "unknown");
  cityCounts[city] = (cityCounts[city] || 0) + 1;

  const model = clean(doc.model || doc.modelName || doc.model_name || doc.fullModel || "unknown");
  modelCounts[model] = (modelCounts[model] || 0) + 1;

  variantSamples.push({
    _id: String(doc._id),
    make: doc.make,
    brand: doc.brand,
    model: doc.model,
    modelName: doc.modelName,
    model_name: doc.model_name,
    fullModel: doc.fullModel,
    variant: doc.variant,
    variantName: doc.variantName,
    variant_name: doc.variant_name,
    name: doc.name,
    title: doc.title,
    city: doc.city,
    cityName: doc.cityName,
    citySlug: doc.citySlug,
    city_slug: doc.city_slug,

    exShowroomPriceNumeric: doc.exShowroomPriceNumeric,
    exShowroomPrice: doc.exShowroomPrice,
    ex_showroom_price: doc.ex_showroom_price,
    price: doc.price,
    priceNumeric: doc.priceNumeric,
    onRoadPriceNumeric: doc.onRoadPriceNumeric,
    onRoadPrice: doc.onRoadPrice,
    on_road_price: doc.on_road_price,
    startingOnRoadPrice: doc.startingOnRoadPrice,

    fuel: doc.fuel,
    fuelType: doc.fuelType,
    fuel_type: doc.fuel_type,
    transmission: doc.transmission,
    transmissionType: doc.transmissionType,
    transmission_type: doc.transmission_type,
    gearbox: doc.gearbox,

    raw_price_json_keys:
      doc.raw_price_json && typeof doc.raw_price_json === "object"
        ? Object.keys(doc.raw_price_json).slice(0, 80)
        : [],
    raw_keys:
      doc.raw && typeof doc.raw === "object" ? Object.keys(doc.raw).slice(0, 80) : [],
    raw_price_json: doc.raw_price_json || null,
    raw: doc.raw || null,
  });
}

print("City counts", cityCounts);
print("Model counts", modelCounts);
print("Variant/price samples", variantSamples.slice(0, 25));

const newDelhiDocs = docs.filter((doc) => {
  const city = compact(doc.citySlug || doc.city_slug || doc.city || doc.cityName || "");
  return city.includes("delhi") || city.includes("newdelhi");
});

print("New Delhi / Delhi matching docs", {
  count: newDelhiDocs.length,
  samples: newDelhiDocs.slice(0, 20).map((doc) => ({
    _id: String(doc._id),
    make: doc.make,
    brand: doc.brand,
    model: doc.model,
    variant: doc.variant,
    city: doc.city,
    citySlug: doc.citySlug,
    exShowroomPriceNumeric: doc.exShowroomPriceNumeric,
    exShowroomPrice: doc.exShowroomPrice,
    price: doc.price,
    onRoadPrice: doc.onRoadPrice,
    raw_price_json: doc.raw_price_json,
  })),
});

await mongoose.disconnect();
