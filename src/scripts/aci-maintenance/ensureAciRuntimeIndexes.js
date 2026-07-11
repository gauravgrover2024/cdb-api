#!/usr/bin/env node

import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const sameKey = (left = {}, right = {}) =>
  JSON.stringify(left) === JSON.stringify(right);

const INDEXES = [
  {
    collection: "aci_vehicle_price_rows",
    keys: { citySlug: 1, onRoadPrice: 1, modelKey: 1, variantKey: 1 },
    options: { name: "aci_runtime_price_city_onroad_model_variant" },
    purpose: "On-road budget discovery and exact-variant ranking.",
  },
  {
    collection: "aci_vehicle_price_rows",
    keys: { citySlug: 1, bodyTypeKey: 1, onRoadPrice: 1, modelKey: 1 },
    options: { name: "aci_runtime_price_city_body_onroad_model" },
    purpose: "Body-type discovery under an on-road cap.",
  },
  {
    collection: "aci_vehicle_price_rows",
    keys: {
      citySlug: 1,
      fuelKey: 1,
      transmissionKey: 1,
      onRoadPrice: 1,
      modelKey: 1,
    },
    options: { name: "aci_runtime_price_city_powertrain_onroad_model" },
    purpose: "Fuel and transmission filtered recommendations under an on-road cap.",
  },
  {
    collection: "aci_home_popular_cars_v1",
    keys: { cacheKey: 1 },
    options: { name: "aci_home_popular_cache_key", unique: true },
    purpose: "Single-read ACI Assist home payload lookup.",
  },
  {
    collection: "aci_home_popular_cars_v1",
    keys: { builtAt: -1 },
    options: { name: "aci_home_popular_built_at" },
    purpose: "Snapshot freshness audit and maintenance.",
  },
  {
    collection: "monthly_car_sales",
    keys: { source: 1, month: -1, rank: 1 },
    options: { name: "monthly_sales_source_month_rank" },
    purpose: "Latest popular-car sales ranking.",
  },
  {
    collection: "vehicle_colors_v2",
    keys: { brand: 1, model: 1, variant: 1 },
    options: { name: "vehicle_colors_v2_brand_model_variant" },
    purpose: "Exact model/variant colour and media lookup.",
  },
  {
    collection: "aci_feature_explainers_v1",
    keys: { canonicalKey: 1 },
    options: { name: "canonicalKey_1", unique: true },
    purpose: "Canonical feature explanation lookup.",
  },
];

const ensureIndex = async (db, spec) => {
  const collection = db.collection(spec.collection);
  const indexes = await collection.indexes().catch((error) => {
    if (error?.code === 26 || error?.codeName === "NamespaceNotFound") return [];
    throw error;
  });
  const sameShape = indexes.find((index) => sameKey(index.key, spec.keys));
  if (sameShape) {
    return {
      ...spec,
      status: "exists",
      actualName: sameShape.name,
    };
  }

  const actualName = await collection.createIndex(spec.keys, {
    background: true,
    ...spec.options,
  });
  return { ...spec, status: "created", actualName };
};

await connectDB();

try {
  const db = mongoose.connection.db;
  const results = [];
  for (const spec of INDEXES) {
    results.push(await ensureIndex(db, spec));
  }

  console.log(
    JSON.stringify(
      {
        suite: "ACI runtime index ensure v1",
        ok: true,
        database: db.databaseName,
        total: results.length,
        created: results.filter((item) => item.status === "created").length,
        existing: results.filter((item) => item.status === "exists").length,
        results,
      },
      null,
      2,
    ),
  );
} finally {
  await mongoose.disconnect();
}
