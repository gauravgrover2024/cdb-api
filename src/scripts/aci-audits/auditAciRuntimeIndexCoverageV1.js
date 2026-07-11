#!/usr/bin/env node

import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const EXPECTED_INDEXES = [
  ["aci_vehicle_price_rows", "aci_runtime_price_city_onroad_model_variant"],
  ["aci_vehicle_price_rows", "aci_runtime_price_city_body_onroad_model"],
  ["aci_vehicle_price_rows", "aci_runtime_price_city_powertrain_onroad_model"],
  ["aci_home_popular_cars_v1", "aci_home_popular_cache_key"],
  ["monthly_car_sales", "monthly_sales_source_month_rank"],
  ["vehicle_colors_v2", "vehicle_colors_v2_brand_model_variant"],
  ["aci_feature_explainers_v1", "canonicalKey_1"],
];

const visitPlan = (value, stages = [], indexNames = []) => {
  if (!value || typeof value !== "object") return { stages, indexNames };
  if (typeof value.stage === "string") stages.push(value.stage);
  if (typeof value.indexName === "string") indexNames.push(value.indexName);
  Object.values(value).forEach((child) => {
    if (Array.isArray(child)) child.forEach((item) => visitPlan(item, stages, indexNames));
    else visitPlan(child, stages, indexNames);
  });
  return { stages, indexNames };
};

const explainFind = async ({ collection, query, sort, limit = 25 }) => {
  const startedAt = Date.now();
  let cursor = collection.find(query);
  if (sort) cursor = cursor.sort(sort);
  const explain = await cursor.limit(limit).explain("executionStats");
  const plan = visitPlan(explain.queryPlanner?.winningPlan || {});
  const stats = explain.executionStats || {};
  return {
    query,
    sort: sort || null,
    stages: [...new Set(plan.stages)],
    indexNames: [...new Set(plan.indexNames)],
    totalDocsExamined: stats.totalDocsExamined ?? null,
    totalKeysExamined: stats.totalKeysExamined ?? null,
    rowsReturned: stats.nReturned ?? null,
    executionTimeMillis: stats.executionTimeMillis ?? null,
    wallTimeMs: Date.now() - startedAt,
    collscan: plan.stages.includes("COLLSCAN"),
  };
};

await connectDB();

try {
  const db = mongoose.connection.db;
  const collectionIndexes = new Map();
  const missingIndexes = [];

  for (const [collectionName, indexName] of EXPECTED_INDEXES) {
    if (!collectionIndexes.has(collectionName)) {
      const indexes = await db
        .collection(collectionName)
        .indexes()
        .catch((error) => {
          if (error?.code === 26 || error?.codeName === "NamespaceNotFound") return [];
          throw error;
        });
      collectionIndexes.set(collectionName, indexes);
    }
    if (!collectionIndexes.get(collectionName).some((index) => index.name === indexName)) {
      missingIndexes.push({ collection: collectionName, indexName });
    }
  }

  const prices = db.collection("aci_vehicle_price_rows");
  const sample = await prices.findOne(
    {
      citySlug: { $type: "string", $ne: "" },
      onRoadPrice: { $gt: 0 },
      bodyTypeKey: { $type: "string", $ne: "" },
      fuelKey: { $type: "string", $ne: "" },
      transmissionKey: { $type: "string", $ne: "" },
    },
    {
      projection: {
        citySlug: 1,
        bodyTypeKey: 1,
        fuelKey: 1,
        transmissionKey: 1,
        onRoadPrice: 1,
      },
    },
  );

  const checks = [];
  if (sample) {
    const maxPrice = Math.max(Number(sample.onRoadPrice) * 1.5, Number(sample.onRoadPrice) + 1);
    checks.push(
      await explainFind({
        collection: prices,
        query: { citySlug: sample.citySlug, onRoadPrice: { $gt: 0, $lte: maxPrice } },
        sort: { citySlug: 1, onRoadPrice: 1 },
      }),
      await explainFind({
        collection: prices,
        query: {
          citySlug: sample.citySlug,
          bodyTypeKey: sample.bodyTypeKey,
          onRoadPrice: { $gt: 0, $lte: maxPrice },
        },
        sort: { citySlug: 1, onRoadPrice: 1 },
      }),
      await explainFind({
        collection: prices,
        query: {
          citySlug: sample.citySlug,
          fuelKey: sample.fuelKey,
          transmissionKey: sample.transmissionKey,
          onRoadPrice: { $gt: 0, $lte: maxPrice },
        },
        sort: { citySlug: 1, onRoadPrice: 1 },
      }),
    );
  }

  checks.push(
    await explainFind({
      collection: db.collection("aci_home_popular_cars_v1"),
      query: { cacheKey: "aci_home_popular_cars_v1:new-delhi:25" },
      limit: 1,
    }),
  );

  const collscans = checks.filter((check) => check.collscan);
  const ok = missingIndexes.length === 0 && collscans.length === 0 && Boolean(sample);
  console.log(
    JSON.stringify(
      {
        suite: "ACI runtime index coverage v1",
        ok,
        database: db.databaseName,
        expectedIndexCount: EXPECTED_INDEXES.length,
        missingIndexes,
        sampleFound: Boolean(sample),
        checks,
      },
      null,
      2,
    ),
  );
  if (!ok) process.exitCode = 1;
} finally {
  await mongoose.disconnect();
}
