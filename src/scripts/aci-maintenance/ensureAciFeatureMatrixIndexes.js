import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const COLLECTION_NAME = "vehicle_variant_feature_matrix_v2";

const REQUIRED_INDEXES = [
  {
    keys: { modelKey: 1, activeForFeatureExplorer: 1 },
    options: {
      name: "matrix_model_feature_explorer_active",
      background: true,
    },
    reason: "Fast model-level feature explorer and multi-feature answers.",
  },
  {
    keys: { brandModelKey: 1, activeForFeatureExplorer: 1 },
    options: {
      name: "matrix_brand_model_feature_explorer_active",
      background: true,
    },
    reason: "Fast make+model scoped feature explorer and multi-feature answers.",
  },
];

const normalizeKeySpec = (keys = {}) => JSON.stringify(keys || {});

const main = async () => {
  await connectDB();

  const db = mongoose.connection.db;
  const collection = db.collection(COLLECTION_NAME);

  const beforeIndexes = await collection.indexes();
  const beforeByName = new Map(beforeIndexes.map((index) => [index.name, index]));

  const results = [];

  for (const indexSpec of REQUIRED_INDEXES) {
    const existing = beforeByName.get(indexSpec.options.name);

    if (
      existing &&
      normalizeKeySpec(existing.key) === normalizeKeySpec(indexSpec.keys)
    ) {
      results.push({
        name: indexSpec.options.name,
        status: "exists",
        keys: indexSpec.keys,
        reason: indexSpec.reason,
      });
      continue;
    }

    if (existing) {
      results.push({
        name: indexSpec.options.name,
        status: "name_conflict",
        expectedKeys: indexSpec.keys,
        existingKeys: existing.key,
        reason: indexSpec.reason,
      });
      continue;
    }

    const createdName = await collection.createIndex(indexSpec.keys, indexSpec.options);

    results.push({
      name: createdName,
      status: "created",
      keys: indexSpec.keys,
      reason: indexSpec.reason,
    });
  }

  const afterIndexes = await collection.indexes();

  console.log(JSON.stringify({
    ok: results.every((item) => item.status !== "name_conflict"),
    collection: COLLECTION_NAME,
    requiredIndexes: REQUIRED_INDEXES.map((item) => item.options.name),
    results,
    availableIndexes: afterIndexes
      .filter((index) => REQUIRED_INDEXES.some((item) => item.options.name === index.name))
      .map((index) => ({
        name: index.name,
        key: index.key,
      })),
  }, null, 2));

  await mongoose.disconnect();

  if (results.some((item) => item.status === "name_conflict")) {
    process.exit(1);
  }
};

main().catch(async (error) => {
  console.error(error?.stack || error?.message || error);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
