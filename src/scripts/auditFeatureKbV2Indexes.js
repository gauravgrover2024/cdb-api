import dotenv from "dotenv";
import mongoose from "mongoose";

dotenv.config();

const uri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.DATABASE_URL;

if (!uri) {
  console.error("Missing Mongo URI in .env");
  process.exit(1);
}

await mongoose.connect(uri);

const db = mongoose.connection.db;

const matrix = db.collection("vehicle_variant_feature_matrix_v2");
const catalog = db.collection("vehicle_feature_catalog_v2");

const mb = (bytes = 0) => Number((bytes / 1024 / 1024).toFixed(2));

const printIndexes = async (name, collection) => {
  console.log(`\n=== INDEXES: ${name} ===`);
  const indexes = await collection.indexes();
  console.table(
    indexes.map((idx) => ({
      name: idx.name,
      key: JSON.stringify(idx.key),
      unique: !!idx.unique,
      sparse: !!idx.sparse,
    })),
  );
};

const printStats = async (name) => {
  const stats = await db.command({ collStats: name });
  console.log(`\n=== STATS: ${name} ===`);
  console.log({
    count: stats.count,
    sizeMB: mb(stats.size),
    storageMB: mb(stats.storageSize),
    totalIndexSizeMB: mb(stats.totalIndexSize),
  });
};

const explainFind = async ({ label, collection, filter, sort = null, limit = 20 }) => {
  let cursor = collection.find(filter);
  if (sort) cursor = cursor.sort(sort);
  cursor = cursor.limit(limit);

  const explain = await cursor.explain("executionStats");
  const stats = explain.executionStats || {};
  const winningPlan = explain.queryPlanner?.winningPlan || {};

  console.log(`\n=== EXPLAIN: ${label} ===`);
  console.log({
    filter,
    sort,
    limit,
    nReturned: stats.nReturned,
    totalKeysExamined: stats.totalKeysExamined,
    totalDocsExamined: stats.totalDocsExamined,
    executionTimeMillis: stats.executionTimeMillis,
    winningStage: winningPlan.stage,
    inputStage: winningPlan.inputStage?.stage,
    indexName:
      winningPlan.inputStage?.indexName ||
      winningPlan.indexName ||
      winningPlan.queryPlan?.inputStage?.indexName ||
      "",
  });
};

await printStats("vehicle_feature_catalog_v2");
await printStats("vehicle_variant_feature_matrix_v2");

await printIndexes("vehicle_feature_catalog_v2", catalog);
await printIndexes("vehicle_variant_feature_matrix_v2", matrix);

console.log("\n=== SAMPLE MATRIX DOC KEYS ===");
const sample = await matrix.findOne(
  { modelKey: "creta", activePricelistMatched: true },
  {
    projection: {
      _id: 0,
      brandKey: 1,
      modelKey: 1,
      variantKey: 1,
      variantFamilyKey: 1,
      activePricelistMatched: 1,
      priceMin: 1,
      priceMax: 1,
      featureGroups: 1,
      "featuresByKey.sunroof": 1,
      "featuresByKey.adas_package": 1,
      "featuresByKey.speakers": 1,
      "featuresByKey.integrated_2din_audio": 1,
      "featuresByKey.radio": 1,
    },
  },
);
console.dir(sample, { depth: 8 });

await explainFind({
  label: "Explorer: active Creta variants",
  collection: matrix,
  filter: { modelKey: "creta", activePricelistMatched: true },
  sort: { priceMin: 1, variantKey: 1 },
  limit: 80,
});

await explainFind({
  label: "Variant family: Creta King",
  collection: matrix,
  filter: {
    modelKey: "creta",
    activePricelistMatched: true,
    variantFamilyKey: "king",
  },
  sort: { priceMin: 1, variantKey: 1 },
  limit: 40,
});

await explainFind({
  label: "Variant exact: Creta EX (O)",
  collection: matrix,
  filter: {
    modelKey: "creta",
    activePricelistMatched: true,
    variantKey: "ex_o",
  },
  limit: 10,
});

await explainFind({
  label: "Cheapest feature: Creta ADAS active variants",
  collection: matrix,
  filter: {
    modelKey: "creta",
    activePricelistMatched: true,
    "featuresByKey.adas_package.available": true,
  },
  sort: { priceMin: 1, variantKey: 1 },
  limit: 10,
});

await explainFind({
  label: "Catalog exact feature key",
  collection: catalog,
  filter: { key: "sunroof" },
  limit: 5,
});

await explainFind({
  label: "Catalog alias lookup",
  collection: catalog,
  filter: { aliases: "music system" },
  limit: 5,
});

await mongoose.disconnect();
