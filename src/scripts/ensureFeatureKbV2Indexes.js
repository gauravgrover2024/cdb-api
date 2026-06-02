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
const catalog = db.collection("vehicle_feature_catalog_v2");
const matrix = db.collection("vehicle_variant_feature_matrix_v2");

const ensureIndex = async (collection, key, options) => {
  try {
    await collection.createIndex(key, options);
    console.log("✅ ensured:", options.name);
  } catch (error) {
    console.error("❌ failed:", options.name, error.message);
  }
};

const hasIndexWithKey = (indexes, key) => {
  const wanted = JSON.stringify(key);
  return indexes.some((index) => JSON.stringify(index.key) === wanted);
};

console.log("=== FEATURE KB V2 INDEX HEALTH ===");

const nullKeyCount = await catalog.countDocuments({
  $or: [
    { key: null },
    { key: { $exists: false } },
  ],
});

const nullCanonicalKeyCount = await catalog.countDocuments({
  $or: [
    { canonicalKey: null },
    { canonicalKey: { $exists: false } },
  ],
});

console.log({
  catalogCount: await catalog.countDocuments(),
  matrixCount: await matrix.countDocuments(),
  catalogNullOrMissingKey: nullKeyCount,
  catalogNullOrMissingCanonicalKey: nullCanonicalKeyCount,
});

console.log("\n=== SAMPLE CATALOG DOCS WITH NULL/MISSING KEY ===");
console.dir(
  await catalog
    .find(
      {
        $or: [
          { key: null },
          { key: { $exists: false } },
        ],
      },
      {
        projection: {
          _id: 1,
          key: 1,
          canonicalKey: 1,
          displayName: 1,
          name: 1,
          aliases: 1,
          groupKey: 1,
        },
      },
    )
    .limit(10)
    .toArray(),
  { depth: 5 },
);

console.log("\n=== DROPPING OLD BROKEN INDEX NAME IF PRESENT ===");
for (const indexName of [
  "feature_catalog_key_unique",
  "feature_catalog_key_unique_partial",
  "feature_catalog_canonical_key_unique_partial",
]) {
  try {
    await catalog.dropIndex(indexName);
    console.log("Dropped:", indexName);
  } catch {
    console.log("Not present:", indexName);
  }
}

console.log("\n=== ENSURING CATALOG INDEXES ===");

// IMPORTANT:
// Do not use normal unique index on key because some docs may have key:null.
// This index only includes real string keys.
await ensureIndex(
  catalog,
  { key: 1 },
  {
    name: "feature_catalog_key_unique_partial",
    unique: true,
    partialFilterExpression: {
      key: { $type: "string", $gt: "" },
    },
  },
);

const catalogIndexes = await catalog.indexes();

if (hasIndexWithKey(catalogIndexes, { canonicalKey: 1 })) {
  console.log(
    "skipped: feature_catalog_canonical_key_unique_partial covered by existing canonicalKey index",
  );
} else {
  await ensureIndex(
    catalog,
    { canonicalKey: 1 },
    {
      name: "feature_catalog_canonical_key_unique_partial",
      unique: true,
      partialFilterExpression: {
        canonicalKey: { $type: "string", $gt: "" },
      },
    },
  );
}

if (hasIndexWithKey(catalogIndexes, { aliases: 1 })) {
  console.log("skipped: feature_catalog_aliases covered by existing aliases index");
} else {
  await ensureIndex(
    catalog,
    { aliases: 1 },
    {
      name: "feature_catalog_aliases",
      partialFilterExpression: {
        aliases: { $exists: true },
      },
    },
  );
}

await ensureIndex(
  catalog,
  { groupKey: 1, displayName: 1 },
  { name: "feature_catalog_group_display" },
);

console.log("\n=== ENSURING MATRIX INDEXES ===");

await ensureIndex(
  matrix,
  { modelKey: 1, activePricelistMatched: 1, priceMin: 1, variantKey: 1 },
  { name: "matrix_model_active_price_variant" },
);

await ensureIndex(
  matrix,
  { modelKey: 1, activePricelistMatched: 1, variantFamilyKey: 1, priceMin: 1 },
  { name: "matrix_model_active_variant_family_price" },
);

await ensureIndex(
  matrix,
  { modelKey: 1, activePricelistMatched: 1, variantKey: 1 },
  { name: "matrix_model_active_variant_key" },
);

await ensureIndex(
  matrix,
  { brandKey: 1, modelKey: 1, activePricelistMatched: 1 },
  { name: "matrix_brand_model_active" },
);

const hotFeatureKeys = [
  "sunroof",
  "adas_package",
  "six_airbags",
  "rear_camera",
  "camera",
  "ventilated_seats",
  "wireless_charging",
  "cruise_control",
  "alloy_wheels",
  "led_headlamps",
  "rear_ac_vents",
  "automatic_climate_control",
  "hill_hold",
  "tpms",
  "speakers",
  "integrated_2din_audio",
  "radio",
  "touchscreen",
  "android_auto",
  "apple_carplay",
];

for (const key of hotFeatureKeys) {
  await ensureIndex(
    matrix,
    {
      modelKey: 1,
      activePricelistMatched: 1,
      [`featuresByKey.${key}.available`]: 1,
      priceMin: 1,
      variantKey: 1,
    },
    {
      name: `matrix_model_active_${key}_price`.slice(0, 120),
      sparse: true,
    },
  );
}

console.log("\n=== CATALOG INDEXES ===");
console.table(
  (await catalog.indexes()).map((idx) => ({
    name: idx.name,
    key: JSON.stringify(idx.key),
    unique: !!idx.unique,
    sparse: !!idx.sparse,
    partial: idx.partialFilterExpression
      ? JSON.stringify(idx.partialFilterExpression)
      : "",
  })),
);

console.log("\n=== MATRIX INDEXES ===");
console.table(
  (await matrix.indexes()).map((idx) => ({
    name: idx.name,
    key: JSON.stringify(idx.key),
    unique: !!idx.unique,
    sparse: !!idx.sparse,
  })),
);

await mongoose.disconnect();
