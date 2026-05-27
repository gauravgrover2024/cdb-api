import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const STRICT = String(process.env.ACI_CORE_INDEX_AUDIT_STRICT || "").toLowerCase() === "true";

const CORE_COLLECTIONS = [
  {
    name: "aci_vehicle_model_summary",
    purpose: "Fast model/make discovery and model candidate retrieval",
    required: true,
    usefulIndexShapes: [
      ["brand", "model"],
      ["make", "model"],
      ["model"],
      ["modelKey"],
      ["normalizedModel"],
      ["displayName"],
    ],
    probeCandidates: [
      { label: "brand_model", fields: ["brand", "model"] },
      { label: "make_model", fields: ["make", "model"] },
      { label: "model", fields: ["model"] },
      { label: "modelKey", fields: ["modelKey"] },
      { label: "normalizedModel", fields: ["normalizedModel"] },
    ],
  },
  {
    name: "aci_vehicle_price_rows",
    purpose: "Fast price/variant/city/fuel/transmission retrieval",
    required: true,
    usefulIndexShapes: [
      ["brand", "model"],
      ["make", "model"],
      ["model"],
      ["variant"],
      ["model", "variant"],
      ["model", "fuelType"],
      ["model", "transmission"],
      ["model", "city"],
      ["city", "model"],
    ],
    probeCandidates: [
      { label: "model_variant", fields: ["model", "variant"] },
      { label: "brand_model", fields: ["brand", "model"] },
      { label: "make_model", fields: ["make", "model"] },
      { label: "model_fuel", fields: ["model", "fuelType"] },
      { label: "model_transmission", fields: ["model", "transmission"] },
      { label: "city_model", fields: ["city", "model"] },
      { label: "model", fields: ["model"] },
    ],
  },
  {
    name: "vehicle_feature_catalog_v2",
    purpose: "Fast feature alias/canonical feature lookup",
    required: true,
    usefulIndexShapes: [
      ["canonicalKey"],
      ["featureKey"],
      ["normalizedName"],
      ["displayName"],
      ["aliases"],
      ["category"],
    ],
    probeCandidates: [
      { label: "canonicalKey", fields: ["canonicalKey"] },
      { label: "featureKey", fields: ["featureKey"] },
      { label: "normalizedName", fields: ["normalizedName"] },
      { label: "displayName", fields: ["displayName"] },
      { label: "category", fields: ["category"] },
    ],
  },
  {
    name: "vehicle_variant_feature_matrix_v2",
    purpose: "Fast variant-feature availability and comparison retrieval",
    required: true,
    usefulIndexShapes: [
      ["model", "featureKeys"],
      ["brand", "model", "featureKeys"],
      ["make", "model", "featureKeys"],
      ["variant", "featureKeys"],
      ["model", "variant"],
      ["isActive", "model", "featureKeys"],
    ],
    knownImportantIndexNames: [
      "matrix_model_feature_explorer_active",
      "matrix_brand_model_feature_explorer_active",
    ],
    probeCandidates: [
      { label: "model_featureKeys", fields: ["model", "featureKeys"] },
      { label: "brand_model_featureKeys", fields: ["brand", "model", "featureKeys"] },
      { label: "make_model_featureKeys", fields: ["make", "model", "featureKeys"] },
      { label: "model_variant", fields: ["model", "variant"] },
      { label: "variant_featureKeys", fields: ["variant", "featureKeys"] },
    ],
  },
  {
    name: "vehicle_colors_v2",
    purpose: "Fast color lookup and color-specific answer retrieval",
    required: true,
    usefulIndexShapes: [
      ["brand", "model"],
      ["make", "model"],
      ["model"],
      ["model", "colors.name"],
      ["model", "colors.normalizedName"],
      ["model", "colors.colorName"],
    ],
    probeCandidates: [
      { label: "brand_model", fields: ["brand", "model"] },
      { label: "make_model", fields: ["make", "model"] },
      { label: "model", fields: ["model"] },
    ],
  },
];

const getByPath = (obj, path) => {
  const parts = String(path || "").split(".");
  let current = obj;
  for (const part of parts) {
    if (current == null) return undefined;
    if (Array.isArray(current)) {
      current = current[0];
    }
    current = current[part];
  }
  return current;
};

const isUsefulValue = (value) => {
  if (value == null) return false;
  if (typeof value === "string") return value.trim().length > 0;
  if (Array.isArray(value)) return value.length > 0;
  return true;
};

const buildFilterFromSample = (sample, fields = []) => {
  const filter = {};
  for (const field of fields) {
    const value = getByPath(sample, field);
    if (!isUsefulValue(value)) return null;
    filter[field] = Array.isArray(value) ? value[0] : value;
  }
  return Object.keys(filter).length ? filter : null;
};

const indexKeys = (index = {}) => Object.keys(index.key || {});

const hasIndexPrefix = (indexes = [], shape = []) =>
  indexes.some((index) => {
    const keys = indexKeys(index);
    if (keys.length < shape.length) return false;
    return shape.every((field, idx) => keys[idx] === field);
  });

const hasIndexContainingAll = (indexes = [], shape = []) =>
  indexes.some((index) => {
    const keys = indexKeys(index);
    return shape.every((field) => keys.includes(field));
  });

const summarizeIndexes = (indexes = []) =>
  indexes.map((index) => ({
    name: index.name,
    key: index.key,
    unique: Boolean(index.unique),
    sparse: Boolean(index.sparse),
    partialFilterExpression: index.partialFilterExpression || null,
  }));

const getWinningStage = (plan = {}) => {
  if (!plan || typeof plan !== "object") return null;
  if (plan.stage) return plan.stage;
  if (plan.inputStage) return getWinningStage(plan.inputStage);
  if (plan.inputStages?.length) return plan.inputStages.map(getWinningStage).filter(Boolean).join(",");
  if (plan.queryPlan) return getWinningStage(plan.queryPlan);
  if (plan.winningPlan) return getWinningStage(plan.winningPlan);
  return null;
};

const explainProbe = async (collection, probe, sample) => {
  const filter = buildFilterFromSample(sample, probe.fields);
  if (!filter) {
    return {
      label: probe.label,
      skipped: true,
      reason: "sample_missing_probe_fields",
      fields: probe.fields,
    };
  }

  const startedAt = Date.now();
  const explain = await collection
    .find(filter)
    .project({ _id: 1 })
    .limit(1)
    .explain("executionStats");

  const stage = getWinningStage(explain.queryPlanner?.winningPlan);
  const executionStats = explain.executionStats || {};

  return {
    label: probe.label,
    skipped: false,
    fields: probe.fields,
    filter,
    winningStage: stage,
    usesCollectionScan: String(stage || "").includes("COLLSCAN"),
    totalDocsExamined: executionStats.totalDocsExamined ?? null,
    totalKeysExamined: executionStats.totalKeysExamined ?? null,
    nReturned: executionStats.nReturned ?? null,
    durationMs: Date.now() - startedAt,
  };
};

const auditCollection = async (db, config) => {
  const failures = [];
  const warnings = [];
  const recommendations = [];

  const exists = await db.listCollections({ name: config.name }).hasNext();

  if (!exists) {
    const message = `Missing required collection: ${config.name}`;
    if (config.required) failures.push(message);
    else warnings.push(message);

    return {
      collection: config.name,
      purpose: config.purpose,
      exists: false,
      required: config.required,
      failures,
      warnings,
      recommendations,
    };
  }

  const collection = db.collection(config.name);
  const [count, indexes, sample] = await Promise.all([
    collection.estimatedDocumentCount(),
    collection.indexes(),
    collection.findOne({}),
  ]);

  if (!count) {
    warnings.push(`Collection ${config.name} has 0 documents`);
  }

  const indexSummary = summarizeIndexes(indexes);

  const usefulIndexMatches = (config.usefulIndexShapes || []).map((shape) => ({
    shape,
    hasPrefixMatch: hasIndexPrefix(indexes, shape),
    hasContainingMatch: hasIndexContainingAll(indexes, shape),
  }));

  const missingUsefulShapes = usefulIndexMatches
    .filter((item) => !item.hasPrefixMatch && !item.hasContainingMatch)
    .map((item) => item.shape);

  const knownImportantIndexes = (config.knownImportantIndexNames || []).map((name) => ({
    name,
    present: indexes.some((index) => index.name === name),
  }));

  for (const item of knownImportantIndexes) {
    if (!item.present) {
      warnings.push(`Known important index missing on ${config.name}: ${item.name}`);
    }
  }

  if (missingUsefulShapes.length) {
    recommendations.push({
      type: "review_missing_useful_index_shapes",
      shapes: missingUsefulShapes,
    });
  }

  const probes = [];
  if (sample) {
    for (const probe of config.probeCandidates || []) {
      try {
        const result = await explainProbe(collection, probe, sample);
        probes.push(result);

        if (!result.skipped && result.usesCollectionScan && count > 100) {
          warnings.push(`Probe ${config.name}.${probe.label} uses COLLSCAN on ${count} docs`);
        }
      } catch (error) {
        warnings.push(`Probe ${config.name}.${probe.label} explain failed: ${error.message}`);
        probes.push({
          label: probe.label,
          skipped: true,
          reason: error.message,
          fields: probe.fields,
        });
      }
    }
  } else {
    warnings.push(`Collection ${config.name} has no sample document for probes`);
  }

  if (STRICT && warnings.length) {
    failures.push(...warnings);
  }

  return {
    collection: config.name,
    purpose: config.purpose,
    exists: true,
    required: config.required,
    count,
    indexes: indexSummary,
    usefulIndexMatches,
    knownImportantIndexes,
    probes,
    failures,
    warnings,
    recommendations,
  };
};

const main = async () => {
  await connectDB();
  const db = mongoose.connection.db;

  const startedAt = Date.now();

  const collections = [];
  for (const config of CORE_COLLECTIONS) {
    collections.push(await auditCollection(db, config));
  }

  const failures = collections.flatMap((item) =>
    (item.failures || []).map((failure) => ({
      collection: item.collection,
      failure,
    })),
  );

  const warnings = collections.flatMap((item) =>
    (item.warnings || []).map((warning) => ({
      collection: item.collection,
      warning,
    })),
  );

  const recommendations = collections.flatMap((item) =>
    (item.recommendations || []).map((recommendation) => ({
      collection: item.collection,
      ...recommendation,
    })),
  );

  const summary = {
    suite: "ACI Core read-model and index audit",
    ok: failures.length === 0,
    strict: STRICT,
    durationMs: Date.now() - startedAt,
    databaseName: db.databaseName,
    checkedCollections: CORE_COLLECTIONS.length,
    failures,
    warnings,
    recommendations,
    collections,
  };

  console.log(JSON.stringify(summary, null, 2));

  if (failures.length > 0) {
    process.exit(1);
  }

  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI Core read-model and index audit",
    ok: false,
    error: error.message,
    stack: error.stack,
  }, null, 2));

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
