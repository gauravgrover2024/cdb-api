import "dotenv/config";
import fs from "fs";
import path from "path";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const TS = new Date().toISOString().replace(/[:.]/g, "-");
const OUT_DIR = path.resolve(`aci_db_performance_audit_${TS}`);
const OUT_JSON = path.join(OUT_DIR, "aci_db_performance_audit.json");
const OUT_MD = path.join(OUT_DIR, "aci_db_performance_audit.md");

const IMPORTANT_COLLECTION_RE =
  /(vehicle|car|color|colour|feature|price|offer|lead|quotation|quote|customer|conversation|ai)/i;

const TYPE_LIMIT = 80;

const ensureDir = (dir) => {
  fs.mkdirSync(dir, { recursive: true });
};

const safeString = (value) => {
  if (value === null || value === undefined) return "";
  return String(value).trim();
};

const isPlainObject = (value) =>
  Boolean(value && typeof value === "object" && !Array.isArray(value));

const normalizeKey = (value = "") =>
  safeString(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const byteSize = (value) => Buffer.byteLength(JSON.stringify(value || {}));

const getByPath = (object, dottedPath) => {
  if (!object || !dottedPath) return undefined;
  return dottedPath.split(".").reduce((acc, key) => acc?.[key], object);
};

const flattenKeys = (object, prefix = "", out = []) => {
  if (!isPlainObject(object)) return out;

  for (const [key, value] of Object.entries(object)) {
    const full = prefix ? `${prefix}.${key}` : key;
    out.push(full);

    if (isPlainObject(value) && out.length < 500) {
      flattenKeys(value, full, out);
    }
  }

  return out;
};

const typeOfValue = (value) => {
  if (Array.isArray(value)) return `array(${value.length})`;
  if (value === null) return "null";
  if (value instanceof Date) return "date";
  return typeof value;
};

const summarizeShape = (doc = {}) => {
  const topLevel = Object.entries(doc || {})
    .slice(0, TYPE_LIMIT)
    .map(([key, value]) => ({
      key,
      type: typeOfValue(value),
      sizeBytes: byteSize(value),
    }));

  const flattened = flattenKeys(doc).slice(0, 250);

  const heavyTopLevel = [...topLevel]
    .sort((a, b) => b.sizeBytes - a.sizeBytes)
    .slice(0, 15);

  return {
    topLevel,
    flattened,
    heavyTopLevel,
    approxDocSizeBytes: byteSize(doc),
  };
};

const compactExplain = (explain = {}) => {
  const stats = explain.executionStats || {};
  const queryPlanner = explain.queryPlanner || {};

  const winningPlan = queryPlanner.winningPlan || {};
  const stages = [];

  const walkStage = (stage, depth = 0) => {
    if (!stage || typeof stage !== "object" || depth > 8) return;

    if (stage.stage) {
      stages.push({
        stage: stage.stage,
        indexName: stage.indexName || "",
        direction: stage.direction || "",
        filter: stage.filter || undefined,
        keyPattern: stage.keyPattern || undefined,
      });
    }

    for (const key of ["inputStage", "inputStages", "outerStage", "innerStage", "shards"]) {
      const next = stage[key];
      if (Array.isArray(next)) next.forEach((item) => walkStage(item, depth + 1));
      else if (next && typeof next === "object") walkStage(next, depth + 1);
    }
  };

  walkStage(winningPlan);

  return {
    executionTimeMillis: stats.executionTimeMillis ?? null,
    nReturned: stats.nReturned ?? null,
    totalKeysExamined: stats.totalKeysExamined ?? null,
    totalDocsExamined: stats.totalDocsExamined ?? null,
    stageSummary: stages,
    usesCollscan: stages.some((s) => s.stage === "COLLSCAN"),
    usesIndex: stages.some((s) => s.stage === "IXSCAN"),
  };
};

const scoreExplain = (compact = {}) => {
  if (compact.usesCollscan) return "danger";
  if ((compact.totalDocsExamined || 0) > 1000 && !compact.usesIndex) return "danger";
  if ((compact.totalDocsExamined || 0) > 250) return "warning";
  if ((compact.executionTimeMillis || 0) > 100) return "warning";
  return "ok";
};

const queryFromFields = (sample = {}, fields = []) => {
  const query = {};

  for (const field of fields) {
    const value = getByPath(sample, field);
    if (value === undefined || value === null || value === "") return null;
    if (typeof value === "object") return null;
    query[field] = value;
  }

  return Object.keys(query).length ? query : null;
};

const maybeAddQuery = (queries, label, query, projection = null, sort = null) => {
  if (!query || !Object.keys(query).length) return;
  const key = JSON.stringify({ label, query, projection, sort });
  if (queries.some((item) => JSON.stringify({
    label: item.label,
    query: item.query,
    projection: item.projection,
    sort: item.sort,
  }) === key)) return;

  queries.push({ label, query, projection, sort });
};

const buildQueriesForCollection = ({ name, sample }) => {
  const lower = name.toLowerCase();
  const queries = [];

  const commonProjection = {
    _id: 1,
    make: 1,
    brand: 1,
    model: 1,
    variant: 1,
    fullModel: 1,
    modelKey: 1,
    makeKey: 1,
    variantKey: 1,
    city: 1,
    citySlug: 1,
    exShowroomPrice: 1,
    onRoadPrice: 1,
    imageUrl: 1,
    normalizedImageUrl: 1,
  };

  maybeAddQuery(
    queries,
    "makeKey + modelKey + citySlug",
    queryFromFields(sample, ["makeKey", "modelKey", "citySlug"]),
    commonProjection,
  );

  maybeAddQuery(
    queries,
    "brand + model + citySlug",
    queryFromFields(sample, ["brand", "model", "citySlug"]),
    commonProjection,
  );

  maybeAddQuery(
    queries,
    "make + model + citySlug",
    queryFromFields(sample, ["make", "model", "citySlug"]),
    commonProjection,
  );

  maybeAddQuery(
    queries,
    "modelKey",
    queryFromFields(sample, ["modelKey"]),
    commonProjection,
  );

  maybeAddQuery(
    queries,
    "model exact",
    queryFromFields(sample, ["model"]),
    commonProjection,
  );

  maybeAddQuery(
    queries,
    "modelKey + variantKey + citySlug",
    queryFromFields(sample, ["modelKey", "variantKey", "citySlug"]),
    commonProjection,
  );

  maybeAddQuery(
    queries,
    "model + variant + citySlug",
    queryFromFields(sample, ["model", "variant", "citySlug"]),
    commonProjection,
  );

  maybeAddQuery(
    queries,
    "modelKey + fuelKey + transmissionKey + citySlug",
    queryFromFields(sample, ["modelKey", "fuelKey", "transmissionKey", "citySlug"]),
    commonProjection,
  );

  maybeAddQuery(
    queries,
    "bodyTypeKey + price sort",
    queryFromFields(sample, ["bodyTypeKey"]),
    commonProjection,
    { exShowroomPriceNumeric: 1 },
  );

  if (/color|colour/.test(lower)) {
    maybeAddQuery(
      queries,
      "color modelKey",
      queryFromFields(sample, ["modelKey"]),
      { model: 1, modelKey: 1, colorName: 1, name: 1, hex: 1, imageUrl: 1, normalizedImageUrl: 1 },
    );

    maybeAddQuery(
      queries,
      "color modelKey + colorName",
      queryFromFields(sample, ["modelKey", "colorName"]),
      { model: 1, modelKey: 1, colorName: 1, name: 1, hex: 1, imageUrl: 1, normalizedImageUrl: 1 },
    );
  }

  if (/feature/.test(lower)) {
    maybeAddQuery(
      queries,
      "feature modelKey",
      queryFromFields(sample, ["modelKey"]),
      { model: 1, modelKey: 1, variant: 1, variantKey: 1, features: 1 },
    );

    maybeAddQuery(
      queries,
      "feature modelKey + variantKey",
      queryFromFields(sample, ["modelKey", "variantKey"]),
      { model: 1, modelKey: 1, variant: 1, variantKey: 1, features: 1 },
    );

    maybeAddQuery(
      queries,
      "featureKey",
      queryFromFields(sample, ["featureKey"]),
      { model: 1, modelKey: 1, variant: 1, featureKey: 1, value: 1 },
    );
  }

  if (/lead|quotation|quote|customer/.test(lower)) {
    maybeAddQuery(
      queries,
      "lead mobile",
      queryFromFields(sample, ["mobile"]),
      { name: 1, mobile: 1, city: 1, leadStage: 1, createdAt: 1 },
    );

    maybeAddQuery(
      queries,
      "lead status createdAt",
      queryFromFields(sample, ["status"]),
      { name: 1, mobile: 1, status: 1, leadStage: 1, createdAt: 1 },
      { createdAt: -1 },
    );
  }

  return queries;
};

const classifyCollection = (name = "") => {
  const lower = name.toLowerCase();
  if (/vehicle|car/.test(lower)) return "vehicle";
  if (/color|colour/.test(lower)) return "color";
  if (/feature/.test(lower)) return "feature";
  if (/price/.test(lower)) return "price";
  if (/offer/.test(lower)) return "offer";
  if (/lead|quotation|quote/.test(lower)) return "lead";
  if (/customer/.test(lower)) return "customer";
  if (/conversation|ai/.test(lower)) return "ai";
  return "other";
};

const recommendedIndexesFor = ({ name, sampleKeys = [] }) => {
  const type = classifyCollection(name);
  const keys = new Set(sampleKeys);
  const recs = [];

  const has = (...fields) => fields.every((field) => keys.has(field));

  if (type === "vehicle" || type === "price") {
    if (has("makeKey", "modelKey", "citySlug")) {
      recs.push("{ makeKey: 1, modelKey: 1, citySlug: 1 }");
    }
    if (has("modelKey", "variantKey", "citySlug")) {
      recs.push("{ modelKey: 1, variantKey: 1, citySlug: 1 }");
    }
    if (has("modelKey", "fuelKey", "transmissionKey", "citySlug")) {
      recs.push("{ modelKey: 1, fuelKey: 1, transmissionKey: 1, citySlug: 1 }");
    }
    if (has("bodyTypeKey", "exShowroomPriceNumeric")) {
      recs.push("{ bodyTypeKey: 1, exShowroomPriceNumeric: 1 }");
    }
    if (has("make", "model", "citySlug") && !has("makeKey", "modelKey")) {
      recs.push("Add makeKey/modelKey normalized fields, then index them.");
    }
  }

  if (type === "color") {
    if (has("modelKey")) recs.push("{ modelKey: 1 }");
    if (has("makeKey", "modelKey")) recs.push("{ makeKey: 1, modelKey: 1 }");
    if (has("modelKey", "colorKey")) recs.push("{ modelKey: 1, colorKey: 1 }");
  }

  if (type === "feature") {
    if (has("modelKey")) recs.push("{ modelKey: 1 }");
    if (has("modelKey", "variantKey")) recs.push("{ modelKey: 1, variantKey: 1 }");
    if (has("modelKey", "featureKey")) recs.push("{ modelKey: 1, featureKey: 1 }");
    if (!has("featureKey")) recs.push("Add normalized featureKey if feature lookup is frequent.");
  }

  if (type === "lead" || type === "customer") {
    if (has("mobile")) recs.push("{ mobile: 1 }");
    if (has("createdAt")) recs.push("{ createdAt: -1 }");
    if (has("status", "createdAt")) recs.push("{ status: 1, createdAt: -1 }");
    if (has("leadStage", "createdAt")) recs.push("{ leadStage: 1, createdAt: -1 }");
    if (has("selectedVehicle.modelKey")) recs.push("{ 'selectedVehicle.modelKey': 1, createdAt: -1 }");
  }

  return [...new Set(recs)];
};

const auditCollection = async (db, collectionInfo) => {
  const name = collectionInfo.name;
  const collection = db.collection(name);

  const indexes = await collection.indexes().catch((error) => [
    { error: error?.message || "Could not load indexes" },
  ]);

  const count = await collection.estimatedDocumentCount().catch(() => null);
  const sample = await collection.findOne({}).catch(() => null);
  const shape = sample ? summarizeShape(sample) : null;
  const sampleKeys = sample ? flattenKeys(sample).slice(0, 500) : [];
  const queries = sample ? buildQueriesForCollection({ name, sample }) : [];
  const explains = [];

  for (const q of queries) {
    try {
      const cursor = collection.find(q.query);
      if (q.projection) cursor.project(q.projection);
      if (q.sort) cursor.sort(q.sort);
      cursor.limit(20);

      const explain = await cursor.explain("executionStats");
      const compact = compactExplain(explain);

      explains.push({
        label: q.label,
        query: q.query,
        projection: q.projection,
        sort: q.sort,
        ...compact,
        score: scoreExplain(compact),
      });
    } catch (error) {
      explains.push({
        label: q.label,
        query: q.query,
        projection: q.projection,
        sort: q.sort,
        error: error?.message || "Explain failed",
        score: "error",
      });
    }
  }

  const warnings = [];

  if (shape?.approxDocSizeBytes > 200_000) {
    warnings.push("Very large sample document. Consider smaller projections or splitting heavy media/gallery fields.");
  } else if (shape?.approxDocSizeBytes > 75_000) {
    warnings.push("Large sample document. Ensure common card/list queries use projections.");
  }

  for (const explain of explains) {
    if (explain.usesCollscan) {
      warnings.push(`COLLSCAN detected for query: ${explain.label}`);
    }
    if ((explain.totalDocsExamined || 0) > 1000) {
      warnings.push(`High docs examined for query: ${explain.label} (${explain.totalDocsExamined})`);
    }
  }

  return {
    name,
    type: classifyCollection(name),
    count,
    indexes,
    sampleShape: shape,
    sampleKeys,
    explains,
    recommendedIndexes: recommendedIndexesFor({ name, sampleKeys }),
    warnings: [...new Set(warnings)],
  };
};

const markdownTableRow = (items = []) =>
  `| ${items.map((item) => String(item ?? "").replace(/\n/g, " ")).join(" | ")} |`;

const renderMarkdown = (report) => {
  const lines = [];

  lines.push("# ACI Assist MongoDB Performance + Index Audit");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`- Database: ${report.databaseName}`);
  lines.push(`- Collections scanned: ${report.collections.length}`);
  lines.push(`- Important collections audited: ${report.auditedCollections.length}`);
  lines.push(`- Total warnings: ${report.warningCount}`);
  lines.push("");

  lines.push("## High-risk warnings");
  lines.push("");
  if (!report.highRiskWarnings.length) {
    lines.push("No high-risk warnings found by this script.");
  } else {
    for (const warning of report.highRiskWarnings) {
      lines.push(`- **${warning.collection}**: ${warning.warning}`);
    }
  }
  lines.push("");

  lines.push("## Collection overview");
  lines.push("");
  lines.push(markdownTableRow(["Collection", "Type", "Count", "Indexes", "Warnings"]));
  lines.push(markdownTableRow(["---", "---", "---:", "---:", "---:"]));
  for (const c of report.auditedCollections) {
    lines.push(markdownTableRow([
      c.name,
      c.type,
      c.count ?? "unknown",
      Array.isArray(c.indexes) ? c.indexes.length : "unknown",
      c.warnings.length,
    ]));
  }
  lines.push("");

  for (const c of report.auditedCollections) {
    lines.push(`## ${c.name}`);
    lines.push("");
    lines.push(`- Type: ${c.type}`);
    lines.push(`- Estimated count: ${c.count ?? "unknown"}`);
    lines.push(`- Approx sample document size: ${c.sampleShape?.approxDocSizeBytes ?? "n/a"} bytes`);
    lines.push("");

    lines.push("### Existing indexes");
    lines.push("");
    if (!Array.isArray(c.indexes) || !c.indexes.length) {
      lines.push("No indexes returned.");
    } else {
      for (const index of c.indexes) {
        lines.push(`- \`${index.name || "unnamed"}\`: \`${JSON.stringify(index.key || index)}\``);
      }
    }
    lines.push("");

    lines.push("### Suggested index/data-shape notes");
    lines.push("");
    if (!c.recommendedIndexes.length) {
      lines.push("- No automatic recommendation. Review manually.");
    } else {
      for (const rec of c.recommendedIndexes) {
        lines.push(`- ${rec}`);
      }
    }
    lines.push("");

    lines.push("### Query explain results");
    lines.push("");
    if (!c.explains.length) {
      lines.push("No explain queries generated from sample shape.");
    } else {
      lines.push(markdownTableRow([
        "Query",
        "Score",
        "ms",
        "Returned",
        "Keys",
        "Docs",
        "Index?",
        "COLLSCAN?",
      ]));
      lines.push(markdownTableRow(["---", "---", "---:", "---:", "---:", "---:", "---", "---"]));

      for (const e of c.explains) {
        lines.push(markdownTableRow([
          e.label,
          e.score,
          e.executionTimeMillis ?? "",
          e.nReturned ?? "",
          e.totalKeysExamined ?? "",
          e.totalDocsExamined ?? "",
          e.usesIndex ? "yes" : "no",
          e.usesCollscan ? "YES" : "no",
        ]));
      }
    }
    lines.push("");

    lines.push("### Heaviest top-level fields in sample");
    lines.push("");
    if (!c.sampleShape?.heavyTopLevel?.length) {
      lines.push("No sample shape.");
    } else {
      lines.push(markdownTableRow(["Field", "Type", "Approx bytes"]));
      lines.push(markdownTableRow(["---", "---", "---:"]));
      for (const item of c.sampleShape.heavyTopLevel) {
        lines.push(markdownTableRow([item.key, item.type, item.sizeBytes]));
      }
    }
    lines.push("");

    if (c.warnings.length) {
      lines.push("### Warnings");
      lines.push("");
      for (const warning of c.warnings) {
        lines.push(`- ${warning}`);
      }
      lines.push("");
    }
  }

  lines.push("## Manual interpretation checklist");
  lines.push("");
  lines.push("- Any COLLSCAN on vehicles/features/colors/prices is a launch blocker for high scale.");
  lines.push("- Common public queries should use normalized keys, not case-insensitive regex scans.");
  lines.push("- Common list/card queries should use projections and avoid pulling full image galleries/features when not needed.");
  lines.push("- Lead/customer collections need indexes for mobile, stage/status, source, createdAt, and selected vehicle.");
  lines.push("- If current schema lacks modelKey/variantKey/citySlug/featureKey, add backfill before large-scale launch.");
  lines.push("- After index changes, rerun this audit and compare executionStats.");
  lines.push("");

  return lines.join("\n");
};

const run = async () => {
  ensureDir(OUT_DIR);

  await connectDB();
  const db = mongoose.connection.db;

  const collections = await db.listCollections().toArray();
  const important = collections
    .filter((c) => IMPORTANT_COLLECTION_RE.test(c.name))
    .sort((a, b) => a.name.localeCompare(b.name));

  const auditedCollections = [];

  for (const c of important) {
    console.log(`Auditing collection: ${c.name}`);
    auditedCollections.push(await auditCollection(db, c));
  }

  const highRiskWarnings = auditedCollections.flatMap((c) =>
    c.warnings.map((warning) => ({
      collection: c.name,
      warning,
    })),
  );

  const report = {
    generatedAt: new Date().toISOString(),
    databaseName: db.databaseName,
    collections: collections.map((c) => c.name).sort(),
    auditedCollections,
    warningCount: highRiskWarnings.length,
    highRiskWarnings,
  };

  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(OUT_MD, renderMarkdown(report));

  console.log("");
  console.log("✅ ACI DB performance audit complete");
  console.log(OUT_MD);
  console.log(OUT_JSON);

  await mongoose.disconnect();
};

run().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
