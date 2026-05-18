import dotenv from "dotenv";
import mongoose from "mongoose";
import { askAciAssist } from "../services/aiAgent/aiAgent.service.js";

dotenv.config();

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const toArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value].filter(Boolean);
};

const getWidget = (response = {}) =>
  response.widget || toArray(response.widgets)[0] || {};

const summarizeFeatureMap = (features = {}) => {
  const entries = Object.entries(features || {});
  const sectionCounts = {};

  for (const [key] of entries) {
    const section = String(key).includes("|")
      ? String(key).split("|")[0].trim()
      : "Ungrouped";

    sectionCounts[section] = (sectionCounts[section] || 0) + 1;
  }

  return {
    totalKeys: entries.length,
    sectionCounts,
    firstTwenty: entries.slice(0, 20).map(([key, value]) => ({ key, value })),
  };
};

const inspectQuery = async (message) => {
  const response = await askAciAssist({
    message,
    context: {
      city: "new-delhi",
      anchorCity: "new-delhi",
    },
    user: {
      _id: "inspect-features",
      name: "Inspect Features",
    },
  });

  const widget = getWidget(response);
  const rows = toArray(
    response.rows ||
      response.items ||
      response.features ||
      response.data?.rows ||
      response.data?.features ||
      widget.rows ||
      widget.items ||
      widget.features,
  );

  console.log("\n============================================================");
  console.log("QUERY:", message);
  console.log("============================================================");

  console.log(
    JSON.stringify(
      {
        intent: response.intent,
        displayMode: response.displayMode,
        canvasType: response.canvasType || widget.canvasType,
        inlineType: response.inlineType,
        title: response.title,
        hasWidget: Boolean(widget && Object.keys(widget).length),
        widgetKeys: Object.keys(widget || {}),
        responseKeys: Object.keys(response || {}),
        dataKeys: Object.keys(response.data || {}),
        contextSnapshot: response.contextSnapshot,
        sourceTransparency: response.sourceTransparency,
        runtimeResultsMeta: response.runtimeResultsMeta,
        rowsCount: rows.length,
      },
      null,
      2,
    ),
  );

  const sample = rows[0] || {};
  console.log("\n--- SAMPLE ROW SHAPE ---");
  console.log(
    JSON.stringify(
      {
        keys: Object.keys(sample),
        id: sample.id || sample._id,
        brand: sample.brand || sample.make,
        model: sample.model,
        variant: sample.variant || sample.variantName || sample.label,
        label: sample.label,
        hasRaw: Boolean(sample.raw),
        hasFeaturesObject: Boolean(sample.features && !Array.isArray(sample.features)),
        hasFeaturesArray: Array.isArray(sample.features),
        featureSummary: Array.isArray(sample.features)
          ? {
              length: sample.features.length,
              firstTen: sample.features.slice(0, 10),
            }
          : summarizeFeatureMap(sample.features || sample.raw?.features || {}),
      },
      null,
      2,
    ),
  );

  const widgetFeatureGroups = toArray(widget.featureGroups);
  const widgetVariants = toArray(widget.variants || widget.variantOptions);

  console.log("\n--- UI READINESS CHECK ---");
  console.log(
    JSON.stringify(
      {
        hasVariants: widgetVariants.length > 0,
        variantsCount: widgetVariants.length,
        firstVariants: widgetVariants.slice(0, 8).map((variant) => ({
          label: variant.label || variant.name || variant.variant,
          featureCount: variant.featureCount,
          availableCount: variant.availableCount,
        })),
        hasFeatureGroups: widgetFeatureGroups.length > 0,
        featureGroupsCount: widgetFeatureGroups.length,
        firstGroups: widgetFeatureGroups.slice(0, 8).map((group) => ({
          label: group.label || group.name,
          count: toArray(group.features).length,
          availableCount: group.availableCount,
          totalCount: group.totalCount,
        })),
        hasQuickSpecs: toArray(widget.quickSpecs).length > 0,
        quickSpecsCount: toArray(widget.quickSpecs).length,
        hasHighlights: toArray(widget.highlights).length > 0,
        highlightsCount: toArray(widget.highlights).length,
      },
      null,
      2,
    ),
  );
};

const main = async () => {
  if (mongoUri && mongoose.connection.readyState !== 1) {
    await mongoose.connect(mongoUri);
    console.log("MongoDB connected");
  }

  const queries = process.argv.slice(2);

  const finalQueries = queries.length
    ? queries
    : [
        "Show features of Verna",
        "Show all features of Verna SX",
        "Does Verna SX have sunroof?",
        "Which Verna variants have sunroof?",
      ];

  for (const query of finalQueries) {
    await inspectQuery(query);
  }

  if (mongoose.connection.readyState === 1) {
    await mongoose.disconnect();
  }
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) {
    await mongoose.disconnect();
  }
  process.exit(1);
});
