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

const money = (value) => {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) return "";
  if (amount >= 100000) return `₹${(amount / 100000).toFixed(2)}L`;
  return `₹${amount.toLocaleString("en-IN")}`;
};

const pickRows = (response = {}, widget = {}) =>
  toArray(
    response.rows ||
      response.items ||
      response.features ||
      response.data?.rows ||
      response.data?.features ||
      widget.rows ||
      widget.items ||
      widget.features ||
      widget.matchedVariants,
  );

const pickVariants = (response = {}, widget = {}) =>
  toArray(
    widget.variantOptions ||
      widget.variants ||
      response.data?.variantOptions ||
      response.data?.variants,
  );

const pickAllVariants = (response = {}, widget = {}) =>
  toArray(widget.allVariants || response.data?.allVariants || widget.variants || response.data?.variants);

const pickFeatureGroups = (response = {}, widget = {}) =>
  toArray(widget.featureGroups || response.data?.featureGroups);

const pickQuickSpecs = (response = {}, widget = {}) =>
  toArray(widget.quickSpecs || response.data?.quickSpecs);

const pickHighlights = (response = {}, widget = {}) =>
  toArray(widget.highlights || response.data?.highlights);

const variantMini = (variant = {}) => ({
  label: variant.label || variant.variant || variant.variantName || "",
  active: variant.active,
  current: variant.current,
  currentPricelistMatched: variant.currentPricelistMatched,
  discontinued: variant.discontinued,
  exShowroomPrice: variant.exShowroomPrice || null,
  exShowroomPriceLabel: money(variant.exShowroomPrice),
  onRoadPrice: variant.onRoadPrice || null,
  onRoadPriceLabel: money(variant.onRoadPrice),
  price: variant.price || null,
  priceLabel: money(variant.price),
  featureCount: variant.featureCount,
  availableCount: variant.availableCount,
  fuel: variant.fuel || "",
  transmission: variant.transmission || "",
  vehicleRowId: variant.vehicleRowId || "",
  lastSeenDate: variant.lastSeenDate || "",
});

const featureMini = (feature = {}) => ({
  label: feature.label || feature.name || feature.feature || "",
  section: feature.section || "",
  category: feature.category || "",
  value: feature.value || feature.displayValue || "",
  available: feature.available,
  variant: feature.variant || feature.variantName || "",
});

const groupMini = (group = {}) => ({
  label: group.label || group.name || "",
  category: group.category || "",
  availableCount: group.availableCount,
  unavailableCount: group.unavailableCount,
  totalCount: group.totalCount,
  featureSample: toArray(group.features).slice(0, 5).map(featureMini),
});

const specMini = (spec = {}) => ({
  label: spec.label || spec.name || "",
  value: spec.value || spec.displayValue || "",
  icon: spec.icon || "",
});

const highlightMini = (highlight = {}) => ({
  label: highlight.label || highlight.text || String(highlight || ""),
});

const discoveryRowMini = (row = {}) => ({
  variant: row.variant || row.variantName || row.label || "",
  feature: row.feature || row.matchedFeature || "",
  section: row.section || "",
  value: row.value || row.displayValue || "",
  available: row.available,
  exShowroomPrice: row.exShowroomPrice || null,
  exShowroomPriceLabel: money(row.exShowroomPrice),
  onRoadPrice: row.onRoadPrice || null,
  onRoadPriceLabel: money(row.onRoadPrice),
  featureCount: row.featureCount,
  availableCount: row.availableCount,
});

const inspectQuery = async (message) => {
  const response = await askAciAssist({
    message,
    context: {
      city: "new-delhi",
      anchorCity: "new-delhi",
    },
    user: {
      _id: "inspect-features-backend-contract",
      name: "Inspect Features Backend Contract",
    },
  });

  const widget = getWidget(response);
  const rows = pickRows(response, widget);
  const variants = pickVariants(response, widget);
  const allVariants = pickAllVariants(response, widget);
  const groups = pickFeatureGroups(response, widget);
  const quickSpecs = pickQuickSpecs(response, widget);
  const highlights = pickHighlights(response, widget);

  const activeVariants = variants.filter((variant) => variant.active === true || variant.current === true);
  const inactiveVariants = variants.filter((variant) => variant.active === false || variant.current === false);

  const selectedVariant =
    widget.selectedVariant ||
    response.data?.selectedVariant ||
    response.contextSnapshot?.anchorVariant ||
    "";

  const selectedVariantObj =
    variants.find(
      (variant) =>
        String(variant.label || variant.variant || variant.variantName || "").toLowerCase() ===
        String(selectedVariant || "").toLowerCase(),
    ) ||
    allVariants.find(
      (variant) =>
        String(variant.label || variant.variant || variant.variantName || "").toLowerCase() ===
        String(selectedVariant || "").toLowerCase(),
    ) ||
    null;

  console.log("\n============================================================");
  console.log("QUERY:", message);
  console.log("============================================================");

  console.log(
    JSON.stringify(
      {
        resultContract: {
          intent: response.intent,
          displayMode: response.displayMode,
          canvasType: response.canvasType || widget.canvasType || "",
          inlineType: response.inlineType || "",
          title: response.title,
          hasWidget: Boolean(widget && Object.keys(widget).length),
          rowsCount: rows.length,
          widgetKeys: Object.keys(widget || {}),
        },

        context: response.contextSnapshot,

        runtime: {
          sourceTransparency: response.sourceTransparency,
          runtimeResultsMeta: response.runtimeResultsMeta,
        },

        featureExplorerBackend: {
          selectedVariant,
          selectedVariantId: widget.selectedVariantId || response.data?.selectedVariantId || "",
          selectedVariantObject: selectedVariantObj ? variantMini(selectedVariantObj) : null,

          activeStatusSource:
            widget.activeStatusSource ||
            response.data?.activeStatusSource ||
            "",

          activeVariantCount:
            widget.activeVariantCount ??
            response.data?.activeVariantCount ??
            activeVariants.length,

          totalRawVariantCount:
            widget.totalRawVariantCount ??
            response.data?.totalRawVariantCount ??
            allVariants.length,

          selectedVariantIsActive:
            widget.selectedVariantIsActive ??
            response.data?.selectedVariantIsActive ??
            selectedVariantObj?.active ??
            null,

          currentPricelistMatched:
            widget.currentPricelistMatched ??
            response.data?.currentPricelistMatched ??
            selectedVariantObj?.currentPricelistMatched ??
            null,

          totalVariantOptions: variants.length,
          totalAllVariants: allVariants.length,
          activeVariantOptions: activeVariants.length,
          inactiveVariantOptions: inactiveVariants.length,

          totalFeatureCount:
            widget.totalFeatureCount ||
            response.data?.totalFeatureCount ||
            rows.length,

          availableFeatureCount:
            widget.availableFeatureCount ||
            response.data?.availableFeatureCount ||
            rows.filter((row) => row.available === true).length,

          featureGroupsCount: groups.length,
          quickSpecsCount: quickSpecs.length,
          highlightsCount: highlights.length,
        },

        variantOrdering: {
          firstTenVariantOptions: variants.slice(0, 10).map(variantMini),
          middleVariantOption: variants.length
            ? variantMini(variants[Math.floor((variants.length - 1) / 2)])
            : null,
          lastFiveVariantOptions: variants.slice(-5).map(variantMini),
        },

        featureGroups: groups.slice(0, 12).map(groupMini),

        quickSpecs: quickSpecs.slice(0, 12).map(specMini),

        highlights: highlights.slice(0, 10).map(highlightMini),

        rowSample:
          response.intent === "vehicle_feature_discovery"
            ? rows.slice(0, 12).map(discoveryRowMini)
            : rows.slice(0, 12).map(featureMini),
      },
      null,
      2,
    ),
  );

  const warnings = [];

  if (
    response.intent === "vehicle_model_features_explorer" &&
    (response.canvasType || widget.canvasType) === "features_explorer_canvas"
  ) {
    if (!variants.length) warnings.push("No variants/variantOptions found.");
    if (!groups.length) warnings.push("No featureGroups found.");
    if (!quickSpecs.length) warnings.push("No quickSpecs found.");
    if (!highlights.length) warnings.push("No highlights found.");
    if (!selectedVariant) warnings.push("No selectedVariant found.");
    if ((widget.activeStatusSource || response.data?.activeStatusSource) !== "vehicles") {
      warnings.push("Active status is not confirmed from vehicles collection.");
    }
    if ((widget.currentPricelistMatched ?? response.data?.currentPricelistMatched) !== true) {
      warnings.push("Selected variant is not confirmed matched to current pricelist.");
    }
  }

  if (response.intent === "vehicle_feature_discovery") {
    if (!rows.length) warnings.push("Feature discovery returned zero matched rows.");
    if (!variants.length) warnings.push("Feature discovery has no variant list.");
  }

  console.log("\n--- BACKEND WARNINGS ---");
  if (warnings.length) {
    warnings.forEach((warning) => console.log(`⚠️ ${warning}`));
  } else {
    console.log("✅ No backend contract warnings for this query.");
  }
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
