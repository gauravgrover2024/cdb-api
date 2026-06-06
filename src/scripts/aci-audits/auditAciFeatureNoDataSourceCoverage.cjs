#!/usr/bin/env node

require("dotenv").config();

const mongoose = require("mongoose");

const CASES = [
  {
    ids: ["B41", "F104"],
    message: "i20 sportz features",
    brand: "Hyundai",
    model: "I20",
    variant: "Sportz",
    requestType: "full features",
  },
  {
    ids: ["B42"],
    message: "baleno alpha features",
    brand: "Maruti",
    model: "Baleno",
    variant: "Alpha",
    requestType: "full features",
  },
  {
    ids: ["B53"],
    message: "what features do I lose",
    brand: "Maruti",
    model: "Baleno",
    variant: "Alpha",
    requestType: "feature loss",
  },
  {
    ids: ["B44"],
    message: "city zx features",
    brand: "Honda",
    model: "City",
    variant: "ZX",
    requestType: "full features",
  },
  {
    ids: ["B49", "F111"],
    message: "contextual Creta features",
    brand: "Hyundai",
    model: "Creta",
    variant: "",
    requestType: "full features",
  },
  {
    ids: ["B52"],
    message: "Scorpio N safety features",
    brand: "Mahindra",
    model: "Scorpio N",
    variant: "",
    requestType: "safety features",
    topicTerms: ["safety", "airbag", "abs", "brake", "stability", "traction", "isofix", "sensor", "camera", "adas"],
  },
];

const text = (value = "") => String(value || "").replace(/\s+/g, " ").trim();

const escapeRegex = (value = "") => String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const rxContains = (value = "") => new RegExp(escapeRegex(text(value)), "i");

const rxExact = (value = "") => new RegExp(`^${escapeRegex(text(value))}$`, "i");

const normalize = (value = "") =>
  text(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const unique = (items = []) => [...new Set(items.filter(Boolean))];

const featureKeys = (row = {}) => Object.keys(row.features || {});

const featureSample = ({ row = {}, topicTerms = [] } = {}) => {
  const keys = featureKeys(row);
  const normalizedTerms = topicTerms.map(normalize).filter(Boolean);
  const matching = normalizedTerms.length
    ? keys.filter((key) => normalizedTerms.some((term) => normalize(key).includes(term)))
    : [];

  return {
    brand: row.brand || "",
    model: row.model || "",
    variant: row.variant || "",
    featureCount: keys.length,
    topicFeatureCount: matching.length,
    sampleFeatureKeys: (matching.length ? matching : keys).slice(0, 8),
  };
};

const activeVehicleSignal = (rows = []) => {
  const activeRows = rows.filter((row = {}) => row.is_discontinued !== true && !/discontinued|inactive/i.test(text(row.lifecycleStatus)));
  const discontinuedRows = rows.filter((row = {}) => row.is_discontinued === true || /discontinued/i.test(text(row.lifecycleStatus)));
  const inactiveRows = rows.filter((row = {}) => /inactive/i.test(text(row.lifecycleStatus)));
  const latestSeen = rows
    .map((row = {}) => text(row.LastSeenDate || row.lastSeenAt || row.updatedAt))
    .filter(Boolean)
    .sort()
    .pop() || "";

  return {
    status: activeRows.length
      ? "active"
      : discontinuedRows.length
        ? "discontinued"
        : inactiveRows.length
          ? "inactive"
          : rows.length
            ? "unknown"
            : "missing",
    activeRows: activeRows.length,
    discontinuedRows: discontinuedRows.length,
    inactiveRows: inactiveRows.length,
    totalRows: rows.length,
    latestSeen,
    sample: rows.slice(0, 5).map((row = {}) => ({
      brand: row.brand || "",
      model: row.model || "",
      variant: row.variantDisplayName || row.variant || "",
      city: row.city || "",
      isDiscontinued: Boolean(row.is_discontinued),
      lastSeen: row.LastSeenDate || row.lastSeenAt || "",
    })),
  };
};

const sourceQueryForCase = ({ brand = "", model = "", variant = "" } = {}) => ({
  brand: rxExact(brand),
  model: rxExact(model),
  ...(variant ? { variant: rxContains(variant) } : {}),
});

const vehiclesQueryForCase = ({ brand = "", model = "", variant = "" } = {}) => ({
  brand: rxExact(brand),
  model: rxContains(model),
  ...(variant
    ? {
        $or: [
          { variant: rxContains(variant) },
          { variantDisplayName: rxContains(variant) },
          { variantShortName: rxContains(variant) },
        ],
      }
    : {}),
});

const matrixQueryForCase = ({ brand = "", model = "", variant = "" } = {}) => ({
  brand: rxExact(brand),
  model: rxExact(model),
  ...(variant ? { variant: rxContains(variant) } : {}),
});

const catalogQueryForCase = ({ requestType = "", topicTerms = [] } = {}) => {
  const terms = requestType === "safety features"
    ? topicTerms
    : [];

  if (!terms.length) {
    return {};
  }

  const regexes = terms.map(rxContains);
  return {
    $or: [
      { canonicalKey: { $in: regexes } },
      { displayName: { $in: regexes } },
      { groupKey: { $in: regexes } },
      { groupLabel: { $in: regexes } },
      { aliases: { $elemMatch: { $in: regexes } } },
      { sections: { $elemMatch: { $in: regexes } } },
    ],
  };
};

const classify = ({ sourceRows = [], vehicleSignal = {}, matrixRows = [], catalogRows = [] } = {}) => {
  if (!sourceRows.length || vehicleSignal.status !== "active") {
    return "source_missing_or_inactive";
  }

  if (!matrixRows.length || !catalogRows.length) {
    return "source_has_data_read_model_missing";
  }

  if (sourceRows.length && matrixRows.length && catalogRows.length) {
    return "source_has_data_tool_gap";
  }

  return "needs_manual_review";
};

const inspectCase = async ({ db, testCase }) => {
  const sourceRows = await db.collection("vehicle_features")
    .find(sourceQueryForCase(testCase), {
      projection: {
        brand: 1,
        model: 1,
        variant: 1,
        features: 1,
        last_updated: 1,
        scrape_timestamp: 1,
        updatedAt: 1,
      },
    })
    .limit(20)
    .toArray();

  const vehicleRows = await db.collection("vehicles")
    .find(vehiclesQueryForCase(testCase), {
      projection: {
        brand: 1,
        model: 1,
        variant: 1,
        variantDisplayName: 1,
        variantShortName: 1,
        city: 1,
        is_discontinued: 1,
        LastSeenDate: 1,
        lastSeenAt: 1,
        lifecycleStatus: 1,
        updatedAt: 1,
      },
    })
    .limit(100)
    .toArray();

  const matrixRows = await db.collection("vehicle_variant_feature_matrix_v2")
    .find(matrixQueryForCase(testCase), {
      projection: {
        brand: 1,
        model: 1,
        variant: 1,
        variantFull: 1,
        lifecycleStatus: 1,
        activeForFeatureExplorer: 1,
        activePricelistMatched: 1,
        featureKeys: 1,
        featuresByKey: 1,
      },
    })
    .limit(30)
    .toArray();

  const catalogRows = await db.collection("vehicle_feature_catalog_v2")
    .find(catalogQueryForCase(testCase), {
      projection: {
        canonicalKey: 1,
        displayName: 1,
        groupKey: 1,
        groupLabel: 1,
        aliases: 1,
        sections: 1,
        rows: 1,
        availableRows: 1,
        modelsCount: 1,
        variantsCount: 1,
      },
    })
    .limit(testCase.requestType === "safety features" ? 50 : 500)
    .toArray();

  const vehicleSignal = activeVehicleSignal(vehicleRows);

  return {
    ids: testCase.ids,
    message: testCase.message,
    model: testCase.model,
    variant: testCase.variant || "",
    requestType: testCase.requestType,
    vehicleStatus: vehicleSignal,
    sourceRowsCount: sourceRows.length,
    sourceRowsWithTopicMatches: sourceRows.filter((row) =>
      testCase.topicTerms?.length
        ? featureSample({ row, topicTerms: testCase.topicTerms }).topicFeatureCount > 0
        : featureKeys(row).length > 0,
    ).length,
    sourceSamples: sourceRows.slice(0, 3).map((row) => featureSample({ row, topicTerms: testCase.topicTerms || [] })),
    derivedMatrixRowsCount: matrixRows.length,
    derivedMatrixSamples: matrixRows.slice(0, 5).map((row = {}) => ({
      brand: row.brand || "",
      model: row.model || "",
      variant: row.variant || row.variantFull || "",
      lifecycleStatus: row.lifecycleStatus || "",
      activeForFeatureExplorer: Boolean(row.activeForFeatureExplorer),
      activePricelistMatched: Boolean(row.activePricelistMatched),
      featureKeysCount: Array.isArray(row.featureKeys) ? row.featureKeys.length : 0,
      featuresByKeyCount: Object.keys(row.featuresByKey || {}).length,
    })),
    catalogRowsCount: catalogRows.length,
    catalogSamples: catalogRows.slice(0, 8).map((row = {}) => ({
      canonicalKey: row.canonicalKey || "",
      displayName: row.displayName || "",
      groupKey: row.groupKey || "",
      groupLabel: row.groupLabel || "",
      rows: row.rows || 0,
      availableRows: row.availableRows || 0,
      modelsCount: row.modelsCount || 0,
      variantsCount: row.variantsCount || 0,
    })),
    classification: classify({ sourceRows, vehicleSignal, matrixRows, catalogRows }),
  };
};

const printResult = (result = {}) => {
  console.log(`\n[${result.ids.join("/")}] ${result.message}`);
  console.log(`model=${result.model}${result.variant ? ` variant=${result.variant}` : ""} requestType=${result.requestType}`);
  console.log(`vehicleStatus=${result.vehicleStatus.status} activeRows=${result.vehicleStatus.activeRows} discontinuedRows=${result.vehicleStatus.discontinuedRows} inactiveRows=${result.vehicleStatus.inactiveRows} totalVehicleRows=${result.vehicleStatus.totalRows} latestSeen=${result.vehicleStatus.latestSeen || "n/a"}`);
  console.log(`vehicleSample=${JSON.stringify(result.vehicleStatus.sample)}`);
  console.log(`vehicle_features.sourceRows=${result.sourceRowsCount} sourceRowsWithTopicMatches=${result.sourceRowsWithTopicMatches}`);
  console.log(`vehicle_features.sample=${JSON.stringify(result.sourceSamples)}`);
  console.log(`vehicle_variant_feature_matrix_v2.rows=${result.derivedMatrixRowsCount}`);
  console.log(`vehicle_variant_feature_matrix_v2.sample=${JSON.stringify(result.derivedMatrixSamples)}`);
  console.log(`vehicle_feature_catalog_v2.rows=${result.catalogRowsCount}`);
  console.log(`vehicle_feature_catalog_v2.sample=${JSON.stringify(result.catalogSamples)}`);
  console.log(`classification=${result.classification}`);
};

const main = async () => {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  if (typeof connectDB !== "function") {
    throw new Error("connectDB export not found");
  }

  await connectDB();
  const db = mongoose.connection.db;
  const results = [];

  for (const testCase of CASES) {
    const result = await inspectCase({ db, testCase });
    results.push(result);
    printResult(result);
  }

  const counts = results.reduce((acc, result) => {
    acc[result.classification] = (acc[result.classification] || 0) + 1;
    return acc;
  }, {});

  console.log("\nclassificationSummary=" + JSON.stringify(counts));
};

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (mongoose.connection?.readyState) {
      await mongoose.disconnect();
    }
  });
