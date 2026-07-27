import mongoose from "mongoose";

const text = (value = "") => String(value || "").replace(/\s+/g, " ").trim();

const keyify = (value = "") =>
  text(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const unique = (items = []) => [...new Set(items.map(text).filter(Boolean))];

const rowModelKey = (row = {}) =>
  keyify(
    row.modelKey ||
      row.shortModelKey ||
      row.decisionCandidate?.modelKey ||
      row.candidateMarketConfidence?.evidence?.modelKey ||
      row.candidateActiveMarketEligibility?.evidence?.modelKey ||
      row.fullModel ||
      row.displayName ||
      row.model ||
      "",
  );

const rowLabel = (row = {}) =>
  text(row.fullModel || row.displayName || [row.make || row.brand, row.model].filter(Boolean).join(" "));

const rowMake = (row = {}) => text(row.make || row.brand || row.decisionCandidate?.make || "");
const rowModel = (row = {}) => text(row.model || row.shortModel || row.decisionCandidate?.model || "");

const getDb = () => mongoose.connection?.db || null;

const escapeRegex = (value = "") => text(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const safeCount = async (collection, query) => {
  if (!collection) return 0;

  try {
    return await collection.countDocuments(query);
  } catch {
    return 0;
  }
};

const safeFind = async (collection, query, projection = {}, limit = 50) => {
  if (!collection) return [];

  try {
    return await collection.find(query).project(projection).limit(limit).toArray();
  } catch {
    return [];
  }
};

const safeAggregate = async (collection, pipeline = []) => {
  if (!collection) return [];

  try {
    return await collection.aggregate(pipeline, { allowDiskUse: true }).toArray();
  } catch {
    return [];
  }
};

const buildVehicleQuery = ({ modelKey, make, model, label }) => {
  const clauses = [];

  if (modelKey) clauses.push({ modelKey });
  if (modelKey) clauses.push({ model_normalized: new RegExp(`^${escapeRegex(modelKey).replace(/-/g, "\\s+")}$`, "i") });
  if (model) clauses.push({ model_normalized: new RegExp(`^${escapeRegex(model)}$`, "i") });
  if (label) clauses.push({ model: new RegExp(escapeRegex(label), "i") });

  if (make && model) {
    clauses.push({
      $and: [
        { brand: new RegExp(escapeRegex(make), "i") },
        { model_normalized: new RegExp(escapeRegex(model), "i") },
      ],
    });
  }

  return clauses.length ? { $or: clauses } : { _id: null };
};

const vehicleProjection = {
  _id: 1,
  brand: 1,
  make: 1,
  model: 1,
  model_normalized: 1,
  modelKey: 1,
  variant: 1,
  variant_short: 1,
  city: 1,
  cityName: 1,
  bodyType: 1,
  source: 1,
  url: 1,
  sourceUrl: 1,
  modelUrl: 1,
  variantUrl: 1,
  canonicalUrl: 1,
  carwaleId: 1,
  cardekhoId: 1,
  ex_showroom_price_cardekho: 1,
  LastSeenDate: 1,
  is_discontinued: 1,
  updatedAt: 1,
};

const batchCountByModelKey = async (collection, modelKeys = []) => {
  const keys = unique(modelKeys);

  if (!keys.length) return new Map();

  const docs = await safeAggregate(collection, [
    { $match: { modelKey: { $in: keys } } },
    { $group: { _id: "$modelKey", count: { $sum: 1 } } },
  ]);

  return new Map(docs.map((doc) => [doc._id, doc.count || 0]));
};

const batchVehicleDocsByModelKey = async (collection, modelKeys = [], perModelLimit = 100) => {
  const keys = unique(modelKeys);

  if (!keys.length) return new Map();

  const docs = await safeAggregate(collection, [
    { $match: { modelKey: { $in: keys } } },
    { $project: vehicleProjection },
    {
      $group: {
        _id: "$modelKey",
        docs: { $push: "$$ROOT" },
      },
    },
    {
      $project: {
        docs: { $slice: ["$docs", perModelLimit] },
      },
    },
  ]);

  return new Map(docs.map((doc) => [doc._id, Array.isArray(doc.docs) ? doc.docs : []]));
};

const classifyProvenance = ({
  vehicleCount,
  priceRowCount,
  featureRowCount,
  urlCount,
  sourceValueCount,
  lastSeenCount,
  cardekhoFieldCount,
  activeVehicleCount,
  discontinuedTrueCount,
}) => {
  if (!vehicleCount && !priceRowCount && !featureRowCount) {
    return {
      status: "identity_unverified",
      band: "weak",
      diagnosticUseAllowed: false,
      finalUseAllowed: false,
      reasons: ["no raw vehicle, price row, or feature matrix evidence found"],
    };
  }

  if (discontinuedTrueCount > 0 && activeVehicleCount === 0) {
    return {
      status: "not_current_sale",
      band: "weak",
      diagnosticUseAllowed: false,
      finalUseAllowed: false,
      reasons: ["source rows indicate discontinued status"],
    };
  }

  if (vehicleCount > 0 && (urlCount > 0 || sourceValueCount > 0) && lastSeenCount > 0) {
    return {
      status: "source_grounded",
      band: "strong",
      diagnosticUseAllowed: true,
      finalUseAllowed: false,
      reasons: ["raw vehicle evidence has source/url signal and recent last-seen evidence"],
    };
  }

  if (vehicleCount > 0 && lastSeenCount > 0 && cardekhoFieldCount > 0 && priceRowCount > 0) {
    return {
      status: "db_current_usable_source_url_missing",
      band: "good",
      diagnosticUseAllowed: true,
      finalUseAllowed: false,
      reasons: ["raw vehicle evidence is current and price-backed, but explicit source URL is missing"],
    };
  }

  if (vehicleCount > 0 && (priceRowCount > 0 || featureRowCount > 0)) {
    return {
      status: "db_usable_limited_provenance",
      band: "limited",
      diagnosticUseAllowed: true,
      finalUseAllowed: false,
      reasons: ["candidate has DB evidence but incomplete source provenance"],
    };
  }

  return {
    status: "derived_only",
    band: "weak",
    diagnosticUseAllowed: false,
    finalUseAllowed: false,
    reasons: ["candidate appears only in derived read-model evidence"],
  };
};

const buildProvenanceRow = ({
  row,
  modelKey,
  label,
  vehicleDocs,
  priceRowCount,
  featureRowCount,
  modelSummaryCount,
}) => {
  const vehicleCount = vehicleDocs.length;

  const urlCount = vehicleDocs.filter(
    (doc) => doc.url || doc.sourceUrl || doc.modelUrl || doc.variantUrl || doc.canonicalUrl,
  ).length;
  const sourceValues = unique(vehicleDocs.map((doc) => doc.source));
  const sourceValueCount = sourceValues.length;
  const lastSeenCount = vehicleDocs.filter((doc) => doc.LastSeenDate).length;
  const cardekhoFieldCount = vehicleDocs.filter(
    (doc) => doc.cardekhoId || doc.ex_showroom_price_cardekho,
  ).length;
  const discontinuedTrueCount = vehicleDocs.filter((doc) => doc.is_discontinued === true).length;
  const activeVehicleCount = vehicleDocs.filter((doc) => doc.is_discontinued === false).length;

  const bodyTypes = unique(vehicleDocs.map((doc) => doc.bodyType));
  const cities = unique(vehicleDocs.map((doc) => doc.cityName || doc.city));
  const sampleVariants = unique(vehicleDocs.map((doc) => doc.variant_short || doc.variant)).slice(0, 8);

  const classification = classifyProvenance({
    vehicleCount,
    priceRowCount,
    featureRowCount,
    urlCount,
    sourceValueCount,
    lastSeenCount,
    cardekhoFieldCount,
    activeVehicleCount,
    discontinuedTrueCount,
  });

  return {
    row: {
      ...row,
      candidateSourceProvenance: {
        version: "aci_candidate_source_provenance_v1",
        modelKey,
        label,
        status: classification.status,
        band: classification.band,
        diagnosticUseAllowed: classification.diagnosticUseAllowed,
        finalUseAllowed: classification.finalUseAllowed,
        reasons: classification.reasons,
        evidence: {
          vehicleCount,
          modelSummaryCount,
          priceRowCount,
          featureRowCount,
          urlCount,
          sourceValues,
          lastSeenCount,
          cardekhoFieldCount,
          activeVehicleCount,
          discontinuedTrueCount,
          bodyTypes,
          cities,
          sampleVariants,
        },
      },
      positiveSignals: [
        ...(Array.isArray(row.positiveSignals) ? row.positiveSignals : []),
        classification.band === "strong"
          ? "source provenance strong"
          : classification.band === "good"
            ? "source provenance usable"
            : classification.band === "limited"
              ? "source provenance limited"
              : "",
      ].filter(Boolean),
      risks: [
        ...(Array.isArray(row.risks) ? row.risks : []),
        classification.band === "weak" ? "candidate source provenance weak" : "",
      ].filter(Boolean),
    },
    classification,
  };
};

export const evaluateCandidateSourceProvenance = async ({ rows = [] } = {}) => {
  const db = getDb();
  const inputRows = Array.isArray(rows) ? rows : [];

  const summary = {
    version: "aci_candidate_source_provenance_v1",
    status: "not_evaluated",
    candidateCount: inputRows.length,
    strongCount: 0,
    goodCount: 0,
    limitedCount: 0,
    weakCount: 0,
    diagnosticAllowedCount: 0,
    finalEligibleCount: 0,
    finalRecommendationEnabled: false,
    diagnosticOnly: true,
    guardrail:
      "Candidate source provenance is diagnostic evidence quality only. Final recommendation remains disabled.",
    rows: inputRows,
  };

  if (!inputRows.length || !db) return summary;

  const vehicles = db.collection("vehicles");
  const priceRows = db.collection("aci_vehicle_price_rows");
  const featureMatrix = db.collection("vehicle_variant_feature_matrix_v2");
  const modelSummary = db.collection("aci_vehicle_model_summary");

  const rowMeta = inputRows.map((row) => {
    const modelKey = rowModelKey(row);

    return {
      row,
      modelKey,
      make: rowMake(row),
      model: rowModel(row),
      label: rowLabel(row),
    };
  });

  const modelKeys = unique(rowMeta.map((item) => item.modelKey));

  const [
    batchedVehicleDocs,
    batchedPriceCounts,
    batchedFeatureCounts,
    batchedSummaryCounts,
  ] = await Promise.all([
    batchVehicleDocsByModelKey(vehicles, modelKeys, 100),
    batchCountByModelKey(priceRows, modelKeys),
    batchCountByModelKey(featureMatrix, modelKeys),
    batchCountByModelKey(modelSummary, modelKeys),
  ]);

  const evaluatedRows = [];

  for (const item of rowMeta) {
    let vehicleDocs = item.modelKey ? batchedVehicleDocs.get(item.modelKey) || [] : [];
    let priceRowCount = item.modelKey ? batchedPriceCounts.get(item.modelKey) || 0 : 0;
    let featureRowCount = item.modelKey ? batchedFeatureCounts.get(item.modelKey) || 0 : 0;
    let modelSummaryCount = item.modelKey ? batchedSummaryCounts.get(item.modelKey) || 0 : 0;

    if (!vehicleDocs.length && item.modelKey) {
      const vehicleQuery = buildVehicleQuery(item);

      vehicleDocs = await safeFind(vehicles, vehicleQuery, vehicleProjection, 100);
    }

    if (!priceRowCount && item.modelKey) {
      priceRowCount = await safeCount(priceRows, { modelKey: item.modelKey });
    }

    if (!featureRowCount && item.modelKey) {
      featureRowCount = await safeCount(featureMatrix, { modelKey: item.modelKey });
    }

    if (!modelSummaryCount && item.modelKey) {
      modelSummaryCount = await safeCount(modelSummary, { modelKey: item.modelKey });
    }

    const { row, classification } = buildProvenanceRow({
      row: item.row,
      modelKey: item.modelKey,
      label: item.label,
      vehicleDocs,
      priceRowCount,
      featureRowCount,
      modelSummaryCount,
    });

    if (classification.band === "strong") summary.strongCount += 1;
    else if (classification.band === "good") summary.goodCount += 1;
    else if (classification.band === "limited") summary.limitedCount += 1;
    else summary.weakCount += 1;

    if (classification.diagnosticUseAllowed) summary.diagnosticAllowedCount += 1;
    if (classification.finalUseAllowed) summary.finalEligibleCount += 1;

    evaluatedRows.push(row);
  }

  return {
    ...summary,
    status: "evaluated",
    rows: evaluatedRows,
    canUseForDiagnosticShortlist: summary.diagnosticAllowedCount > 0,
    canUseForFinalRecommendation: false,
    finalRecommendationEnabled: false,
  };
};


export const filterDiagnosticSourceProvenanceRows = (rows = []) =>
  (Array.isArray(rows) ? rows : []).filter(
    (row) => row?.candidateSourceProvenance?.diagnosticUseAllowed !== false,
  );

export default evaluateCandidateSourceProvenance;
