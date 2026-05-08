import {
  VEHICLE_COLLECTION_CANDIDATES,
  getCollection,
  safeFind,
} from "./shared/db.js";

import {
  getToolVariant,
  normalizeVehicleRow,
  searchKey,
  unique,
} from "./shared/normalizers.js";

import {
  buildVehicleMongoQuery,
  buildVariantResolution,
  rowMatchesFilters,
  stripVariantFromToolPlan,
  variantMatchScore,
} from "./shared/matching.js";

import {
  buildPriceSummary,
  sortPriceRows,
} from "./shared/pricing.js";

/**
 * vehicle_pricelist runtime tool
 *
 * Responsibility:
 * - Fetch and normalize price/variant rows for model/variant.
 * - Handle exact variant resolution safely.
 * - Return data only.
 *
 * It does NOT:
 * - build canvasType
 * - build answer text
 * - build frontend actions
 */

export const DEFAULT_PRICELIST_LIMIT = 120;

export const runVehiclePricelistTool = async ({
  toolPlan = {},
  context = {},
  limit = DEFAULT_PRICELIST_LIMIT,
} = {}) => {
  const { collection, collectionName, reason } = await getCollection(
    VEHICLE_COLLECTION_CANDIDATES,
  );

  const requestedVariant = getToolVariant(toolPlan, context);
  const requestedVariantKey = searchKey(requestedVariant);

  const query = buildVehicleMongoQuery({ toolPlan, context });

  let rawRows = await safeFind(collection, query, {
    limit,
  });

  if (requestedVariant && rawRows.length === 0) {
    const modelOnlyToolPlan = stripVariantFromToolPlan(toolPlan);

    rawRows = await safeFind(
      collection,
      buildVehicleMongoQuery({
        toolPlan: modelOnlyToolPlan,
        context,
      }),
      {
        limit,
      },
    );
  }

  let normalizedRows = rawRows.map(normalizeVehicleRow);
  let matchedVariantRows = [];

  if (requestedVariantKey) {
    const scoredRows = normalizedRows
      .map((row) => ({
        row,
        score: variantMatchScore(row, requestedVariant),
      }))
      .filter((item) => item.score >= 88)
      .sort((a, b) => b.score - a.score);

    matchedVariantRows = scoredRows.map((item) => item.row);
    normalizedRows = matchedVariantRows;
  }

  const rows = sortPriceRows(
    normalizedRows.filter((row) =>
      rowMatchesFilters(row, {
        ...(toolPlan.filters || {}),
        variant: "",
      }),
    ),
    toolPlan.ranking || "",
  );

  const allCandidateRows = rawRows.map(normalizeVehicleRow);

  const candidateVariants = requestedVariantKey
    ? unique(
        allCandidateRows
          .map((row) => row.variant || row.variantShort || row.variantNormalized)
          .filter(Boolean),
      ).slice(0, 24)
    : [];

  return {
    tool: "vehicle_pricelist",
    rows,
    records: rows,
    candidateRows: requestedVariantKey ? allCandidateRows.slice(0, 24) : [],
    candidateVariants,
    variantResolution: buildVariantResolution({
      requestedVariant,
      rows: allCandidateRows,
      matchedRows: rows,
    }),
    count: rows.length,
    matched: rows.length,
    modulesChecked: [collectionName || reason || "vehicle_pricelist"],
    source: collectionName || "none",
    dataSource: collectionName ? "mongodb" : "empty",
    summary: buildPriceSummary(rows),
  };
};

export default runVehiclePricelistTool;
