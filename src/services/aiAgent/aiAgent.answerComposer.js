const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const firstText = (...values) => {
  for (const value of values) {
    const text = cleanText(value);
    if (text) return text;
  }
  return "";
};

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const getIntent = (response = {}) =>
  firstText(response.intent, response.tool, response.data?.intent, response.widget?.intent);

const getRows = (response = {}) =>
  asArray(
    response.data?.rows ||
      response.rows ||
      response.items ||
      response.widget?.rows ||
      response.widget?.items,
  );

const getModelGroups = (response = {}) =>
  asArray(
    response.data?.modelGroups ||
      response.modelGroups ||
      response.widget?.modelGroups ||
      response.widget?.data?.modelGroups,
  );

const getVehicleLabel = (vehicle = {}) =>
  firstText(
    [vehicle.fullModel, vehicle.variant || vehicle.variantName].filter(Boolean).join(" "),
    vehicle.fullModel,
    vehicle.displayName,
    vehicle.model,
    vehicle.label,
  );

const getRowVehicleLabel = (row = {}) =>
  firstText(
    [row.fullModel || row.displayName || row.model, row.variant || row.variantName]
      .filter(Boolean)
      .join(" "),
    row.label,
    row.name,
    row.title,
  );

const getPriceLabel = (row = {}) =>
  firstText(row.onRoadPriceLabel, row.exShowroomPriceLabel, row.priceLabel);

const composePriceAnswer = (response = {}) => {
  const rows = getRows(response);
  const row = rows[0] || {};
  const vehicle = getRowVehicleLabel(row) || firstText(response.title, response.data?.title);
  const city = firstText(row.cityName, row.city, response.contextPatch?.anchorCity, "Delhi");
  const onRoad = firstText(row.onRoadPriceLabel, row.onRoadPrice);
  const exShowroom = firstText(row.exShowroomPriceLabel, row.exShowroomPrice);

  if (!vehicle) return response.answer;

  if (onRoad && exShowroom) {
    return `For ${vehicle} in ${city}, the on-road price is ${onRoad}. The ex-showroom price is ${exShowroom}.`;
  }

  if (onRoad) {
    return `For ${vehicle} in ${city}, the on-road price is ${onRoad}.`;
  }

  if (exShowroom) {
    return `For ${vehicle} in ${city}, the ex-showroom price is ${exShowroom}.`;
  }

  return response.answer;
};

const composeFeatureDiscoveryAnswer = (response = {}) => {
  const groups = getModelGroups(response);
  const matched =
    response.matched ||
    response.data?.matched ||
    response.sourceTransparency?.recordCount ||
    groups.length ||
    0;

  const feature = firstText(
    response.data?.feature,
    response.feature,
    response.title?.replace(/matches/i, ""),
    "requested feature",
  );

  if (!groups.length) return response.answer;

  const firstGroups = groups
    .slice(0, 3)
    .map((group) => {
      const name = firstText(group.displayName, group.fullModel, group.model);
      const startsFrom = firstText(group.startsFromVariant);
      const best = firstText(group.bestUnderBudgetVariant);
      if (name && startsFrom && best && startsFrom !== best) {
        return `${name}: starts from ${startsFrom}, best under budget ${best}`;
      }
      if (name && startsFrom) return `${name}: starts from ${startsFrom}`;
      return name;
    })
    .filter(Boolean);

  const intro = `I found ${matched} qualifying ${feature.trim()} variants, grouped by model so you can see where the feature starts and what fits best in budget.`;

  return firstGroups.length
    ? `${intro} Top matches: ${firstGroups.join("; ")}.`
    : intro;
};

const formatBudgetLabel = (value = 0) => {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) return "";

  if (amount >= 10000000) {
    const crore = amount / 10000000;
    return `₹${Number.isInteger(crore) ? crore : crore.toFixed(1)}Cr`;
  }

  const lakh = amount / 100000;
  return `₹${Number.isInteger(lakh) ? lakh : lakh.toFixed(1)}L`;
};

const composeVehicleRecommendationAnswer = (response = {}) => {
  const groups = getModelGroups(response);
  const rows = getRows(response);
  const filters = response.data?.filters || response.filters || {};
  const budgetDiscovery =
    response.data?.budgetDiscovery ||
    response.budgetDiscovery ||
    response.meta?.budgetDiscovery ||
    {};
  const returnedPreviewGroups =
    budgetDiscovery.returnedPreviewGroups ||
    response.data?.returnedPreviewGroups ||
    response.returnedPreviewGroups ||
    groups.length ||
    rows.length ||
    0;
  const totalQualifyingModels =
    budgetDiscovery.totalQualifyingModels ||
    response.data?.totalQualifyingModels ||
    response.totalQualifyingModels ||
    Number(response.matched || 0) ||
    groups.length ||
    rows.length ||
    0;
  const totalQualifyingVariants =
    budgetDiscovery.totalQualifyingVariants ||
    response.data?.totalQualifyingVariants ||
    response.totalQualifyingVariants ||
    response.data?.matchedVariantCount ||
    response.meta?.matchedVariantCount ||
    0;
  const budgetMax =
    filters.budgetMax ||
    budgetDiscovery.budgetMax ||
    0;
  const budgetLabel = formatBudgetLabel(budgetMax);
  const bodyType = firstText(filters.bodyType);
  const transmission = firstText(filters.transmission);
  const filterParts = [
    transmission ? `${transmission} ` : "",
    bodyType ? `${bodyType} ` : "",
    "models",
  ].join("").replace(/\s+/g, " ").trim();

  if (!totalQualifyingModels) return response.answer;

  const modelCountLabel = Number(totalQualifyingModels).toLocaleString("en-IN");
  const variantCountLabel = Number(totalQualifyingVariants || 0).toLocaleString("en-IN");
  const previewLabel = Number(returnedPreviewGroups || 0).toLocaleString("en-IN");
  const hasMore = Boolean(budgetDiscovery.hasMore);
  const variantText = totalQualifyingVariants
    ? ` with ${variantCountLabel} qualifying variant${Number(totalQualifyingVariants) === 1 ? "" : "s"}`
    : "";
  const budgetText = budgetLabel ? ` under ${budgetLabel}` : "";
  const previewText = returnedPreviewGroups
    ? ` Showing the top ${previewLabel} first.`
    : "";
  const moreText = hasMore
    ? " Open the complete budget list to adjust filters."
    : "";

  return `I found ${modelCountLabel} ${filterParts}${variantText}${budgetText}.${previewText}${moreText}`;
};

const humanizeFeatureKey = (value = "") =>
  cleanText(value)
    .replace(/_/g, " ")
    .replace(/\banti lock braking system abs\b/i, "Anti-lock Braking System (ABS)")
    .replace(/\badas package\b/i, "ADAS Package")
    .replace(/\badas\b/i, "ADAS")
    .replace(/\babs\b/i, "ABS")
    .replace(/\bcng\b/i, "CNG")
    .replace(/\bsunroof\b/i, "Sunroof")
    .replace(/\s+/g, " ")
    .trim();

const composeFeatureComparisonAnswer = (response = {}) => {
  const activeComparison =
    response.contextPatch?.activeComparison ||
    response.data?.activeComparison ||
    response.activeComparison ||
    {};

  const vehicles = asArray(
    activeComparison.vehicles ||
      response.contextPatch?.selectedComparisonSet?.vehicles ||
      response.data?.selectedComparisonSet?.vehicles ||
      response.selectedComparisonSet?.vehicles ||
      response.data?.models ||
      response.models,
  );

  const vehicleLabels = vehicles
    .map((vehicle) =>
      typeof vehicle === "string" ? cleanText(vehicle) : getVehicleLabel(vehicle),
    )
    .filter(Boolean);

  if (vehicleLabels.length < 2) return response.answer;

  const rows = getRows(response);

  const rowFeatureNames = rows
    .map((row) =>
      humanizeFeatureKey(
        firstText(row.feature, row.displayName, row.name, row.featureKey, row.key),
      ),
    )
    .filter(Boolean);

  const contextFeatureNames = asArray(activeComparison.features)
    .map((feature) =>
      typeof feature === "string"
        ? humanizeFeatureKey(feature)
        : humanizeFeatureKey(
            firstText(feature.feature, feature.displayName, feature.name, feature.featureKey, feature.key),
          ),
    )
    .filter(Boolean);

  const featureNames = rowFeatureNames.length ? rowFeatureNames : contextFeatureNames;

  if (!featureNames.length) return response.answer;

  const fuelFilter = firstText(
    response.fuelFilter,
    response.data?.fuelFilter,
    activeComparison.fuelFilter,
  );

  const compared = vehicleLabels.slice(0, 2).join(" vs ");
  const scope = fuelFilter ? `${fuelFilter.toUpperCase()} variants` : "the selected comparison";

  return `I compared ${compared} for ${scope} on ${featureNames.join(", ")}. ${
    fuelFilter
      ? `${fuelFilter.toUpperCase()} is being used as a fuel filter, not as a feature row.`
      : "The answer focuses only on the requested comparison scope."
  }`;
};


const composeVehicleComparisonAnswer = (response = {}) => {
  const rows = getRows(response);
  const summary = response.data?.comparisonSummary || response.comparisonSummary || {};
  const differenceSummary = response.data?.differenceSummary || response.differenceSummary || {};
  const featureDifferences = asArray(response.data?.featureDifferences || response.featureDifferences);
  const commonHighlights = asArray(response.data?.commonHighlights || response.commonHighlights);

  const comparedVehicles = asArray(summary.comparedVehicles);
  const labels =
    comparedVehicles.length >= 2
      ? comparedVehicles.map((vehicle) =>
          firstText(
            vehicle.label,
            [vehicle.model, vehicle.variant].filter(Boolean).join(" "),
          ),
        )
      : rows.map(getRowVehicleLabel);

  if (labels.length < 2) return response.answer;

  const priceDeltaLabel = firstText(summary.priceDeltaLabel);
  const cheaperVehicle = firstText(summary.cheaperVehicle);
  const differenceCount =
    differenceSummary.featureDifferenceCount ||
    summary.featureDifferenceCount ||
    featureDifferences.length ||
    0;

  const commonCount =
    differenceSummary.commonHighlightCount ||
    summary.commonHighlightCount ||
    commonHighlights.length ||
    0;

  const priceLine =
    priceDeltaLabel && cheaperVehicle
      ? `${cheaperVehicle} is cheaper by about ${priceDeltaLabel} on-road.`
      : "The price difference is available in the comparison table.";

  const differenceLine = differenceCount
    ? `I also found ${differenceCount} feature/spec differences and ${commonCount} common highlights in the compared variant data.`
    : "Feature/spec differences were not available for these exact variants yet.";

  return `I compared ${labels[0]} and ${labels[1]}. ${priceLine} ${differenceLine}`;
};

export const composeAciAnswer = (response = {}) => {
  if (!response || typeof response !== "object") return response;

  const intent = getIntent(response);
  let composedAnswer = "";

  if (intent === "vehicle_pricelist") {
    composedAnswer = composePriceAnswer(response);
  } else if (intent === "vehicle_recommendation") {
    composedAnswer = composeVehicleRecommendationAnswer(response);
  } else if (intent === "vehicle_feature_discovery") {
    composedAnswer = composeFeatureDiscoveryAnswer(response);
  } else if (intent === "vehicle_feature_comparison") {
    composedAnswer = composeFeatureComparisonAnswer(response);
  } else if (intent === "vehicle_comparison") {
    composedAnswer = composeVehicleComparisonAnswer(response);
  }

  if (!composedAnswer || composedAnswer === response.answer) {
    return response;
  }

  return {
    ...response,
    answer: composedAnswer,
    answerComposer: {
      version: "aci_answer_composer_v1",
      applied: true,
      intent,
      deterministic: true,
    },
    meta: {
      ...(response.meta || {}),
      answerComposer: {
        version: "aci_answer_composer_v1",
        applied: true,
        intent,
        deterministic: true,
      },
    },
  };
};

export default composeAciAnswer;
