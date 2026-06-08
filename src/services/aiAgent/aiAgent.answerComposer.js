import {
  buildAciLanguageSeed,
  renderAciTemplate,
} from "../aciCore/language/aciAnswerLanguageComposer.js";

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

const getRowVariantLabel = (row = {}) =>
  firstText(row.variant, row.variantName, row.fullVariant, row.fullVariantName);

const priceLabelFromRow = (row = {}, labelKeys = [], valueKeys = []) => {
  for (const key of labelKeys) {
    const label = firstText(row?.[key]);
    if (label) return label;
  }

  for (const key of valueKeys) {
    const value = Number(row?.[key] || 0);
    if (!Number.isFinite(value) || value <= 0) continue;
    if (value >= 10000000) return `₹${(value / 10000000).toFixed(value % 10000000 === 0 ? 0 : 2)}Cr`;
    if (value >= 100000) return `₹${(value / 100000).toFixed(value % 100000 === 0 ? 0 : 2)}L`;
    return `₹${Math.round(value).toLocaleString("en-IN")}`;
  }

  return "";
};

const numericPriceFromRow = (row = {}, keys = []) => {
  for (const key of keys) {
    const value = Number(row?.[key] || 0);
    if (Number.isFinite(value) && value > 0) return value;
  }
  return 0;
};

const getExShowroomLabel = (row = {}) =>
  priceLabelFromRow(row, ["exShowroomPriceLabel", "exShowroomLabel"], ["exShowroomPrice", "ex_showroom_price"]);

const getOnRoadWithoutOptionalLabel = (row = {}) =>
  priceLabelFromRow(
    row,
    ["onRoadPriceWithoutOptionalLabel", "onRoadWithoutOptionalLabel"],
    ["onRoadPriceWithoutOptional"],
  );

const getOnRoadLabel = (row = {}) =>
  getOnRoadWithoutOptionalLabel(row) ||
  priceLabelFromRow(
    row,
    ["onRoadPriceLabel", "onRoadLabel", "priceLabel"],
    ["onRoadPrice", "on_road_price", "price"],
  );

const getOptionalAddOnsLabel = (row = {}) =>
  firstText(
    row.optionalChargesTotalLabel,
    row.priceBreakup?.optionalCharges?.formatted,
    row.priceBreakup?.totals?.optionalDeltaFormatted,
  );

const getOptionalOnRoadLabel = (row = {}) =>
  firstText(
    row.onRoadPriceWithOptionalLabel,
    row.priceBreakup?.totals?.onRoadWithOptionalFormatted,
  );

const getRange = (rows = [], keys = [], labelFor = () => "") => {
  const priced = asArray(rows)
    .map((row) => ({
      row,
      value: numericPriceFromRow(row, keys),
      label: labelFor(row),
    }))
    .filter((item) => item.value > 0 && item.label)
    .sort((left, right) => left.value - right.value);

  if (!priced.length) return null;

  const min = priced[0];
  const max = priced[priced.length - 1];
  return {
    minLabel: min.label,
    maxLabel: max.label,
    minVariant: getRowVariantLabel(min.row),
    maxVariant: getRowVariantLabel(max.row),
    isSingle: min.value === max.value,
  };
};

const rangeSentence = (prefix = "", range = null) => {
  if (!range) return "";
  if (range.isSingle) return `${prefix} price is ${range.minLabel}`;
  const start = range.minVariant ? ` from ${range.minVariant} at ${range.minLabel}` : ` from ${range.minLabel}`;
  const end = range.maxVariant ? ` to ${range.maxVariant} at ${range.maxLabel}` : ` to ${range.maxLabel}`;
  return `${prefix} prices range${start}${end}`;
};

const normalizePriceAnswer = (answer = "") =>
  cleanText(answer)
    .replace(/\. the on-road/g, ". The on-road")
    .replace(/\. the ex-showroom/g, ". The ex-showroom")
    .replace(/\bprice rows\b/gi, "prices");

const getPriceQueryText = (response = {}) =>
  firstText(
    response.aciCoreBridge?.effectiveMessage,
    response.aciCoreBridge?.originalMessage,
    response.meta?.aciCoreBridge?.effectiveMessage,
    response.meta?.aciCoreBridge?.originalMessage,
    response.userMessage,
    response.message,
  );

const getLanguageSeed = (response = {}, templateKey = "") =>
  buildAciLanguageSeed(
    templateKey,
    response.sessionId,
    response.requestId,
    response.message,
    response.userMessage,
    response.meta?.sessionId,
    response.meta?.requestId,
    response.meta?.turnId,
    response.meta?.caseId,
    response.aciCoreBridge?.originalMessage,
    response.aciCoreBridge?.effectiveMessage,
    response.title,
  );

const renderLanguage = (templateKey = "", input = {}, response = {}) =>
  renderAciTemplate(templateKey, input, {
    seed: getLanguageSeed(response, templateKey),
    previousVariantId:
      response.previousVariantId ||
      response.meta?.answerLanguage?.variantId ||
      response.answerLanguage?.variantId,
  });

const composePriceAnswer = (response = {}) => {
  const rows = getRows(response);
  const row = rows[0] || {};
  const responseVehicle = response.vehicle || response.widget?.vehicle || response.data?.vehicle || {};
  const rowVehicle = getRowVehicleLabel(row);
  const responseVehicleLabel =
    getVehicleLabel(responseVehicle) ||
    firstText(response.title, response.data?.title);
  const vehicle = rowVehicle || responseVehicleLabel;
  const city = firstText(row.cityName, row.city, response.contextPatch?.anchorCity, "selected city");
  const queryText = getPriceQueryText(response);
  const wantsOnRoad = /\bon\s*road\b|\bonroad\b/i.test(queryText);
  const wantsExShowroom = /\bex\s*showroom\b|\bexshowroom\b/i.test(queryText);

  const onRoadWithoutOptional = getOnRoadWithoutOptionalLabel(row);
  const onRoadWithOptional = getOptionalOnRoadLabel(row);
  const optionalTotal = getOptionalAddOnsLabel(row);
  const onRoad = getOnRoadLabel(row);
  const exShowroom = getExShowroomLabel(row);

  if (!vehicle) return response.answer;

  if (rows.length > 1) {
    const exShowroomRange = getRange(rows, ["exShowroomPrice", "ex_showroom_price"], getExShowroomLabel);
    const onRoadRange = getRange(
      rows,
      ["onRoadPriceWithoutOptional", "onRoadPrice", "on_road_price", "price"],
      getOnRoadLabel,
    );
    const evidence = [];

    if (wantsOnRoad) {
      const onRoadLine = rangeSentence("On-road", onRoadRange);
      if (onRoadLine) evidence.push(onRoadLine);
      const exLine = rangeSentence("Ex-showroom", exShowroomRange);
      if (exLine) evidence.push(exLine);
    } else if (wantsExShowroom) {
      const exLine = rangeSentence("Ex-showroom", exShowroomRange);
      if (exLine) evidence.push(exLine);
      const onRoadLine = rangeSentence("On-road", onRoadRange);
      if (onRoadLine) evidence.push(onRoadLine);
    } else {
      const exLine = rangeSentence("Ex-showroom", exShowroomRange);
      if (exLine) evidence.push(exLine);
      const onRoadLine = rangeSentence("On-road", onRoadRange);
      if (onRoadLine) evidence.push(onRoadLine);
    }

    if (evidence.length) {
      return normalizePriceAnswer(
        `I found ${responseVehicleLabel || vehicle || "this model"} prices in ${city} across ${rows.length} variants. ${evidence.join(". ")}. Optional add-ons remain separate from the default on-road figures.`,
      );
    }

    return normalizePriceAnswer(response.answer);
  }

  if (onRoadWithoutOptional && exShowroom) {
    const optionalLine = onRoadWithOptional && onRoadWithOptional !== onRoadWithoutOptional
      ? ` Optional add-ons are ${optionalTotal || "available separately"} if selected, taking the on-road total to ${onRoadWithOptional}.`
      : " Optional add-ons remain separate from this default on-road figure.";

    return normalizePriceAnswer(
      `${vehicle} in ${city} is ${exShowroom} ex-showroom and ${onRoadWithoutOptional} on-road excluding optional add-ons.${optionalLine}`,
    );
  }

  if (onRoad && exShowroom) {
    return normalizePriceAnswer(
      `${vehicle} in ${city} is ${exShowroom} ex-showroom and ${onRoad} on-road.`,
    );
  }

  if (onRoad) {
    return normalizePriceAnswer(`${vehicle} in ${city} has an on-road price of ${onRoad}.`);
  }

  if (exShowroom) {
    return normalizePriceAnswer(`${vehicle} in ${city} has an ex-showroom price of ${exShowroom}.`);
  }

  return normalizePriceAnswer(response.answer);
};

const composeFeatureDiscoveryAnswer = (response = {}) => {
  const groups = getModelGroups(response);
  const featureDiscovery =
    response.data?.featureDiscovery ||
    response.featureDiscovery ||
    response.meta?.featureDiscovery ||
    {};
  const totalQualifyingModels =
    featureDiscovery.totalQualifyingModels ||
    response.data?.totalQualifyingModels ||
    groups.length ||
    0;
  const returnedPreviewGroups =
    featureDiscovery.returnedPreviewGroups ||
    response.data?.returnedPreviewGroups ||
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

  const budgetMax =
    featureDiscovery.budgetMax ||
    response.data?.filters?.budgetMax ||
    response.filters?.budgetMax ||
    0;
  const budgetLabel = formatBudgetLabel(budgetMax);
  const make = firstText(response.data?.vehicle?.make, response.vehicle?.make);
  const modelCountLabel = Number(totalQualifyingModels || groups.length).toLocaleString("en-IN");
  const makeText = make ? `${make} ` : "";
  const budgetText = budgetLabel ? ` under ${budgetLabel}` : "";
  const previewText = returnedPreviewGroups
    ? " Showing the top matches first."
    : "";
  const intro = `I found ${modelCountLabel} ${makeText}model${Number(totalQualifyingModels || groups.length) === 1 ? "" : "s"} with ${feature.trim()}${budgetText}.${previewText}`;

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
    budgetDiscovery.totalUniqueQualifyingVariants ||
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
  const previewLabel = Number(returnedPreviewGroups || 0).toLocaleString("en-IN");
  const hasMore = Boolean(budgetDiscovery.hasMore);
  const budgetText = budgetLabel ? ` under ${budgetLabel}` : "";
  const isActuallyDiversified =
    Boolean(budgetDiscovery.diversifiedPreview) &&
    Array.isArray(budgetDiscovery.previewBodyTypeGroups) &&
    budgetDiscovery.previewBodyTypeGroups.length > 1;

  const previewText = returnedPreviewGroups
    ? isActuallyDiversified
      ? ` Showing ${previewLabel} good starting points across body styles.`
      : ` Showing the top ${previewLabel} first.`
    : "";
  const moreText = hasMore
    ? " Open the complete budget list to adjust filters."
    : "";

  return `I found ${modelCountLabel} ${filterParts}${budgetText}.${previewText}${moreText}`;
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

  return renderLanguage(
    "comparison_summary",
    {
      vehicleA: vehicleLabels[0],
      vehicleB: vehicleLabels[1],
      priceLine: `Scope: ${scope}.`,
      differenceLine: `Requested features: ${featureNames.join(", ")}. ${
        fuelFilter
          ? `${fuelFilter.toUpperCase()} is being used as a fuel filter, not as a feature row.`
          : "The answer focuses only on the requested comparison scope."
      }`,
    },
    response,
  ).text;
};


const composeVehicleComparisonAnswer = (response = {}) => {
  const rows = getRows(response);
  const summary = response.data?.comparisonSummary || response.comparisonSummary || {};
  const differenceSummary = response.data?.differenceSummary || response.differenceSummary || {};
  const featureDifferences = asArray(response.data?.featureDifferences || response.featureDifferences);
  const commonHighlights = asArray(response.data?.commonHighlights || response.commonHighlights);
  const missingEvidence = asArray(
    response.data?.missingOrUnavailableEvidence || response.missingOrUnavailableEvidence,
  );
  const hasUnavailableRows = rows.some((row) => row?.unavailable === true);

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
    ? `I also found ${differenceCount} indexed feature/spec differences and ${commonCount} common highlights in the compared variant data.${
        missingEvidence.length ? " Some indexed evidence is partial, so this is not a complete brochure-level diff." : ""
      }`
    : missingEvidence.length || hasUnavailableRows
      ? "The feature/spec comparison is partial because one compared variant did not resolve to indexed rows yet."
      : "No indexed feature/spec differences were found for these exact compared variants.";

  return renderLanguage(
    "comparison_summary",
    {
      vehicleA: labels[0],
      vehicleB: labels[1],
      priceLine,
      differenceLine,
    },
    response,
  ).text;
};

const uniqueTexts = (values = []) => [...new Set(values.map(cleanText).filter(Boolean))];

const getSpecValueText = (item = {}) => {
  if (!item || typeof item !== "object") return firstText(item);
  return firstText(item.value, item.displayValue, item.text, item.label);
};

const getSpecVariantText = (item = {}) =>
  item && typeof item === "object"
    ? firstText(item.variant, item.variantName, item.fullVariant, item.variantKey)
    : "";

const summarizeSpecValues = (items = []) => {
  const entries = asArray(items)
    .map((item) => {
      const value = getSpecValueText(item);
      if (!value) return null;
      return {
        value,
        variant: getSpecVariantText(item),
      };
    })
    .filter(Boolean);

  if (!entries.length) return [];

  const groupsByValue = new Map();
  for (const entry of entries) {
    if (!groupsByValue.has(entry.value)) groupsByValue.set(entry.value, []);
    groupsByValue.get(entry.value).push(entry.variant);
  }

  const groups = [...groupsByValue.entries()];
  if (groups.length === 1) {
    const [value, variants] = groups[0];
    const variantCount = uniqueTexts(variants).length;
    if (variantCount > 1) return [`${value} across ${variantCount} indexed variants`];
    return [value];
  }

  const summaries = groups.slice(0, 4).map(([value, variants]) => {
    const examples = uniqueTexts(variants).slice(0, 3);
    return examples.length ? `${examples.join(", ")}: ${value}` : value;
  });

  const remaining = groups.length - summaries.length;
  if (remaining > 0) summaries.push(`plus ${remaining} more indexed value groups`);
  return summaries;
};

const composeVehicleSpecAttributeAnswer = (response = {}) => {
  const data = response.data || {};
  const modelLabel = firstText(
    data.anchorFullModel,
    response.contextPatch?.anchorFullModel,
    response.contextPatch?.selectedVehicle?.fullModel,
    [data.anchorMake, data.anchorModel].filter(Boolean).join(" "),
    response.title,
    "this model",
  );
  const attribute = firstText(data.attributeLabel, data.attributeKey, "requested specification");
  const values = summarizeSpecValues(data.values);

  if (values.length && !data.missingData) {
    return renderLanguage(
      "resolved_spec_value_summary",
      {
        model: modelLabel,
        topic: attribute,
        values,
      },
      response,
    ).text;
  }

  return renderLanguage(
    "resolved_spec_missing_summary",
    {
      model: modelLabel,
      topic: attribute,
    },
    response,
  ).text;
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
  } else if (intent === "vehicle_spec_attribute_answer") {
    composedAnswer = composeVehicleSpecAttributeAnswer(response);
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
