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

  const featureLines = rows.slice(0, 4).map((row) => {
    const featureName = humanizeFeatureKey(
      firstText(row.feature, row.displayName, row.name, row.featureKey, row.key),
    );
    const modelLines = asArray(row.models).slice(0, 2).map((model) => {
      const modelName = firstText(model.model, model.fullModel, model.label);
      const availableCount = Number(model.availableCount || 0);
      const totalVariants = Number(model.totalVariants || model.checkedVariants || 0);

      if (model.status === "unknown") return `${modelName}: not listed in the current data`;
      if (model.available === false) return `${modelName}: not listed on current variants`;
      if (availableCount && totalVariants) {
        return `${modelName}: ${availableCount}/${totalVariants} current variants`;
      }
      return `${modelName}: available`;
    });

    return `${featureName}: ${modelLines.join("; ")}`;
  }).filter(Boolean);

  const priceLine = featureLines.length
    ? `${featureLines.join(". ")}.`
    : `${featureNames.join(", ")} comparison is ready.`;
  const differenceLine = fuelFilter
    ? `This view is limited to ${fuelFilter.toUpperCase()} variants. Open it for the exact variant breakdown.`
    : "Open the comparison for the exact variant breakdown.";

  return renderLanguage(
    "comparison_summary",
    {
      vehicleA: vehicleLabels[0],
      vehicleB: vehicleLabels[1],
      priceLine,
      differenceLine,
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
      ? `${cheaperVehicle} is about ${priceDeltaLabel} cheaper on-road for the variants shown.`
      : "The side-by-side price is ready below.";

  const differenceLine = differenceCount
    ? `Their equipment and specs are meaningfully different${
        commonCount ? ", although they still share several everyday essentials" : ""
      }.${
        missingEvidence.length ? " A few details are still incomplete, so I will not overstate the result." : ""
      } Tell me what matters most (city comfort, family use, off-roading, safety, or value), and I will narrow it down.`
    : missingEvidence.length || hasUnavailableRows
      ? "A few details are still incomplete for these exact variants, so I will keep the verdict open."
      : "These exact variants do not show a meaningful feature or spec difference in the current data.";

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

const cleanDuplicateMakeModelLabel = (label = "") => {
  const clean = firstText(label);
  if (!clean) return "";

  return clean.replace(/\b([A-Z][a-z]+)\s+\1\s+/g, "$1 ");
};


const COLOR_FAMILY_KEYWORDS = {
  black: ["black"],
  white: ["white"],
  red: ["red", "rouge"],
  blue: ["blue"],
  grey: ["grey", "gray"],
  gray: ["grey", "gray"],
  silver: ["silver"],
  green: ["green", "emerald"],
  dual_tone: ["dual tone", "dual-tone", "two tone", "two-tone", "black roof", "with black roof"],
};

const getColorFamilyRequest = (response = {}) => {
  const query = firstText(
    response.aciCoreBridge?.effectiveMessage,
    response.aciCoreBridge?.originalMessage,
    response.meta?.aciCoreBridge?.effectiveMessage,
    response.meta?.aciCoreBridge?.originalMessage,
    response.userMessage,
    response.message,
    response.query,
    response.meta?.queryUsed,
    response.data?.queryUsed,
  )
    .toLowerCase()
    .replace(/[-_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (
    query.includes("dual tone") ||
    query.includes("two tone") ||
    query.includes("black roof") ||
    query.includes("with black roof")
  ) {
    return "dual_tone";
  }

  for (const family of Object.keys(COLOR_FAMILY_KEYWORDS)) {
    if (family === "dual_tone") continue;
    if (new RegExp(`\\b${family}\\b`, "i").test(query)) {
      return family === "gray" ? "grey" : family;
    }
  }

  return "";
};

const getColorName = (color = {}) =>
  firstText(color.name, color.colorName, color.displayName, color.label, color.title);

const getColorRows = (response = {}) =>
  asArray(
    response.colors ||
      response.rows ||
      response.items ||
      response.data?.colors ||
      response.data?.rows ||
      response.data?.items ||
      response.widget?.colors ||
      response.widget?.rows ||
      response.widget?.items,
  );

const getColorVehicleLabel = (response = {}) => {
  const vehicle =
    response.contextPatch?.selectedVehicle ||
    response.vehicle ||
    response.data?.vehicle ||
    response.widget?.vehicle ||
    {};

  return firstText(
    vehicle.fullModel,
    vehicle.displayName,
    [vehicle.make || vehicle.brand, vehicle.model].filter(Boolean).join(" "),
    response.title?.replace(/\s+colors?$/i, ""),
    "this model",
  );
};


const getRequestedColorFamilyMatches = (response = {}) => {
  const requestedFamily = getColorFamilyRequest(response);
  const colors = getColorRows(response);
  if (!requestedFamily || !colors.length) {
    return { requestedFamily, colors, matches: colors };
  }

  const keywords = COLOR_FAMILY_KEYWORDS[requestedFamily] || [requestedFamily];
  const matches = colors.filter((color) => {
    const name = getColorName(color).toLowerCase();
    return keywords.some((keyword) => name.includes(keyword));
  });

  return { requestedFamily, colors, matches };
};

const applyRequestedColorFamilyRows = (response = {}) => {
  if (getIntent(response) !== "vehicle_colors") return response;

  const { requestedFamily, colors, matches } = getRequestedColorFamilyMatches(response);
  if (!requestedFamily || !colors.length) return response;

  const filteredRows = matches;
  const nextWidget = response.widget
    ? {
        ...response.widget,
        colors: filteredRows,
        rows: filteredRows,
        items: filteredRows,
        requestedColorFamily: requestedFamily,
        totalColorCount: colors.length,
        colorMatchCount: filteredRows.length,
        noRequestedColorMatch: filteredRows.length === 0,
      }
    : response.widget;

  return {
    ...response,
    colors: filteredRows,
    rows: filteredRows,
    items: filteredRows,
    count: filteredRows.length,
    matched: filteredRows.length,
    dataStatus: filteredRows.length ? response.dataStatus : "not_available",
    widget: nextWidget,
    widgets: nextWidget ? [nextWidget] : response.widgets,
    data: {
      ...(response.data || {}),
      colors: filteredRows,
      rows: filteredRows,
      items: filteredRows,
      requestedColorFamily: requestedFamily,
      totalColorCount: colors.length,
      colorMatchCount: filteredRows.length,
      noRequestedColorMatch: filteredRows.length === 0,
      dataStatus: filteredRows.length ? response.data?.dataStatus : "not_available",
    },
  };
};


const composeVehicleColorsAnswer = (response = {}) => {
  const requestedFamily = getColorFamilyRequest(response);
  if (!requestedFamily) return response.answer;

  const colors = getColorRows(response);
  const totalColorCount = Number(
    response.data?.totalColorCount ||
    response.widget?.totalColorCount ||
    response.totalColorCount ||
    colors.length ||
    0,
  );
  const keywords = COLOR_FAMILY_KEYWORDS[requestedFamily] || [requestedFamily];
  const matches = colors.filter((color) => {
    const name = getColorName(color).toLowerCase();
    return keywords.some((keyword) => name.includes(keyword));
  });

  const effectiveMatches =
    Number(response.data?.colorMatchCount ?? response.widget?.colorMatchCount ?? -1) >= 0
      ? colors
      : matches;

  const vehicleLabel = getColorVehicleLabel(response);
  const requestedLabel =
    requestedFamily === "grey"
      ? "grey/gray"
      : requestedFamily === "dual_tone"
        ? "dual-tone"
        : requestedFamily;

  if (effectiveMatches.length) {
    const names = effectiveMatches.map(getColorName).filter(Boolean);
    const shownNames = names.slice(0, 4).join(", ");
    const extra = names.length > 4 ? ` +${names.length - 4} more` : "";
    const totalLine =
      totalColorCount && totalColorCount !== names.length
        ? ` I found ${names.length} matching option${names.length > 1 ? "s" : ""} from ${totalColorCount} total colors.`
        : ` I found ${names.length} matching color option${names.length > 1 ? "s" : ""}.`;

    return `Yes — ${vehicleLabel} has ${requestedLabel} option${names.length > 1 ? "s" : ""}: ${shownNames}${extra}.${totalLine}`;
  }

  if (totalColorCount || colors.length) {
    return `No — ${vehicleLabel} does not have ${requestedLabel} color options in the current DB-backed color list.`;
  }

  return `I do not have confirmed ${requestedLabel} color data for ${vehicleLabel} yet.`;
};


const composeVehicleSpecAttributeAnswer = (response = {}) => {
  const data = response.data || {};
  const modelLabel = cleanDuplicateMakeModelLabel(
    firstText(
      data.anchorFullModel,
      response.contextPatch?.anchorFullModel,
      response.contextPatch?.selectedVehicle?.fullModel,
      [data.anchorMake, data.anchorModel].filter(Boolean).join(" "),
      response.title,
      "this model",
    ),
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


const unique = (items = []) =>
  [...new Set(asArray(items).map((item) => cleanText(item)).filter(Boolean))];

const getComposerSourceTransparency = (response = {}) =>
  response.sourceTransparency ||
  response.meta?.sourceTransparency ||
  response.data?.sourceTransparency ||
  {};

const getComposerRuntimeResultsMeta = (response = {}) =>
  asArray(response.runtimeResultsMeta || response.meta?.runtimeResultsMeta || response.data?.runtimeResultsMeta);

const getComposerSelectedVehicle = (response = {}) =>
  response.contextPatch?.selectedVehicle ||
  response.selectedVehicle ||
  response.vehicle ||
  response.data?.selectedVehicle ||
  response.data?.vehicle ||
  response.widget?.vehicle ||
  {};

const getComposerRowCount = (response = {}) => {
  const rows = getRows(response);
  if (rows.length) return rows.length;
  const source = getComposerSourceTransparency(response);
  const count = Number(
    source.recordCount ??
    source.matched ??
    source.matchedCount ??
    response.count ??
    response.matched ??
    0,
  );
  return Number.isFinite(count) && count >= 0 ? count : 0;
};

const inferComposerDataStatus = ({ response = {}, source = {}, rowCount = 0 } = {}) => {
  const existing = firstText(
    response.dataStatus,
    response.meta?.dataStatus,
    response.data?.dataStatus,
    source.dataStatus,
    source.status,
  );
  if (existing) return existing;

  const dataSource = firstText(source.dataSource, response.dataSource, response.meta?.dataSource);
  const canvasType = firstText(response.canvasType, response.data?.canvasType, response.widget?.canvasType);
  const intent = getIntent(response);

  if (
    dataSource === "unsupported_city_fast_path" ||
    dataSource === "unsupported_city" ||
    canvasType === "unsupported_city_canvas"
  ) {
    return "unsupported_city";
  }

  if (
    intent === "clarification" ||
    response.tool === "clarification" ||
    response.mode === "clarification"
  ) {
    return "clarification";
  }

  if (rowCount > 0) return "available";
  return "no_data";
};

const buildAciProvenanceEnvelope = (response = {}) => {
  const source = getComposerSourceTransparency(response);
  const runtimeResultsMeta = getComposerRuntimeResultsMeta(response);
  const selectedVehicle = getComposerSelectedVehicle(response);
  const rowCount = getComposerRowCount(response);
  const sourceCollections = unique([
    ...asArray(source.modulesChecked),
    ...runtimeResultsMeta.flatMap((item) => asArray(item.modulesChecked)),
    source.collection,
    source.module,
  ]).filter((item) => !["mongodb", "empty", "none"].includes(item));

  const tool = firstText(
    response.aciCoreBridge?.tool,
    response.meta?.aciCoreBridge?.tool,
    source.responseTool,
    response.tool,
    response.data?.tool,
    response.widget?.tool,
    getIntent(response),
  );

  const dataSource = firstText(
    source.dataSource,
    response.dataSource,
    response.meta?.dataSource,
    runtimeResultsMeta[0]?.dataSource,
    runtimeResultsMeta[0]?.source,
    sourceCollections[0],
  );

  const dataStatus = inferComposerDataStatus({ response, source, rowCount });
  const comparisonResolutionMode = firstText(
    response.comparisonResolutionMode,
    response.data?.comparisonResolutionMode,
    response.meta?.comparisonResolutionMode,
    source.comparisonResolutionMode,
    source.comparisonTrace?.comparisonResolutionMode,
  );

  const provenance = {
    tool,
    intent: getIntent(response),
    canvasType: firstText(response.canvasType, response.data?.canvasType, response.widget?.canvasType),
    dataSource,
    sourceCollections,
    rowCount,
    matched: Number(source.matched ?? response.matched ?? rowCount) || 0,
    dataStatus,
    comparisonResolutionMode,
    selectedVehicle: {
      make: firstText(selectedVehicle.make, selectedVehicle.brand),
      brand: firstText(selectedVehicle.brand, selectedVehicle.make),
      model: firstText(selectedVehicle.model),
      fullModel: firstText(selectedVehicle.fullModel, selectedVehicle.displayName),
      variant: firstText(selectedVehicle.variant, selectedVehicle.variantName, selectedVehicle.selectedVariant),
      variantKey: firstText(selectedVehicle.variantKey),
      city: firstText(selectedVehicle.city),
      citySlug: firstText(selectedVehicle.citySlug),
    },
    bridge: response.aciCoreBridge || response.meta?.aciCoreBridge || null,
  };

  return {
    sourceCollections,
    dataStatus,
    provenance,
    trace: {
      tool,
      dataSource,
      sourceCollections,
      rowCount,
      dataStatus,
      comparisonResolutionMode,
      bridgeContextIsolation: provenance.bridge?.contextIsolation || "",
      bridgeRoutingReason: provenance.bridge?.routingReason || "",
    },
  };
};


const attachAciProvenanceEnvelope = (response = {}) => {
  if (!response || typeof response !== "object") return response;

  const envelope = buildAciProvenanceEnvelope(response);

  return {
    ...response,
    ...envelope,
    comparisonResolutionMode:
      response.comparisonResolutionMode ||
      envelope.provenance?.comparisonResolutionMode ||
      envelope.trace?.comparisonResolutionMode ||
      "",
    meta: {
      ...(response.meta || {}),
      sourceCollections: envelope.sourceCollections,
      dataStatus: envelope.dataStatus,
      provenance: envelope.provenance,
      trace: envelope.trace,
      comparisonResolutionMode:
        response.comparisonResolutionMode ||
        envelope.provenance?.comparisonResolutionMode ||
        envelope.trace?.comparisonResolutionMode ||
        "",
    },
    data: {
      ...(response.data || {}),
      sourceCollections: envelope.sourceCollections,
      dataStatus: envelope.dataStatus,
      provenance: envelope.provenance,
      trace: envelope.trace,
      comparisonResolutionMode:
        response.comparisonResolutionMode ||
        envelope.provenance?.comparisonResolutionMode ||
        envelope.trace?.comparisonResolutionMode ||
        "",
    },
  };
};


export const composeAciAnswer = (response = {}) => {
  if (!response || typeof response !== "object") return response;

  const finalRecommendation =
    response.finalRecommendation ||
    response.data?.finalRecommendation ||
    response.meta?.finalRecommendation ||
    null;
  if (
    finalRecommendation?.finalRecommendationEnabled === true &&
    finalRecommendation?.answer
  ) {
    return attachAciProvenanceEnvelope({
      ...response,
      title: finalRecommendation.title || response.title,
      answer: finalRecommendation.answer,
      finalRecommendationEnabled: true,
      canUseForFinalRecommendation: true,
      data: {
        ...(response.data || {}),
        title: finalRecommendation.title || response.data?.title || response.title,
        answer: finalRecommendation.answer,
        finalRecommendation,
        finalRecommendationEnabled: true,
        canUseForFinalRecommendation: true,
      },
    });
  }

  const conditionalDecisionGuidance =
    response.conditionalDecisionGuidance ||
    response.data?.conditionalDecisionGuidance ||
    response.meta?.conditionalDecisionGuidance ||
    null;
  if (conditionalDecisionGuidance?.activated === true && response.answer) {
    return attachAciProvenanceEnvelope({
      ...response,
      data: {
        ...(response.data || {}),
        answer: response.answer,
        conditionalDecisionGuidance,
      },
    });
  }

  const responseForEnvelope = applyRequestedColorFamilyRows(response);
  const intent = getIntent(responseForEnvelope);
  let composedAnswer = "";

  if (intent === "vehicle_pricelist") {
    composedAnswer = composePriceAnswer(responseForEnvelope);
  } else if (intent === "vehicle_recommendation") {
    composedAnswer = composeVehicleRecommendationAnswer(responseForEnvelope);
  } else if (intent === "vehicle_feature_discovery") {
    composedAnswer = composeFeatureDiscoveryAnswer(responseForEnvelope);
  } else if (intent === "vehicle_colors") {
    composedAnswer = composeVehicleColorsAnswer(responseForEnvelope);
  } else if (intent === "vehicle_feature_comparison") {
    composedAnswer = composeFeatureComparisonAnswer(responseForEnvelope);
  } else if (intent === "vehicle_comparison") {
    composedAnswer = composeVehicleComparisonAnswer(response);
  } else if (intent === "vehicle_spec_attribute_answer") {
    composedAnswer = composeVehicleSpecAttributeAnswer(response);
  }

  if (!composedAnswer || composedAnswer === responseForEnvelope.answer) {
    return attachAciProvenanceEnvelope(responseForEnvelope);
  }

  return attachAciProvenanceEnvelope({
    ...responseForEnvelope,
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
  });
};

export default composeAciAnswer;
