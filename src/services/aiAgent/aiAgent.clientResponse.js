const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const hasOwn = (value, key) =>
  Object.prototype.hasOwnProperty.call(value || {}, key);

const text = (value = "", max = 240) =>
  String(value || "").replace(/\s+/g, " ").trim().slice(0, max);

const pick = (source = {}, keys = []) => {
  const result = {};
  for (const key of keys) {
    if (source?.[key] !== undefined && source?.[key] !== null) {
      result[key] = source[key];
    }
  }
  return result;
};

const compactFrame = (frame = {}) =>
  pick(frame, [
    "x", "y", "left", "top", "width", "height", "w", "h",
    "aspect_ratio", "canvas_width", "canvas_height", "naturalWidth",
    "naturalHeight", "frameMethod", "scale", "translateX", "translateY",
    "transformOrigin", "bounds",
  ]);

const compactColor = (color = {}) => ({
  ...pick(color, [
    "id", "_id", "make", "brand", "model", "rawModel", "modelKey",
    "colorName", "name", "desktopName", "mobileName", "label", "hex",
    "deep", "hexCodes", "imageModeUsed", "scopeStatus", "description",
  ]),
  imageUrl: text(
    color.imageUrl || color.normalizedImageUrl || color.cleanImageUrl,
    600,
  ),
  normalizedImageUrl: text(
    color.normalizedImageUrl || color.cleanImageUrl || color.imageUrl,
    600,
  ),
  imageFrame: compactFrame(color.imageFrame || color.frameMeta || {}),
});

const compactVehicle = (vehicle = {}, { includeGallery = true } = {}) => {
  if (!vehicle || typeof vehicle !== "object") return null;
  const result = {
    ...pick(vehicle, [
      "id", "_id", "make", "brand", "model", "modelName", "displayName",
      "fullModel", "makeKey", "modelKey", "shortModelKey", "variant",
      "variantName", "selectedVariant", "variantKey", "fuel", "fuelType",
      "fuelKey", "transmission", "transmissionKey", "city", "citySlug",
      "variantCount", "colorCount", "priceRange", "exShowroomPrice",
      "startingOnRoadPrice", "fuelText", "transmissionText", "colorName",
      "confidence", "source",
    ]),
    imageUrl: text(
      vehicle.imageUrl || vehicle.normalizedImageUrl || vehicle.cleanImageUrl,
      600,
    ),
    normalizedImageUrl: text(
      vehicle.normalizedImageUrl || vehicle.cleanImageUrl || vehicle.imageUrl,
      600,
    ),
    imageFrame: compactFrame(vehicle.imageFrame || {}),
  };

  if (vehicle.selectedColor) result.selectedColor = compactColor(vehicle.selectedColor);
  const gallery = includeGallery
    ? asArray(vehicle.visualGallery).slice(0, 14).map(compactColor)
    : [];
  if (gallery.length) result.visualGallery = gallery;
  return result;
};

const compactChargeItem = (item = {}) =>
  pick(item, [
    "key", "label", "amount", "value", "formatted", "displayValue",
    "type", "source", "selectedByDefault",
  ]);

const compactPriceBreakup = (breakup = {}) => {
  if (!breakup || typeof breakup !== "object") return undefined;
  const mandatoryItems = asArray(breakup.mandatoryCharges?.items)
    .slice(0, 12)
    .map(compactChargeItem);
  const optionalItems = asArray(
    breakup.optionalCharges?.items || breakup.optionalItems,
  ).slice(0, 12).map(compactChargeItem);
  const otherItems = asArray(
    breakup.otherChargeItems || breakup.otherItems,
  ).slice(0, 12).map(compactChargeItem);

  return {
    ...pick(breakup, [
      "priceBasis", "city", "currency", "incomplete", "legacyExShowroom",
      "legacyRto", "legacyInsurance", "legacyOtherCharges",
      "legacyOnRoadPrice", "onRoadPriceWithoutOptional",
      "onRoadPriceWithOptional", "mandatoryChargesTotal",
      "optionalChargesTotal", "computedOnRoadPrice", "canonicalOnRoadPrice",
    ]),
    exShowroom: pick(breakup.exShowroom || {}, ["key", "label", "value", "formatted"]),
    mandatoryCharges: {
      ...pick(breakup.mandatoryCharges || {}, ["total", "formatted"]),
      items: mandatoryItems,
    },
    optionalCharges: {
      ...pick(breakup.optionalCharges || {}, ["total", "formatted", "selectedByDefault"]),
      items: optionalItems,
    },
    otherChargeItems: otherItems,
    totals: pick(breakup.totals || {}, [
      "onRoadWithoutOptional", "onRoadWithoutOptionalFormatted",
      "onRoadWithOptional", "onRoadWithOptionalFormatted", "optionalDelta",
      "optionalDeltaFormatted",
    ]),
  };
};

const compactFeatureModel = (model = {}) => ({
  ...pick(model, [
    "make", "brand", "model", "fullModel", "variant", "variantName",
    "label", "featureKey", "feature", "displayName", "available", "status",
    "checkedVariants", "totalVariants", "availableCount", "previewVariants",
  ]),
  imageUrl: text(model.imageUrl || model.normalizedImageUrl, 600),
});

const compactScoreModules = (modules = {}) =>
  Object.fromEntries(
    Object.entries(modules || {}).map(([key, module = {}]) => [
      key,
      pick(module, ["key", "label", "score", "band", "status", "confidence", "scoreType"]),
    ]),
  );

const compactRow = (row = {}) => {
  const result = {
    ...pick(row, [
      "id", "_id", "make", "brand", "model", "rawModel", "modelKey",
      "modelDisplayName", "displayName", "fullModel", "name", "title",
      "label", "variant", "variantName", "variantKey", "fuel", "fuelType",
      "fuelKey", "transmission", "transmissionKey", "recommended", "price",
      "priceLabel", "exShowroomPrice", "exShowroomPriceValue",
      "exShowroomPriceLabel", "onRoadPrice", "onRoadPriceLabel",
      "onRoadPriceWithoutOptional", "onRoadPriceWithoutOptionalLabel",
      "onRoadPriceWithOptional", "onRoadPriceWithOptionalLabel", "rto",
      "rtoCharges", "insurance", "otherCharges", "mandatoryChargesTotal",
      "optionalChargesTotal", "feature", "featureKey", "category", "value",
      "displayValue", "available", "present", "included", "status",
      "checkedVariants", "totalVariants", "availableCount", "previewVariants",
      "variantFullName", "referenceExShowroomPrice", "priceSegment", "strengths",
      "watchouts", "scoreReadiness", "scoreSummary", "usageGuardrail",
      "colorName", "hex", "deep", "hexCodes", "description",
      "bodyType", "bodyTypeKey", "segment", "city", "citySlug",
      "startsFromVariant", "startsFromPrice", "startsFromPriceLabel",
      "bestUnderBudgetVariant", "bestUnderBudgetPrice", "bestUnderBudgetPriceLabel",
      "qualifyingVariantCount", "fuelTypes", "transmissions", "priceRangeLabel",
      "matchedFeature", "matchedFeatureKeys", "featureName",
    ]),
    imageUrl: text(row.imageUrl || row.normalizedImageUrl || row.cleanImageUrl, 600),
    normalizedImageUrl: text(
      row.normalizedImageUrl || row.cleanImageUrl || row.imageUrl,
      600,
    ),
    imageFrame: compactFrame(row.imageFrame || {}),
  };

  if (row.vehicle) result.vehicle = compactVehicle(row.vehicle, { includeGallery: false });
  if (row.selectedColor) result.selectedColor = compactColor(row.selectedColor);
  if (row.priceBreakup) result.priceBreakup = compactPriceBreakup(row.priceBreakup);
  if (row.optionalChargeItems) {
    result.optionalChargeItems = asArray(row.optionalChargeItems).slice(0, 12).map(compactChargeItem);
  }
  if (row.otherChargeItems) {
    result.otherChargeItems = asArray(row.otherChargeItems).slice(0, 12).map(compactChargeItem);
  }
  if (row.models) result.models = asArray(row.models).slice(0, 5).map(compactFeatureModel);
  if (row.modules) result.modules = compactScoreModules(row.modules);
  return result;
};

const compactComparisonVehicle = (vehicle = {}) =>
  compactVehicle(vehicle, { includeGallery: false }) ||
  pick(vehicle, ["make", "brand", "model", "fullModel", "variant"]);

const compactContextState = (state = {}, comparisonMode = false) => ({
  schemaVersion: state.schemaVersion || "aci_context_state_v1",
  selectedVehicle: comparisonMode
    ? null
    : hasOwn(state, "selectedVehicle") && state.selectedVehicle === null
      ? null
      : compactVehicle(state.selectedVehicle || {}, { includeGallery: false }) || {},
  activeComparison: {
    vehicles: asArray(state.activeComparison?.vehicles).map(compactComparisonVehicle),
    features: asArray(state.activeComparison?.features).slice(0, 20),
    ...pick(state.activeComparison || {}, [
      "fuelKey", "transmissionKey", "city", "citySlug", "confidence", "source",
    ]),
  },
  requested: pick(state.requested || {}, [
    "facts", "features", "topics", "specAttributes", "topic", "budget", "city", "citySlug",
  ]),
  buyerContext: state.buyerContext || {},
});

const compactContextPatch = (patch = {}) => {
  const comparisonMode = Number(patch.compoundRequest?.modelCount || 0) > 1;
  const selectedComparisonSet = patch.selectedComparisonSet || {};
  const activeComparison = patch.activeComparison || {};
  const result = {
    ...pick(patch, [
      "anchorMake", "anchorModel", "anchorFullModel", "anchorVariant",
      "anchorCity", "customerStage", "customerJourney", "leadContext",
      "compoundRequest", "conversationMode", "clearSelectedVehicle",
    ]),
    selectedVehicle: comparisonMode
      ? null
      : hasOwn(patch, "selectedVehicle")
        ? compactVehicle(patch.selectedVehicle, { includeGallery: false })
        : undefined,
    selectedComparisonSet: {
      ...pick(selectedComparisonSet, ["models", "variantSelectionMode"]),
      vehicles: asArray(selectedComparisonSet.vehicles).map(compactComparisonVehicle),
    },
    activeComparison: {
      ...pick(activeComparison, ["type", "fuelFilter", "features", "city"]),
      vehicles: asArray(activeComparison.vehicles).map(compactComparisonVehicle),
    },
  };

  if (comparisonMode) {
    result.anchorMake = "";
    result.anchorModel = "";
    result.anchorFullModel = "";
    result.anchorVariant = "";
  }

  const state = patch.contextState || patch.aciContextState;
  if (state) {
    result.contextState = compactContextState(state, comparisonMode);
    result.aciContextState = result.contextState;
  }
  return result;
};

const compactAction = (action = {}) => ({
  ...pick(action, [
    "id", "label", "title", "type", "query", "intent", "canvasType",
    "inlineType", "leadType", "priority", "displayStyle", "icon", "tone",
    "entities", "filters",
  ]),
  contextPatch: compactContextPatch(action.contextPatch || {}),
  vehicle: compactVehicle(
    action.vehicle || action.contextPatch?.selectedVehicle || {},
    { includeGallery: false },
  ),
});

const compactFilters = (filters = {}) =>
  pick(filters, [
    "budgetMin", "budgetMax", "maxBudget", "maxPrice", "priceBasis",
    "budgetBasis", "bodyType", "bodyStyle", "fuelType", "fuelKey",
    "transmission", "city", "citySlug", "feature", "ranking",
    "mustHaveFeatures", "compareFeatures",
  ]);

const getWidget = (response = {}) =>
  response.widget || asArray(response.widgets)[0] || {};

const compactWidget = (response = {}) => {
  const widget = getWidget(response);
  const rows = asArray(
    widget.rows || widget.items || response.rows || response.items || response.data?.rows,
  ).map(compactRow);
  const colors = asArray(widget.colors || response.colors || response.data?.colors).map(compactColor);
  const features = asArray(
    widget.features || widget.featureList || response.features || response.data?.features,
  ).map(compactRow);
  const contextPatch = compactContextPatch({
    ...(widget.contextPatch || {}),
    ...(response.contextPatch || {}),
  });
  const comparisonMode = Number(contextPatch.compoundRequest?.modelCount || 0) > 1;
  const vehicle = comparisonMode
    ? null
    : compactVehicle(
        widget.vehicle || response.vehicle || response.contextPatch?.selectedVehicle || {},
      );
  const actions = asArray(widget.actions || response.actions).slice(0, 8).map(compactAction);
  const leadingQuestions = asArray(
    widget.leadingQuestions || response.leadingQuestions,
  ).slice(0, 5).map(compactAction);

  return {
    ...pick(widget, [
      "type", "title", "subtitle", "summary", "answer", "intent", "displayMode",
      "canvasType", "inlineType", "city", "count", "totalVariants",
      "totalColorCount", "dataStatus", "source", "dataSource",
      "comparisonResolutionMode", "featureName", "featureKey",
    ]),
    intent: response.intent || widget.intent || "",
    displayMode: response.displayMode || widget.displayMode || "",
    canvasType: response.canvasType || widget.canvasType || "",
    inlineType: response.inlineType || widget.inlineType || "",
    rows,
    colors,
    features,
    featureList: features,
    filters: compactFilters(widget.filters || response.filters || response.data?.filters || {}),
    vehicle,
    selectedColor: widget.selectedColor ? compactColor(widget.selectedColor) : undefined,
    contextPatch,
    actions,
    leadingQuestions,
    data: {
      vehicle,
      contextPatch,
      filters: compactFilters(widget.filters || response.filters || response.data?.filters || {}),
    },
  };
};

const compactAnswerBlock = (response = {}, index = 0) => {
  const widget = compactWidget(response);
  return {
    id: `answer-block-${index}-${text(response.intent || widget.intent || "result", 80)}`,
    answer: text(response.answer || widget.answer, 1200),
    title: text(response.title || widget.title, 180),
    intent: response.intent || widget.intent || "",
    displayMode: response.displayMode || widget.displayMode || "",
    canvasType: response.canvasType || widget.canvasType || "",
    inlineType: response.inlineType || widget.inlineType || "",
    widget,
  };
};

export const compactAciClientResponse = (response = {}) => {
  if (!response || typeof response !== "object") return response;
  const related = asArray(response.secondaryResponses);
  const answerBlocks = related.length
    ? [response, ...related].map(compactAnswerBlock)
    : [];
  const widget = answerBlocks[0]?.widget || compactWidget(response);
  const contextPatch = compactContextPatch(response.contextPatch || {});

  return {
    ok: true,
    answer: text(response.answer, 1800),
    title: text(response.title || widget.title, 180),
    intent: response.intent || widget.intent || "",
    displayMode: response.displayMode || widget.displayMode || "",
    canvasType: response.canvasType || widget.canvasType || "",
    inlineType: response.inlineType || widget.inlineType || "",
    widget,
    vehicle: widget.vehicle,
    contextPatch,
    actions: asArray(response.actions).slice(0, 12).map(compactAction),
    leadingQuestions: asArray(response.leadingQuestions).slice(0, 5).map(compactAction),
    journeyGuidance: response.journeyGuidance || contextPatch.customerJourney || null,
    answerBlocks,
    compoundRequest: contextPatch.compoundRequest || null,
    sourceTransparency: pick(response.sourceTransparency || {}, [
      "responseTool", "tools", "modulesChecked", "sourceCollections",
    ]),
    runtimeResultsMeta: asArray(response.runtimeResultsMeta).map((item = {}) =>
      pick(item, ["tool", "intent", "matched", "durationMs", "status"]),
    ),
    service: response.service || {},
  };
};

export default compactAciClientResponse;
