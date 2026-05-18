import { buildFeatureExplorerPayload, buildFeatureDiscoveryPayload } from "./aiAgent.featurePayloadBuilder.js";
const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const toArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value].filter(Boolean);
};

const firstText = (...values) => {
  for (const value of values) {
    const text = cleanText(value);
    if (text) return text;
  }
  return "";
};

const slugify = (value = "", fallback = "item") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "") || fallback;

const getWidget = (response = {}) =>
  response.widget || toArray(response.widgets)[0] || null;

const getRows = (response = {}, widget = null) =>
  toArray(
    response.rows ||
      response.items ||
      response.records ||
      response.data?.rows ||
      response.data?.items ||
      response.data?.records ||
      response.data?.features ||
      response.features ||
      widget?.rows ||
      widget?.items ||
      widget?.records ||
      widget?.features ||
      widget?.colors,
  );


const normalizeFeatureSourceTransparency = (response = {}, source = {}) => {
  const intent = response.intent || response.widget?.intent || "";
  const canvasType = response.canvasType || response.widget?.canvasType || "";

  if (
    intent !== "vehicle_model_features_explorer" &&
    intent !== "vehicle_feature_discovery" &&
    canvasType !== "features_explorer_canvas" &&
    canvasType !== "feature_match_builder_canvas"
  ) {
    return source;
  }

  const modules = new Set([
    ...toArray(source.modulesChecked),
    "vehicle_features",
  ]);

  const activeStatusSource =
    response.widget?.activeStatusSource ||
    response.data?.activeStatusSource ||
    "";

  if (activeStatusSource === "vehicles" || response.widget?.currentPricelistMatched === true) {
    modules.add("vehicles");
  }

  return {
    ...source,
    responseTool:
      intent === "vehicle_feature_discovery"
        ? "vehicle_feature_discovery"
        : "vehicle_features",
    modulesChecked: [...modules],
    dataSource: source.dataSource || "mongodb",
  };
};


const normalizeSourceTransparency = (response = {}, widget = null) => {
  const raw =
    response.sourceTransparency ||
    widget?.sourceTransparency ||
    response.meta?.sourceTransparency ||
    response.data?.sourceTransparency ||
    null;

  if (Array.isArray(raw)) {
    return {
      modulesChecked: raw.filter(Boolean),
      recordCount: getRows(response, widget).length,
      matched: getRows(response, widget).length,
      dataSource: raw[0] || "",
    };
  }

  if (raw && typeof raw === "object") {
    const modulesChecked =
      toArray(raw.modulesChecked).length > 0
        ? toArray(raw.modulesChecked)
        : toArray(raw.module || raw.collection || raw.dataSource);

    return normalizeFeatureSourceTransparency(response, {
      ...raw,
      modulesChecked,
      recordCount:
        raw.recordCount ??
        raw.matched ??
        raw.matchedCount ??
        getRows(response, widget).length,
      matched:
        raw.matched ??
        raw.matchedCount ??
        raw.recordCount ??
        getRows(response, widget).length,
    });
  }

  const inferredModule =
    response.intent === "vehicle_colors"
      ? "vehicle_colors_v2"
      : response.intent?.includes("feature")
        ? "vehicle_features"
        : response.intent?.includes("price") || response.canvasType === "pricelist_canvas"
          ? "vehicles"
          : response.tool || response.intent || "";

  return normalizeFeatureSourceTransparency(response, {
    modulesChecked: inferredModule ? [inferredModule] : [],
    recordCount: getRows(response, widget).length,
    matched: getRows(response, widget).length,
    dataSource: inferredModule,
  });
};

const buildRuntimeResultsMeta = (response = {}, widget = null, source = {}) => {
  const existing = toArray(
    response.runtimeResultsMeta ||
      response.executor?.runtimeResultsMeta ||
      response.meta?.runtimeResultsMeta,
  );

  const modulesChecked = toArray(source.modulesChecked);
  const tool =
    source.responseTool ||
    response.tool ||
    widget?.tool ||
    (response.intent === "vehicle_colors"
      ? "vehicle_colors"
      : response.intent === "vehicle_model_features_explorer"
        ? "vehicle_features"
        : response.intent === "vehicle_feature_discovery"
          ? "vehicle_feature_discovery"
          : response.intent?.includes("feature")
            ? "vehicle_feature_lookup"
            : response.intent || "");

  if (existing.length) {
    return existing.map((item, index) => {
      const shouldNormalizeFeatureMeta =
        response.intent === "vehicle_model_features_explorer" ||
        response.intent === "vehicle_feature_discovery" ||
        response.canvasType === "features_explorer_canvas" ||
        response.canvasType === "feature_match_builder_canvas";

      if (!shouldNormalizeFeatureMeta) return item;

      return {
        ...item,
        index: item.index ?? index,
        tool,
        source: modulesChecked[0] || item.source || "",
        modulesChecked:
          modulesChecked.length > 0
            ? modulesChecked
            : toArray(item.modulesChecked),
      };
    });
  };

  return [
    {
      tool,
      index: 0,
      matched:
        source.matched ??
        source.matchedCount ??
        source.recordCount ??
        getRows(response, widget).length,
      source: source.dataSource || modulesChecked[0] || "",
      modulesChecked,
      error: "",
    },
  ];
};

const buildContextSnapshot = ({ response = {}, widget = null, message = "", context = {} } = {}) => {
  const contextPatch = response.contextPatch || {};
  const selectedVehicle =
    contextPatch.selectedVehicle ||
    response.vehicle ||
    response.data?.vehicle ||
    widget?.vehicle ||
    context.selectedVehicle ||
    {};

  const anchorMake = firstText(
    contextPatch.anchorMake,
    contextPatch.anchorBrand,
    selectedVehicle.make,
    selectedVehicle.brand,
    response.make,
    response.brand,
    widget?.make,
    widget?.brand,
    response.data?.make,
    response.data?.brand,
  );

  const anchorModel = firstText(
    contextPatch.anchorModel,
    selectedVehicle.model,
    response.model,
    widget?.model,
    response.data?.model,
    context.anchorModel,
  );

  const anchorVariant = firstText(
    contextPatch.anchorVariant,
    selectedVehicle.variant,
    selectedVehicle.selectedVariant,
    response.variant,
    widget?.variant,
    response.data?.variant,
    context.anchorVariant,
  );

  const anchorCity = firstText(
    contextPatch.anchorCity,
    selectedVehicle.citySlug,
    selectedVehicle.city,
    response.city,
    widget?.city,
    response.data?.city,
    context.anchorCity,
    context.city,
    "new-delhi",
  );

  const feature = firstText(
    contextPatch.feature,
    response.feature,
    response.data?.feature,
    widget?.feature,
  );

  return {
    intent: response.intent || "",
    displayMode: response.displayMode || "",
    canvasType: response.canvasType || widget?.canvasType || "",
    inlineType: response.inlineType || "",
    anchorMake,
    make: anchorMake,
    anchorModel,
    model: anchorModel,
    anchorVariant,
    variant: anchorVariant,
    anchorCity,
    city: anchorCity,
    feature,
    message: cleanText(message),
  };
};

const hasSpecificFeature = (text = "") =>
  /\b(sunroof|airbags?|6 airbags|adas|wireless charging|ventilated seats|360 camera|camera|cruise|climate control|tpms|isofix|abs|esc|esp|carplay|android auto|boot space|ground clearance|mileage|engine|transmission)\b/i.test(
    text,
  );

const isFeatureExplorerRequest = (message = "", response = {}) => {
  const text = cleanText(message).toLowerCase();

  if (
    /\b(show|open|explore|list|display|view)\b/i.test(text) &&
    /\b(features?|specs|specifications|catalogue|catalog|brochure|feature explorer)\b/i.test(text) &&
    !hasSpecificFeature(text)
  ) {
    return true;
  }

  return (
    response.intent === "vehicle_feature_answer" &&
    cleanText(response.data?.feature).toLowerCase() === "features"
  );
};

const isFeatureDiscoveryRequest = (message = "") => {
  const text = cleanText(message).toLowerCase();

  return (
    /\b(which|show|find|list|cheapest)\b/i.test(text) &&
    /\b(variants?|cars?|suvs?|sedans?|hatchbacks?)\b/i.test(text) &&
    /\b(have|has|with|get|gets)\b/i.test(text) &&
    hasSpecificFeature(text)
  );
};

const normalizeFeatureRowsForWidget = (rows = []) =>
  rows.map((row, index) => ({
    id: row.id || row._id || `feature-row-${index + 1}`,
    ...row,
    label: row.label || row.variant || row.variantName || row.model || `Result ${index + 1}`,
  }));

const applyFeatureIntentCorrections = ({ response = {}, message = "", widget = null } = {}) => {
  const corrected = { ...response };
  const rows = normalizeFeatureRowsForWidget(getRows(corrected, widget));

  if (isFeatureExplorerRequest(message, corrected)) {
    corrected.intent = "vehicle_model_features_explorer";
    corrected.displayMode = "canvas";
    corrected.canvasType = "features_explorer_canvas";
    corrected.inlineType = null;
    corrected.tool = corrected.tool || "vehicle_features";
    corrected.title =
      corrected.title && !/features in/i.test(corrected.title)
        ? corrected.title
        : `${firstText(corrected.data?.model, corrected.contextPatch?.anchorModel, "Selected car")} features`;
    corrected.answer =
      corrected.answer && !/features feature records/i.test(corrected.answer)
        ? corrected.answer
        : `I found stored feature data for ${firstText(corrected.data?.model, corrected.contextPatch?.anchorModel, "this car")}.`;

    corrected.widget = {
      ...(widget || {}),
      type: "vehicle_features",
      tool: "vehicle_features",
      intent: corrected.intent,
      canvasType: corrected.canvasType,
      title: corrected.title,
      vehicle:
        corrected.contextPatch?.selectedVehicle ||
        corrected.data?.vehicle ||
        widget?.vehicle ||
        {},
      rows,
      items: rows,
      features: rows,
      featureList: rows,
      model: firstText(corrected.data?.model, widget?.model, corrected.contextPatch?.anchorModel),
      variant: firstText(corrected.data?.variant, widget?.variant, corrected.contextPatch?.anchorVariant),
    };
    corrected.widgets = [corrected.widget];
  }

  if (isFeatureDiscoveryRequest(message)) {
    corrected.intent = "vehicle_feature_discovery";
    corrected.displayMode = "canvas";
    corrected.canvasType = corrected.canvasType || "feature_match_builder_canvas";
    corrected.inlineType = null;
    corrected.tool = corrected.tool || "vehicle_feature_discovery";
    corrected.title =
      corrected.title && !/\bin\b/i.test(corrected.title)
        ? corrected.title
        : `${firstText(corrected.data?.feature, "Feature")} matches`;
    corrected.answer =
      rows.length > 0
        ? `I found ${rows.length} matching feature records.`
        : corrected.answer || "I checked stored feature records for matching variants.";

    corrected.widget = {
      ...(widget || {}),
      type: "vehicle_feature_discovery",
      tool: "vehicle_feature_discovery",
      intent: corrected.intent,
      canvasType: corrected.canvasType,
      title: corrected.title,
      vehicle:
        corrected.contextPatch?.selectedVehicle ||
        corrected.data?.vehicle ||
        widget?.vehicle ||
        {},
      rows,
      items: rows,
      feature: firstText(corrected.data?.feature, widget?.feature),
      matchedFeature: firstText(corrected.data?.feature, widget?.feature),
      model: firstText(corrected.data?.model, widget?.model, corrected.contextPatch?.anchorModel),
      variant: firstText(corrected.data?.variant, widget?.variant, corrected.contextPatch?.anchorVariant),
    };
    corrected.widgets = [corrected.widget];
  }

  return corrected;
};

const ensureWidget = (response = {}) => {
  const existingWidget = getWidget(response);
  const canvasType = response.canvasType || existingWidget?.canvasType || "";
  const rows = getRows(response, existingWidget);

  if (existingWidget) {
    return {
      ...response,
      widget: existingWidget,
      widgets: toArray(response.widgets).length ? response.widgets : [existingWidget],
    };
  }

  if (!canvasType && !response.inlineType) return response;

  const widget = {
    type: response.tool || response.intent || "aci_response",
    tool: response.tool || response.intent || "aci_response",
    intent: response.intent || "",
    canvasType,
    inlineType: response.inlineType || "",
    title: response.title || "",
    answer: response.answer || "",
    vehicle: response.vehicle || response.data?.vehicle || response.contextPatch?.selectedVehicle || {},
    rows,
    items: rows,
    data: response.data || {},
  };

  return {
    ...response,
    widget,
    widgets: [widget],
  };
};

const applyColorAvailabilityMarkers = (response = {}) => {
  if (response.intent !== "vehicle_colors" && response.canvasType !== "color_studio_canvas") {
    return response;
  }

  const widget = getWidget(response) || {};

  return {
    ...response,
    availabilityScope: response.availabilityScope || "model_level",
    modelLevelAvailability: true,
    variantWiseAvailabilityAvailable: false,
    widget: {
      ...widget,
      availabilityScope: widget.availabilityScope || "model_level",
      modelLevelAvailability: true,
      variantWiseAvailabilityAvailable: false,
    },
    widgets: [
      {
        ...widget,
        availabilityScope: widget.availabilityScope || "model_level",
        modelLevelAvailability: true,
        variantWiseAvailabilityAvailable: false,
      },
    ],
  };
};


const enhanceFeaturePayloads = async (response = {}) => {
  const widget = getWidget(response) || {};
  const intent = response.intent || widget.intent || "";
  const canvasType = response.canvasType || widget.canvasType || "";

  if (
    intent === "vehicle_model_features_explorer" ||
    canvasType === "features_explorer_canvas"
  ) {
    try {
      const payload = await buildFeatureExplorerPayload({ response, widget });

      if (!payload) return response;

      const enhancedWidget = {
        ...widget,
        ...payload,
        rows: payload.rows || [],
        items: payload.items || [],
        features: payload.features || [],
        featureList: payload.featureList || payload.features || [],
      };

      return {
        ...response,
        intent: "vehicle_model_features_explorer",
        displayMode: "canvas",
        canvasType: "features_explorer_canvas",
        inlineType: null,
        title: payload.title || response.title,
        answer: payload.answer || response.answer,
        vehicle: payload.vehicle || response.vehicle,
        data: {
          ...(response.data || {}),
          ...(payload.data || {}),
          variants: payload.variants || [],
          variantOptions: payload.variantOptions || payload.variants || [],
          selectedVariant: payload.selectedVariant || "",
          selectedVariantId: payload.selectedVariantId || "",
          featureGroups: payload.featureGroups || [],
          features: payload.features || [],
          featureList: payload.featureList || payload.features || [],
          quickSpecs: payload.quickSpecs || [],
          highlights: payload.highlights || [],
          categoryStats: payload.categoryStats || {},
          featureStats: payload.featureStats || payload.categoryStats || {},
        },
        widget: enhancedWidget,
        widgets: [enhancedWidget],
        rows: payload.rows || [],
        items: payload.items || [],
        features: payload.features || [],
        contextPatch: {
          ...(response.contextPatch || {}),
          selectedVehicle: payload.vehicle || response.contextPatch?.selectedVehicle,
          anchorMake:
            payload.vehicle?.make ||
            response.contextPatch?.anchorMake ||
            response.contextPatch?.anchorBrand ||
            "",
          anchorModel:
            payload.vehicle?.model ||
            response.contextPatch?.anchorModel ||
            response.data?.model ||
            "",
          anchorVariant:
            payload.selectedVariant ||
            response.contextPatch?.anchorVariant ||
            response.data?.variant ||
            "",
          anchorCity:
            payload.vehicle?.citySlug ||
            payload.vehicle?.city ||
            response.contextPatch?.anchorCity ||
            response.data?.city ||
            "new-delhi",
        },
      };
    } catch (error) {
      return {
        ...response,
        meta: {
          ...(response.meta || {}),
          featurePayloadBuilderError: error.message,
        },
      };
    }
  }

  if (
    intent === "vehicle_feature_discovery" ||
    canvasType === "feature_match_builder_canvas"
  ) {
    try {
      const payload = await buildFeatureDiscoveryPayload({ response, widget });

      if (!payload) return response;

      const enhancedWidget = {
        ...widget,
        ...payload,
        rows: payload.rows || [],
        items: payload.items || [],
        features: payload.features || [],
        featureList: payload.featureList || payload.features || [],
      };

      return {
        ...response,
        intent: "vehicle_feature_discovery",
        displayMode: "canvas",
        canvasType: payload.canvasType || "feature_match_builder_canvas",
        inlineType: null,
        title: payload.title || response.title,
        answer: payload.answer || response.answer,
        vehicle: payload.vehicle || response.vehicle,
        data: {
          ...(response.data || {}),
          ...(payload.data || {}),
          feature: payload.feature || response.data?.feature || "",
          variants: payload.variants || [],
          matchedVariants: payload.matchedVariants || [],
          rows: payload.rows || [],
        },
        widget: enhancedWidget,
        widgets: [enhancedWidget],
        rows: payload.rows || [],
        items: payload.items || [],
        features: payload.features || [],
        contextPatch: {
          ...(response.contextPatch || {}),
          selectedVehicle: payload.vehicle || response.contextPatch?.selectedVehicle,
          anchorMake:
            payload.vehicle?.make ||
            response.contextPatch?.anchorMake ||
            response.contextPatch?.anchorBrand ||
            "",
          anchorModel:
            payload.vehicle?.model ||
            response.contextPatch?.anchorModel ||
            response.data?.model ||
            "",
          anchorCity:
            payload.vehicle?.city ||
            response.contextPatch?.anchorCity ||
            response.data?.city ||
            "new-delhi",
          feature: payload.feature || response.contextPatch?.feature || response.data?.feature || "",
        },
      };
    } catch (error) {
      return {
        ...response,
        meta: {
          ...(response.meta || {}),
          featurePayloadBuilderError: error.message,
        },
      };
    }
  }

  return response;
};


export const normalizeAciFinalResponse = async (response = {}, options = {}) => {
  if (!response || typeof response !== "object") return response;

  const message = firstText(options.message, response.message, response.query);
  const context = options.context || {};
  let normalized = { ...response };

  normalized = applyFeatureIntentCorrections({
    response: normalized,
    message,
    widget: getWidget(normalized),
  });

  normalized = ensureWidget(normalized);
  normalized = applyColorAvailabilityMarkers(normalized);
  normalized = await enhanceFeaturePayloads(normalized);

  const widget = getWidget(normalized);
  const sourceTransparency = normalizeSourceTransparency(normalized, widget);
  const runtimeResultsMeta = buildRuntimeResultsMeta(normalized, widget, sourceTransparency);
  const contextSnapshot =
    normalized.contextSnapshot && Object.keys(normalized.contextSnapshot).length
      ? normalized.contextSnapshot
      : buildContextSnapshot({
          response: normalized,
          widget,
          message,
          context,
        });

  return {
    ...normalized,
    sourceTransparency,
    runtimeResultsMeta,
    contextSnapshot,
    contextPatch: {
      ...(normalized.contextPatch || {}),
      anchorMake:
        normalized.contextPatch?.anchorMake ||
        normalized.contextPatch?.anchorBrand ||
        contextSnapshot.anchorMake ||
        "",
      anchorModel:
        normalized.contextPatch?.anchorModel || contextSnapshot.anchorModel || "",
      anchorVariant:
        normalized.contextPatch?.anchorVariant || contextSnapshot.anchorVariant || "",
      anchorCity:
        normalized.contextPatch?.anchorCity || contextSnapshot.anchorCity || "new-delhi",
      selectedVehicle:
        normalized.contextPatch?.selectedVehicle ||
        normalized.vehicle ||
        normalized.data?.vehicle ||
        widget?.vehicle ||
        {
          make: contextSnapshot.anchorMake,
          brand: contextSnapshot.anchorMake,
          model: contextSnapshot.anchorModel,
          variant: contextSnapshot.anchorVariant,
          city: contextSnapshot.anchorCity,
        },
    },
  };
};

export default normalizeAciFinalResponse;
