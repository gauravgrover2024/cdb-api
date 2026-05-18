import mongoose from "mongoose";
import { buildFeatureExplorerPayload } from "../../aiAgent.featurePayloadBuilder.js";

const TOOL_NAME = "vehicle_features";
const INTENT = "vehicle_model_features_explorer";
const CANVAS_TYPE = "features_explorer_canvas";
const COLLECTION_NAME = "vehicle_features";
const DEFAULT_CITY = "new-delhi";

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const titleCaseWords = (value = "") =>
  cleanText(value)
    .split(" ")
    .map((part) => {
      if (!part) return "";
      if (/^[A-Z0-9]+$/.test(part)) return part;
      if (/^\d/.test(part)) return part.toUpperCase();
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join(" ");

const firstText = (...values) => {
  for (const value of values) {
    const text = cleanText(value);
    if (text) return text;
  }
  return "";
};

const escapeRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const exactRegex = (value = "") =>
  new RegExp(`^\\s*${escapeRegex(cleanText(value))}\\s*$`, "i");

const slugify = (value = "", fallback = "") => {
  const slug = cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  return slug || fallback;
};

const normalizeKey = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeCompactKey = (value = "") =>
  normalizeKey(value).replace(/[^a-z0-9]/g, "");

const normalizeModelKey = (value = "", make = "") => {
  let text = normalizeKey(value);
  const makeKey = normalizeKey(make);

  if (makeKey && text.startsWith(`${makeKey} `)) {
    text = text.slice(makeKey.length).trim();
  }

  if (makeKey === "maruti" && text.startsWith("maruti suzuki ")) {
    text = text.replace(/^maruti suzuki\s+/, "");
  }

  return text;
};

const asArray = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) return value.filter(Boolean);
  return [value].filter(Boolean);
};

const getEntities = (toolPlan = {}) => ({
  ...(toolPlan.entities || {}),
  ...(toolPlan.input || {}),
  ...(toolPlan.filters || {}),
});

const removeRequestNoise = (message = "") =>
  cleanText(message)
    .replace(/\b(show|open|display|list|all|full|complete|get|find|tell|me|please|pls)\b/gi, " ")
    .replace(/\b(features?|feature explorer|specs|specifications|catalogue|catalog|brochure)\b/gi, " ")
    .replace(/\b(of|for|in|with|has|have|does|available|variant|variants|car|cars)\b/gi, " ")
    .replace(/\b(sunroof|airbags?|adas|wireless charging|ventilated seats|360 camera|camera|cruise|climate|tpms|isofix|abs|esc|esp)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

const getRequestedMake = ({ toolPlan = {}, context = {} } = {}) => {
  const entities = getEntities(toolPlan);
  return firstText(
    entities.make,
    entities.brand,
    toolPlan.make,
    toolPlan.brand,
    context.anchorMake,
    context.selectedVehicle?.make,
    context.selectedVehicle?.brand,
  );
};

const getRequestedModel = ({ toolPlan = {}, context = {}, userMessage = "" } = {}) => {
  const entities = getEntities(toolPlan);

  const direct = firstText(
    entities.model,
    entities.models?.[0],
    toolPlan.model,
    toolPlan.models?.[0],
    context.anchorModel,
    context.selectedVehicle?.model,
  );

  if (direct) return titleCaseWords(direct);

  const fallback = removeRequestNoise(userMessage);
  return titleCaseWords(fallback);
};

const getRequestedVariant = ({ toolPlan = {}, context = {}, userMessage = "" } = {}) => {
  const entities = getEntities(toolPlan);

  const direct = firstText(
    entities.variant,
    entities.selectedVariant,
    toolPlan.variant,
    toolPlan.selectedVariant,
    context.anchorVariant,
    context.selectedVehicle?.variant,
    context.selectedVehicle?.selectedVariant,
  );

  if (direct) return titleCaseWords(direct);

  const match = cleanText(userMessage).match(
    /\b(hte|htk|htx|gtx|sx|vx|zx|lxi|zxi|vxi|xza|xz|alpha|delta|sigma|sportz|asta|premium|comfortline|highline|hx\d+|s\s?opt|sx\s?opt|top|base|ivt|dct|mt|at|amt|cvt|turbo)\b(?:\s+\b(plus|turbo|ivt|dct|mt|at|amt|cvt|dt|opt|automatic|manual|line|hybrid)\b)*/i,
  );

  return titleCaseWords(match?.[0] || "");
};

const buildMakeModelQuery = ({ make = "", model = "" } = {}) => {
  const cleanMake = titleCaseWords(make);
  const cleanModel = titleCaseWords(model);
  const modelWithoutMake = normalizeModelKey(cleanModel, cleanMake);

  const modelRegex = exactRegex(modelWithoutMake || cleanModel);
  const modelSlug = slugify(modelWithoutMake || cleanModel);
  const makeRegex = cleanMake ? exactRegex(cleanMake) : null;
  const makeSlug = cleanMake ? slugify(cleanMake) : "";

  const and = [
    {
      $or: [
        { model: modelRegex },
        { modelName: modelRegex },
        { model_name: modelRegex },
        { model_slug: modelSlug },
        { modelSlug },
      ],
    },
    {
      $or: [
        { features: { $exists: true, $ne: {} } },
        { featureList: { $exists: true, $ne: [] } },
      ],
    },
  ];

  if (makeRegex) {
    and.push({
      $or: [
        { brand: makeRegex },
        { make: makeRegex },
        { brandName: makeRegex },
        { brand_slug: makeSlug },
        { makeSlug },
      ],
    });
  }

  return { $and: and };
};

const rowModelMatchesRequest = ({ row = {}, requestedModel = "", requestedMake = "" } = {}) => {
  const rowModel = firstText(row.model, row.modelName, row.model_name);
  if (!requestedModel || !rowModel) return true;

  const rowModelKey = normalizeModelKey(rowModel, requestedMake);
  const requestedModelKey = normalizeModelKey(requestedModel, requestedMake);

  return (
    rowModelKey === requestedModelKey ||
    normalizeCompactKey(rowModelKey) === normalizeCompactKey(requestedModelKey)
  );
};

const variantMatches = (doc = {}, requestedVariant = "") => {
  const variant = firstText(doc.variant, doc.variantName, doc.name, doc.title);
  if (!requestedVariant) return true;
  if (!variant) return false;

  const rowKey = normalizeKey(variant);
  const requestedKey = normalizeKey(requestedVariant);

  return (
    rowKey === requestedKey ||
    rowKey.includes(requestedKey) ||
    requestedKey.includes(rowKey)
  );
};

const splitFeatureKey = (key = "") => {
  const text = cleanText(key);

  if (text.includes("|")) {
    const [section, ...rest] = text.split("|").map(cleanText);
    return { section, name: rest.join(" | ") || section };
  }

  if (text.includes(" - ")) {
    const [section, ...rest] = text.split(" - ").map(cleanText);
    return { section, name: rest.join(" - ") || section };
  }

  if (text.includes(":")) {
    const [section, ...rest] = text.split(":").map(cleanText);
    return { section, name: rest.join(": ") || section };
  }

  return { section: "", name: text };
};

const normalizeCategory = (section = "", name = "") => {
  const text = `${section} ${name}`.toLowerCase();

  if (/adas|lane|cruise|aeb|blind|collision|autonomous|assist|departure|forward collision|driver assistance/.test(text)) {
    return "adas";
  }

  if (/airbag|safety|esc|esp|isofix|brake|tpms|hill|stability|ncap|child|abs|traction|seat belt/.test(text)) {
    return "safety";
  }

  if (/audio|speaker|touch|screen|android|apple|carplay|infotain|connected|music|jbl|display|navigation|bluetooth|usb/.test(text)) {
    return "infotainment";
  }

  if (/wireless|charger|mode|convenience|keyless|tailgate|memory|climate|boot|start|parking|camera|sensor|wiper|defogger|orvm|mirror/.test(text)) {
    return "convenience";
  }

  return "comfort";
};

const isUnavailableValue = (value) => {
  if (value === false || value === null || value === undefined) return true;

  const text = cleanText(value).toLowerCase();
  if (!text) return true;

  return /^(no|na|n\/a|not available|unavailable|absent|nil|false|-)$/.test(text);
};

const isPositiveValue = (value) => !isUnavailableValue(value);

const isHighlightFeature = (section = "", name = "", value = "") => {
  const text = `${section} ${name} ${value}`.toLowerCase();

  return /sunroof|adas|airbags?|360|camera|ventilated|wireless|carplay|android auto|cruise|climate|tpms|isofix|esc|abs|touchscreen|speaker|connected/.test(text);
};

const objectToFeaturesArray = (featuresObj = {}, doc = {}) => {
  if (!featuresObj || typeof featuresObj !== "object") return [];

  return Object.entries(featuresObj)
    .map(([rawKey, rawValue], index) => {
      const { section, name } = splitFeatureKey(rawKey);
      const value = cleanText(rawValue);
      const category = normalizeCategory(section, name);
      const available = isPositiveValue(rawValue);

      return {
        id: `${slugify(`${doc._id || doc.variant || "feature"}-${rawKey}`, `feature-${index}`)}`,
        key: rawKey,
        name,
        label: name,
        title: name,
        section,
        group: section,
        category,
        value,
        displayValue: value || (available ? "Available" : "Not available"),
        available,
        present: available,
        included: available,
        highlight: isHighlightFeature(section, name, value),
        variant: firstText(doc.variant, doc.variantName),
        model: firstText(doc.model, doc.modelName),
        brand: firstText(doc.brand, doc.make),
      };
    })
    .filter((item) => item.name);
};

const buildCategoryStats = (features = []) => {
  const categories = ["comfort", "safety", "infotainment", "convenience", "adas"];

  return categories.reduce((acc, category) => {
    const rows = features.filter((feature) => feature.category === category);
    acc[category] = {
      available: rows.filter((feature) => feature.available).length,
      total: rows.length,
    };
    return acc;
  }, {});
};

const buildQuickSearches = (features = []) => {
  const preferred = [];

  const add = (value) => {
    if (!value) return;
    if (!preferred.some((item) => item.toLowerCase() === value.toLowerCase())) {
      preferred.push(value);
    }
  };

  features.forEach((feature) => {
    const text = `${feature.name} ${feature.section}`.toLowerCase();
    if (/sunroof/.test(text)) add("sunroof");
    if (/adas/.test(text)) add("ADAS");
    if (/360|camera/.test(text)) add("360 camera");
    if (/airbag/.test(text)) add("airbags");
    if (/carplay|android/.test(text)) add("wireless CarPlay");
    if (/ventilated/.test(text)) add("ventilated seats");
  });

  return preferred.slice(0, 5);
};

const buildHighlights = (features = []) =>
  features
    .filter((feature) => feature.available && feature.highlight)
    .slice(0, 5)
    .map((feature) =>
      feature.value && !/^(yes|available)$/i.test(feature.value)
        ? `${feature.name}: ${feature.value}`
        : `${feature.name} available`,
    );

const buildVehicle = ({ make = "", model = "", variant = "", city = DEFAULT_CITY, features = [] } = {}) => {
  const brand = titleCaseWords(make);
  const cleanModel = titleCaseWords(model);
  const displayName = [brand, cleanModel].filter(Boolean).join(" ") || cleanModel || "Selected car";

  return {
    id: slugify([brand, cleanModel].filter(Boolean).join("-"), "vehicle-features"),
    make: brand,
    brand,
    model: cleanModel,
    displayName,
    variant: titleCaseWords(variant),
    selectedVariant: titleCaseWords(variant),
    city,
    citySlug: city,
    features,
    featureList: features,
  };
};

const normalizeVariantRow = (doc = {}, selected = false) => {
  const label = firstText(doc.variant, doc.variantName, doc.name, doc.title, "Variant");
  const features = objectToFeaturesArray(doc.features || {}, doc);

  return {
    id: String(doc._id || slugify(label)),
    label,
    name: label,
    variant: label,
    variantName: label,
    selected,
    bodyType: doc.body_type_bucket || doc.bodyType || "",
    seatingCapacity: doc.seating_capacity || doc.seatingCapacity || null,
    featureCount: features.length,
    features,
    featureList: features,
  };
};

const buildActions = ({ vehicle, model, variant }) => [
  {
    id: "features-show-price",
    label: "Show price",
    title: "Show price",
    query: `Show ${model}${variant ? ` ${variant}` : ""} price`,
    intent: "vehicle_pricelist",
    canvasType: "pricelist_canvas",
    vehicle,
    contextPatch: {
      selectedVehicle: vehicle,
      anchorModel: model,
      anchorVariant: variant,
      anchorCity: vehicle.citySlug || DEFAULT_CITY,
    },
  },
  {
    id: "features-show-colors",
    label: "Show colors",
    title: "Show colors",
    query: `Show colors of ${model}`,
    intent: "vehicle_colors",
    canvasType: "color_studio_canvas",
    vehicle,
    contextPatch: {
      selectedVehicle: vehicle,
      anchorModel: model,
      anchorVariant: variant,
      anchorCity: vehicle.citySlug || DEFAULT_CITY,
    },
  },
  {
    id: "features-compare-variants",
    label: "Compare variants",
    title: "Compare variants",
    query: `Compare ${model} variants`,
    intent: "vehicle_variant_comparison",
    canvasType: "comparison_canvas",
    vehicle,
    contextPatch: {
      selectedVehicle: vehicle,
      anchorModel: model,
      anchorVariant: variant,
      anchorCity: vehicle.citySlug || DEFAULT_CITY,
    },
  },
];

const buildLeadingQuestions = ({ model, variant }) => [
  {
    id: "features-sunroof",
    label: "Does it have sunroof?",
    title: "Does it have sunroof?",
    query: `Does ${model}${variant ? ` ${variant}` : ""} have sunroof?`,
    intent: "vehicle_feature_answer",
    inlineType: "feature_answer_card",
  },
  {
    id: "features-airbags",
    label: "How many airbags?",
    title: "How many airbags?",
    query: `How many airbags does ${model}${variant ? ` ${variant}` : ""} have?`,
    intent: "vehicle_feature_answer",
    inlineType: "feature_answer_card",
  },
  {
    id: "features-safety",
    label: "Show safety features",
    title: "Show safety features",
    query: `Show safety features of ${model}${variant ? ` ${variant}` : ""}`,
    intent: INTENT,
    canvasType: CANVAS_TYPE,
  },
];

const buildUnavailableResponse = ({ make, model, variant, queryUsed } = {}) => {
  const displayName = [make, model].filter(Boolean).join(" ") || model || "this model";
  const vehicle = buildVehicle({ make, model, variant, features: [] });

  const widget = {
    type: TOOL_NAME,
    tool: TOOL_NAME,
    intent: INTENT,
    canvasType: CANVAS_TYPE,
    title: `${displayName} features`,
    subtitle: "No feature rows found in vehicle_features.",
    model,
    make,
    brand: make,
    selectedVariant: variant,
    vehicle,
    variants: [],
    variantOptions: [],
    features: [],
    featureList: [],
    rows: [],
    items: [],
    categoryStats: buildCategoryStats([]),
    quickSearches: [],
    highlights: [],
    isUnavailable: true,
    emptyReason: "No stored features found for this exact model/variant.",
  };

  return {
    tool: TOOL_NAME,
    intent: INTENT,
    canvasType: CANVAS_TYPE,
    title: `${displayName} features`,
    answer: `I could not find stored feature data for ${displayName}${variant ? ` ${variant}` : ""}.`,
    vehicle,
    widget,
    widgets: [widget],
    rows: [],
    features: [],
    variants: [],
    actions: [],
    leadingQuestions: [],
    contextPatch: {
      selectedVehicle: vehicle,
      anchorMake: make || "",
      anchorModel: model || "",
      anchorVariant: variant || "",
      anchorCity: DEFAULT_CITY,
    },
    sourceTransparency: {
      modulesChecked: [COLLECTION_NAME],
      recordCount: 0,
    },
    meta: {
      collection: COLLECTION_NAME,
      queryUsed,
    },
  };
};

export const runVehicleFeaturesTool = async (args = {}) => {
  const { toolPlan = {}, context = {}, userMessage = "" } = args;

  const requestedModel = getRequestedModel({ toolPlan, context, userMessage });
  const requestedMake = getRequestedMake({ toolPlan, context });
  const requestedVariant = getRequestedVariant({ toolPlan, context, userMessage });
  const requestedCity =
    firstText(
      getEntities(toolPlan).city,
      toolPlan.city,
      context.anchorCity,
      context.selectedVehicle?.citySlug,
      context.selectedVehicle?.city,
    ) || DEFAULT_CITY;

  if (!requestedModel) {
    return buildUnavailableResponse({
      make: requestedMake,
      model: "",
      variant: requestedVariant,
      queryUsed: null,
    });
  }

  const collection = mongoose.connection.db.collection(COLLECTION_NAME);

  const queries = [
    {
      query: buildMakeModelQuery({
        make: requestedMake,
        model: requestedModel,
      }),
      make: requestedMake,
    },
    requestedMake
      ? {
          query: buildMakeModelQuery({
            make: "",
            model: requestedModel,
          }),
          make: "",
        }
      : null,
  ].filter(Boolean);

  let docs = [];
  let queryUsed = null;
  let queryMakeUsed = requestedMake;

  for (const { query, make } of queries) {
    const found = await collection
      .find(query)
      .sort({ brand: 1, model: 1, variant: 1, updatedAt: -1 })
      .limit(Number(toolPlan.limit || toolPlan.input?.limit || 120))
      .toArray();

    docs = found.filter((row) =>
      rowModelMatchesRequest({
        row,
        requestedModel,
        requestedMake: make,
      }),
    );

    if (docs.length) {
      queryUsed = query;
      queryMakeUsed = make;
      break;
    }
  }

  if (!docs.length) {
    return buildUnavailableResponse({
      make: requestedMake,
      model: requestedModel,
      variant: requestedVariant,
      queryUsed,
    });
  }

  const dedupe = new Map();
  docs.forEach((doc) => {
    const key = normalizeKey(`${doc.brand || doc.make || ""} ${doc.model || ""} ${doc.variant || ""}`);
    if (!key) return;

    const existing = dedupe.get(key);
    if (!existing) {
      dedupe.set(key, doc);
      return;
    }

    const existingCount = Object.keys(existing.features || {}).length;
    const nextCount = Object.keys(doc.features || {}).length;

    if (nextCount >= existingCount) dedupe.set(key, doc);
  });

  const variantDocs = [...dedupe.values()].sort((a, b) =>
    firstText(a.variant).localeCompare(firstText(b.variant), undefined, {
      numeric: true,
      sensitivity: "base",
    }),
  );

  const exactVariantDocs = requestedVariant
    ? variantDocs.filter((doc) => variantMatches(doc, requestedVariant))
    : [];

  const selectedDoc =
    exactVariantDocs[0] ||
    variantDocs.find((doc) => Object.keys(doc.features || {}).length) ||
    variantDocs[0];

  const make = titleCaseWords(firstText(selectedDoc.brand, selectedDoc.make, requestedMake, queryMakeUsed));
  const model = titleCaseWords(firstText(selectedDoc.model, requestedModel));
  const selectedVariant = titleCaseWords(firstText(selectedDoc.variant, requestedVariant));

  const selectedFeatures = objectToFeaturesArray(selectedDoc.features || {}, selectedDoc);
  const variants = variantDocs.map((doc) =>
    normalizeVariantRow(
      doc,
      String(doc._id || "") === String(selectedDoc._id || ""),
    ),
  );

  const categoryStats = buildCategoryStats(selectedFeatures);
  const quickSearches = buildQuickSearches(selectedFeatures);
  const highlights = buildHighlights(selectedFeatures);
  const vehicle = buildVehicle({
    make,
    model,
    variant: selectedVariant,
    city: requestedCity,
    features: selectedFeatures,
  });

  const displayName = [make, model].filter(Boolean).join(" ") || model;
  const title = `${displayName}${selectedVariant ? ` ${selectedVariant}` : ""} features`;

  const widget = {
    type: TOOL_NAME,
    tool: TOOL_NAME,
    intent: INTENT,
    canvasType: CANVAS_TYPE,

    title,
    heading: title,
    subtitle: `${selectedFeatures.length} feature records found${variants.length ? ` across ${variants.length} variants` : ""}.`,
    answer: `I found ${selectedFeatures.length} features for ${displayName}${selectedVariant ? ` ${selectedVariant}` : ""}.`,

    make,
    brand: make,
    model,
    selectedVariant,
    variant: selectedVariant,
    vehicle,

    variants,
    variantOptions: variants,
    features: selectedFeatures,
    featureList: selectedFeatures,
    rows: selectedFeatures.slice(0, 12),
    items: selectedFeatures.slice(0, 12),

    categoryStats,
    featureStats: categoryStats,
    quickSearches,
    popularSearches: quickSearches,
    highlights,
    whyThisVariant: highlights,

    selectedFeatureDocId: String(selectedDoc._id || ""),
    totalVariantCount: variants.length,
    totalFeatureCount: selectedFeatures.length,
  };

  const actions = buildActions({
    vehicle,
    model,
    variant: selectedVariant,
  });

  const leadingQuestions = buildLeadingQuestions({
    model,
    variant: selectedVariant,
  });

  const baseResponse = {
    tool: TOOL_NAME,
    intent: INTENT,
    displayMode: "canvas",
    canvasType: CANVAS_TYPE,
    title,
    answer: widget.answer,

    vehicle,
    widget,
    widgets: [widget],

    rows: selectedFeatures,
    items: selectedFeatures,
    features: selectedFeatures,
    variants,
    actions,
    leadingQuestions,

    data: {
      vehicle,
      rows: variants,
      features: selectedFeatures,
      variants,
      selectedVariant,
      categoryStats,
      quickSearches,
      highlights,
      city: requestedCity,
    },

    contextPatch: {
      selectedVehicle: vehicle,
      anchorMake: make,
      anchorModel: model,
      anchorVariant: selectedVariant,
      anchorCity: requestedCity,
    },

    sourceTransparency: {
      modulesChecked: [COLLECTION_NAME],
      recordCount: variantDocs.length,
      selectedFeatureCount: selectedFeatures.length,
      responseTool: TOOL_NAME,
      dataSource: "mongodb",
    },

    runtimeResultsMeta: [
      {
        tool: TOOL_NAME,
        index: 0,
        matched: variantDocs.length,
        source: COLLECTION_NAME,
        modulesChecked: [COLLECTION_NAME],
        error: "",
      },
    ],

    meta: {
      collection: COLLECTION_NAME,
      queryUsed,
      requestedMake,
      requestedModel,
      requestedVariant,
      matchedVariants: variantDocs.length,
    },
  };

  const enhancedPayload = await buildFeatureExplorerPayload({
    response: baseResponse,
    widget,
  });

  if (!enhancedPayload) return baseResponse;

  const enhancedWidget = {
    ...widget,
    ...enhancedPayload,
    rows: enhancedPayload.rows || [],
    items: enhancedPayload.items || [],
    features: enhancedPayload.features || [],
    featureList: enhancedPayload.featureList || enhancedPayload.features || [],
  };

  return {
    ...baseResponse,
    intent: enhancedPayload.intent || INTENT,
    displayMode: "canvas",
    canvasType: enhancedPayload.canvasType || CANVAS_TYPE,
    title: enhancedPayload.title || title,
    answer: enhancedPayload.answer || widget.answer,
    vehicle: enhancedPayload.vehicle || vehicle,

    widget: enhancedWidget,
    widgets: [enhancedWidget],

    rows: enhancedPayload.rows || [],
    items: enhancedPayload.items || [],
    features: enhancedPayload.features || [],
    variants: enhancedPayload.variants || variants,

    data: {
      ...(baseResponse.data || {}),
      ...(enhancedPayload.data || {}),
      variants: enhancedPayload.variants || [],
      variantOptions: enhancedPayload.variantOptions || enhancedPayload.variants || [],
      selectedVariant: enhancedPayload.selectedVariant || selectedVariant,
      selectedVariantId: enhancedPayload.selectedVariantId || "",
      featureGroups: enhancedPayload.featureGroups || [],
      features: enhancedPayload.features || [],
      featureList: enhancedPayload.featureList || enhancedPayload.features || [],
      quickSpecs: enhancedPayload.quickSpecs || [],
      highlights: enhancedPayload.highlights || [],
      categoryStats: enhancedPayload.categoryStats || {},
      featureStats: enhancedPayload.featureStats || enhancedPayload.categoryStats || {},
      activeStatusSource: enhancedPayload.activeStatusSource || "feature_rows",
      activeVariantCount: enhancedPayload.activeVariantCount || 0,
      totalRawVariantCount: enhancedPayload.totalRawVariantCount || variants.length,
      selectedVariantIsActive: enhancedPayload.selectedVariantIsActive ?? null,
      currentPricelistMatched: enhancedPayload.currentPricelistMatched ?? false,
    },

    contextPatch: {
      ...(baseResponse.contextPatch || {}),
      selectedVehicle: enhancedPayload.vehicle || vehicle,
      anchorMake: enhancedPayload.vehicle?.make || make,
      anchorModel: enhancedPayload.vehicle?.model || model,
      anchorVariant: enhancedPayload.selectedVariant || selectedVariant,
      anchorCity: enhancedPayload.vehicle?.citySlug || enhancedPayload.vehicle?.city || requestedCity,
    },

    sourceTransparency: {
      ...(baseResponse.sourceTransparency || {}),
      responseTool: TOOL_NAME,
      modulesChecked:
        enhancedPayload.activeStatusSource === "vehicles" ||
        enhancedPayload.currentPricelistMatched === true
          ? [COLLECTION_NAME, "vehicles"]
          : [COLLECTION_NAME],
      recordCount: variantDocs.length,
      selectedFeatureCount: enhancedPayload.totalFeatureCount || selectedFeatures.length,
      dataSource: "mongodb",
    },

    runtimeResultsMeta: [
      {
        tool: TOOL_NAME,
        index: 0,
        matched: variantDocs.length,
        source: COLLECTION_NAME,
        modulesChecked:
          enhancedPayload.activeStatusSource === "vehicles" ||
          enhancedPayload.currentPricelistMatched === true
            ? [COLLECTION_NAME, "vehicles"]
            : [COLLECTION_NAME],
        error: "",
      },
    ],
  };
};

export default runVehicleFeaturesTool;
