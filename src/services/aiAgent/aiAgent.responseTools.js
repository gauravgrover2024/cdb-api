import { normalizeSearchKey } from "./aiAgent.planSchema.js";
import {
  buildAciLanguageSeed,
  renderAciTemplate,
} from "../aciCore/language/aciAnswerLanguageComposer.js";

/**
 * ACI Assist Response Tools
 *
 * Purpose:
 * - Planner decides WHAT should happen.
 * - Response tools decide HOW the frontend response should look.
 * - Each planner tool maps to one deterministic response builder.
 * - No DB querying happens here.
 * - No old toolRegistry / old aiAgent.tools dependency.
 *
 * Flow:
 * plannerPlan -> executor fetches data -> responseTools builds frontend contract
 *
 * Frontend contract:
 * {
 *   intent,
 *   displayMode,
 *   canvasType,
 *   inlineType,
 *   title,
 *   answer,
 *   data,
 *   actions,
 *   leadingQuestions,
 *   conversationSuggestions,
 *   contextPatch,
 *   sourceTransparency,
 *   secondaryResponses,
 *   meta
 * }
 */

/* -------------------------------------------------------------------------- */
/*  Constants                                                                 */
/* -------------------------------------------------------------------------- */

export const ACI_RESPONSE_TOOL_IDS = [
  "vehicle_pricelist",
  "vehicle_colors",
  "vehicle_feature_lookup",
  "vehicle_spec_attribute_lookup",
  "vehicle_compare",
  "vehicle_recommend",
  "vehicle_similar",
  "vehicle_price_breakup",
  "vehicle_emi",
  "vehicle_finance_knowledge",
  "vehicle_price_history",
  "vehicle_explainer",
  "aci_lead_capture",
  "used_car_passthrough",
  "internal_passthrough",
  "clarification",
  "unavailable",
  "general_response",
];

export const DISPLAY_MODES = ["inline", "canvas", "both"];

export const DEFAULT_CITY = "new-delhi";

export const RESPONSE_INTENTS = {
  vehicle_pricelist: "vehicle_pricelist",
  vehicle_colors: "vehicle_colors",
  vehicle_feature_lookup: "vehicle_feature_answer",
  vehicle_spec_attribute_lookup: "vehicle_spec_attribute_answer",
  vehicle_compare: "vehicle_comparison",
  vehicle_recommend: "vehicle_recommendation",
  vehicle_similar: "vehicle_similar",
  vehicle_price_breakup: "vehicle_price_breakup",
  vehicle_emi: "vehicle_emi_calculator",
  vehicle_finance_knowledge: "new_car_finance_faq",
  vehicle_price_history: "vehicle_price_history",
  vehicle_explainer: "vehicle_explainer",
  aci_lead_capture: "aci_new_car_quotation",
  used_car_passthrough: "used_car_passthrough",
  internal_passthrough: "internal_passthrough",
  clarification: "clarification",
  unavailable: "unavailable",
  general_response: "general_response",
};

export const RESPONSE_CANVAS_TYPES = {
  vehicle_pricelist: "pricelist_canvas",
  vehicle_colors: "color_studio_canvas",
  vehicle_feature_lookup: "",
  vehicle_spec_attribute_lookup: "",
  vehicle_compare: "comparison_canvas",
  vehicle_recommend: "recommendation_results_canvas",
  vehicle_similar: "similar_cars_canvas",
  vehicle_price_breakup: "price_breakup_canvas",
  vehicle_emi: "emi_calculator_canvas",
  vehicle_finance_knowledge: "",
  vehicle_price_history: "price_history_canvas",
  vehicle_explainer: "explainer_canvas",
  aci_lead_capture: "aci_quotation_canvas",
  used_car_passthrough: "text_notice_canvas",
  internal_passthrough: "",
  clarification: "",
  unavailable: "unavailable_notice_canvas",
  general_response: "text_notice_canvas",
};

export const RESPONSE_INLINE_TYPES = {
  vehicle_pricelist: "",
  vehicle_colors: "",
  vehicle_feature_lookup: "feature_answer_card",
  vehicle_spec_attribute_lookup: "spec_attribute_answer_card",
  vehicle_compare: "",
  vehicle_recommend: "",
  vehicle_similar: "similar_cars_summary",
  vehicle_price_breakup: "",
  vehicle_emi: "",
  vehicle_finance_knowledge: "finance_faq_card",
  vehicle_price_history: "",
  vehicle_explainer: "explainer_card",
  aci_lead_capture: "",
  used_car_passthrough: "text_notice",
  internal_passthrough: "",
  clarification: "clarification_card",
  unavailable: "unavailable_notice",
  general_response: "text_notice",
};

/* -------------------------------------------------------------------------- */
/*  Generic Helpers                                                           */
/* -------------------------------------------------------------------------- */

export const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

export const searchKey = (value = "") => normalizeSearchKey(value || "");

export const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

export const firstMeaningful = (...values) =>
  values.find(
    (value) =>
      value !== undefined &&
      value !== null &&
      String(value).trim() !== "",
  ) || "";

export const uniqueBy = (items = [], getKey = (item) => item) => {
  const seen = new Set();
  const output = [];

  for (const item of asArray(items)) {
    const key = searchKey(getKey(item));
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(item);
  }

  return output;
};

export const displayName = (value = "") => {
  const text = cleanText(value);
  if (!text) return "";

  return text
    .split(" ")
    .map((part) => {
      if (!part) return "";
      if (/[A-Z0-9()]/.test(part)) return part;
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join(" ");
};

export const formatCity = (city = DEFAULT_CITY) => {
  const text = cleanText(city || DEFAULT_CITY);
  if (text === "new-delhi") return "Delhi";
  return displayName(text.replace(/-/g, " "));
};

export const formatMoney = (value) => {
  const number = Number(value || 0);

  if (!Number.isFinite(number) || number <= 0) return "";

  if (number >= 10000000) {
    return `₹${(number / 10000000).toFixed(number % 10000000 === 0 ? 0 : 2)}Cr`;
  }

  if (number >= 100000) {
    return `₹${(number / 100000).toFixed(number % 100000 === 0 ? 0 : 2)}L`;
  }

  return `₹${Math.round(number).toLocaleString("en-IN")}`;
};

export const getPlannerTool = (plan = {}, index = 0) =>
  asArray(plan.tools)[index] || {};

export const getToolEntities = (toolPlan = {}) => toolPlan.entities || {};
export const getToolFilters = (toolPlan = {}) => toolPlan.filters || {};

export const getModel = (toolPlan = {}, context = {}) =>
  displayName(
    firstMeaningful(
      toolPlan.entities?.model,
      toolPlan.entities?.primaryModel,
      toolPlan.entities?.fullModel,
      toolPlan.filters?.model,
      toolPlan.filters?.fullModel,
      toolPlan.input?.model,
      toolPlan.input?.fullModel,
      context?.selectedVehicle?.fullModel,
      context?.selectedVehicle?.model,
      context?.anchorModel,
      context?.model,
    ),
  );

export const getVariant = (toolPlan = {}, context = {}) =>
  displayName(
    firstMeaningful(
      toolPlan.entities?.variant,
      toolPlan.entities?.primaryVariant,
      toolPlan.filters?.variant,
      context?.selectedVehicle?.variant,
      context?.anchorVariant,
      context?.variant,
    ),
  );

export const getModels = (toolPlan = {}, context = {}) => {
  const entities = getToolEntities(toolPlan);
  const filters = getToolFilters(toolPlan);

  return uniqueBy(
    [
      ...asArray(entities.models),
      ...asArray(entities.comparisonModels),
      ...asArray(filters.models),
      entities.model,
      filters.model,
      context?.selectedVehicle?.model,
      context?.anchorModel,
    ]
      .filter(Boolean)
      .map(displayName),
  );
};

export const getCity = (toolPlan = {}, context = {}) =>
  firstMeaningful(
    toolPlan.filters?.city,
    toolPlan.entities?.city,
    context?.selectedVehicle?.city,
    context?.anchorCity,
    DEFAULT_CITY,
  );

export const getFeature = (toolPlan = {}) =>
  cleanText(
    firstMeaningful(
      toolPlan.entities?.feature,
      asArray(toolPlan.entities?.features)[0],
      asArray(toolPlan.filters?.mustHaveFeatures)[0],
      asArray(toolPlan.filters?.compareFeatures)[0],
    ),
  );

export const getRuntimeRows = (runtimeData = {}) =>
  asArray(
    runtimeData.rows ||
      runtimeData.records ||
      runtimeData.items ||
      runtimeData.variants ||
      runtimeData.cars ||
      runtimeData.results,
  );

const responseToolDecisionLanguageText = (templateKey = "", input = {}) => {
  if (templateKey === "decision_score_module_summary_note") {
    return "Use this as directional module-score diagnostics, not as a final purchase verdict.";
  }
  if (templateKey === "decision_similar_graph_guardrail_reason") {
    return "Similar cars graph v1 is a deterministic discovery aid, not a purchase verdict.";
  }
  if (templateKey === "decision_similar_graph_note") {
    return "This is a deterministic alternatives graph, not a purchase verdict.";
  }
  return "";
};

const priceLabelFromRow = (row = {}, labelKeys = [], valueKeys = []) => {
  for (const key of labelKeys) {
    const label = cleanText(row?.[key]);
    if (label) return label;
  }

  for (const key of valueKeys) {
    const label = formatMoney(row?.[key]);
    if (label) return label;
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

const getRowVariantLabel = (row = {}) =>
  cleanText(
    firstMeaningful(
      row.variant,
      row.variantName,
      row.fullVariant,
      row.fullVariantName,
      row.displayVariant,
    ),
  );

const getExShowroomLabel = (row = {}) =>
  priceLabelFromRow(
    row,
    ["exShowroomPriceLabel", "exShowroomLabel"],
    ["exShowroomPrice", "ex_showroom_price"],
  );

const getOnRoadLabel = (row = {}) =>
  priceLabelFromRow(
    row,
    [
      "onRoadPriceWithoutOptionalLabel",
      "onRoadPriceLabel",
      "onRoadLabel",
      "priceLabel",
    ],
    [
      "onRoadPriceWithoutOptional",
      "onRoadPrice",
      "on_road_price",
      "price",
    ],
  );

const COLOR_FAMILY_DEFS = [
  { key: "dual tone", label: "dual tone", aliases: ["dual tone", "dual-tone", "dualtone", "dual"] },
  { key: "black", label: "black", aliases: ["black"] },
  { key: "white", label: "white", aliases: ["white"] },
  { key: "red", label: "red", aliases: ["red"] },
  { key: "blue", label: "blue", aliases: ["blue"] },
  { key: "grey", label: "grey", aliases: ["grey", "gray"] },
  { key: "silver", label: "silver", aliases: ["silver"] },
  { key: "green", label: "green", aliases: ["green"] },
  { key: "brown", label: "brown", aliases: ["brown"] },
  { key: "orange", label: "orange", aliases: ["orange"] },
  { key: "yellow", label: "yellow", aliases: ["yellow"] },
  { key: "gold", label: "gold", aliases: ["gold", "golden"] },
];

const normalizedColorText = (value = "") =>
  searchKey(value).replace(/\bgray\b/g, "grey");

const colorNameFromRow = (row = {}) =>
  cleanText(
    firstMeaningful(
      row.colorName,
      row.color_name,
      row.color,
      row.colour,
      row.name,
      row.displayName,
      row.title,
    ),
  );

const detectRequestedColorFamily = ({ userMessage = "", toolPlan = {}, context = {} } = {}) => {
  const text = normalizedColorText(
    [
      userMessage,
      toolPlan.entities?.color,
      toolPlan.filters?.color,
      context?.selectedColor?.colorName,
      context?.selectedColor?.name,
      context?.anchorColor,
    ]
      .filter(Boolean)
      .join(" "),
  );

  if (!text) return null;

  return COLOR_FAMILY_DEFS.find((family) =>
    family.aliases.some((alias) =>
      new RegExp(`(^|\\s)${searchKey(alias).replace(/\s+/g, "\\s+")}(\\s|$)`, "i").test(text),
    ),
  ) || null;
};

const colorMatchesFamily = (row = {}, family = null) => {
  if (!family) return false;
  const name = normalizedColorText(colorNameFromRow(row));
  if (!name) return false;

  if (family.key === "dual tone") {
    return /\bdual\s*tone\b|\bdual\b|\btwo\s*tone\b|\bdt\b/.test(name);
  }

  return family.aliases.some((alias) =>
    new RegExp(`(^|\\s)${normalizedColorText(alias).replace(/\s+/g, "\\s+")}(\\s|$)`, "i").test(name),
  );
};

const buildRequestedColorAnswer = ({
  colors = [],
  requestedFamily = null,
  vehicleLabel = "",
  fallbackAnswer = "",
} = {}) => {
  if (!requestedFamily) return cleanText(fallbackAnswer);

  const colorRows = asArray(colors);
  if (!colorRows.length) return cleanText(fallbackAnswer);

  const matchingNames = uniqueBy(
    colorRows
      .filter((row) => colorMatchesFamily(row, requestedFamily))
      .map(colorNameFromRow)
      .filter(Boolean),
    (name) => name,
  );

  const availableNames = uniqueBy(
    colorRows.map(colorNameFromRow).filter(Boolean),
    (name) => name,
  ).slice(0, 6);
  const label = requestedFamily.label;
  const carLabel = cleanText(vehicleLabel) || "this model";

  if (matchingNames.length) {
    return `Yes, ${carLabel} has ${label} shade(s): ${matchingNames.join(", ")}.`;
  }

  if (availableNames.length) {
    return `I could not find an exact ${label} shade for ${carLabel} in the current color data. Available colors include ${availableNames.join(", ")}.`;
  }

  return cleanText(fallbackAnswer);
};

const getPriceRange = (rows = [], keys = [], labelFor = () => "") => {
  const pricedRows = asArray(rows)
    .map((row) => ({
      row,
      value: numericPriceFromRow(row, keys),
      label: labelFor(row),
    }))
    .filter((item) => item.value > 0 && item.label);

  if (!pricedRows.length) return null;

  const sorted = [...pricedRows].sort((left, right) => left.value - right.value);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];

  return {
    minLabel: min.label,
    maxLabel: max.label,
    minVariant: getRowVariantLabel(min.row),
    maxVariant: getRowVariantLabel(max.row),
    isSingle: min.value === max.value,
  };
};

const describePriceRange = (label = "", range = null) => {
  if (!range) return "";
  if (range.isSingle) {
    return `${label} price is ${range.minLabel}`;
  }
  const entry = range.minVariant ? ` from ${range.minVariant} at ${range.minLabel}` : ` from ${range.minLabel}`;
  const top = range.maxVariant ? ` to ${range.maxVariant} at ${range.maxLabel}` : ` to ${range.maxLabel}`;
  return `${label} prices range${entry}${top}`;
};

const normalizePriceAnswerGrammar = (answer = "") =>
  cleanText(answer)
    .replace(/\. the on-road/g, ". The on-road")
    .replace(/\. the ex-showroom/g, ". The ex-showroom")
    .replace(/\bprice rows\b/gi, "prices");

const buildPricelistBuyerAnswer = ({
  rows = [],
  model = "",
  variant = "",
  city = "",
  query = "",
  fallbackAnswer = "",
} = {}) => {
  const cleanRows = asArray(rows);
  if (!cleanRows.length) return normalizePriceAnswerGrammar(fallbackAnswer);

  const cityLabel = formatCity(city);
  const firstRow = cleanRows[0] || {};
  const rowModel = cleanText(
    firstMeaningful(
      firstRow.fullModel,
      firstRow.modelLabel,
      firstRow.displayName,
      firstRow.model,
      model,
    ),
  );
  const rowVariant = getRowVariantLabel(firstRow);
  const requestedVariant = cleanText(variant || rowVariant);
  const vehicleLabel = cleanText(`${rowModel || model}${requestedVariant && cleanRows.length === 1 ? ` ${requestedVariant}` : ""}`) || "this model";
  const normalizedQuery = searchKey(query);
  const wantsOnRoad = /\bon\s*road\b|\bonroad\b/.test(normalizedQuery);
  const wantsExShowroom = /\bex\s*showroom\b|\bexshowroom\b/.test(normalizedQuery);

  if (cleanRows.length === 1) {
    const exShowroom = getExShowroomLabel(firstRow);
    const onRoad = getOnRoadLabel(firstRow);
    const parts = [];
    if (exShowroom) parts.push(`ex-showroom price is ${exShowroom}`);
    if (onRoad) parts.push(`on-road price is ${onRoad}`);

    if (parts.length) {
      return normalizePriceAnswerGrammar(
        `For ${vehicleLabel} in ${cityLabel}, the ${parts.join(", and the ")}.`,
      );
    }
  }

  const exShowroomRange = getPriceRange(
    cleanRows,
    ["exShowroomPrice", "ex_showroom_price"],
    getExShowroomLabel,
  );
  const onRoadRange = getPriceRange(
    cleanRows,
    ["onRoadPriceWithoutOptional", "onRoadPrice", "on_road_price", "price"],
    getOnRoadLabel,
  );

  const evidence = [];
  if (wantsOnRoad) {
    const onRoadText = describePriceRange("On-road", onRoadRange);
    if (onRoadText) evidence.push(onRoadText);
    const exText = describePriceRange("Ex-showroom", exShowroomRange);
    if (exText) evidence.push(exText);
  } else if (wantsExShowroom) {
    const exText = describePriceRange("Ex-showroom", exShowroomRange);
    if (exText) evidence.push(exText);
    const onRoadText = describePriceRange("On-road", onRoadRange);
    if (onRoadText) evidence.push(onRoadText);
  } else {
    const exText = describePriceRange("Ex-showroom", exShowroomRange);
    if (exText) evidence.push(exText);
    const onRoadText = describePriceRange("On-road", onRoadRange);
    if (onRoadText) evidence.push(onRoadText);
  }

  if (evidence.length) {
    return normalizePriceAnswerGrammar(
      `I found ${rowModel || model || "this model"} prices in ${cityLabel} across ${cleanRows.length} variants. ${evidence.join(". ")}.`,
    );
  }

  return normalizePriceAnswerGrammar(fallbackAnswer);
};

export const getRuntimeDataForTool = ({
  runtimeResults = {},
  toolPlan = {},
  index = 0,
} = {}) => {
  if (Array.isArray(runtimeResults)) return runtimeResults[index] || {};

  const tool = toolPlan.tool;

  return (
    runtimeResults?.[toolPlan.id] ||
    runtimeResults?.[tool] ||
    runtimeResults?.[index] ||
    runtimeResults ||
    {}
  );
};

const renderResponseLanguage = (templateKey = "", input = {}, seedParts = []) =>
  renderAciTemplate(templateKey, input, {
    seed: buildAciLanguageSeed(templateKey, ...asArray(seedParts)),
  }).text;

export const makeAction = ({
  id,
  label,
  type = "ask",
  query = "",
  intent = "",
  canvasType = "",
  inlineType = "",
  leadType = "",
  entities = {},
  filters = {},
  contextPatch = {},
  priority = 50,
  icon = "",
  tone = "neutral",
} = {}) => ({
  id: id || searchKey(`${label} ${query}`).replace(/\s+/g, "-"),
  label: cleanText(label).slice(0, 80),
  type,
  query: cleanText(query).slice(0, 220),
  intent,
  canvasType,
  inlineType,
  leadType,
  entities,
  filters,
  contextPatch,
  priority,
  icon,
  tone,
});

export const normalizeActions = (actions = [], { max = 6 } = {}) =>
  uniqueBy(
    asArray(actions)
      .filter((action) => action && action.label && action.query)
      .map((action) => ({
        ...action,
        id: action.id || searchKey(`${action.label} ${action.query}`).replace(/\s+/g, "-"),
      })),
    (action) => `${action.label} ${action.query} ${action.intent}`,
  ).slice(0, max);

export const makeContextPatch = (toolPlan = {}, context = {}) => {
  // Internal CDrive answers must not overwrite selected car context.
  if (toolPlan.tool === "internal_passthrough") {
    return {
      customerStage: "",
      conversationMode: "",
    };
  }

  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const color = cleanText(firstMeaningful(toolPlan.entities?.color, toolPlan.filters?.color));

  const selectedVehicle = {};

  if (model) selectedVehicle.model = model;
  if (variant) selectedVehicle.variant = variant;
  if (city) selectedVehicle.city = city;
  if (color) selectedVehicle.color = color;

  return {
    ...(model ? { anchorModel: model } : {}),
    ...(variant ? { anchorVariant: variant } : {}),
    ...(city ? { anchorCity: city } : {}),
    ...(color ? { anchorColor: color } : {}),
    ...(Object.keys(selectedVehicle).length ? { selectedVehicle } : {}),
    customerStage: "",
    conversationMode: "",
  };
};

export const makeSourceTransparency = ({
  toolPlan = {},
  runtimeData = {},
  modulesChecked = [],
} = {}) => ({
  responseTool: toolPlan.tool || "",
  modulesChecked:
    modulesChecked.length > 0
      ? modulesChecked
      : asArray(runtimeData.modulesChecked || runtimeData.sourceTransparency?.modulesChecked),
  matched:
    runtimeData.matched ??
    runtimeData.count ??
    getRuntimeRows(runtimeData).length ??
    0,
  dataSource: runtimeData.dataSource || runtimeData.source || "aci_backend",
  generatedBy: "aci_response_tools_v1",
});

export const baseResponse = ({
  toolPlan = {},
  context = {},
  runtimeData = {},
  intent = "",
  displayMode = "canvas",
  canvasType = "",
  inlineType = "",
  title = "",
  answer = "",
  data = {},
  actions = [],
  leadingQuestions = [],
  conversationSuggestions = [],
  sourceTransparency = {},
  contextPatch = null,
  meta = {},
} = {}) => {
  const tool = toolPlan.tool || "general_response";

  return {
    intent: intent || RESPONSE_INTENTS[tool] || tool,
    displayMode,
    canvasType: canvasType ?? RESPONSE_CANVAS_TYPES[tool] ?? "",
    inlineType: inlineType ?? RESPONSE_INLINE_TYPES[tool] ?? "",
    title: cleanText(title),
    answer: cleanText(answer),
    data: data || {},
    actions: normalizeActions(actions),
    leadingQuestions: normalizeActions(leadingQuestions),
    conversationSuggestions: normalizeActions(conversationSuggestions),
    contextPatch: contextPatch || makeContextPatch(toolPlan, context),
    sourceTransparency: {
      ...makeSourceTransparency({ toolPlan, runtimeData }),
      ...sourceTransparency,
    },
    meta: {
      responseTool: tool,
      ...meta,
    },
  };
};

/* -------------------------------------------------------------------------- */
/*  Shared Action Builders                                                    */
/* -------------------------------------------------------------------------- */

export const modelActions = ({ model = "", variant = "", city = DEFAULT_CITY } = {}) => {
  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "this car";

  return [
    makeAction({
      id: "show-colors",
      label: "Show colors",
      query: `Show colors of ${model || carLabel}`,
      intent: "vehicle_colors",
      canvasType: "color_studio_canvas",
      entities: { model },
      filters: { city },
      priority: 92,
    }),
    makeAction({
      id: "calculate-emi",
      label: "Calculate EMI",
      query: `Calculate EMI for ${carLabel}`,
      intent: "vehicle_emi_calculator",
      canvasType: "emi_calculator_canvas",
      entities: { model, variant },
      filters: { city },
      priority: 88,
    }),
    makeAction({
      id: "get-quotation",
      label: "Get quotation",
      type: "lead",
      query: `Get quotation for ${carLabel}`,
      intent: "aci_new_car_quotation",
      canvasType: "aci_quotation_canvas",
      leadType: "quotation",
      entities: { model, variant },
      filters: { city },
      priority: 84,
    }),
  ];
};

export const priceActions = ({ model = "", variant = "", city = DEFAULT_CITY } = {}) => {
  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "this car";

  return [
    makeAction({
      id: "show-price-breakup",
      label: "Show price breakup",
      query: `Show price breakup of ${carLabel}`,
      intent: "vehicle_price_breakup",
      canvasType: "price_breakup_canvas",
      entities: { model, variant },
      filters: { city, priceBasis: "on_road" },
      priority: 96,
    }),
    makeAction({
      id: "compare-variants",
      label: "Compare variants",
      query: `Compare ${model || carLabel} variants`,
      intent: "vehicle_variant_comparison",
      canvasType: "comparison_canvas",
      entities: { model },
      filters: { city },
      priority: 90,
    }),
    ...modelActions({ model, variant, city }).filter(
      (action) => action.id !== "show-colors",
    ),
  ];
};

export const recommendationActions = ({ city = DEFAULT_CITY } = {}) => [
  makeAction({
    id: "compare-top-matches",
    label: "Compare top matches",
    query: "Compare top matching cars",
    intent: "vehicle_comparison",
    canvasType: "comparison_canvas",
    filters: { city },
    priority: 96,
  }),
  makeAction({
    id: "show-lowest-emi",
    label: "Lowest EMI option",
    query: "Show lowest EMI option from these matches",
    intent: "vehicle_emi_calculator",
    canvasType: "emi_calculator_canvas",
    filters: { city },
    priority: 90,
  }),
  makeAction({
    id: "show-safest-option",
    label: "Safest option",
    query: "Show safest option from these matches",
    intent: "vehicle_safety_search",
    canvasType: "safety_advisor_canvas",
    filters: { city },
    priority: 86,
  }),
  makeAction({
    id: "get-quote-for-match",
    label: "Get quote",
    type: "lead",
    query: "Get quotation for my preferred match",
    intent: "aci_new_car_quotation",
    canvasType: "aci_quotation_canvas",
    leadType: "quotation",
    filters: { city },
    priority: 80,
  }),
];

/* -------------------------------------------------------------------------- */
/*  Response Tool Builders                                                    */
/* -------------------------------------------------------------------------- */



export const makeVariantAmbiguityContextPatch = ({
  model = "",
  requestedVariant = "",
  city = DEFAULT_CITY,
} = {}) => ({
  ...(model ? { anchorModel: model } : {}),
  ...(city ? { anchorCity: city } : {}),
  ...(model || city
    ? {
        selectedVehicle: {
          ...(model ? { model } : {}),
          ...(city ? { city } : {}),
        },
      }
    : {}),
  pendingVehicleResolution: {
    type: "variant",
    requestedVariant,
    status: "not_found",
  },
  customerStage: "",
  conversationMode: "",
});

export const buildVariantNotFoundActions = ({
  model = "",
  requestedVariant = "",
  candidateVariants = [],
  city = DEFAULT_CITY,
  targetIntent = "vehicle_pricelist",
} = {}) => {
  const topCandidates = asArray(candidateVariants).slice(0, 5);

  const candidateActions = topCandidates.map((variant, index) =>
    makeAction({
      id: `choose-db-variant-${index + 1}`,
      label: displayName(variant),
      query:
        targetIntent === "vehicle_emi_calculator"
          ? `Calculate EMI for ${cleanText(`${model} ${variant}`)}`
          : `Show price of ${cleanText(`${model} ${variant}`)}`,
      intent: targetIntent,
      canvasType:
        targetIntent === "vehicle_emi_calculator"
          ? "emi_calculator_canvas"
          : "pricelist_canvas",
      entities: { model, variant },
      filters: { city },
      priority: 100 - index,
    }),
  );

  return [
    ...candidateActions,
    makeAction({
      id: "open-model-pricelist",
      label: "Open full price list",
      query: `Show ${model} price list`,
      intent: "vehicle_pricelist",
      canvasType: "pricelist_canvas",
      entities: { model },
      filters: { city },
      priority: 70,
    }),
  ];
};

export const buildVehiclePricelistResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {

  if (
    runtimeData?.canvasType === "unsupported_city_canvas" ||
    runtimeData?.unsupportedCity ||
    runtimeData?.widget?.unsupportedCity
  ) {
    const unsupportedCity = runtimeData.unsupportedCity || runtimeData.widget?.unsupportedCity || null;
    const widget = runtimeData.widget || {
      type: "vehicle_pricelist",
      widgetType: "unsupported_city",
      canvasType: "unsupported_city_canvas",
      title: runtimeData.title || "Pricing unavailable",
      answer: runtimeData.answer || "",
      unsupportedCity,
      rows: [],
      variants: [],
    };

    return {
      intent: runtimeData.intent || "vehicle_pricelist",
      displayMode: "canvas",
      canvasType: "unsupported_city_canvas",
      inlineType: null,
      title: runtimeData.title || widget.title || "Pricing unavailable",
      answer: runtimeData.answer || widget.answer || "",
      data: {
        ...runtimeData,
        widget,
        rows: [],
        records: [],
        variants: [],
        unsupportedCity,
      },
      widget,
      widgets: [widget],
      rows: [],
      records: [],
      variants: [],
      unsupportedCity,
      actions: runtimeData.actions || [],
      leadingQuestions: runtimeData.leadingQuestions || [],
      conversationSuggestions: runtimeData.leadingQuestions || [],
      contextPatch: runtimeData.contextPatch || widget.contextPatch || {},
      sourceTransparency: runtimeData.sourceTransparency || {
        modulesChecked: runtimeData.modulesChecked || [],
        matched: 0,
        dataSource: runtimeData.dataSource || "unsupported_city",
      },
      meta: {
        ...(runtimeData.meta || {}),
        unsupportedCity,
        source: runtimeData.source,
        dataSource: runtimeData.dataSource,
        modulesChecked: runtimeData.modulesChecked || [],
      },
    };
  }

  /* ACI_PRICE_V2_PASSTHROUGH_START */
  if (
    ["pricelist_canvas", "price_breakup_canvas"].includes(runtimeData?.canvasType) &&
    ["pricelist_canvas", "price_breakup_canvas"].includes(runtimeData?.widget?.canvasType) &&
    Array.isArray(runtimeData?.widget?.rows)
  ) {
    const widget = runtimeData.widget;
    const rows = widget.rows || runtimeData.rows || [];
    const canvasType = runtimeData.canvasType || widget.canvasType || "pricelist_canvas";
    const answer = buildPricelistBuyerAnswer({
      rows,
      model:
        widget?.vehicle?.displayName ||
        widget?.vehicle?.fullModel ||
        widget?.vehicle?.model ||
        runtimeData?.vehicle?.displayName ||
        runtimeData?.vehicle?.model ||
        getModel(toolPlan, context),
      variant:
        toolPlan.input?.variant ||
        toolPlan.entities?.variant ||
        widget?.vehicle?.variant ||
        runtimeData?.requested?.variant ||
        getVariant(toolPlan, context),
      city:
        widget.city ||
        runtimeData.city ||
        runtimeData.requested?.city ||
        getCity(toolPlan, context),
      query:
        toolPlan.input?.message ||
        toolPlan.input?.query ||
        toolPlan.query ||
        runtimeData.requested?.query ||
        "",
      fallbackAnswer:
        runtimeData.answer ||
        widget.answer ||
        `Here is the price list for ${widget?.vehicle?.displayName || widget?.vehicle?.model || "this model"}.`,
    });

    return {
      intent: runtimeData.intent || "vehicle_pricelist",
      displayMode: "canvas",
      canvasType,
      inlineType: null,
      title: widget.title || runtimeData.title || "Price list",
      answer,

      data: {
        ...runtimeData,
        widget,
        rows,
        records: rows,
        variants: widget.variants || rows,
      },

      widget,
      widgets: [widget],
      rows,
      records: rows,
      variants: widget.variants || rows,

      actions: widget.actions || runtimeData.actions || [],
      leadingQuestions:
        widget.leadingQuestions ||
        runtimeData.leadingQuestions ||
        [],

      conversationSuggestions:
        widget.leadingQuestions ||
        runtimeData.leadingQuestions ||
        [],

      contextPatch:
        runtimeData.contextPatch ||
        widget.contextPatch ||
        {},

      sourceTransparency:
        runtimeData.sourceTransparency ||
        runtimeData.modulesChecked ||
        [],

      meta: {
        ...(runtimeData.meta || {}),
        source: runtimeData.source,
        dataSource: runtimeData.dataSource,
        modulesChecked: runtimeData.modulesChecked || [],
        v2PricelistPassthrough: true,
      },
    };
  }
  /* ACI_PRICE_V2_PASSTHROUGH_END */

  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const rows = getRuntimeRows(runtimeData);
  const cityLabel = formatCity(city);
  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "selected car";
  const variantResolution = runtimeData.variantResolution || {};

  if (variantResolution.status === "not_found") {
    return baseResponse({
      toolPlan,
      context,
      runtimeData,
      intent: "vehicle_variant_ambiguity",
      displayMode: "inline",
      canvasType: "",
      inlineType: "variant_ambiguity_card",
      title: `Choose exact ${model} variant`,
      answer: `I found ${model}, but I could not match ${variantResolution.requestedVariant || variant} to a current variant I can verify. Choose one of the available variants, or I can continue with model-level price and features.`,
      data: {
        model,
        requestedVariant: variantResolution.requestedVariant || variant,
        city,
        candidateVariants: variantResolution.candidateVariants || runtimeData.candidateVariants || [],
        candidateRows: runtimeData.candidateRows || [],
      },
      contextPatch: makeVariantAmbiguityContextPatch({
        model,
        requestedVariant: variantResolution.requestedVariant || variant,
        city,
      }),
      actions: buildVariantNotFoundActions({
        model,
        requestedVariant: variantResolution.requestedVariant || variant,
        candidateVariants: variantResolution.candidateVariants || runtimeData.candidateVariants || [],
        city,
        targetIntent: "vehicle_pricelist",
      }),
    });
  }

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: variant ? "vehicle_variant_price" : "vehicle_pricelist",
    displayMode: variant ? "both" : "canvas",
    canvasType: "pricelist_canvas",
    inlineType: variant ? "short_price_card" : "",
    title: variant ? `${carLabel} price` : `${model || "Vehicle"} price list`,
    answer: rows.length
      ? buildPricelistBuyerAnswer({
          rows,
          model,
          variant,
          city,
          query:
            toolPlan.input?.message ||
            toolPlan.input?.query ||
            toolPlan.query ||
            "",
          fallbackAnswer: variant
            ? `Here is the ${carLabel} price information for ${cityLabel}.`
            : `Here is the variant-wise price list for ${model || "this model"} in ${cityLabel}.`,
        })
      : `I could not find confirmed prices for ${carLabel} in ${cityLabel}.`,
    data: {
      model,
      variant,
      city,
      cityLabel,
      priceBasis: toolPlan.filters?.priceBasis || "on_road",
      rows,
      summary: runtimeData.summary || {},
    },
    actions: priceActions({ model, variant, city }),
  });
};

export const buildVehicleColorsResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
  userMessage = "",
} = {}) => {
  /* ACI_COLORS_V2_PASSTHROUGH_START */
  if (
    runtimeData?.canvasType === "color_studio_canvas" &&
    runtimeData?.widget?.canvasType === "color_studio_canvas" &&
    Array.isArray(runtimeData?.widget?.colors)
  ) {
    const widget = runtimeData.widget;
    const colors = widget.colors || runtimeData.colors || [];

    const vehicle =
      widget.vehicle ||
      runtimeData.vehicle ||
      runtimeData.contextPatch?.selectedVehicle ||
      {};

    const selectedColor =
      widget.selectedColor ||
      runtimeData.selectedColor ||
      vehicle.selectedColor ||
      colors.find((item) => item?.isSelected || item?.selected) ||
      colors[0] ||
      null;

    const visualGallery =
      widget.visualGallery ||
      runtimeData.visualGallery ||
      vehicle.visualGallery ||
      [];

    const finalVehicle = {
      ...vehicle,
      selectedColor,
      imageUrl:
        selectedColor?.normalizedImageUrl ||
        selectedColor?.imageUrl ||
        vehicle.imageUrl ||
        vehicle.normalizedImageUrl ||
        "",
      normalizedImageUrl:
        selectedColor?.normalizedImageUrl ||
        vehicle.normalizedImageUrl ||
        vehicle.imageUrl ||
        "",
      imageFrame: selectedColor?.imageFrame || vehicle.imageFrame || null,
      visualGallery,
    };

    const finalWidget = {
      ...widget,
      vehicle: finalVehicle,
      colors,
      rows: widget.rows || colors,
      records: widget.records || colors,
      items: widget.items || colors,
      selectedColor,
      visualGallery,
    };
    const requestedFamily = detectRequestedColorFamily({
      userMessage,
      toolPlan,
      context,
    });
    const fallbackAnswer =
      runtimeData.answer ||
      widget.answer ||
      `I found ${colors.length} colors for ${
        finalVehicle.displayName || finalVehicle.model || "this model"
      }.`;
    const answer = buildRequestedColorAnswer({
      colors,
      requestedFamily,
      vehicleLabel: finalVehicle.displayName || finalVehicle.fullModel || finalVehicle.model || "",
      fallbackAnswer,
    });

    return {
      intent: runtimeData.intent || "vehicle_colors",
      displayMode: "canvas",
      canvasType: "color_studio_canvas",
      inlineType: null,

      title: widget.title || runtimeData.title || "Colors",
      answer,

      data: {
        ...runtimeData,
        widget: finalWidget,
        vehicle: finalVehicle,
        colors,
        rows: colors,
        records: colors,
        items: colors,
        selectedColor,
        visualGallery,
      },

      widget: finalWidget,
      widgets: [finalWidget],

      vehicle: finalVehicle,
      colors,
      rows: colors,
      records: colors,
      items: colors,
      selectedColor,
      visualGallery,

      actions: normalizeActions(widget.actions || runtimeData.actions || []),
      leadingQuestions: normalizeActions(
        widget.leadingQuestions || runtimeData.leadingQuestions || [],
      ),
      conversationSuggestions: normalizeActions(
        widget.leadingQuestions || runtimeData.leadingQuestions || [],
      ),

      contextPatch: runtimeData.contextPatch ||
        widget.contextPatch || {
          selectedVehicle: finalVehicle,
          anchorMake: finalVehicle.make || finalVehicle.brand || "",
          anchorModel: finalVehicle.model || "",
          anchorCity:
            finalVehicle.citySlug ||
            finalVehicle.city ||
            context.anchorCity ||
            DEFAULT_CITY,
          selectedColor,
        },

      sourceTransparency: runtimeData.sourceTransparency ||
        runtimeData.modulesChecked || ["vehicle_colors_v2"],

      meta: {
        ...(runtimeData.meta || {}),
        source: runtimeData.source,
        dataSource: runtimeData.dataSource,
        modulesChecked: runtimeData.modulesChecked || ["vehicle_colors_v2"],
        v2ColorsPassthrough: true,
      },
    };
  }
  /* ACI_COLORS_V2_PASSTHROUGH_END */

  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const colors = getRuntimeRows(runtimeData);
  const color = cleanText(
    firstMeaningful(toolPlan.entities?.color, toolPlan.filters?.color),
  );
  const requestedFamily = detectRequestedColorFamily({
    userMessage,
    toolPlan,
    context,
  });
  const fallbackAnswer = colors.length
    ? color
      ? `I found model-level colour information for ${model}. Exact variant-wise ${color} stock availability needs confirmation.`
      : `Here are the available model-level colours for ${model || "this car"}.`
    : `I cannot confirm the colour range for ${model || "this car"} yet.`;

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "vehicle_colors",
    displayMode: "canvas",
    canvasType: "color_studio_canvas",
    title: `${model || "Vehicle"} colors`,
    answer: buildRequestedColorAnswer({
      colors,
      requestedFamily,
      vehicleLabel: model,
      fallbackAnswer,
    }),
    data: {
      model,
      variant,
      city,
      selectedColor: color,
      colors,
      variantWiseAvailability: false,
    },
    actions: [
      makeAction({
        id: "show-price",
        label: "Show price",
        query: `Show price of ${model || "this car"}`,
        intent: "vehicle_pricelist",
        canvasType: "pricelist_canvas",
        entities: { model },
        filters: { city },
        priority: 90,
      }),
      makeAction({
        id: "get-quote-color",
        label: "Get quote",
        type: "lead",
        query: `Get quotation for ${model || "this car"}${color ? ` in ${color}` : ""}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType: "quotation",
        entities: { model, variant, color },
        filters: { city },
        priority: 86,
      }),
    ],
  });
};

export const buildVehicleFeatureLookupResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const rawFeature = getFeature(toolPlan);
  const feature = rawFeature || "features";
  const featureKey = normalizeSearchKey(feature);
  const isFeatureSummaryRequest =
    !rawFeature ||
    ["features", "feature", "feature_summary", "safety_features", "safety"].includes(featureKey);
  const rows = getRuntimeRows(runtimeData);
  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "this car";
  const hasMatch = rows.length > 0 || runtimeData.matched > 0;
  const summaryAnswer = hasMatch
    ? `Here are the confirmed features for ${carLabel}. Check the exact fuel and transmission in the feature card, because equipment can vary within the same trim.`
    : `I found ${carLabel}, but I cannot verify its full feature list yet. I would rather leave a feature open than guess.`;

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "vehicle_feature_answer",
    displayMode: "inline",
    canvasType: "",
    inlineType: "feature_answer_card",
    title: isFeatureSummaryRequest ? `Features in ${carLabel}` : `${feature} in ${carLabel}`,
    answer: isFeatureSummaryRequest
      ? summaryAnswer
      : hasMatch
        ? `Yes — ${feature} is confirmed for ${carLabel}. Check the exact fuel and transmission in the feature card, because equipment can vary within the same trim.`
        : `I could not confirm ${feature} for ${carLabel}.`,
    data: {
      model,
      variant,
      city,
      feature,
      featureRequestType: isFeatureSummaryRequest ? "feature_summary" : "feature_availability",
      rows,
      matched: hasMatch,
    },
    actions: [
      makeAction({
        id: "open-features",
        label: "Open features",
        query: `Show features of ${model || carLabel}`,
        intent: "vehicle_model_features_explorer",
        canvasType: "feature_explorer_canvas",
        inlineType: "feature_answer_card",
        entities: { model, variant, feature },
        filters: { city },
        priority: 96,
      }),
      makeAction({
        id: "show-variant-price",
        label: "Show price",
        query: `Show ${carLabel} price`,
        intent: "vehicle_variant_price",
        canvasType: "pricelist_canvas",
        entities: { model, variant },
        filters: { city },
        priority: 92,
      }),
      makeAction({
        id: "compare-variants",
        label: "Compare variants",
        query: `Compare ${model || carLabel} variants`,
        intent: "vehicle_variant_comparison",
        canvasType: "comparison_canvas",
        entities: { model },
        filters: { city },
        priority: 88,
      }),
      makeAction({
        id: "calculate-emi",
        label: "Calculate EMI",
        query: `Calculate EMI for ${carLabel}`,
        intent: "vehicle_emi_calculator",
        canvasType: "emi_calculator_canvas",
        entities: { model, variant },
        filters: { city },
        priority: 84,
      }),
    ],
  });
};

export const buildVehicleFeatureComparisonResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) =>
  baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "vehicle_feature_comparison",
    displayMode: "canvas",
    canvasType: "feature_comparison_canvas",
    inlineType: "feature_comparison_summary",
    title: runtimeData.title || "Feature comparison",
    answer:
      runtimeData.answer ||
      "I compared the requested features across both cars.",
    data: {
      ...(runtimeData.data || {}),
      models: runtimeData.models || runtimeData.data?.models || [],
      features: runtimeData.features || runtimeData.data?.features || [],
      rows: getRuntimeRows(runtimeData),
    },
    actions: runtimeData.actions || [],
  });

export const buildVehicleCompareResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const runtimeVehicles = asArray(runtimeData?.selectedComparisonSet?.vehicles);
  const runtimeModels = asArray(runtimeData?.selectedComparisonSet?.models);
  const runtimeVehicleLabels = runtimeVehicles
    .map((vehicle = {}) =>
      firstMeaningful(
        vehicle.fullModel,
        [vehicle.make || vehicle.brand, vehicle.model].filter(Boolean).join(" "),
        vehicle.model,
      ),
    )
    .filter(Boolean);

  const models = runtimeModels.length >= 2
    ? runtimeModels
    : runtimeVehicleLabels.length >= 2
      ? runtimeVehicleLabels
      : getModels(toolPlan, context);
  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const rows = getRuntimeRows(runtimeData);

  const compactComparisonRow = (row = {}) => ({
    id: row.id || row._id || "",
    make: row.make || row.brand || "",
    brand: row.brand || row.make || "",
    model: row.model || row.rawModel || "",
    displayName: row.displayName || row.modelDisplayName || row.fullModel || row.model || "",
    modelDisplayName: row.modelDisplayName || row.displayName || row.fullModel || row.model || "",
    variant: row.variant || row.variantName || "",
    variantName: row.variantName || row.variant || "",
    fuel: row.fuel || row.fuelType || "",
    fuelType: row.fuelType || row.fuel || "",
    transmission: row.transmission || "",
    city: row.city || row.citySlug || "",
    exShowroomPrice: row.exShowroomPrice || row.exShowroomPriceValue || null,
    exShowroomPriceLabel: row.exShowroomPriceLabel || "",
    onRoadPrice: row.onRoadPrice || row.onRoadPriceValue || row.totalOnRoadPrice || null,
    onRoadPriceLabel: row.onRoadPriceLabel || row.totalOnRoadPriceLabel || "",
    rto: row.rto || row.rtoCharges || null,
    insurance: row.insurance || row.insuranceCharges || null,
    otherChargesTotal: row.otherChargesTotal || row.otherCharges || null,
    unavailable: Boolean(row.unavailable),
    variantResolution: row.variantResolution
      ? {
          status: row.variantResolution.status || "",
          selectedVariant: row.variantResolution.selectedVariant || "",
          reason: row.variantResolution.reason || "",
        }
      : null,
  });

  const compactRows = rows.map(compactComparisonRow);

  const comparisonVehicleLabel = (vehicle = {}) =>
    firstMeaningful(
      vehicle.fullModel,
      [vehicle.make || vehicle.brand, vehicle.model].filter(Boolean).join(" "),
      vehicle.model,
    );

  const actionCompareModels = runtimeVehicles
    .map(comparisonVehicleLabel)
    .filter(Boolean);

  const compareLabel =
    actionCompareModels.length >= 2
      ? actionCompareModels.slice(0, 2).join(" vs ")
      : models.length >= 2
        ? models.slice(0, 2).join(" vs ")
        : model
          ? `${model}${variant ? ` ${variant}` : ""} comparison`
          : "Vehicle comparison";

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "vehicle_comparison",
    displayMode: "canvas",
    canvasType: "comparison_canvas",
    title: compareLabel,
    answer: rows.length
      ? `I compared ${compareLabel} with price and feature/spec differences.`
      : `I’ll compare representative/popular variants for ${compareLabel}. You can change variants anytime.`,
    data: {
      models,
      model,
      variant,
      city,
      comparisonLevel: toolPlan.resolution?.comparisonLevel || "model",
      rows: compactRows,
      selectedComparisonSet: runtimeData.selectedComparisonSet || {},
      comparisonSummary: runtimeData.comparisonSummary || {},
      differenceSummary: runtimeData.differenceSummary || {},
      featureDifferences: runtimeData.featureDifferences || [],
      commonHighlights: runtimeData.commonHighlights || [],
      missingOrUnavailableEvidence: runtimeData.missingOrUnavailableEvidence || [],
      decisionHighlights: runtimeData.decisionHighlights || [],
      matrixCoverage: runtimeData.matrixCoverage || [],
      variantSelection: runtimeData.variantSelection || [],
      comparisonPairing: runtimeData.comparisonPairing || {},
    },
    comparisonSummary: runtimeData.comparisonSummary || {},
    differenceSummary: runtimeData.differenceSummary || {},
    featureDifferences: runtimeData.featureDifferences || [],
    commonHighlights: runtimeData.commonHighlights || [],
    missingOrUnavailableEvidence: runtimeData.missingOrUnavailableEvidence || [],
    decisionHighlights: runtimeData.decisionHighlights || [],
    matrixCoverage: runtimeData.matrixCoverage || [],
    variantSelection: runtimeData.variantSelection || [],
    comparisonPairing: runtimeData.comparisonPairing || {},
    actions: [
      ...asArray(runtimeData.comparisonPairing?.choices).map((choice, index) =>
        makeAction({
          id: choice.id || `comparison-pairing-choice-${index + 1}`,
          label: choice.label || "Use this pairing",
          query: choice.query || compareLabel,
          intent: "vehicle_comparison",
          canvasType: "comparison_canvas",
          filters: { city },
          priority: 98 - index,
        }),
      ),
      makeAction({
        id: "change-variants",
        label: "Change variants",
        query: `Change variants for ${compareLabel}`,
        intent: "vehicle_comparison",
        canvasType: "comparison_canvas",
        filters: { city },
        priority: 96,
      }),
      makeAction({
        id: "emi-difference",
        label: "Check EMI difference",
        query: `Compare EMI for ${compareLabel}`,
        intent: "vehicle_emi_calculator",
        canvasType: "emi_calculator_canvas",
        filters: { city },
        priority: 90,
      }),
      makeAction({
        id: "price-difference",
        label: "Show price difference",
        query: `Compare price for ${compareLabel}`,
        intent: "vehicle_comparison",
        canvasType: "comparison_canvas",
        filters: { city },
        priority: 86,
      }),
    ],
  });
};

export const buildVehicleRecommendResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const city = getCity(toolPlan, context);
  const rows = getRuntimeRows(runtimeData);
  const modelGroups = asArray(runtimeData.modelGroups || runtimeData.data?.modelGroups);
  const previewModelGroups = asArray(
    runtimeData.previewModelGroups ||
      runtimeData.data?.previewModelGroups ||
      runtimeData.rows,
  );
  const ranking = toolPlan.ranking || "value";
  const filters = getToolFilters(toolPlan);
  const isBudgetDiscovery = Boolean(runtimeData.budgetDiscovery?.enabled);
  const effectiveRows = isBudgetDiscovery
    ? previewModelGroups
    : !modelGroups.length || runtimeData.budgetDiscovery?.isFeatureDiscovery
      ? rows
      : modelGroups;
  const totalQualifyingModels =
    runtimeData.budgetDiscovery?.totalQualifyingModels ||
    runtimeData.totalQualifyingModels ||
    modelGroups.length ||
    effectiveRows.length;
  const totalQualifyingVariants =
    runtimeData.budgetDiscovery?.totalUniqueQualifyingVariants ||
    runtimeData.budgetDiscovery?.totalQualifyingVariants ||
    runtimeData.totalQualifyingVariants ||
    runtimeData.matchedVariantCount ||
    0;
  const totalQualifyingPriceRows =
    runtimeData.budgetDiscovery?.totalQualifyingPriceRows ||
    runtimeData.totalQualifyingPriceRows ||
    0;

  const budgetLabel = filters.budgetMax ? ` under ${formatMoney(filters.budgetMax)}` : "";
  const bodyLabel = filters.bodyType ? ` ${displayName(filters.bodyType)}` : "";
  const transmissionLabel = filters.transmission ? ` ${displayName(filters.transmission)}` : "";
  const mustHaveFeatures = asArray(filters.mustHaveFeatures);
  const primaryFeature = cleanText(
    firstMeaningful(
      toolPlan.entities?.feature,
      asArray(toolPlan.entities?.features)[0],
      mustHaveFeatures[0],
      runtimeData.feature,
      runtimeData.data?.feature,
    ),
  );
  const resolvedFeatureLabel = cleanText(
    runtimeData.budgetDiscovery?.featureResolution?.resolvedFeatures?.[0]?.displayName ||
      runtimeData.featureResolution?.resolvedFeatures?.[0]?.displayName ||
      "",
  );
  const featureLabel = resolvedFeatureLabel || (primaryFeature ? displayName(primaryFeature.replace(/_/g, " ")) : "");
  const isFeatureMatch = ranking === "feature_match" || mustHaveFeatures.length > 0;

  const title = isFeatureMatch
    ? `${featureLabel || "Feature"} matches${budgetLabel}`.replace(/\s+/g, " ").trim()
    : runtimeData.title ||
      `Best${transmissionLabel}${bodyLabel} cars${budgetLabel}`.replace(/\s+/g, " ").trim();
  const featureModelAnswer = isFeatureMatch && rows.length
    ? `I found ${totalQualifyingModels || rows.length} ${filters.bodyType && String(filters.bodyType).toLowerCase() === "suv" ? "SUV " : bodyLabel.trim() ? `${displayName(filters.bodyType)} ` : ""}model${(totalQualifyingModels || rows.length) === 1 ? "" : "s"} with ${featureLabel || "this feature"}${budgetLabel}. Showing the best matches first.`
    : "";

  const answer = isFeatureMatch
    ? rows.length
      ? featureModelAnswer
      : `I could not find strong matches${featureLabel ? ` with ${featureLabel}` : ""}${budgetLabel}. Try relaxing budget, body type, transmission, or must-have features.`
    : effectiveRows.length
      ? isBudgetDiscovery
        ? `I found ${totalQualifyingModels} model${totalQualifyingModels === 1 ? "" : "s"}${budgetLabel}. Showing the top ${effectiveRows.length} first.`
        : `I found matching cars for your filters. I’ll show the best model cards first instead of overwhelming you with every variant.`
      : `I could not find strong matches for these filters yet. Try relaxing budget, body type, transmission, or must-have features.`;

  const response = baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent:
      isFeatureMatch
        ? "vehicle_must_have_feature_builder"
        : "vehicle_recommendation",
    displayMode: "canvas",
    canvasType:
      isFeatureMatch
        ? "feature_match_builder_canvas"
        : "recommendation_results_canvas",
    title,
    answer,
    data: {
      city,
      ranking,
      filters,
      feature: primaryFeature,
      featureName: featureLabel || primaryFeature,
      features: mustHaveFeatures.length ? mustHaveFeatures : asArray(toolPlan.entities?.features),
      mustHaveFeatures,
      rows: effectiveRows,
      items: effectiveRows,
      modelGroups,
      allModelGroups: modelGroups,
      previewModelGroups: effectiveRows,
      modelGroupCount: effectiveRows.length,
      previewModelGroupCount: effectiveRows.length,
      fullModelGroupCount: runtimeData.budgetDiscovery?.fullModelGroupCount || modelGroups.length,
      returnedPreviewGroups: runtimeData.budgetDiscovery?.returnedPreviewGroups || effectiveRows.length,
      returnedModelGroups: runtimeData.budgetDiscovery?.returnedModelGroups || effectiveRows.length,
      totalModelGroupCount: totalQualifyingModels,
      totalQualifyingModels,
      totalQualifyingVariants,
      totalUniqueQualifyingVariants: totalQualifyingVariants,
      totalQualifyingPriceRows,
      matchedVariants: rows,
      variants: rows,
      groupBy: toolPlan.output?.groupBy || "model",
      summary: runtimeData.summary || {},
      budgetDiscovery: runtimeData.budgetDiscovery || null,
      noResultRecovery: runtimeData.noResultRecovery || runtimeData.budgetDiscovery?.noResultRecovery || null,
      matchedVariantCount: runtimeData.matchedVariantCount || 0,
      facets: runtimeData.facets || {},
    },
    feature: primaryFeature,
    featureName: featureLabel || primaryFeature,
    features: mustHaveFeatures.length ? mustHaveFeatures : asArray(toolPlan.entities?.features),
    actions: recommendationActions({ city }),
    leadingQuestions: buildRecommendationLeadingQuestions({ toolPlan }),
    sourceTransparency: runtimeData.sourceTransparency || {
      ...(runtimeData.modulesChecked ? { modulesChecked: runtimeData.modulesChecked } : {}),
      ...(runtimeData.matched !== undefined ? { matched: runtimeData.matched } : {}),
      ...(runtimeData.dataSource ? { dataSource: runtimeData.dataSource } : {}),
    },
    meta: {
      budgetDiscovery: runtimeData.budgetDiscovery || null,
      noResultRecovery: runtimeData.noResultRecovery || runtimeData.budgetDiscovery?.noResultRecovery || null,
      matchedVariantCount: runtimeData.matchedVariantCount || 0,
      totalQualifyingModels,
      totalQualifyingVariants,
      totalUniqueQualifyingVariants: totalQualifyingVariants,
      totalQualifyingPriceRows,
    },
  });

  return {
    ...response,
    rows: effectiveRows,
    items: effectiveRows,
    modelGroups: isBudgetDiscovery ? effectiveRows : modelGroups,
    allModelGroups: modelGroups,
    previewModelGroups: effectiveRows,
    modelGroupCount: effectiveRows.length,
    returnedPreviewGroups: runtimeData.budgetDiscovery?.returnedPreviewGroups || effectiveRows.length,
    returnedModelGroups: runtimeData.budgetDiscovery?.returnedModelGroups || effectiveRows.length,
    totalModelGroupCount: totalQualifyingModels,
    totalQualifyingModels,
    totalQualifyingVariants,
    totalUniqueQualifyingVariants: totalQualifyingVariants,
    totalQualifyingPriceRows,
    budgetDiscovery: runtimeData.budgetDiscovery || null,
    noResultRecovery: runtimeData.noResultRecovery || runtimeData.budgetDiscovery?.noResultRecovery || null,
    facets: runtimeData.facets || {},
    matched: runtimeData.matched ?? totalQualifyingModels,
  };
};

export const buildVehicleSimilarResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const rows = asArray(
    runtimeData.similarModels ||
      runtimeData.rows ||
      runtimeData.items,
  );
  const anchor = runtimeData.anchor || {};
  const anchorLabel = anchor.displayName || getModel(toolPlan, context) || "selected model";
  const answer =
    runtimeData.answer ||
    (
      rows.length
        ? `I found similar cars for ${anchorLabel}: ${rows
            .slice(0, 5)
            .map((row) => row.displayName)
            .filter(Boolean)
            .join(", ")}. ${responseToolDecisionLanguageText("decision_similar_graph_note", {
              mode: "similar_graph_answer",
            })}`
        : `I could not find enough similar-car graph data for ${anchorLabel} yet.`
    );

  const response = baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: runtimeData.intent || "vehicle_similar",
    displayMode: "canvas",
    canvasType: runtimeData.canvasType || "similar_cars_canvas",
    inlineType: runtimeData.inlineType || "similar_cars_summary",
    title: runtimeData.title || `Similar cars to ${anchorLabel}`,
    answer,
    data: {
      anchor,
      rows,
      items: rows,
      similarModels: rows,
      usageGuardrail: runtimeData.usageGuardrail || {
        canUseForFinalRecommendation: false,
        reason:
          responseToolDecisionLanguageText("decision_similar_graph_guardrail_reason", {
            mode: "similar_graph_guardrail",
          }),
      },
    },
    actions: runtimeData.actions || [],
    leadingQuestions: runtimeData.leadingQuestions || [],
    conversationSuggestions: runtimeData.conversationSuggestions || [],
    sourceTransparency: runtimeData.sourceTransparency || {
      modulesChecked: runtimeData.modulesChecked || [],
      matched: runtimeData.matched ?? rows.length,
      dataSource: runtimeData.dataSource || "",
    },
    meta: {
      ...(runtimeData.meta || {}),
      finalRecommendationEnabled: false,
    },
  });

  return {
    ...response,
    rows,
    items: rows,
    similarModels: rows,
    anchor,
    matched: runtimeData.matched ?? rows.length,
    usageGuardrail: runtimeData.usageGuardrail || response.data.usageGuardrail,
  };
};

export const buildRecommendationLeadingQuestions = ({ toolPlan = {} } = {}) => {
  const filters = getToolFilters(toolPlan);
  const questions = [];

  if (!filters.transmission) {
    questions.push(
      makeAction({
        id: "ask-transmission",
        label: "Automatic or manual?",
        query: "Do you prefer automatic or manual?",
        intent: "vehicle_recommendation",
        canvasType: "recommendation_results_canvas",
        priority: 90,
      }),
    );
  }

  if (!filters.bodyType) {
    questions.push(
      makeAction({
        id: "ask-body-type",
        label: "SUV, sedan or hatchback?",
        query: "Do you prefer SUV, sedan or hatchback?",
        intent: "vehicle_recommendation",
        canvasType: "recommendation_results_canvas",
        priority: 84,
      }),
    );
  }

  if (!filters.mustHaveFeatures?.length) {
    questions.push(
      makeAction({
        id: "ask-priority",
        label: "What matters most?",
        query: "What matters most: mileage, safety, features, comfort or value?",
        intent: "vehicle_recommendation",
        canvasType: "recommendation_results_canvas",
        priority: 78,
      }),
    );
  }

  return questions.slice(0, 1);
};

export const buildVehiclePriceBreakupResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  if (
    runtimeData?.canvasType === "unsupported_city_canvas" ||
    runtimeData?.unsupportedCity ||
    runtimeData?.widget?.unsupportedCity
  ) {
    return buildVehiclePricelistResponse({ toolPlan, runtimeData, context });
  }

  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "this car";
  const rows = getRuntimeRows(runtimeData);

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "vehicle_price_breakup",
    displayMode: "canvas",
    canvasType: "price_breakup_canvas",
    title: `${carLabel} on-road breakup`,
    answer: `Here is the on-road price breakup for ${carLabel} in ${formatCity(city)}.`,
    data: {
      model,
      variant,
      city,
      rows,
      breakup: runtimeData.breakup || runtimeData.data || {},
    },
    actions: [
      makeAction({
        id: "calculate-emi",
        label: "Calculate EMI",
        query: `Calculate EMI for ${carLabel}`,
        intent: "vehicle_emi_calculator",
        canvasType: "emi_calculator_canvas",
        entities: { model, variant },
        filters: { city },
        priority: 96,
      }),
      makeAction({
        id: "explain-charges",
        label: "Explain charges",
        query: "Explain other charges in on-road price",
        intent: "vehicle_explainer",
        inlineType: "explainer_card",
        entities: { topic: "other_charges" },
        filters: { city },
        priority: 90,
      }),
      makeAction({
        id: "get-quotation",
        label: "Get quotation",
        type: "lead",
        query: `Get quotation for ${carLabel}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType: "quotation",
        entities: { model, variant },
        filters: { city },
        priority: 84,
      }),
    ],
  });
};

export const buildVehicleEmiResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const filters = getToolFilters(toolPlan);
  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "this car";
  const variantResolution = runtimeData.variantResolution || {};

  if (variantResolution.status === "not_found") {
    return baseResponse({
      toolPlan,
      context,
      runtimeData,
      intent: "vehicle_variant_ambiguity",
      displayMode: "inline",
      canvasType: "",
      inlineType: "variant_ambiguity_card",
      title: `Choose exact ${model} variant for EMI`,
      answer: `I found ${model}, but I could not match ${variantResolution.requestedVariant || variant} to a current variant I can verify. I should not calculate EMI from the wrong price, so choose an available variant first or ask for model-level pricing.`,
      data: {
        model,
        requestedVariant: variantResolution.requestedVariant || variant,
        city,
        assumptions: {
          priceBasis: filters.priceBasis || "on_road",
          downPayment: filters.downPayment,
          loanAmount: filters.loanAmount,
          loanPercent: filters.loanPercent,
          tenureMonths: filters.tenureMonths,
          roi: filters.roi,
        },
        candidateVariants: variantResolution.candidateVariants || runtimeData.candidateVariants || [],
        candidateRows: runtimeData.candidateRows || [],
      },
      contextPatch: makeVariantAmbiguityContextPatch({
        model,
        requestedVariant: variantResolution.requestedVariant || variant,
        city,
      }),
      actions: buildVariantNotFoundActions({
        model,
        requestedVariant: variantResolution.requestedVariant || variant,
        candidateVariants: variantResolution.candidateVariants || runtimeData.candidateVariants || [],
        city,
        targetIntent: "vehicle_emi_calculator",
      }),
    });
  }

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "vehicle_emi_calculator",
    displayMode: "canvas",
    canvasType: "emi_calculator_canvas",
    title: `${carLabel} EMI`,
    answer: `I’ll calculate EMI for ${carLabel} using ${formatCity(city)} on-road pricing and your loan assumptions.`,
    data: {
      model,
      variant,
      city,
      assumptions: {
        priceBasis: filters.priceBasis || "on_road",
        downPayment: filters.downPayment,
        loanAmount: filters.loanAmount,
        loanPercent: filters.loanPercent,
        tenureMonths: filters.tenureMonths,
        roi: filters.roi,
      },
      emi: runtimeData.emi || runtimeData.data || {},
      rows: getRuntimeRows(runtimeData),
    },
    actions: [
      makeAction({
        id: "lower-emi",
        label: "Lower EMI",
        query: `Lower EMI for ${carLabel}`,
        intent: "vehicle_emi_calculator",
        canvasType: "emi_calculator_canvas",
        entities: { model, variant },
        filters: { city },
        priority: 96,
      }),
      makeAction({
        id: "show-price-breakup",
        label: "Show breakup",
        query: `Show price breakup of ${carLabel}`,
        intent: "vehicle_price_breakup",
        canvasType: "price_breakup_canvas",
        entities: { model, variant },
        filters: { city },
        priority: 90,
      }),
      makeAction({
        id: "get-finance-callback",
        label: "Finance callback",
        type: "lead",
        query: `Request finance callback for ${carLabel}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType: "finance_callback",
        entities: { model, variant },
        filters: { city, leadType: "finance_callback" },
        priority: 84,
      }),
    ],
  });
};

export const buildVehicleFinanceKnowledgeResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const records = asArray(runtimeData.records);
  const checklist = uniqueBy(
    records.flatMap((record) => asArray(record.checklist)),
    (item) => item,
  ).slice(0, 10);
  const caveats = uniqueBy(
    records.flatMap((record) => asArray(record.caveats)),
    (item) => item,
  ).slice(0, 5);
  const summaries = records.map((record) => cleanText(record.summary)).filter(Boolean);
  const applicantType = runtimeData.applicantType || "general";

  if (!records.length) {
    return baseResponse({
      toolPlan,
      context,
      runtimeData,
      intent: "new_car_finance_faq",
      displayMode: "inline",
      canvasType: "",
      inlineType: "finance_faq_card",
      title: "Car loan guidance",
      answer:
        "I can help with car-loan eligibility, documents, down payment and EMI, but the reviewed finance guide is unavailable right now. I would rather not guess lender rules. You can still calculate an EMI, or try the finance question again shortly.",
      data: { topics: runtimeData.topics || [], checklist: [], caveats: [] },
      actions: [
        makeAction({
          id: "finance-calculate-emi",
          label: "Calculate EMI",
          query: "Calculate car loan EMI",
          intent: "vehicle_emi_calculator",
          canvasType: "emi_calculator_canvas",
          priority: 90,
        }),
      ],
    });
  }

  const answer = summaries.join(" ");
  const applicantPrompt = applicantType === "general"
    ? "Tell me whether you are salaried or self-employed and I’ll narrow the document list."
    : "Share the car and expected down payment when you want an EMI estimate.";

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "new_car_finance_faq",
    displayMode: "inline",
    canvasType: "",
    inlineType: "finance_faq_card",
    title: records.length === 1 ? records[0].title : "Car loan guidance",
    answer: `${answer} ${applicantPrompt}`,
    data: {
      topics: runtimeData.topics || [],
      applicantType,
      sections: records.map((record) => ({
        key: record.key,
        title: record.title,
        summary: record.summary,
        checklist: asArray(record.checklist),
        caveats: asArray(record.caveats),
      })),
      checklist,
      caveats,
      approvalGuaranteed: false,
      liveLenderOfferUsed: false,
    },
    actions: [
      ...(applicantType === "general"
        ? [
            makeAction({
              id: "finance-salaried-documents",
              label: "I’m salaried",
              query: "Car loan documents and eligibility for a salaried applicant",
              intent: "new_car_finance_faq",
              inlineType: "finance_faq_card",
              priority: 98,
            }),
            makeAction({
              id: "finance-self-employed-documents",
              label: "I’m self-employed",
              query: "Car loan documents and eligibility for a self-employed applicant",
              intent: "new_car_finance_faq",
              inlineType: "finance_faq_card",
              priority: 97,
            }),
          ]
        : []),
      makeAction({
        id: "finance-calculate-emi",
        label: "Calculate EMI",
        query: "Calculate car loan EMI for the car I am considering",
        intent: "vehicle_emi_calculator",
        canvasType: "emi_calculator_canvas",
        priority: 92,
      }),
    ],
    sourceTransparency: runtimeData.sourceTransparency || {},
    meta: {
      financeKnowledgeVersion: records.map((record) => record.version).filter(Boolean).at(-1) || "",
      liveLenderOfferUsed: false,
    },
  });
};

export const buildVehiclePriceHistoryResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const rows = getRuntimeRows(runtimeData);
  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "this car";

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "vehicle_price_history",
    displayMode: "canvas",
    canvasType: "price_history_canvas",
    title: `${carLabel} price history`,
    answer: rows.length
      ? `Here is the available price history for ${carLabel}.`
      : `I do not have enough price-history rows for ${carLabel} yet.`,
    data: {
      model,
      variant,
      city,
      rows,
    },
    actions: [
      makeAction({
        id: "current-price",
        label: "Show current price",
        query: `Show current price of ${carLabel}`,
        intent: "vehicle_pricelist",
        canvasType: "pricelist_canvas",
        entities: { model, variant },
        filters: { city },
        priority: 90,
      }),
      makeAction({
        id: "get-quotation",
        label: "Get quotation",
        type: "lead",
        query: `Get quotation for ${carLabel}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType: "quotation",
        entities: { model, variant },
        filters: { city },
        priority: 80,
      }),
    ],
  });
};

export const buildVehicleExplainerResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const topic = cleanText(
    firstMeaningful(
      toolPlan.entities?.topic,
      asArray(toolPlan.entities?.topics)[0],
      runtimeData.topic,
      "car buying",
    ),
  );

  const answer =
    runtimeData.answer ||
    runtimeData.explanation ||
    buildGenericExplainerAnswer(topic);

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "vehicle_explainer",
    displayMode: "inline",
    canvasType: runtimeData.useCanvas ? "explainer_canvas" : "",
    inlineType: "explainer_card",
    title: runtimeData.title || displayName(topic.replace(/_/g, " ")),
    answer,
    data: {
      topic,
      sections: runtimeData.sections || [],
      references: runtimeData.references || [],
    },
    actions: [
      makeAction({
        id: "calculate-emi",
        label: "Calculate EMI",
        query: "Calculate EMI for my selected car",
        intent: "vehicle_emi_calculator",
        canvasType: "emi_calculator_canvas",
        priority: 70,
      }),
      makeAction({
        id: "get-quotation",
        label: "Get quotation",
        type: "lead",
        query: "Get quotation for my selected car",
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType: "quotation",
        priority: 60,
      }),
    ],
  });
};

export const buildGenericExplainerAnswer = (topic = "") => {
  const keyTopic = searchKey(topic);

  if (keyTopic.includes("emi")) {
    return "EMI depends on on-road price, down payment, loan amount, tenure and interest rate. A longer tenure lowers monthly EMI but increases total interest.";
  }

  if (keyTopic.includes("on road") || keyTopic.includes("ex showroom")) {
    return "Ex-showroom is the base vehicle price before registration and insurance. On-road price includes ex-showroom, RTO, insurance, TCS, FASTag, handling or other applicable charges.";
  }

  if (keyTopic.includes("fuel") || keyTopic.includes("petrol") || keyTopic.includes("diesel")) {
    return "Fuel choice depends on monthly running, city/highway usage, upfront price, running cost, boot-space needs and expected ownership period.";
  }

  if (keyTopic.includes("rto")) {
    return "RTO charges are government registration charges. They vary by state, vehicle price, fuel type, registration type and applicable local rules.";
  }

  if (keyTopic.includes("insurance")) {
    return "Car insurance pricing depends on IDV, coverage type, add-ons like zero depreciation, engine protect, consumables, and insurer terms.";
  }

  return "I can explain this in simple terms and connect it to price, EMI, quotation or variant selection whenever you are ready.";
};

export const buildAciLeadCaptureResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const leadType =
    toolPlan.entities?.leadType ||
    toolPlan.filters?.leadType ||
    runtimeData.leadType ||
    "quotation";

  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "your selected car";

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "aci_new_car_quotation",
    displayMode: "canvas",
    canvasType: leadType === "callback" ? "lead_capture_canvas" : "aci_quotation_canvas",
    title:
      leadType === "finance_callback"
        ? "Finance callback"
        : leadType === "offer_enquiry"
          ? "Best offer request"
          : leadType === "callback"
            ? "Advisor callback"
            : "ACI quotation request",
    answer:
      runtimeData.created || runtimeData.requestId
        ? `Done. Your request for ${carLabel} has been created.`
        : `I can create the ${leadType.replace(/_/g, " ")} request for ${carLabel}. I’ll ask only the missing details needed to proceed.`,
    data: {
      model,
      variant,
      city,
      leadType,
      selectedServices:
        toolPlan.entities?.selectedServices ||
        toolPlan.filters?.selectedServices ||
        runtimeData.selectedServices ||
        [leadType],
      request: runtimeData.request || {},
      requestId: runtimeData.requestId || "",
      requiredFields: runtimeData.requiredFields || ["name", "mobile", "city"],
    },
    actions: [
      makeAction({
        id: "submit-request",
        label: runtimeData.created ? "Track request" : "Continue request",
        type: "lead",
        query: runtimeData.created
          ? `Track request for ${carLabel}`
          : `Continue quotation request for ${carLabel}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType,
        entities: { model, variant },
        filters: { city, leadType },
        priority: 96,
      }),
      makeAction({
        id: "add-finance",
        label: "Add finance",
        type: "lead",
        query: `Add finance requirement for ${carLabel}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType: "finance_callback",
        entities: { model, variant },
        filters: { city, leadType: "finance_callback" },
        priority: 84,
      }),
      makeAction({
        id: "add-exchange",
        label: "Add exchange",
        type: "lead",
        query: `Add exchange car for ${carLabel}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType: "quotation",
        entities: { model, variant },
        filters: { city, selectedServices: ["quotation", "exchange"] },
        priority: 78,
      }),
    ],
  });
};

export const buildUnavailableResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const reason =
    toolPlan.unavailableReason ||
    toolPlan.filters?.unavailableReason ||
    runtimeData.unavailableReason ||
    "unsupported_request";

  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`);

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "unavailable",
    displayMode: "inline",
    canvasType: "",
    inlineType: "unavailable_notice",
    title: "Not available yet",
    answer: buildUnavailableAnswer({ reason, carLabel }),
    data: {
      reason,
      model,
      variant,
      fallbackOptions: runtimeData.fallbackOptions || [],
    },
    actions: buildUnavailableActions({ reason, model, variant, city: getCity(toolPlan, context) }),
  });
};

export const buildUnavailableAnswer = ({ reason = "", carLabel = "" } = {}) => {
  if (reason === "offers_not_available") {
    return `Verified live offers are not available in the system yet. I can still create an offer enquiry or quotation request${carLabel ? ` for ${carLabel}` : ""}.`;
  }

  if (reason === "variant_wise_color_not_available") {
    return `I can show model-level colours, but exact ${carLabel ? `${carLabel} ` : ""}variant-wise colour or stock availability is not available yet.`;
  }

  if (reason === "dealer_inventory_not_available" || reason === "waiting_period_not_available") {
    return `Dealer inventory and waiting-period data is not available yet. I can create a callback or quotation request to confirm availability.`;
  }

  if (reason === "service_centers_not_available") {
    return "Service-center data is not available in this system yet.";
  }

  if (reason === "service_cost_not_available") {
    return "Exact service-cost data is not available yet. I can explain ownership cost generally, but should not invent figures.";
  }

  if (reason === "bank_finance_schemes_not_available") {
    return "Bank-wise finance schemes are not available yet. I can create a finance callback request or calculate generic EMI.";
  }

  return renderResponseLanguage(
    "generic_no_data_but_can_help",
    {
      topic: reason ? displayName(String(reason).replace(/_/g, " ")) : "this request",
    },
    [reason, carLabel],
  );
};

export const buildUnavailableActions = ({ reason = "", model = "", variant = "", city = DEFAULT_CITY } = {}) => {
  const carLabel = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "this car";

  if (["offers_not_available", "bank_finance_schemes_not_available"].includes(reason)) {
    return [
      makeAction({
        id: "create-callback",
        label: reason === "bank_finance_schemes_not_available" ? "Finance callback" : "Request offer callback",
        type: "lead",
        query:
          reason === "bank_finance_schemes_not_available"
            ? `Request finance callback for ${carLabel}`
            : `Request best offer for ${carLabel}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType: reason === "bank_finance_schemes_not_available" ? "finance_callback" : "offer_enquiry",
        entities: { model, variant },
        filters: { city },
        priority: 90,
      }),
    ];
  }

  if (reason === "variant_wise_color_not_available") {
    return [
      makeAction({
        id: "show-colors",
        label: "Show model colors",
        query: `Show colors of ${model || "this car"}`,
        intent: "vehicle_color_gallery",
        canvasType: "color_studio_canvas",
        entities: { model },
        filters: { city },
        priority: 90,
      }),
      makeAction({
        id: "get-quote",
        label: "Get quote",
        type: "lead",
        query: `Get quotation for ${carLabel}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        leadType: "quotation",
        entities: { model, variant },
        filters: { city },
        priority: 84,
      }),
    ];
  }

  return [];
};

const buildRegistryClarificationAnswer = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
  plan = {},
} = {}) => {
  const model = getModel(toolPlan, context);
  const topic = cleanText(
    runtimeData.topic ||
      runtimeData.feature ||
      runtimeData.attributeLabel ||
      plan.topic ||
      getFeature(toolPlan),
  );

  if (runtimeData.question || plan.clarification) return "";

  if (model && !topic) {
    return renderResponseLanguage(
      "clarification_known_model_missing_topic",
      { model },
      [model, toolPlan.tool, runtimeData.reason],
    );
  }

  if (topic && !model) {
    return renderResponseLanguage(
      "clarification_known_topic_missing_model",
      { topic },
      [topic, toolPlan.tool, runtimeData.reason],
    );
  }

  return "";
};

export const buildClarificationResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
  plan = {},
} = {}) =>
  baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "clarification",
    displayMode: "inline",
    canvasType: "",
    inlineType: "clarification_card",
    title: "Need one detail",
    answer:
      buildRegistryClarificationAnswer({ toolPlan, runtimeData, context, plan }) ||
      runtimeData.question ||
      plan.clarification ||
      "Can you clarify what you want to check?",
    data: {
      options: runtimeData.options || [],
    },
    actions: asArray(runtimeData.options).map((option, index) =>
      makeAction({
        id: `clarify-${index + 1}`,
        label: option.label || option,
        query: option.query || option.label || option,
        intent: option.intent || "clarification",
        priority: 90 - index,
      }),
    ),
  });

export const buildGeneralResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) =>
  baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "general_response",
    displayMode: "inline",
    canvasType: "",
    inlineType: "text_notice",
    title: runtimeData.title || "ACI Assist",
    answer:
      runtimeData.answer ||
      runtimeData.message ||
      "I can help with new-car prices, comparison, features, EMI, colours, offers, quotation and finance questions.",
    data: runtimeData.data || {},
    actions: runtimeData.actions || [],
  });

const removeDuplicateMakePrefixFromText = (value = "") =>
  cleanText(value).replace(/\b([A-Z][a-z]+)\s+\1\s+/g, "$1 ");

const stripMakeFromModelLabel = ({ make = "", model = "" } = {}) => {
  const brand = cleanText(make);
  let label = cleanText(model);

  if (!brand || !label) return label;

  let guard = 0;
  while (
    guard < 3 &&
    normalizeSearchKey(label).startsWith(`${normalizeSearchKey(brand)} `)
  ) {
    const makeWordCount = brand.split(/\s+/).filter(Boolean).length;
    const next = cleanText(label.split(/\s+/).slice(makeWordCount).join(" "));
    if (!next || normalizeSearchKey(next) === normalizeSearchKey(label)) break;
    label = next;
    guard += 1;
  }

  return label;
};

const normalizeSpecSelectedVehicleLabel = (vehicle = {}, fallback = {}) => {
  const source = vehicle && typeof vehicle === "object" ? vehicle : {};
  const make = cleanText(
    source.make ||
      source.brand ||
      fallback.make ||
      fallback.brand ||
      "",
  );

  const model = stripMakeFromModelLabel({
    make,
    model:
      source.model ||
      fallback.model ||
      source.fullModel ||
      source.displayName ||
      fallback.fullModel ||
      fallback.displayName ||
      "",
  });

  const fullModelRaw = cleanText(
    source.fullModel ||
      source.displayName ||
      fallback.fullModel ||
      fallback.displayName ||
      "",
  );

  const fullModelBase =
    stripMakeFromModelLabel({ make, model: fullModelRaw }) ||
    model ||
    stripMakeFromModelLabel({ make, model: fallback.model || "" });

  const fullModel = removeDuplicateMakePrefixFromText(
    make && fullModelBase ? `${make} ${fullModelBase}` : fullModelBase || model || make,
  );

  return {
    ...source,
    make,
    brand: cleanText(source.brand || source.make || "") || make,
    model,
    fullModel,
    displayName: fullModel,
  };
};

export const buildVehicleSpecAttributeResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const rawData = runtimeData.data || runtimeData || {};
  const rawContextPatch = runtimeData.contextPatch || rawData.contextPatch || null;

  const selectedVehicle = normalizeSpecSelectedVehicleLabel(
    rawContextPatch?.selectedVehicle ||
      rawData.selectedVehicle ||
      context.selectedVehicle ||
      {},
    {
      make:
        rawContextPatch?.anchorMake ||
        rawData.anchorMake ||
        context.anchorMake ||
        context.selectedVehicle?.make ||
        context.selectedVehicle?.brand ||
        "",
      model:
        rawContextPatch?.anchorModel ||
        rawData.anchorModel ||
        context.anchorModel ||
        context.selectedVehicle?.model ||
        "",
      fullModel:
        rawContextPatch?.anchorFullModel ||
        rawData.anchorFullModel ||
        context.anchorFullModel ||
        context.selectedVehicle?.fullModel ||
        "",
    },
  );

  const cleanContextPatch = rawContextPatch
    ? {
        ...rawContextPatch,
        selectedVehicle,
        anchorMake: rawContextPatch.anchorMake || selectedVehicle.make || "",
        anchorModel: selectedVehicle.model || rawContextPatch.anchorModel || "",
        anchorFullModel: selectedVehicle.fullModel || rawContextPatch.anchorFullModel || "",
        contextState: rawContextPatch.contextState
          ? {
              ...rawContextPatch.contextState,
              selectedVehicle: normalizeSpecSelectedVehicleLabel(
                rawContextPatch.contextState.selectedVehicle || selectedVehicle,
                selectedVehicle,
              ),
              anchors: rawContextPatch.contextState.anchors
                ? {
                    ...rawContextPatch.contextState.anchors,
                    primaryVehicle: normalizeSpecSelectedVehicleLabel(
                      rawContextPatch.contextState.anchors.primaryVehicle || selectedVehicle,
                      selectedVehicle,
                    ),
                  }
                : rawContextPatch.contextState.anchors,
            }
          : rawContextPatch.contextState,
        aciContextState: rawContextPatch.aciContextState
          ? {
              ...rawContextPatch.aciContextState,
              selectedVehicle: normalizeSpecSelectedVehicleLabel(
                rawContextPatch.aciContextState.selectedVehicle || selectedVehicle,
                selectedVehicle,
              ),
              anchors: rawContextPatch.aciContextState.anchors
                ? {
                    ...rawContextPatch.aciContextState.anchors,
                    primaryVehicle: normalizeSpecSelectedVehicleLabel(
                      rawContextPatch.aciContextState.anchors.primaryVehicle || selectedVehicle,
                      selectedVehicle,
                    ),
                  }
                : rawContextPatch.aciContextState.anchors,
            }
          : rawContextPatch.aciContextState,
      }
    : null;

  const cleanData = {
    ...rawData,
    anchorMake: rawData.anchorMake || selectedVehicle.make || "",
    anchorModel: selectedVehicle.model || rawData.anchorModel || "",
    anchorFullModel: selectedVehicle.fullModel || rawData.anchorFullModel || "",
    selectedVehicle,
    ...(cleanContextPatch ? { contextPatch: cleanContextPatch } : {}),
  };

  return baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: runtimeData.intent || "vehicle_spec_attribute_answer",
    displayMode: "inline",
    canvasType: "",
    inlineType: runtimeData.inlineType || "spec_attribute_answer_card",
    title: removeDuplicateMakePrefixFromText(runtimeData.title || "Vehicle specification"),
    answer:
      removeDuplicateMakePrefixFromText(runtimeData.answer || "") ||
      "I found the model, but I cannot verify that exact specification yet.",
    data: cleanData,
    actions: runtimeData.actions || runtimeData.data?.nextActions || [],
    leadingQuestions:
      runtimeData.leadingQuestions ||
      runtimeData.data?.nextActions ||
      [],
    conversationSuggestions:
      runtimeData.conversationSuggestions ||
      runtimeData.leadingQuestions ||
      runtimeData.data?.nextActions ||
      [],
    sourceTransparency: runtimeData.sourceTransparency || {},
    contextPatch: cleanContextPatch,
    meta: {
      ...(runtimeData.meta || {}),
      responseTool: "vehicle_spec_attribute_lookup",
    },
  });
};

export const buildInternalPassthroughResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) =>
  baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: runtimeData.intent || "internal_passthrough",
    displayMode: runtimeData.canvasType ? "canvas" : "inline",
    canvasType: runtimeData.canvasType || "",
    inlineType: runtimeData.inlineType || "",
    title: runtimeData.title || "Internal CDrive result",
    answer:
      runtimeData.answer ||
      runtimeData.message ||
      "I found a result for this request.",
    data: runtimeData.data || runtimeData,
    actions: runtimeData.actions || [],
    leadingQuestions: [],
    conversationSuggestions: [],
  });

export const buildUsedCarPassthroughResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) =>
  baseResponse({
    toolPlan,
    context,
    runtimeData,
    intent: "used_car_passthrough",
    displayMode: "inline",
    canvasType: "",
    inlineType: "text_notice",
    title: "Used-car request",
    answer:
      runtimeData.answer ||
      "This looks like a used-car request. New-car ACI Assist can hand this off to the used-car module.",
    data: runtimeData.data || {},
    actions: runtimeData.actions || [],
  });


export const buildVehicleScoreInsightResponse = ({
  toolPlan = {},
  runtimeData = {},
  context = {},
} = {}) => {
  const data = runtimeData.data || runtimeData || {};
  const operation =
    runtimeData.operation ||
    toolPlan.operation ||
    toolPlan.input?.operation ||
    "variant_score_insight";

  const usageGuardrail =
    runtimeData.usageGuardrail ||
    data.usageGuardrail ||
    {
      canUseForFinalRecommendation: false,
      finalRecommendationEnabled: false,
      reason:
        "These are diagnostic module scores only. Final recommendation needs buyer-context weighting, similar-cars graph, upgrade ladder, service/resale evidence and recommendation policy.",
    };

  const modules = data.modules || {};
  const featureScore = modules.features?.score ?? "NA";
  const safetyScore = modules.safety?.score ?? "NA";
  const valueScore = modules.value?.score ?? "NA";
  const regretRisk = modules.regretRisk?.score ?? "NA";

  const firstVariant = asArray(data.variants || runtimeData.variants || runtimeData.rows)[0] || {};
  const modelLabel = cleanText(
    data.modelLabel ||
      runtimeData.modelLabel ||
      data.fullModel ||
      runtimeData.fullModel ||
      firstVariant.fullModel ||
      firstVariant.modelLabel ||
      firstVariant.model ||
      toolPlan.input?.fullModel ||
      toolPlan.input?.model ||
      toolPlan.entities?.fullModel ||
      toolPlan.entities?.model ||
      context.selectedVehicle?.fullModel ||
      [context.selectedVehicle?.make, context.selectedVehicle?.model].filter(Boolean).join(" "),
  );
  const variantName =
    data.variantFullName ||
    runtimeData.variantFullName ||
    firstVariant.variantFullName ||
    firstVariant.fullVariantName ||
    firstVariant.fullName ||
    runtimeData.title ||
    modelLabel ||
    "this model";
  const isCrossModelScoreDiagnostic =
    operation === "cross_model_score_diagnostic" ||
    data.diagnosticType === "cross_model_score_diagnostic";

  const scoreDiagnosticModelSummaries = asArray(
    data.modelSummaries ||
      data.models ||
      runtimeData.modelSummaries ||
      runtimeData.models ||
      data.rows ||
      runtimeData.rows,
  );

  const scoreDiagnosticTitleText = [
    runtimeData.userMessage,
    runtimeData.message,
    runtimeData.query,
    data.userMessage,
    data.message,
    data.query,
    toolPlan.input?.userMessage,
    toolPlan.input?.message,
    toolPlan.input?.query,
    toolPlan.args?.userMessage,
    toolPlan.args?.message,
    toolPlan.args?.query,
    toolPlan.params?.userMessage,
    toolPlan.params?.message,
    toolPlan.params?.query,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const normalizeScoreDiagnosticTitleToken = (value = "") =>
    String(value || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();

  const scoreDiagnosticModelLabels = scoreDiagnosticModelSummaries
    .map((model = {}, index) => {
      const rawLabel =
        model.label ||
        model.fullModel ||
        model.displayName ||
        [model.make, model.makeKey, model.model, model.modelKey].filter(Boolean).join(" ") ||
        "";

      const label = String(rawLabel || "")
        .replace(/[_-]+/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .replace(/\b\w/g, (char) => char.toUpperCase());

      const aliases = [
        model.modelKey,
        model.model,
        model.fullModel,
        model.displayName,
        model.label,
      ]
        .map(normalizeScoreDiagnosticTitleToken)
        .filter(Boolean);

      const positions = aliases
        .map((alias) => scoreDiagnosticTitleText.indexOf(alias))
        .filter((position) => position >= 0);

      return {
        label,
        index,
        position: positions.length ? Math.min(...positions) : Number.MAX_SAFE_INTEGER,
      };
    })
    .filter((item) => item.label)
    .sort((left, right) => {
      if (left.position !== right.position) return left.position - right.position;
      return left.index - right.index;
    })
    .map((item) => item.label);

  const crossModelScoreTitle =
    isCrossModelScoreDiagnostic && scoreDiagnosticModelLabels.length >= 2
      ? `${scoreDiagnosticModelLabels.slice(0, 2).join(" vs ")} score diagnostics`
      : "";

  const scoreTitle =
    crossModelScoreTitle ||
    data.variantFullName ||
    runtimeData.variantFullName ||
    runtimeData.title ||
    (modelLabel ? `${modelLabel} value diagnostics` : `${variantName} value diagnostics`);

  const baseAnswer =
    runtimeData.answer ||
    data.answer ||
    (
      data.variantFullName
        ? `${variantName}: safety ${safetyScore}, features ${featureScore}, same-model value ${valueScore}, regret risk ${regretRisk}.`
        : `${variantName}: diagnostic value data is available.`
    );

  const stripScoreDiagnosticNotes = (value = "") =>
    cleanText(value)
      .replace(/\s*(?:Use this as directional module-score diagnostics, not as a final purchase verdict\.|Use this as directional scoring, not as a final purchase verdict\.|These are module-score signals only, not a final recommendation\.|Use these module scores as diagnostics, not as a final recommendation\.|This score view is diagnostic-only and should not be treated as a final recommendation\.|This is diagnostic-only, not a final recommendation\.|Treat this as diagnostic-only guidance, not as a final recommendation\.|This is a diagnostic signal only; it is not a final purchase recommendation\.|Use this as directional context\.)/gi, " ")
      .replace(/\s*;\s*\./g, ".")
      .replace(/\s+\./g, ".")
      .replace(/\.\./g, ".")
      .replace(/\s+/g, " ")
      .trim();

  const hasScoreDiagnosticNote = (value = "") =>
    /\b(?:directional module-score diagnostics|diagnostic-only|final purchase verdict|final recommendation|diagnostic signal only|directional context)\b/i.test(cleanText(value));

  const buildSingleScoreDiagnosticAnswer = (body = "", note = "") => {
    const originalBody = cleanText(body);
    const cleanBody = stripScoreDiagnosticNotes(originalBody);
    const cleanNote = cleanText(note);

    if (!cleanBody) return cleanNote || originalBody;
    if (hasScoreDiagnosticNote(originalBody)) return cleanBody ? `${cleanBody} ${cleanNote}`.replace(/\s+/g, " ").trim() : cleanNote;
    return [cleanBody, cleanNote].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
  };

  const guardrailText = responseToolDecisionLanguageText("decision_score_module_summary_note", {
    operation,
  });
  const buyerSafeBaseAnswer = stripScoreDiagnosticNotes(baseAnswer);
  const modelSummaries = scoreDiagnosticModelSummaries;
  const moduleComparisons = asArray(
    data.moduleComparisons ||
      runtimeData.moduleComparisons ||
      data.scoreComparison?.moduleComparisons ||
      runtimeData.scoreComparison?.moduleComparisons,
  );
  const responseRows = isCrossModelScoreDiagnostic
    ? modelSummaries
    : (runtimeData.rows || data.variants || []);


  return {
    intent: runtimeData.intent || toolPlan.tool || "vehicle_score_insight",
    displayMode: "canvas",
    canvasType: "score_insight_canvas",
    inlineType: "score_insight_summary",
    operation,
    diagnosticType: isCrossModelScoreDiagnostic ? "cross_model_score_diagnostic" : data.diagnosticType,
    title: scoreTitle,
    answer: buildSingleScoreDiagnosticAnswer(buyerSafeBaseAnswer || baseAnswer, guardrailText),
    data,
    rows: responseRows,
    ...(isCrossModelScoreDiagnostic
      ? {
          models: modelSummaries,
          modelSummaries,
          moduleComparisons,
          scoreComparison: {
            diagnosticType: "cross_model_score_diagnostic",
            models: modelSummaries,
            modelSummaries,
            moduleComparisons,
          },
          modelCount: modelSummaries.length,
          rowsCount: responseRows.length,
          moduleComparisonCount: moduleComparisons.length,
        }
      : {}),
    variants: data.variants || runtimeData.variants || [],
    usageGuardrail,
    actions: runtimeData.actions || [],
    leadingQuestions:
      runtimeData.leadingQuestions ||
      [
        "Show strengths and weak points",
        "Compare value within this model",
        "Show safety score details",
      ],
    conversationSuggestions:
      runtimeData.conversationSuggestions ||
      [
        "Show strengths and weak points",
        "Compare value within this model",
        "Show safety score details",
      ],
    contextPatch: runtimeData.contextPatch || {},
    sourceTransparency:
      runtimeData.sourceTransparency ||
      {
        modulesChecked: ["aci_vehicle_variant_score_profile"],
        matched:
          runtimeData.count ??
          data.count ??
          (Array.isArray(data.variants) ? data.variants.length : data.scoreProfileKey ? 1 : 0),
        dataSource: "aci_vehicle_variant_score_profile",
      },
    meta: {
      ...(runtimeData.meta || {}),
      responseTool: "vehicle_score_insight",
      operation,
      finalRecommendationEnabled: false,
      scoreInsightGuardrail: usageGuardrail,
    },
  };
};


/* -------------------------------------------------------------------------- */
/*  Registry                                                                  */
/* -------------------------------------------------------------------------- */

export const ACI_RESPONSE_TOOLS = {
  vehicle_pricelist: {
    id: "vehicle_pricelist",
    run: buildVehiclePricelistResponse,
  },
  vehicle_colors: {
    id: "vehicle_colors",
    run: buildVehicleColorsResponse,
  },
  vehicle_feature_lookup: {
    id: "vehicle_feature_lookup",
    run: buildVehicleFeatureLookupResponse,
  },
  vehicle_feature_comparison: {
    id: "vehicle_feature_comparison",
    run: buildVehicleFeatureComparisonResponse,
  },
  vehicle_spec_attribute_lookup: {
    id: "vehicle_spec_attribute_lookup",
    run: buildVehicleSpecAttributeResponse,
  },
  vehicle_compare: {
    id: "vehicle_compare",
    run: buildVehicleCompareResponse,
  },
  vehicle_recommend: {
    id: "vehicle_recommend",
    run: buildVehicleRecommendResponse,
  },
  vehicle_similar: {
    id: "vehicle_similar",
    run: buildVehicleSimilarResponse,
  },
  vehicle_score_insight: {
    id: "vehicle_score_insight",
    run: buildVehicleScoreInsightResponse,
  },
  vehicle_score_profile: {
    id: "vehicle_score_profile",
    run: buildVehicleScoreInsightResponse,
  },
  vehicle_model_score_insights: {
    id: "vehicle_model_score_insights",
    run: buildVehicleScoreInsightResponse,
  },
  vehicle_same_family_value_insights: {
    id: "vehicle_same_family_value_insights",
    run: buildVehicleScoreInsightResponse,
  },
  vehicle_top_score_insights: {
    id: "vehicle_top_score_insights",
    run: buildVehicleScoreInsightResponse,
  },
  vehicle_price_breakup: {
    id: "vehicle_price_breakup",
    run: buildVehiclePriceBreakupResponse,
  },
  vehicle_emi: {
    id: "vehicle_emi",
    run: buildVehicleEmiResponse,
  },
  vehicle_finance_knowledge: {
    id: "vehicle_finance_knowledge",
    run: buildVehicleFinanceKnowledgeResponse,
  },
  vehicle_price_history: {
    id: "vehicle_price_history",
    run: buildVehiclePriceHistoryResponse,
  },
  vehicle_explainer: {
    id: "vehicle_explainer",
    run: buildVehicleExplainerResponse,
  },
  aci_lead_capture: {
    id: "aci_lead_capture",
    run: buildAciLeadCaptureResponse,
  },
  used_car_passthrough: {
    id: "used_car_passthrough",
    run: buildUsedCarPassthroughResponse,
  },
  internal_passthrough: {
    id: "internal_passthrough",
    run: buildInternalPassthroughResponse,
  },
  clarification: {
    id: "clarification",
    run: buildClarificationResponse,
  },
  unavailable: {
    id: "unavailable",
    run: buildUnavailableResponse,
  },
  general_response: {
    id: "general_response",
    run: buildGeneralResponse,
  },
};

export const getAciResponseTool = (tool = "") =>
  ACI_RESPONSE_TOOLS[tool] || ACI_RESPONSE_TOOLS.general_response;

export const runAciResponseTool = ({
  toolPlan = {},
  plan = {},
  runtimeData = {},
  context = {},
  userMessage = "",
  index = 0,
} = {}) => {
  const responseTool = getAciResponseTool(toolPlan.tool);

  return responseTool.run({
    toolPlan,
    plan,
    runtimeData,
    context,
    userMessage,
    index,
  });
};

export const buildAciAssistResponseFromPlan = ({
  plan = {},
  runtimeResults = {},
  context = {},
  userMessage = "",
} = {}) => {
  const tools = asArray(plan.tools);

  if (!tools.length) {
    return buildUnavailableResponse({
      toolPlan: { tool: "unavailable", filters: { unavailableReason: "unsupported_request" } },
      runtimeData: {
        unavailableReason: "unsupported_request",
      },
      context,
    });
  }

  const responses = tools.map((toolPlan, index) =>
    runAciResponseTool({
      toolPlan,
      plan,
      runtimeData: getRuntimeDataForTool({ runtimeResults, toolPlan, index }),
      context,
      userMessage,
      index,
    }),
  );

  if (responses.length === 1) {
    return {
      ...responses[0],
      mode: plan.mode || "single_tool",
      customerStage: plan.customerStage || responses[0].meta?.customerStage || "",
      conversationMode: plan.conversationMode || "",
    };
  }

  return buildMultiToolResponse({
    plan,
    responses,
    context,
    userMessage,
  });
};

export const buildMultiToolResponse = ({
  plan = {},
  responses = [],
  context = {},
  userMessage = "",
} = {}) => {
  const primary = responses[0] || {};
  const secondaryResponses = responses.slice(1);

  let mergedActions = normalizeActions([
    ...asArray(primary.actions),
    ...secondaryResponses.flatMap((response) => asArray(response.actions).slice(0, 2)),
  ]);

  const isCompoundResearchResult = Boolean(
    plan.contextPatch?.compoundRequest?.version || asArray(plan.tools).length > 2,
  );

  if (isCompoundResearchResult) {
    const openRelatedResults = secondaryResponses
      .map((response, secondaryIndex) => {
        const relatedToolPlan = asArray(plan.tools)[secondaryIndex + 1] || {};
        const model = firstMeaningful(
          asArray(response.data?.models)
            .map((item) => item.fullModel || item.model || item)
            .filter(Boolean)
            .join(" vs "),
          relatedToolPlan.entities?.primaryModel,
          relatedToolPlan.entities?.model,
          response.data?.model,
          response.data?.anchorFullModel,
          response.contextPatch?.selectedVehicle?.fullModel,
          response.data?.rows?.[0]?.fullModel,
          response.data?.rows?.[0]?.model,
          response.vehicle?.fullModel,
          response.vehicle?.model,
        );
        const intent = response.intent || "";
        const config = {
          vehicle_colors: {
            noun: "colours",
            query: `Show colours of ${model}`,
            canvasType: "color_studio_canvas",
          },
          vehicle_pricelist: {
            noun: "price list",
            query: `Show price list of ${model}`,
            canvasType: "pricelist_canvas",
          },
          vehicle_price_breakup: {
            noun: "price breakup",
            query: `Show on-road price breakup of ${model}`,
            canvasType: "price_breakup_canvas",
          },
          vehicle_price_history: {
            noun: "price history",
            query: `Show price history of ${model}`,
            canvasType: "price_history_canvas",
          },
          vehicle_emi: {
            noun: "EMI",
            query: `Calculate EMI for ${model}`,
            canvasType: "emi_calculator_canvas",
          },
          vehicle_emi_calculator: {
            noun: "EMI",
            query: `Calculate EMI for ${model}`,
            canvasType: "emi_calculator_canvas",
          },
          new_car_finance_faq: {
            noun: "finance guide",
            query: "Show car loan eligibility and documents",
            canvasType: "",
          },
          vehicle_spec_attribute_answer: {
            noun: response.data?.attributeLabel || response.data?.attributeKey || "specification",
            query: `Show ${response.data?.attributeLabel || response.data?.attributeKey || "specification"} of ${model}`,
            canvasType: "",
          },
          vehicle_feature_answer: {
            noun: response.data?.feature || "feature details",
            query: `Check ${response.data?.feature || "features"} in ${model}`,
            canvasType: "",
          },
          vehicle_feature_comparison: {
            noun: "feature comparison",
            query: `Compare requested features of ${model}`,
            canvasType: "feature_comparison_canvas",
          },
          vehicle_comparison: {
            noun: "comparison",
            query: response.actions?.[0]?.query || `Compare ${model}`,
            canvasType: "comparison_canvas",
          },
          vehicle_similar: {
            noun: "alternatives",
            query: `Show alternatives to ${model}`,
            canvasType: "recommendation_results_canvas",
          },
          vehicle_score_insight: {
            noun: "score insights",
            query: `Show score insights for ${model}`,
            canvasType: "score_insight_canvas",
          },
        }[intent];

        if (!config) return null;
        return {
          ...makeAction({
          id: `compound-open-${normalizeSearchKey(config.noun)}-${normalizeSearchKey(model)}`,
          label: `Open ${model || "car"} ${config.noun}`,
          query: config.query,
          intent,
          canvasType: config.canvasType,
          entities: { model },
          priority: 90,
          contextPatch: response.contextPatch || {},
          }),
          widget: response.widget || null,
          payload: response.widget
            ? { widget: response.widget, __fromBackend: true }
            : {},
        };
      })
      .filter(Boolean);

    mergedActions = normalizeActions(openRelatedResults, { max: 40 });
  }

  const mergedSuggestions = normalizeActions([
    ...asArray(primary.conversationSuggestions),
    ...secondaryResponses.flatMap((response) => asArray(response.conversationSuggestions).slice(0, 1)),
  ]);

  return {
    ...primary,
    mode: "multi_tool",
    displayMode: "both",
    title: primary.title || "ACI Assist result",
    answer: buildMultiToolAnswer({ plan, responses, userMessage }),
    actions: mergedActions,
    conversationSuggestions: mergedSuggestions,
    secondaryResponses,
    data: {
      ...(primary.data || {}),
      primary: primary.data || {},
      secondary: secondaryResponses.map((response) => ({
        intent: response.intent,
        canvasType: response.canvasType,
        inlineType: response.inlineType,
        title: response.title,
        answer: response.answer,
        data: response.data,
      })),
    },
    contextPatch: mergeContextPatches([
      primary.contextPatch,
      ...secondaryResponses.map((response) => response.contextPatch),
    ]),
    sourceTransparency: {
      ...(primary.sourceTransparency || {}),
      responseTool: "multi_tool",
      tools: responses.map((response) => response.meta?.responseTool || response.intent),
      modulesChecked: responses.flatMap((response) =>
        asArray(response.sourceTransparency?.modulesChecked),
      ),
    },
    meta: {
      ...(primary.meta || {}),
      responseTool: "multi_tool",
      toolCount: responses.length,
      plannerMode: plan.mode || "multi_tool",
    },
  };
};

export const buildMultiToolAnswer = ({ plan = {}, responses = [] } = {}) => {
  const tools = asArray(plan.tools).map((item) => item.tool);

  if (
    tools.includes("vehicle_feature_comparison") &&
    tools.includes("vehicle_colors") &&
    tools.includes("vehicle_pricelist")
  ) {
    const featureResponse = responses.find(
      (response) => response.intent === "vehicle_feature_comparison",
    );
    const modelNames = asArray(featureResponse?.data?.models)
      .map((model) => model.fullModel || model.model)
      .filter(Boolean);
    const featureCount = asArray(featureResponse?.data?.features).length;
    const cars = modelNames.length ? modelNames.join(" and ") : "both cars";

    return `I compared ${cars} on the ${featureCount || "requested"} features. I also checked each car's current model-level colours and ${getCity(plan.tools?.[0] || {}, {})} price list; use the related options to open either one without repeating your question.`;
  }

  if (
    tools.includes("vehicle_pricelist") &&
    tools.includes("vehicle_compare") &&
    tools.includes("vehicle_emi")
  ) {
    const primary = responses[0];
    const model = primary?.data?.model || "the selected car";

    return `I’ll handle this in parts: ${model} price, comparison, EMI estimate, and quotation/offer path. I’ll show the main result first, then you can open the other cards.`;
  }

  if (
    tools.includes("vehicle_emi") &&
    tools.includes("vehicle_feature_lookup")
  ) {
    const primary = responses[0];
    const model = primary?.data?.model || "the selected car";
    const variant = primary?.data?.variant || "";

    return `I’ll calculate EMI for ${cleanText(`${model} ${variant}`)} and also verify the requested feature.`;
  }

  return "I found multiple useful results for your question. I’ll show the most important result first and keep the related results available below.";
};

export const mergeContextPatches = (patches = []) => {
  const output = {};

  for (const patch of asArray(patches)) {
    if (!patch || typeof patch !== "object") continue;

    Object.assign(output, patch);

    if (patch.selectedVehicle) {
      output.selectedVehicle = {
        ...(output.selectedVehicle || {}),
        ...patch.selectedVehicle,
      };
    }

    if (patch.userPreferences) {
      output.userPreferences = {
        ...(output.userPreferences || {}),
        ...patch.userPreferences,
      };
    }

    if (patch.leadContext) {
      output.leadContext = {
        ...(output.leadContext || {}),
        ...patch.leadContext,
      };
    }
  }

  return output;
};

export const validateAciAssistResponseContract = (response = {}) => {
  const errors = [];

  if (!response || typeof response !== "object") {
    return {
      valid: false,
      errors: ["Response must be an object"],
    };
  }

  if (!response.intent) errors.push("Missing intent");
  if (!DISPLAY_MODES.includes(response.displayMode)) {
    errors.push(`Invalid displayMode: ${response.displayMode}`);
  }

  if (!("data" in response)) errors.push("Missing data");
  if (!Array.isArray(response.actions)) errors.push("actions must be an array");
  if (!Array.isArray(response.leadingQuestions)) {
    errors.push("leadingQuestions must be an array");
  }
  if (!Array.isArray(response.conversationSuggestions)) {
    errors.push("conversationSuggestions must be an array");
  }
  if (!response.contextPatch || typeof response.contextPatch !== "object") {
    errors.push("Missing contextPatch");
  }

  return {
    valid: errors.length === 0,
    errors,
  };
};

export default ACI_RESPONSE_TOOLS;
