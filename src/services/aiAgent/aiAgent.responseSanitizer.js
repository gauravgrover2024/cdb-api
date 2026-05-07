import { normalizeSearchKey } from "./aiAgent.planSchema.js";

const key = (value = "") => normalizeSearchKey(value || "");

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const firstMeaningful = (...values) =>
  values.find(
    (value) =>
      value !== undefined &&
      value !== null &&
      String(value).trim() !== "",
  ) || "";

const selectedVehicleFrom = (context = {}) =>
  context?.selectedVehicle ||
  context?.anchorVehicle ||
  context?.vehicle ||
  context?.history?.selectedVehicle ||
  {};

const firstToolFrom = (response = {}) =>
  response?.plan?.tools?.[0] ||
  response?.planner?.tools?.[0] ||
  response?.tools?.[0] ||
  {};

const displayName = (value = "") => {
  const text = String(value || "").replace(/\s+/g, " ").trim();
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

const getResponseModel = (response = {}, context = {}) => {
  const selected = selectedVehicleFrom(context);
  const tool = firstToolFrom(response);

  return displayName(
    firstMeaningful(
      selected?.model,
      context?.anchorModel,
      context?.model,
      response?.contextPatch?.selectedVehicle?.model,
      response?.contextPatch?.anchorModel,
      response?.entities?.model,
      response?.primaryModel,
      tool?.entities?.model,
      tool?.entities?.primaryModel,
      tool?.filters?.model,
    ),
  );
};

const getResponseVariant = (response = {}, context = {}) => {
  const selected = selectedVehicleFrom(context);
  const tool = firstToolFrom(response);

  return displayName(
    firstMeaningful(
      selected?.variant,
      context?.anchorVariant,
      context?.variant,
      response?.contextPatch?.selectedVehicle?.variant,
      response?.contextPatch?.anchorVariant,
      response?.entities?.variant,
      response?.primaryVariant,
      tool?.entities?.variant,
      tool?.entities?.primaryVariant,
      tool?.filters?.variant,
    ),
  );
};

const getResponseModelPreferResponse = (response = {}, context = {}) => {
  const selected = selectedVehicleFrom(context);
  const tool = firstToolFrom(response);

  return displayName(
    firstMeaningful(
      response?.entities?.model,
      response?.primaryModel,
      tool?.entities?.model,
      tool?.entities?.primaryModel,
      tool?.filters?.model,
      response?.contextPatch?.selectedVehicle?.model,
      response?.contextPatch?.anchorModel,
      selected?.model,
      context?.anchorModel,
      context?.model,
    ),
  );
};

const getResponseVariantPreferResponse = (response = {}, context = {}) => {
  const selected = selectedVehicleFrom(context);
  const tool = firstToolFrom(response);

  return displayName(
    firstMeaningful(
      response?.entities?.variant,
      response?.primaryVariant,
      tool?.entities?.variant,
      tool?.entities?.primaryVariant,
      tool?.filters?.variant,
      response?.contextPatch?.selectedVehicle?.variant,
      response?.contextPatch?.anchorVariant,
      selected?.variant,
      context?.anchorVariant,
      context?.variant,
    ),
  );
};

const labelOf = (item = {}) =>
  [
    item.id,
    item.label,
    item.title,
    item.subtitle,
    item.query,
    item.intent,
    item.leadType,
    item.type,
    item.canvasType,
  ]
    .filter(Boolean)
    .join(" ");

const isTestDriveItem = (item = {}) =>
  /\btest drive\b|\btest_drive\b|\bvehicle test drive\b/.test(key(labelOf(item)));

const isVehicleCta = (item = {}) =>
  /\bshow price\b|\bpricelist\b|\bcalculate emi\b|\bget quotation\b|\bget quote\b|\baci quotation\b|\bvehicle emi\b|\bvehicle_emi\b|\bvehicle pricelist\b/.test(
    key(labelOf(item)),
  );

const isAvailabilityItem = (item = {}) =>
  /\bfastest variant\b|\bfastest delivery\b|\bwaiting period\b|\bavailable now\b|\bavailability\b|\bin stock\b|\breserve\b/.test(
    key(labelOf(item)),
  );

const isBudgetQuestionAlreadyAnswered = (message = "") =>
  /\bunder\s+\d+|\bbelow\s+\d+|\bupto\s+\d+|\bup to\s+\d+/.test(key(message));

const isSuvAlreadyAnswered = (message = "") => /\bsuvs?\b/.test(key(message));
const isAutomaticAlreadyAnswered = (message = "") => /\bautomatic\b/.test(key(message));

const isFeatureAlreadyAnswered = (message = "") =>
  /\bsunroof\b|\bairbags?\b|\badas\b|\b360 camera\b|\bventilated\b/.test(key(message));

const isInternalIntent = (response = {}) => {
  const blob = key(
    [
      response.intent,
      response.canvasType,
      response.inlineType,
      response.answer,
      response.title,
    ]
      .filter(Boolean)
      .join(" "),
  );

  return /\bloan\b|\breceivable\b|\bpayment\b|\bcustomer 360\b|\bdelivery order\b|\binternal\b/.test(
    blob,
  );
};

const makeAction = ({
  id,
  label,
  type = "ask",
  query = "",
  canvasType = "",
  intent = "",
  leadType = "",
}) => ({
  id,
  label,
  type,
  query,
  canvasType,
  leadType,
  route: "",
  intent,
  entities: {},
  contextPatch: {},
  icon: "",
  tone: "",
});

const dedupeList = (items = []) => {
  const seen = new Set();
  const out = [];

  for (const item of asArray(items)) {
    if (!item || typeof item !== "object") continue;

    const dedupeKey = key(
      `${item.label || ""} ${item.title || ""} ${item.query || ""} ${item.intent || ""} ${item.canvasType || ""}`,
    );

    if (!dedupeKey || seen.has(dedupeKey)) continue;

    seen.add(dedupeKey);
    out.push({
      ...item,
      id: item.id || dedupeKey,
    });
  }

  return out;
};

const sanitizeList = (
  items = [],
  {
    removeVehicleCtas = false,
    removeAvailability = false,
    max = 5,
  } = {},
) => {
  const cleanItems = [];

  for (const item of dedupeList(items)) {
    if (isTestDriveItem(item)) continue;
    if (removeVehicleCtas && isVehicleCta(item)) continue;
    if (removeAvailability && isAvailabilityItem(item)) continue;

    cleanItems.push(item);
    if (cleanItems.length >= max) break;
  }

  return cleanItems;
};

const extractColorFromMessage = (message = "") => {
  const match = String(message || "").match(
    /\b(black|white|red|blue|grey|gray|silver|brown|green|orange|yellow|gold|pearl|titan|dark|cosmic|atlas|abyss|midnight)\b/i,
  );

  return match?.[1]?.toLowerCase() || "";
};

const wantsAutomaticFromMessage = (message = "") =>
  /\bautomatic|auto|ivt|cvt|dct|amt|at\b/i.test(message);

const variantHasSpecificAutomaticGearbox = (variant = "") =>
  /\bivt\b|\bcvt\b|\bdct\b|\bamt\b|\bat\b/i.test(variant);

const variantLooksLikeLooseAutomaticPreference = (variant = "") =>
  /\bautomatic\b|\bauto\b/i.test(variant) && !variantHasSpecificAutomaticGearbox(variant);

const cleanLooseAutomaticVariant = (variant = "") =>
  displayName(String(variant || "").replace(/\bautomatic\b|\bauto\b/gi, "").replace(/\s+/g, " ").trim());

const extractFeatureFromMessage = (message = "") =>
  String(message || "").match(
    /\b(sunroof|ADAS|airbags?|360 camera|camera|mileage|boot space|ground clearance)\b/i,
  )?.[1] || "this feature";

const extractCompareTarget = (message = "") => {
  const raw = String(message || "");

  const direct =
    raw.match(/\bcompare\s+with\s+([^,.;]+)/i)?.[1] ||
    raw.match(/\b(?:with|vs|versus)\s+([^,.;]+)/i)?.[1] ||
    raw.match(/\bcompare\s+.+?\s+(?:and|with|vs|versus)\s+([^,.;]+)/i)?.[1] ||
    "";

  const cleaned = direct
    .replace(/\btell\b.*$/i, "")
    .replace(/\bemi\b.*$/i, "")
    .replace(/\boffers?\b.*$/i, "")
    .replace(/\bprice\b.*$/i, "")
    .trim();

  return displayName(cleaned);
};

const patchFeatureMatchResponse = (response = {}, message = "") => {
  if (response.intent !== "vehicle_must_have_feature_builder") return response;

  const actions = [
    makeAction({
      id: "feature-match-compare-top",
      label: "Compare top matches",
      query: "Compare top matching cars",
      canvasType: "comparison_canvas",
      intent: "vehicle_comparison",
    }),
    makeAction({
      id: "feature-match-lowest-emi",
      label: "Lowest EMI option",
      query: "Show lowest EMI option from these matches",
      canvasType: "emi_calculator_canvas",
      intent: "vehicle_emi_calculator",
    }),
    makeAction({
      id: "feature-match-safest",
      label: "Safest option",
      query: "Show safest option from these matches",
      canvasType: "safety_advisor_canvas",
      intent: "vehicle_safety_search",
    }),
    makeAction({
      id: "feature-match-quote",
      label: "Get quote",
      type: "lead",
      query: "Get quotation for my preferred match",
      canvasType: "aci_quotation_canvas",
      intent: "aci_new_car_quotation",
      leadType: "quotation",
    }),
  ];

  const leadingQuestions = [];

  if (!isBudgetQuestionAlreadyAnswered(message)) {
    leadingQuestions.push({
      id: "feature-match-budget",
      label: "Budget?",
      query: "What is your budget?",
      intent: "vehicle_must_have_feature_builder",
      displayMode: "canvas",
      canvasType: "feature_match_builder_canvas",
      entities: {},
      contextPatch: {},
    });
  }

  if (!isSuvAlreadyAnswered(message)) {
    leadingQuestions.push({
      id: "feature-match-body",
      label: "SUV only?",
      query: "Do you want SUV only?",
      intent: "vehicle_must_have_feature_builder",
      displayMode: "canvas",
      canvasType: "feature_match_builder_canvas",
      entities: {},
      contextPatch: {},
    });
  }

  if (!isAutomaticAlreadyAnswered(message)) {
    leadingQuestions.push({
      id: "feature-match-transmission",
      label: "Automatic only?",
      query: "Do you want automatic only?",
      intent: "vehicle_must_have_feature_builder",
      displayMode: "canvas",
      canvasType: "feature_match_builder_canvas",
      entities: {},
      contextPatch: {},
    });
  }

  if (!isFeatureAlreadyAnswered(message)) {
    leadingQuestions.push({
      id: "feature-match-feature",
      label: "Must-have features?",
      query: "Any must-have features?",
      intent: "vehicle_must_have_feature_builder",
      displayMode: "canvas",
      canvasType: "feature_match_builder_canvas",
      entities: {},
      contextPatch: {},
    });
  }

  return {
    ...response,
    answer:
      "I found matching cars for your filters. Since you already gave budget/body type/features, I’ll show the best matches first instead of asking the same questions again.",
    actions,
    leadingQuestions: leadingQuestions.slice(0, 1),
    conversationSuggestions: [],
  };
};

const patchFuelDecisionResponse = (response = {}, message = "") => {
  if (!/\b(cng|petrol|diesel|hybrid|ev)\b/i.test(message)) return response;
  if (!/\bwhich is better|daily|running|km|fuel\b/i.test(message)) return response;

  return {
    ...response,
    intent: "vehicle_fuel_decision_advisor",
    displayMode: "inline",
    canvasType: "fuel_decision_canvas",
    inlineType: "explainer_card",
    answer:
      "For daily running like 50 km/day, CNG usually wins on running cost. Petrol is simpler, smoother, easier to maintain, and keeps boot space/practicality better. If your running is consistently high and CNG stations are convenient, CNG is the practical pick; otherwise petrol is safer overall.",
    actions: [
      makeAction({
        id: "fuel-show-cng-cars",
        label: "Show CNG cars",
        query: "Show CNG cars for daily running",
        canvasType: "recommendation_results_canvas",
        intent: "vehicle_fuel_decision_advisor",
      }),
      makeAction({
        id: "fuel-show-petrol-cars",
        label: "Show petrol cars",
        query: "Show petrol cars for daily running",
        canvasType: "recommendation_results_canvas",
        intent: "vehicle_fuel_decision_advisor",
      }),
      makeAction({
        id: "fuel-compare-running-cost",
        label: "Compare running cost",
        query: "Compare CNG and petrol running cost for 50 km daily",
        canvasType: "fuel_decision_canvas",
        intent: "vehicle_fuel_decision_advisor",
      }),
    ],
    leadingQuestions: [
      {
        id: "fuel-monthly-running",
        label: "Monthly running?",
        query: "My monthly running is around 1500 km",
        intent: "vehicle_fuel_decision_advisor",
        displayMode: "inline",
        canvasType: "fuel_decision_canvas",
        entities: {},
        contextPatch: {},
      },
    ],
    conversationSuggestions: [],
    widgets: [],
  };
};

const patchBestPriceColorQuoteResponse = (response = {}, message = "", context = {}) => {
  if (!/\bbest price|final price|quote|quotation\b/i.test(message)) return response;
  if (!/\bblack|white|red|blue|grey|gray|silver|brown|green|orange|yellow|gold|pearl|titan|dark|cosmic|atlas|abyss|midnight\b/i.test(message)) return response;

  const model = getResponseModel(response, context) || "this car";
  const variant = getResponseVariant(response, context);
  const color = extractColorFromMessage(message) || "selected colour";

  const looseBaseVariant = cleanLooseAutomaticVariant(variant);
  const needsExactAutoVariant =
    wantsAutomaticFromMessage(message) &&
    (!variant || variantLooksLikeLooseAutomaticPreference(variant));

  const exactVariantAction = needsExactAutoVariant
    ? [
        makeAction({
          id: "quote-choose-exact-automatic-variant",
          label: "Choose exact automatic variant",
          query: `Show automatic variants of ${model}${looseBaseVariant ? ` ${looseBaseVariant}` : ""}`,
          canvasType: "pricelist_canvas",
          intent: "vehicle_pricelist",
        }),
      ]
    : [];

  const cleanedActions = sanitizeList(response.actions || [], { max: 3 }).filter(
    (item) => !/\bshare your budget\b/i.test(item.label || item.title || ""),
  );

  return {
    ...response,
    intent: "aci_new_car_quotation",
    displayMode: "canvas",
    canvasType: "aci_quotation_canvas",
    answer:
      `I can start the ACI quotation request for ${model} in ${color}.` +
      (needsExactAutoVariant
        ? " I understood automatic as a preference, so I’ll confirm the exact automatic variant before final quote."
        : "") +
      " Final best price and exact colour availability will depend on city, variant, stock, finance/exchange, and dealer confirmation.",
    actions: dedupeList([...exactVariantAction, ...cleanedActions]).slice(0, 4),
    leadingQuestions: dedupeList([
      ...exactVariantAction,
      ...sanitizeList(response.leadingQuestions || [], { max: 2 }),
    ]).slice(0, 1),
    conversationSuggestions: sanitizeList(response.conversationSuggestions || [], { max: 3 }).filter(
      (item) => !/\bshare your budget\b/i.test(item.title || item.label || ""),
    ),
  };
};


const patchLooseAutomaticQuoteConfirmation = (response = {}, message = "", context = {}) => {
  const isQuoteRequest = /\bbest price|final price|quote|quotation\b/i.test(message);
  const hasLooseAutomatic = /\bautomatic\b|\bauto\b/i.test(message);
  const hasSpecificGearbox = /\bivt\b|\bcvt\b|\bdct\b|\bamt\b|\bat\b/i.test(message);

  if (!isQuoteRequest || !hasLooseAutomatic || hasSpecificGearbox) return response;

  const model = getResponseModelPreferResponse(response, context) || getResponseModel(response, context) || "this car";
  const responseVariant = getResponseVariantPreferResponse(response, context);
  const contextVariant = getResponseVariant(response, context);
  const variant = cleanLooseAutomaticVariant(responseVariant || contextVariant);

  const alreadyHasAnswerLine = String(response.answer || "")
    .toLowerCase()
    .includes("exact automatic variant");

  const exactVariantAction = makeAction({
    id: "quote-choose-exact-automatic-variant",
    label: "Choose exact automatic variant",
    query: `Show automatic variants of ${model}${variant ? ` ${variant}` : ""}`,
    canvasType: "pricelist_canvas",
    intent: "vehicle_pricelist",
  });

  const removeLooseAutoItems = (items = []) =>
    asArray(items).filter((item) => {
      const blob = key(labelOf(item));
      return !/sx automatic|automatic/.test(blob) || /choose exact automatic variant/.test(blob);
    });

  return {
    ...response,
    answer: alreadyHasAnswerLine
      ? response.answer
      : `${response.answer || `I can start the ACI quotation request for ${model}.`} I understood automatic as a preference, so I’ll confirm the exact automatic variant before final quote.`,
    actions: dedupeList([exactVariantAction, ...removeLooseAutoItems(response.actions || [])]).slice(0, 4),
    leadingQuestions: dedupeList([exactVariantAction]).slice(0, 1),
    conversationSuggestions: removeLooseAutoItems(response.conversationSuggestions || []),
  };
};

const patchMultiIntentResponse = (response = {}, message = "", context = {}) => {
  const text = key(message);

  const isMulti =
    /\bprice\b|\bpricelist\b|\bon road\b/.test(text) &&
    /\bcompare\b|\b vs \b|\bversus\b/.test(text) &&
    /\bemi\b|\bloan\b/.test(text) &&
    /\boffer|offers|discount|scheme\b/.test(text);

  if (!isMulti) return response;

  const model = getResponseModel(response, context) || "selected car";
  const compareTarget = extractCompareTarget(message) || "the comparison car";

  return {
    ...response,
    intent: "multi_intent_new_car",
    displayMode: "both",
    canvasType: "pricelist_canvas",
    inlineType: "",
    answer:
      `I’ll handle this in parts: ${model} price in Delhi, comparison with ${compareTarget}, 5-year EMI estimate, and offer/quotation path. I’ll show price first, then let you open comparison, EMI, or quotation.`,
    actions: [
      makeAction({
        id: "multi-open-pricelist",
        label: "Open price list",
        query: `Show ${model} price list in Delhi`,
        canvasType: "pricelist_canvas",
        intent: "vehicle_pricelist",
      }),
      makeAction({
        id: "multi-open-comparison",
        label: "Open comparison",
        query: `Compare ${model} with ${compareTarget}`,
        canvasType: "comparison_canvas",
        intent: "vehicle_comparison",
      }),
      makeAction({
        id: "multi-open-emi",
        label: "Calculate EMI",
        query: `Calculate EMI for ${model} for 5 years`,
        canvasType: "emi_calculator_canvas",
        intent: "vehicle_emi_calculator",
      }),
      makeAction({
        id: "multi-get-quote",
        label: "Get ACI quotation",
        type: "lead",
        query: `Get quotation for ${model}`,
        canvasType: "aci_quotation_canvas",
        intent: "aci_new_car_quotation",
        leadType: "quotation",
      }),
    ],
    leadingQuestions: [
      {
        id: "multi-choose-first",
        label: "Finalize variant first?",
        query: `Help me choose the right ${model} variant first`,
        intent: "vehicle_variant_recommendation",
        displayMode: "canvas",
        canvasType: "variant_finder_canvas",
        entities: {},
        contextPatch: {},
      },
    ],
    conversationSuggestions: [],
  };
};

const patchCompanyNameResponse = (response = {}, message = "") => {
  if (!/\bcompany name|company buy|business name|firm name\b/i.test(message)) return response;

  return {
    ...response,
    intent: "new_car_finance_faq",
    displayMode: "inline",
    canvasType: "",
    inlineType: "finance_faq_card",
    answer:
      "Yes, a car can usually be purchased in a company name, subject to dealer billing, KYC, GST/company documents, board/authorization requirements, and bank finance policy if a loan is needed. For an exact quotation, ACI should capture company name, city, model/variant, finance requirement, and GST/KYC details.",
    actions: [],
    leadingQuestions: [],
    conversationSuggestions: [],
    widgets: [],
  };
};

const patchFeatureAnswerWording = (response = {}, message = "", context = {}) => {
  if (!/\bdoes\b|\bhave\b|\bhas\b|\bgets\b/i.test(message)) return response;
  if (!/\bsunroof|adas|airbags|camera|mileage|boot|ground clearance\b/i.test(message)) return response;
  if (response.inlineType !== "feature_answer_card") return response;

  const feature = extractFeatureFromMessage(message);
  const model = getResponseModelPreferResponse(response, context) || "this car";
  const variant = getResponseVariantPreferResponse(response, context);

  const widgets = asArray(response.widgets || response.cards || response.canvases);
  const recordCount = widgets.reduce((sum, widget) => {
    if (Array.isArray(widget?.records)) return sum + widget.records.length;
    if (Array.isArray(widget?.items)) return sum + widget.items.length;
    if (Array.isArray(widget?.rows)) return sum + widget.rows.length;
    return sum;
  }, 0);

  const hasMatchingRecords =
    recordCount > 0 ||
    /matched/i.test(JSON.stringify(response.sourceTransparency || {}));

  const prefix = hasMatchingRecords ? "Yes —" : "I found matching records —";

  return {
    ...response,
    answer:
      variant
        ? `${prefix} ${model} ${variant} appears in the matching ${feature} feature records. Please confirm exact fuel/transmission sub-variant in the feature card, because features can differ within the same trim family.`
        : `${prefix} ${model} appears in the matching ${feature} feature records. Please confirm the exact variant in the feature card, because features can differ by trim.`,
  };
};

const patchContextCompareResponse = (response = {}, message = "", context = {}) => {
  if (!/\bcompare with\b/i.test(message)) return response;

  const baseModel = getResponseModel(response, context);
  const baseVariant = getResponseVariant(response, context);
  const target = extractCompareTarget(message) || "the requested car";

  if (!baseModel) return response;

  return {
    ...response,
    intent: "vehicle_comparison",
    displayMode: "both",
    canvasType: "comparison_canvas",
    inlineType: "",
    answer:
      `I’ll compare your selected ${baseModel}${baseVariant ? ` ${baseVariant}` : ""} with ${target}. I’ll use representative/popular variants for the other car unless you choose a specific variant.`,
    actions: [
      makeAction({
        id: "context-compare-change-variants",
        label: "Change variants",
        query: `Change variants for ${baseModel} vs ${target}`,
        canvasType: "comparison_canvas",
        intent: "vehicle_comparison",
      }),
      makeAction({
        id: "context-compare-emi",
        label: "Check EMI difference",
        query: `Compare EMI of ${baseModel} and ${target}`,
        canvasType: "emi_calculator_canvas",
        intent: "vehicle_emi_calculator",
      }),
      makeAction({
        id: "context-compare-price",
        label: "Show price difference",
        query: `Compare price of ${baseModel} and ${target}`,
        canvasType: "comparison_canvas",
        intent: "vehicle_comparison",
      }),
    ],
    leadingQuestions: [],
    conversationSuggestions: [],
    widgets: [],
  };
};

const patchBlackAvailableResponse = (response = {}, message = "", context = {}) => {
  if (!/\bblack\b/i.test(message)) return response;
  if (!/\bavailable|availability|in stock|reserve|\?$/.test(key(message))) return response;

  const model = getResponseModel(response, context);
  const variant = getResponseVariant(response, context);

  return {
    ...response,
    intent: "vehicle_color_gallery",
    displayMode: "inline",
    canvasType: "",
    inlineType: "unavailable_notice",
    answer:
      model
        ? `I can show model-level colours for ${model}, but exact ${variant ? `${variant} ` : ""}black stock/variant-wise colour availability is not available yet.`
        : "I can show model-level colours, but exact black stock/variant-wise colour availability is not available yet.",
    actions: [
      makeAction({
        id: "black-show-model-colours",
        label: "Show model colours",
        query: model ? `Show colors of ${model}` : "Show colors",
        canvasType: "color_studio_canvas",
        intent: "vehicle_color_gallery",
      }),
      makeAction({
        id: "black-get-quote",
        label: "Get quote",
        type: "lead",
        query: model ? `Get quotation for ${model} black` : "Get quotation for black colour",
        canvasType: "aci_quotation_canvas",
        intent: "aci_new_car_quotation",
        leadType: "quotation",
      }),
      makeAction({
        id: "black-callback",
        label: "Request callback",
        type: "lead",
        query: model ? `Request callback for ${model} black availability` : "Request callback for black availability",
        intent: "aci_new_car_quotation",
        leadType: "callback",
      }),
    ],
    leadingQuestions: [],
    conversationSuggestions: [],
    widgets: [
      {
        type: "unavailable_notice",
        title: "Variant-wise colour availability unavailable",
      },
    ],
  };
};

const isHeavyMultiIntentMessage = (message = "") => {
  const text = key(message);

  return (
    (/\bprice\b|\bpricelist\b|\bon road\b|\bon-road\b/.test(text)) &&
    (/\bcompare\b|\b vs \b|\bversus\b/.test(text)) &&
    (/\bemi\b|\bloan\b/.test(text)) &&
    (/\boffer|offers|discount|scheme\b/.test(text))
  );
};

export const sanitizeAiAgentResponse = (response = {}, { message = "", context = {} } = {}) => {
  if (!response || typeof response !== "object") return response;

  let next = { ...response };

  next = patchFeatureMatchResponse(next, message);
  next = patchFuelDecisionResponse(next, message);
  next = patchBestPriceColorQuoteResponse(next, message, context);
  next = patchLooseAutomaticQuoteConfirmation(next, message, context);
  next = patchCompanyNameResponse(next, message);
  next = patchFeatureAnswerWording(next, message, context);
  if (!isHeavyMultiIntentMessage(message)) {
    next = patchContextCompareResponse(next, message, context);
  }
  next = patchMultiIntentResponse(next, message, context);
  next = patchBlackAvailableResponse(next, message, context);

  const removeVehicleCtas = isInternalIntent(next);
  const removeAvailability =
    next.inlineType === "unavailable_notice" ||
    next.intent === "vehicle_color_gallery";

  next.actions = sanitizeList(next.actions || [], {
    removeVehicleCtas,
    removeAvailability,
    max: 5,
  });

  next.leadingQuestions = sanitizeList(next.leadingQuestions || [], {
    removeVehicleCtas,
    removeAvailability,
    max: 4,
  });

  next.conversationSuggestions = sanitizeList(next.conversationSuggestions || [], {
    removeVehicleCtas,
    removeAvailability,
    max: 5,
  });

  next.suggestions = sanitizeList(next.suggestions || [], {
    removeVehicleCtas,
    removeAvailability,
    max: 5,
  });

  next.followUpSuggestions = sanitizeList(next.followUpSuggestions || [], {
    removeVehicleCtas,
    removeAvailability,
    max: 5,
  });

  next.salesNudges = sanitizeList(next.salesNudges || [], {
    removeVehicleCtas,
    removeAvailability,
    max: 5,
  });

  next.closingActions = sanitizeList(next.closingActions || [], {
    removeVehicleCtas,
    removeAvailability,
    max: 5,
  });

  if (removeVehicleCtas) {
    next.actions = [];
    next.leadingQuestions = [];
    next.conversationSuggestions = [];
    next.suggestions = [];
    next.followUpSuggestions = [];
    next.salesNudges = [];
    next.closingActions = [];
  }

  return next;
};

export default sanitizeAiAgentResponse;
