const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const unique = (items = []) => [...new Set(items.filter(Boolean))];

const STAGE_RANK = {
  discovery: 0,
  research: 1,
  evaluation: 2,
  decision: 3,
  enquiry_ready: 4,
  enquiry: 5,
};

const isLeadAction = (action = {}) =>
  action.type === "lead" ||
  /\b(lead|quotation|quote|callback|enquiry)\b/i.test(
    [
      action.intent,
      action.leadType,
      action.label,
      action.title,
      action.query,
      action.canvasType,
    ]
      .filter(Boolean)
      .join(" "),
  );

const makeAction = ({ id, label, query, intent, canvasType = "" }) => ({
  id,
  label,
  type: "ask",
  query,
  intent,
  canvasType,
  entities: {},
  contextPatch: {},
});

const getVehicle = (response = {}, context = {}) =>
  response.contextPatch?.selectedVehicle ||
  response.vehicle ||
  context.selectedVehicle ||
  {};

const getComparisonVehicles = (response = {}, context = {}) => {
  const vehicles = [
    ...asArray(
      response.contextPatch?.activeComparison?.vehicles ||
        response.contextPatch?.selectedComparisonSet?.vehicles ||
        context.activeComparison?.vehicles ||
        context.selectedComparisonSet?.vehicles,
    ),
    ...asArray(response.contextPatch?.compoundRequest?.models).map((model) => ({
      model,
      fullModel: model,
    })),
  ];
  const seen = new Set();

  return vehicles.filter((vehicle) => {
    const key = cleanText(vehicle.fullModel || vehicle.model).toLowerCase();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const getVehicleLabel = (vehicle = {}) =>
  cleanText(
    vehicle.fullModel ||
      vehicle.displayName ||
      [vehicle.make || vehicle.brand, vehicle.model].filter(Boolean).join(" ") ||
      vehicle.model,
  );

const detectTopics = ({ message = "", response = {} } = {}) => {
  const text = `${message} ${response.intent || ""} ${response.canvasType || ""}`.toLowerCase();
  const topics = [];

  if (/\b(feature|sunroof|adas|airbags?|abs|ebd|camera|tpms|charging)\b/.test(text)) topics.push("features");
  if (/\b(colors?|colours?|paint|shade options?)\b/.test(text)) topics.push("colors");
  if (/\b(prices?|pricelist|on.?road|ex.?showroom|breakup)\b/.test(text)) topics.push("price");
  if (/\b(mileage|range|boot space|ground clearance|dimensions|power|torque|wheelbase)\b/.test(text)) topics.push("specifications");
  if (/\b(compare|comparison|versus|\bvs\b|difference)\b/.test(text)) topics.push("comparison");
  if (/\b(variant|trim|automatic|manual|petrol|diesel|cng|hybrid|electric)\b/.test(text)) topics.push("variant");
  if (/\b(emi|loan|finance|down.?payment|tenure)\b/.test(text)) topics.push("finance");
  if (/\b(recommend|best|better|which.*buy|verdict|choose|final)\b/.test(text)) topics.push("decision");
  if (/\b(quote|quotation|callback|enquiry|contact me|buy|book)\b/.test(text)) topics.push("enquiry");

  return unique(topics);
};

const inferTurnStage = ({ topics = [], message = "", response = {} } = {}) => {
  const text = `${message} ${response.intent || ""}`;
  if (/\b(quote|quotation|callback|enquiry|contact me|proceed|buy now)\b/i.test(text)) return "enquiry";
  if (topics.includes("decision")) return "decision";
  if (
    topics.includes("comparison") ||
    topics.includes("variant") ||
    topics.includes("finance") ||
    (topics.includes("price") && topics.includes("features"))
  ) return "evaluation";
  if (topics.length) return "research";
  return "discovery";
};

const maxStage = (left = "discovery", right = "discovery") =>
  (STAGE_RANK[right] || 0) > (STAGE_RANK[left] || 0) ? right : left;

const buildReadiness = ({
  previous = {},
  topics = [],
  vehicle = {},
  comparisonVehicles = [],
  message = "",
  turnStage = "discovery",
} = {}) => {
  const previousTopics = asArray(previous.exploredTopics);
  const exploredTopics = unique([...previousTopics, ...topics]);
  let score = 0;

  if (vehicle.model || comparisonVehicles.length) score += 14;
  if (vehicle.variant || vehicle.variantName || vehicle.selectedVariant) score += 16;
  if (comparisonVehicles.length >= 2 || exploredTopics.includes("comparison")) score += 12;
  if (exploredTopics.includes("features")) score += 8;
  if (exploredTopics.includes("specifications")) score += 6;
  if (exploredTopics.includes("colors")) score += 4;
  if (exploredTopics.includes("price")) score += 12;
  if (exploredTopics.includes("variant")) score += 12;
  if (exploredTopics.includes("finance")) score += 10;
  if (exploredTopics.includes("decision")) score += 14;
  if (/\b(budget|under|below|up to|upto|lakh|crore)\b/i.test(message)) score += 6;
  if (turnStage === "enquiry") score = 100;

  return {
    score: Math.min(100, Math.max(Number(previous.readinessScore || 0), score)),
    exploredTopics,
  };
};

const pickNextBestAction = ({
  stage = "discovery",
  readinessScore = 0,
  exploredTopics = [],
  vehicle = {},
  comparisonVehicles = [],
} = {}) => {
  const model = getVehicleLabel(vehicle);
  const comparisonLabels = comparisonVehicles.map(getVehicleLabel).filter(Boolean).slice(0, 2);
  const comparisonText = comparisonLabels.join(" and ");
  const comparisonQuery = comparisonLabels.join(" vs ");

  if (stage === "enquiry" || stage === "enquiry_ready") {
    return {
      ...makeAction({
        id: "journey-exact-quotation",
        label: "Get exact quotation",
        query: model ? `Get exact quotation for ${model}` : "Get an exact quotation",
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
      }),
      type: "lead",
      leadType: "quotation",
    };
  }

  if (comparisonLabels.length >= 2) {
    if (!exploredTopics.includes("variant")) {
      return makeAction({
        id: "journey-match-variants",
        label: "Compare equivalent variants",
        query: `Compare ${comparisonQuery}`,
        intent: "vehicle_comparison",
        canvasType: "comparison_canvas",
      });
    }
    if (!exploredTopics.includes("decision")) {
      return makeAction({
        id: "journey-narrow-comparison",
        label: "Narrow the better fit",
        query: `Which of ${comparisonText} suits my usage better?`,
        intent: "vehicle_comparison",
        canvasType: "comparison_canvas",
      });
    }
  }

  if (!model) {
    return makeAction({
      id: "journey-pick-car",
      label: "Find cars for me",
      query: "Help me shortlist cars for my budget and usage",
      intent: "vehicle_recommendation",
      canvasType: "recommendation_results_canvas",
    });
  }

  if (!exploredTopics.includes("features")) {
    return makeAction({
      id: "journey-check-features",
      label: "Check useful features",
      query: `Which ${model} features genuinely matter while choosing a variant?`,
      intent: "vehicle_model_features_explorer",
      canvasType: "features_explorer_canvas",
    });
  }

  if (!exploredTopics.includes("variant")) {
    return makeAction({
      id: "journey-choose-variant",
      label: "Choose the right variant",
      query: `Which ${model} variant is the best fit for my usage?`,
      intent: "vehicle_variant_recommendation",
      canvasType: "variant_finder_canvas",
    });
  }

  if (!exploredTopics.includes("price")) {
    return makeAction({
      id: "journey-check-price",
      label: "Check current prices",
      query: `Show the current ${model} price list in my city`,
      intent: "vehicle_pricelist",
      canvasType: "pricelist_canvas",
    });
  }

  if (!exploredTopics.includes("finance")) {
    return makeAction({
      id: "journey-check-emi",
      label: "See a realistic EMI",
      query: `Calculate a realistic EMI for ${model}`,
      intent: "vehicle_emi_calculator",
      canvasType: "emi_calculator_canvas",
    });
  }

  if (readinessScore >= 65) {
    return {
      ...makeAction({
        id: "journey-exact-quotation",
        label: "Get exact quotation",
        query: `Get exact quotation for ${model}`,
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
      }),
      type: "lead",
      leadType: "quotation",
    };
  }

  return makeAction({
    id: "journey-decision-check",
    label: "Check if it fits me",
    query: `Is ${model} the right choice for my usage and priorities?`,
    intent: "vehicle_recommendation",
    canvasType: "recommendation_results_canvas",
  });
};

const dedupeActions = (items = [], max = 5) => {
  const seen = new Set();
  const output = [];
  for (const item of items.filter(Boolean)) {
    const key = cleanText(
      item.query || `${item.intent || ""} ${item.label || ""}`,
    ).toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(item);
    if (output.length >= max) break;
  }
  return output;
};

export const applyAciCustomerJourneyGuidance = ({
  response = {},
  message = "",
  context = {},
} = {}) => {
  if (!response || typeof response !== "object") return response;
  if (response.intent === "internal_passthrough") return response;

  const previous = context.customerJourney || {};
  const topics = detectTopics({ message, response });
  const vehicle = getVehicle(response, context);
  const comparisonVehicles = getComparisonVehicles(response, context);
  const turnStage = inferTurnStage({ topics, message, response });
  const readiness = buildReadiness({
    previous,
    topics,
    vehicle,
    comparisonVehicles,
    message,
    turnStage,
  });

  let stage = maxStage(previous.stage, turnStage);
  if (turnStage !== "enquiry" && readiness.score >= 72 && STAGE_RANK[stage] >= STAGE_RANK.decision) {
    stage = "enquiry_ready";
  }

  const leadMode =
    turnStage === "enquiry"
      ? "explicit"
      : stage === "enquiry_ready"
        ? "ready"
        : readiness.score >= 58
          ? "soft"
          : "hidden";
  const nextBestAction = pickNextBestAction({
    stage,
    readinessScore: readiness.score,
    exploredTopics: readiness.exploredTopics,
    vehicle,
    comparisonVehicles,
  });

  const existingActions = asArray(response.actions);
  const existingQuestions = asArray(response.leadingQuestions);
  const visibleActions = existingActions.filter((action) => !isLeadAction(action));
  const visibleQuestions = existingQuestions.filter((action) => !isLeadAction(action));
  const journey = {
    version: "aci_customer_journey_v1",
    stage,
    turnStage,
    readinessScore: readiness.score,
    leadMode,
    turnCount: Number(previous.turnCount || 0) + 1,
    exploredTopics: readiness.exploredTopics,
    selectedModel: vehicle.model || previous.selectedModel || "",
    selectedVariant:
      vehicle.variant ||
      vehicle.variantName ||
      vehicle.selectedVariant ||
      previous.selectedVariant ||
      "",
    comparisonCount: Math.max(
      Number(previous.comparisonCount || 0),
      comparisonVehicles.length,
    ),
    nextBestQuestion: nextBestAction?.query || "",
    rationale:
      leadMode === "hidden"
        ? "Continue useful research without showing a lead prompt."
        : leadMode === "soft"
          ? "The buyer is narrowing choices; keep enquiry optional."
          : "The buyer has enough decision context for a calm enquiry handoff.",
  };

  const compoundRequest = response.contextPatch?.compoundRequest || {};
  const actionLimit = compoundRequest.version
    ? Math.min(40, Number(compoundRequest.toolCount || 0) + 1)
    : 5;
  response.actions = dedupeActions(
    [nextBestAction, ...visibleActions],
    actionLimit,
  );
  response.leadingQuestions = dedupeActions(
    [nextBestAction, ...visibleQuestions],
    3,
  );
  response.conversationSuggestions = response.leadingQuestions;
  response.journeyGuidance = {
    ...journey,
    nextBestAction,
  };
  response.contextPatch = {
    ...(response.contextPatch || {}),
    customerStage: stage,
    customerJourney: journey,
    leadContext: {
      ...(response.contextPatch?.leadContext || context.leadContext || {}),
      readinessScore: readiness.score,
      leadMode,
    },
  };

  if (
    compoundRequest.version &&
    asArray(response.secondaryResponses).length &&
    !/related options/i.test(String(response.answer || ""))
  ) {
    const capabilityLabels = {
      features: "features",
      specifications: "specifications",
      comparison: "comparison",
      colors: "colours",
      prices: "price lists",
      priceBreakup: "on-road breakups",
      priceHistory: "price history",
      variants: "variants",
      emi: "EMI",
      finance: "loan eligibility and documents",
      similar: "alternatives",
      score: "score insights",
    };
    const covered = asArray(compoundRequest.requestedCapabilities)
      .map((capability) => capabilityLabels[capability] || capability)
      .filter(Boolean);
    const coverageText = covered.length
      ? covered.join(", ").replace(/, ([^,]*)$/, " and $1")
      : "the related parts";
    const modelCount = Number(compoundRequest.modelCount || 0);
    const coverageNote = modelCount > 1
      ? `I’ve also checked ${coverageText} for each car, and kept every part in the related options so you can compare them without repeating yourself.`
      : `I’ve also kept ${coverageText} in the related options, so you can move through every part without asking again.`;
    response.answer = `${cleanText(response.answer)} ${coverageNote}`;
  }

  if (response.data && typeof response.data === "object") {
    response.data.journeyGuidance = response.journeyGuidance;
    response.data.contextPatch = response.contextPatch;
  }

  return response;
};

export default applyAciCustomerJourneyGuidance;
