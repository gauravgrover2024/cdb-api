import { z } from "zod";

/**
 * ACI Assist AI Planner Schema
 *
 * Purpose:
 * - Gemini/Vercel AI SDK should output ONLY this structured plan.
 * - The AI does not answer the user directly.
 * - The AI does not query MongoDB.
 * - The AI does not calculate final prices, EMI, offers, service costs, resale, or bank schemes.
 * - The AI only decides which safe backend tool should run with which entities/filters/ranking/output.
 * - The AI may suggest next conversation steps, but backend must sanitize them before display.
 *
 * Runtime:
 * user message -> AI plan -> validate/sanitize -> deterministic backend tool executor
 */

/* -------------------------------------------------------------------------- */
/*  Constants                                                                 */
/* -------------------------------------------------------------------------- */

export const PLANNER_MODES = [
  "single_tool",
  "multi_tool",
  "clarification",
  "unavailable",
  "general_response",
];

export const PLANNER_DOMAINS = [
  "new_car",
  "used_car",
  "loan",
  "customer",
  "payment",
  "insurance",
  "delivery_order",
  "internal",
  "general",
  "unknown",
];

export const CONVERSATION_MODES = [
  "direct_answer",
  "guided_advisor",
  "comparison",
  "recommendation",
  "calculation",
  "lead_capture",
  "clarification",
  "education",
  "internal_passthrough",
  "unavailable",
];

export const CUSTOMER_STAGES = [
  "unknown",
  "exploration",
  "evaluation",
  "consideration",
  "closing",
  "post_lead",
];

/**
 * Keep this list capability-based.
 * Do NOT add one tool per question.
 */
export const PLANNER_TOOLS = [
  // New-car data tools
  "vehicle_pricelist",
  "vehicle_colors",
  "vehicle_feature_lookup",
  "vehicle_compare",
  "vehicle_recommend",
  "vehicle_price_breakup",
  "vehicle_emi",
  "vehicle_price_history",
  "vehicle_explainer",

  // Lead / conversion tools
  "aci_lead_capture",

  // Future / non-new-car handoff tools
  "used_car_passthrough",
  "internal_passthrough",

  // Universal safe tools
  "clarification",
  "unavailable",
  "general_response",
];

export const NEW_CAR_PLANNER_TOOLS = [
  "vehicle_pricelist",
  "vehicle_colors",
  "vehicle_feature_lookup",
  "vehicle_compare",
  "vehicle_recommend",
  "vehicle_price_breakup",
  "vehicle_emi",
  "vehicle_price_history",
  "vehicle_explainer",
  "aci_lead_capture",
];

export const INTERNAL_PASSTHROUGH_DOMAINS = [
  "loan",
  "customer",
  "payment",
  "insurance",
  "delivery_order",
  "internal",
];

export const PLANNER_RANKINGS = [
  "price_low_to_high",
  "price_high_to_low",
  "value",
  "safety",
  "automatic_value",
  "family",
  "senior_friendly",
  "performance",
  "space",
  "feature_match",
  "similarity",
  "ownership_estimate",
  "variant_value",
  "fuel_efficiency",
  "comfort",
  "premium_features",
  "balanced",
];

export const PRICE_BASIS_VALUES = ["on_road", "ex_showroom"];

export const GROUP_BY_VALUES = ["model", "variant", "none"];

export const LEAD_TYPES = [
  "quotation",
  "test_drive",
  "callback",
  "finance_callback",
  "offer_enquiry",
];

export const CANVAS_TYPES = [
  "pricelist_canvas",
  "color_studio_canvas",
  "comparison_canvas",
  "recommendation_results_canvas",
  "safety_advisor_canvas",
  "emi_calculator_canvas",
  "price_breakup_canvas",
  "price_history_canvas",
  "aci_quotation_canvas",
  "lead_capture_canvas",
  "explainer_canvas",
  "unavailable_notice_canvas",
  "text_notice_canvas",
];

export const INLINE_TYPES = [
  "feature_answer_card",
  "model_ambiguity_card",
  "variant_ambiguity_card",
  "text_notice",
  "unavailable_notice",
  "clarification_card",
  "explainer_card",
];

export const EXPLAINER_TOPICS = [
  "on_road_vs_ex_showroom",
  "emi",
  "down_payment",
  "loan_tenure",
  "roi",
  "fuel_type",
  "transmission",
  "automatic_types",
  "cvt",
  "dct",
  "ivt",
  "amt",
  "torque_converter",
  "adas",
  "airbags",
  "ncap",
  "safety_features",
  "sunroof",
  "insurance",
  "zero_dep",
  "rto",
  "tcs",
  "optional_charges",
  "other_charges",
  "variant_selection",
  "petrol_vs_diesel",
  "hybrid",
  "ev",
  "ownership_cost",
  "resale",
  "price_history",
  "quotation",
  "test_drive",
  "general_car_buying",
];

export const UNAVAILABLE_REASONS = [
  "offers_not_available",
  "schemes_not_available",
  "service_centers_not_available",
  "service_cost_not_available",
  "bank_finance_schemes_not_available",
  "variant_wise_color_not_available",
  "dealer_inventory_not_available",
  "waiting_period_not_available",
  "exact_resale_value_not_available",
  "exact_tco_not_available",
  "outside_current_scope",
  "insufficient_information",
  "unsupported_request",
];

export const ALLOWED_ENTITY_KEYS = [
  "brand",
  "make",
  "model",
  "models",
  "variant",
  "variants",
  "city",
  "color",
  "fuelType",
  "transmission",
  "bodyType",
  "feature",
  "features",
  "topic",
  "topics",
  "leadType",
  "customerName",
  "mobile",
  "email",
  "pincode",
  "registrationNumber",
  "loanId",
  "customerId",
];

export const ALLOWED_FILTER_KEYS = [
  "brand",
  "make",
  "model",
  "models",
  "variant",
  "variants",
  "city",
  "budgetMin",
  "budgetMax",
  "priceBasis",
  "bodyType",
  "fuelType",
  "transmission",
  "activeOnly",
  "includeDiscontinued",
  "mustHaveFeatures",
  "compareFeatures",
  "color",
  "monthlyEmiBudget",
  "downPayment",
  "loanAmount",
  "tenureMonths",
  "roi",
  "leadType",
];

export const DATA_AVAILABILITY = {
  available: [
    "vehicle_pricelist",
    "vehicle_features",
    "model_level_vehicle_colors",
    "price_history",
    "generic_emi_calculation",
    "aci_lead_capture_payload",
    "vehicle_explainers",
  ],
  unavailable: [
    "verified_live_offers",
    "verified_schemes",
    "service_centers",
    "service_cost",
    "bank_wise_finance_schemes",
    "exact_resale_value",
    "dealer_inventory",
    "waiting_period",
    "variant_wise_color_availability",
    "exact_tco",
  ],
};

/* -------------------------------------------------------------------------- */
/*  Basic Helpers                                                             */
/* -------------------------------------------------------------------------- */

export const clamp = (number, min, max) =>
  Math.min(max, Math.max(min, Number(number) || 0));

export const isPlainObject = (value) =>
  Boolean(value) &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  !(value instanceof Date);

export const asArray = (value) => {
  if (Array.isArray(value)) {
    return value.filter(
      (item) => item !== undefined && item !== null && item !== "",
    );
  }

  if (value === undefined || value === null || value === "") return [];

  return [value];
};

export const unique = (items = []) => [...new Set(items.filter(Boolean))];

export const normalizeText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

export const normalizeSearchKey = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

export const normalizeCity = (value = "") => {
  const key = normalizeSearchKey(value);

  if (!key) return "new-delhi";

  if (["delhi", "new delhi", "ncr", "new delhi ncr"].includes(key)) {
    return "new-delhi";
  }

  return key.replace(/\s+/g, "-");
};

export const numberFromValue = (value) => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  const text = String(value ?? "")
    .replace(/,/g, "")
    .trim();

  if (!text) return null;

  const match = text.match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;

  const number = Number(match[0]);
  return Number.isFinite(number) ? number : null;
};

export const normalizeMoneyAmount = (
  value,
  { message = "", field = "" } = {},
) => {
  const number = numberFromValue(value);
  if (number === null) return undefined;

  const text = String(value ?? "").toLowerCase();
  const source = `${message || ""} ${field || ""}`.toLowerCase();

  if (
    /\b(cr|crore|crores)\b/.test(text) ||
    /\b(cr|crore|crores)\b/.test(source)
  ) {
    if (number <= 100) return Math.round(number * 10000000);
  }

  if (
    /\b(lakh|lakhs|lac|lacs|l)\b/.test(text) ||
    /\b(lakh|lakhs|lac|lacs|l)\b/.test(source)
  ) {
    if (number <= 300) return Math.round(number * 100000);
  }

  if (
    ["budgetMin", "budgetMax", "downPayment", "loanAmount"].includes(field) &&
    number > 0 &&
    number <= 300
  ) {
    return Math.round(number * 100000);
  }

  if (field === "monthlyEmiBudget") {
    if (/\bk\b/i.test(text) && number <= 500) return Math.round(number * 1000);
    return Math.round(number);
  }

  return Math.round(number);
};

export const normalizePercent = (value) => {
  const number = numberFromValue(value);
  if (number === null) return undefined;
  if (number < 0 || number > 100) return undefined;
  return number;
};

export const normalizeBoolean = (value, fallback = false) => {
  if (typeof value === "boolean") return value;

  const text = String(value ?? "")
    .toLowerCase()
    .trim();

  if (["true", "yes", "y", "1", "active"].includes(text)) return true;
  if (["false", "no", "n", "0"].includes(text)) return false;

  return fallback;
};

export const stripUnknownKeys = (object = {}, allowedKeys = []) => {
  if (!isPlainObject(object)) return {};

  const allowed = new Set(allowedKeys);

  return Object.fromEntries(
    Object.entries(object).filter(
      ([key, value]) => allowed.has(key) && value !== undefined,
    ),
  );
};

export const normalizeStringArray = (value) =>
  unique(
    asArray(value)
      .flatMap((item) =>
        typeof item === "string" && item.includes(",")
          ? item.split(",")
          : [item],
      )
      .map((item) => normalizeText(item))
      .filter(Boolean),
  );

export const slug = (value = "") =>
  normalizeSearchKey(value).replace(/\s+/g, "-");

/* -------------------------------------------------------------------------- */
/*  Zod Schemas                                                               */
/* -------------------------------------------------------------------------- */

const LooseRecordSchema = z.record(z.any()).default({});

export const PlannerOutputSchema = z
  .object({
    canvasType: z.enum(CANVAS_TYPES).nullable().optional().default(null),
    inlineType: z.enum(INLINE_TYPES).nullable().optional().default(null),
    groupBy: z.enum(GROUP_BY_VALUES).nullable().optional().default(null),
    preferredWidgetType: z.string().nullable().optional().default(null),
  })
  .default({});

export const PlannerToolSchema = z.object({
  tool: z.enum(PLANNER_TOOLS),
  entities: LooseRecordSchema,
  filters: LooseRecordSchema,
  ranking: z.enum(PLANNER_RANKINGS).nullable().optional().default(null),
  output: PlannerOutputSchema,
});

export const PlannerNextStepSchema = z.object({
  id: z.string().optional().default(""),
  label: z.string().min(1).max(80),
  query: z.string().min(1).max(180),
  tool: z.enum(PLANNER_TOOLS).nullable().optional().default(null),
  entities: LooseRecordSchema.optional().default({}),
  filters: LooseRecordSchema.optional().default({}),
  ranking: z.enum(PLANNER_RANKINGS).nullable().optional().default(null),
  reason: z.string().max(240).optional().default(""),
  priority: z.number().min(0).max(100).optional().default(50),
  output: PlannerOutputSchema.optional().default({}),
});

export const AciPlannerSchema = z.object({
  mode: z.enum(PLANNER_MODES),
  domain: z.enum(PLANNER_DOMAINS).default("unknown"),
  conversationMode: z.enum(CONVERSATION_MODES).default("direct_answer"),
  customerStage: z.enum(CUSTOMER_STAGES).default("unknown"),
  tools: z.array(PlannerToolSchema).default([]),
  nextSteps: z.array(PlannerNextStepSchema).max(6).default([]),
  clarification: z.string().nullable().optional().default(null),
  confidence: z.number().min(0).max(1).default(0.5),
  reasoningSummary: z.string().max(500).optional().default(""),
  unavailableReason: z
    .enum(UNAVAILABLE_REASONS)
    .nullable()
    .optional()
    .default(null),
});

/**
 * Loose schema for raw LLM output.
 * We sanitize this into strict AciPlannerSchema.
 */
export const LooseAciPlannerSchema = z.object({
  mode: z.string().optional(),
  domain: z.string().optional(),
  conversationMode: z.string().optional(),
  customerStage: z.string().optional(),
  tools: z.array(z.record(z.any())).optional(),
  nextSteps: z.array(z.record(z.any())).optional(),
  clarification: z.any().optional(),
  confidence: z.any().optional(),
  reasoningSummary: z.any().optional(),
  unavailableReason: z.any().optional(),
});

/* -------------------------------------------------------------------------- */
/*  Classification Helpers                                                    */
/* -------------------------------------------------------------------------- */

export const isAllowedPlannerTool = (tool) =>
  PLANNER_TOOLS.includes(String(tool || ""));

export const isNewCarPlannerTool = (tool) =>
  NEW_CAR_PLANNER_TOOLS.includes(String(tool || ""));

export const isInternalPlannerDomain = (domain) =>
  INTERNAL_PASSTHROUGH_DOMAINS.includes(String(domain || ""));

export const isLeadTool = (tool) => tool === "aci_lead_capture";

export const shouldUseUnavailableForReason = (reason) =>
  UNAVAILABLE_REASONS.includes(String(reason || ""));

export const looksLikeInternalOpsQuery = (message = "") => {
  const text = normalizeSearchKey(message);

  return [
    /\bloan\b/,
    /\bclosure\b/,
    /\bapproved\b/,
    /\bdisbursed\b/,
    /\bdisbursal\b/,
    /\bbusiness\b/,
    /\bcustomer\s*360\b/,
    /\bpayment\b/,
    /\breceivable\b/,
    /\binsurance\b/,
    /\bdelivery\s*order\b/,
    /\bdo\b/,
    /\bcase\b/,
    /\bfile\b/,
    /\bemi\s*received\b/,
    /\bpending\s*payment\b/,
  ].some((regex) => regex.test(text));
};

export const looksLikeNewCarQuery = (message = "") => {
  const text = normalizeSearchKey(message);

  return [
    /\bprice\b/,
    /\bpricelist\b/,
    /\bon road\b/,
    /\bex showroom\b/,
    /\bvariant\b/,
    /\bcolors?\b/,
    /\bcolours?\b/,
    /\bcompare\b/,
    /\bfeatures?\b/,
    /\bsunroof\b/,
    /\bairbags?\b/,
    /\badas\b/,
    /\bsuv\b/,
    /\bsedan\b/,
    /\bhatchback\b/,
    /\bmpv\b/,
    /\bautomatic\b/,
    /\bmanual\b/,
    /\bemi\b/,
    /\bquotation\b/,
    /\bquote\b/,
    /\btest drive\b/,
    /\bcar\b/,
    /\bcars\b/,
    /\bverna\b/,
    /\bcreta\b/,
    /\belevate\b/,
    /\bseltos\b/,
    /\bvenue\b/,
    /\bcity\b/,
    /\bslavia\b/,
    /\bvirtus\b/,
    /\bsafari\b/,
    /\bxuv\b/,
  ].some((regex) => regex.test(text));
};

/* -------------------------------------------------------------------------- */
/*  Normalizers                                                               */
/* -------------------------------------------------------------------------- */

export const normalizePlannerEntities = (entities = {}) => {
  const clean = stripUnknownKeys(entities, ALLOWED_ENTITY_KEYS);

  const normalized = {
    ...clean,
  };

  if (normalized.brand) normalized.brand = normalizeText(normalized.brand);
  if (normalized.make) normalized.make = normalizeText(normalized.make);
  if (normalized.model) normalized.model = normalizeText(normalized.model);
  if (normalized.variant)
    normalized.variant = normalizeText(normalized.variant);
  if (normalized.city) normalized.city = normalizeCity(normalized.city);
  if (normalized.color) normalized.color = normalizeText(normalized.color);
  if (normalized.fuelType)
    normalized.fuelType = normalizeText(normalized.fuelType);
  if (normalized.transmission) {
    normalized.transmission = normalizeText(
      normalized.transmission,
    ).toLowerCase();
  }
  if (normalized.bodyType) {
    normalized.bodyType = normalizeText(normalized.bodyType).toLowerCase();
  }
  if (normalized.feature)
    normalized.feature = normalizeText(normalized.feature);
  if (normalized.topic) normalized.topic = normalizeText(normalized.topic);

  if (normalized.models)
    normalized.models = normalizeStringArray(normalized.models);
  if (normalized.variants) {
    normalized.variants = normalizeStringArray(normalized.variants);
  }
  if (normalized.features) {
    normalized.features = normalizeStringArray(normalized.features);
  }
  if (normalized.topics)
    normalized.topics = normalizeStringArray(normalized.topics);

  if (normalized.leadType && !LEAD_TYPES.includes(normalized.leadType)) {
    delete normalized.leadType;
  }

  return Object.fromEntries(
    Object.entries(normalized).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      return value !== undefined && value !== null && value !== "";
    }),
  );
};

export const normalizePlannerFilters = (
  filters = {},
  { message = "" } = {},
) => {
  const clean = stripUnknownKeys(filters, ALLOWED_FILTER_KEYS);

  const normalized = {
    ...clean,
  };

  if (normalized.brand) normalized.brand = normalizeText(normalized.brand);
  if (normalized.make) normalized.make = normalizeText(normalized.make);
  if (normalized.model) normalized.model = normalizeText(normalized.model);
  if (normalized.variant)
    normalized.variant = normalizeText(normalized.variant);
  if (normalized.city) normalized.city = normalizeCity(normalized.city);
  if (normalized.bodyType) {
    normalized.bodyType = normalizeText(normalized.bodyType).toLowerCase();
  }
  if (normalized.fuelType)
    normalized.fuelType = normalizeText(normalized.fuelType);
  if (normalized.transmission) {
    normalized.transmission = normalizeText(
      normalized.transmission,
    ).toLowerCase();
  }
  if (normalized.color) normalized.color = normalizeText(normalized.color);

  if (normalized.models)
    normalized.models = normalizeStringArray(normalized.models);
  if (normalized.variants) {
    normalized.variants = normalizeStringArray(normalized.variants);
  }
  if (normalized.mustHaveFeatures) {
    normalized.mustHaveFeatures = normalizeStringArray(
      normalized.mustHaveFeatures,
    );
  }
  if (normalized.compareFeatures) {
    normalized.compareFeatures = normalizeStringArray(
      normalized.compareFeatures,
    );
  }

  for (const key of [
    "budgetMin",
    "budgetMax",
    "downPayment",
    "loanAmount",
    "monthlyEmiBudget",
  ]) {
    if (normalized[key] !== undefined) {
      const amount = normalizeMoneyAmount(normalized[key], {
        message,
        field: key,
      });

      if (amount !== undefined) normalized[key] = amount;
      else delete normalized[key];
    }
  }

  if (normalized.tenureMonths !== undefined) {
    const tenure = numberFromValue(normalized.tenureMonths);
    if (tenure && tenure > 0 && tenure <= 120) {
      normalized.tenureMonths = Math.round(tenure);
    } else {
      delete normalized.tenureMonths;
    }
  }

  if (normalized.roi !== undefined) {
    const roi = normalizePercent(normalized.roi);
    if (roi !== undefined) normalized.roi = roi;
    else delete normalized.roi;
  }

  if (normalized.activeOnly !== undefined) {
    normalized.activeOnly = normalizeBoolean(normalized.activeOnly, true);
  }

  if (normalized.includeDiscontinued !== undefined) {
    normalized.includeDiscontinued = normalizeBoolean(
      normalized.includeDiscontinued,
      false,
    );
  }

  if (
    normalized.priceBasis &&
    !PRICE_BASIS_VALUES.includes(normalized.priceBasis)
  ) {
    normalized.priceBasis = "on_road";
  }

  if (normalized.leadType && !LEAD_TYPES.includes(normalized.leadType)) {
    delete normalized.leadType;
  }

  return Object.fromEntries(
    Object.entries(normalized).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      return value !== undefined && value !== null && value !== "";
    }),
  );
};

export const normalizePlannerOutput = (output = {}, tool = "") => {
  const normalized = {
    canvasType: output?.canvasType || null,
    inlineType: output?.inlineType || null,
    groupBy: output?.groupBy || null,
    preferredWidgetType: output?.preferredWidgetType || null,
  };

  if (normalized.canvasType && !CANVAS_TYPES.includes(normalized.canvasType)) {
    normalized.canvasType = null;
  }

  if (normalized.inlineType && !INLINE_TYPES.includes(normalized.inlineType)) {
    normalized.inlineType = null;
  }

  if (normalized.groupBy && !GROUP_BY_VALUES.includes(normalized.groupBy)) {
    normalized.groupBy = null;
  }

  if (!normalized.canvasType && !normalized.inlineType) {
    if (tool === "vehicle_pricelist")
      normalized.canvasType = "pricelist_canvas";
    if (tool === "vehicle_colors")
      normalized.canvasType = "color_studio_canvas";
    if (tool === "vehicle_feature_lookup") {
      normalized.inlineType = "feature_answer_card";
    }
    if (tool === "vehicle_compare") normalized.canvasType = "comparison_canvas";
    if (tool === "vehicle_recommend") {
      normalized.canvasType = "recommendation_results_canvas";
    }
    if (tool === "vehicle_price_breakup") {
      normalized.canvasType = "price_breakup_canvas";
    }
    if (tool === "vehicle_emi") normalized.canvasType = "emi_calculator_canvas";
    if (tool === "vehicle_price_history") {
      normalized.canvasType = "price_history_canvas";
    }
    if (tool === "vehicle_explainer") normalized.inlineType = "explainer_card";
    if (tool === "aci_lead_capture")
      normalized.canvasType = "lead_capture_canvas";
    if (tool === "unavailable") normalized.inlineType = "unavailable_notice";
    if (tool === "clarification") normalized.inlineType = "clarification_card";
    if (tool === "general_response") normalized.inlineType = "text_notice";
  }

  if (tool === "vehicle_recommend" && !normalized.groupBy) {
    normalized.groupBy = "model";
  }

  return normalized;
};

export const normalizePlannerTool = (toolPlan = {}, { message = "" } = {}) => {
  const tool = String(toolPlan?.tool || "").trim();

  if (!isAllowedPlannerTool(tool)) return null;

  const entities = normalizePlannerEntities(toolPlan.entities || {});
  const filters = normalizePlannerFilters(toolPlan.filters || {}, { message });

  let ranking = toolPlan.ranking || null;
  if (ranking && !PLANNER_RANKINGS.includes(ranking)) ranking = null;

  const output = normalizePlannerOutput(toolPlan.output || {}, tool);

  if (isNewCarPlannerTool(tool)) {
    if (!filters.city && !entities.city) filters.city = "new-delhi";

    if (
      filters.activeOnly === undefined &&
      filters.includeDiscontinued !== true
    ) {
      filters.activeOnly = true;
    }

    if (
      !filters.priceBasis &&
      [
        "vehicle_recommend",
        "vehicle_emi",
        "vehicle_price_breakup",
        "vehicle_pricelist",
        "vehicle_compare",
      ].includes(tool)
    ) {
      filters.priceBasis = "on_road";
    }
  }

  if (tool === "aci_lead_capture") {
    const leadType = entities.leadType || filters.leadType || "quotation";
    entities.leadType = LEAD_TYPES.includes(leadType) ? leadType : "quotation";
    filters.leadType = entities.leadType;
  }

  return {
    tool,
    entities,
    filters,
    ranking,
    output,
  };
};

/* -------------------------------------------------------------------------- */
/*  Next Step Normalization                                                   */
/* -------------------------------------------------------------------------- */

export const normalizePlannerNextStep = (
  nextStep = {},
  { message = "", index = 0 } = {},
) => {
  const label = normalizeText(nextStep.label || nextStep.title || "");
  const query = normalizeText(
    nextStep.query || nextStep.message || nextStep.followUpQuery || "",
  );

  if (!label || !query) return null;

  const rawTool = nextStep.tool || null;
  const tool = isAllowedPlannerTool(rawTool) ? rawTool : null;

  const entities = normalizePlannerEntities(nextStep.entities || {});
  const filters = normalizePlannerFilters(nextStep.filters || {}, { message });

  let ranking = nextStep.ranking || null;
  if (ranking && !PLANNER_RANKINGS.includes(ranking)) ranking = null;

  const output = normalizePlannerOutput(nextStep.output || {}, tool || "");

  const fallbackId = `next-${slug(tool || "step")}-${slug(label)}-${index + 1}`;

  return {
    id: normalizeText(nextStep.id) || fallbackId,
    label: label.slice(0, 80),
    query: query.slice(0, 180),
    tool,
    entities,
    filters,
    ranking,
    reason: normalizeText(nextStep.reason || "").slice(0, 240),
    priority: clamp(nextStep.priority ?? 50, 0, 100),
    output,
  };
};

export const formatBudgetForQuery = (value, fallbackLakh = 20) => {
  const num = Number(value || 0);

  if (!Number.isFinite(num) || num <= 0) return `${fallbackLakh} lakh`;

  if (num > 1000) {
    const lakh = num / 100000;
    return `${Number.isInteger(lakh) ? lakh : lakh.toFixed(1)} lakh`;
  }

  return `${num} lakh`;
};

export const sanitizeNextStepQuery = (step = {}) => {
  let query = normalizeText(step.query || "");

  // Fix "2000000 lakh" style generated queries.
  query = query.replace(/\b(\d{6,})\s*lakh\b/gi, (_, rawAmount) => {
    const label = formatBudgetForQuery(Number(rawAmount), 20);
    return label;
  });

  // Avoid factual variant-wise color claims.
  if (
    /\b(which|does|do)\b/i.test(query) &&
    /\bvariant|variants|sx|zx|vx|zxi|vxi|htx|gtx\b/i.test(query) &&
    /\bcolor|colour|grey|gray|white|black|red|blue|silver|pearl\b/i.test(query)
  ) {
    query = query.replace(/^which\s+variants?\s+get\s+/i, "Confirm ");
    query = query.replace(/\?$/, "");
    query = `${query} for your preferred variant`;
  }

  return {
    ...step,
    query,
  };
};

export const sanitizePlannerNextSteps = (
  nextSteps = [],
  { message = "", tools = [] } = {},
) => {
  const normalized = asArray(nextSteps)
    .map((step, index) => normalizePlannerNextStep(step, { message, index }))
    .filter(Boolean)
    .map(sanitizeNextStepQuery);

  const deduped = [];
  const seen = new Set();

  for (const step of normalized.sort((a, b) => b.priority - a.priority)) {
    const key = normalizeSearchKey(step.query);

    if (!key || seen.has(key)) continue;

    seen.add(key);
    deduped.push(step);

    if (deduped.length >= 6) break;
  }

  if (deduped.length) return deduped;

  return buildFallbackNextSteps({ tools, message });
};

export const buildFallbackNextSteps = ({ tools = [], message = "" } = {}) => {
  const primaryTool = tools[0] || {};
  const entities = primaryTool.entities || {};
  const filters = primaryTool.filters || {};

  const model =
    entities.model ||
    filters.model ||
    asArray(entities.models)[0] ||
    asArray(filters.models)[0] ||
    "";

  const modelLabel = model || "this car";
  const budgetLabel = formatBudgetForQuery(filters.budgetMax, 20);

  if (!model && primaryTool.tool === "vehicle_recommend") {
    return [
      {
        id: "next-filter-automatic",
        label: "Show automatic only",
        query: `Show automatic cars under ${budgetLabel}`,
        tool: "vehicle_recommend",
        entities: {},
        filters: {
          ...filters,
          transmission: "automatic",
        },
        ranking: primaryTool.ranking || "value",
        reason: "Narrow recommendations by automatic transmission.",
        priority: 82,
        output: normalizePlannerOutput({}, "vehicle_recommend"),
      },
      {
        id: "next-safest-options",
        label: "Show safest options",
        query: `Show safest cars under ${budgetLabel}`,
        tool: "vehicle_recommend",
        entities: {},
        filters,
        ranking: "safety",
        reason: "Safety is a common next filter.",
        priority: 78,
        output: normalizePlannerOutput({}, "vehicle_recommend"),
      },
    ];
  }

  if (model) {
    return [
      {
        id: `next-colors-${slug(modelLabel)}`,
        label: "Show colors",
        query: `Show colors of ${modelLabel}`,
        tool: "vehicle_colors",
        entities: { model },
        filters: { city: filters.city || "new-delhi" },
        ranking: null,
        reason:
          "Color selection is a common next step after pricing or features.",
        priority: 90,
        output: normalizePlannerOutput({}, "vehicle_colors"),
      },
      {
        id: `next-features-${slug(modelLabel)}`,
        label: "Show features",
        query: `Show features of ${modelLabel}`,
        tool: "vehicle_feature_lookup",
        entities: { model },
        filters: { city: filters.city || "new-delhi" },
        ranking: null,
        reason: "Feature details help the customer evaluate the car.",
        priority: 85,
        output: normalizePlannerOutput(
          { canvasType: "recommendation_results_canvas" },
          "vehicle_feature_lookup",
        ),
      },
      {
        id: `next-emi-${slug(modelLabel)}`,
        label: "Calculate EMI",
        query: `Calculate EMI for ${modelLabel}`,
        tool: "vehicle_emi",
        entities: { model },
        filters: { city: filters.city || "new-delhi", priceBasis: "on_road" },
        ranking: null,
        reason: "EMI is a natural next step after price.",
        priority: 80,
        output: normalizePlannerOutput({}, "vehicle_emi"),
      },
      {
        id: `next-quote-${slug(modelLabel)}`,
        label: "Get quotation",
        query: `Get quotation for ${modelLabel}`,
        tool: "aci_lead_capture",
        entities: { model, leadType: "quotation" },
        filters: { leadType: "quotation", city: filters.city || "new-delhi" },
        ranking: null,
        reason: "Quotation moves the customer toward conversion.",
        priority: 76,
        output: normalizePlannerOutput({}, "aci_lead_capture"),
      },
      {
        id: `next-compare-${slug(modelLabel)}`,
        label: "Compare similar cars",
        query: `Compare ${modelLabel} with similar cars`,
        tool: "vehicle_compare",
        entities: { model },
        filters: { city: filters.city || "new-delhi" },
        ranking: "similarity",
        reason: "Comparison helps evaluate alternatives.",
        priority: 72,
        output: normalizePlannerOutput({}, "vehicle_compare"),
      },
    ];
  }

  return [];
};

/* -------------------------------------------------------------------------- */
/*  Availability / Safety Guards                                              */
/* -------------------------------------------------------------------------- */

export const detectUnavailableReason = ({
  message = "",
  toolPlan = null,
} = {}) => {
  const text = normalizeSearchKey(message);
  const tool = toolPlan?.tool || "";
  const filters = toolPlan?.filters || {};
  const entities = toolPlan?.entities || {};

  const features = [
    ...asArray(filters.mustHaveFeatures),
    ...asArray(filters.compareFeatures),
    ...asArray(entities.features),
    entities.feature,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (
    /\b(offer|offers|discount|scheme|cash discount|exchange bonus|corporate offer|month end)\b/.test(
      text,
    )
  ) {
    return "offers_not_available";
  }

  if (
    /\b(service center|service centre|nearest service|workshop)\b/.test(text)
  ) {
    return "service_centers_not_available";
  }

  if (/\b(service cost|maintenance cost|scheduled service)\b/.test(text)) {
    return "service_cost_not_available";
  }

  if (
    /\b(best bank|which bank|bank offer|loan offer|roi by bank|processing fee)\b/.test(
      text,
    )
  ) {
    return "bank_finance_schemes_not_available";
  }

  if (
    /\b(waiting period|delivery time|stock|inventory|available at dealer)\b/.test(
      text,
    )
  ) {
    return "dealer_inventory_not_available";
  }

  if (/\b(resale|resale value)\b/.test(text)) {
    return "exact_resale_value_not_available";
  }

  if (
    /\b(tco|total cost of ownership|5 year ownership|five year ownership)\b/.test(
      text,
    )
  ) {
    return "exact_tco_not_available";
  }

  if (
    /\b(get|gets|available in|comes in)\b/.test(text) &&
    /\b(color|colour|grey|gray|white|black|red|blue|silver|titan|pearl)\b/.test(
      text,
    ) &&
    (entities.variant ||
      /\b(sx|zx|vx|zxi|vxi|htx|gtx|xv|top model)\b/.test(text))
  ) {
    return "variant_wise_color_not_available";
  }

  if (tool === "vehicle_colors" && features.includes("variant wise color")) {
    return "variant_wise_color_not_available";
  }

  return null;
};

export const applyUnavailableDataGuard = (toolPlan, { message = "" } = {}) => {
  if (!toolPlan) return null;

  const reason = detectUnavailableReason({ message, toolPlan });

  if (!reason) return toolPlan;

  if (
    ["offers_not_available", "schemes_not_available"].includes(reason) &&
    (toolPlan.entities?.model || toolPlan.filters?.model)
  ) {
    return {
      tool: "aci_lead_capture",
      entities: {
        ...toolPlan.entities,
        leadType: "offer_enquiry",
      },
      filters: {
        ...toolPlan.filters,
        leadType: "offer_enquiry",
        unavailableReason: reason,
      },
      ranking: null,
      output: normalizePlannerOutput({}, "aci_lead_capture"),
      unavailableReason: reason,
    };
  }

  if (
    reason === "bank_finance_schemes_not_available" &&
    (toolPlan.entities?.model || toolPlan.filters?.model)
  ) {
    return {
      tool: "aci_lead_capture",
      entities: {
        ...toolPlan.entities,
        leadType: "finance_callback",
      },
      filters: {
        ...toolPlan.filters,
        leadType: "finance_callback",
        unavailableReason: reason,
      },
      ranking: null,
      output: normalizePlannerOutput({}, "aci_lead_capture"),
      unavailableReason: reason,
    };
  }

  return {
    tool: "unavailable",
    entities: toolPlan.entities || {},
    filters: {
      ...(toolPlan.filters || {}),
      unavailableReason: reason,
    },
    ranking: null,
    output: normalizePlannerOutput({}, "unavailable"),
    unavailableReason: reason,
  };
};

/* -------------------------------------------------------------------------- */
/*  Plan Sanitizer                                                            */
/* -------------------------------------------------------------------------- */

export const inferModeFromTools = (tools = []) => {
  if (!tools.length) return "unavailable";

  if (tools.length === 1) {
    const only = tools[0]?.tool;

    if (only === "clarification") return "clarification";
    if (only === "unavailable") return "unavailable";
    if (only === "general_response") return "general_response";

    return "single_tool";
  }

  return "multi_tool";
};

export const inferDomainFromTools = (tools = [], fallback = "unknown") => {
  if (!tools.length) return fallback || "unknown";

  if (tools.some((item) => isNewCarPlannerTool(item.tool))) return "new_car";
  if (tools.some((item) => item.tool === "internal_passthrough"))
    return "internal";
  if (tools.some((item) => item.tool === "used_car_passthrough"))
    return "used_car";
  if (tools.some((item) => item.tool === "general_response")) return "general";

  return fallback || "unknown";
};

export const inferConversationModeFromTools = (
  tools = [],
  fallback = "direct_answer",
) => {
  const first = tools[0]?.tool;

  if (first === "vehicle_recommend") return "recommendation";
  if (first === "vehicle_compare") return "comparison";
  if (first === "vehicle_emi") return "calculation";
  if (first === "aci_lead_capture") return "lead_capture";
  if (first === "vehicle_explainer") return "education";
  if (first === "clarification") return "clarification";
  if (first === "unavailable") return "unavailable";
  if (first === "internal_passthrough") return "internal_passthrough";

  return fallback || "direct_answer";
};

export const inferCustomerStageFromTools = (
  tools = [],
  fallback = "unknown",
) => {
  const toolNames = tools.map((item) => item.tool);

  if (toolNames.includes("aci_lead_capture")) return "closing";
  if (
    toolNames.includes("vehicle_emi") ||
    toolNames.includes("vehicle_price_breakup")
  ) {
    return "consideration";
  }
  if (
    toolNames.includes("vehicle_compare") ||
    toolNames.includes("vehicle_feature_lookup")
  ) {
    return "evaluation";
  }
  if (
    toolNames.includes("vehicle_recommend") ||
    toolNames.includes("vehicle_pricelist")
  ) {
    return "exploration";
  }

  return fallback || "unknown";
};

export const makeUnavailablePlan = ({
  reason = "unsupported_request",
  message = "",
  confidence = 0.7,
} = {}) => ({
  mode: "unavailable",
  domain: "unknown",
  conversationMode: "unavailable",
  customerStage: "unknown",
  tools: [
    {
      tool: "unavailable",
      entities: {},
      filters: {
        unavailableReason: reason,
      },
      ranking: null,
      output: normalizePlannerOutput({}, "unavailable"),
    },
  ],
  nextSteps: [],
  clarification: null,
  confidence,
  reasoningSummary:
    message ||
    "The request needs data or functionality that is not available in the current ACI Assist backend.",
  unavailableReason: reason,
});

export const makeClarificationPlan = ({
  question = "Can you clarify what you want to check?",
  domain = "unknown",
  confidence = 0.6,
} = {}) => ({
  mode: "clarification",
  domain,
  conversationMode: "clarification",
  customerStage: "unknown",
  tools: [
    {
      tool: "clarification",
      entities: {},
      filters: {},
      ranking: null,
      output: normalizePlannerOutput({}, "clarification"),
    },
  ],
  nextSteps: [],
  clarification: question,
  confidence,
  reasoningSummary: "The user request is ambiguous and needs clarification.",
  unavailableReason: null,
});

export const makeInternalPassthroughPlan = ({
  domain = "internal",
  confidence = 0.95,
  summary = "Internal CDrive request should use deterministic backend routing.",
} = {}) => ({
  mode: "single_tool",
  domain,
  conversationMode: "internal_passthrough",
  customerStage: "unknown",
  tools: [
    {
      tool: "internal_passthrough",
      entities: {},
      filters: {},
      ranking: null,
      output: {
        canvasType: null,
        inlineType: null,
        groupBy: null,
        preferredWidgetType: null,
      },
    },
  ],
  nextSteps: [],
  clarification: null,
  confidence,
  reasoningSummary: summary,
  unavailableReason: null,
});

export const sanitizePlannerPlan = (rawPlan, { message = "" } = {}) => {
  if (!rawPlan || !isPlainObject(rawPlan)) {
    return makeUnavailablePlan({
      reason: "unsupported_request",
      message: "Planner returned an empty or invalid plan.",
      confidence: 0,
    });
  }

  const loose = LooseAciPlannerSchema.safeParse(rawPlan);

  if (!loose.success) {
    return makeUnavailablePlan({
      reason: "unsupported_request",
      message: "Planner output could not be parsed.",
      confidence: 0,
    });
  }

  const input = loose.data;

  let tools = asArray(input.tools)
    .map((toolPlan) => normalizePlannerTool(toolPlan, { message }))
    .filter(Boolean)
    .map((toolPlan) => applyUnavailableDataGuard(toolPlan, { message }))
    .filter(Boolean);

  const requestedMode = String(input.mode || "").trim();

  if (!tools.length && requestedMode === "clarification") {
    return makeClarificationPlan({
      question:
        normalizeText(input.clarification) || "Can you clarify your request?",
      domain: PLANNER_DOMAINS.includes(input.domain) ? input.domain : "unknown",
      confidence: clamp(input.confidence ?? 0.6, 0, 1),
    });
  }

  if (!tools.length && requestedMode === "general_response") {
    tools = [
      {
        tool: "general_response",
        entities: {},
        filters: {},
        ranking: null,
        output: normalizePlannerOutput({}, "general_response"),
      },
    ];
  }

  if (!tools.length) {
    return makeUnavailablePlan({
      reason: shouldUseUnavailableForReason(input.unavailableReason)
        ? input.unavailableReason
        : "unsupported_request",
      confidence: clamp(input.confidence ?? 0.4, 0, 1),
    });
  }

  const concreteTools = tools.filter(
    (item) =>
      ![
        "unavailable",
        "clarification",
        "general_response",
        "internal_passthrough",
      ].includes(item.tool),
  );

  if (concreteTools.length) {
    tools = concreteTools;
  } else {
    tools = tools.slice(0, 1);
  }

  const rawDomain = PLANNER_DOMAINS.includes(input.domain)
    ? input.domain
    : inferDomainFromTools(tools, "unknown");

  const rawMode = PLANNER_MODES.includes(input.mode)
    ? input.mode
    : inferModeFromTools(tools);

  const conversationMode = CONVERSATION_MODES.includes(input.conversationMode)
    ? input.conversationMode
    : inferConversationModeFromTools(tools, "direct_answer");

  const customerStage = CUSTOMER_STAGES.includes(input.customerStage)
    ? input.customerStage
    : inferCustomerStageFromTools(tools, "unknown");

  const sanitizedCore = {
    mode:
      tools.length > 1
        ? "multi_tool"
        : tools[0]?.tool === "clarification"
          ? "clarification"
          : tools[0]?.tool === "unavailable"
            ? "unavailable"
            : tools[0]?.tool === "general_response"
              ? "general_response"
              : rawMode === "multi_tool" && tools.length === 1
                ? "single_tool"
                : rawMode,
    domain: inferDomainFromTools(tools, rawDomain),
    conversationMode,
    customerStage,
    tools,
    nextSteps: [],
    clarification: normalizeText(input.clarification) || null,
    confidence: clamp(input.confidence ?? 0.5, 0, 1),
    reasoningSummary: normalizeText(input.reasoningSummary).slice(0, 500),
    unavailableReason: shouldUseUnavailableForReason(input.unavailableReason)
      ? input.unavailableReason
      : tools[0]?.unavailableReason || null,
  };

  sanitizedCore.nextSteps = sanitizePlannerNextSteps(input.nextSteps || [], {
    message,
    tools,
  });

  const parsed = AciPlannerSchema.safeParse(sanitizedCore);

  if (!parsed.success) {
    return makeUnavailablePlan({
      reason: "unsupported_request",
      message: "Sanitized planner output failed strict schema validation.",
      confidence: 0,
    });
  }

  return parsed.data;
};

/* -------------------------------------------------------------------------- */
/*  Validation Wrapper                                                        */
/* -------------------------------------------------------------------------- */

export const validatePlannerPlan = (plan, { message = "" } = {}) => {
  const sanitized = sanitizePlannerPlan(plan, { message });
  const parsed = AciPlannerSchema.safeParse(sanitized);

  if (!parsed.success) {
    return {
      valid: false,
      plan: null,
      errors: parsed.error.issues.map((issue) => ({
        path: issue.path.join("."),
        message: issue.message,
      })),
      warnings: [],
    };
  }

  const warnings = [];

  for (const toolPlan of parsed.data.tools || []) {
    if (toolPlan.tool === "unavailable") {
      warnings.push(
        toolPlan.filters?.unavailableReason ||
          parsed.data.unavailableReason ||
          "unavailable",
      );
    }

    if (toolPlan.tool === "vehicle_colors") {
      warnings.push(
        "vehicle_colors currently supports model-level color availability only",
      );
    }

    if (
      toolPlan.tool === "vehicle_recommend" &&
      toolPlan.ranking === "ownership_estimate"
    ) {
      warnings.push(
        "ownership estimate cannot use exact service/resale data until those collections exist",
      );
    }

    if (toolPlan.tool === "vehicle_explainer") {
      warnings.push(
        "vehicle_explainer should explain concepts, not invent live data",
      );
    }
  }

  return {
    valid: true,
    plan: parsed.data,
    errors: [],
    warnings: unique(warnings),
  };
};

/* -------------------------------------------------------------------------- */
/*  Prompt Metadata Helpers                                                   */
/* -------------------------------------------------------------------------- */

export const plannerSchemaForPrompt = () => ({
  modes: PLANNER_MODES,
  domains: PLANNER_DOMAINS,
  conversationModes: CONVERSATION_MODES,
  customerStages: CUSTOMER_STAGES,
  tools: PLANNER_TOOLS,
  newCarTools: NEW_CAR_PLANNER_TOOLS,
  rankings: PLANNER_RANKINGS,
  allowedEntityKeys: ALLOWED_ENTITY_KEYS,
  allowedFilterKeys: ALLOWED_FILTER_KEYS,
  priceBasisValues: PRICE_BASIS_VALUES,
  leadTypes: LEAD_TYPES,
  canvasTypes: CANVAS_TYPES,
  inlineTypes: INLINE_TYPES,
  explainerTopics: EXPLAINER_TOPICS,
  unavailableReasons: UNAVAILABLE_REASONS,
  dataAvailability: DATA_AVAILABILITY,
});

export const plannerSystemRules = () =>
  `
You are ACI Assist Planner.

You only create JSON plans matching the schema.
You do not answer the user directly.
You do not query the database.
You do not calculate final prices.
You do not invent offers, service centers, service costs, bank schemes, resale values, waiting periods, or dealer inventory.
You only choose allowed tools, entities, filters, ranking, output, and nextSteps.

Available data:
- vehicle/pricelist data
- vehicle feature data
- model-level vehicle color data
- price history
- generic EMI calculation
- vehicle explainers
- ACI lead capture payload

Unavailable data:
- verified live offers/schemes
- service centers
- service cost
- bank-wise finance schemes
- exact resale values
- dealer inventory
- waiting periods
- variant-wise color availability
- exact TCO

Default rules:
- New-car budget means on-road price unless user explicitly says ex-showroom.
- Default priceBasis is "on_road".
- Default city is "new-delhi".
- Default activeOnly is true.
- 20 lakh / 20L means 2000000.
- 2 lakh down payment means 200000.
- 30k EMI means 30000 monthly EMI budget.
- If data is unavailable, choose unavailable or aci_lead_capture, never invent values.
- Internal CDrive operations should use internal_passthrough, not new-car tools.
- Conceptual car-buying questions should use vehicle_explainer.
- nextSteps should be useful, safe, and executable using allowed tools.
- Do not ask variant-wise color factual questions because only model-level color data is available.
- Do not generate queries like "2000000 lakh"; use "20 lakh".
`.trim();

export default AciPlannerSchema;
