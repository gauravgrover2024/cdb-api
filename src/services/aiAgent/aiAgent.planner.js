import { compileSemanticPlan } from "./aiAgent.semanticCompiler.js";


import { generateObject, generateText } from "ai";
import { createGoogleGenerativeAI } from "@ai-sdk/google";
import { compilePlannerRedFix } from "./aiAgent.plannerRedFixes.js";

import {
  AciPlannerSchema,
  DATA_AVAILABILITY,
  makeClarificationPlan,
  makeInternalPassthroughPlan,
  makeUnavailablePlan,
  plannerSchemaForPrompt,
  plannerSystemRules,
  sanitizePlannerPlan,
  validatePlannerPlan,
  looksLikeInternalOpsQuery,
  looksLikeNewCarQuery,
  normalizeSearchKey,
} from "./aiAgent.planSchema.js";

const DEFAULT_PLANNER_MODEL = "gemini-2.5-flash-lite";
const DEFAULT_FALLBACK_PLANNER_MODEL = "gemini-2.5-flash";
const DEFAULT_TIMEOUT_MS = 12000;
const LOW_CONFIDENCE_THRESHOLD = 0.55;

const getEnv = (key, fallback = "") => {
  const value = process.env[key];
  return value === undefined || value === null || value === ""
    ? fallback
    : value;
};

export const isAiPlannerEnabled = () =>
  String(process.env.ACI_USE_AI_PLANNER || "false").toLowerCase() === "true";

export const getPlannerModelName = () =>
  getEnv("ACI_PLANNER_MODEL", DEFAULT_PLANNER_MODEL);

export const getPlannerFallbackModelName = () =>
  getEnv("ACI_PLANNER_FALLBACK_MODEL", DEFAULT_FALLBACK_PLANNER_MODEL);

export const getPlannerTimeoutMs = () => {
  const parsed = Number(
    process.env.ACI_PLANNER_TIMEOUT_MS || DEFAULT_TIMEOUT_MS,
  );
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_TIMEOUT_MS;
};

const getGeminiApiKey = () =>
  getEnv("GEMINI_API_KEY") ||
  getEnv("GOOGLE_GENERATIVE_AI_API_KEY") ||
  getEnv("GOOGLE_API_KEY");

const createPlannerProvider = () => {
  const apiKey = getGeminiApiKey();

  if (!apiKey) {
    throw new Error(
      "Gemini planner API key missing. Set GEMINI_API_KEY or GOOGLE_GENERATIVE_AI_API_KEY.",
    );
  }

  return createGoogleGenerativeAI({
    apiKey,
  });
};

const safeJsonStringify = (value, space = 2) => {
  try {
    return JSON.stringify(value, null, space);
  } catch {
    return JSON.stringify(String(value || ""));
  }
};

const truncateText = (value = "", max = 1200) => {
  const text = String(value || "");
  return text.length > max ? `${text.slice(0, max)}…` : text;
};

const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (value === undefined || value === null || value === "") return false;
      if (Array.isArray(value)) return value.length > 0;
      if (typeof value === "object") return Object.keys(value).length > 0;
      return true;
    }),
  );

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const titleCaseModel = (value = "") => {
  const text = String(value || "").trim();

  const canonical = {
    verna: "Verna",
    city: "City",
    creta: "Creta",
    seltos: "Seltos",
    venue: "Venue",
    "venue n line": "Venue N Line",
    elevate: "Elevate",
    slavia: "Slavia",
    virtus: "Virtus",
    safari: "Safari",
    harrier: "Harrier",
    nexon: "Nexon",
    sonet: "Sonet",
    brezza: "Brezza",
    fronx: "Fronx",
    exter: "Exter",
    alcazar: "Alcazar",
    i20: "i20",
    aura: "Aura",
    amaze: "Amaze",
    xuv700: "XUV700",
    "xuv 700": "XUV700",
    xuv3xo: "XUV 3XO",
    "xuv 3xo": "XUV 3XO",
    fortuner: "Fortuner",
    "innova hycross": "Innova Hycross",
  };

  const key = normalizeSearchKey(text);
  if (canonical[key]) return canonical[key];

  return text
    .split(/\s+/)
    .map((part) =>
      /^(xuv|ivt|cvt|dct|amt|gtx|htx)$/i.test(part)
        ? part.toUpperCase()
        : part.charAt(0).toUpperCase() + part.slice(1).toLowerCase(),
    )
    .join(" ");
};

const KNOWN_MODELS = [
  "Venue N Line",
  "Innova Hycross",
  "XUV 3XO",
  "XUV700",
  "Verna",
  "City",
  "Creta",
  "Seltos",
  "Venue",
  "Elevate",
  "Slavia",
  "Virtus",
  "Safari",
  "Harrier",
  "Nexon",
  "Sonet",
  "Brezza",
  "Fronx",
  "Exter",
  "Alcazar",
  "i20",
  "Aura",
  "Amaze",
  "Fortuner",
];

const KNOWN_VARIANTS = [
  "SX(O)",
  "SX O",
  "SX IVT",
  "SX",
  "ZX CVT",
  "ZX",
  "VX",
  "V",
  "SV",
  "S",
  "E",
  "EX",
  "HTK",
  "HTX",
  "GTX",
  "GTX Plus",
  "XZ+",
  "XZ",
  "ZXI",
  "VXI",
  "LXI",
  "Alpha",
  "Zeta",
  "Delta",
  "Sigma",
  "AX5",
  "AX7",
  "MX",
  "Creative+",
  "Creative",
  "Adventure",
  "Accomplished",
];

const extractModelsFromText = (message = "") => {
  const key = ` ${normalizeSearchKey(message)} `;
  const found = [];

  for (const model of [...KNOWN_MODELS].sort((a, b) => b.length - a.length)) {
    const modelKey = normalizeSearchKey(model);
    const regex = new RegExp(`\\b${modelKey.replace(/\s+/g, "\\s+")}\\b`, "i");
    if (regex.test(key)) found.push(titleCaseModel(model));
  }

  return [...new Set(found)];
};

const extractVariantFromText = (message = "") => {
  const raw = String(message || "");

  for (const variant of KNOWN_VARIANTS.sort((a, b) => b.length - a.length)) {
    const escaped = variant.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    if (new RegExp(`\\b${escaped}\\b`, "i").test(raw)) return variant;
  }

  return "";
};

const extractCityFromText = (message = "") => {
  const text = normalizeSearchKey(message);

  if (/\b(delhi|new delhi|ncr)\b/.test(text)) return "new-delhi";
  if (/\bgurgaon|gurugram\b/.test(text)) return "gurgaon";
  if (/\bnoida\b/.test(text)) return "noida";
  if (/\bmumbai\b/.test(text)) return "mumbai";
  if (/\bbangalore|bengaluru\b/.test(text)) return "bengaluru";

  return "new-delhi";
};

const extractBudgetMax = (message = "") => {
  const text = String(message || "").toLowerCase();

  const match = text.match(
    /\b(?:under|below|upto|up to|within|around|budget)\s*(?:₹|rs\.?|inr)?\s*(\d+(?:\.\d+)?)\s*(lakh|lakhs|lac|lacs|l|cr|crore|crores)?\b/i,
  );

  if (!match) return null;

  const amount = Number(match[1]);
  const unit = String(match[2] || "").toLowerCase();

  if (!Number.isFinite(amount)) return null;

  if (["cr", "crore", "crores"].includes(unit)) {
    return Math.round(amount * 10000000);
  }

  if (["lakh", "lakhs", "lac", "lacs", "l"].includes(unit) || amount <= 300) {
    return Math.round(amount * 100000);
  }

  return Math.round(amount);
};

const extractDownPayment = (message = "") => {
  const text = String(message || "").toLowerCase();

  const match = text.match(
    /\b(?:with\s*)?(?:₹|rs\.?|inr)?\s*(\d+(?:\.\d+)?)\s*(lakh|lakhs|lac|lacs|l|k|thousand)?\s*(?:down payment|dp)\b/i,
  );

  if (!match) return null;

  const amount = Number(match[1]);
  const unit = String(match[2] || "").toLowerCase();

  if (!Number.isFinite(amount)) return null;

  if (["lakh", "lakhs", "lac", "lacs", "l"].includes(unit) || amount <= 300) {
    return Math.round(amount * 100000);
  }

  if (["k", "thousand"].includes(unit)) {
    return Math.round(amount * 1000);
  }

  return Math.round(amount);
};

const extractTenureMonths = (message = "") => {
  const text = String(message || "").toLowerCase();

  const yearMatch = text.match(/\b(\d+(?:\.\d+)?)\s*(?:year|years|yr|yrs)\b/i);
  if (yearMatch) {
    const years = Number(yearMatch[1]);
    if (Number.isFinite(years) && years > 0) return Math.round(years * 12);
  }

  const monthMatch = text.match(/\b(\d+)\s*(?:month|months|mo)\b/i);
  if (monthMatch) {
    const months = Number(monthMatch[1]);
    if (Number.isFinite(months) && months > 0) return Math.round(months);
  }

  return null;
};

const extractLoanPercent = (message = "") => {
  const text = String(message || "").toLowerCase();
  const match = text.match(/\b(\d{1,3})\s*%\s*(?:loan|funding|finance)\b/i);

  if (!match) return null;

  const value = Number(match[1]);
  return Number.isFinite(value) && value > 0 && value <= 100 ? value : null;
};

const extractRecommendationFilters = (message = "") => {
  const text = normalizeSearchKey(message);
  const filters = {};

  const budgetMax = extractBudgetMax(message);
  if (budgetMax) filters.budgetMax = budgetMax;

  if (/\bsuv|compact suv\b/.test(text)) filters.bodyType = "suv";
  if (/\bsedan\b/.test(text)) filters.bodyType = "sedan";
  if (/\bhatchback\b/.test(text)) filters.bodyType = "hatchback";
  if (/\bmpv|muv|7 seater|seven seater\b/.test(text)) filters.bodyType = "mpv";

  if (/\bautomatic|amt|cvt|dct|ivt|torque converter\b/.test(text)) {
    filters.transmission = "automatic";
  } else if (/\bmanual|mt\b/.test(text)) {
    filters.transmission = "manual";
  }

  if (/\bpetrol\b/.test(text)) filters.fuelType = "Petrol";
  if (/\bdiesel\b/.test(text)) filters.fuelType = "Diesel";
  if (/\bcng\b/.test(text)) filters.fuelType = "CNG";
  if (/\bev|electric\b/.test(text)) filters.fuelType = "Electric";
  if (/\bhybrid\b/.test(text)) filters.fuelType = "Hybrid";

  const mustHaveFeatures = [];
  if (/\bpanoramic sunroof\b/.test(text))
    mustHaveFeatures.push("panoramic sunroof");
  else if (/\bsunroof\b/.test(text)) mustHaveFeatures.push("sunroof");
  if (/\b6 airbags|six airbags\b/.test(text))
    mustHaveFeatures.push("6 airbags");
  else if (/\bairbags?\b/.test(text)) mustHaveFeatures.push("airbags");
  if (/\badas\b/.test(text)) mustHaveFeatures.push("ADAS");
  if (/\b360 camera|360 degree camera\b/.test(text))
    mustHaveFeatures.push("360 camera");
  if (/\bventilated seats?\b/.test(text))
    mustHaveFeatures.push("ventilated seats");
  if (/\bwireless charger|wireless charging\b/.test(text))
    mustHaveFeatures.push("wireless charging");

  if (mustHaveFeatures.length) filters.mustHaveFeatures = mustHaveFeatures;

  return filters;
};

const extractRanking = (message = "") => {
  const text = normalizeSearchKey(message);

  if (/\bsafest|safety|ncap|crash|airbags?\b/.test(text)) return "safety";
  if (/\bautomatic\b/.test(text) && /\bbest|value|under\b/.test(text)) {
    return "automatic_value";
  }
  if (/\bfamily|parents|senior|elderly\b/.test(text)) return "family";
  if (/\bperformance|power|fast|turbo\b/.test(text)) return "performance";
  if (/\bspace|spacious|boot|practical\b/.test(text)) return "space";
  if (/\bmileage|fuel efficient|fuel efficiency\b/.test(text))
    return "fuel_efficiency";
  if (/\bsimilar|alternative|rival\b/.test(text)) return "similarity";
  if (/\bfeatures?|sunroof|adas|airbags|camera\b/.test(text))
    return "feature_match";

  return "value";
};

const sanitizeVehicleLike = (vehicle = {}) => {
  if (!vehicle || typeof vehicle !== "object") return {};

  return compactObject({
    brand: vehicle.brand || vehicle.make || "",
    make: vehicle.make || vehicle.brand || "",
    model: vehicle.model || "",
    variant: vehicle.variant || "",
    city: vehicle.city || "",
    color: vehicle.color || vehicle.colorName || "",
    fuelType: vehicle.fuelType || vehicle.fuel || "",
    transmission: vehicle.transmission || "",
    price:
      vehicle.price ||
      vehicle.onRoadPrice ||
      vehicle.exShowroom ||
      vehicle.exShowroomPrice ||
      "",
    modelKey: vehicle.modelKey || "",
    variantKey: vehicle.variantKey || "",
  });
};

const getAnchorFromContext = ({ context = {}, selectedEntity = null } = {}) => {
  const selectedVehicle =
    selectedEntity ||
    context.selectedVehicle ||
    context.anchorVehicle ||
    context.vehicle ||
    context.currentVehicle ||
    {};

  const vehicle = sanitizeVehicleLike(selectedVehicle);

  const model =
    vehicle.model ||
    context.anchorModel ||
    context.model ||
    context.entities?.model ||
    context.selectedModels?.[0] ||
    "";

  const variant =
    vehicle.variant ||
    context.anchorVariant ||
    context.variant ||
    context.entities?.variant ||
    context.selectedVariants?.[0] ||
    "";

  const city =
    vehicle.city ||
    context.anchorCity ||
    context.city ||
    context.entities?.city ||
    "new-delhi";

  return compactObject({
    brand: vehicle.brand || context.anchorBrand || context.brand || "",
    model,
    variant,
    city,
    color: vehicle.color || context.anchorColor || "",
    selectedVehicle: {
      ...vehicle,
      model,
      variant,
      city,
    },
  });
};

const sanitizeContextForPlanner = (context = {}) => {
  const anchor = getAnchorFromContext({ context });

  const selectedComparisonSet =
    context.selectedComparisonSet ||
    context.comparisonSet ||
    context.compareContext ||
    {};

  const allowed = {
    lastIntent: context.lastIntent || context.intent || "",
    stage: context.stage || context.mode || "",
    customerStage: context.customerStage || "",
    conversationMode: context.conversationMode || "",

    anchorBrand: anchor.brand || "",
    anchorModel: anchor.model || "",
    anchorVariant: anchor.variant || "",
    anchorCity: anchor.city || "",
    anchorColor: anchor.color || "",

    selectedVehicle: sanitizeVehicleLike(anchor.selectedVehicle),

    selectedModels: Array.isArray(context.selectedModels)
      ? context.selectedModels.slice(0, 5)
      : [],
    selectedVariants: Array.isArray(context.selectedVariants)
      ? context.selectedVariants.slice(0, 5)
      : [],

    selectedComparisonSet:
      selectedComparisonSet && typeof selectedComparisonSet === "object"
        ? {
            models: asArray(selectedComparisonSet.models).slice(0, 5),
            variants: asArray(selectedComparisonSet.variants).slice(0, 5),
            selectedVariants: asArray(
              selectedComparisonSet.selectedVariants,
            ).slice(0, 5),
            comparisonLevel: selectedComparisonSet.comparisonLevel || "",
          }
        : {},

    budgetMax:
      context.budgetMax ||
      context.entities?.budgetMax ||
      context.filters?.budgetMax ||
      context.userPreferences?.budgetMax ||
      null,
    budgetMin:
      context.budgetMin ||
      context.entities?.budgetMin ||
      context.filters?.budgetMin ||
      context.userPreferences?.budgetMin ||
      null,

    userPreferences: compactObject({
      preferredBudget:
        context.userPreferences?.preferredBudget ||
        context.profile?.preferredBudget ||
        null,
      preferredBodyType:
        context.userPreferences?.preferredBodyType ||
        context.profile?.preferredBodyType ||
        "",
      preferredFuel:
        context.userPreferences?.preferredFuel ||
        context.profile?.preferredFuel ||
        "",
      preferredTransmission:
        context.userPreferences?.preferredTransmission ||
        context.profile?.preferredTransmission ||
        "",
      buyingPriority:
        context.userPreferences?.buyingPriority ||
        context.profile?.buyingPriority ||
        "",
      usage: context.userPreferences?.usage || context.usage || "",
      priority: context.userPreferences?.priority || context.priority || "",
    }),

    leadContext:
      context.leadContext && typeof context.leadContext === "object"
        ? compactObject({
            leadType: context.leadContext.leadType || "",
            selectedServices: asArray(context.leadContext.selectedServices),
            customerName: context.leadContext.customerName || "",
            mobile: context.leadContext.mobile || "",
            city: context.leadContext.city || "",
          })
        : {},

    history: context.history || {},
  };

  return compactObject(allowed);
};

const sanitizeSelectedEntityForPlanner = (selectedEntity = null) => {
  if (!selectedEntity || typeof selectedEntity !== "object") return null;

  return compactObject({
    entityType: selectedEntity.entityType || selectedEntity.type || "",
    brand: selectedEntity.brand || selectedEntity.make || "",
    make: selectedEntity.make || selectedEntity.brand || "",
    model: selectedEntity.model || "",
    variant: selectedEntity.variant || "",
    city: selectedEntity.city || "",
    color: selectedEntity.color || selectedEntity.colorName || "",
    fuelType: selectedEntity.fuelType || selectedEntity.fuel || "",
    transmission: selectedEntity.transmission || "",
    price:
      selectedEntity.price ||
      selectedEntity.onRoadPrice ||
      selectedEntity.exShowroom ||
      "",
    source: selectedEntity.source || "",
  });
};

const sanitizeFiltersForPlanner = (filters = {}) => {
  if (!filters || typeof filters !== "object") return {};

  return compactObject({
    city: filters.city || "",
    budgetMin: filters.budgetMin || null,
    budgetMax: filters.budgetMax || null,
    bodyType: filters.bodyType || "",
    fuelType: filters.fuelType || "",
    transmission: filters.transmission || "",
    priceBasis: filters.priceBasis || "",
    activeOnly: filters.activeOnly,
    includeDiscontinued: filters.includeDiscontinued,
    mustHaveFeatures: Array.isArray(filters.mustHaveFeatures)
      ? filters.mustHaveFeatures.slice(0, 12)
      : [],
    compareFeatures: Array.isArray(filters.compareFeatures)
      ? filters.compareFeatures.slice(0, 12)
      : [],
    downPayment: filters.downPayment || null,
    loanPercent: filters.loanPercent || null,
    tenureMonths: filters.tenureMonths || null,
    roi: filters.roi || null,
    selectedServices: Array.isArray(filters.selectedServices)
      ? filters.selectedServices.slice(0, 8)
      : [],
  });
};

const isTestDriveRequest = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(test drive|book test|schedule test|drive the car|trial drive)\b/.test(
    text,
  );
};

const isServiceCenterRequest = (message = "") => {
  const text = normalizeSearchKey(message);
  return /\b(service center|service centre|nearest service|workshop)\b/.test(
    text,
  );
};

const isBankSchemeRequest = (message = "") => {
  const text = normalizeSearchKey(message);
  return /\b(best bank|which bank|bank offer|loan offer|roi by bank|processing fee)\b/.test(
    text,
  );
};

const isOfferRequest = (message = "") => {
  const text = normalizeSearchKey(message);
  return /\b(offer|offers|discount|scheme|cash discount|exchange bonus|corporate offer|month end)\b/.test(
    text,
  );
};

const createTool = ({
  tool,
  entities = {},
  filters = {},
  ranking = null,
  output = {},
  resolution = {},
} = {}) => ({
  tool,
  entities,
  filters,
  ranking,
  output,
  resolution,
});

const defaultOutputForTool = (tool) => {
  if (tool === "vehicle_pricelist") {
    return {
      canvasType: "pricelist_canvas",
      inlineType: null,
      groupBy: "variant",
    };
  }
  if (tool === "vehicle_colors") {
    return {
      canvasType: "color_studio_canvas",
      inlineType: null,
      groupBy: null,
    };
  }
  if (tool === "vehicle_feature_lookup") {
    return {
      canvasType: null,
      inlineType: "feature_answer_card",
      groupBy: null,
    };
  }
  if (tool === "vehicle_compare") {
    return {
      canvasType: "comparison_canvas",
      inlineType: null,
      groupBy: "variant",
    };
  }
  if (tool === "vehicle_recommend") {
    return {
      canvasType: "recommendation_results_canvas",
      inlineType: null,
      groupBy: "model",
    };
  }
  if (tool === "vehicle_emi") {
    return {
      canvasType: "emi_calculator_canvas",
      inlineType: null,
      groupBy: null,
    };
  }
  if (tool === "aci_lead_capture") {
    return {
      canvasType: "lead_capture_canvas",
      inlineType: null,
      groupBy: null,
    };
  }
  if (tool === "unavailable") {
    return {
      canvasType: null,
      inlineType: "unavailable_notice",
      groupBy: null,
    };
  }
  return { canvasType: null, inlineType: null, groupBy: null };
};

const buildLocalPlan = ({
  mode = "single_tool",
  domain = "new_car",
  conversationMode = "direct_answer",
  customerStage = "exploration",
  tools = [],
  nextSteps = [],
  ambiguity = { level: "none", type: "none", message: "" },
  contextPatch = {},
  clarification = null,
  confidence = 0.96,
  reasoningSummary = "Deterministic local planner handled this request.",
  unavailableReason = null,
} = {}) => ({
  mode,
  domain,
  conversationMode,
  customerStage,
  tools,
  nextSteps,
  ambiguity,
  contextPatch,
  clarification,
  confidence,
  reasoningSummary,
  unavailableReason,
});

const vehicleContextPatch = ({
  model = "",
  variant = "",
  city = "new-delhi",
  color = "",
  customerStage = "exploration",
  conversationMode = "direct_answer",
  extra = {},
} = {}) => ({
  anchorModel: model || "",
  anchorVariant: variant || "",
  anchorCity: city || "new-delhi",
  anchorColor: color || "",
  selectedVehicle: compactObject({ model, variant, city, color }),
  customerStage,
  conversationMode,
  ...extra,
});

const quoteNextSteps = (model = "", variant = "") => [
  {
    label: "Get quotation",
    query:
      `Get quotation for ${[model, variant].filter(Boolean).join(" ")}`.trim(),
    tool: "aci_lead_capture",
    priority: 90,
    displayStyle: "primary_cta",
    icon: "file-text",
  },
];

const createTestDriveUnavailablePlan = (message = "") => {
  const model = extractModelsFromText(message)[0] || "";
  const city = extractCityFromText(message);

  return buildLocalPlan({
    mode: "unavailable",
    domain: "new_car",
    conversationMode: "unavailable",
    customerStage: "unknown",
    tools: [
      createTool({
        tool: "unavailable",
        entities: compactObject({ model, primaryModel: model }),
        filters: compactObject({
          city,
          unavailableReason: "outside_current_scope",
        }),
        output: defaultOutputForTool("unavailable"),
        resolution: {
          variantSelectionMode: "not_required",
          selectedModels: model ? [{ model }] : [],
          changeAllowed: true,
          note: "Test-drive booking is intentionally not supported for now.",
        },
      }),
    ],
    nextSteps: model
      ? [
          ...quoteNextSteps(model),
          {
            label: "Talk to advisor",
            query: `Talk to an advisor about ${model}`,
            tool: "aci_lead_capture",
            entities: { model, leadType: "callback" },
            filters: { city, leadType: "callback" },
            priority: 75,
            displayStyle: "secondary_cta",
            icon: "phone",
          },
        ]
      : [],
    contextPatch: model
      ? vehicleContextPatch({
          model,
          city,
          customerStage: "unknown",
          conversationMode: "unavailable",
        })
      : {},
    confidence: 0.98,
    reasoningSummary:
      "Test-drive booking is outside the current ACI Assist scope.",
    unavailableReason: "outside_current_scope",
  });
};

const createUnavailablePlanForReason = ({
  message = "",
  reason = "unsupported_request",
  model = "",
  city = "new-delhi",
} = {}) =>
  buildLocalPlan({
    mode: "unavailable",
    domain: "new_car",
    conversationMode: "unavailable",
    customerStage: "unknown",
    tools: [
      createTool({
        tool: "unavailable",
        entities: compactObject({ model, primaryModel: model }),
        filters: compactObject({ city, unavailableReason: reason }),
        output: defaultOutputForTool("unavailable"),
        resolution: {
          variantSelectionMode: "not_required",
          selectedModels: model ? [{ model }] : [],
          changeAllowed: true,
          note: reason,
        },
      }),
    ],
    contextPatch: model
      ? vehicleContextPatch({
          model,
          city,
          customerStage: "unknown",
          conversationMode: "unavailable",
        })
      : {},
    confidence: 0.96,
    reasoningSummary: `Unavailable data guard handled request: ${message}`,
    unavailableReason: reason,
  });

const createLeadPlan = ({
  model = "",
  variant = "",
  city = "new-delhi",
  leadType = "quotation",
  selectedServices = [],
  unavailableReason = null,
  conversationMode = "lead_capture",
  customerStage = "closing",
} = {}) =>
  buildLocalPlan({
    mode: "single_tool",
    domain: "new_car",
    conversationMode,
    customerStage,
    tools: [
      createTool({
        tool: "aci_lead_capture",
        entities: compactObject({
          model,
          variant,
          primaryModel: model,
          primaryVariant: variant,
          leadType,
          selectedServices,
        }),
        filters: compactObject({
          city,
          leadType,
          selectedServices,
          unavailableReason,
        }),
        output: defaultOutputForTool("aci_lead_capture"),
        resolution: {
          variantSelectionMode: variant ? "exact" : "not_required",
          selectedVariants: variant ? [{ model, variant }] : [],
          selectedModels: model ? [{ model }] : [],
          changeAllowed: true,
          note: unavailableReason || "Capture lead request.",
        },
      }),
    ],
    nextSteps:
      leadType === "quotation"
        ? []
        : [
            {
              label: "Get quotation",
              query:
                `Get quotation for ${[model, variant].filter(Boolean).join(" ")}`.trim(),
              tool: "aci_lead_capture",
              priority: 90,
              displayStyle: "primary_cta",
              icon: "file-text",
            },
          ],
    contextPatch: {
      ...vehicleContextPatch({
        model,
        variant,
        city,
        customerStage,
        conversationMode,
      }),
      leadContext: compactObject({ leadType, selectedServices }),
    },
    confidence: 0.96,
    reasoningSummary: "Deterministic local planner created lead capture plan.",
    unavailableReason,
  });

const createEmiPlan = ({
  model = "",
  variant = "",
  city = "new-delhi",
  downPayment = null,
  tenureMonths = null,
  loanPercent = null,
} = {}) =>
  buildLocalPlan({
    mode: "single_tool",
    domain: "new_car",
    conversationMode: "calculation",
    customerStage: "consideration",
    tools: [
      createTool({
        tool: "vehicle_emi",
        entities: compactObject({
          model,
          variant,
          primaryModel: model,
          primaryVariant: variant,
        }),
        filters: compactObject({
          city,
          priceBasis: "on_road",
          activeOnly: true,
          downPayment,
          tenureMonths,
          loanPercent,
        }),
        output: defaultOutputForTool("vehicle_emi"),
        resolution: {
          variantSelectionMode: variant ? "exact" : "representative_default",
          selectedVariants: [
            variant
              ? { model, variant }
              : { model, variantStrategy: "popular_or_best_value" },
          ],
          selectedModels: model ? [{ model }] : [],
          changeAllowed: true,
          note: variant
            ? "Use selected variant for EMI."
            : "Use selected or representative variant for EMI.",
        },
      }),
    ],
    nextSteps: [
      {
        label: "Get quotation",
        query:
          `Get quotation for ${[model, variant].filter(Boolean).join(" ")}`.trim(),
        tool: "aci_lead_capture",
        priority: 86,
        displayStyle: "primary_cta",
        icon: "file-text",
      },
    ],
    ambiguity: variant
      ? { level: "none", type: "none", message: "" }
      : {
          level: "soft_default",
          type: "variant",
          message:
            "I’ll calculate EMI using a selected or popular variant. You can change the variant anytime.",
          selectedDefault: { variantSelectionMode: "representative_default" },
        },
    contextPatch: vehicleContextPatch({
      model,
      variant,
      city,
      customerStage: "consideration",
      conversationMode: "calculation",
    }),
    confidence: 0.96,
    reasoningSummary: "Deterministic local planner created EMI plan.",
  });

const createRecommendationPlan = ({ message = "" } = {}) => {
  const filters = extractRecommendationFilters(message);
  const ranking = extractRanking(message);
  const city = extractCityFromText(message);

  return buildLocalPlan({
    mode: "single_tool",
    domain: "new_car",
    conversationMode: "recommendation",
    customerStage: "exploration",
    tools: [
      createTool({
        tool: "vehicle_recommend",
        entities: {},
        filters: {
          city,
          priceBasis: "on_road",
          activeOnly: true,
          ...filters,
        },
        ranking,
        output: defaultOutputForTool("vehicle_recommend"),
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          changeAllowed: true,
          note: "Show model cards first with suggested representative variants.",
        },
      }),
    ],
    nextSteps: [
      {
        label:
          ranking === "safety" ? "Show automatic only" : "Show safest options",
        query:
          ranking === "safety"
            ? `Show automatic SUVs under ${
                filters.budgetMax ? filters.budgetMax / 100000 : 20
              } lakh`
            : `Show safest cars under ${
                filters.budgetMax ? filters.budgetMax / 100000 : 20
              } lakh`,
        tool: "vehicle_recommend",
        ranking: ranking === "safety" ? "automatic_value" : "safety",
        priority: 82,
        displayStyle: "row_card",
        icon: ranking === "safety" ? "calculator" : "shield",
      },
    ],
    contextPatch: {
      userPreferences: filters,
      customerStage: "exploration",
      conversationMode: "recommendation",
    },
    confidence: 0.96,
    reasoningSummary:
      "Deterministic local planner created recommendation plan.",
  });
};

const createComparePlan = ({
  models = [],
  anchorModel = "",
  anchorVariant = "",
  city = "new-delhi",
} = {}) => {
  const [primaryModel, secondaryModel] = models;

  return buildLocalPlan({
    mode: "single_tool",
    domain: "new_car",
    conversationMode: "comparison",
    customerStage: "evaluation",
    tools: [
      createTool({
        tool: "vehicle_compare",
        entities: compactObject({
          models,
          comparisonModels: models,
          primaryModel: primaryModel || anchorModel,
          primaryVariant: anchorVariant,
          variant: anchorVariant,
        }),
        filters: {
          city,
          priceBasis: "on_road",
          activeOnly: true,
        },
        output: defaultOutputForTool("vehicle_compare"),
        resolution: {
          comparisonLevel: "variant",
          variantSelectionMode: anchorVariant
            ? "representative_default"
            : "representative_default",
          selectedVariants: [
            anchorVariant
              ? { model: primaryModel || anchorModel, variant: anchorVariant }
              : {
                  model: primaryModel || anchorModel,
                  variantStrategy: "popular_automatic",
                },
            {
              model: secondaryModel,
              variantStrategy: "comparable_by_price_transmission",
            },
          ].filter((item) => item.model),
          selectedModels: models.map((model) => ({ model })),
          changeAllowed: true,
          note: "Use representative comparable variants and allow user to change.",
        },
      }),
    ],
    nextSteps: [
      {
        label: "Change variants",
        query: "Change comparison variants",
        tool: "vehicle_compare",
        priority: 90,
        displayStyle: "pill",
        icon: "compare",
        requiresSelection: true,
      },
      {
        label: "Check EMI difference",
        query: `Compare EMI for ${models.join(" and ")}`,
        tool: "vehicle_emi",
        priority: 82,
        displayStyle: "pill",
        icon: "calculator",
      },
    ],
    ambiguity: {
      level: "soft_default",
      type: "comparison_variant",
      message:
        "I’ll compare popular comparable variants for now. You can change variants anytime.",
      selectedDefault: { variantSelectionMode: "representative_default" },
    },
    contextPatch: {
      anchorModel: primaryModel || anchorModel || "",
      anchorVariant: anchorVariant || "",
      anchorCity: city,
      selectedVehicle: compactObject({
        model: primaryModel || anchorModel,
        variant: anchorVariant,
        city,
      }),
      selectedComparisonSet: {
        models,
        variantSelectionMode: "representative_default",
      },
      customerStage: "evaluation",
      conversationMode: "comparison",
    },
    confidence: 0.96,
    reasoningSummary: "Deterministic local planner created comparison plan.",
  });
};

const createPricePlan = ({ model = "", city = "new-delhi" } = {}) =>
  buildLocalPlan({
    mode: "single_tool",
    domain: "new_car",
    conversationMode: "direct_answer",
    customerStage: "exploration",
    tools: [
      createTool({
        tool: "vehicle_pricelist",
        entities: { model, primaryModel: model },
        filters: { city, priceBasis: "on_road", activeOnly: true },
        output: defaultOutputForTool("vehicle_pricelist"),
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [{ model }],
          changeAllowed: true,
          note: "Show model-level price list with variant rows.",
        },
      }),
    ],
    nextSteps: [
      {
        label: "Calculate EMI",
        query: `Calculate EMI for ${model}`,
        tool: "vehicle_emi",
        priority: 82,
        displayStyle: "pill",
        icon: "calculator",
      },
      ...quoteNextSteps(model),
    ],
    contextPatch: vehicleContextPatch({
      model,
      city,
      customerStage: "exploration",
      conversationMode: "direct_answer",
    }),
    confidence: 0.96,
    reasoningSummary: "Deterministic local planner created price-list plan.",
  });

const createMultiIntentPlan = ({ message = "" } = {}) => {
  const models = extractModelsFromText(message);
  const city = extractCityFromText(message);
  const primaryModel = models[0] || "";
  const comparisonModels =
    models.length >= 2
      ? [models[0], models[1]]
      : primaryModel
        ? [primaryModel]
        : [];
  const tenureMonths = extractTenureMonths(message);
  const downPayment = extractDownPayment(message);
  const loanPercent = extractLoanPercent(message);

  if (!primaryModel) return null;

  const tools = [
    createTool({
      tool: "vehicle_pricelist",
      entities: { model: primaryModel, primaryModel },
      filters: { city, priceBasis: "on_road", activeOnly: true },
      output: defaultOutputForTool("vehicle_pricelist"),
      resolution: {
        variantSelectionMode: "not_required",
        selectedModels: [{ model: primaryModel }],
        selectedVariants: [],
        changeAllowed: true,
        note: "Show model-level price list with variant rows.",
      },
    }),
  ];

  if (comparisonModels.length >= 2) {
    tools.push(
      createTool({
        tool: "vehicle_compare",
        entities: {
          models: comparisonModels,
          comparisonModels,
          primaryModel,
        },
        filters: { city, priceBasis: "on_road", activeOnly: true },
        output: defaultOutputForTool("vehicle_compare"),
        resolution: {
          comparisonLevel: "variant",
          variantSelectionMode: "representative_default",
          selectedVariants: comparisonModels.map((model) => ({
            model,
            variantStrategy: "popular_automatic",
          })),
          selectedModels: comparisonModels.map((model) => ({ model })),
          changeAllowed: true,
          note: "Use representative comparable variants because user gave model names only.",
        },
      }),
    );
  }

  tools.push(
    createTool({
      tool: "vehicle_emi",
      entities: { model: primaryModel, primaryModel },
      filters: compactObject({
        city,
        priceBasis: "on_road",
        activeOnly: true,
        tenureMonths,
        downPayment,
        loanPercent,
      }),
      output: defaultOutputForTool("vehicle_emi"),
      resolution: {
        variantSelectionMode: "representative_default",
        selectedVariants: [
          { model: primaryModel, variantStrategy: "popular_or_best_value" },
        ],
        selectedModels: [{ model: primaryModel }],
        changeAllowed: true,
        note: "Use selected or representative variant for EMI.",
      },
    }),
  );

  if (isOfferRequest(message)) {
    tools.push(
      createTool({
        tool: "aci_lead_capture",
        entities: {
          model: primaryModel,
          primaryModel,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        filters: {
          city,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
          unavailableReason: "offers_not_available",
        },
        output: defaultOutputForTool("aci_lead_capture"),
        resolution: {
          variantSelectionMode: "not_required",
          selectedModels: [{ model: primaryModel }],
          selectedVariants: [],
          changeAllowed: true,
          note: "Verified offers are unavailable, convert to offer enquiry lead.",
        },
      }),
    );
  }

  return buildLocalPlan({
    mode: "multi_tool",
    domain: "new_car",
    conversationMode: "comparison",
    customerStage: "evaluation",
    tools,
    nextSteps: [
      {
        label: "Choose variant",
        query: `Show ${primaryModel} variants`,
        tool: "vehicle_pricelist",
        priority: 90,
        displayStyle: "pill",
        icon: "car",
      },
      {
        label: "Get quotation",
        query: `Get quotation for ${primaryModel}`,
        tool: "aci_lead_capture",
        priority: 86,
        displayStyle: "primary_cta",
        icon: "file-text",
      },
    ],
    ambiguity:
      comparisonModels.length >= 2
        ? {
            level: "soft_default",
            type: "comparison_variant",
            message:
              "I’ll compare popular comparable variants for now. You can change variants anytime.",
            selectedDefault: { variantSelectionMode: "representative_default" },
          }
        : { level: "none", type: "none", message: "" },
    contextPatch: {
      anchorModel: primaryModel,
      anchorCity: city,
      selectedVehicle: { model: primaryModel, city },
      selectedComparisonSet:
        comparisonModels.length >= 2
          ? {
              models: comparisonModels,
              variantSelectionMode: "representative_default",
            }
          : {},
      leadContext: isOfferRequest(message)
        ? {
            leadType: "offer_enquiry",
            selectedServices: ["offer_enquiry", "quotation"],
          }
        : {},
      customerStage: "evaluation",
      conversationMode: "comparison",
    },
    confidence: 0.97,
    reasoningSummary:
      "Deterministic local planner split the multi-intent request.",
    unavailableReason: isOfferRequest(message) ? "offers_not_available" : null,
  });
};

const tryBuildLocalPlan = ({
  message = "",
  context = {},
  selectedEntity = null,
} = {}) => {
  const text = normalizeSearchKey(message);
  const models = extractModelsFromText(message);
  const model = models[0] || "";
  const variant = extractVariantFromText(message);
  const city = extractCityFromText(message);
  const anchor = getAnchorFromContext({ context, selectedEntity });

  if (isTestDriveRequest(message))
    return createTestDriveUnavailablePlan(message);

  if (isServiceCenterRequest(message)) {
    return createUnavailablePlanForReason({
      message,
      reason: "service_centers_not_available",
      model,
      city,
    });
  }

  if (isBankSchemeRequest(message)) {
    return createLeadPlan({
      model: model || anchor.model || "",
      variant: anchor.variant || "",
      city: city || anchor.city || "new-delhi",
      leadType: "finance_callback",
      selectedServices: ["finance"],
      unavailableReason: "bank_finance_schemes_not_available",
      conversationMode: "lead_capture",
      customerStage: "closing",
    });
  }

  const hasPrice =
    /\b(price|pricelist|on road|onroad|ex showroom|ex-showroom)\b/.test(text);
  const hasCompare = /\b(compare|vs|versus|better than)\b/.test(text);
  const hasEmi = /\bemi|loan\b/.test(text);
  const hasOffer = isOfferRequest(message);
  const multiIntentCount = [hasPrice, hasCompare, hasEmi, hasOffer].filter(
    Boolean,
  ).length;

  if (multiIntentCount >= 2 && model) return createMultiIntentPlan({ message });

  if (anchor.model && /\bemi\b/.test(text)) {
    return createEmiPlan({
      model: anchor.model,
      variant: anchor.variant,
      city: anchor.city,
      downPayment: extractDownPayment(message),
      tenureMonths: extractTenureMonths(message),
      loanPercent: extractLoanPercent(message),
    });
  }

  if (
    anchor.model &&
    /\b(quote|quotation|final price|best price)\b/.test(text)
  ) {
    return createLeadPlan({
      model: anchor.model,
      variant: anchor.variant,
      city: anchor.city,
      leadType: "quotation",
      selectedServices: ["quotation"],
      conversationMode: "lead_capture",
      customerStage: "closing",
    });
  }

  if (anchor.model && /\bcompare\b/.test(text) && models.length >= 1) {
    return createComparePlan({
      models: [anchor.model, models[0]].filter(Boolean),
      anchorModel: anchor.model,
      anchorVariant: anchor.variant,
      city: anchor.city,
    });
  }

  if (model && hasOffer) {
    return createLeadPlan({
      model,
      city,
      leadType: "offer_enquiry",
      selectedServices: ["offer_enquiry", "quotation"],
      unavailableReason: "offers_not_available",
      conversationMode: "lead_capture",
      customerStage: "closing",
    });
  }

  if (hasEmi && (model || anchor.model)) {
    return createEmiPlan({
      model: model || anchor.model,
      variant: model ? variant : anchor.variant,
      city,
      downPayment: extractDownPayment(message),
      tenureMonths: extractTenureMonths(message),
      loanPercent: extractLoanPercent(message),
    });
  }

  if (
    /\bsafest|safety|best|recommend|suggest|under\b/.test(text) &&
    /\bcar|cars|suv|sedan|hatchback|mpv\b/.test(text)
  ) {
    return createRecommendationPlan({ message });
  }

  if (hasPrice && model) return createPricePlan({ model, city });

  if (hasCompare && models.length >= 2) {
    return createComparePlan({ models: models.slice(0, 3), city });
  }

  return null;
};

export const classifyPlannerDomain = (message = "") => {
  const text = normalizeSearchKey(message);

  const explicitInternal =
    /\b(loan closure|loan id|approved but not disbursed|disbursed cases|total business|customer 360|payment pending|receivable|delivery order|do number|insurance renewal)\b/.test(
      text,
    );

  if (explicitInternal) return "internal";

  if (looksLikeInternalOpsQuery(message) && !looksLikeNewCarQuery(message)) {
    return "internal";
  }

  if (isTestDriveRequest(message)) return "new_car";
  if (isServiceCenterRequest(message)) return "new_car";
  if (isBankSchemeRequest(message)) return "new_car";
  if (isOfferRequest(message)) return "new_car";

  if (looksLikeNewCarQuery(message)) return "new_car";

  if (
    /\b(what is|explain|meaning|difference between|how does|why is)\b/.test(
      text,
    ) &&
    /\b(car|emi|loan|on road|ex showroom|cvt|dct|ivt|amt|adas|airbag|ncap|sunroof|insurance|rto|tcs)\b/.test(
      text,
    )
  ) {
    return "new_car";
  }

  return "unknown";
};

const buildPlannerExamples = () => [
  {
    user: "Verna pricelist",
    plan: createPricePlan({ model: "Verna", city: "new-delhi" }),
  },
  {
    user: "Compare Verna and City",
    plan: createComparePlan({ models: ["Verna", "City"], city: "new-delhi" }),
  },
  {
    user: "Best automatic SUV under 20 lakh with sunroof and 6 airbags",
    plan: createRecommendationPlan({
      message: "Best automatic SUV under 20 lakh with sunroof and 6 airbags",
    }),
  },
  {
    user: "EMI for Verna with 2 lakh down payment for 5 years",
    plan: createEmiPlan({
      model: "Verna",
      city: "new-delhi",
      downPayment: 200000,
      tenureMonths: 60,
    }),
  },
  {
    user: "Latest offers on Verna",
    plan: createLeadPlan({
      model: "Verna",
      city: "new-delhi",
      leadType: "offer_enquiry",
      selectedServices: ["offer_enquiry", "quotation"],
      unavailableReason: "offers_not_available",
    }),
  },
];

export const buildPlannerPrompt = ({
  message,
  context = {},
  selectedEntity = null,
  filters = {},
} = {}) => {
  const schema = plannerSchemaForPrompt();

  return `
User message:
${truncateText(message, 1200)}

Current safe context:
${safeJsonStringify(sanitizeContextForPlanner(context))}

Selected entity:
${safeJsonStringify(sanitizeSelectedEntityForPlanner(selectedEntity))}

Additional filters:
${safeJsonStringify(sanitizeFiltersForPlanner(filters))}

Planner schema metadata:
${safeJsonStringify(schema)}

Data availability:
${safeJsonStringify(DATA_AVAILABILITY)}

Examples:
${safeJsonStringify(buildPlannerExamples())}

Return only a JSON object matching the planner schema.

Important:
- Do not answer the user.
- Do not invent prices, offers, service centers, service costs, bank schemes, waiting periods, resale values, or dealer stock.
- Use unavailable or aci_lead_capture when the requested data is not stored.
- Use vehicle_explainer for conceptual car-buying questions.
- Use vehicle_recommend for analytical recommendation questions.
- Use nextSteps for safe follow-up questions the user can click next.
- nextSteps must be executable using allowed tools.
- Do not generate "2000000 lakh"; say "20 lakh" in nextStep query text.

No test-drive rule:
- Do not create test-drive booking plans.
- Do not use leadType "test_drive".
- Do not add "Book test drive" as a nextStep.
- If the user asks for test drive, return unavailable with unavailableReason "outside_current_scope".
- Suggest "Get quotation" or "Talk to advisor" instead.

Context memory rule:
- If user has already selected a model/variant in context, use it for follow-up questions like "EMI", "quote", "breakup", "colors", or "compare with City".
- Do not overwrite the selected vehicle unless the user explicitly names/selects another vehicle.
- Always include contextPatch when a plan establishes or updates anchorModel, anchorVariant, city, color, selectedVehicle, comparison set, preferences, or lead context.

Comparison rule:
- Model-only comparisons like "Compare Verna and City" should not block the user.
- Plan vehicle_compare using models and representative_default variant selection.
- Set ambiguity.level "soft_default", ambiguity.type "comparison_variant", and allow "Change variants".
`.trim();
};

const runWithTimeout = async (factory, timeoutMs = DEFAULT_TIMEOUT_MS) => {
  const controller = new AbortController();

  let timer = null;

  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      controller.abort();
      reject(new Error(`AI planner timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });

  try {
    return await Promise.race([factory(controller.signal), timeoutPromise]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

const parseJsonFromText = (text = "") => {
  const raw = String(text || "").trim();

  if (!raw) {
    throw new Error("Planner text fallback returned empty output.");
  }

  try {
    return JSON.parse(raw);
  } catch {
    const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (fenced?.[1]) return JSON.parse(fenced[1].trim());

    const first = raw.indexOf("{");
    const last = raw.lastIndexOf("}");

    if (first >= 0 && last > first)
      return JSON.parse(raw.slice(first, last + 1));

    throw new Error("Could not parse JSON from planner fallback text.");
  }
};

const callGeminiPlannerObject = async ({
  model,
  system,
  prompt,
  signal,
  debug = false,
}) => {
  const result = await generateObject({
    model,
    schema: AciPlannerSchema,
    system,
    prompt,
    temperature: 0,
    abortSignal: signal,
    providerOptions: {
      google: {
        structuredOutputs: true,
      },
    },
  });

  return {
    rawPlan: result.object,
    usage: result.usage || null,
    finishReason: result.finishReason || null,
    mode: "generateObject",
    debug: debug
      ? {
          warnings: result.warnings || [],
          response: result.response || null,
        }
      : undefined,
  };
};

const callGeminiPlannerTextFallback = async ({
  model,
  system,
  prompt,
  signal,
  debug = false,
}) => {
  const result = await generateText({
    model,
    system,
    prompt: `${prompt}

Return valid JSON only. Do not wrap it in markdown.`,
    temperature: 0,
    abortSignal: signal,
  });

  return {
    rawPlan: parseJsonFromText(result.text),
    usage: result.usage || null,
    finishReason: result.finishReason || null,
    mode: "generateText-json-fallback",
    debug: debug
      ? {
          text: result.text,
          warnings: result.warnings || [],
          response: result.response || null,
        }
      : undefined,
  };
};

const shouldRetryWithFallbackModel = ({ validation, plan }) => {
  const confidence = Number(plan?.confidence || 0);

  return (
    !validation?.valid ||
    confidence < LOW_CONFIDENCE_THRESHOLD ||
    plan?.tools?.length === 0
  );
};

const runPlannerWithModel = async ({
  google,
  modelName,
  system,
  prompt,
  timeoutMs,
  debug,
}) => {
  const model = google(modelName);

  let plannerResult;
  let objectError = null;

  try {
    plannerResult = await runWithTimeout(
      (signal) =>
        callGeminiPlannerObject({
          model,
          system,
          prompt,
          signal,
          debug,
        }),
      timeoutMs,
    );
  } catch (error) {
    objectError = error;

    plannerResult = await runWithTimeout(
      (signal) =>
        callGeminiPlannerTextFallback({
          model,
          system,
          prompt,
          signal,
          debug,
        }),
      timeoutMs,
    );
  }

  return {
    ...plannerResult,
    objectError,
    modelName,
  };
};

const makeLocalResult = ({
  rawPlan,
  message,
  startedAt,
  plannerMode,
  debug = false,
} = {}) => {
  const plan = sanitizePlannerPlan(rawPlan, { message });
  const validation = validatePlannerPlan(plan, { message });
  const confidence = Number(plan?.confidence || 0);

  return {
    ok: validation.valid,
    plan: validation.plan || plan,
    validation,
    provider: "local",
    model: "none",
    plannerMode,
    fallbackRequired:
      !validation.valid || confidence < LOW_CONFIDENCE_THRESHOLD,
    lowConfidence: confidence < LOW_CONFIDENCE_THRESHOLD,
    durationMs: Date.now() - startedAt,
    debug: debug
      ? {
          rawPlan,
          safeContext: {},
        }
      : undefined,
  };
};

export const buildAiPlan = async ({
  message,
  context = {},
  selectedEntity = null,
  filters = {},
  debug = false,
  force = false,
} = {}) => {
  const startedAt = Date.now();
  const cleanMessage = String(message || "").trim();


  /* ACI_PLANNER_REDFIX_PRE_ROUTE_START */

  const deterministicRedFixPlan = await compilePlannerRedFix({

    message: cleanMessage,

    context: typeof context !== "undefined" ? context : {},

    selectedEntity:

      typeof selectedEntity !== "undefined" ? selectedEntity : null,

  });


  if (

    !(typeof force !== "undefined" ? force : false) &&

    deterministicRedFixPlan

  ) {

    return {

      provider: "local",

      model: "none",

      plannerMode: "db-red-fix",

      plan: deterministicRedFixPlan,

      fallbackModelUsed: false,

      fallbackRequired: false,

      lowConfidence: false,

      durationMs:

        typeof startedAt !== "undefined" ? Date.now() - startedAt : 0,

    };

  }

  /* ACI_PLANNER_REDFIX_PRE_ROUTE_END */


  if (!cleanMessage) {
    const plan = makeClarificationPlan({
      question: "What would you like to check?",
      domain: "unknown",
      confidence: 0.8,
    });

    return {
      ok: true,
      plan,
      validation: validatePlannerPlan(plan, { message: cleanMessage }),
      provider: "local",
      model: "none",
      plannerMode: "empty-message",
      fallbackRequired: false,
      durationMs: Date.now() - startedAt,
    };
  }

  const semanticPlan = await compileSemanticPlan({
    message: cleanMessage,
    context,
    selectedEntity,
    filters,
    startedAt,
  });

  if (!force && semanticPlan) {
    return semanticPlan;
  }

  const domain = classifyPlannerDomain(cleanMessage);

  if (!force && domain === "internal") {
    const plan = makeInternalPassthroughPlan({
      domain: "internal",
      confidence: 0.95,
      summary:
        "Internal CDrive operation should use deterministic backend routing.",
    });

    return {
      ok: true,
      plan,
      validation: validatePlannerPlan(plan, { message: cleanMessage }),
      provider: "local",
      model: "none",
      plannerMode: "internal-passthrough",
      fallbackRequired: true,
      durationMs: Date.now() - startedAt,
    };
  }

  if (!force) {
    const localPlan = tryBuildLocalPlan({
      message: cleanMessage,
      context,
      selectedEntity,
    });

    if (localPlan) {
      return makeLocalResult({
        rawPlan: localPlan,
        message: cleanMessage,
        startedAt,
        plannerMode: "local-semantic-compiler",
        debug,
      });
    }
  }

  if (!force && domain === "unknown") {
    const plan = makeClarificationPlan({
      question:
        "Are you asking about a new car, pricing, comparison, EMI, quotation, or something inside CDrive?",
      domain: "unknown",
      confidence: 0.55,
    });

    return {
      ok: true,
      plan,
      validation: validatePlannerPlan(plan, { message: cleanMessage }),
      provider: "local",
      model: "none",
      plannerMode: "unknown-clarification",
      fallbackRequired: false,
      durationMs: Date.now() - startedAt,
    };
  }

  try {
    const google = createPlannerProvider();
    const modelName = getPlannerModelName();
    const fallbackModelName = getPlannerFallbackModelName();
    const system = plannerSystemRules();
    const prompt = buildPlannerPrompt({
      message: cleanMessage,
      context,
      selectedEntity,
      filters,
    });

    const timeoutMs = getPlannerTimeoutMs();

    let plannerResult = await runPlannerWithModel({
      google,
      modelName,
      system,
      prompt,
      timeoutMs,
      debug,
    });

    let plan = sanitizePlannerPlan(plannerResult.rawPlan, {
      message: cleanMessage,
    });

    let validation = validatePlannerPlan(plan, {
      message: cleanMessage,
    });

    let usedFallbackModel = false;

    if (
      fallbackModelName &&
      fallbackModelName !== modelName &&
      shouldRetryWithFallbackModel({ validation, plan })
    ) {
      try {
        const fallbackResult = await runPlannerWithModel({
          google,
          modelName: fallbackModelName,
          system,
          prompt,
          timeoutMs,
          debug,
        });

        const fallbackPlan = sanitizePlannerPlan(fallbackResult.rawPlan, {
          message: cleanMessage,
        });

        const fallbackValidation = validatePlannerPlan(fallbackPlan, {
          message: cleanMessage,
        });

        if (
          fallbackValidation.valid &&
          Number(fallbackPlan?.confidence || 0) >= Number(plan?.confidence || 0)
        ) {
          plannerResult = fallbackResult;
          plan = fallbackPlan;
          validation = fallbackValidation;
          usedFallbackModel = true;
        }
      } catch {
        // Keep primary model result if fallback fails.
      }
    }

    const confidence = Number(plan?.confidence || 0);
    const fallbackRequired =
      !validation.valid ||
      confidence < LOW_CONFIDENCE_THRESHOLD ||
      plan?.tools?.[0]?.tool === "internal_passthrough";

    return {
      ok: validation.valid,
      plan: validation.plan || plan,
      validation,
      provider: "google",
      model: plannerResult.modelName || modelName,
      plannerMode: plannerResult.mode,
      fallbackModelUsed: usedFallbackModel,
      fallbackRequired,
      lowConfidence: confidence < LOW_CONFIDENCE_THRESHOLD,
      usage: plannerResult.usage,
      finishReason: plannerResult.finishReason,
      durationMs: Date.now() - startedAt,
      debug: debug
        ? compactObject({
            domain,
            objectError: plannerResult.objectError?.message || "",
            plannerDebug: plannerResult.debug,
            rawPlan: plannerResult.rawPlan,
            primaryModel: modelName,
            fallbackModel: fallbackModelName,
            fallbackModelUsed: usedFallbackModel,
            safeContext: sanitizeContextForPlanner(context),
            selectedEntity: sanitizeSelectedEntityForPlanner(selectedEntity),
          })
        : undefined,
    };
  } catch (error) {
    const plan = makeUnavailablePlan({
      reason: "unsupported_request",
      message: "AI planner failed and deterministic fallback should be used.",
      confidence: 0,
    });

    return {
      ok: false,
      plan,
      validation: validatePlannerPlan(plan, { message: cleanMessage }),
      provider: "google",
      model: getPlannerModelName(),
      plannerMode: "error",
      fallbackRequired: true,
      error: error?.message || "AI planner failed",
      durationMs: Date.now() - startedAt,
      debug: debug
        ? {
            stack: error?.stack,
          }
        : undefined,
    };
  }
};

export const buildAiPlanOrFallback = async (args = {}) => {
  const result = await buildAiPlan(args);

  if (!result.ok || result.fallbackRequired) {
    return {
      ...result,
      shouldUseDeterministicFallback: true,
    };
  }

  return {
    ...result,
    shouldUseDeterministicFallback: false,
  };
};

export default buildAiPlan;
