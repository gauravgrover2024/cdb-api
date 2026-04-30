import {
  compactObject,
  extractVehicleLast4,
  normalizeCitySlug,
  normalizeName,
  normalizeText,
  normalizeVehicleNumber,
  parseDateRange,
} from "./aiAgent.normalizers.js";

const MODEL_HINTS = [
  "verna",
  "city",
  "slavia",
  "creta",
  "venue",
  "i20",
  "i10",
  "grand i10",
  "swift",
  "baleno",
  "brezza",
  "fronx",
  "nexon",
  "harrier",
  "seltos",
  "sonet",
  "alcazar",
  "innova",
  "fortuner",
  "thar",
  "scorpio",
  "xuv700",
  "amaze",
  "elevate",
  "ciaz",
  "hector",
];

const MAKE_HINTS = [
  "hyundai",
  "honda",
  "volkswagen",
  "skoda",
  "maruti",
  "suzuki",
  "tata",
  "mahindra",
  "kia",
  "toyota",
  "mg",
];

const FEATURE_HINTS = [
  "sunroof",
  "airbag",
  "airbags",
  "adas",
  "cruise control",
  "ventilated seats",
  "camera",
  "engine",
  "mileage",
  "boot space",
  "ground clearance",
  "automatic",
  "dct",
  "cvt",
  "abs",
  "esp",
  "tpms",
];

const CITY_HINTS = [
  "new-delhi",
  "new delhi",
  "delhi",
  "gurgaon",
  "gurugram",
  "noida",
  "mumbai",
  "pune",
  "bangalore",
  "bengaluru",
  "chandigarh",
  "faridabad",
  "ghaziabad",
];

const STOP_NAME_WORDS = new Set([
  "latest",
  "insurance",
  "policy",
  "loan",
  "closure",
  "status",
  "customer",
  "vehicle",
  "profile",
  "full",
  "cases",
  "case",
  "show",
  "all",
  "of",
  "for",
  "the",
  "with",
  "price",
  "pricelist",
  "colors",
  "features",
]);

export const INTENT_DEFINITIONS = [
  {
    intent: "vehicle_colors",
    priority: 10,
    patterns: [/\b(colou?rs?|color options|available colou?rs?|show colou?rs?)\b/i],
    collections: ["vehicle_colors"],
    requiredEntities: ["model"],
    optionalEntities: ["make"],
    widgetType: "vehicle_colors",
    failureMessage: "No stored color records matched the requested model.",
  },
  {
    intent: "vehicle_feature_answer",
    priority: 20,
    patterns: [/\b(does|has|have|available|with)\b.*\b(sunroof|airbags?|adas|mileage|boot space|ground clearance|camera|cruise|ventilated|automatic|dct|cvt|abs|esp|tpms)\b/i],
    collections: ["vehicle_features"],
    requiredEntities: ["model", "feature"],
    optionalEntities: ["make", "variant"],
    widgetType: "variant_feature_availability",
    failureMessage: "No stored feature value matched this question.",
  },
  {
    intent: "vehicle_features",
    priority: 30,
    patterns: [/\b(features?|specs|specifications|catalogue|catalog|brochure|engine|mileage|boot space|ground clearance)\b/i],
    collections: ["vehicle_features"],
    requiredEntities: ["model"],
    optionalEntities: ["make", "variant"],
    widgetType: "vehicle_features",
    failureMessage: "No stored feature catalogue matched the requested model.",
  },
  {
    intent: "vehicle_comparison",
    priority: 40,
    patterns: [/\b(compare|comparison| vs | versus |difference between)\b/i],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: ["models"],
    optionalEntities: ["city", "variant"],
    widgetType: "variant_selector",
    failureMessage: "Ask with at least two models to compare.",
  },
  {
    intent: "similar_cars",
    priority: 50,
    patterns: [/\b(similar cars|alternatives|competitors|same segment|cars like|similar to)\b/i],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: ["model"],
    optionalEntities: ["city"],
    widgetType: "similar_cars",
    failureMessage: "Ask for similar cars with a model.",
  },
  {
    intent: "vehicle_pricelist",
    priority: 60,
    patterns: [/\b(pricelist|price list|price|pricing|prices|rate list|on road|on-road|ex showroom|ex-showroom|variant price|new car price|new)\b/i],
    collections: ["vehicles"],
    requiredEntities: ["model"],
    optionalEntities: ["make", "variant", "city"],
    widgetType: "vehicle_pricelist",
    failureMessage: "Ask for a model such as Verna, City, or Slavia.",
  },
  {
    intent: "loan_disbursal_report",
    priority: 70,
    patterns: [/\b(approved but not disbursed|approved not disbursed|pending disbursal|approval done disbursal pending|approved cases pending disbursal)\b/i],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["dateRange"],
    widgetType: "records_table",
    failureMessage: "No approved but not disbursed loan cases were found.",
  },
  {
    intent: "latest_insurance",
    priority: 80,
    patterns: [/\b(latest insurance|insurance of|policy of|active insurance|current policy|latest policy)\b/i],
    collections: ["insurance"],
    requiredEntities: [],
    optionalEntities: ["customerName", "last4", "registrationNumber", "model"],
    widgetType: "insurance_case_card",
    failureMessage: "No matching insurance policy was found.",
  },
  {
    intent: "customer_360",
    priority: 90,
    patterns: [/\b(customer\s*360|customer profile|full profile|show all cases of|all records of|full customer details)\b/i],
    collections: ["customers", "loans", "insurance", "payments", "usedCarLeads"],
    requiredEntities: ["customerName"],
    optionalEntities: ["last4"],
    widgetType: "customer_360",
    failureMessage: "Ask Customer 360 with a customer name.",
  },
  {
    intent: "vehicle_360",
    priority: 100,
    patterns: [/\b(vehicle\s*360|vehicle profile|full vehicle history|full history of car|all records of vehicle)\b/i],
    collections: ["vehicle_master_records", "loans", "insurance", "payments", "deliveryOrders"],
    requiredEntities: [],
    optionalEntities: ["registrationNumber", "last4", "model"],
    widgetType: "vehicle_360",
    failureMessage: "Ask Vehicle 360 with a registration number or last 4 digits.",
  },
  {
    intent: "delivery_order_report",
    priority: 110,
    patterns: [/\b(delivery order|dealer letter|do created|do pending|delivery pending|approved loans without do|approved but no do)\b/i],
    collections: ["deliveryOrders"],
    requiredEntities: [],
    optionalEntities: ["customerName", "last4", "model"],
    widgetType: "records_table",
    failureMessage: "No delivery order records matched this request.",
  },
  {
    intent: "payment_pending_report",
    priority: 120,
    patterns: [/\b(payment pending|pending payment|balance pending|showroom payment|customer payment|amount pending|commission)\b/i],
    collections: ["payments"],
    requiredEntities: [],
    optionalEntities: ["customerName", "last4", "model"],
    widgetType: "records_table",
    failureMessage: "No payment records matched this request.",
  },
  {
    intent: "payout_missing_report",
    priority: 130,
    patterns: [/\b(payout missing|payout not entered|payout pending|payout blank|payout not received|receivable missing|net payout missing|receivable|bill missing|tds)\b/i],
    collections: ["receivables"],
    requiredEntities: [],
    optionalEntities: ["customerName", "dateRange"],
    widgetType: "payout_missing_report",
    failureMessage: "No receivable or payout records matched this request.",
  },
  {
    intent: "used_car_rc_pending_report",
    priority: 140,
    patterns: [/\b(used car lead|used car|inspection|pdi|background check|bgc|negotiation|challan|noc|hypothecation)\b/i],
    collections: ["usedCarLeads"],
    requiredEntities: [],
    optionalEntities: ["customerName", "last4", "model"],
    widgetType: "records_table",
    failureMessage: "No used-car lead records matched this request.",
  },
  {
    intent: "price_history_report",
    priority: 150,
    patterns: [/\b(new variants|variants.*added|price updated|price history|updated last|new prices|variant added)\b/i],
    collections: ["price_history"],
    requiredEntities: [],
    optionalEntities: ["model", "city", "dateRange"],
    widgetType: "price_history_report",
    failureMessage: "No price history records matched this request.",
  },
];

const sortedDefinitions = [...INTENT_DEFINITIONS].sort((a, b) => a.priority - b.priority);

export const extractModels = (lower) => {
  const compact = lower.replace(/[^a-z0-9]/g, "");
  return [
    ...new Set(
      MODEL_HINTS.filter(
        (model) =>
          new RegExp(`\\b${model.replace(/\s+/g, "\\s+")}\\b`, "i").test(lower) ||
          compact.includes(model.replace(/\s+/g, "")),
      ),
    ),
  ];
};

const extractMake = (lower) =>
  MAKE_HINTS.find((make) => new RegExp(`\\b${make}\\b`, "i").test(lower)) || "";

const extractFeature = (lower) =>
  FEATURE_HINTS.find((feature) => new RegExp(`\\b${feature.replace(/\s+/g, "\\s+")}\\b`, "i").test(lower)) || "";

const extractCity = (lower, context = {}, filters = {}) => {
  const explicit = CITY_HINTS.find((city) => new RegExp(`\\b${city.replace("-", "\\s*-?\\s*")}\\b`, "i").test(lower));
  return normalizeCitySlug(explicit || context?.city || context?.entities?.city || filters?.city || "");
};

const extractVariant = (message, lower) =>
  normalizeText(
    message.match(/\b(sx|vx|zx|zxi|vxi|alpha|delta|sigma|sportz|asta|hx\d+|s\s?opt|sx\s?opt|top|base|ivt|dct|mt|at|cvt|turbo)\b(?:\s+\b(turbo|ivt|dct|mt|at|cvt|dt|opt)\b)*/i)?.[0] || "",
  );

const extractCustomerName = (message, intent, models) => {
  if (!["latest_insurance", "loan_status", "loan_closure", "customer_360", "vehicle_360"].includes(intent)) return "";
  const text = normalizeText(message)
    .replace(/[A-Z]{2}[\s-]?\d{1,2}[\s-]?[A-Z]{1,3}[\s-]?\d{4}/gi, " ")
    .replace(/\b\d{4}\b/g, " ");
  const modelSet = new Set(models.map((item) => item.toLowerCase()));
  const tokens = text
    .split(/\s+/)
    .map((token) => token.replace(/[^a-zA-Z]/g, ""))
    .filter(Boolean)
    .filter((token) => {
      const lowerToken = token.toLowerCase();
      return !STOP_NAME_WORDS.has(lowerToken) && !modelSet.has(lowerToken) && !MAKE_HINTS.includes(lowerToken);
    });
  return normalizeName(tokens.slice(0, 4).join(" "));
};

export const routeAiAgentIntent = ({ message = "", context = {}, selectedEntity = null, filters = {} } = {}) => {
  const text = normalizeText(message);
  const lower = text.toLowerCase();
  const matchedDefinition = sortedDefinitions.find((definition) =>
    definition.patterns.some((pattern) => pattern.test(text)),
  );
  const intent = matchedDefinition?.intent || "generic_search";
  const models = extractModels(lower);
  const registrationNumber =
    normalizeVehicleNumber(text).match(/[A-Z]{2}\d{1,2}[A-Z]{1,3}\d{4}/)?.[0] ||
    selectedEntity?.registrationNumber ||
    selectedEntity?.context?.registrationNumber ||
    context?.registrationNumber ||
    context?.entities?.registrationNumber ||
    filters?.registrationNumber ||
    "";
  const last4 =
    extractVehicleLast4(text) ||
    selectedEntity?.last4 ||
    selectedEntity?.context?.last4 ||
    context?.last4 ||
    context?.entities?.last4 ||
    filters?.last4 ||
    filters?.vehicleLast4 ||
    extractVehicleLast4(registrationNumber);
  const hasFreshVehicleContext = Boolean(models.length || extractMake(lower) || registrationNumber || last4);
  const model = models[0] || (!hasFreshVehicleContext ? context?.model || context?.entities?.model || filters?.model || "" : "");
  const variant = extractVariant(text, lower) || (!hasFreshVehicleContext ? context?.variant || context?.entities?.variant || filters?.variant || "" : "");

  return {
    intent,
    definition: matchedDefinition || null,
    confidence: matchedDefinition ? 0.9 : 0.35,
    entities: compactObject({
      selectedEntityId: selectedEntity?.id || selectedEntity?._id,
      selectedEntityType: selectedEntity?.entityType,
      customerName: extractCustomerName(text, intent, models) || (!hasFreshVehicleContext ? context?.customerName || context?.entities?.customerName || filters?.customerName || filters?.customer || "" : ""),
      registrationNumber,
      last4,
      make: extractMake(lower),
      model,
      models,
      variant,
      city: extractCity(lower, context, filters),
      feature: extractFeature(lower),
      dateRange: parseDateRange(lower),
    }),
    collections: matchedDefinition?.collections || [],
    widgetType: matchedDefinition?.widgetType || "records_table",
    failureMessage: matchedDefinition?.failureMessage || "I could not identify the exact request.",
    structured: Boolean(matchedDefinition),
  };
};
