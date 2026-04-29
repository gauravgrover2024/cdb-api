import {
  compactObject,
  digitsOnly,
  normalizeRegistration,
  normalizeText,
} from "./aiAgent.normalizers.js";

const STOP_WORDS = new Set([
  "show",
  "all",
  "case",
  "cases",
  "of",
  "the",
  "for",
  "with",
  "where",
  "what",
  "can",
  "you",
  "do",
  "ask",
  "anything",
  "help",
  "me",
  "latest",
  "insurance",
  "policy",
  "loan",
  "status",
  "approx",
  "closure",
  "amount",
  "customer",
  "vehicle",
  "full",
  "profile",
  "history",
  "price",
  "prices",
  "pricelist",
  "price list",
  "variants",
  "variant",
  "colors",
  "compare",
  "similar",
  "cars",
  "selected",
  "new",
  "how",
  "many",
  "are",
  "is",
  "there",
  "this",
  "month",
  "week",
  "pending",
  "missing",
  "active",
  "expired",
  "added",
  "dashboard",
  "report",
]);

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
];

const DATE_RANGE_KEYS = [
  ["last 30 days", "last_30_days"],
  ["last month", "last_month"],
  ["this month", "this_month"],
  ["this week", "this_week"],
  ["yesterday", "yesterday"],
  ["today", "today"],
];

const CITY_HINTS = [
  "delhi",
  "new delhi",
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

const INTENT_PRIORITY = [
  ["vehicle_city_change", /\b(change city|show .* price in|price in|on road in|on-road in)\b/],
  ["vehicle_colors", /\b(colou?rs?|color options|available colou?rs?|show colou?rs?)\b/],
  ["vehicle_comparison", /\b(compare|comparison| vs | versus |difference between)\b/],
  ["similar_cars", /\b(similar cars|alternatives|competitors|same segment|cars like|similar to)\b/],
  ["loan_disbursal_report", /\b(approved but not disbursed|approved not disbursed|pending disbursal|approval done disbursal pending|approved cases pending disbursal)\b/],
  ["loan_pending_approval_report", /\b(pending approval|approval pending|not approved|approval stage)\b/],
  ["loan_disbursed_report", /\b(disbursed cases|disbursed loans|loans disbursed)\b/],
  ["missing_registration_report", /\b(without registration|missing registration|reg(?:istration)? number missing|registration not captured|vehicle number missing|cars without number|no registration)\b/],
  ["payout_missing_report", /\b(payout missing|payout not entered|payout pending|payout blank|payout not received|receivable missing|net payout missing|cases with payout)\b/],
  ["payout_entered_report", /\b(payout entered|payout has been entered|cases have payout entered|with payout entered|payout available)\b/],
  ["payment_pending_report", /\b(payment pending|pending payment|balance pending|showroom payment|customer payment|amount pending|receivable pending)\b/],
  ["active_loan_expired_insurance_report", /\b(active loan.*expired insurance|loan active.*insurance expired|insurance expired.*loan active)\b/],
  ["latest_insurance", /\b(latest insurance|insurance of|policy of|active insurance|current policy|latest policy|insurance expiring|insurance expired)\b/],
  ["insurance_expiry_report", /\b(policies expiring|insurance due|renewal due|expired policies|active policies)\b/],
  ["loan_closure", /\b(loan closure|approx loan closure|closure amount|foreclosure|close loan|loan closing|settlement amount)\b/],
  ["loan_status", /\b(loan status|loan case|bank status|approval status|disbursal status|active loan|loan of)\b/],
  ["customer_360", /\b(customer\s*360|customer profile|full profile|show all cases of|all records of|full customer details)\b/],
  ["vehicle_360", /\b(vehicle\s*360|vehicle profile|full vehicle history|full history of car|all records of vehicle)\b/],
  ["delivery_order_report", /\b(delivery order|dealer letter|do created|do pending|delivery pending|approved loans without do|approved but no do)\b/],
  ["inspection_report", /\b(inspection|pdi|inspection report|road test|engine noise|no-go|inspection pending|inspection done)\b/],
  ["background_check_report", /\b(background check|bgc|noc|hypothecation|blacklist|ownership|transfer)\b/],
  ["challan_report", /\b(challan|traffic fine|pending challan|e-challan)\b/],
  ["rc_lookup", /\b(rc|registration certificate|vehicle registration|owner details|registration details)\b/],
  ["document_report", /\b(document|pdf|policy copy|invoice|rc copy|upload|download|document missing|sanction letter)\b/],
  ["followup_report", /\b(follow-up|follow up|callback|assign|task|due|pending follow-up)\b/],
  ["price_history_report", /\b(new variants|variants.*added|price updated|price history|updated last|new prices|variant added)\b/],
  ["vehicle_feature_answer", /\b(does|has|have|available|with)\b.*\b(sunroof|airbag|cruise|adas|camera|ventilated|automatic|dct|cvt|abs|esp|tpms|engine|mileage|boot space|ground clearance)\b/],
  ["vehicle_features", /\b(features?|specs|specifications|catalogue|catalog|brochure|engine|mileage|boot space|ground clearance)\b/],
  ["vehicle_pricelist", /\b(pricelist|price list|price|pricing|prices|rate list|on road|on-road|ex showroom|ex-showroom|variant price|new car price|new )\b/],
  ["used_car_rc_pending_report", /\b(used car.*rc|rc check pending|challan check|used car lead|procurement lead|qualified lead|seller)\b/],
  ["data_quality_workbench", /\b(data quality|data issues|missing data|cleanup workbench|quality workbench|duplicate|data issue|mismatch|blank|invalid|not captured|quality report)\b/],
  ["operations_digest", /\b(what needs attention|attention today|today.*pending|pending today|operations digest|ops digest|daily digest|workbench)\b/],
  ["finance_intelligence", /\b(finance intelligence|finance digest|receivable summary|payment intelligence|payout intelligence)\b/],
];

const parseDateRange = (lower) => {
  const now = new Date();
  const start = new Date(now);
  const end = new Date(now);
  for (const [needle, key] of DATE_RANGE_KEYS) {
    if (!lower.includes(needle)) continue;
    if (key === "today") {
      start.setHours(0, 0, 0, 0);
      end.setHours(23, 59, 59, 999);
    } else if (key === "yesterday") {
      start.setDate(start.getDate() - 1);
      end.setDate(end.getDate() - 1);
      start.setHours(0, 0, 0, 0);
      end.setHours(23, 59, 59, 999);
    } else if (key === "this_week") {
      const day = start.getDay() || 7;
      start.setDate(start.getDate() - day + 1);
      start.setHours(0, 0, 0, 0);
    } else if (key === "this_month") {
      start.setDate(1);
      start.setHours(0, 0, 0, 0);
    } else if (key === "last_month") {
      start.setMonth(start.getMonth() - 1, 1);
      start.setHours(0, 0, 0, 0);
      end.setDate(0);
      end.setHours(23, 59, 59, 999);
    } else if (key === "last_30_days") {
      start.setDate(start.getDate() - 30);
      start.setHours(0, 0, 0, 0);
    }
    return { key, start: start.toISOString(), end: end.toISOString() };
  }
  return null;
};

const detectIntent = (lower) => {
  const compact = lower.replace(/[^a-z0-9]/g, "");
  for (const [intent, pattern] of INTENT_PRIORITY) {
    if (pattern.test(lower)) return intent;
  }
  if (compact.includes("pricelist") || compact.includes("prices")) return "vehicle_pricelist";
  if (/^\d{4}$/.test(lower.trim())) return "vehicle_lookup";
  return "general_search";
};

const extractFullRegistration = (message) => {
  const compact = normalizeRegistration(message);
  const match = compact.match(/[A-Z]{2}\d{1,2}[A-Z]{1,3}\d{4}/);
  return match?.[0] || "";
};

const extractLast4 = (message) => {
  const match = normalizeText(message).match(/\b\d{4}\b/);
  return match?.[0] || "";
};

const extractModelTokens = (lower) => {
  const compact = lower.replace(/[^a-z0-9]/g, "");
  const models = MODEL_HINTS.filter((model) =>
    new RegExp(`\\b${model.replace(/\s+/g, "\\s+")}\\b`, "i").test(lower) ||
    compact.includes(model.replace(/\s+/g, "")),
  );
  return [...new Set(models)];
};

const extractName = (message, lower, models) => {
  const clean = normalizeText(message)
    .replace(/[A-Z]{2}[\s-]?\d{1,2}[\s-]?[A-Z]{1,3}[\s-]?\d{4}/gi, " ")
    .replace(/\b\d{4}\b/g, " ");
  const tokens = clean
    .split(/\s+/)
    .map((token) => token.replace(/[^a-zA-Z]/g, ""))
    .filter(Boolean);
  const modelSet = new Set(models.map((model) => model.toLowerCase()));
  const nameTokens = tokens.filter((token) => {
    const lowerToken = token.toLowerCase();
    return (
      token.length > 1 &&
      !STOP_WORDS.has(lowerToken) &&
      !MODEL_HINTS.includes(lowerToken) &&
      !MAKE_HINTS.includes(lowerToken) &&
      !modelSet.has(lowerToken)
    );
  });
  if (!nameTokens.length) return "";
  if (
    [
      "vehicle_pricelist",
      "vehicle_comparison",
      "similar_cars",
      "price_history_report",
      "vehicle_features",
      "vehicle_feature_answer",
      "vehicle_colors",
      "loan_disbursal_report",
      "loan_pending_approval_report",
      "loan_disbursed_report",
      "payout_missing_report",
      "payment_pending_report",
    ].some((term) => lower.includes(term))
  ) {
    return "";
  }
  return nameTokens.slice(0, 4).join(" ");
};

const extractCity = (lower, context = {}, filters = {}) => {
  const explicit = CITY_HINTS.find((city) => new RegExp(`\\b${city}\\b`, "i").test(lower));
  return explicit || context?.city || context?.entities?.city || filters?.city || "";
};

const extractVariant = (original, lower, models, intent) => {
  if (!/^vehicle_|^price_history_report$/.test(intent)) return "";
  const known = original.match(/\b(sx|vx|zx|zxi|vxi|alpha|delta|sigma|sportz|asta|hx\d+|s\s?opt|sx\s?opt|top|base|ivt|dct|mt|at|cvt|turbo)\b(?:\s+\b(turbo|ivt|dct|mt|at|cvt|dt|opt)\b)*/i)?.[0];
  if (known) return normalizeText(known);
  if (!/feature|spec|catalog|brochure|price|variant|does|has|have/.test(lower)) return "";
  const blocked = new Set([
    ...STOP_WORDS,
    ...MAKE_HINTS,
    ...models,
    "feature",
    "features",
    "spec",
    "specs",
    "catalogue",
    "catalog",
    "brochure",
  ]);
  const tokens = normalizeText(original)
    .split(/\s+/)
    .map((token) => token.replace(/[^a-zA-Z0-9]/g, ""))
    .filter(Boolean)
    .filter((token) => !blocked.has(token.toLowerCase()) && !MODEL_HINTS.includes(token.toLowerCase()));
  return tokens.slice(0, 4).join(" ");
};

export const parseAgentMessage = (message, context = {}, selectedEntity = null, filters = {}) => {
  const original = normalizeText(message);
  const lower = original.toLowerCase();
  const intent = detectIntent(lower);
  const models = extractModelTokens(lower);
  const make = MAKE_HINTS.find((hint) => new RegExp(`\\b${hint}\\b`, "i").test(lower)) || "";
  const explicitRegistrationNumber = extractFullRegistration(original);
  const explicitLast4 = extractLast4(original);
  const registrationNumber =
    explicitRegistrationNumber ||
    selectedEntity?.registrationNumber ||
    selectedEntity?.context?.registrationNumber ||
    context?.registrationNumber ||
    context?.entities?.registrationNumber ||
    filters?.registrationNumber ||
    "";
  const last4 =
    explicitLast4 ||
    selectedEntity?.last4 ||
    selectedEntity?.context?.last4 ||
    context?.last4 ||
    context?.entities?.last4 ||
    filters?.last4 ||
    filters?.vehicleLast4 ||
    (registrationNumber ? digitsOnly(registrationNumber).slice(-4) : "");
  const nameIntent =
    intent === "latest_insurance" ||
    intent === "loan_closure" ||
    intent === "loan_status" ||
    intent === "customer_360";
  const explicitCustomerName = nameIntent ? extractName(original, lower, models) : "";
  const hasFreshEntityInMessage = Boolean(explicitCustomerName || explicitRegistrationNumber || explicitLast4 || models.length || make);
  const contextualCustomerName =
    selectedEntity?.customerName ||
    selectedEntity?.context?.customerName ||
    context?.customerName ||
    context?.entities?.customerName ||
    filters?.customerName ||
    filters?.customer ||
    "";
  const customerName =
    explicitCustomerName ||
    (hasFreshEntityInMessage ? "" : contextualCustomerName);

  const statuses = ["pending", "missing", "expired", "active", "approved", "disbursed", "closed"].filter(
    (status) => lower.includes(status),
  );
  const featureTerm =
    [
      "sunroof",
      "adas",
      "cruise control",
      "ventilated seats",
      "camera",
      "airbag",
      "abs",
      "esp",
      "tpms",
      "automatic",
      "dct",
      "cvt",
    ].find((term) => lower.includes(term)) || "";

  return {
    message: original,
    lower,
    intent,
    confidence: intent === "general_search" ? 0.45 : 0.78,
    wantsDebug: /query plan|show plan|debug/.test(lower),
    selectedEntity,
    context,
    filters,
    dateRange: parseDateRange(lower),
    entities: compactObject({
      customerName,
      registrationNumber,
      last4,
      make,
      model: models[0] || (!hasFreshEntityInMessage ? context?.model || context?.entities?.model || filters?.model || "" : ""),
      models,
      variant: extractVariant(original, lower, models, intent) || (!hasFreshEntityInMessage ? context?.variant || context?.entities?.variant || filters?.variant || "" : ""),
      city: extractCity(lower, context, filters),
      feature: featureTerm,
    }),
    statusTerms: statuses,
    financeTerms: ["payout", "receivable", "payment", "closure", "outstanding", "disbursal"].filter((term) =>
      lower.includes(term),
    ),
    documentTerms: ["rc", "challan", "registration", "insurance", "policy"].filter((term) =>
      lower.includes(term),
    ),
  };
};
