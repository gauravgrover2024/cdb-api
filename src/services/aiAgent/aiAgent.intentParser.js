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
  if (/data quality|data issues|missing data|cleanup workbench|quality workbench/.test(lower)) {
    return "data_quality_workbench";
  }
  if (/what needs attention|attention today|today.*pending|pending today|operations digest|ops digest|daily digest|workbench/.test(lower)) {
    return "operations_digest";
  }
  if (/finance intelligence|finance digest|receivable summary|payment intelligence|payout intelligence/.test(lower)) {
    return "finance_intelligence";
  }
  if (/customer\s*360|all cases|full profile/.test(lower)) return "customer_360";
  if (/vehicle\s*360|full history/.test(lower)) return "vehicle_360";
  if (/latest insurance|policy of|insurance expiring|insurance of/.test(lower)) {
    return "latest_insurance";
  }
  if (/loan closure|approx loan closure|closure amount|foreclosure/.test(lower)) {
    return "loan_closure";
  }
  if (/loan status|loan of/.test(lower)) return "loan_status";
  if (/without registration|registration number.*missing|vehicle number.*missing|rc.*missing/.test(lower)) {
    return "missing_registration_report";
  }
  if (/payout.*missing|payout.*pending|payout not entered|cases with payout/.test(lower)) {
    return "payout_missing_report";
  }
  if (/new variants|variants.*added|price history/.test(lower)) return "price_history_report";
  if (/compare\b/.test(lower)) return "vehicle_comparison";
  if (/similar cars|similar .* to/.test(lower)) return "similar_cars";
  if (/(does|has|have|available|with).*(sunroof|airbag|cruise|adas|camera|ventilated|automatic|dct|cvt|abs|esp|tpms)|\b(sunroof|adas|cruise control|ventilated seats)\b/.test(lower)) {
    return "vehicle_feature_availability";
  }
  if (
    /\b(price|pricing|prices|pricelist|variants|colors|colours|sunroof)\b|price list|on[-\s]?road|ex[-\s]?showroom|new /.test(lower) ||
    compact.includes("pricelist") ||
    compact.includes("prices")
  ) {
    return "vehicle_pricelist";
  }
  if (/active loan.*expired insurance|loan active.*insurance expired/.test(lower)) {
    return "active_loan_expired_insurance_report";
  }
  if (/used car.*rc|rc check pending|challan check/.test(lower)) {
    return "used_car_rc_pending_report";
  }
  if (/^\d{4}$/.test(lower.trim())) return "vehicle_360";
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
  if (["vehicle_pricelist", "vehicle_comparison", "similar_cars", "price_history_report"].some((term) => lower.includes(term))) {
    return "";
  }
  return nameTokens.slice(0, 4).join(" ");
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
    intent === "customer_360" ||
    intent === "general_search";
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
      variant:
        original.match(/\b(sx|vx|zx|zxi|vxi|alpha|delta|sigma|sportz|asta)\b/i)?.[0] || "",
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
