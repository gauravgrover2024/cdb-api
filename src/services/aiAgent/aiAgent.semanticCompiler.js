import {
  sanitizePlannerPlan,
  validatePlannerPlan,
  normalizeSearchKey,
  normalizeCity,
} from "./aiAgent.planSchema.js";

import { resolveVehicleEntities } from "./aiAgent.vehicleEntityIndex.js";

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};


const amountToRupees = (value, rawUnit = "", kind = "budget") => {
  const number = Number(value);

  if (!Number.isFinite(number) || number <= 0) return undefined;

  const unit = normalizeSearchKey(rawUnit);

  if (/\b(cr|crore|crores)\b/.test(unit)) {
    return Math.round(number * 10000000);
  }

  if (/\b(lakh|lakhs|lac|lacs|l)\b/.test(unit)) {
    return Math.round(number * 100000);
  }

  if (/\b(k|thousand)\b/.test(unit)) {
    return Math.round(number * 1000);
  }

  // In car-buying language, "under 20" and "2 down payment"
  // normally mean lakh unless user explicitly gives a larger rupee amount.
  if (["budget", "downPayment"].includes(kind) && number <= 300) {
    return Math.round(number * 100000);
  }

  return Math.round(number);
};

const moneyFromText = (message = "", kind = "budget") => {
  const text = String(message || "").toLowerCase().replace(/,/g, " ");

  if (kind === "budget") {
    const match =
      text.match(
        /\b(?:under|below|upto|up to|less than|within|around)\s*(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|thousand|k)?\b/i,
      ) ||
      text.match(
        /\b(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l)\s*(?:budget|range)?\b/i,
      );

    if (!match) return undefined;

    return amountToRupees(match[1], match[2] || "lakh", "budget");
  }

  if (kind === "downPayment") {
    const match =
      text.match(
        /\b(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|thousand|k)?\s*(?:down\s*payment|dp)\b/i,
      ) ||
      text.match(
        /\b(?:down\s*payment|dp)\s*(?:of|is|=|as)?\s*(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|thousand|k)?\b/i,
      );

    if (!match) return undefined;

    return amountToRupees(match[1], match[2] || "lakh", "downPayment");
  }

  return undefined;
};


const tenureFromText = (message = "") => {
  const text = String(message || "").toLowerCase();

  const years = text.match(/\b(\d+(?:\.\d+)?)\s*(?:year|years|yr|yrs)\b/i);
  if (years) return Math.round(Number(years[1]) * 12);

  const months = text.match(/\b(\d+)\s*(?:month|months|mo)\b/i);
  if (months) return Math.round(Number(months[1]));

  return undefined;
};

const loanPercentFromText = (message = "") => {
  const text = String(message || "").toLowerCase();

  const match = text.match(/\b(\d{1,3})\s*%\s*(?:loan|finance|funding)?\b/i);
  if (!match) return undefined;

  const value = Number(match[1]);
  if (!Number.isFinite(value) || value <= 0 || value > 100) return undefined;

  return value;
};

const detectFeatures = (message = "") => {
  const text = normalizeSearchKey(message);
  const features = [];

  const add = (condition, label) => {
    if (condition && !features.includes(label)) features.push(label);
  };

  add(/\bpanoramic\s+sunroof\b/.test(text), "panoramic sunroof");
  add(/\bsunroof\b/.test(text), "sunroof");
  add(/\b6\s+airbags?\b/.test(text), "6 airbags");
  add(/\bairbags?\b/.test(text) && !/\b6\s+airbags?\b/.test(text), "airbags");
  add(/\badas\b/.test(text), "ADAS");
  add(/\b360\b.*\bcamera\b|\bcamera\b.*\b360\b/.test(text), "360 camera");
  add(/\brear\s+camera\b/.test(text), "rear camera");
  add(/\bventilated\s+seats?\b/.test(text), "ventilated seats");
  add(/\bwireless\s+charg/.test(text), "wireless charging");
  add(/\bcruise\s+control\b/.test(text), "cruise control");
  add(/\btpms\b/.test(text), "TPMS");
  add(/\besc\b/.test(text), "ESC");
  add(/\bisofix\b/.test(text), "ISOFIX");
  add(/\bapple\s+carplay\b/.test(text), "Apple CarPlay");
  add(/\bandroid\s+auto\b/.test(text), "Android Auto");
  add(/\bboot\s+space\b/.test(text), "boot space");
  add(/\bground\s+clearance\b/.test(text), "ground clearance");
  add(/\bmileage\b/.test(text), "mileage");
  add(/\bpower\b/.test(text), "power");
  add(/\btorque\b/.test(text), "torque");

  return features;
};

const detectBodyType = (message = "") => {
  const text = normalizeSearchKey(message);

  if (/\bcompact\s+suv\b/.test(text)) return "suv";
  if (/\bsuv\b/.test(text)) return "suv";
  if (/\bsedan\b/.test(text)) return "sedan";
  if (/\bhatchback\b/.test(text)) return "hatchback";
  if (/\bmpv|muv|7 seater|seven seater|6 seater|six seater\b/.test(text)) {
    return "mpv";
  }

  return undefined;
};

const detectTransmission = (message = "") => {
  const text = normalizeSearchKey(message);

  if (/\bautomatic|amt|cvt|dct|ivt|torque converter|at\b/.test(text)) {
    return "automatic";
  }

  if (/\bmanual|mt\b/.test(text)) return "manual";

  return undefined;
};

const detectFuelType = (message = "") => {
  const text = normalizeSearchKey(message);

  if (/\bdiesel\b/.test(text)) return "diesel";
  if (/\bcng\b/.test(text)) return "cng";
  if (/\bpetrol\b/.test(text)) return "petrol";
  if (/\belectric|ev\b/.test(text)) return "electric";
  if (/\bhybrid\b/.test(text)) return "hybrid";

  return undefined;
};

const hasSafetyIntent = (message = "") =>
  /\b(safest|safety|safer|ncap|crash|child safety|adult safety)\b/.test(
    normalizeSearchKey(message),
  );

const hasColorIntent = (message = "") =>
  /\b(colors?|colours?|paint|shade|grey|gray|black|white|red|blue|silver|pearl|titan)\b/.test(
    normalizeSearchKey(message),
  );

const hasFeatureIntent = (message = "") =>
  detectFeatures(message).length > 0 &&
  /\b(does|do|has|have|get|gets|comes|available|which|show)\b/.test(
    normalizeSearchKey(message),
  );

const hasCompareIntent = (message = "") =>
  /\b(compare|vs|versus|better than)\b/.test(normalizeSearchKey(message)) ||
  /^\s*compare\s+with\b/i.test(message);

const hasPriceIntent = (message = "") =>
  /\b(price|pricelist|price list|on road|on-road|ex showroom|ex-showroom|cost)\b/.test(
    normalizeSearchKey(message),
  );

const hasEmiIntent = (message = "") =>
  /\b(emi|loan|finance|down payment|dp)\b/.test(normalizeSearchKey(message));

const hasOfferIntent = (message = "") =>
  /\b(offer|offers|discount|scheme|cash discount|exchange bonus|corporate offer)\b/.test(
    normalizeSearchKey(message),
  );

const hasBankFinanceIntent = (message = "") =>
  /\b(best bank|which bank|bank offer|loan offer|roi by bank|processing fee)\b/.test(
    normalizeSearchKey(message),
  );

const hasServiceCenterIntent = (message = "") =>
  /\b(service center|service centre|nearest service|workshop)\b/.test(
    normalizeSearchKey(message),
  );

const hasTestDriveIntent = (message = "") =>
  /\b(test drive|book test|schedule test|trial drive)\b/.test(
    normalizeSearchKey(message),
  );

const isRecommendationIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return (
    /\b(best|suggest|recommend|find|show)\b/.test(text) &&
    /\b(car|cars|suv|sedan|hatchback|automatic|manual|under|below|sunroof|airbags|adas|mileage|family|parents|safe|safest)\b/.test(
      text,
    )
  );
};

const selectedVehicleFromContext = (context = {}, selectedEntity = null) => {
  const selected =
    selectedEntity ||
    context.selectedVehicle ||
    context.anchorVehicle ||
    context.vehicle ||
    {};

  return {
    brand: selected.brand || selected.make || context.anchorBrand || "",
    model: selected.model || context.anchorModel || context.model || "",
    variant: selected.variant || context.anchorVariant || context.variant || "",
    city: selected.city || context.anchorCity || context.city || "new-delhi",
    color: selected.color || context.anchorColor || "",
  };
};

const baseTool = ({
  tool,
  model,
  variant,
  city = "new-delhi",
  filters = {},
  entities = {},
  ranking = null,
  output = {},
  resolution = {},
}) => ({
  tool,
  entities: {
    ...(model ? { model, primaryModel: model } : {}),
    ...(variant ? { variant, primaryVariant: variant } : {}),
    ...entities,
  },
  filters: {
    city,
    activeOnly: true,
    ...filters,
  },
  ranking,
  output,
  resolution,
});

const contextPatch = ({
  model = "",
  variant = "",
  city = "new-delhi",
  selectedComparisonSet = {},
  userPreferences = {},
  leadContext = {},
  customerStage = "unknown",
  conversationMode = "direct_answer",
} = {}) => ({
  anchorModel: model || "",
  anchorVariant: variant || "",
  anchorCity: city || "new-delhi",
  selectedVehicle: model
    ? {
        model,
        ...(variant ? { variant } : {}),
        city: city || "new-delhi",
      }
    : {},
  selectedComparisonSet,
  userPreferences,
  leadContext,
  customerStage,
  conversationMode,
});

const buildResult = ({ rawPlan, message, startedAt }) => {
  const plan = sanitizePlannerPlan(rawPlan, { message });
  const validation = validatePlannerPlan(plan, { message });

  return {
    ok: validation.valid,
    plan: validation.plan || plan,
    validation,
    provider: "local",
    model: "none",
    plannerMode: "db-semantic-compiler",
    fallbackRequired: false,
    lowConfidence: false,
    durationMs: Date.now() - startedAt,
  };
};


/* ACI_FULL_REGRESSION_FIXES_START */

const detectCity = (message = "", fallback = "new-delhi") => {
  const text = normalizeSearchKey(message);

  if (/\b(new delhi|delhi|ncr)\b/.test(text)) return "new-delhi";
  if (/\bgurgaon|gurugram\b/.test(text)) return "gurgaon";
  if (/\bnoida\b/.test(text)) return "noida";
  if (/\bghaziabad\b/.test(text)) return "ghaziabad";
  if (/\bfaridabad\b/.test(text)) return "faridabad";
  if (/\bmumbai\b/.test(text)) return "mumbai";
  if (/\bbangalore|bengaluru\b/.test(text)) return "bengaluru";
  if (/\bpune\b/.test(text)) return "pune";
  if (/\bhyderabad\b/.test(text)) return "hyderabad";
  if (/\bchennai\b/.test(text)) return "chennai";
  if (/\bkolkata\b/.test(text)) return "kolkata";
  if (/\bjaipur\b/.test(text)) return "jaipur";
  if (/\bahmedabad\b/.test(text)) return "ahmedabad";
  if (/\blucknow\b/.test(text)) return "lucknow";
  if (/\bchandigarh\b/.test(text)) return "chandigarh";

  return fallback || "new-delhi";
};

const detectPriceBasis = (message = "") => {
  const text = normalizeSearchKey(message);

  if (/\bex showroom|exshowroom|ex-showroom\b/.test(text)) return "ex_showroom";
  if (/\bon road|on-road|final price|road price\b/.test(text)) return "on_road";

  return "on_road";
};

const sanitizeVariantForPhrase = (variant = "", message = "") => {
  const text = normalizeSearchKey(message);
  const v = String(variant || "").trim();

  if (!v) return "";

  // Prevent "ex-showroom" from becoming variant EX.
  if (normalizeSearchKey(v) === "ex" && /\bex showroom|exshowroom|ex-showroom\b/.test(text)) {
    return "";
  }

  // "base model price" is a ranking request, not variant = Base.
  if (normalizeSearchKey(v) === "base" && /\bbase model|base variant|base price\b/.test(text)) {
    return "";
  }

  // "top model price" is a ranking request, not variant = Top.
  if (normalizeSearchKey(v) === "top" && /\btop model|top variant|top price\b/.test(text)) {
    return "";
  }

  return v;
};

const detectMonthlyEmiBudget = (message = "") => {
  const raw = String(message || "").toLowerCase().replace(/,/g, " ");
  const text = normalizeSearchKey(message);

  const match =
    raw.match(/\bemi\s*(?:under|below|upto|up to|around|near|within)\s*(\d+(?:\.\d+)?)\s*(k|thousand)?\b/i) ||
    raw.match(/\bmonthly\s+budget\s*(?:is|of|=|around|near|within)?\s*(\d+(?:\.\d+)?)\s*(k|thousand)?\b/i) ||
    raw.match(/\b(\d+(?:\.\d+)?)\s*(k|thousand)?\s*(?:emi|monthly emi|monthly budget)\b/i);

  if (!match) return undefined;

  const value = Number(match[1]);
  if (!Number.isFinite(value) || value <= 0) return undefined;

  if (match[2] || value <= 500) return Math.round(value * 1000);

  if (/\bemi under|emi below|monthly budget\b/.test(text)) return Math.round(value);

  return undefined;
};

const downPaymentPercentFromText = (message = "") => {
  const text = String(message || "").toLowerCase();

  const match =
    text.match(/\b(\d{1,3})\s*%\s*(?:down\s*payment|dp)\b/i) ||
    text.match(/\b(?:down\s*payment|dp)\s*(?:of|is|=|as)?\s*(\d{1,3})\s*%\b/i);

  if (!match) return undefined;

  const value = Number(match[1]);
  if (!Number.isFinite(value) || value <= 0 || value >= 100) return undefined;

  return value;
};

const detectBreakupIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(price breakup|on road breakup|on-road breakup|breakup|break up|rto charges?|insurance amount|insurance charge|other charges?|optional charges?|handling charges?|fastag|tcs)\b/.test(text);
};

const detectExplainerTopic = (message = "") => {
  const text = normalizeSearchKey(message);

  if (/\bon road.*ex showroom|ex showroom.*on road|why.*on road.*higher/.test(text)) return "on_road_vs_ex_showroom";
  if (/\boptional charges?|optional accessories?\b/.test(text)) return "optional_charges";
  if (/\bother charges?\b/.test(text)) return "other_charges";
  if (/\brto|road tax|registration charge|registration cost\b/.test(text)) return "rto";
  if (/\btcs\b/.test(text)) return "tcs";
  if (/\bzero dep|zero depreciation\b/.test(text)) return "zero_dep";
  if (/\binsurance\b/.test(text) && /\bwhat|explain|zero|cover|included|finance|premium\b/.test(text)) return "insurance";
  if (/\bemi|loan|finance|down payment|interest rate|roi|tenure|pre approved|pre-approved|cibil|documents|required documents|minimum down payment|100 loan|100% loan|eligibility|self employed|salary|lease\b/.test(text)) return "emi";
  if (/\bivt\b/.test(text)) return "ivt";
  if (/\bcvt\b/.test(text)) return "cvt";
  if (/\bdct\b/.test(text)) return "dct";
  if (/\bamt\b/.test(text)) return "amt";
  if (/\btorque converter\b/.test(text)) return "torque_converter";
  if (/\badas\b/.test(text)) return "adas";
  if (/\bncap|crash test|safety rating\b/.test(text)) return "ncap";
  if (/\bairbags?\b/.test(text)) return "airbags";
  if (/\bsunroof\b/.test(text)) return "sunroof";
  if (/\bpetrol.*diesel|diesel.*petrol|cng.*petrol|petrol.*cng|ev.*petrol|hybrid.*petrol|fuel type\b/.test(text)) return "petrol_vs_diesel";
  if (/\bmanual.*automatic|automatic.*manual|transmission\b/.test(text)) return "transmission";
  if (/\bbh series|bh registration|temporary registration|number plate|register.*another state|corporate registration|company name|registration documents?\b/.test(text)) return "rto";
  if (/\bbooking amount|booking refundable|how do i book|book online|proforma invoice|reserve.*color|reserve.*variant|pre book|pre-book|after booking\b/.test(text)) return "quotation";
  if (/\bwarranty|extended warranty|rsa|roadside assistance|service interval|first service|service due|service package|accessories\b/.test(text)) return "ownership_cost";
  if (/\bexchange.*work|documents.*exchange|outstanding loan|old car|trade in|trade-in\b/.test(text)) return "resale";

  return "";
};

const hasLocalExplainerIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return Boolean(detectExplainerTopic(message)) &&
    /\b(what is|explain|meaning|how does|why|can i|is|are|does|do|should i|manual vs|petrol vs|cng vs|diesel worth|documents|required|booking|warranty|registration|bh series)\b/.test(text);
};

const hasSecurityInjectionIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(ignore previous instructions|print the prompt|show dealer profit|internal discount|hidden inventory|showroom margin|admin only|admin-only|leak customer|raw prices|bypass|override routing|developer message|system prompt)\b/.test(text);
};

const hasLeadCaptureIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(get quote|get quotation|quotation|quote|best price|final price|call me|talk to advisor|advisor|i want to buy|buy this car|purchase|lock deal|book this car|proforma invoice)\b/.test(text);
};

const hasFinanceLeadIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(finance eligibility|check eligibility|finance callback|loan callback|pre approved|pre-approved|low cibil|self employed|100 loan|100% loan|car loan|need loan)\b/.test(text);
};

const hasExchangeLeadIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(exchange|trade in|trade-in|old car|valuation)\b/.test(text);
};

const hasInsuranceLeadIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(insurance quote|insurance quotation|zero dep insurance|renew insurance|insurance premium)\b/.test(text);
};

const hasAvailabilityIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(available now|availability|in stock|stock|waiting period|waiting time|delivery time|immediate delivery|delivery this week|shortest waiting|this month stock|reserve color|reserve variant)\b/.test(text);
};

const hasServiceCostIntent = (message = "") => {
  const text = normalizeSearchKey(message);
  return /\b(service cost|maintenance cost|annual maintenance|service interval|first service|service due|service package)\b/.test(text);
};

const hasServiceOnlyOosIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(book my car service|service booking|noise issue|ac is not cooling|check engine|diagnose|not starting|roadside assistance|accident claim|claim support|body shop|repair)\b/.test(text);
};

const hasAlternativeIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(similar cars?|alternatives?|alternative to|cars like|cheaper alternative|premium alternative|better alternative)\b/.test(text);
};

const hasVariantLogicIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(which .*variant|best .*variant|best value|worth paying|worth over|worth it|extra features?|what do i lose|lower variant|base model|top model|cheapest variant|variant has|variant is good|is .* enough)\b/.test(text);
};

const hasUseCaseComparisonIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(which is better for city|which is better for highway|which is better for family|which is better for long trips|which is better for bad roads|lower maintenance|rear seat comfort|mileage in traffic|easier to drive|senior citizens)\b/.test(text);
};

const hasHinglishOrShorthandIntent = (message = "") => {
  const text = normalizeSearchKey(message);

  return /\b(bhai|kitni|kitna|ka price|me on road|mein on road|loan possible|same as above|same model|that one|black available|top model ka price)\b/.test(text);
};

const buildLocalResult = ({ rawPlan, cleanMessage, startedAt }) =>
  buildResult({ rawPlan, message: cleanMessage, startedAt });

const makeExplainerPlan = ({
  topic,
  cleanMessage,
  startedAt,
  model = "",
  variant = "",
  city = "new-delhi",
  confidence = 0.98,
}) => {
  const rawPlan = {
    mode: "single_tool",
    domain: "new_car",
    conversationMode: "education",
    customerStage: "exploration",
    tools: [
      baseTool({
        tool: "vehicle_explainer",
        model,
        variant,
        city,
        entities: { topic },
        filters: {},
        output: { inlineType: "explainer_card" },
        resolution: {
          variantSelectionMode: "not_required",
          selectedModels: model ? [{ model }] : [],
          selectedVariants: variant ? [{ model, variant }] : [],
          changeAllowed: true,
          note: `Explain ${topic}.`,
        },
      }),
    ],
    nextSteps: model
      ? [
          {
            label: "Show price",
            query: `Show ${model}${variant ? ` ${variant}` : ""} price`,
            tool: "vehicle_pricelist",
            entities: { model, ...(variant ? { variant } : {}) },
            filters: { city },
            priority: 75,
            displayStyle: "pill",
            icon: "tag",
          },
        ]
      : [],
    ambiguity: { level: "none", type: "none", message: "" },
    contextPatch: contextPatch({
      model,
      variant,
      city,
      customerStage: "exploration",
      conversationMode: "education",
    }),
    confidence,
    reasoningSummary: `User asked an explainer question for ${topic}.`,
  };

  return buildLocalResult({ rawPlan, cleanMessage, startedAt });
};

const makeUnavailableLocalPlan = ({
  reason,
  cleanMessage,
  startedAt,
  model = "",
  variant = "",
  city = "new-delhi",
  note = "",
}) => {
  const rawPlan = {
    mode: "unavailable",
    domain: "new_car",
    conversationMode: "unavailable",
    customerStage: "unknown",
    tools: [
      baseTool({
        tool: "unavailable",
        model,
        variant,
        city,
        filters: { unavailableReason: reason },
        output: { inlineType: "unavailable_notice" },
        resolution: {
          variantSelectionMode: variant ? "exact" : "not_required",
          selectedModels: model ? [{ model }] : [],
          selectedVariants: variant ? [{ model, variant }] : [],
          changeAllowed: true,
          note: note || reason,
        },
      }),
    ],
    nextSteps: model
      ? [
          {
            label: "Get quotation",
            query: `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
            tool: "aci_lead_capture",
            entities: { model, ...(variant ? { variant } : {}), leadType: "quotation" },
            filters: { city, leadType: "quotation" },
            priority: 80,
            displayStyle: "primary_cta",
            icon: "file-text",
          },
        ]
      : [],
    ambiguity: { level: "none", type: "none", message: "" },
    contextPatch: contextPatch({
      model,
      variant,
      city,
      customerStage: "unknown",
      conversationMode: "unavailable",
    }),
    confidence: 0.98,
    reasoningSummary: note || reason,
    unavailableReason: reason,
  };

  return buildLocalResult({ rawPlan, cleanMessage, startedAt });
};

const makeLeadPlan = ({
  leadType = "quotation",
  selectedServices = [],
  cleanMessage,
  startedAt,
  model = "",
  variant = "",
  city = "new-delhi",
  customerStage = "closing",
}) => {
  const services = selectedServices.length ? selectedServices : [leadType];

  const rawPlan = {
    mode: "single_tool",
    domain: "new_car",
    conversationMode: "lead_capture",
    customerStage,
    tools: [
      baseTool({
        tool: "aci_lead_capture",
        model,
        variant,
        city,
        entities: {
          leadType,
          selectedServices: services,
        },
        filters: {
          leadType,
          selectedServices: services,
        },
        output: { canvasType: "lead_capture_canvas" },
        resolution: {
          variantSelectionMode: variant ? "exact" : "not_required",
          selectedModels: model ? [{ model }] : [],
          selectedVariants: variant ? [{ model, variant }] : [],
          changeAllowed: true,
          note: `Lead capture for ${leadType}.`,
        },
      }),
    ],
    nextSteps: [],
    ambiguity: {
      level: model ? "none" : "ask_user",
      type: model ? "none" : "model",
      message: model ? "" : "Which car should I use for this request?",
    },
    contextPatch: contextPatch({
      model,
      variant,
      city,
      leadContext: {
        leadType,
        selectedServices: services,
      },
      customerStage,
      conversationMode: "lead_capture",
    }),
    confidence: 0.98,
    reasoningSummary: `User wants ${leadType}.`,
  };

  return buildLocalResult({ rawPlan, cleanMessage, startedAt });
};

/* ACI_FULL_REGRESSION_FIXES_END */


export const compileSemanticPlan = async ({
  message,
  context = {},
  selectedEntity = null,
  filters = {},
  startedAt = Date.now(),
} = {}) => {
  const cleanMessage = String(message || "").trim();
  const text = normalizeSearchKey(cleanMessage);
  if (!cleanMessage) return null;

  const selectedVehicle = selectedVehicleFromContext(context, selectedEntity);

  const resolved = await resolveVehicleEntities({
    message: cleanMessage,
    context,
    selectedEntity,
  });

  const model = resolved.primaryModel || selectedVehicle.model || "";
  const brand = resolved.primaryBrand || selectedVehicle.brand || "";
  let variant = resolved.primaryVariant || selectedVehicle.variant || "";
  const city =
    filters.city ||
    resolved.primaryCity ||
    selectedVehicle.city ||
    (/(\bdelhi\b|\bnew delhi\b|\bncr\b)/.test(text)
      ? "new-delhi"
      : "new-delhi");

  const features = detectFeatures(cleanMessage);
  const budgetMax = moneyFromText(cleanMessage, "budget");
  const downPayment = moneyFromText(cleanMessage, "downPayment");
  const tenureMonths = tenureFromText(cleanMessage);
  const loanPercent = loanPercentFromText(cleanMessage);
  const bodyType = detectBodyType(cleanMessage);
  const transmission = detectTransmission(cleanMessage);
  const fuelType = detectFuelType(cleanMessage);

  /* ACI_FULL_REGRESSION_OVERRIDES_START */

  const explicitCity = detectCity(cleanMessage, resolved.primaryCity || selectedVehicle.city || "new-delhi");
  const priceBasis = detectPriceBasis(cleanMessage);
  const cleanVariant = sanitizeVariantForPhrase(variant, cleanMessage);
  const monthlyEmiBudget = detectMonthlyEmiBudget(cleanMessage);
  const downPaymentPercent = downPaymentPercentFromText(cleanMessage);
  const effectiveLoanPercent =
    downPaymentPercent !== undefined
      ? 100 - downPaymentPercent
      : loanPercent;
  const explainerTopic = detectExplainerTopic(cleanMessage);

  // Security/prompt injection must never route to data tools.
  if (hasSecurityInjectionIntent(cleanMessage)) {
    return makeUnavailableLocalPlan({
      reason: "unsupported_request",
      cleanMessage,
      startedAt,
      city: explicitCity,
      note: "Security-sensitive or prompt-injection style request.",
    });
  }

  // Pure service/repair/diagnostic support is outside current new-car scope.
  if (hasServiceOnlyOosIntent(cleanMessage)) {
    return makeUnavailableLocalPlan({
      reason: "outside_current_scope",
      cleanMessage,
      startedAt,
      model,
      variant: cleanVariant,
      city: explicitCity,
      note: "Repair/service support is outside current ACI Assist scope.",
    });
  }

  // Availability/waiting/stock/inventory is not available.
  if (hasAvailabilityIntent(cleanMessage)) {
    return makeUnavailableLocalPlan({
      reason: "dealer_inventory_not_available",
      cleanMessage,
      startedAt,
      model,
      variant: cleanVariant,
      city: explicitCity,
      note: "Dealer inventory/waiting-period data is not available.",
    });
  }

  // Service cost is unavailable; warranty/service concepts can be explained.
  if (hasServiceCostIntent(cleanMessage)) {
    return makeUnavailableLocalPlan({
      reason: "service_cost_not_available",
      cleanMessage,
      startedAt,
      model,
      variant: cleanVariant,
      city: explicitCity,
      note: "Service-cost data is not available.",
    });
  }

  // Deterministic explainers: IVT/CVT/DCT/ADAS/RTO/TCS/registration/booking/warranty etc.
  if (explainerTopic && hasLocalExplainerIntent(cleanMessage)) {
    return makeExplainerPlan({
      topic: explainerTopic,
      cleanMessage,
      startedAt,
      model,
      variant: cleanVariant,
      city: explicitCity,
    });
  }

  // Price-breakup must win over generic price/pricelist.
  if (detectBreakupIntent(cleanMessage) && (model || selectedVehicle.model)) {
    const anchorModel = model || selectedVehicle.model;
    const anchorVariant = cleanVariant || selectedVehicle.variant || "";

    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "direct_answer",
      customerStage: "consideration",
      tools: [
        baseTool({
          tool: "vehicle_price_breakup",
          model: anchorModel,
          variant: anchorVariant,
          city: explicitCity,
          filters: { priceBasis: "on_road" },
          output: { canvasType: "price_breakup_canvas" },
          resolution: {
            variantSelectionMode: anchorVariant ? "exact" : "representative_default",
            selectedModels: [{ model: anchorModel }],
            selectedVariants: anchorVariant
              ? [{ model: anchorModel, variant: anchorVariant }]
              : [{ model: anchorModel, variantStrategy: "popular_or_best_value" }],
            changeAllowed: true,
            note: "Show on-road price breakup.",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Calculate EMI",
          query: `Calculate EMI for ${anchorModel}${anchorVariant ? ` ${anchorVariant}` : ""}`,
          tool: "vehicle_emi",
          entities: { model: anchorModel, ...(anchorVariant ? { variant: anchorVariant } : {}) },
          filters: { city: explicitCity },
          priority: 80,
          displayStyle: "pill",
          icon: "calculator",
        },
      ],
      ambiguity: {
        level: anchorVariant ? "none" : "soft_default",
        type: anchorVariant ? "none" : "variant",
        message: anchorVariant
          ? ""
          : "I’ll show breakup using a selected or popular variant. You can change the variant anytime.",
        selectedDefault: anchorVariant ? null : { variantSelectionMode: "representative_default" },
      },
      contextPatch: contextPatch({
        model: anchorModel,
        variant: anchorVariant,
        city: explicitCity,
        customerStage: "consideration",
        conversationMode: "direct_answer",
      }),
      confidence: 0.98,
      reasoningSummary: "User asked for on-road price breakup/charge details.",
    };

    return buildLocalResult({ rawPlan, cleanMessage, startedAt });
  }

  // EMI monthly budget / affordability.
  if (monthlyEmiBudget !== undefined && !model) {
    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "recommendation",
      customerStage: "exploration",
      tools: [
        baseTool({
          tool: "vehicle_recommend",
          city: explicitCity,
          filters: {
            monthlyEmiBudget,
            priceBasis: "on_road",
          },
          ranking: "value",
          output: { canvasType: "recommendation_results_canvas", groupBy: "model" },
          resolution: {
            variantSelectionMode: "not_required",
            selectedVariants: [],
            selectedModels: [],
            changeAllowed: true,
            note: "Recommend cars fitting monthly EMI budget.",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Add down payment",
          query: `Find cars with EMI under ${monthlyEmiBudget} and 2 lakh down payment`,
          tool: "vehicle_recommend",
          filters: { monthlyEmiBudget, city: explicitCity },
          priority: 75,
          displayStyle: "pill",
          icon: "wallet",
        },
      ],
      ambiguity: { level: "none", type: "none", message: "" },
      contextPatch: contextPatch({
        city: explicitCity,
        userPreferences: { monthlyEmiBudget },
        customerStage: "exploration",
        conversationMode: "recommendation",
      }),
      confidence: 0.98,
      reasoningSummary: "User asked for cars by monthly EMI budget.",
    };

    return buildLocalResult({ rawPlan, cleanMessage, startedAt });
  }

  // Lead capture / quote / advisor / buy.
  if (hasLeadCaptureIntent(cleanMessage) && (model || selectedVehicle.model)) {
    const anchorModel = model || selectedVehicle.model;
    const anchorVariant = cleanVariant || selectedVehicle.variant || "";

    let leadType = "quotation";
    let services = ["quotation"];

    if (hasFinanceLeadIntent(cleanMessage)) {
      services.push("finance");
    }

    if (hasExchangeLeadIntent(cleanMessage)) {
      services.push("exchange");
    }

    if (hasInsuranceLeadIntent(cleanMessage)) {
      services.push("insurance");
    }

    if (/\btalk to advisor|advisor|call me|callback\b/.test(normalizeSearchKey(cleanMessage)) && !/\bquote|quotation\b/.test(normalizeSearchKey(cleanMessage))) {
      leadType = "callback";
      services = ["callback"];
    }

    return makeLeadPlan({
      leadType,
      selectedServices: [...new Set(services)],
      cleanMessage,
      startedAt,
      model: anchorModel,
      variant: anchorVariant,
      city: explicitCity,
      customerStage: "closing",
    });
  }

  // Finance callback/eligibility when no quote wording.
  if (hasFinanceLeadIntent(cleanMessage)) {
    return makeLeadPlan({
      leadType: "finance_callback",
      selectedServices: ["finance"],
      cleanMessage,
      startedAt,
      model,
      variant: cleanVariant,
      city: explicitCity,
      customerStage: "closing",
    });
  }

  // Exchange valuation.
  if (hasExchangeLeadIntent(cleanMessage)) {
    return makeLeadPlan({
      leadType: "exchange_valuation",
      selectedServices: ["exchange"],
      cleanMessage,
      startedAt,
      model,
      variant: cleanVariant,
      city: explicitCity,
      customerStage: "closing",
    });
  }

  // Insurance quote.
  if (hasInsuranceLeadIntent(cleanMessage) && !explainerTopic) {
    return makeLeadPlan({
      leadType: "insurance_quote",
      selectedServices: ["insurance"],
      cleanMessage,
      startedAt,
      model,
      variant: cleanVariant,
      city: explicitCity,
      customerStage: "closing",
    });
  }

  // Alternatives / similar cars.
  if (hasAlternativeIntent(cleanMessage) && model) {
    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "recommendation",
      customerStage: "evaluation",
      tools: [
        baseTool({
          tool: "vehicle_recommend",
          model,
          city: explicitCity,
          filters: {
            priceBasis: "on_road",
          },
          entities: {
            model,
            primaryModel: model,
          },
          ranking: "similarity",
          output: { canvasType: "recommendation_results_canvas", groupBy: "model" },
          resolution: {
            variantSelectionMode: "not_required",
            selectedModels: [{ model }],
            selectedVariants: [],
            changeAllowed: true,
            note: "Find similar or alternative cars.",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Compare alternatives",
          query: `Compare ${model} with alternatives`,
          tool: "vehicle_compare",
          entities: { model },
          filters: { city: explicitCity },
          priority: 80,
          displayStyle: "pill",
          icon: "compare",
        },
      ],
      ambiguity: { level: "none", type: "none", message: "" },
      contextPatch: contextPatch({
        model,
        city: explicitCity,
        customerStage: "evaluation",
        conversationMode: "recommendation",
      }),
      confidence: 0.98,
      reasoningSummary: "User asked for alternatives/similar cars.",
    };

    return buildLocalResult({ rawPlan, cleanMessage, startedAt });
  }

  // Variant advisor / upgrade value / value variant.
  if (hasVariantLogicIntent(cleanMessage) && (model || selectedVehicle.model)) {
    const anchorModel = model || selectedVehicle.model;
    const anchorVariant = cleanVariant || selectedVehicle.variant || "";

    const isDiff =
      /\b(compare|difference|extra|lose|worth|over|lower variant|paying)\b/.test(normalizeSearchKey(cleanMessage));

    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: isDiff ? "comparison" : "recommendation",
      customerStage: "evaluation",
      tools: [
        baseTool({
          tool: isDiff ? "vehicle_compare" : "vehicle_recommend",
          model: anchorModel,
          variant: anchorVariant,
          city: explicitCity,
          filters: {
            priceBasis: "on_road",
            ...(transmission ? { transmission } : {}),
          },
          ranking: isDiff ? "variant_value" : "variant_value",
          output: {
            canvasType: isDiff ? "comparison_canvas" : "recommendation_results_canvas",
            groupBy: "variant",
          },
          resolution: {
            comparisonLevel: isDiff ? "variant" : null,
            variantSelectionMode: anchorVariant ? "exact" : "representative_default",
            selectedModels: [{ model: anchorModel }],
            selectedVariants: anchorVariant
              ? [{ model: anchorModel, variant: anchorVariant }]
              : [{ model: anchorModel, variantStrategy: "best_value" }],
            changeAllowed: true,
            note: isDiff
              ? "Compare variants for upgrade/downgrade value."
              : "Find best-value variant.",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Calculate EMI",
          query: `Calculate EMI for ${anchorModel}${anchorVariant ? ` ${anchorVariant}` : ""}`,
          tool: "vehicle_emi",
          entities: { model: anchorModel, ...(anchorVariant ? { variant: anchorVariant } : {}) },
          filters: { city: explicitCity },
          priority: 75,
          displayStyle: "pill",
          icon: "calculator",
        },
      ],
      ambiguity: {
        level: anchorVariant ? "none" : "soft_default",
        type: anchorVariant ? "none" : "variant",
        message: anchorVariant
          ? ""
          : "I’ll use best-value/popular variants first. You can change variants anytime.",
        selectedDefault: anchorVariant ? null : { variantSelectionMode: "representative_default" },
      },
      contextPatch: contextPatch({
        model: anchorModel,
        variant: anchorVariant,
        city: explicitCity,
        customerStage: "evaluation",
        conversationMode: isDiff ? "comparison" : "recommendation",
      }),
      confidence: 0.98,
      reasoningSummary: "User asked for variant advice or variant upgrade value.",
    };

    return buildLocalResult({ rawPlan, cleanMessage, startedAt });
  }

  // Use-case comparison with selected/comparison models.
  if (hasUseCaseComparisonIntent(cleanMessage) && (resolved.comparisonModels.length >= 2 || selectedVehicle.model)) {
    const comparisonModels =
      resolved.comparisonModels.length >= 2
        ? resolved.comparisonModels
        : selectedVehicle.model && model && selectedVehicle.model !== model
          ? [selectedVehicle.model, model]
          : resolved.comparisonModels;

    if (comparisonModels.length >= 2) {
      const rawPlan = {
        mode: "single_tool",
        domain: "new_car",
        conversationMode: "comparison",
        customerStage: "evaluation",
        tools: [
          baseTool({
            tool: "vehicle_compare",
            model: comparisonModels[0],
            variant: selectedVehicle.variant || cleanVariant,
            city: explicitCity,
            entities: {
              models: comparisonModels,
              comparisonModels,
              compareFeatures: ["use case", "comfort", "mileage", "maintenance", "space", "safety"],
            },
            filters: { priceBasis: "on_road" },
            output: { canvasType: "comparison_canvas", groupBy: "variant" },
            resolution: {
              comparisonLevel: "variant",
              variantSelectionMode: "representative_default",
              selectedModels: comparisonModels.map((item) => ({ model: item })),
              selectedVariants: comparisonModels.map((item, index) =>
                index === 0 && selectedVehicle.variant
                  ? { model: item, variant: selectedVehicle.variant }
                  : { model: item, variantStrategy: "comparable_by_price_transmission" },
              ),
              changeAllowed: true,
              note: "Use representative variants for use-case comparison.",
            },
          }),
        ],
        nextSteps: [],
        ambiguity: {
          level: "soft_default",
          type: "comparison_variant",
          message: "I’ll compare popular comparable variants for now. You can change variants anytime.",
          selectedDefault: { variantSelectionMode: "representative_default" },
        },
        contextPatch: contextPatch({
          model: comparisonModels[0],
          variant: selectedVehicle.variant || "",
          city: explicitCity,
          selectedComparisonSet: {
            models: comparisonModels,
            variantSelectionMode: "representative_default",
          },
          customerStage: "evaluation",
          conversationMode: "comparison",
        }),
        confidence: 0.98,
        reasoningSummary: "User asked comparison by use case.",
      };

      return buildLocalResult({ rawPlan, cleanMessage, startedAt });
    }
  }

  /* ACI_FULL_REGRESSION_OVERRIDES_END */



  if (hasTestDriveIntent(cleanMessage)) {
    const rawPlan = {
      mode: "unavailable",
      domain: "new_car",
      conversationMode: "unavailable",
      customerStage: "unknown",
      tools: [
        baseTool({
          tool: "unavailable",
          model,
          variant: "",
          city,
          filters: {
            unavailableReason: "outside_current_scope",
          },
          output: {
            inlineType: "unavailable_notice",
          },
          resolution: {
            variantSelectionMode: "not_required",
            selectedModels: model ? [{ model }] : [],
            note: "Test-drive booking is intentionally not supported for now.",
          },
        }),
      ],
      nextSteps: model
        ? [
            {
              label: "Get quotation",
              query: `Get quotation for ${model}`,
              tool: "aci_lead_capture",
              entities: { model, leadType: "quotation" },
              filters: { city, leadType: "quotation" },
              priority: 90,
              displayStyle: "primary_cta",
              icon: "file-text",
            },
          ]
        : [],
      ambiguity: { level: "none", type: "none", message: "" },
      contextPatch: contextPatch({
        model,
        city,
        customerStage: "unknown",
        conversationMode: "unavailable",
      }),
      confidence: 0.98,
      reasoningSummary: "Test-drive booking is not supported for now.",
      unavailableReason: "outside_current_scope",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  if (hasServiceCenterIntent(cleanMessage)) {
    const rawPlan = {
      mode: "unavailable",
      domain: "new_car",
      conversationMode: "unavailable",
      customerStage: "unknown",
      tools: [
        baseTool({
          tool: "unavailable",
          model,
          city,
          filters: {
            unavailableReason: "service_centers_not_available",
          },
          output: {
            inlineType: "unavailable_notice",
          },
          resolution: {
            variantSelectionMode: "not_required",
            note: "service_centers_not_available",
          },
        }),
      ],
      nextSteps: [],
      ambiguity: { level: "none", type: "none", message: "" },
      contextPatch: contextPatch({
        model,
        city,
        customerStage: "unknown",
        conversationMode: "unavailable",
      }),
      confidence: 0.96,
      reasoningSummary: "Service center data is not available.",
      unavailableReason: "service_centers_not_available",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  if (hasBankFinanceIntent(cleanMessage) && model) {
    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "lead_capture",
      customerStage: "closing",
      tools: [
        baseTool({
          tool: "aci_lead_capture",
          model,
          variant,
          city,
          entities: {
            leadType: "finance_callback",
            selectedServices: ["finance"],
          },
          filters: {
            leadType: "finance_callback",
            selectedServices: ["finance"],
          },
          output: { canvasType: "lead_capture_canvas" },
          resolution: {
            variantSelectionMode: variant ? "exact" : "not_required",
            selectedModels: [{ model }],
            selectedVariants: variant ? [{ model, variant }] : [],
            note: "bank_finance_schemes_not_available",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Get quotation",
          query: `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
          tool: "aci_lead_capture",
          entities: {
            model,
            ...(variant ? { variant } : {}),
            leadType: "quotation",
          },
          filters: { city, leadType: "quotation" },
          priority: 80,
          displayStyle: "primary_cta",
          icon: "file-text",
        },
      ],
      ambiguity: { level: "none", type: "none", message: "" },
      contextPatch: contextPatch({
        model,
        variant,
        city,
        leadContext: {
          leadType: "finance_callback",
          selectedServices: ["finance"],
        },
        customerStage: "closing",
        conversationMode: "lead_capture",
      }),
      confidence: 0.96,
      reasoningSummary:
        "Bank-wise finance schemes are unavailable; create finance callback lead.",
      unavailableReason: "bank_finance_schemes_not_available",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  if (model && variant && hasColorIntent(cleanMessage)) {
    const rawPlan = {
      mode: "unavailable",
      domain: "new_car",
      conversationMode: "unavailable",
      customerStage: "evaluation",
      tools: [
        baseTool({
          tool: "unavailable",
          model,
          variant,
          city,
          filters: {
            unavailableReason: "variant_wise_color_not_available",
          },
          output: { inlineType: "unavailable_notice" },
          resolution: {
            variantSelectionMode: "exact",
            selectedModels: [{ model }],
            selectedVariants: [{ model, variant }],
            note: "variant_wise_color_not_available",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Show model colors",
          query: `Show colors of ${model}`,
          tool: "vehicle_colors",
          entities: { model },
          filters: { city },
          priority: 85,
          displayStyle: "pill",
          icon: "paintbrush",
        },
        {
          label: "Get quotation",
          query: `Get quotation for ${model} ${variant}`,
          tool: "aci_lead_capture",
          entities: { model, variant, leadType: "quotation" },
          filters: { city, leadType: "quotation" },
          priority: 75,
          displayStyle: "primary_cta",
          icon: "file-text",
        },
      ],
      ambiguity: { level: "none", type: "none", message: "" },
      contextPatch: contextPatch({
        model,
        variant,
        city,
        customerStage: "evaluation",
        conversationMode: "unavailable",
      }),
      confidence: 0.98,
      reasoningSummary:
        "Variant-wise color data is not available; do not make factual color claim.",
      unavailableReason: "variant_wise_color_not_available",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  if (
    model &&
    /\b(show|see|available|which|what)\b/.test(text) &&
    /\b(colors?|colours?|paint|shade)\b/.test(text)
  ) {
    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "direct_answer",
      customerStage: "exploration",
      tools: [
        baseTool({
          tool: "vehicle_colors",
          model,
          city,
          output: { canvasType: "color_studio_canvas" },
          resolution: {
            variantSelectionMode: "not_required",
            selectedModels: [{ model }],
            note: "Show model-level colors only.",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Show price",
          query: `Show ${model} price`,
          tool: "vehicle_pricelist",
          entities: { model },
          filters: { city },
          priority: 80,
          displayStyle: "pill",
          icon: "tag",
        },
      ],
      ambiguity: { level: "none", type: "none", message: "" },
      contextPatch: contextPatch({
        model,
        city,
        customerStage: "exploration",
        conversationMode: "direct_answer",
      }),
      confidence: 0.98,
      reasoningSummary: "User wants model-level colors.",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  if (model && features.length && hasFeatureIntent(cleanMessage)) {
    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "direct_answer",
      customerStage: "evaluation",
      tools: [
        baseTool({
          tool: "vehicle_feature_lookup",
          model,
          variant,
          city,
          entities: { features },
          output: { inlineType: "feature_answer_card" },
          resolution: {
            variantSelectionMode: variant ? "exact" : "needs_user_selection",
            selectedModels: [{ model }],
            selectedVariants: variant ? [{ model, variant }] : [],
            note: variant
              ? "Feature lookup for selected variant."
              : "Feature lookup may need variant selection.",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Show all features",
          query: `Show features of ${model}${variant ? ` ${variant}` : ""}`,
          tool: "vehicle_feature_lookup",
          entities: { model, ...(variant ? { variant } : {}) },
          filters: { city },
          priority: 85,
          displayStyle: "pill",
          icon: "sparkles",
        },
        {
          label: "Show price",
          query: `Show ${model}${variant ? ` ${variant}` : ""} price`,
          tool: "vehicle_pricelist",
          entities: { model, ...(variant ? { variant } : {}) },
          filters: { city },
          priority: 75,
          displayStyle: "pill",
          icon: "tag",
        },
      ],
      ambiguity: {
        level: variant ? "none" : "ask_user",
        type: variant ? "none" : "variant",
        message: variant
          ? ""
          : "Which variant should I check this feature for?",
      },
      contextPatch: contextPatch({
        model,
        variant,
        city,
        customerStage: "evaluation",
        conversationMode: "direct_answer",
      }),
      confidence: 0.98,
      reasoningSummary: "User asked for feature availability.",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }


  const primaryIntents = [
    hasPriceIntent(cleanMessage),
    hasCompareIntent(cleanMessage),
    hasEmiIntent(cleanMessage),
    hasOfferIntent(cleanMessage),
  ].filter(Boolean).length;

  if (model && primaryIntents >= 2) {
    const tools = [];

    if (hasPriceIntent(cleanMessage)) {
      tools.push(
        baseTool({
          tool: "vehicle_pricelist",
          model,
          variant,
          city,
          filters: { priceBasis: "on_road" },
          output: {
            canvasType: "pricelist_canvas",
            groupBy: variant ? "none" : "variant",
          },
          resolution: {
            variantSelectionMode: variant ? "exact" : "not_required",
            selectedModels: [{ model }],
            selectedVariants: variant ? [{ model, variant }] : [],
            changeAllowed: true,
            note: variant
              ? "Show variant price if exact variant resolves."
              : "Show model-level price list with variant rows.",
          },
        }),
      );
    }

    let comparisonModels = resolved.comparisonModels || [];

    if (hasCompareIntent(cleanMessage)) {
      if (comparisonModels.length < 2) {
        const selectedModel = selectedVehicle.model || model;
        comparisonModels = [selectedModel, ...comparisonModels].filter(Boolean);
      }

      comparisonModels = [...new Set(comparisonModels)];

      if (comparisonModels.length >= 2) {
        tools.push(
          baseTool({
            tool: "vehicle_compare",
            model: comparisonModels[0],
            variant: comparisonModels[0] === model ? variant : "",
            city,
            entities: {
              models: comparisonModels,
              comparisonModels,
            },
            filters: { priceBasis: "on_road" },
            output: { canvasType: "comparison_canvas", groupBy: "variant" },
            resolution: {
              comparisonLevel: "variant",
              variantSelectionMode: "representative_default",
              selectedModels: comparisonModels.map((item) => ({ model: item })),
              selectedVariants: comparisonModels.map((item, index) =>
                index === 0 && variant
                  ? { model: item, variant }
                  : {
                      model: item,
                      variantStrategy:
                        index === 0
                          ? "popular_automatic"
                          : "comparable_by_price_transmission",
                    },
              ),
              changeAllowed: true,
              note: "Use representative comparable variants and allow user to change.",
            },
          }),
        );
      }
    }

    if (hasEmiIntent(cleanMessage)) {
      tools.push(
        baseTool({
          tool: "vehicle_emi",
          model,
          variant,
          city,
          filters: {
            priceBasis: "on_road",
            ...(downPayment ? { downPayment } : {}),
            ...(tenureMonths ? { tenureMonths } : {}),
            ...(effectiveLoanPercent ? { loanPercent: effectiveLoanPercent } : {}),
          },
          output: { canvasType: "emi_calculator_canvas" },
          resolution: {
            variantSelectionMode: variant ? "exact" : "representative_default",
            selectedModels: [{ model }],
            selectedVariants: variant
              ? [{ model, variant }]
              : [{ model, variantStrategy: "popular_or_best_value" }],
            changeAllowed: true,
            note: variant
              ? "Use selected variant for EMI."
              : "Use selected or representative variant for EMI.",
          },
        }),
      );
    }

    if (hasOfferIntent(cleanMessage)) {
      tools.push(
        baseTool({
          tool: "aci_lead_capture",
          model,
          variant,
          city,
          entities: {
            leadType: "offer_enquiry",
            selectedServices: ["offer_enquiry", "quotation"],
          },
          filters: {
            leadType: "offer_enquiry",
            selectedServices: ["offer_enquiry", "quotation"],
          },
          output: { canvasType: "lead_capture_canvas" },
          resolution: {
            variantSelectionMode: variant ? "exact" : "not_required",
            selectedModels: [{ model }],
            selectedVariants: variant ? [{ model, variant }] : [],
            changeAllowed: true,
            note: "offers_not_available",
          },
        }),
      );
    }

    if (tools.length >= 2) {
      const comparisonModelsForContext =
        comparisonModels && comparisonModels.length >= 2 ? comparisonModels : [];

      const rawPlan = {
        mode: "multi_tool",
        domain: "new_car",
        conversationMode: hasCompareIntent(cleanMessage)
          ? "comparison"
          : hasEmiIntent(cleanMessage)
            ? "calculation"
            : "direct_answer",
        customerStage: hasOfferIntent(cleanMessage)
          ? "evaluation"
          : hasEmiIntent(cleanMessage)
            ? "consideration"
            : "exploration",
        tools,
        nextSteps: [
          {
            label: "Get quotation",
            query: `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
            tool: "aci_lead_capture",
            entities: { model, ...(variant ? { variant } : {}), leadType: "quotation" },
            filters: { city, leadType: "quotation" },
            priority: 90,
            displayStyle: "primary_cta",
            icon: "file-text",
          },
        ],
        ambiguity: hasCompareIntent(cleanMessage)
          ? {
              level: "soft_default",
              type: "comparison_variant",
              message:
                "I’ll compare popular comparable variants for now. You can change variants anytime.",
              selectedDefault: {
                variantSelectionMode: "representative_default",
              },
            }
          : {
              level: variant ? "none" : "soft_default",
              type: variant ? "none" : "variant",
              message: variant
                ? ""
                : "I’ll use a selected or popular variant where needed. You can change the variant anytime.",
              selectedDefault: variant
                ? null
                : { variantSelectionMode: "representative_default" },
            },
        contextPatch: contextPatch({
          model,
          variant,
          city,
          selectedComparisonSet: comparisonModelsForContext.length
            ? {
                models: comparisonModelsForContext,
                variantSelectionMode: "representative_default",
              }
            : {},
          leadContext: hasOfferIntent(cleanMessage)
            ? {
                leadType: "offer_enquiry",
                selectedServices: ["offer_enquiry", "quotation"],
              }
            : {},
          customerStage: hasOfferIntent(cleanMessage)
            ? "evaluation"
            : hasEmiIntent(cleanMessage)
              ? "consideration"
              : "exploration",
          conversationMode: hasCompareIntent(cleanMessage)
            ? "comparison"
            : hasEmiIntent(cleanMessage)
              ? "calculation"
              : "direct_answer",
        }),
        confidence: 0.98,
        reasoningSummary: "User asked a multi-intent new-car question.",
        unavailableReason: hasOfferIntent(cleanMessage)
          ? "offers_not_available"
          : null,
      };

      return buildResult({ rawPlan, message: cleanMessage, startedAt });
    }
  }


  if (hasEmiIntent(cleanMessage) && model) {
    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "calculation",
      customerStage: "consideration",
      tools: [
        baseTool({
          tool: "vehicle_emi",
          model,
          variant,
          city,
          filters: {
            priceBasis: "on_road",
            ...(downPayment ? { downPayment } : {}),
            ...(tenureMonths ? { tenureMonths } : {}),
            ...(effectiveLoanPercent ? { loanPercent: effectiveLoanPercent } : {}),
          },
          output: { canvasType: "emi_calculator_canvas" },
          resolution: {
            variantSelectionMode: variant ? "exact" : "representative_default",
            selectedModels: [{ model }],
            selectedVariants: variant
              ? [{ model, variant }]
              : [{ model, variantStrategy: "popular_or_best_value" }],
            note: variant
              ? "Use selected variant for EMI."
              : "Use selected or representative variant for EMI.",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Get quotation",
          query: `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
          tool: "aci_lead_capture",
          entities: {
            model,
            ...(variant ? { variant } : {}),
            leadType: "quotation",
          },
          filters: { city, leadType: "quotation" },
          priority: 80,
          displayStyle: "primary_cta",
          icon: "file-text",
        },
      ],
      ambiguity: {
        level: variant ? "none" : "soft_default",
        type: variant ? "none" : "variant",
        message: variant
          ? ""
          : "I’ll calculate EMI using a selected or popular variant. You can change the variant anytime.",
        selectedDefault: variant
          ? null
          : { variantSelectionMode: "representative_default" },
      },
      contextPatch: contextPatch({
        model,
        variant,
        city,
        customerStage: "consideration",
        conversationMode: "calculation",
      }),
      confidence: 0.98,
      reasoningSummary: "User wants EMI calculation.",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  if (
    hasCompareIntent(cleanMessage) &&
    (resolved.comparisonModels.length >= 2 ||
      (selectedVehicle.model && resolved.comparisonModels.length))
  ) {
    const comparisonModels =
      resolved.comparisonModels.length >= 2
        ? resolved.comparisonModels
        : [selectedVehicle.model, ...resolved.comparisonModels].filter(Boolean);

    const primaryModel = comparisonModels[0];
    const selectedPrimaryVariant =
      selectedVehicle.model === primaryModel
        ? selectedVehicle.variant
        : variant;

    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "comparison",
      customerStage: "evaluation",
      tools: [
        baseTool({
          tool: "vehicle_compare",
          model: primaryModel,
          variant: selectedPrimaryVariant,
          city,
          entities: {
            models: comparisonModels,
            comparisonModels,
          },
          filters: { priceBasis: "on_road" },
          output: { canvasType: "comparison_canvas", groupBy: "variant" },
          resolution: {
            comparisonLevel: "variant",
            variantSelectionMode: "representative_default",
            selectedModels: comparisonModels.map((item) => ({ model: item })),
            selectedVariants: comparisonModels.map((item, index) =>
              index === 0 && selectedPrimaryVariant
                ? { model: item, variant: selectedPrimaryVariant }
                : {
                    model: item,
                    variantStrategy:
                      index === 0
                        ? "popular_automatic"
                        : "comparable_by_price_transmission",
                  },
            ),
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
      ],
      ambiguity: {
        level: "soft_default",
        type: "comparison_variant",
        message:
          "I’ll compare popular comparable variants for now. You can change variants anytime.",
        selectedDefault: { variantSelectionMode: "representative_default" },
      },
      contextPatch: contextPatch({
        model: primaryModel,
        variant: selectedPrimaryVariant,
        city,
        selectedComparisonSet: {
          models: comparisonModels,
          variantSelectionMode: "representative_default",
        },
        customerStage: "evaluation",
        conversationMode: "comparison",
      }),
      confidence: 0.98,
      reasoningSummary: "User wants comparison.",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  if (isRecommendationIntent(cleanMessage)) {
    const mustHaveFeatures = features.filter((feature) =>
      [
        "sunroof",
        "panoramic sunroof",
        "6 airbags",
        "airbags",
        "ADAS",
        "360 camera",
        "ventilated seats",
      ].includes(feature),
    );

    const ranking = hasSafetyIntent(cleanMessage)
      ? "safety"
      : mustHaveFeatures.length
        ? "feature_match"
        : transmission === "automatic"
          ? "automatic_value"
          : "value";

    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "recommendation",
      customerStage: "exploration",
      tools: [
        baseTool({
          tool: "vehicle_recommend",
          city,
          filters: {
            ...(budgetMax ? { budgetMax } : {}),
            priceBasis: "on_road",
            ...(bodyType ? { bodyType } : {}),
            ...(transmission ? { transmission } : {}),
            ...(fuelType ? { fuelType } : {}),
            ...(mustHaveFeatures.length ? { mustHaveFeatures } : {}),
          },
          ranking,
          output: {
            canvasType: "recommendation_results_canvas",
            groupBy: "model",
          },
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
          label: "Compare top cars",
          query: "Compare the top recommended cars",
          tool: "vehicle_compare",
          priority: 75,
          displayStyle: "pill",
          icon: "compare",
          requiresSelection: true,
        },
      ],
      ambiguity: { level: "none", type: "none", message: "" },
      contextPatch: contextPatch({
        city,
        userPreferences: {
          ...(budgetMax ? { budgetMax } : {}),
          ...(bodyType ? { bodyType } : {}),
          ...(transmission ? { transmission } : {}),
          ...(fuelType ? { fuelType } : {}),
          ...(mustHaveFeatures.length ? { mustHaveFeatures } : {}),
        },
        customerStage: "exploration",
        conversationMode: "recommendation",
      }),
      confidence: 0.98,
      reasoningSummary: "User wants filtered recommendations.",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  if (hasPriceIntent(cleanMessage) && model) {
    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "direct_answer",
      customerStage: "exploration",
      tools: [
        baseTool({
          tool: "vehicle_pricelist",
          model,
          variant,
          city,
          filters: { priceBasis: "on_road" },
          output: {
            canvasType: "pricelist_canvas",
            groupBy: variant ? "none" : "variant",
          },
          resolution: {
            variantSelectionMode: variant ? "exact" : "not_required",
            selectedModels: [{ model }],
            selectedVariants: variant ? [{ model, variant }] : [],
            changeAllowed: true,
            note: variant
              ? "Show variant price if exact variant resolves."
              : "Show model-level price list with variant rows.",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Calculate EMI",
          query: `Calculate EMI for ${model}${variant ? ` ${variant}` : ""}`,
          tool: "vehicle_emi",
          entities: { model, ...(variant ? { variant } : {}) },
          filters: { city },
          priority: 80,
          displayStyle: "pill",
          icon: "calculator",
        },
      ],
      ambiguity: {
        level: "none",
        type: "none",
        message: "",
      },
      contextPatch: contextPatch({
        model,
        variant,
        city,
        customerStage: "exploration",
        conversationMode: "direct_answer",
      }),
      confidence: 0.98,
      reasoningSummary: "User wants price/pricelist.",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  if (hasOfferIntent(cleanMessage) && model) {
    const rawPlan = {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "lead_capture",
      customerStage: "closing",
      tools: [
        baseTool({
          tool: "aci_lead_capture",
          model,
          variant,
          city,
          entities: {
            leadType: "offer_enquiry",
            selectedServices: ["offer_enquiry", "quotation"],
          },
          filters: {
            leadType: "offer_enquiry",
            selectedServices: ["offer_enquiry", "quotation"],
          },
          output: { canvasType: "lead_capture_canvas" },
          resolution: {
            variantSelectionMode: variant ? "exact" : "not_required",
            selectedModels: [{ model }],
            selectedVariants: variant ? [{ model, variant }] : [],
            note: "offers_not_available",
          },
        }),
      ],
      nextSteps: [
        {
          label: "Get quotation",
          query: `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
          tool: "aci_lead_capture",
          entities: {
            model,
            ...(variant ? { variant } : {}),
            leadType: "quotation",
          },
          filters: { city, leadType: "quotation" },
          priority: 90,
          displayStyle: "primary_cta",
          icon: "file-text",
        },
      ],
      ambiguity: { level: "none", type: "none", message: "" },
      contextPatch: contextPatch({
        model,
        variant,
        city,
        leadContext: {
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        customerStage: "closing",
        conversationMode: "lead_capture",
      }),
      confidence: 0.98,
      reasoningSummary:
        "Verified offers are unavailable; create offer enquiry lead.",
      unavailableReason: "offers_not_available",
    };

    return buildResult({ rawPlan, message: cleanMessage, startedAt });
  }

  return null;
};

export default compileSemanticPlan;
