import {
  compactObject,
  extractVehicleLast4,
  normalizeCitySlug,
  normalizeName,
  normalizeText,
  normalizeVehicleNumber,
  parseDateRange,
} from "./aiAgent.normalizers.js";

/**
 * ACI Assist intent router
 * ------------------------
 * Purpose:
 * - Route user query to the correct deterministic backend tool.
 * - Extract safe entities.
 * - Avoid stale context leakage.
 * - Avoid mixing similar models like Venue + Venue N Line.
 * - Avoid treating "city" as Honda City unless context clearly means the car.
 *
 * Important:
 * This router DOES NOT query MongoDB.
 * Model/variant ambiguity must still be finally resolved inside vehicle tools
 * because only tools can inspect actual distinct catalogue models/variants.
 */

/* -------------------------------------------------------------------------- */
/* HINTS                                                                       */
/* -------------------------------------------------------------------------- */

/**
 * Keep this as fallback only.
 * The real catalogue model list should ideally come from DB cache and be passed
 * through context.catalogueModelHints / context.modelHints by aiAgent.service.js.
 *
 * IMPORTANT:
 * Do not put plain "city" here. Honda City is handled separately because
 * “change city to Mumbai” must not be parsed as model = city.
 */
const STATIC_MODEL_HINTS = [
  "city hybrid",
  "honda city",
  "verna",
  "slavia",
  "creta n line",
  "creta",
  "venue n line",
  "venue",
  "i20 n line",
  "grand i10",
  "i20",
  "i10",
  "swift",
  "baleno",
  "brezza",
  "fronx",
  "nexon",
  "harrier",
  "safari",
  "seltos x line",
  "seltos",
  "sonet",
  "alcazar",
  "ignis",
  "innova",
  "fortuner",
  "thar",
  "scorpio",
  "xuv700",
  "amaze",
  "elevate",
  "ciaz",
  "hector",
  "taigun",
  "virtus",
  "a4",
  "a6",
  "dbx",
  "db12",
];

const MAKE_HINTS = [
  "hyundai",
  "honda",
  "volkswagen",
  "skoda",
  "maruti",
  "suzuki",
  "maruti suzuki",
  "tata",
  "mahindra",
  "kia",
  "toyota",
  "mg",
  "audi",
  "bmw",
  "mercedes",
  "aston martin",
  "isuzu",
  "jeep",
  "force",
];

const FEATURE_HINTS = [
  "voice assisted sunroof",
  "panoramic sunroof",
  "sunroof",
  "6 airbags",
  "airbags",
  "airbag",
  "adas",
  "wireless charging",
  "ventilated seats",
  "cruise control",
  "adaptive cruise control",
  "rear camera",
  "360 camera",
  "alloy wheels",
  "automatic climate control",
  "climate control",
  "hill assist",
  "isofix",
  "lane keep assist",
  "blind spot monitor",
  "tpms",
  "esc",
  "esp",
  "abs",
  "traction control",
  "rear ac vents",
  "boot space",
  "ground clearance",
  "mileage",
  "engine displacement",
  "engine",
  "transmission",
  "automatic",
  "dct",
  "cvt",
  "ivt",
  "amt",
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
  "jaipur",
  "lucknow",
  "hyderabad",
  "chennai",
  "kolkata",
  "ahmedabad",
];

const BODY_TYPE_HINTS = [
  "compact suv",
  "premium suv",
  "7 seater",
  "7-seater",
  "suv",
  "suvs",
  "sedan",
  "sedans",
  "hatchback",
  "hatchbacks",
  "mpv",
  "mpvs",
];

const FUEL_HINTS = ["petrol", "diesel", "cng", "electric", "ev", "hybrid"];
const TRANSMISSION_HINTS = [
  "automatic",
  "manual",
  "amt",
  "cvt",
  "dct",
  "ivt",
  "mt",
];

const COLOR_HINTS = [
  "black",
  "white",
  "red",
  "blue",
  "grey",
  "gray",
  "silver",
  "green",
  "orange",
  "brown",
  "gold",
  "matte",
  "dual tone",
  "dual-tone",
];

const STOP_NAME_WORDS = new Set([
  "find",
  "search",
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
  "colours",
  "features",
  "compare",
  "emi",
  "business",
  "pending",
]);

const VEHICLE_QUERY_STOPWORDS = new Set([
  "show",
  "find",
  "search",
  "of",
  "for",
  "the",
  "a",
  "an",
  "car",
  "cars",
  "new",
  "price",
  "prices",
  "pricing",
  "pricelist",
  "list",
  "rate",
  "rates",
  "on",
  "road",
  "ex",
  "showroom",
  "breakup",
  "color",
  "colors",
  "colour",
  "colours",
  "available",
  "options",
  "feature",
  "features",
  "specs",
  "catalogue",
  "catalog",
  "emi",
  "calculate",
]);

/* -------------------------------------------------------------------------- */
/* INTENT DEFINITIONS                                                          */
/* Lower priority number = checked earlier                                     */
/* -------------------------------------------------------------------------- */

export const INTENT_DEFINITIONS = [
  /* --------------------------- New-car expert ---------------------------- */

  {
    intent: "vehicle_price_history",
    priority: 5,
    patterns: [
      /\b(price history|price trend|price changed|price change|price updated|price updates|price hike|got cheaper|variants? added|new variants|updated today|updated this week|added this month|added in|current price with last month)\b/i,
    ],
    collections: ["price_history"],
    requiredEntities: [],
    optionalEntities: ["make", "model", "variant", "city", "dateRange"],
    widgetType: "vehicle_price_history",
    failureMessage: "No stored price history records matched this request.",
  },

  {
    intent: "vehicle_color_search",
    priority: 8,
    patterns: [
      /\b(which|show|find)\b.*\b(black|white|red|blue|grey|gray|silver|green|orange|brown|gold|matte|dual[- ]tone)\b.*\b(cars?|models?|suvs?|sedans?|hatchbacks?)\b/i,
      /\b(which|show|find)\b.*\b(cars?|models?|suvs?|sedans?|hatchbacks?)\b.*\b(black|white|red|blue|grey|gray|silver|green|orange|brown|gold|matte|dual[- ]tone)\b/i,
    ],
    collections: ["vehicle_colors", "vehicles", "vehicle_features"],
    requiredEntities: ["color"],
    optionalEntities: ["make", "model", "budgetMax", "bodyType"],
    widgetType: "vehicle_color_search",
    failureMessage: "No stored color records matched this request.",
  },

  {
    intent: "vehicle_colors",
    priority: 10,
    patterns: [
      /\b(colou?rs?|color options|available colou?rs?|show colou?rs?|color gallery|colour gallery)\b/i,
    ],
    collections: ["vehicle_colors"],
    requiredEntities: ["model"],
    optionalEntities: ["make", "color"],
    widgetType: "vehicle_colors",
    failureMessage: "No stored color records matched the requested model.",
  },

  {
    intent: "vehicle_feature_discovery",
    priority: 12,
    patterns: [
      /\b(which|cheapest|show|find)\b.*\b(variants?|cars?|suvs?|sedans?|hatchbacks?)\b.*\b(have|with|sunroof|airbags?|6 airbags|adas|wireless charging|ventilated seats|360 camera|tpms|hill assist|isofix|lane keep assist|cruise control|automatic climate control|esc|abs)\b/i,
      /\b(cheapest|cars?|suvs?|sedans?|hatchbacks?)\b.*\b(with|have)\b.*\b(sunroof|adas|6 airbags|wireless charging|ventilated seats|tpms|isofix)\b/i,
    ],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: ["feature"],
    optionalEntities: [
      "model",
      "budgetMax",
      "bodyType",
      "city",
      "fuelType",
      "transmission",
    ],
    widgetType: "vehicle_feature_discovery",
    failureMessage:
      "No variants matched the requested feature in stored catalogue data.",
  },

  {
    intent: "vehicle_feature_answer",
    priority: 14,
    patterns: [
      /\b(does|has|have|available|how many|what is|what transmission|what engine|what mileage|what boot|what ground clearance)\b.*\b(sunroof|airbags?|6 airbags|adas|wireless charging|ventilated seats|mileage|boot space|ground clearance|engine displacement|transmission|camera|cruise|alloy wheels|climate control|hill assist|isofix|lane keep assist|automatic|dct|cvt|abs|esp|esc|tpms)\b/i,
      /\b(sunroof|airbags?|6 airbags|adas|wireless charging|ventilated seats|mileage|boot space|ground clearance|engine displacement|transmission|camera|cruise|alloy wheels|tpms|isofix)\b.*\b(in|of)\b/i,
    ],
    collections: ["vehicle_features"],
    requiredEntities: ["model", "feature"],
    optionalEntities: ["make", "variant"],
    widgetType: "vehicle_feature_answer",
    failureMessage: "No stored feature value matched this question.",
  },

  {
    intent: "vehicle_emi_budget_search",
    priority: 16,
    patterns: [
      /\b(cars?|suvs?|sedans?|hatchbacks?|automatic cars?)\b.*\bemi\b.*\b(under|below|less than|upto|up to)\b/i,
      /\b(lowest emi|low emi)\b/i,
    ],
    collections: ["vehicles"],
    requiredEntities: ["emiMax"],
    optionalEntities: ["budgetMax", "bodyType", "fuelType", "transmission"],
    widgetType: "vehicle_emi_recommendations",
    failureMessage: "No vehicles matched the requested EMI budget.",
  },

  {
    intent: "vehicle_emi_calculator",
    priority: 18,
    patterns: [
      /\b(emi|down payment|roi|interest|tenure|finance amount|calculate emi)\b/i,
    ],
    collections: ["vehicles"],
    requiredEntities: [],
    optionalEntities: ["model", "variant", "city", "manualAmount"],
    widgetType: "vehicle_emi_calculator",
    failureMessage: "Ask EMI with a vehicle, price, or amount.",
  },

  {
    intent: "vehicle_variant_difference",
    priority: 20,
    patterns: [
      /\b(difference between|what extra|extra do i get|missing in|vs|versus)\b.*\b(sx|vx|zx|hte|htk|htx|hx\d+|base|top|opt|plus|turbo|dct|ivt|amt|cvt|variant)\b/i,
      /\b(sx|vx|zx|hte|htk|htx|hx\d+)\b.*\b(vs|versus)\b.*\b(sx|vx|zx|hte|htk|htx|hx\d+|opt|plus|turbo)\b/i,
    ],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: ["model"],
    optionalEntities: ["variant", "variants"],
    widgetType: "vehicle_variant_difference",
    failureMessage: "Ask with two variants to compare their differences.",
  },

  {
    intent: "vehicle_best_variant_recommendation",
    priority: 22,
    patterns: [
      /\b(which .*variant should i buy|best .*variant|best value .*variant|value for money|worth it|worth extra|cheapest .*with|cheapest .*automatic|best automatic variant|best family variant)\b/i,
    ],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: ["model"],
    optionalEntities: ["feature", "transmission", "budgetMax"],
    widgetType: "vehicle_variant_recommendation",
    failureMessage: "Ask with a model to recommend a variant.",
  },

  {
    intent: "vehicle_comparison",
    priority: 24,
    patterns: [
      /\b(compare|comparison| vs | versus |difference between|compare selected variants|show only differences)\b/i,
    ],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: ["models"],
    optionalEntities: ["city", "variant", "variants"],
    widgetType: "vehicle_model_comparison",
    failureMessage: "Ask with at least two models to compare.",
  },

  {
    intent: "similar_cars",
    priority: 26,
    patterns: [
      /\b(similar cars|alternatives|competitors|same segment|cars like|similar to|cheaper alternatives|premium alternatives)\b/i,
    ],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: ["model"],
    optionalEntities: [
      "city",
      "budgetMax",
      "feature",
      "bodyType",
      "transmission",
    ],
    widgetType: "similar_cars",
    failureMessage: "Ask for similar cars with a model.",
  },

  {
    intent: "vehicle_dimension_space_search",
    priority: 27,
    patterns: [
      /\b(biggest boot|boot space above|boot space more than|good ground clearance|high ground clearance|fuel tank|wheelbase|spacious|7 seater|7-seater|large fuel tank|longest wheelbase|best spacious)\b/i,
    ],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: [],
    optionalEntities: ["budgetMax", "bodyType"],
    widgetType: "vehicle_spec_ranking",
    failureMessage:
      "No stored dimension or space records matched this request.",
  },

  {
    intent: "vehicle_performance_mileage_search",
    priority: 28,
    patterns: [
      /\b(highest mileage|best mileage|mileage above|good mileage|most powerful|turbo petrol|power above|bhp|ps|torque|compare mileage|compare engine)\b/i,
    ],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: [],
    optionalEntities: [
      "budgetMax",
      "bodyType",
      "fuelType",
      "transmission",
      "model",
    ],
    widgetType: "vehicle_spec_ranking",
    failureMessage:
      "No stored performance or mileage records matched this request.",
  },

  {
    intent: "vehicle_safety_expert",
    priority: 29,
    patterns: [
      /\b(safest|safety|6 airbags|adas|esc|esp|tpms|isofix|hill assist|blind spot|lane keep|adaptive cruise|collision warning|forward collision)\b/i,
    ],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: [],
    optionalEntities: ["budgetMax", "bodyType", "model"],
    widgetType: "vehicle_safety_results",
    failureMessage: "No stored safety feature records matched this request.",
  },

  {
    intent: "vehicle_use_case_recommendation",
    priority: 30,
    patterns: [
      /\b(best|recommend|which car)\b.*\b(family|parents|city driving|highway|daily running|low emi|safe|value|feature-loaded|feature loaded|long drives|chauffeur|rear seat|office use|first car|upgrade|premium sedan)\b/i,
    ],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: [],
    optionalEntities: ["budgetMax", "bodyType", "transmission", "useCase"],
    widgetType: "vehicle_recommendation_results",
    failureMessage: "No stored vehicles matched this use-case request.",
  },

  {
    intent: "vehicle_budget_search",
    priority: 32,
    patterns: [
      /\b(under|below|less than|upto|up to|between)\b.*\d+\s*(lakh|lac|l|cr|crore|₹|rs|inr)\b/i,
      /\b(best cars?|top cars?|best value|feature loaded|feature-loaded|family cars?|city automatic|highway car|first car|upgrade from hatchback)\b/i,
    ],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: [],
    optionalEntities: [
      "budgetMin",
      "budgetMax",
      "bodyType",
      "fuelType",
      "transmission",
      "feature",
      "useCase",
    ],
    widgetType: "vehicle_recommendation_results",
    failureMessage: "No vehicles matched these filters.",
  },

  {
    intent: "vehicle_body_type_search",
    priority: 34,
    patterns: [
      /\b(show|find)?\s*(sedans?|suvs?|hatchbacks?|mpvs?|7[- ]seater cars?|compact suvs?|premium suvs?)\b/i,
    ],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: ["bodyType"],
    optionalEntities: ["budgetMax", "fuelType", "transmission"],
    widgetType: "vehicle_recommendation_results",
    failureMessage: "No stored cars matched this body type.",
  },

  {
    intent: "vehicle_fuel_transmission_search",
    priority: 36,
    patterns: [
      /\b(petrol|diesel|cng|electric|ev|hybrid|automatic|manual|amt|cvt|dct|ivt)\b.*\b(cars?|suvs?|sedans?|hatchbacks?|variants?)\b/i,
      /\b(cars?|suvs?|sedans?|hatchbacks?)\b.*\b(petrol|diesel|cng|electric|ev|hybrid|automatic|manual|amt|cvt|dct|ivt)\b/i,
    ],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: [],
    optionalEntities: ["budgetMax", "bodyType", "fuelType", "transmission"],
    widgetType: "vehicle_recommendation_results",
    failureMessage: "No stored cars matched this fuel or transmission.",
  },

  {
    intent: "vehicle_launch_status",
    priority: 44,
    patterns: [
      /\b(new .*cars added|latest new car additions|recently updated models|recently added variants|active new launches|new launches|latest additions)\b/i,
    ],
    collections: ["vehicles", "price_history"],
    requiredEntities: [],
    optionalEntities: ["make", "dateRange"],
    widgetType: "latest_catalogue_updates",
    failureMessage: "Launch status is not captured in current database.",
  },

  {
    intent: "vehicle_price_breakup",
    priority: 50,
    patterns: [
      /\b(price breakup|rto|road tax|insurance amount|tcs|accessories|on road without accessories|on-road without accessories|on road with accessories|on-road with accessories|all charges)\b/i,
    ],
    collections: ["vehicles"],
    requiredEntities: ["model"],
    optionalEntities: ["make", "variant", "city"],
    widgetType: "vehicle_price_breakup",
    failureMessage: "Ask for price breakup with a model and optional variant.",
  },

  {
    intent: "vehicle_city_change",
    priority: 52,
    patterns: [
      /\b(change city to|same car in|what about in|prices? in|price in|show .* in)\b/i,
      /^\s*(mumbai|delhi|bangalore|bengaluru|pune|gurgaon|gurugram|noida|new delhi|jaipur|lucknow|hyderabad|chennai|kolkata|ahmedabad)\s*$/i,
    ],
    collections: ["vehicles"],
    requiredEntities: ["city"],
    optionalEntities: ["model", "make", "variant"],
    widgetType: "vehicle_pricelist",
    failureMessage: "No vehicles found in the requested city.",
  },

  {
    intent: "vehicle_pricelist",
    priority: 60,
    patterns: [
      /\b(pricelist|price list|price|pricing|prices|rate list|on road|on-road|ex showroom|ex-showroom|variant price|new car price|variants?|cheapest|top model|automatic .*variants?|manual .*variants?|petrol .*variants?|diesel .*variants?|cng .*variants?|active .*variants?|discontinued .*variants?|sorted by price|lowest on-road|lowest on road)\b/i,
    ],
    collections: ["vehicles"],
    requiredEntities: ["model"],
    optionalEntities: ["make", "variant", "city"],
    widgetType: "vehicle_pricelist",
    failureMessage: "Ask for a model such as Verna, Venue, City, or Slavia.",
  },

  {
    intent: "vehicle_features",
    priority: 70,
    patterns: [
      /\b(features?|specs|specifications|catalogue|catalog|brochure|safety features|engine specs|adas features|comfort features|interior features|exterior features|dimensions)\b/i,
    ],
    collections: ["vehicle_features"],
    requiredEntities: ["model"],
    optionalEntities: ["make", "variant", "section"],
    widgetType: "vehicle_features",
    failureMessage: "No stored feature catalogue matched the requested model.",
  },

  /* ---------------------------- Internal tools --------------------------- */
  /* Kept below new-car intents so they do not steal new-car expert questions */

  {
    intent: "loan_closure_pos",
    priority: 200,
    patterns: [
      /\b(pos|principal outstanding|loan closure|closure amount|approx closure|foreclosure|settlement amount|current outstanding)\b/i,
    ],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["customerName", "last4", "registrationNumber", "loanId"],
    widgetType: "loan_closure_card",
    failureMessage: "No matching loan was found for POS or closure.",
  },
  {
    intent: "loan_disbursal_report",
    priority: 210,
    patterns: [
      /\b(approved but not disbursed|approved not disbursed|pending disbursal|approval done disbursal pending|approved cases pending disbursal)\b/i,
    ],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["dateRange"],
    widgetType: "loan_disbursal_report",
    failureMessage: "No approved but not disbursed loan cases were found.",
  },
  {
    intent: "loan_pending_approval_report",
    priority: 212,
    patterns: [
      /\b(pending approval|approval pending|not approved|approval stage)\b/i,
    ],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["dateRange"],
    widgetType: "records_table",
    failureMessage: "No pending approval loan cases were found.",
  },
  {
    intent: "loan_business_report",
    priority: 213,
    patterns: [
      /\b(total business|business this month|total cases this month|total car business|total new car business|total used car business|total cash cars|cash car business|book value)\b/i,
    ],
    collections: ["loans", "deliveryOrders", "insurance"],
    requiredEntities: [],
    optionalEntities: ["dateRange"],
    widgetType: "loan_business_report",
    failureMessage: "No loan business records matched this request.",
  },
  {
    intent: "latest_insurance",
    priority: 220,
    patterns: [
      /\b(latest insurance|insurance of|policy of|active insurance|current policy|latest policy)\b/i,
    ],
    collections: ["insurance"],
    requiredEntities: [],
    optionalEntities: ["customerName", "last4", "registrationNumber", "model"],
    widgetType: "insurance_case_card",
    failureMessage: "No matching insurance policy was found.",
  },
  {
    intent: "customer_360",
    priority: 230,
    patterns: [
      /\b(customer\s*360|customer profile|full profile|show all cases of|all records of|full customer details)\b/i,
    ],
    collections: [
      "customers",
      "loans",
      "insurance",
      "payments",
      "usedCarLeads",
    ],
    requiredEntities: ["customerName"],
    optionalEntities: ["last4"],
    widgetType: "customer_360",
    failureMessage: "Ask Customer 360 with a customer name.",
  },
  {
    intent: "vehicle_registration_search",
    priority: 240,
    patterns: [
      /\b(vehicle number|registration number|reg number|car ending|vehicle ending)\b/i,
    ],
    collections: ["vehicle_master_records", "loans"],
    requiredEntities: [],
    optionalEntities: ["registrationNumber", "last4", "model"],
    widgetType: "records_table",
    failureMessage: "No matching vehicle registration records were found.",
  },
  {
    intent: "vehicle_360",
    priority: 245,
    patterns: [
      /\b(vehicle\s*360|vehicle profile|full vehicle history|full history of car|all records of vehicle)\b/i,
    ],
    collections: [
      "vehicle_master_records",
      "loans",
      "insurance",
      "payments",
      "deliveryOrders",
    ],
    requiredEntities: [],
    optionalEntities: ["registrationNumber", "last4", "model"],
    widgetType: "vehicle_360",
    failureMessage:
      "Ask Vehicle 360 with a registration number or last 4 digits.",
  },
  {
    intent: "customer_lookup",
    priority: 900,
    patterns: [
      /\b(find customer|search customer|customer details|mobile number|customerid|customer id)\b/i,
      /^\s*(find|search)\s+[a-z]/i,
    ],
    collections: ["customers"],
    requiredEntities: [],
    optionalEntities: ["customerName", "mobile", "customerId"],
    widgetType: "customer_card",
    failureMessage: "No matching customer was found.",
  },
];

const sortedDefinitions = [...INTENT_DEFINITIONS].sort(
  (a, b) => a.priority - b.priority,
);

/* -------------------------------------------------------------------------- */
/* CONTEXT GROUPS                                                              */
/* -------------------------------------------------------------------------- */

const VEHICLE_CONTEXT_INTENTS = new Set([
  "vehicle_colors",
  "vehicle_color_search",
  "vehicle_features",
  "vehicle_feature_answer",
  "vehicle_feature_discovery",
  "vehicle_price_breakup",
  "vehicle_pricelist",
  "vehicle_city_change",
  "vehicle_comparison",
  "similar_cars",
  "vehicle_budget_search",
  "vehicle_use_case_recommendation",
  "vehicle_emi_calculator",
  "vehicle_emi_budget_search",
  "vehicle_price_history",
  "vehicle_launch_status",
  "vehicle_body_type_search",
  "vehicle_fuel_transmission_search",
  "vehicle_dimension_space_search",
  "vehicle_performance_mileage_search",
  "vehicle_safety_expert",
  "vehicle_best_variant_recommendation",
  "vehicle_variant_difference",
]);

const CUSTOMER_CONTEXT_INTENTS = new Set([
  "customer_lookup",
  "customer_360",
  "latest_insurance",
  "loan_status",
  "loan_closure_pos",
  "vehicle_360",
  "vehicle_registration_search",
]);

const REPORT_INTENTS_THAT_MUST_NOT_INHERIT_VEHICLE = new Set([
  "loan_disbursal_report",
  "loan_pending_approval_report",
  "loan_business_report",
  "customer_data_quality_report",
  "loan_missing_registration_report",
  "loan_invoice_missing_report",
  "loan_insurance_missing_report_basic",
]);

/* -------------------------------------------------------------------------- */
/* EXTRACTION HELPERS                                                          */
/* -------------------------------------------------------------------------- */

const unique = (arr = []) => [...new Set(arr.filter(Boolean))];

const escapeRegex = (value = "") =>
  String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const toModelKey = (value = "") =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");

const getDynamicModelHints = (context = {}) => {
  const raw =
    context?.catalogueModelHints ||
    context?.modelHints ||
    context?.availableModels ||
    context?.catalogueModels ||
    [];

  if (!Array.isArray(raw)) return [];

  return raw
    .map((item) => {
      if (typeof item === "string") return item;
      return item?.model || item?.name || item?.displayName || "";
    })
    .filter(Boolean);
};

const buildModelHints = (context = {}) => {
  const hints = unique([
    ...getDynamicModelHints(context),
    ...STATIC_MODEL_HINTS,
  ])
    .map((item) => normalizeText(item).toLowerCase())
    .filter(Boolean)
    .filter((item) => item !== "city"); // Never allow plain city here.

  return hints.sort((a, b) => b.length - a.length);
};

const shouldTreatCityAsHondaCity = (lower) => {
  if (
    /\b(change city|price in|prices in|same car in|what about in|available cities)\b/i.test(
      lower,
    )
  )
    return false;
  if (/\bhonda\s+city\b/i.test(lower)) return true;
  if (/\b(compare|vs|versus)\b/i.test(lower) && /\bcity\b/i.test(lower))
    return true;
  if (
    /\b(city vx|city zx|city hybrid|city pricelist|city price|city features|city colours?|city colors?)\b/i.test(
      lower,
    )
  )
    return true;
  return false;
};

export const extractModels = (lower, context = {}) => {
  const compact = lower.replace(/[^a-z0-9]/g, "");
  const hints = buildModelHints(context);

  let matches = hints.filter((model) => {
    const phrasePattern = new RegExp(
      `\\b${escapeRegex(model).replace(/\s+/g, "\\s+")}\\b`,
      "i",
    );
    const compactModel = model.replace(/[^a-z0-9]/g, "");
    return (
      phrasePattern.test(lower) ||
      (compactModel.length >= 3 && compact.includes(compactModel))
    );
  });

  // Honda City special case.
  if (shouldTreatCityAsHondaCity(lower)) {
    matches.push("city");
  }

  matches = unique(matches);

  // Longest-match cleanup:
  // If "venue n line" matched, do not also return "venue" for the same query.
  const sorted = matches.sort((a, b) => b.length - a.length);
  const cleaned = [];
  for (const model of sorted) {
    const key = toModelKey(model);
    const alreadyCovered = cleaned.some((existing) => {
      const existingKey = toModelKey(existing);
      return existingKey.includes(key) && existingKey !== key;
    });
    if (!alreadyCovered) cleaned.push(model);
  }

  return cleaned;
};

const extractMake = (lower) =>
  MAKE_HINTS.find((make) =>
    new RegExp(`\\b${escapeRegex(make).replace(/\s+/g, "\\s+")}\\b`, "i").test(
      lower,
    ),
  ) || "";

const extractFeature = (lower) =>
  FEATURE_HINTS.find((feature) =>
    new RegExp(
      `\\b${escapeRegex(feature).replace(/\s+/g, "\\s+")}\\b`,
      "i",
    ).test(lower),
  ) || "";

const normalizeBodyTypeValue = (value = "") => {
  const text = String(value).toLowerCase().trim();

  if (/(compact\s+)?suvs?/.test(text))
    return text.includes("compact") ? "compact suv" : "suv";
  if (/premium\s+suvs?/.test(text)) return "premium suv";
  if (/sedans?/.test(text)) return "sedan";
  if (/hatchbacks?/.test(text)) return "hatchback";
  if (/mpvs?/.test(text)) return "mpv";
  if (/7[-\s]?seater/.test(text)) return "7 seater";

  return text;
};

const extractBodyType = (lower) => {
  const raw =
    BODY_TYPE_HINTS.find((bodyType) =>
      new RegExp(
        `\\b${escapeRegex(bodyType).replace(/\\-/g, "[- ]").replace(/\s+/g, "\\s+")}\\b`,
        "i",
      ).test(lower),
    ) || "";

  return normalizeBodyTypeValue(raw);
};

const extractFuelType = (lower) =>
  FUEL_HINTS.find((fuel) =>
    new RegExp(`\\b${escapeRegex(fuel)}\\b`, "i").test(lower),
  ) || "";

const extractTransmission = (lower) => {
  if (/\bautomatic\b/i.test(lower)) return "automatic";
  if (/\bmanual\b/i.test(lower)) return "manual";
  if (/\bamt\b/i.test(lower)) return "amt";
  if (/\bcvt\b/i.test(lower)) return "cvt";
  if (/\bdct\b/i.test(lower)) return "dct";
  if (/\bivt\b/i.test(lower)) return "ivt";
  if (/\bmt\b/i.test(lower)) return "mt";

  // AT is ambiguous because "at 9 percent" is common in EMI queries.
  // Only accept AT if it appears like a variant/transmission token, not before a number.
  if (/\bat\b/i.test(lower) && !/\bat\s+\d/i.test(lower)) return "at";

  return "";
};

const extractColor = (lower) =>
  COLOR_HINTS.find((color) =>
    new RegExp(
      `\\b${escapeRegex(color).replace(/\\-/g, "[- ]").replace(/\s+/g, "\\s+")}\\b`,
      "i",
    ).test(lower),
  ) || "";

const moneyToRupees = (raw = "", suffix = "") => {
  const value = Number(String(raw).replace(/,/g, ""));
  if (!Number.isFinite(value)) return null;
  const unit = String(suffix || "").toLowerCase();

  if (/cr|crore/.test(unit)) return value * 10000000;
  if (/lakh|lac|\bl\b/.test(unit)) return value * 100000;
  return value;
};

const extractBudget = (lower) => {
  const between = lower.match(
    /\bbetween\s+₹?\s*([\d,.]+)\s*(lakh|lac|l|cr|crore)?\s+(?:and|to|-)\s+₹?\s*([\d,.]+)\s*(lakh|lac|l|cr|crore)\b/i,
  );

  if (between) {
    const min = moneyToRupees(between[1], between[2] || between[4]);
    const max = moneyToRupees(between[3], between[4]);
    return compactObject({ budgetMin: min, budgetMax: max });
  }

  const under = lower.match(
    /\b(?:under|below|less than|upto|up to)\s*₹?\s*([\d,.]+)\s*(lakh|lac|l|cr|crore|rs|inr)?\b/i,
  );

  if (under) {
    if (!under[2] && /\bemi\b/i.test(lower)) return {};
    return compactObject({
      budgetMax: moneyToRupees(under[1], under[2] || "lakh"),
    });
  }

  return {};
};

const extractManualAmount = (lower) => {
  const match = lower.match(
    /\bon[- ]?road\s+₹?\s*([\d,.]+)\s*(lakh|lac|l|cr|crore|rs|inr)?\b/i,
  );
  if (!match) return null;
  return moneyToRupees(match[1], match[2] || "lakh");
};

const extractEmiMax = (lower) => {
  const match =
    lower.match(
      /\bemi\s*(?:under|below|less than|upto|up to)?\s*₹?\s*([\d,.]+)\b/i,
    ) ||
    lower.match(
      /\b(?:under|below|less than|upto|up to)\s*₹?\s*([\d,.]+)\s*(?:emi)\b/i,
    );

  return match ? Number(String(match[1]).replace(/[^\d.]/g, "")) || null : null;
};

const extractEmiInputs = (lower) => {
  const downPaymentAmount = lower.match(
    /\b₹?\s*([\d,.]+)\s*(lakh|lac|l|cr|crore|rs|inr)?\s+down payment\b/i,
  );
  const downPaymentPercent =
    lower.match(/\b(\d+(?:\.\d+)?)\s*%\s+down\b/i) ||
    lower.match(/\b(\d+(?:\.\d+)?)\s*percent\s+down\b/i);
  const years = lower.match(/\b(\d+)\s*(?:years?|yrs?)\b/i);
  const months = lower.match(/\b(\d+)\s*months?\b/i);
  const roi = lower.match(/\b(?:at\s*)?(\d+(?:\.\d+)?)\s*(?:percent|%)\b/i);

  return compactObject({
    downPayment: downPaymentAmount
      ? moneyToRupees(downPaymentAmount[1], downPaymentAmount[2] || "lakh")
      : null,
    downPaymentPercent: downPaymentPercent
      ? Number(downPaymentPercent[1])
      : null,
    tenureMonths: years
      ? Number(years[1]) * 12
      : months
        ? Number(months[1])
        : null,
    annualRate: roi ? Number(roi[1]) : null,
  });
};

const extractExplicitCity = (lower) => {
  const explicit = CITY_HINTS.find((city) =>
    new RegExp(
      `\\b${escapeRegex(city).replace("-", "\\s*-?\\s*").replace(/\s+/g, "\\s+")}\\b`,
      "i",
    ).test(lower),
  );

  return explicit ? normalizeCitySlug(explicit) : "";
};

const extractVariant = (message) => {
  const match = message.match(
    /\b(hte|htk|htx|gtx|sx|vx|zx|lxi|zxi|vxi|xza|xz|alpha|delta|sigma|sportz|asta|premium|comfortline|highline|hx\d+|s\s?opt|sx\s?opt|top|base|ivt|dct|mt|at|amt|cvt|turbo)\b(?:\s+\b(plus|turbo|ivt|dct|mt|at|amt|cvt|dt|opt|automatic|manual|line|hybrid)\b)*/i,
  );

  return normalizeText(match?.[0] || "");
};

const extractUseCase = (lower) => {
  const cases = [
    "family",
    "parents",
    "city driving",
    "highway",
    "daily running",
    "low emi",
    "safe",
    "value",
    "feature-loaded",
    "feature loaded",
    "long drives",
    "chauffeur",
    "rear seat",
    "office use",
    "first car",
    "upgrade",
    "premium sedan",
  ];

  return cases.find((item) => lower.includes(item)) || "";
};

const extractSection = (lower) => {
  const sections = [
    "safety",
    "engine",
    "adas",
    "comfort",
    "interior",
    "exterior",
    "dimensions",
    "mileage",
    "performance",
    "entertainment",
  ];
  return sections.find((section) => lower.includes(section)) || "";
};

const extractCustomerName = (message, intent, models) => {
  if (
    ![
      "latest_insurance",
      "loan_status",
      "loan_closure_pos",
      "customer_lookup",
      "customer_360",
      "vehicle_360",
    ].includes(intent)
  )
    return "";

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
      return (
        !STOP_NAME_WORDS.has(lowerToken) &&
        !modelSet.has(lowerToken) &&
        !MAKE_HINTS.includes(lowerToken)
      );
    });

  return normalizeName(tokens.slice(0, 4).join(" "));
};

const likelyVehicleCatalogueQuery = ({
  lower,
  models,
  make,
  variant,
  matchedDefinition,
}) => {
  if (matchedDefinition) return false;
  if (models.length) return true;

  const tokens = lower
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .filter((token) => !VEHICLE_QUERY_STOPWORDS.has(token));

  return Boolean(
    (make && tokens.length >= 1) || (variant && tokens.length >= 1),
  );
};

const isFollowUpVehicleQuery = (lower) =>
  /^(show colors?|show colours?|show features?|show specs?|calculate emi|emi|change city|show similar|similar cars|compare with|show price breakup|price breakup|show variants?|open features|show safety|show engine|show adas|show only differences)\b/i.test(
    lower.trim(),
  );

const hasExplicitTopLevelEntity = ({
  models,
  make,
  registrationNumber,
  explicitLast4,
  loanId,
  customerId,
  mobile,
}) =>
  Boolean(
    models.length ||
    make ||
    registrationNumber ||
    explicitLast4 ||
    loanId ||
    customerId ||
    mobile,
  );

const shouldUseSelectedEntity = ({
  lower,
  intent,
  selectedEntity,
  freshEntities,
}) => {
  if (!selectedEntity) return false;
  if (hasExplicitTopLevelEntity(freshEntities)) return false;

  const sameCaseLanguage = /\b(this|selected|same|that one|this one)\b/i.test(
    lower,
  );
  if (sameCaseLanguage) return true;

  // For closure ambiguity selection, selectedEntity may be sent with same original query.
  if (intent === "loan_closure_pos" && selectedEntity?.entityType === "loan")
    return true;

  return false;
};

/* -------------------------------------------------------------------------- */
/* MAIN ROUTER                                                                 */
/* -------------------------------------------------------------------------- */

export const routeAiAgentIntent = ({
  message = "",
  context = {},
  selectedEntity = null,
  filters = {},
} = {}) => {
  const text = normalizeText(message);
  const lower = text.toLowerCase();

  let matchedDefinition = sortedDefinitions.find((definition) =>
    definition.patterns.some((pattern) => pattern.test(text)),
  );

  const models = extractModels(lower, context);
  const make = extractMake(lower);
  const explicitVariant = extractVariant(text);
  const normalizedRegistration =
    normalizeVehicleNumber(text).match(/[A-Z]{2}\d{1,2}[A-Z]{1,3}\d{4}/)?.[0] ||
    "";
  const extractExplicitVehicleLast4 = (text = "", lower = "") => {
    const trimmed = String(text).trim();

    // Standalone 4 digits should still work: "7077"
    if (/^\d{4}$/.test(trimmed)) return trimmed;

    // Full registration number should produce last4 from registration.
    const fullReg =
      normalizeVehicleNumber(text).match(
        /[A-Z]{2}\d{1,2}[A-Z]{1,3}\d{4}/,
      )?.[0] || "";
    if (fullReg) return fullReg.slice(-4);

    // Only treat 4 digits as vehicle last4 if query is explicitly about a vehicle/customer/loan lookup.
    const last4Allowed =
      /\b(vehicle|car|registration|reg|number|ending|loan closure|closure|pos|principal outstanding|insurance|policy|customer|vehicle 360)\b/i.test(
        lower,
      );

    if (!last4Allowed) return "";

    const match = trimmed.match(/\b\d{4}\b/);
    return match?.[0] || "";
  };

  if (
    !matchedDefinition &&
    (normalizedRegistration || /^\s*\d{4}\s*$/.test(text))
  ) {
    matchedDefinition = sortedDefinitions.find(
      (definition) => definition.intent === "vehicle_registration_search",
    );
  }

  if (
    likelyVehicleCatalogueQuery({
      lower,
      models,
      make,
      variant: explicitVariant,
      matchedDefinition,
    })
  ) {
    matchedDefinition = sortedDefinitions.find(
      (definition) => definition.intent === "vehicle_pricelist",
    );
  }

  const explicitLast4 = extractExplicitVehicleLast4(text, lower);

  const intent = matchedDefinition?.intent || "generic_search";

  const loanId = text.match(/\bLN-\d{4}-\d+\b/i)?.[0]?.toUpperCase() || "";
  const mobile = text.match(/\b[6-9]\d{9}\b/)?.[0] || "";
  const customerId =
    text.match(/\bACILLP-\d{4}-\d+\b/i)?.[0]?.toUpperCase() || "";

  const explicitCity = extractExplicitCity(lower);
  const vehicleIntent = VEHICLE_CONTEXT_INTENTS.has(intent);
  const customerIntent = CUSTOMER_CONTEXT_INTENTS.has(intent);
  const reportNoVehicleContext =
    REPORT_INTENTS_THAT_MUST_NOT_INHERIT_VEHICLE.has(intent);

  const freshEntities = {
    models,
    make,
    registrationNumber: normalizedRegistration,
    explicitLast4,
    loanId,
    customerId,
    mobile,
  };

  const safeVehicleFollowUp =
    vehicleIntent && !reportNoVehicleContext && isFollowUpVehicleQuery(lower);
  const canCarryVehicleContext =
    vehicleIntent &&
    !reportNoVehicleContext &&
    !models.length &&
    !make &&
    !normalizedRegistration &&
    !explicitLast4 &&
    safeVehicleFollowUp;

  const useSelected = shouldUseSelectedEntity({
    lower,
    intent,
    selectedEntity,
    freshEntities,
  });

  const registrationNumber =
    normalizedRegistration ||
    (useSelected
      ? selectedEntity?.registrationNumber ||
        selectedEntity?.context?.registrationNumber
      : "") ||
    (customerIntent && !vehicleIntent
      ? context?.registrationNumber ||
        context?.entities?.registrationNumber ||
        filters?.registrationNumber
      : "") ||
    "";

  const last4 =
    explicitLast4 ||
    extractVehicleLast4(registrationNumber) ||
    (useSelected
      ? selectedEntity?.last4 || selectedEntity?.context?.last4
      : "") ||
    (customerIntent && !vehicleIntent
      ? context?.last4 ||
        context?.entities?.last4 ||
        filters?.last4 ||
        filters?.vehicleLast4
      : "");

  const model =
    models[0] ||
    (canCarryVehicleContext
      ? context?.model || context?.entities?.model || filters?.model || ""
      : "");

  const carriedModels =
    models.length > 0
      ? models
      : canCarryVehicleContext && Array.isArray(context?.entities?.models)
        ? context.entities.models
        : model
          ? [model]
          : [];

  const variant =
    explicitVariant ||
    (canCarryVehicleContext
      ? context?.variant || context?.entities?.variant || filters?.variant || ""
      : "");

  const city =
    explicitCity ||
    (canCarryVehicleContext
      ? context?.city || context?.entities?.city || filters?.city || ""
      : "") ||
    (vehicleIntent ? "new-delhi" : "");

  const budget = extractBudget(lower);
  const emiMax = extractEmiMax(lower);
  const emiInputs = extractEmiInputs(lower);
  const manualAmount = extractManualAmount(lower);

  const customerName =
    extractCustomerName(text, intent, carriedModels) ||
    (!hasExplicitTopLevelEntity(freshEntities) && customerIntent
      ? context?.customerName ||
        context?.entities?.customerName ||
        filters?.customerName ||
        filters?.customer ||
        ""
      : "");

  const entities = compactObject({
    selectedEntityId: useSelected
      ? selectedEntity?.id || selectedEntity?._id
      : "",
    selectedEntityType: useSelected ? selectedEntity?.entityType : "",
    customerName,
    customerId,
    mobile,
    loanId,
    registrationNumber,
    last4,
    make,
    model,
    models: carriedModels,
    variant,
    city,
    feature: extractFeature(lower),
    bodyType: extractBodyType(lower),
    fuelType: extractFuelType(lower),
    transmission: extractTransmission(lower),
    color: extractColor(lower),
    useCase: extractUseCase(lower),
    section: extractSection(lower),
    emiMax,
    manualAmount,
    ...budget,
    ...emiInputs,
    dateRange: parseDateRange(lower),
  });

  return {
    intent,
    definition: matchedDefinition || null,
    confidence: matchedDefinition ? 0.9 : 0.35,
    entities,
    collections: matchedDefinition?.collections || [],
    widgetType: matchedDefinition?.widgetType || "records_table",
    failureMessage:
      matchedDefinition?.failureMessage ||
      "I could not identify the exact request.",
    structured: Boolean(matchedDefinition),
    queryPlan: {
      matchedIntent: intent,
      rawModels: models,
      carriedVehicleContext: canCarryVehicleContext,
      selectedEntityUsed: useSelected,
      selectedEntityDropReason:
        selectedEntity && !useSelected
          ? "New explicit entity or no same-case language"
          : "",
      physicalCollectionsHint: matchedDefinition?.collections || [],
    },
  };
};
