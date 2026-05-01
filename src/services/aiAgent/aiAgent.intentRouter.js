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
  "city hybrid",
  "slavia",
  "creta n line",
  "creta",
  "venue n line",
  "venue",
  "i20 n line",
  "i20",
  "i10",
  "grand i10",
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
  "tata",
  "mahindra",
  "kia",
  "toyota",
  "mg",
];

const FEATURE_HINTS = [
  "sunroof",
  "6 airbags",
  "airbag",
  "airbags",
  "adas",
  "wireless charging",
  "ventilated seats",
  "cruise control",
  "ventilated seats",
  "rear camera",
  "360 camera",
  "alloy wheels",
  "climate control",
  "hill assist",
  "isofix",
  "lane keep assist",
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
  "features",
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
]);

const BODY_TYPE_HINTS = [
  "suv",
  "suvs",
  "compact suv",
  "sedan",
  "sedans",
  "hatchback",
  "hatchbacks",
  "mpv",
  "mpvs",
  "7 seater",
  "7-seater",
  "premium suv",
];

const FUEL_HINTS = ["petrol", "diesel", "cng", "electric", "ev", "hybrid"];
const TRANSMISSION_HINTS = ["automatic", "manual", "amt", "cvt", "dct", "ivt", "at", "mt"];
const COLOR_HINTS = ["black", "white", "red", "blue", "grey", "gray", "silver", "green", "orange", "brown", "gold", "matte", "dual tone", "dual-tone"];

export const INTENT_DEFINITIONS = [
  {
    intent: "vehicle_price_history",
    priority: 6,
    patterns: [/\b(price history|price trend|price changed|price change|price updated|price updates|price hike|got cheaper|variants? added|new variants|updated today|updated this week|added this month|added in)\b/i],
    collections: ["price_history"],
    requiredEntities: [],
    optionalEntities: ["make", "model", "variant", "city", "dateRange"],
    widgetType: "vehicle_price_history",
    failureMessage: "No stored price history records matched this request.",
  },
  {
    intent: "vehicle_color_search",
    priority: 8,
    patterns: [/\b(which|show|find)\b.*\b(black|white|red|blue|grey|gray|silver|green|orange|brown|gold|matte|dual[- ]tone)\b.*\b(cars?|models?|suvs?|sedans?|hatchbacks?)\b/i, /\b(which|show|find)\b.*\b(cars?|models?|suvs?|sedans?|hatchbacks?)\b.*\b(black|white|red|blue|grey|gray|silver|green|orange|brown|gold|matte|dual[- ]tone)\b/i],
    collections: ["vehicle_colors", "vehicles", "vehicle_features"],
    requiredEntities: ["color"],
    optionalEntities: ["make", "model", "budgetMax", "bodyType"],
    widgetType: "vehicle_color_search",
    failureMessage: "No stored color records matched this request.",
  },
  {
    intent: "customer_data_quality_report",
    priority: 5,
    patterns: [/\b(kyc pending|customers? missing|missing pan|missing aadhaar|missing aadhar|missing email|missing mobile|missing address|duplicate customers?|duplicate mobile|duplicate pan)\b/i],
    collections: ["customers"],
    requiredEntities: [],
    optionalEntities: ["issueType"],
    widgetType: "customer_data_quality_report",
    failureMessage: "No customer data quality issues matched this request.",
  },
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
    patterns: [/\b(does|has|have|available|with|how many|what is|what transmission)\b.*\b(sunroof|airbags?|6 airbags|adas|wireless charging|ventilated seats|mileage|boot space|ground clearance|engine displacement|transmission|camera|cruise|alloy wheels|climate control|hill assist|isofix|lane keep assist|automatic|dct|cvt|abs|esp|tpms)\b/i],
    collections: ["vehicle_features"],
    requiredEntities: ["model", "feature"],
    optionalEntities: ["make", "variant"],
    widgetType: "vehicle_feature_answer",
    failureMessage: "No stored feature value matched this question.",
  },
  {
    intent: "vehicle_feature_discovery",
    priority: 18,
    patterns: [/\b(which|cheapest|show|find)\b.*\b(variants?|cars?|suvs?|sedans?|hatchbacks?)\b.*\b(have|with|sunroof|airbags?|6 airbags|adas|wireless charging|ventilated seats|360 camera|tpms|hill assist|isofix|lane keep assist|cruise control|automatic climate control)\b/i, /\b(cheapest|cars?|suvs?|sedans?)\b.*\b(with|have)\b.*\b(sunroof|adas|6 airbags|wireless charging|ventilated seats)\b/i],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: ["feature"],
    optionalEntities: ["model", "budgetMax", "bodyType", "city"],
    widgetType: "vehicle_feature_discovery",
    failureMessage: "No variants matched the requested feature in stored catalogue data.",
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
    intent: "vehicle_emi_budget_search",
    priority: 32,
    patterns: [/\b(emi)\b.*\b(under|below|less than|upto|up to)\b/i, /\b(lowest emi|low emi)\b/i],
    collections: ["vehicles"],
    requiredEntities: ["emiMax"],
    optionalEntities: ["budgetMax", "bodyType", "fuelType", "transmission"],
    widgetType: "vehicle_emi_recommendations",
    failureMessage: "No vehicles matched the requested EMI budget.",
  },
  {
    intent: "vehicle_emi_calculator",
    priority: 34,
    patterns: [/\b(emi|down payment|roi|interest|tenure|finance amount|calculate emi)\b/i],
    collections: ["vehicles"],
    requiredEntities: [],
    optionalEntities: ["model", "variant", "city"],
    widgetType: "vehicle_emi_calculator",
    failureMessage: "Ask EMI with a vehicle, price, or amount.",
  },
  {
    intent: "vehicle_variant_difference",
    priority: 36,
    patterns: [/\b(difference between|what extra|extra do i get|missing in|vs)\b.*\b(variant|sx|vx|zx|hte|htk|htx|hx\d+|base|top|opt|plus)\b/i],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: ["model"],
    optionalEntities: ["variant"],
    widgetType: "vehicle_variant_difference",
    failureMessage: "Ask with two variants to compare their differences.",
  },
  {
    intent: "vehicle_best_variant_recommendation",
    priority: 38,
    patterns: [/\b(which .*variant should i buy|best .*variant|best value .*variant|value for money|worth it|worth extra|cheapest .*with|cheapest .*automatic|best automatic variant|best family variant)\b/i],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: ["model"],
    optionalEntities: ["feature", "transmission", "budgetMax"],
    widgetType: "vehicle_variant_recommendation",
    failureMessage: "Ask with a model to recommend a variant.",
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
    intent: "vehicle_safety_expert",
    priority: 42,
    patterns: [/\b(safest|safety|6 airbags|adas|esc|tpms|isofix|hill assist|blind spot|lane keep|adaptive cruise|collision warning)\b/i],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: [],
    optionalEntities: ["budgetMax", "bodyType", "model"],
    widgetType: "vehicle_safety_results",
    failureMessage: "No stored safety feature records matched this request.",
  },
  {
    intent: "vehicle_body_type_search",
    priority: 43,
    patterns: [/\b(show|find)?\s*(sedans?|suvs?|hatchbacks?|mpvs?|7[- ]seater cars?|compact suvs?|premium suvs?)\b/i],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: ["bodyType"],
    optionalEntities: ["budgetMax", "fuelType", "transmission"],
    widgetType: "vehicle_recommendation_results",
    failureMessage: "No stored cars matched this body type.",
  },
  {
    intent: "vehicle_fuel_transmission_search",
    priority: 44,
    patterns: [/\b(petrol|diesel|cng|electric|ev|hybrid|automatic|manual|amt|cvt|dct|ivt)\b.*\b(cars?|suvs?|sedans?|hatchbacks?|variants?)\b/i, /\b(cars?|suvs?|sedans?|hatchbacks?)\b.*\b(petrol|diesel|cng|electric|ev|hybrid|automatic|manual|amt|cvt|dct|ivt)\b/i],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: [],
    optionalEntities: ["budgetMax", "bodyType", "fuelType", "transmission"],
    widgetType: "vehicle_recommendation_results",
    failureMessage: "No stored cars matched this fuel or transmission.",
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
    intent: "vehicle_dimension_space_search",
    priority: 51,
    patterns: [/\b(biggest boot|boot space|ground clearance|fuel tank|wheelbase|spacious|7 seater|7-seater|large fuel tank|longest wheelbase)\b/i],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: [],
    optionalEntities: ["budgetMax", "bodyType"],
    widgetType: "vehicle_spec_ranking",
    failureMessage: "No stored dimension or space records matched this request.",
  },
  {
    intent: "vehicle_performance_mileage_search",
    priority: 52,
    patterns: [/\b(highest mileage|best mileage|mileage above|good mileage|most powerful|turbo petrol|power above|bhp|ps|torque|engine)\b/i],
    collections: ["vehicle_features", "vehicles"],
    requiredEntities: [],
    optionalEntities: ["budgetMax", "bodyType", "fuelType", "transmission"],
    widgetType: "vehicle_spec_ranking",
    failureMessage: "No stored performance or mileage records matched this request.",
  },
  {
    intent: "vehicle_budget_search",
    priority: 53,
    patterns: [/\b(under|below|less than|upto|up to|between)\b.*\d+\s*(lakh|lac|l|cr|crore|₹|rs|inr)\b/i, /\b(best cars?|top cars?|best value|feature loaded|family cars?|city automatic|highway car|first car|upgrade from hatchback)\b/i],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: [],
    optionalEntities: ["budgetMin", "budgetMax", "bodyType", "fuelType", "transmission", "feature"],
    widgetType: "vehicle_recommendation_results",
    failureMessage: "No vehicles matched these filters.",
  },
  {
    intent: "vehicle_use_case_recommendation",
    priority: 54,
    patterns: [/\b(best|recommend|which car)\b.*\b(family|parents|city driving|highway|daily running|low emi|safe|value|feature-loaded|feature loaded|long drives|chauffeur|rear seat|office use|first car|upgrade|premium sedan)\b/i],
    collections: ["vehicles", "vehicle_features"],
    requiredEntities: [],
    optionalEntities: ["budgetMax", "bodyType", "transmission", "useCase"],
    widgetType: "vehicle_recommendation_results",
    failureMessage: "No stored vehicles matched this use-case request.",
  },
  {
    intent: "vehicle_price_breakup",
    priority: 55,
    patterns: [/\b(price breakup|rto|insurance amount|tcs|accessories|on road without accessories|on-road without accessories|on road with accessories|on-road with accessories)\b/i],
    collections: ["vehicles"],
    requiredEntities: ["model"],
    optionalEntities: ["make", "variant", "city"],
    widgetType: "vehicle_price_breakup",
    failureMessage: "Ask for price breakup with a model and optional variant.",
  },
  {
    intent: "vehicle_pricelist",
    priority: 60,
    patterns: [/\b(pricelist|price list|price|pricing|prices|rate list|on road|on-road|ex showroom|ex-showroom|variant price|new car price|new|variants?|cheapest|top model|automatic .*variants?|manual .*variants?|petrol .*variants?|diesel .*variants?|cng .*variants?|active .*variants?|discontinued .*variants?|sorted by price|lowest on-road|lowest on road)\b/i],
    collections: ["vehicles"],
    requiredEntities: ["model"],
    optionalEntities: ["make", "variant", "city"],
    widgetType: "vehicle_pricelist",
    failureMessage: "Ask for a model such as Verna, City, or Slavia.",
  },
  {
    intent: "vehicle_launch_status",
    priority: 62,
    patterns: [/\b(new .*cars added|latest new car additions|recently updated models|recently added variants|active new launches|new launches)\b/i],
    collections: ["vehicles", "price_history"],
    requiredEntities: [],
    optionalEntities: ["make", "dateRange"],
    widgetType: "latest_catalogue_updates",
    failureMessage: "Launch status is not captured in current database.",
  },
  {
    intent: "loan_closure_pos",
    priority: 65,
    patterns: [/\b(pos|principal outstanding|loan closure|closure amount|approx closure|foreclosure|settlement amount|current outstanding)\b/i],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["customerName", "last4", "registrationNumber", "loanId"],
    widgetType: "loan_closure_card",
    failureMessage: "No matching loan was found for POS or closure.",
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
    intent: "loan_pending_approval_report",
    priority: 72,
    patterns: [/\b(pending approval|approval pending|not approved|approval stage)\b/i],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["dateRange"],
    widgetType: "records_table",
    failureMessage: "No pending approval loan cases were found.",
  },
  {
    intent: "loan_business_report",
    priority: 73,
    patterns: [/\b(total business|business this month|total cases this month|total car business|total new car business|total used car business|total cash cars|cash car business|book value)\b/i],
    collections: ["loans", "deliveryOrders", "insurance"],
    requiredEntities: [],
    optionalEntities: ["dateRange"],
    widgetType: "loan_business_report",
    failureMessage: "No loan business records matched this request.",
  },
  {
    intent: "loan_missing_registration_report",
    priority: 74,
    patterns: [/\b(loans? without registration|loan registration missing|rc number missing in loans?|rc missing in loans?)\b/i],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["dateRange"],
    widgetType: "records_table",
    failureMessage: "No loan records with missing registration were found.",
  },
  {
    intent: "loan_invoice_missing_report",
    priority: 75,
    patterns: [/\b(invoice missing|invoice pending|invoice not received|invoice number missing)\b/i],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["dateRange"],
    widgetType: "records_table",
    failureMessage: "No loan records with missing invoice were found.",
  },
  {
    intent: "loan_insurance_missing_report_basic",
    priority: 76,
    patterns: [/\b(insurance missing in loan|policy number missing in loan|insurance company missing in loan)\b/i],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["dateRange"],
    widgetType: "records_table",
    failureMessage: "No loan records with missing insurance fields were found.",
  },
  {
    intent: "loan_status",
    priority: 77,
    patterns: [/\b(loan status|loan case|bank status|approval status|loan of|case of)\b/i],
    collections: ["loans"],
    requiredEntities: [],
    optionalEntities: ["customerName", "last4", "registrationNumber", "loanId"],
    widgetType: "loan_case_card",
    failureMessage: "No matching loan case was found.",
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
    intent: "vehicle_registration_search",
    priority: 95,
    patterns: [/\b(vehicle number|registration number|reg number|car ending|vehicle ending)\b/i],
    collections: ["vehicle_master_records", "loans"],
    requiredEntities: [],
    optionalEntities: ["registrationNumber", "last4", "model"],
    widgetType: "records_table",
    failureMessage: "No matching vehicle registration records were found.",
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
    intent: "vehicle_data_quality_report",
    priority: 101,
    patterns: [/\b(vehicles? without registration|missing engine number|missing chassis number|fuel type blank|hypothecation missing)\b/i],
    collections: ["vehicle_master_records"],
    requiredEntities: [],
    optionalEntities: ["issueType"],
    widgetType: "records_table",
    failureMessage: "No customer-linked vehicle data quality records matched.",
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
    intent: "customer_lookup",
    priority: 900,
    patterns: [/\b(find customer|search customer|customer details|mobile number|customerid|customer id)\b/i, /^\s*(find|search)\s+[a-z]/i],
    collections: ["customers"],
    requiredEntities: [],
    optionalEntities: ["customerName", "mobile", "customerId"],
    widgetType: "customer_card",
    failureMessage: "No matching customer was found.",
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

const VEHICLE_CONTEXT_INTENTS = new Set([
  "vehicle_colors",
  "vehicle_features",
  "vehicle_feature_answer",
  "vehicle_price_breakup",
  "vehicle_pricelist",
  "vehicle_comparison",
  "similar_cars",
  "vehicle_color_search",
  "vehicle_feature_discovery",
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

export const extractModels = (lower) => {
  const compact = lower.replace(/[^a-z0-9]/g, "");
  return [
    ...new Set(
      [...MODEL_HINTS].sort((a, b) => b.length - a.length).filter(
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

const extractBodyType = (lower) =>
  BODY_TYPE_HINTS.find((bodyType) => new RegExp(`\\b${bodyType.replace("-", "[- ]").replace(/\s+/g, "\\s+")}\\b`, "i").test(lower)) || "";

const extractFuelType = (lower) =>
  FUEL_HINTS.find((fuel) => new RegExp(`\\b${fuel}\\b`, "i").test(lower)) || "";

const extractTransmission = (lower) =>
  TRANSMISSION_HINTS.find((transmission) => new RegExp(`\\b${transmission}\\b`, "i").test(lower)) || "";

const extractColor = (lower) =>
  COLOR_HINTS.find((color) => new RegExp(`\\b${color.replace("-", "[- ]").replace(/\s+/g, "\\s+")}\\b`, "i").test(lower)) || "";

const moneyToRupees = (raw = "", suffix = "") => {
  const value = Number(String(raw).replace(/,/g, ""));
  if (!Number.isFinite(value)) return null;
  const unit = String(suffix || "").toLowerCase();
  if (/cr|crore/.test(unit)) return value * 10000000;
  if (/lakh|lac|\bl\b/.test(unit)) return value * 100000;
  return value;
};

const extractBudget = (lower) => {
  const between = lower.match(/\bbetween\s+([\d,.]+)\s*(lakh|lac|l|cr|crore)?\s+(?:and|to|-)\s+([\d,.]+)\s*(lakh|lac|l|cr|crore)\b/i);
  if (between) {
    const min = moneyToRupees(between[1], between[2] || between[4]);
    const max = moneyToRupees(between[3], between[4]);
    return compactObject({ budgetMin: min, budgetMax: max });
  }
  const under = lower.match(/\b(?:under|below|less than|upto|up to)\s*₹?\s*([\d,.]+)\s*(lakh|lac|l|cr|crore|rs|inr)?\b/i);
  if (under) {
    if (!under[2] && /\bemi\b/i.test(lower)) return {};
    return compactObject({ budgetMax: moneyToRupees(under[1], under[2] || "lakh") });
  }
  return {};
};

const extractEmiMax = (lower) => {
  const match = lower.match(/\bemi\s*(?:under|below|less than|upto|up to)?\s*₹?\s*([\d,.]+)\b/i) || lower.match(/\b(?:under|below|less than|upto|up to)\s*₹?\s*([\d,.]+)\s*(?:emi)\b/i);
  return match ? Number(String(match[1]).replace(/[^\d.]/g, "")) || null : null;
};

const extractEmiInputs = (lower) => {
  const downPaymentAmount = lower.match(/\b([\d,.]+)\s*(lakh|lac|l|cr|crore|rs|inr)?\s+down payment\b/i);
  const downPaymentPercent = lower.match(/\b(\d+(?:\.\d+)?)\s*%\s+down\b/i);
  const years = lower.match(/\b(\d+)\s*(?:years?|yrs?)\b/i);
  const months = lower.match(/\b(\d+)\s*months?\b/i);
  const roi = lower.match(/\b(?:at\s*)?(\d+(?:\.\d+)?)\s*(?:percent|%)\b/i);
  return compactObject({
    downPayment: downPaymentAmount ? moneyToRupees(downPaymentAmount[1], downPaymentAmount[2] || "lakh") : null,
    downPaymentPercent: downPaymentPercent ? Number(downPaymentPercent[1]) : null,
    tenureMonths: years ? Number(years[1]) * 12 : months ? Number(months[1]) : null,
    annualRate: roi ? Number(roi[1]) : null,
  });
};

const extractCity = (lower, context = {}, filters = {}) => {
  const explicit = CITY_HINTS.find((city) => new RegExp(`\\b${city.replace("-", "\\s*-?\\s*")}\\b`, "i").test(lower));
  return normalizeCitySlug(explicit || context?.city || context?.entities?.city || filters?.city || "");
};

const extractVariant = (message, lower) =>
  normalizeText(
    message.match(/\b(hte|htk|htx|gtx|sx|vx|zx|lxi|zxi|vxi|xza|xz|alpha|delta|sigma|sportz|asta|premium|comfortline|highline|hx\d+|s\s?opt|sx\s?opt|top|base|ivt|dct|mt|at|amt|cvt|turbo)\b(?:\s+\b(plus|turbo|ivt|dct|mt|at|amt|cvt|dt|opt|automatic|manual)\b)*/i)?.[0] || "",
  );

const likelyVehicleCatalogueQuery = ({ lower, models, make, variant, matchedDefinition }) => {
  if (matchedDefinition) return false;
  if (models.length) return true;
  const tokens = lower
    .replace(/[^a-z0-9]+/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .filter((token) => !VEHICLE_QUERY_STOPWORDS.has(token));
  return Boolean(
    (make && tokens.length >= 2) ||
    (variant && tokens.length >= 1) ||
    tokens.some((token) => MODEL_HINTS.includes(token)),
  );
};

const extractCustomerName = (message, intent, models) => {
  if (!["latest_insurance", "loan_status", "loan_closure_pos", "customer_lookup", "customer_360", "vehicle_360"].includes(intent)) return "";
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
  let matchedDefinition = sortedDefinitions.find((definition) =>
    definition.patterns.some((pattern) => pattern.test(text)),
  );
  const models = extractModels(lower);
  const make = extractMake(lower);
  const explicitVariant = extractVariant(text, lower);
  const normalizedRegistration = normalizeVehicleNumber(text).match(/[A-Z]{2}\d{1,2}[A-Z]{1,3}\d{4}/)?.[0] || "";
  const explicitLast4 = extractVehicleLast4(text);
  if (!matchedDefinition && (normalizedRegistration || /^\s*\d{4}\s*$/.test(text))) {
    matchedDefinition = sortedDefinitions.find((definition) => definition.intent === "vehicle_registration_search");
  }
  if (likelyVehicleCatalogueQuery({ lower, models, make, variant: explicitVariant, matchedDefinition })) {
    matchedDefinition = sortedDefinitions.find((definition) => definition.intent === "vehicle_pricelist");
  }
  const intent = matchedDefinition?.intent || "generic_search";
  const loanId = text.match(/\bLN-\d{4}-\d+\b/i)?.[0]?.toUpperCase() || "";
  const mobile = text.match(/\b[6-9]\d{9}\b/)?.[0] || "";
  const customerId = text.match(/\bACILLP-\d{4}-\d+\b/i)?.[0]?.toUpperCase() || "";
  const registrationNumber =
    normalizedRegistration ||
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
    extractVehicleLast4(registrationNumber);
  const hasFreshVehicleContext = Boolean(models.length || make || registrationNumber || explicitLast4 || normalizedRegistration);
  const canUseVehicleContext = VEHICLE_CONTEXT_INTENTS.has(intent);
  const canUseCustomerContext = CUSTOMER_CONTEXT_INTENTS.has(intent);
  const model = models[0] || (!hasFreshVehicleContext && canUseVehicleContext ? context?.model || context?.entities?.model || filters?.model || "" : "");
  const variant = explicitVariant || (!hasFreshVehicleContext && canUseVehicleContext ? context?.variant || context?.entities?.variant || filters?.variant || "" : "");
  const budget = extractBudget(lower);
  const emiMax = extractEmiMax(lower);
  const emiInputs = extractEmiInputs(lower);

  return {
    intent,
    definition: matchedDefinition || null,
    confidence: matchedDefinition ? 0.9 : 0.35,
    entities: compactObject({
      selectedEntityId: selectedEntity?.id || selectedEntity?._id,
      selectedEntityType: selectedEntity?.entityType,
      customerName: extractCustomerName(text, intent, models) || (!hasFreshVehicleContext && canUseCustomerContext ? context?.customerName || context?.entities?.customerName || filters?.customerName || filters?.customer || "" : ""),
      customerId,
      mobile,
      loanId,
      registrationNumber,
      last4,
      make,
      model,
      models,
      variant,
      city: extractCity(lower, context, filters),
      feature: extractFeature(lower),
      bodyType: extractBodyType(lower),
      fuelType: extractFuelType(lower),
      transmission: extractTransmission(lower),
      color: extractColor(lower),
      emiMax,
      ...budget,
      ...emiInputs,
      dateRange: parseDateRange(lower),
    }),
    collections: matchedDefinition?.collections || [],
    widgetType: matchedDefinition?.widgetType || "records_table",
    failureMessage: matchedDefinition?.failureMessage || "I could not identify the exact request.",
    structured: Boolean(matchedDefinition),
  };
};
