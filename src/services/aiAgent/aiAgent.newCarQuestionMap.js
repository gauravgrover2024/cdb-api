import { normalizeText } from "./aiAgent.normalizers.js";

export const NEW_CAR_CANVAS_TYPES = [
  "aci_home_canvas",
  "recommendation_results_canvas",
  "brand_results_canvas",
  "model_overview_canvas",
  "pricelist_canvas",
  "price_breakup_canvas",
  "feature_explorer_canvas",
  "color_studio_canvas",
  "comparison_canvas",
  "similar_cars_canvas",
  "variant_finder_canvas",
  "variant_upgrade_value_canvas",
  "emi_calculator_canvas",
  "monthly_budget_planner_canvas",
  "finance_guide_canvas",
  "offers_canvas",
  "aci_quotation_canvas",
  "availability_waiting_canvas",
  "service_center_canvas",
  "ownership_service_warranty_canvas",
  "tco_canvas",
  "fuel_decision_canvas",
  "resale_value_canvas",
  "lifestyle_fit_canvas",
  "safety_advisor_canvas",
  "feature_match_builder_canvas",
  "feature_value_score_canvas",
  "senior_friendly_advisor_canvas",
  "space_practicality_canvas",
  "performance_spec_ranking_canvas",
];

export const NEW_CAR_INLINE_TYPES = [
  "feature_answer_card",
  "spec_answer_card",
  "short_price_card",
  "model_ambiguity_card",
  "variant_ambiguity_card",
  "finance_faq_card",
  "offer_summary_card",
  "service_center_answer_card",
  "availability_answer_card",
  "fallback_card",
];

const PRIORITY_ORDER = [
  "aci_new_car_quotation",
  "vehicle_test_drive_request",
  "vehicle_callback_request",
  "vehicle_emi_calculator",
  "vehicle_emi_options",
  "vehicle_monthly_budget_planner",
  "new_car_finance_faq",
  "new_car_loan_enquiry",
  "vehicle_offers",
  "vehicle_offer_lookup",
  "vehicle_availability",
  "vehicle_waiting_period",
  "new_car_service_center_search",
  "new_car_service_cost",
  "new_car_warranty",
  "new_car_ownership_guide",
  "vehicle_tco_analysis",
  "vehicle_fuel_decision_advisor",
  "vehicle_resale_value_analysis",
  "vehicle_lifestyle_fit_score",
  "vehicle_senior_friendly_recommendation",
  "vehicle_space_practicality_advisor",
  "vehicle_performance_advisor",
  "vehicle_spec_ranking",
  "vehicle_bad_roads_advisor",
  "vehicle_safety_search",
  "vehicle_safety_answer",
  "vehicle_safety_comparison",
  "vehicle_mileage_search",
  "vehicle_city_price",
  "vehicle_variant_price",
  "vehicle_pricelist",
  "vehicle_price_breakup",
  "vehicle_comparison",
  "vehicle_model_comparison",
  "vehicle_variant_comparison",
  "vehicle_variant_upgrade_value",
  "vehicle_variant_recommendation",
  "vehicle_feature_discovery",
  "vehicle_model_features_explorer",
  "vehicle_feature_answer",
  "vehicle_spec_lookup",
  "vehicle_colors",
  "vehicle_color_gallery",
  "vehicle_similar_cars",
  "vehicle_alternative_search",
  "vehicle_brand_search",
  "vehicle_budget_search",
  "vehicle_recommendation_discovery",
  "vehicle_body_type_search",
  "vehicle_use_case_search",
  "new_car_unavailable_or_out_of_scope",
];

const REGEX = {
  quotation:
    /\b(quotation|quote|best price|final price|best quotation|get quote|get quotation)\b/i,
  testDrive: /\btest drive\b/i,
  callback: /\bcallback|call me\b/i,
  emi: /\bemi|down payment|tenure|interest|roi|loan\b/i,
  budgetEmi:
    /\b(monthly budget|emi under|emi around|afford|down payment needed|can i buy with)\b/i,
  financeFaq:
    /\b(documents?|interest rate|processing fee|prepay|foreclosure|cibil|eligibility|loan without itr|maximum tenure|min(?:imum)? down payment)\b/i,
  offers: /\b(offers?|discount|bonus|festive|year-end|corporate discount|exchange bonus|loyalty bonus|scrappage)\b/i,
  availability:
    /\b(available|availability|waiting period|delivery time|immediate delivery|fast delivery|discontinued)\b/i,
  serviceCenter:
    /\b(service center|workshop|authori[sz]ed.*service|body shop|pickup and drop|nearest .*service)\b/i,
  ownership:
    /\b(service cost|maintenance|warranty|rsa|service interval|extended warranty|ownership cost)\b/i,
  tco: /\b(total cost of ownership|tco|real monthly cost|own for\s*\d+\s*years?)\b/i,
  fuelDecision:
    /\b(petrol vs diesel|petrol or diesel|diesel or petrol|cng vs petrol|cng or petrol|ev vs petrol|ev or petrol|fuel type is best|diesel worth)\b/i,
  resale:
    /\b(resale|depreciation|value retention|lowest depreciation)\b/i,
  lifestyle:
    /\b(lifestyle|daily \d+ ?km|office and family|city plus highway|chauffeur-driven)\b/i,
  senior:
    /\b(parents|senior citizens?|elderly|easy entry exit|high seating)\b/i,
  space:
    /\b(spacious|boot space|rear seat comfort|7-seater|luggage|practicality)\b/i,
  performance:
    /\b(performance|fastest|most powerful|turbo petrol|fun to drive|power)\b/i,
  specRanking:
    /\b(highest ground clearance|ground clearance|bad roads|rough roads|speed breakers|spec ranking)\b/i,
  colors:
    /\b(colors?|colours?|color gallery|show .* in (black|white|red|blue|grey|gray|silver|green|orange|brown|gold))\b/i,
  comparison:
    /\b(compare|comparison|\bvs\b|versus|difference between|which is better)\b/i,
  similar:
    /\b(similar cars|alternatives?|cars like|competitors|better alternative)\b/i,
  variantFinder:
    /\b(which .*variant should i buy|best .*variant|value .*variant|worth buying)\b/i,
  variantUpgrade:
    /\b(extra features|worth paying extra|price difference|what do i lose|is the top model worth|difference between)\b/i,
  featureQuestion:
    /\b(does|have|has|how many|what is)\b.*\b(sunroof|adas|airbags?|boot space|ground clearance|mileage|engine|wireless|ventilated|camera)\b/i,
  featureExplorer:
    /\b(show|all)\b.*\b(features?|specs?)\b/i,
  featureDiscovery:
    /\b(which .*variants? have|cars? with|suvs? with)\b.*\b(sunroof|adas|airbags?|ventilated|wireless|camera)\b/i,
  featureBuilder:
    /\b(i want|must have|all features)\b.*\b(automatic|sunroof|adas|airbags?|ventilated|wireless|camera)\b/i,
  safety:
    /\b(safest|safety|5-star|5 star|ncap|child safety|6 airbags|adas)\b/i,
  mileage:
    /\b(mileage|fuel efficient|running cost|cheapest to run|cost per km)\b/i,
  modelOverview:
    /\b(tell me about|is .* good|is .* worth buying|show .* details|what is special about)\b/i,
  brandSearch: /\b(show|best)\b.*\b(hyundai|tata|maruti|kia|toyota|honda|mahindra|skoda|volkswagen)\b/i,
  budgetSearch: /\b(under|below|less than|between)\b.*\b(\d+\s*l\b|lakh|lac|crore|cr|on-road)\b/i,
  bodyType: /\b(suv|sedan|hatchback|mpv|7-seater|7 seater|compact suv)\b/i,
  recommendation:
    /\b(which car should i buy|suggest me a car|best car for|first car buyer|value for money)\b/i,
  priceBreakup: /\b(price breakup|rto charges|insurance amount|other charges|on-road breakup)\b/i,
  price: /\b(pricelist|price|on-road|ex-showroom|variant price|cheapest variant|top model price)\b/i,
  outOfScope:
    /\b(used car|sell my used|payment status|loan closure|insurance renewal|bike loan|truck price|delivery order|receivable)\b/i,
};

const createConfig = (config) => ({
  ambiguityPolicy: "ask_before_assuming",
  entityRequirements: [],
  defaultActions: [],
  leadingQuestions: [],
  dataSources: [],
  frontendNotes: "",
  regexes: [],
  priority: 999,
  toolIntent: config.intent,
  ...config,
});

export const NEW_CAR_QUESTION_MAP = {
  vehicle_recommendation_discovery: createConfig({
    intent: "vehicle_recommendation_discovery",
    displayMode: "canvas",
    canvasType: "recommendation_results_canvas",
    inlineType: null,
    exampleQuestions: ["Which car should I buy?", "Suggest me a car"],
    entityRequirements: ["budget OR useCase"],
    leadingQuestions: [
      "What is your budget?",
      "What body type do you prefer?",
      "What will you use the car for?",
      "Manual or automatic?",
      "Petrol, diesel, CNG, hybrid, or electric?",
      "Is safety a priority?",
      "Do you want sunroof?",
      "Do you want low EMI?",
      "Mileage or performance?",
      "Premium features or value for money?",
    ],
    defaultActions: [
      { label: "Compare top 3", type: "ask", query: "Compare top 3 options" },
      { label: "Show variants", type: "ask", query: "Show variants" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Check offers", type: "open_canvas", canvasType: "offers_canvas" },
      { label: "Get quotation", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Book test drive", type: "lead", leadType: "test_drive" },
    ],
    dataSources: ["vehicles", "vehicle_features"],
    frontendNotes: "Group by model, not by all variants.",
    regexes: [REGEX.recommendation],
    priority: 60,
    toolIntent: "vehicle_use_case_recommendation",
  }),

  vehicle_budget_search: createConfig({
    intent: "vehicle_budget_search",
    displayMode: "canvas",
    canvasType: "recommendation_results_canvas",
    inlineType: null,
    exampleQuestions: ["SUVs under 20 lakh", "Automatic cars under 15 lakh"],
    leadingQuestions: [
      "Ex-showroom or on-road?",
      "Which city?",
      "Automatic only?",
      "Fuel preference?",
      "Active cars only?",
    ],
    defaultActions: [
      { label: "Show variants", type: "ask", query: "Show variants" },
      { label: "Compare top results", type: "ask", query: "Compare top results" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Check safety", type: "open_canvas", canvasType: "safety_advisor_canvas" },
      { label: "Check offers", type: "open_canvas", canvasType: "offers_canvas" },
      { label: "Get quotation", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicles", "vehicle_features"],
    frontendNotes: "Show models first and variants on drill-down.",
    regexes: [REGEX.budgetSearch],
    priority: 55,
    toolIntent: "vehicle_budget_search",
  }),

  vehicle_body_type_search: createConfig({
    intent: "vehicle_body_type_search",
    displayMode: "canvas",
    canvasType: "recommendation_results_canvas",
    inlineType: null,
    exampleQuestions: ["Best compact SUV", "Best 7-seater SUV"],
    leadingQuestions: [
      "5-seater or 7-seater?",
      "City or highway use?",
      "Automatic needed?",
      "Mileage vs comfort?",
      "Ground clearance priority?",
    ],
    defaultActions: [
      { label: "Compare", type: "ask", query: "Compare shortlisted options" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Check offers", type: "open_canvas", canvasType: "offers_canvas" },
      { label: "Service cost", type: "open_canvas", canvasType: "ownership_service_warranty_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Book test drive", type: "lead", leadType: "test_drive" },
    ],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.bodyType],
    priority: 52,
    toolIntent: "vehicle_body_type_search",
  }),

  vehicle_use_case_search: createConfig({
    intent: "vehicle_use_case_search",
    displayMode: "canvas",
    canvasType: "recommendation_results_canvas",
    inlineType: null,
    exampleQuestions: ["Best car for city", "Best car for elderly parents"],
    regexes: [REGEX.recommendation],
    priority: 50,
    toolIntent: "vehicle_use_case_recommendation",
  }),

  vehicle_brand_search: createConfig({
    intent: "vehicle_brand_search",
    displayMode: "canvas",
    canvasType: "brand_results_canvas",
    inlineType: null,
    exampleQuestions: ["Show Hyundai cars", "Kia SUVs under 20 lakh"],
    leadingQuestions: ["All models or within budget?", "Body type preference?", "City?", "Active models only?"],
    defaultActions: [
      { label: "Open model overview", type: "ask", query: "Open model overview" },
      { label: "Compare models", type: "ask", query: "Compare shortlisted models" },
      { label: "Offers", type: "open_canvas", canvasType: "offers_canvas" },
      { label: "Service centers", type: "open_canvas", canvasType: "service_center_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.brandSearch],
    priority: 49,
    toolIntent: "vehicle_budget_search",
  }),

  vehicle_model_overview: createConfig({
    intent: "vehicle_model_overview",
    displayMode: "canvas",
    canvasType: "model_overview_canvas",
    inlineType: null,
    exampleQuestions: ["Tell me about Verna", "Is Creta good?"],
    leadingQuestions: ["Price?", "Variants?", "Features?", "Colors?", "Compare with competitors?"],
    defaultActions: [
      { label: "Which variant should I buy", type: "open_canvas", canvasType: "variant_finder_canvas" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Offers", type: "open_canvas", canvasType: "offers_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Book test drive", type: "lead", leadType: "test_drive" },
    ],
    dataSources: ["vehicles", "vehicle_features", "vehicle_colors"],
    regexes: [REGEX.modelOverview],
    priority: 46,
    toolIntent: "vehicle_pricelist",
  }),

  vehicle_model_ambiguity: createConfig({
    intent: "vehicle_model_ambiguity",
    displayMode: "inline",
    canvasType: null,
    inlineType: "model_ambiguity_card",
    exampleQuestions: ["Show Venue price", "Show Safari colors"],
    defaultActions: [
      { label: "Compare both", type: "ask", query: "Compare both models" },
      { label: "Show all variants", type: "ask", query: "Show all variants" },
    ],
    dataSources: ["vehicles"],
    regexes: [],
    priority: 10,
    toolIntent: "vehicle_pricelist",
  }),

  vehicle_variant_ambiguity: createConfig({
    intent: "vehicle_variant_ambiguity",
    displayMode: "inline",
    canvasType: null,
    inlineType: "variant_ambiguity_card",
    exampleQuestions: ["Verna SX price", "Creta SX features"],
    defaultActions: [
      { label: "Compare SX variants", type: "ask", query: "Compare SX variants" },
      { label: "Show all variants", type: "ask", query: "Show all variants" },
    ],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [],
    priority: 11,
    toolIntent: "vehicle_pricelist",
  }),

  vehicle_pricelist: createConfig({
    intent: "vehicle_pricelist",
    displayMode: "canvas",
    canvasType: "pricelist_canvas",
    inlineType: null,
    exampleQuestions: ["Verna pricelist", "Show Verna price"],
    leadingQuestions: ["Ex-showroom or on-road?", "Which city?", "Automatic only?", "Fuel preference?"],
    defaultActions: [
      { label: "Toggle ex-showroom/on-road", type: "ask", query: "Show ex-showroom and on-road" },
      { label: "Change city", type: "ask", query: "Change city price" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Price breakup", type: "open_canvas", canvasType: "price_breakup_canvas" },
      { label: "Offers", type: "open_canvas", canvasType: "offers_canvas" },
      { label: "Compare variants", type: "open_canvas", canvasType: "comparison_canvas" },
      { label: "Features", type: "open_canvas", canvasType: "feature_explorer_canvas" },
      { label: "Colors", type: "open_canvas", canvasType: "color_studio_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Book test drive", type: "lead", leadType: "test_drive" },
    ],
    dataSources: ["vehicles"],
    frontendNotes: "Default city Delhi/New Delhi; mention fallback when city unavailable.",
    regexes: [REGEX.price],
    priority: 40,
    toolIntent: "vehicle_pricelist",
  }),

  vehicle_city_price: createConfig({
    intent: "vehicle_city_price",
    displayMode: "canvas",
    canvasType: "pricelist_canvas",
    inlineType: null,
    exampleQuestions: ["Verna price in Mumbai", "Creta price in Bangalore"],
    dataSources: ["vehicles"],
    regexes: [/\bprice in\b/i],
    priority: 39,
    toolIntent: "vehicle_city_change",
  }),

  vehicle_variant_price: createConfig({
    intent: "vehicle_variant_price",
    displayMode: "inline",
    canvasType: null,
    inlineType: "short_price_card",
    exampleQuestions: ["Verna SX price", "Verna SX on-road price"],
    defaultActions: [
      { label: "Open pricelist", type: "open_canvas", canvasType: "pricelist_canvas" },
      { label: "Change city", type: "ask", query: "Change city" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicles"],
    regexes: [/\b(sx|vx|zx|htx|variant)\b.*\bprice\b/i],
    priority: 38,
    toolIntent: "vehicle_pricelist",
  }),

  vehicle_price_breakup: createConfig({
    intent: "vehicle_price_breakup",
    displayMode: "canvas",
    canvasType: "price_breakup_canvas",
    inlineType: null,
    exampleQuestions: ["Show on-road breakup", "RTO charges of Creta"],
    defaultActions: [
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Check offers", type: "open_canvas", canvasType: "offers_canvas" },
      { label: "Get exact quotation", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Change city", type: "ask", query: "Change city" },
    ],
    dataSources: ["vehicles"],
    regexes: [REGEX.priceBreakup],
    priority: 37,
    toolIntent: "vehicle_price_breakup",
  }),

  vehicle_feature_answer: createConfig({
    intent: "vehicle_feature_answer",
    displayMode: "inline",
    canvasType: null,
    inlineType: "feature_answer_card",
    exampleQuestions: ["Does Verna SX have sunroof?"],
    defaultActions: [
      { label: "Open features", type: "open_canvas", canvasType: "feature_explorer_canvas" },
      { label: "Show variants with this feature", type: "ask", query: "Show variants with this feature" },
      { label: "Compare variants", type: "open_canvas", canvasType: "comparison_canvas" },
      { label: "Show price", type: "open_canvas", canvasType: "pricelist_canvas" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicle_features"],
    regexes: [REGEX.featureQuestion],
    priority: 35,
    toolIntent: "vehicle_feature_answer",
  }),

  vehicle_spec_lookup: createConfig({
    intent: "vehicle_spec_lookup",
    displayMode: "inline",
    canvasType: null,
    inlineType: "spec_answer_card",
    exampleQuestions: ["What is boot space of Verna?"],
    defaultActions: [
      { label: "Open features", type: "open_canvas", canvasType: "feature_explorer_canvas" },
      { label: "Compare variants", type: "open_canvas", canvasType: "comparison_canvas" },
      { label: "Show price", type: "open_canvas", canvasType: "pricelist_canvas" },
    ],
    dataSources: ["vehicle_features"],
    regexes: [REGEX.featureQuestion],
    priority: 34,
    toolIntent: "vehicle_feature_answer",
  }),

  vehicle_model_features_explorer: createConfig({
    intent: "vehicle_model_features_explorer",
    displayMode: "canvas",
    canvasType: "feature_explorer_canvas",
    inlineType: null,
    exampleQuestions: ["Show features of Verna"],
    defaultActions: [
      { label: "Search another feature", type: "ask", query: "Search another feature" },
      { label: "Compare selected variants", type: "open_canvas", canvasType: "comparison_canvas" },
      { label: "Find best variant", type: "open_canvas", canvasType: "variant_finder_canvas" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Get quotation", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicle_features", "vehicles"],
    frontendNotes:
      "Return grouped categories + selected variants; do not render all variants and all features at once.",
    regexes: [REGEX.featureExplorer],
    priority: 33,
    toolIntent: "vehicle_features",
  }),

  vehicle_feature_discovery: createConfig({
    intent: "vehicle_feature_discovery",
    displayMode: "canvas",
    canvasType: "feature_explorer_canvas",
    inlineType: null,
    exampleQuestions: ["Which Verna variants have sunroof?"],
    dataSources: ["vehicle_features", "vehicles"],
    regexes: [REGEX.featureDiscovery],
    priority: 32,
    toolIntent: "vehicle_feature_discovery",
  }),

  vehicle_must_have_feature_builder: createConfig({
    intent: "vehicle_must_have_feature_builder",
    displayMode: "canvas",
    canvasType: "feature_match_builder_canvas",
    inlineType: null,
    exampleQuestions: ["I want automatic, sunroof and 6 airbags under 15 lakh"],
    leadingQuestions: ["Must-have features?", "Optional features?", "Budget?", "SUV only?", "Automatic only?"],
    defaultActions: [
      { label: "Compare matching cars", type: "open_canvas", canvasType: "comparison_canvas" },
      { label: "Show closest matches", type: "ask", query: "Show closest matches" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.featureBuilder],
    priority: 31,
    toolIntent: "vehicle_feature_discovery",
  }),

  vehicle_safety_search: createConfig({
    intent: "vehicle_safety_search",
    displayMode: "canvas",
    canvasType: "safety_advisor_canvas",
    inlineType: null,
    exampleQuestions: ["Safest SUVs under 20 lakh"],
    leadingQuestions: [
      "Safety priority: NCAP, airbags, or ADAS?",
      "Budget range?",
      "Body type preference?",
      "City or highway usage?",
      "Automatic needed?",
    ],
    dataSources: ["vehicle_features", "vehicles"],
    regexes: [REGEX.safety],
    priority: 30,
    toolIntent: "vehicle_safety_expert",
  }),

  vehicle_safety_answer: createConfig({
    intent: "vehicle_safety_answer",
    displayMode: "inline",
    canvasType: null,
    inlineType: "feature_answer_card",
    exampleQuestions: ["Does Verna have 6 airbags?"],
    dataSources: ["vehicle_features"],
    regexes: [/(\bdoes\b|\bhas\b|\bhave\b|\bhow many\b).*\b(airbags?|adas|ncap|isofix|esc|esp|tpms|hill assist)\b/i],
    priority: 29,
    toolIntent: "vehicle_feature_answer",
  }),

  vehicle_safety_comparison: createConfig({
    intent: "vehicle_safety_comparison",
    displayMode: "canvas",
    canvasType: "comparison_canvas",
    inlineType: null,
    exampleQuestions: ["Which is safer Verna or Slavia?"],
    dataSources: ["vehicle_features", "vehicles"],
    regexes: [/\b(compare|comparison|vs|versus|which is safer)\b.*\b(safety|safe|ncap|airbags?|adas|esc|tpms)\b|\b(safety|safe|ncap|airbags?|adas|esc|tpms)\b.*\b(compare|comparison|vs|versus|which is safer)\b/i],
    priority: 28,
    toolIntent: "vehicle_comparison",
  }),

  vehicle_mileage_search: createConfig({
    intent: "vehicle_mileage_search",
    displayMode: "canvas",
    canvasType: "recommendation_results_canvas",
    inlineType: null,
    exampleQuestions: ["Best mileage cars under 10 lakh"],
    dataSources: ["vehicle_features", "vehicles"],
    regexes: [REGEX.mileage],
    priority: 27,
    toolIntent: "vehicle_performance_mileage_search",
  }),

  vehicle_running_cost: createConfig({
    intent: "vehicle_running_cost",
    displayMode: "canvas",
    canvasType: "tco_canvas",
    inlineType: null,
    exampleQuestions: ["Running cost of Creta for 1000 km"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.mileage],
    priority: 26,
    toolIntent: "vehicle_performance_mileage_search",
  }),

  vehicle_fuel_decision_advisor: createConfig({
    intent: "vehicle_fuel_decision_advisor",
    displayMode: "canvas",
    canvasType: "fuel_decision_canvas",
    inlineType: null,
    exampleQuestions: ["Should I buy petrol or diesel?"],
    leadingQuestions: [
      "Monthly running in km?",
      "City/highway split?",
      "Ownership period?",
      "Lower running cost or lower upfront price?",
      "Boot space priority?",
    ],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.fuelDecision],
    priority: 25,
    toolIntent: "vehicle_performance_mileage_search",
  }),

  vehicle_colors: createConfig({
    intent: "vehicle_colors",
    displayMode: "canvas",
    canvasType: "color_studio_canvas",
    inlineType: null,
    exampleQuestions: ["Show colors of Verna"],
    defaultActions: [
      { label: "Color by variant", type: "ask", query: "Show color availability by variant" },
      { label: "Show price", type: "open_canvas", canvasType: "pricelist_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Book test drive", type: "lead", leadType: "test_drive" },
    ],
    dataSources: ["vehicle_colors", "vehicles"],
    regexes: [REGEX.colors],
    priority: 24,
    toolIntent: "vehicle_colors",
  }),

  vehicle_color_gallery: createConfig({
    intent: "vehicle_color_gallery",
    displayMode: "canvas",
    canvasType: "color_studio_canvas",
    inlineType: null,
    exampleQuestions: ["Show Verna in black"],
    dataSources: ["vehicle_colors", "vehicles"],
    regexes: [REGEX.colors],
    priority: 23,
    toolIntent: "vehicle_color_search",
  }),

  vehicle_comparison: createConfig({
    intent: "vehicle_comparison",
    displayMode: "both",
    canvasType: "comparison_canvas",
    inlineType: null,
    exampleQuestions: ["Compare Verna and City"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.comparison],
    priority: 22,
    toolIntent: "vehicle_comparison",
  }),

  vehicle_model_comparison: createConfig({
    intent: "vehicle_model_comparison",
    displayMode: "canvas",
    canvasType: "comparison_canvas",
    inlineType: null,
    exampleQuestions: ["Which is better Verna or Slavia?"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [],
    priority: 21,
    toolIntent: "vehicle_comparison",
  }),

  vehicle_variant_comparison: createConfig({
    intent: "vehicle_variant_comparison",
    displayMode: "canvas",
    canvasType: "comparison_canvas",
    inlineType: null,
    exampleQuestions: ["Compare Verna SX and SX(O)"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [/\b(compare|difference between|vs|versus)\b.*\b(sx|vx|zx|htx|gtx|opt|plus|turbo|dct|ivt|mt|at|cvt)\b/i],
    priority: 20,
    toolIntent: "vehicle_variant_difference",
  }),

  vehicle_similar_cars: createConfig({
    intent: "vehicle_similar_cars",
    displayMode: "canvas",
    canvasType: "similar_cars_canvas",
    inlineType: null,
    exampleQuestions: ["Cars similar to Verna"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.similar],
    priority: 19,
    toolIntent: "similar_cars",
  }),

  vehicle_alternative_search: createConfig({
    intent: "vehicle_alternative_search",
    displayMode: "canvas",
    canvasType: "similar_cars_canvas",
    inlineType: null,
    exampleQuestions: ["Alternatives to Creta"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.similar],
    priority: 18,
    toolIntent: "similar_cars",
  }),

  vehicle_variant_recommendation: createConfig({
    intent: "vehicle_variant_recommendation",
    displayMode: "canvas",
    canvasType: "variant_finder_canvas",
    inlineType: null,
    exampleQuestions: ["Which Verna variant should I buy?"],
    defaultActions: [
      { label: "Compare with top model", type: "open_canvas", canvasType: "comparison_canvas" },
      { label: "Feature difference", type: "open_canvas", canvasType: "variant_upgrade_value_canvas" },
      { label: "Price difference", type: "open_canvas", canvasType: "variant_upgrade_value_canvas" },
      { label: "EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.variantFinder],
    priority: 17,
    toolIntent: "vehicle_best_variant_recommendation",
  }),

  vehicle_variant_upgrade_value: createConfig({
    intent: "vehicle_variant_upgrade_value",
    displayMode: "canvas",
    canvasType: "variant_upgrade_value_canvas",
    inlineType: null,
    exampleQuestions: ["Is SX(O) worth paying extra over SX?"],
    defaultActions: [
      { label: "Calculate EMI difference", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Open full feature diff", type: "open_canvas", canvasType: "feature_explorer_canvas" },
      { label: "Get quote for better value", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Compare another variant", type: "ask", query: "Compare another variant" },
      { label: "Book test drive", type: "lead", leadType: "test_drive" },
    ],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.variantUpgrade],
    priority: 16,
    toolIntent: "vehicle_variant_difference",
  }),

  vehicle_variant_difference: createConfig({
    intent: "vehicle_variant_difference",
    displayMode: "canvas",
    canvasType: "variant_upgrade_value_canvas",
    inlineType: null,
    exampleQuestions: ["Difference between Verna SX and SX(O)"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.variantUpgrade],
    priority: 41,
    toolIntent: "vehicle_variant_difference",
  }),

  vehicle_emi_calculator: createConfig({
    intent: "vehicle_emi_calculator",
    displayMode: "both",
    canvasType: "emi_calculator_canvas",
    inlineType: null,
    exampleQuestions: ["EMI for Verna with 2 lakh down payment"],
    defaultActions: [
      { label: "Show ex-showroom EMI", type: "ask", query: "Show EMI on ex-showroom price" },
      { label: "Show on-road EMI", type: "ask", query: "Show EMI on on-road price" },
      { label: "Change down payment", type: "ask", query: "Change down payment" },
      { label: "Change tenure", type: "ask", query: "Change EMI tenure" },
      { label: "Finance guide", type: "open_canvas", canvasType: "finance_guide_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicles"],
    regexes: [REGEX.emi],
    priority: 14,
    toolIntent: "vehicle_emi_calculator",
  }),

  vehicle_emi_options: createConfig({
    intent: "vehicle_emi_options",
    displayMode: "canvas",
    canvasType: "emi_calculator_canvas",
    inlineType: null,
    exampleQuestions: ["Cars with EMI under 25000"],
    leadingQuestions: ["EMI-only or total monthly budget?", "Down payment budget?", "Preferred tenure?", "Fuel/body type preference?"],
    defaultActions: [
      { label: "Compare EMI", type: "open_canvas", canvasType: "comparison_canvas" },
      { label: "Open EMI calculator", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicles"],
    regexes: [REGEX.emi, /\bemi under\b/i],
    priority: 13,
    toolIntent: "vehicle_emi_budget_search",
  }),

  vehicle_monthly_budget_planner: createConfig({
    intent: "vehicle_monthly_budget_planner",
    displayMode: "canvas",
    canvasType: "monthly_budget_planner_canvas",
    inlineType: null,
    exampleQuestions: ["My monthly budget is 30000, which car can I buy?"],
    leadingQuestions: ["EMI only or EMI + running cost?", "Down payment?", "Tenure?", "ROI assumption?", "Safe or stretch budget?"],
    defaultActions: [
      { label: "Comfort plan", type: "ask", query: "Show comfort budget plan" },
      { label: "Stretch plan", type: "ask", query: "Show stretch budget plan" },
      { label: "Exact EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Finance eligibility", type: "open_canvas", canvasType: "finance_guide_canvas" },
    ],
    dataSources: ["vehicles"],
    regexes: [REGEX.budgetEmi],
    priority: 12,
    toolIntent: "vehicle_emi_budget_search",
  }),

  new_car_finance_faq: createConfig({
    intent: "new_car_finance_faq",
    displayMode: "inline",
    canvasType: null,
    inlineType: "finance_faq_card",
    exampleQuestions: ["What documents are needed for car loan?"],
    defaultActions: [
      { label: "Open finance guide", type: "open_canvas", canvasType: "finance_guide_canvas" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Documents required", type: "ask", query: "Documents required for car loan" },
      { label: "Request callback", type: "lead", leadType: "finance_callback" },
    ],
    dataSources: ["finance_faq"],
    regexes: [REGEX.financeFaq],
    priority: 9,
    toolIntent: "new_car_finance_faq",
  }),

  new_car_loan_enquiry: createConfig({
    intent: "new_car_loan_enquiry",
    displayMode: "canvas",
    canvasType: "finance_guide_canvas",
    inlineType: "finance_faq_card",
    exampleQuestions: ["Can I get 90% loan?"],
    dataSources: ["finance_faq"],
    regexes: [REGEX.financeFaq, REGEX.emi],
    priority: 8,
    toolIntent: "new_car_loan_enquiry",
  }),

  vehicle_offers: createConfig({
    intent: "vehicle_offers",
    displayMode: "canvas",
    canvasType: "offers_canvas",
    inlineType: null,
    exampleQuestions: ["Latest offers on Verna"],
    defaultActions: [
      { label: "Exchange eligibility", type: "ask", query: "Check exchange eligibility" },
      { label: "Corporate eligibility", type: "ask", query: "Check corporate eligibility" },
      { label: "Loyalty bonus", type: "ask", query: "Check loyalty bonus" },
      { label: "EMI after offer", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Final quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Request callback", type: "lead", leadType: "callback" },
    ],
    dataSources: ["offers", "vehicles"],
    regexes: [REGEX.offers],
    priority: 7,
    toolIntent: "vehicle_offers",
  }),

  vehicle_offer_lookup: createConfig({
    intent: "vehicle_offer_lookup",
    displayMode: "inline",
    canvasType: null,
    inlineType: "offer_summary_card",
    exampleQuestions: ["Any offer on Creta?"],
    dataSources: ["offers", "vehicles"],
    regexes: [REGEX.offers],
    priority: 6,
    toolIntent: "vehicle_offers",
  }),

  aci_new_car_quotation: createConfig({
    intent: "aci_new_car_quotation",
    displayMode: "canvas",
    canvasType: "aci_quotation_canvas",
    inlineType: null,
    exampleQuestions: ["Get quotation for Verna SX in Delhi"],
    defaultActions: [
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Download quote", type: "ask", query: "Download quotation" },
      { label: "WhatsApp quote", type: "lead", leadType: "quote_whatsapp" },
      { label: "Book test drive", type: "lead", leadType: "test_drive" },
      { label: "Request callback", type: "lead", leadType: "callback" },
    ],
    dataSources: ["vehicles", "quotation"],
    regexes: [REGEX.quotation],
    priority: 1,
    toolIntent: "aci_new_car_quotation",
  }),

  vehicle_availability: createConfig({
    intent: "vehicle_availability",
    displayMode: "canvas",
    canvasType: "availability_waiting_canvas",
    inlineType: "availability_answer_card",
    exampleQuestions: ["Is Verna available in Delhi?"],
    defaultActions: [
      { label: "Fastest variant", type: "ask", query: "Show fastest delivery variant" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Request callback", type: "lead", leadType: "callback" },
      { label: "Show alternatives", type: "open_canvas", canvasType: "similar_cars_canvas" },
    ],
    dataSources: ["vehicles", "availability"],
    regexes: [REGEX.availability],
    priority: 5,
    toolIntent: "vehicle_availability",
  }),

  vehicle_waiting_period: createConfig({
    intent: "vehicle_waiting_period",
    displayMode: "canvas",
    canvasType: "availability_waiting_canvas",
    inlineType: "availability_answer_card",
    exampleQuestions: ["Waiting period of Creta"],
    defaultActions: [
      { label: "Fastest variant", type: "ask", query: "Show fastest delivery variant" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicles", "availability"],
    regexes: [REGEX.availability],
    priority: 5,
    toolIntent: "vehicle_waiting_period",
  }),

  new_car_service_center_search: createConfig({
    intent: "new_car_service_center_search",
    displayMode: "canvas",
    canvasType: "service_center_canvas",
    inlineType: "service_center_answer_card",
    exampleQuestions: ["Nearest Hyundai service center in Delhi"],
    defaultActions: [
      { label: "Directions", type: "navigate", route: "/" },
      { label: "Call", type: "lead", leadType: "callback" },
      { label: "Book service", type: "lead", leadType: "service" },
      { label: "Service cost", type: "open_canvas", canvasType: "ownership_service_warranty_canvas" },
      { label: "Warranty", type: "open_canvas", canvasType: "ownership_service_warranty_canvas" },
    ],
    dataSources: ["service_centers"],
    regexes: [REGEX.serviceCenter],
    priority: 4,
    toolIntent: "new_car_service_center_search",
  }),

  new_car_ownership_guide: createConfig({
    intent: "new_car_ownership_guide",
    displayMode: "canvas",
    canvasType: "ownership_service_warranty_canvas",
    inlineType: null,
    exampleQuestions: ["Ownership cost of Verna"],
    dataSources: ["vehicles", "service_costs"],
    regexes: [REGEX.ownership],
    priority: 4,
    toolIntent: "new_car_ownership_guide",
  }),

  new_car_service_cost: createConfig({
    intent: "new_car_service_cost",
    displayMode: "canvas",
    canvasType: "ownership_service_warranty_canvas",
    inlineType: null,
    exampleQuestions: ["What is Verna service cost?"],
    defaultActions: [
      { label: "Service center", type: "open_canvas", canvasType: "service_center_canvas" },
      { label: "Book service", type: "lead", leadType: "service" },
      { label: "Compare maintenance", type: "ask", query: "Compare maintenance cost" },
      { label: "Extended warranty", type: "ask", query: "Show extended warranty details" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["service_costs"],
    regexes: [REGEX.ownership],
    priority: 4,
    toolIntent: "new_car_service_cost",
  }),

  new_car_warranty: createConfig({
    intent: "new_car_warranty",
    displayMode: "canvas",
    canvasType: "ownership_service_warranty_canvas",
    inlineType: null,
    exampleQuestions: ["Warranty of Verna"],
    dataSources: ["warranty"],
    regexes: [REGEX.ownership],
    priority: 4,
    toolIntent: "new_car_warranty",
  }),

  vehicle_tco_analysis: createConfig({
    intent: "vehicle_tco_analysis",
    displayMode: "canvas",
    canvasType: "tco_canvas",
    inlineType: null,
    exampleQuestions: ["Which car is cheapest to own for 5 years?"],
    leadingQuestions: ["Monthly running?", "City/highway split?", "Include EMI?", "3-year or 5-year horizon?", "Fuel price assumption?"],
    defaultActions: [
      { label: "Compare another car", type: "open_canvas", canvasType: "comparison_canvas" },
      { label: "Adjust assumptions", type: "ask", query: "Adjust TCO assumptions" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Book test drive", type: "lead", leadType: "test_drive" },
    ],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.tco],
    priority: 3,
    toolIntent: "vehicle_running_cost",
  }),

  vehicle_resale_value_analysis: createConfig({
    intent: "vehicle_resale_value_analysis",
    displayMode: "canvas",
    canvasType: "resale_value_canvas",
    inlineType: null,
    exampleQuestions: ["Which car has best resale value?"],
    leadingQuestions: ["Ownership period?", "Segment preference?", "Fuel/transmission?", "Resale vs features priority?"],
    defaultActions: [
      { label: "Best resale variants", type: "ask", query: "Show best resale variants" },
      { label: "Ownership cost", type: "open_canvas", canvasType: "tco_canvas" },
      { label: "Get quote", type: "open_canvas", canvasType: "aci_quotation_canvas" },
    ],
    dataSources: ["vehicles", "resale"],
    regexes: [REGEX.resale],
    priority: 3,
    toolIntent: "vehicle_resale_value_analysis",
  }),

  vehicle_lifestyle_fit_score: createConfig({
    intent: "vehicle_lifestyle_fit_score",
    displayMode: "canvas",
    canvasType: "lifestyle_fit_canvas",
    inlineType: null,
    exampleQuestions: ["Best car for my lifestyle"],
    leadingQuestions: ["Passengers?", "Monthly running?", "City/highway split?", "Comfort/mileage/safety/performance priority?", "Must-have features?"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.lifestyle],
    priority: 3,
    toolIntent: "vehicle_lifestyle_fit_score",
  }),

  vehicle_senior_friendly_recommendation: createConfig({
    intent: "vehicle_senior_friendly_recommendation",
    displayMode: "canvas",
    canvasType: "senior_friendly_advisor_canvas",
    inlineType: null,
    exampleQuestions: ["Best car for parents"],
    leadingQuestions: ["Driver or passenger?", "Need high seating?", "City/highway usage?", "Automatic required?", "Budget?"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.senior],
    priority: 3,
    toolIntent: "vehicle_senior_friendly_recommendation",
  }),

  vehicle_space_practicality_advisor: createConfig({
    intent: "vehicle_space_practicality_advisor",
    displayMode: "canvas",
    canvasType: "space_practicality_canvas",
    inlineType: null,
    exampleQuestions: ["Most spacious cars under 20 lakh"],
    leadingQuestions: ["How many people?", "Boot space priority?", "Need 7 seats?", "Rear comfort priority?", "Budget?"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.space],
    priority: 3,
    toolIntent: "vehicle_space_practicality_advisor",
  }),

  vehicle_performance_advisor: createConfig({
    intent: "vehicle_performance_advisor",
    displayMode: "canvas",
    canvasType: "performance_spec_ranking_canvas",
    inlineType: null,
    exampleQuestions: ["Best performance car under 20 lakh"],
    leadingQuestions: ["Fuel preference?", "Transmission preference?", "Mileage tradeoff acceptable?", "City/highway split?", "Budget?"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.performance],
    priority: 3,
    toolIntent: "vehicle_performance_advisor",
  }),

  vehicle_spec_ranking: createConfig({
    intent: "vehicle_spec_ranking",
    displayMode: "canvas",
    canvasType: "performance_spec_ranking_canvas",
    inlineType: null,
    exampleQuestions: ["Cars with highest ground clearance"],
    leadingQuestions: ["Which spec matters most?", "Budget range?", "Preferred body type?", "City or mixed roads?", "Automatic required?"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.specRanking],
    priority: 3,
    toolIntent: "vehicle_spec_ranking",
  }),

  vehicle_bad_roads_advisor: createConfig({
    intent: "vehicle_bad_roads_advisor",
    displayMode: "canvas",
    canvasType: "performance_spec_ranking_canvas",
    inlineType: null,
    exampleQuestions: ["Best car for bad roads"],
    leadingQuestions: ["Ground clearance target?", "Body type preference?", "Budget?", "City or rural roads?", "Need automatic?"],
    dataSources: ["vehicles", "vehicle_features"],
    regexes: [REGEX.specRanking],
    priority: 3,
    toolIntent: "vehicle_bad_roads_advisor",
  }),

  new_car_unavailable_or_out_of_scope: createConfig({
    intent: "new_car_unavailable_or_out_of_scope",
    displayMode: "inline",
    canvasType: null,
    inlineType: "fallback_card",
    exampleQuestions: ["Sell my used car", "Insurance renewal"],
    defaultActions: [
      { label: "New car prices", type: "ask", query: "Show new car prices" },
      { label: "Compare cars", type: "ask", query: "Compare cars" },
      { label: "Calculate EMI", type: "open_canvas", canvasType: "emi_calculator_canvas" },
      { label: "Get quotation", type: "open_canvas", canvasType: "aci_quotation_canvas" },
      { label: "Find service center", type: "open_canvas", canvasType: "service_center_canvas" },
    ],
    dataSources: [],
    regexes: [REGEX.outOfScope],
    priority: 1000,
    toolIntent: "new_car_unavailable_or_out_of_scope",
  }),
};

export const NEW_CAR_INTENTS = Object.keys(NEW_CAR_QUESTION_MAP);

export const LEGACY_TO_CANONICAL_INTENT = {
  vehicle_pricelist: "vehicle_pricelist",
  vehicle_city_change: "vehicle_city_price",
  vehicle_price_breakup: "vehicle_price_breakup",
  vehicle_colors: "vehicle_colors",
  vehicle_color_search: "vehicle_color_gallery",
  vehicle_features: "vehicle_model_features_explorer",
  vehicle_feature_answer: "vehicle_feature_answer",
  vehicle_feature_discovery: "vehicle_feature_discovery",
  similar_cars: "vehicle_similar_cars",
  vehicle_comparison: "vehicle_comparison",
  vehicle_budget_search: "vehicle_budget_search",
  vehicle_use_case_recommendation: "vehicle_recommendation_discovery",
  vehicle_body_type_search: "vehicle_body_type_search",
  vehicle_fuel_transmission_search: "vehicle_use_case_search",
  vehicle_dimension_space_search: "vehicle_spec_ranking",
  vehicle_performance_mileage_search: "vehicle_performance_advisor",
  vehicle_safety_expert: "vehicle_safety_search",
  vehicle_emi_calculator: "vehicle_emi_calculator",
  vehicle_emi_budget_search: "vehicle_emi_options",
  vehicle_best_variant_recommendation: "vehicle_variant_recommendation",
  vehicle_variant_difference: "vehicle_variant_upgrade_value",
};

export const WIDGET_TO_CANONICAL_INTENT = {
  model_ambiguity: "vehicle_model_ambiguity",
  vehicle_model_ambiguity: "vehicle_model_ambiguity",
  variant_ambiguity: "vehicle_variant_ambiguity",
  vehicle_variant_ambiguity: "vehicle_variant_ambiguity",
  vehicle_pricelist: "vehicle_pricelist",
  vehicle_price_breakup: "vehicle_price_breakup",
  vehicle_feature_answer: "vehicle_feature_answer",
  vehicle_features: "vehicle_model_features_explorer",
  vehicle_feature_discovery: "vehicle_feature_discovery",
  vehicle_colors: "vehicle_colors",
  vehicle_color_search: "vehicle_color_gallery",
  vehicle_model_comparison: "vehicle_comparison",
  similar_cars: "vehicle_similar_cars",
  vehicle_variant_recommendation: "vehicle_variant_recommendation",
  vehicle_variant_difference: "vehicle_variant_upgrade_value",
  vehicle_emi_calculator: "vehicle_emi_calculator",
  vehicle_emi_recommendations: "vehicle_emi_options",
  vehicle_safety_results: "vehicle_safety_search",
  vehicle_recommendation_results: "vehicle_recommendation_discovery",
  vehicle_spec_ranking: "vehicle_spec_ranking",
};

export const mapIntentAlias = (intent = "") => {
  const clean = String(intent || "").trim();
  if (!clean) return "";
  if (NEW_CAR_QUESTION_MAP[clean]) return clean;
  return LEGACY_TO_CANONICAL_INTENT[clean] || clean;
};

const priorityFor = (intent = "") => {
  const fromOrder = PRIORITY_ORDER.indexOf(intent);
  if (fromOrder >= 0) return fromOrder;
  return NEW_CAR_QUESTION_MAP[intent]?.priority ?? 999;
};

export const sortIntentsByPriority = (intents = []) =>
  [...new Set(intents.filter(Boolean))].sort(
    (a, b) => priorityFor(a) - priorityFor(b),
  );

export const detectNewCarIntentCandidates = (message = "") => {
  const text = normalizeText(message);
  const lower = text.toLowerCase();
  if (!lower) return [];

  const matches = NEW_CAR_INTENTS.filter((intent) => {
    const def = NEW_CAR_QUESTION_MAP[intent];
    if (!def?.regexes?.length) return false;
    return def.regexes.some((regex) => regex?.test?.(text));
  });

  if (!matches.length) return [];

  if (REGEX.outOfScope.test(text)) {
    return ["new_car_unavailable_or_out_of_scope"];
  }

  return sortIntentsByPriority(matches);
};

export const pickPrimaryIntent = (intents = [], fallback = "", message = "") => {
  const sorted = sortIntentsByPriority(intents);
  const text = normalizeText(message).toLowerCase();
  if (!sorted.length) return fallback;

  const has = (intent) => sorted.includes(intent);
  const hasAny = (list = []) => list.some((intent) => has(intent));

  // Explicit multi-intent and disambiguation heuristics.
  if (has("aci_new_car_quotation")) return "aci_new_car_quotation";
  if (
    /\bprice\b.*\bin\s+[a-z]/i.test(text) &&
    has("vehicle_city_price")
  ) {
    return "vehicle_city_price";
  }
  if (
    /\b(sx|vx|zx|htx|gtx|opt|plus|turbo|dct|ivt|mt|at|cvt)\b.*\bprice\b/i.test(text) &&
    has("vehicle_variant_price")
  ) {
    return "vehicle_variant_price";
  }
  if (
    /\bdifference between\b.*\b(sx|vx|zx|htx|gtx|opt|plus|turbo|dct|ivt|mt|at|cvt)\b/i.test(
      text,
    ) &&
    hasAny(["vehicle_variant_upgrade_value", "vehicle_variant_comparison"])
  ) {
    return has("vehicle_variant_upgrade_value")
      ? "vehicle_variant_upgrade_value"
      : "vehicle_variant_comparison";
  }
  if (/\b(compare|vs|versus|which is better)\b/i.test(text) && has("vehicle_comparison")) {
    return "vehicle_comparison";
  }
  if (
    /\bwhich\b.*\bvariants?\b.*\b(have|with)\b/i.test(text) &&
    has("vehicle_feature_discovery")
  ) {
    return "vehicle_feature_discovery";
  }
  if (
    /\b(documents?|interest rate|processing fee|cibil|eligibility|prepay|foreclosure|min(?:imum)? down payment|max(?:imum)? tenure)\b/i.test(
      text,
    ) &&
    has("new_car_finance_faq")
  ) {
    return "new_car_finance_faq";
  }
  if (
    /\bpetrol\s+or\s+diesel|diesel\s+or\s+petrol|cng\s+or\s+petrol|ev\s+or\s+petrol|petrol\s+vs\s+diesel\b/i.test(
      text,
    ) &&
    has("vehicle_fuel_decision_advisor")
  ) {
    return "vehicle_fuel_decision_advisor";
  }
  if (
    /\bunder\b.*\b(\d+\s*l|lakh|lac|cr|crore)\b/i.test(text) &&
    has("vehicle_budget_search")
  ) {
    if (has("vehicle_must_have_feature_builder")) return "vehicle_must_have_feature_builder";
    if (has("vehicle_safety_search") && /\bsaf(est|ety)|ncap|airbags?|adas\b/i.test(text)) {
      return "vehicle_safety_search";
    }
    if (has("vehicle_performance_advisor") && /\bperformance|fastest|powerful|turbo\b/i.test(text)) {
      return "vehicle_performance_advisor";
    }
    if (has("vehicle_space_practicality_advisor") && /\bspacious|boot|7[\s-]?seater|rear seat|luggage\b/i.test(text)) {
      return "vehicle_space_practicality_advisor";
    }
    return "vehicle_budget_search";
  }

  if (
    /\bprice|pricelist|on[-\s]?road|ex[-\s]?showroom\b/i.test(text) &&
    has("vehicle_pricelist")
  ) {
    return "vehicle_pricelist";
  }

  if (
    /\bemi|down payment|loan\b/i.test(text) &&
    has("vehicle_emi_calculator")
  ) {
    return "vehicle_emi_calculator";
  }

  if (
    /\bservice center|workshop|authorized.*service|nearest.*service\b/i.test(text) &&
    has("new_car_service_center_search")
  ) {
    return "new_car_service_center_search";
  }

  if (hasAny(["vehicle_model_ambiguity", "vehicle_variant_ambiguity"])) {
    const intent =
      has("vehicle_model_ambiguity") ? "vehicle_model_ambiguity" : "vehicle_variant_ambiguity";
    if (!hasAny(["vehicle_fuel_decision_advisor", "vehicle_tco_analysis"])) return intent;
  }

  return sorted[0] || fallback;
};

export const getNewCarQuestionConfig = (intent = "") =>
  NEW_CAR_QUESTION_MAP[mapIntentAlias(intent)] || null;

export const getIntentForWidgetType = (widgetType = "") =>
  WIDGET_TO_CANONICAL_INTENT[String(widgetType || "").trim()] || "";

export const resolveToolIntentForQuestionIntent = (intent = "") => {
  const config = getNewCarQuestionConfig(intent);
  return config?.toolIntent || intent;
};
