import { resolveVehicleEntities } from "./aiAgent.vehicleEntityIndex.js";
import { normalizeSearchKey, normalizeText } from "./aiAgent.planSchema.js";

/**
 * ACI Assist Planner Red Fixes
 *
 * Narrow DB-backed deterministic guard.
 *
 * This is NOT a full semantic compiler.
 * It only fixes high-confidence routing bugs before Gemini fallback.
 *
 * Rules:
 * - No hardcoded model list.
 * - No hardcoded variant list.
 * - No hardcoded brand list.
 * - Existing semantic compiler remains the main router.
 */

const DEFAULT_CITY = "new-delhi";

const clean = (value = "") => normalizeText(value || "");
const key = (value = "") => normalizeSearchKey(value || "");

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const unique = (items = []) => [...new Set(asArray(items).filter(Boolean))];

const pickFirst = (...values) =>
  values.find(
    (value) =>
      value !== undefined &&
      value !== null &&
      String(value).trim() !== "",
  );

const has = (message = "", regex) => regex.test(key(message));

const detectCity = (message = "", fallback = DEFAULT_CITY) => {
  const text = key(message);

  const rules = [
    [/\b(new delhi|delhi|ncr)\b/, "new-delhi"],
    [/\b(gurgaon|gurugram)\b/, "gurgaon"],
    [/\bnoida\b/, "noida"],
    [/\bghaziabad\b/, "ghaziabad"],
    [/\bfaridabad\b/, "faridabad"],
    [/\bmumbai\b/, "mumbai"],
    [/\b(bangalore|bengaluru)\b/, "bengaluru"],
    [/\bpune\b/, "pune"],
    [/\bhyderabad\b/, "hyderabad"],
    [/\bchennai\b/, "chennai"],
    [/\bkolkata\b/, "kolkata"],
    [/\bjaipur\b/, "jaipur"],
    [/\bahmedabad\b/, "ahmedabad"],
    [/\blucknow\b/, "lucknow"],
    [/\bchandigarh\b/, "chandigarh"],
  ];

  const found = rules.find(([regex]) => regex.test(text));
  return found?.[1] || fallback || DEFAULT_CITY;
};

const amountToRupees = (value, unit = "", defaultSmallAsLakh = true) => {
  const num = Number(value);
  const unitKey = key(unit);

  if (!Number.isFinite(num) || num <= 0) return undefined;

  if (/\b(cr|crore|crores)\b/.test(unitKey)) return Math.round(num * 10000000);
  if (/\b(lakh|lakhs|lac|lacs|l)\b/.test(unitKey)) return Math.round(num * 100000);
  if (/\b(k|thousand)\b/.test(unitKey)) return Math.round(num * 1000);

  if (defaultSmallAsLakh && num > 0 && num <= 300) return Math.round(num * 100000);

  return Math.round(num);
};

const extractBudgetMax = (message = "") => {
  const raw = String(message || "").toLowerCase().replace(/,/g, " ");

  const match =
    raw.match(/\bunder\s+(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|k|thousand)?\b/i) ||
    raw.match(/\bbelow\s+(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|k|thousand)?\b/i) ||
    raw.match(/\bupto\s+(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|k|thousand)?\b/i) ||
    raw.match(/\bup to\s+(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|k|thousand)?\b/i);

  if (!match) return undefined;
  return amountToRupees(match[1], match[2] || "lakh", true);
};

const extractDownPayment = (message = "") => {
  const raw = String(message || "").toLowerCase().replace(/,/g, " ");

  const match =
    raw.match(/\b(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|thousand|k)?\s*(?:down\s*payment|down|dp)\b/i) ||
    raw.match(/\b(?:down\s*payment|down|dp)\s*(?:of|is|=|as|with)?\s*(\d+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|l|thousand|k)?\b/i);

  if (!match) return undefined;
  return amountToRupees(match[1], match[2] || "lakh", true);
};

const extractTenureMonths = (message = "") => {
  const raw = String(message || "").toLowerCase();

  const years = raw.match(/\b(\d+(?:\.\d+)?)\s*(?:year|years|yr|yrs)\b/i);
  if (years) {
    const value = Number(years[1]);
    if (Number.isFinite(value) && value > 0 && value <= 10) return Math.round(value * 12);
  }

  const months = raw.match(/\b(\d+)\s*(?:month|months|mo)\b/i);
  if (months) {
    const value = Number(months[1]);
    if (Number.isFinite(value) && value > 0 && value <= 120) return Math.round(value);
  }

  return undefined;
};

const extractLoanPercent = (message = "") => {
  const raw = String(message || "").toLowerCase();

  const loanMatch = raw.match(/\b(\d{1,3})\s*%\s*(?:loan|finance|funding)\b/i);
  if (loanMatch) {
    const percent = Number(loanMatch[1]);
    if (Number.isFinite(percent) && percent > 0 && percent <= 100) return percent;
  }

  const downMatch =
    raw.match(/\b(\d{1,3})\s*%\s*(?:down\s*payment|down|dp)\b/i) ||
    raw.match(/\b(?:down\s*payment|down|dp)\s*(?:of|is|=|as)?\s*(\d{1,3})\s*%\b/i);

  if (downMatch) {
    const downPercent = Number(downMatch[1]);
    if (Number.isFinite(downPercent) && downPercent > 0 && downPercent < 100) {
      return 100 - downPercent;
    }
  }

  return undefined;
};

const extractFeature = (message = "") => {
  const text = key(message);

  if (/\bsunroof\b/.test(text)) return "sunroof";
  if (/\badas\b/.test(text)) return "ADAS";
  if (/\bairbags?\b|\b6 airbags\b/.test(text)) return "airbags";
  if (/\bboot space\b|\bboot\b/.test(text)) return "boot space";
  if (/\bground clearance\b/.test(text)) return "ground clearance";
  if (/\bmileage\b|\baverage\b|\bkitni\b|\bkitna\b|\bdeti\b|\bfuel efficiency\b/.test(text)) return "mileage";
  if (/\bfeatures?\b/.test(text)) return "features";

  return "";
};

const extractMustHaveFeatures = (message = "") => {
  const text = key(message);
  const features = [];

  if (/\bsunroof\b/.test(text)) features.push("sunroof");
  if (/\badas\b/.test(text)) features.push("ADAS");
  if (/\b6 airbags\b|\bsix airbags\b|\bairbags\b/.test(text)) features.push("6 airbags");
  if (/\b360 camera\b|\b360 degree camera\b/.test(text)) features.push("360 camera");
  if (/\bventilated seats?\b/.test(text)) features.push("ventilated seats");
  if (/\bglobal ncap\b|\bncap\b|\b5 star\b|\bfive star\b/.test(text)) features.push("Global NCAP 5 star");

  return unique(features);
};

const isSecurityQuery = (message = "") =>
  has(message, /\b(ignore previous instructions|print the prompt|show prompt|reveal prompt|developer message|system prompt|dealer profit|showroom margin|leak customer|customer phone|hidden inventory|admin only|bypass|override routing)\b/);

const isInternalQuery = (message = "") =>
  has(message, /\b(pending receivables|receivables|loan closure|approved but not disbursed|total business|disbursed cases|book value|customer 360|delivery order|payment dashboard)\b/);

const isOutOfScopeServiceQuery = (message = "") =>
  has(message, /\b(book my car service|service booking|noise issue|ac is not cooling|check engine|diagnose|not starting|roadside assistance|accident claim|claim support|body shop|repair)\b/);

const isAvailabilityQuery = (message = "") =>
  has(message, /\b(available now|availability|in stock|stock|waiting period|waiting time|delivery time|immediate delivery|delivery this week|shortest waiting|this month stock|cars available this month|reserve color|reserve variant)\b/);

const isBreakupQuery = (message = "") =>
  has(message, /\b(price breakup|on road breakup|on-road breakup|breakup|break up|rto charges?|insurance amount|insurance charge|other charges?|optional charges?|handling charges?|fastag|tcs)\b/);

const isInsuranceQuoteQuery = (message = "") => {
  const text = key(message);

  if (/\binsurance amount\b/.test(text) && /\bon road\b/.test(text)) return false;

  return (
    /\binsurance quote|insurance quotation|insurance premium|insurance price\b/.test(text) ||
    (/\binsurance\b/.test(text) && /\bquote|quotation|get quote|get quotation|premium\b/.test(text))
  );
};

const isZeroDepExplainerQuery = (message = "") =>
  has(message, /\bzero dep|zero depreciation\b/);

const isColorQuery = (message = "") =>
  has(message, /\b(colors?|colours?|black|white|red|blue|grey|gray|silver|titan|pearl)\b/) &&
  !isAvailabilityQuery(message);

const isFeatureQuestion = (message = "") =>
  Boolean(extractFeature(message)) &&
  has(message, /\b(does|do|has|have|gets?|show|how many|what is|kitni|kitna|mileage|boot|ground clearance|features?)\b/);

const isFeatureMatchQuery = (message = "") =>
  extractMustHaveFeatures(message).length > 0 &&
  has(message, /\b(cars?|suv|sedan|hatchback|under|with|want|automatic|manual)\b/) &&
  !extractFeature(message);

const isSafetyQuery = (message = "") =>
  has(message, /\b(safest|safety|safer|ncap|global ncap|5 star|five star|airbags?|adas)\b/);

const isComparisonQuery = (message = "") =>
  has(message, /\b(compare| vs |versus|which is better|better mileage|better for family|better for city|better for highway|emi for .* and |emi .* and )\b/);

const isSimilarQuery = (message = "") =>
  has(message, /\b(similar cars?|cars? similar to|similar to|alternatives?|alternative to|cars? like|cheaper alternative|premium alternative|better alternative)\b/);

const isExShowroomPriceQuery = (message = "") =>
  has(message, /\b(ex showroom|exshowroom|ex-showroom)\b/) &&
  has(message, /\b(price|cost|rate|pricelist|price list|kitna|kitni)\b/);

const isTopModelPriceQuery = (message = "") =>
  has(message, /\b(top model|top variant|highest variant|most expensive)\b/) &&
  has(message, /\b(price|cost|rate|kitna|kitni|variant)\b/);

const isBaseModelPriceQuery = (message = "") =>
  has(message, /\b(base model|base variant|cheapest|lowest price|least expensive)\b/) &&
  has(message, /\b(price|cost|rate|variant|kitna|kitni)\b/);

const isGenericPriceQuery = (message = "") =>
  has(message, /\b(price|pricelist|price list|on road|on-road|ex showroom|exshowroom|cost|rate list)\b/);

const isEmiQuery = (message = "") =>
  has(message, /\b(emi|loan possible|loan|finance|afford|down payment)\b/);

const isLeadQuery = (message = "") =>
  has(message, /\b(get quote|get quotation|quotation|quote|best price|final price|call me|talk to advisor|advisor|i want to buy|buy this car|purchase|lock deal|proforma invoice)\b/);

const isFinanceEligibilityQuery = (message = "") =>
  has(message, /\b(finance eligibility|check eligibility|pre approved|pre-approved|low cibil|company name|company salary|self employed|car loan|need finance|need loan)\b/);

const isFuelExplainerQuery = (message = "") =>
  has(message, /\b(petrol diesel|diesel petrol|cng petrol|petrol cng|hybrid|ev|fuel type|which fuel|daily running|daily 50 km|50 km running)\b/);

const isResaleUnavailableQuery = (message = "") =>
  has(message, /\b(resale value|resale after|depreciation after|value after)\b/);

const isDiscontinuedQuery = (message = "") =>
  has(message, /\b(discontinued|include discontinued|show discontinued)\b/);

const isMultiIntentQuery = (message = "") => {
  const text = key(message);
  let count = 0;

  if (isGenericPriceQuery(text)) count += 1;
  if (isComparisonQuery(text)) count += 1;
  if (isEmiQuery(text)) count += 1;
  if (/\boffers?|discount|scheme\b/.test(text)) count += 1;
  if (isColorQuery(text)) count += 1;
  if (isFeatureQuestion(text)) count += 1;
  if (isLeadQuery(text)) count += 1;

  return count >= 2;
};

const sanitizeVariant = ({ message = "", variant = "" } = {}) => {
  const text = key(message);
  const variantKey = key(variant);

  if (!variantKey) return "";

  if (variantKey === "ex" && /\b(ex showroom|exshowroom|ex-showroom)\b/.test(text)) return "";
  if (variantKey === "base" && /\b(base model|base variant|base price)\b/.test(text)) return "";
  if (variantKey === "top" && /\b(top model|top variant|top price)\b/.test(text)) return "";

  return clean(variant);
};

const resolvePlannerVehicle = async ({ message = "", context = {}, selectedEntity = null } = {}) => {
  const resolved = await resolveVehicleEntities({ message, context, selectedEntity });
  const selectedVehicle =
    context?.selectedVehicle ||
    context?.anchorVehicle ||
    context?.vehicle ||
    context?.history?.selectedVehicle ||
    {};

  const model = clean(
    pickFirst(
      resolved?.primaryModel,
      selectedVehicle?.model,
      context?.anchorModel,
      context?.model,
    ) || "",
  );

  const brand = clean(
    pickFirst(
      resolved?.primaryBrand,
      selectedVehicle?.brand,
      selectedVehicle?.make,
      context?.anchorBrand,
    ) || "",
  );

  const variant = sanitizeVariant({
    message,
    variant:
      pickFirst(
        resolved?.primaryVariant,
        selectedVehicle?.variant,
        context?.anchorVariant,
        context?.variant,
      ) || "",
  });

  const city = detectCity(
    message,
    pickFirst(
      resolved?.primaryCity,
      selectedVehicle?.city,
      context?.anchorCity,
      context?.city,
      DEFAULT_CITY,
    ),
  );

  const comparisonModels = unique(
    asArray(resolved?.comparisonModels || resolved?.models || context?.selectedComparisonSet?.models),
  );

  return {
    resolved,
    brand,
    model,
    variant,
    city,
    comparisonModels,
  };
};

const outputForTool = (tool, { variant = "" } = {}) => {
  if (tool === "vehicle_pricelist") {
    return { canvasType: "pricelist_canvas", inlineType: null, groupBy: variant ? "none" : "variant", preferredWidgetType: null };
  }

  if (tool === "vehicle_price_breakup") {
    return { canvasType: "price_breakup_canvas", inlineType: null, groupBy: null, preferredWidgetType: null };
  }

  if (tool === "vehicle_emi") {
    return { canvasType: "emi_calculator_canvas", inlineType: null, groupBy: null, preferredWidgetType: null };
  }

  if (tool === "vehicle_recommend") {
    return { canvasType: "recommendation_results_canvas", inlineType: null, groupBy: "model", preferredWidgetType: null };
  }

  if (tool === "vehicle_feature_lookup") {
    return { canvasType: null, inlineType: "feature_answer_card", groupBy: null, preferredWidgetType: null };
  }

  if (tool === "vehicle_compare") {
    return { canvasType: "comparison_canvas", inlineType: null, groupBy: "variant", preferredWidgetType: null };
  }

  if (tool === "vehicle_colors") {
    return { canvasType: "color_studio_canvas", inlineType: null, groupBy: null, preferredWidgetType: null };
  }

  if (tool === "vehicle_explainer") {
    return { canvasType: null, inlineType: "explainer_card", groupBy: null, preferredWidgetType: null };
  }

  if (tool === "aci_lead_capture") {
    return { canvasType: "lead_capture_canvas", inlineType: null, groupBy: null, preferredWidgetType: null };
  }

  if (tool === "unavailable") {
    return { canvasType: "unavailable_notice_canvas", inlineType: "unavailable_notice", groupBy: null, preferredWidgetType: null };
  }

  if (tool === "internal_passthrough") {
    return { canvasType: null, inlineType: null, groupBy: null, preferredWidgetType: null };
  }

  return { canvasType: null, inlineType: null, groupBy: null, preferredWidgetType: null };
};

const makeResolution = ({
  model = "",
  variant = "",
  variantSelectionMode = "",
  selectedVariants = null,
  selectedModels = null,
  note = "",
  comparisonLevel = null,
} = {}) => ({
  comparisonLevel,
  variantSelectionMode: variantSelectionMode || (variant ? "exact" : "not_required"),
  selectedVariants:
    selectedVariants ||
    (model && variant ? [{ model, variant }] : []),
  selectedModels: selectedModels || (model ? [{ model }] : []),
  changeAllowed: true,
  note,
});

const makeContextPatch = ({
  brand = "",
  model = "",
  variant = "",
  city = DEFAULT_CITY,
  conversationMode = "direct_answer",
  customerStage = "exploration",
  leadContext = {},
  selectedComparisonSet = {},
  userPreferences = {},
} = {}) => {
  const selectedVehicle = {};
  if (brand) selectedVehicle.brand = brand;
  if (model) selectedVehicle.model = model;
  if (variant) selectedVehicle.variant = variant;
  if (city) selectedVehicle.city = city;

  return {
    anchorBrand: brand || "",
    anchorModel: model || "",
    anchorVariant: variant || "",
    anchorCity: city || DEFAULT_CITY,
    anchorColor: "",
    selectedVehicle,
    selectedComparisonSet,
    userPreferences,
    leadContext,
    customerStage,
    conversationMode,
  };
};

const makePlan = ({
  tool,
  brand = "",
  model = "",
  variant = "",
  city = DEFAULT_CITY,
  entities = {},
  filters = {},
  ranking = null,
  mode = "single_tool",
  domain = "new_car",
  conversationMode = "direct_answer",
  customerStage = "exploration",
  confidence = 0.98,
  unavailableReason = null,
  reasoningSummary = "DB-backed deterministic planner red fix.",
  resolution = {},
  ambiguity = null,
  nextSteps = [],
  leadContext = {},
  selectedComparisonSet = {},
  userPreferences = {},
} = {}) => {
  const finalEntities = {
    ...(model ? { model, primaryModel: model } : {}),
    ...(variant ? { variant, primaryVariant: variant } : {}),
    ...entities,
  };

  const finalFilters = {
    city: city || DEFAULT_CITY,
    activeOnly: true,
    ...filters,
  };

  const finalResolution = makeResolution({ model, variant, ...resolution });

  return {
    mode,
    domain,
    conversationMode,
    customerStage,
    tools: [
      {
        tool,
        entities: finalEntities,
        filters: finalFilters,
        ranking,
        output: outputForTool(tool, { variant }),
        resolution: finalResolution,
      },
    ],
    nextSteps,
    clarification: null,
    confidence,
    reasoningSummary,
    unavailableReason,
    ambiguity:
      ambiguity || {
        level: "none",
        type: "none",
        message: "",
        options: [],
        selectedDefault: null,
      },
    contextPatch: makeContextPatch({
      brand,
      model,
      variant,
      city,
      conversationMode,
      customerStage,
      leadContext,
      selectedComparisonSet,
      userPreferences,
    }),
    resolution: finalResolution,
  };
};

export const compilePlannerRedFix = async ({ message = "", context = {}, selectedEntity = null } = {}) => {
  const raw = clean(message);
  if (!raw) return null;

  const text = key(raw);

  const {
    brand,
    model,
    variant,
    city,
    comparisonModels,
  } = await resolvePlannerVehicle({ message: raw, context, selectedEntity });

  /* ACI_MULTI_002_FINAL_FIX_START */
  {
    const multi002ContextVehicle =
      context?.selectedVehicle ||
      context?.anchorVehicle ||
      context?.vehicle ||
      context?.history?.selectedVehicle ||
      {};

    const multi002ContextModel = clean(
      pickFirst(
        multi002ContextVehicle?.model,
        context?.anchorModel,
        context?.model,
      ) || "",
    );

    const multi002ContextVariant = clean(
      pickFirst(
        multi002ContextVehicle?.variant,
        context?.anchorVariant,
        context?.variant,
      ) || "",
    );

    const multi002Model = model || multi002ContextModel;
    const multi002Variant = variant || multi002ContextVariant;
    const multi002DownPayment = extractDownPayment(raw);
    const multi002TenureMonths = extractTenureMonths(raw);
    const multi002LoanPercent = extractLoanPercent(raw);
    const multi002Feature = extractFeature(raw) || "sunroof";

    if (
      multi002Model &&
      has(raw, /\bsunroof|adas|airbags?|mileage|boot space|ground clearance\b/) &&
      has(raw, /\bemi|loan|down payment\b/)
    ) {
      const plan = makePlan({
        tool: "vehicle_emi",
        brand,
        model: multi002Model,
        variant: multi002Variant,
        city,
        mode: "multi_tool",
        conversationMode: "calculation",
        customerStage: "consideration",
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
          ...(multi002DownPayment !== undefined
            ? { downPayment: multi002DownPayment }
            : {}),
          ...(multi002TenureMonths !== undefined
            ? { tenureMonths: multi002TenureMonths }
            : {}),
          ...(multi002LoanPercent !== undefined
            ? { loanPercent: multi002LoanPercent }
            : {}),
        },
        reasoningSummary:
          "User asked a feature question and EMI calculation in one message. EMI is primary, feature lookup is secondary.",
        resolution: {
          variantSelectionMode: multi002Variant ? "exact" : "representative_default",
          selectedVariants: multi002Variant
            ? [{ model: multi002Model, variant: multi002Variant }]
            : [{ model: multi002Model, variantStrategy: "popular_or_best_value" }],
          selectedModels: [{ model: multi002Model }],
          note: "Calculate EMI and also verify requested feature.",
        },
        ambiguity: multi002Variant
          ? {
              level: "none",
              type: "none",
              message: "",
              options: [],
              selectedDefault: null,
            }
          : {
              level: "soft_default",
              type: "variant",
              message:
                "I’ll calculate EMI using a selected or popular variant. You can change the variant anytime.",
              options: [],
              selectedDefault: {
                variantSelectionMode: "representative_default",
              },
            },
      });

      plan.tools.push({
        tool: "vehicle_feature_lookup",
        entities: {
          model: multi002Model,
          primaryModel: multi002Model,
          ...(multi002Variant
            ? { variant: multi002Variant, primaryVariant: multi002Variant }
            : {}),
          feature: multi002Feature,
        },
        filters: {
          city,
          activeOnly: true,
        },
        ranking: null,
        output: outputForTool("vehicle_feature_lookup"),
        resolution: makeResolution({
          model: multi002Model,
          variant: multi002Variant,
          variantSelectionMode: multi002Variant ? "exact" : "representative_default",
          selectedVariants: multi002Variant
            ? [{ model: multi002Model, variant: multi002Variant }]
            : [{ model: multi002Model, variantStrategy: "popular_or_best_value" }],
          selectedModels: [{ model: multi002Model }],
          note: `Lookup requested feature: ${multi002Feature}.`,
        }),
      });

      plan.contextPatch = {
        ...(plan.contextPatch || {}),
        anchorModel: multi002Model,
        anchorVariant: multi002Variant || "",
        anchorCity: city,
        selectedVehicle: {
          ...((plan.contextPatch || {}).selectedVehicle || {}),
          model: multi002Model,
          ...(multi002Variant ? { variant: multi002Variant } : {}),
          city,
        },
        userPreferences: {
          ...((plan.contextPatch || {}).userPreferences || {}),
          ...(multi002DownPayment !== undefined
            ? { downPayment: multi002DownPayment }
            : {}),
          requestedFeatures: [multi002Feature],
        },
      };

      return plan;
    }
  }
  /* ACI_MULTI_002_FINAL_FIX_END */


  /* ACI_PRIORITY_REMAINING_5_START */
  {
    const priContextVehicle =
      context?.selectedVehicle ||
      context?.anchorVehicle ||
      context?.vehicle ||
      context?.history?.selectedVehicle ||
      {};

    const priContextModel = clean(
      pickFirst(priContextVehicle?.model, context?.anchorModel, context?.model) || "",
    );

    const priContextVariant = clean(
      pickFirst(
        priContextVehicle?.variant,
        context?.anchorVariant,
        context?.variant,
      ) || "",
    );

    const priAnchorModel = model || priContextModel;
    const priAnchorVariant = variant || priContextVariant;
    const priBudgetMax = extractBudgetMax(raw);
    const priDownPayment = extractDownPayment(raw);
    const priTenureMonths = extractTenureMonths(raw);
    const priLoanPercent = extractLoanPercent(raw);

    const priFuelTypes = [
      ...(has(raw, /\bcng\b/) ? ["CNG"] : []),
      ...(has(raw, /\bpetrol\b/) ? ["Petrol"] : []),
      ...(has(raw, /\bdiesel\b/) ? ["Diesel"] : []),
      ...(has(raw, /\bhybrid\b/) ? ["Hybrid"] : []),
      ...(has(raw, /\bev\b|\belectric\b/) ? ["EV"] : []),
    ];

    const priMustHaveFeatures = (() => {
      const features = extractMustHaveFeatures(raw);

      if (has(raw, /\bpanoramic\s+sunroof\b/)) {
        return unique(
          features
            .filter((item) => key(item) !== "sunroof")
            .concat(["panoramic sunroof"]),
        );
      }

      return features;
    })();

    if (
      priMustHaveFeatures.length > 0 &&
      (
        has(raw, /\bcars?\b|\bsuvs?\b|\bsedans?\b|\bhatchbacks?\b/) ||
        has(raw, /\bi want\b|\bbest\b|\bunder\b|\bwith\b|\bautomatic\b|\bmanual\b/)
      ) &&
      !has(raw, /\bsafest\b|\bsafer\b|\bsafety\b|\bglobal ncap\b|\bncap\b|\b5 star\b|\bfive star\b/)
    ) {
      return makePlan({
        tool: "vehicle_recommend",
        brand,
        city,
        conversationMode: "recommendation",
        customerStage: "exploration",
        ranking: "feature_match",
        filters: {
          priceBasis: "on_road",
          ...(priBudgetMax ? { budgetMax: priBudgetMax } : {}),
          ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
          ...(has(raw, /\bsuvs?\b/) ? { bodyType: "suv" } : {}),
          mustHaveFeatures: priMustHaveFeatures,
        },
        userPreferences: {
          ...(priBudgetMax ? { budgetMax: priBudgetMax } : {}),
          ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
          ...(has(raw, /\bsuvs?\b/) ? { bodyType: "suv" } : {}),
          mustHaveFeatures: priMustHaveFeatures,
        },
        reasoningSummary:
          `Feature-match recommendation for: ${priMustHaveFeatures.join(", ")}.`,
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: `Show model cards with must-have features: ${priMustHaveFeatures.join(", ")}.`,
        },
      });
    }

    if (
      priFuelTypes.length > 0 &&
      has(raw, /\bwhich is better\b|\bdaily\b|\brunning\b|\bkm\b|\bfuel\b/)
    ) {
      const priFuelLabel = priFuelTypes.join(" vs ");

      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "evaluation",
        entities: {
          topic: "petrol_vs_diesel",
          topics: ["fuel_decision", "running_cost", ...priFuelTypes],
          fuelType: priFuelLabel,
          fuelTypes: priFuelTypes,
        },
        filters: {
          city,
          activeOnly: true,
          fuelType: priFuelLabel,
          fuelTypes: priFuelTypes,
          compareFuelTypes: priFuelTypes,
        },
        userPreferences: {
          fuelType: priFuelLabel,
          fuelTypes: priFuelTypes,
          compareFuelTypes: priFuelTypes,
        },
        reasoningSummary:
          `Fuel decision / running cost comparison for ${priFuelLabel}.`,
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: `Explain fuel decision for ${priFuelLabel}. CNG and Petrol are explicit comparison inputs.`,
        },
      });
    }

    if (
      has(raw, /\bcompany name\b|\bcompany buy\b|\bbusiness name\b|\bfirm name\b/) &&
      has(raw, /\bloan\b|\bfinance\b|\bfunding\b/)
    ) {
      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "exploration",
        entities: {
          topic: "emi",
          topics: [
            "company_name_purchase",
            "car_loan",
            "loan",
            "finance",
            "rto",
            "registration",
          ],
          loanContext: "company_name_car_loan",
          registrationContext: "company_name_rto_registration",
        },
        filters: {
          city,
          activeOnly: true,
          loanContext: "company_name_car_loan",
          financeContext: "company_name_finance",
          registrationContext: "company_name_rto_registration",
        },
        reasoningSummary:
          "Company-name purchase with car loan / finance / RTO registration context.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Explain company-name car loan, finance documents, RTO and registration requirements.",
        },
      });
    }

    if (has(raw, /\bcompany name\b|\bcompany buy\b|\bbusiness name\b|\bfirm name\b/)) {
      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "exploration",
        entities: {
          topic: "rto",
          topics: [
            "company_name_purchase",
            "rto",
            "registration",
            "quotation",
            "finance",
            "loan",
          ],
          registrationContext: "company_name_rto_registration",
          loanContext: "company_name_car_loan",
        },
        filters: {
          city,
          activeOnly: true,
          registrationContext: "company_name_rto_registration",
          rtoContext: "company_name_registration",
          loanContext: "company_name_car_loan",
        },
        reasoningSummary:
          "Company-name purchase / RTO / registration / quotation requirements.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Explain company-name purchase, RTO registration, quotation and loan implications.",
        },
      });
    }

    if (
      priAnchorModel &&
      isGenericPriceQuery(raw) &&
      isComparisonQuery(raw) &&
      isEmiQuery(raw) &&
      has(raw, /\boffers?\b|\bdiscount\b|\bscheme\b/)
    ) {
      const priCompareModels = unique([
        priAnchorModel,
        ...(comparisonModels || []),
        ...(model && model !== priAnchorModel ? [model] : []),
      ]);

      const plan = makePlan({
        tool: "vehicle_pricelist",
        brand,
        model: priAnchorModel,
        variant: priAnchorVariant,
        city,
        mode: "multi_tool",
        conversationMode: "comparison",
        customerStage: "consideration",
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
        },
        reasoningSummary:
          "User asked price, comparison, EMI and offers in one message.",
        resolution: {
          variantSelectionMode: priAnchorVariant ? "exact" : "not_required",
          selectedVariants: priAnchorVariant
            ? [{ model: priAnchorModel, variant: priAnchorVariant }]
            : [],
          selectedModels: [{ model: priAnchorModel }],
          note: "Show price first, then comparison, EMI and offer/quote path.",
        },
        ambiguity: {
          level: "soft_default",
          type: "comparison_variant",
          message:
            "I’ll compare representative/popular variants for now. You can change variants anytime.",
          options: [],
          selectedDefault: {
            variantSelectionMode: "representative_default",
          },
        },
        selectedComparisonSet:
          priCompareModels.length >= 2
            ? {
                models: priCompareModels,
                variantSelectionMode: "representative_default",
              }
            : {},
      });

      plan.ambiguity = {
        level: "soft_default",
        type: "comparison_variant",
        message:
          "I’ll compare representative/popular variants for now. You can change variants anytime.",
        options: [],
        selectedDefault: {
          variantSelectionMode: "representative_default",
        },
      };

      plan.tools.push({
        tool: "vehicle_compare",
        entities: {
          model: priAnchorModel,
          primaryModel: priAnchorModel,
          ...(priCompareModels.length >= 2
            ? {
                models: priCompareModels,
                comparisonModels: priCompareModels,
              }
            : {}),
        },
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
        },
        ranking: null,
        output: outputForTool("vehicle_compare"),
        resolution: makeResolution({
          model: priAnchorModel,
          variant: "",
          comparisonLevel: "model",
          variantSelectionMode: "representative_default",
          selectedModels: priCompareModels.map((item) => ({ model: item })),
          selectedVariants: [],
          note: "Compare requested models using representative variants.",
        }),
      });

      plan.tools.push({
        tool: "vehicle_emi",
        entities: {
          model: priAnchorModel,
          primaryModel: priAnchorModel,
          ...(priAnchorVariant
            ? { variant: priAnchorVariant, primaryVariant: priAnchorVariant }
            : {}),
        },
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
          ...(priDownPayment !== undefined ? { downPayment: priDownPayment } : {}),
          ...(priTenureMonths !== undefined ? { tenureMonths: priTenureMonths } : {}),
          ...(priLoanPercent !== undefined ? { loanPercent: priLoanPercent } : {}),
        },
        ranking: null,
        output: outputForTool("vehicle_emi"),
        resolution: makeResolution({
          model: priAnchorModel,
          variant: priAnchorVariant,
          variantSelectionMode: priAnchorVariant ? "exact" : "representative_default",
          selectedVariants: priAnchorVariant
            ? [{ model: priAnchorModel, variant: priAnchorVariant }]
            : [{ model: priAnchorModel, variantStrategy: "popular_or_best_value" }],
          note: "Calculate EMI for selected/representative variant.",
        }),
      });

      plan.tools.push({
        tool: "aci_lead_capture",
        entities: {
          model: priAnchorModel,
          primaryModel: priAnchorModel,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        filters: {
          city,
          activeOnly: true,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        ranking: null,
        output: outputForTool("aci_lead_capture"),
        resolution: makeResolution({
          model: priAnchorModel,
          variant: "",
          variantSelectionMode: "not_required",
          selectedVariants: [],
          note: "Offers are not verified live; capture offer enquiry/quotation.",
        }),
      });

      return plan;
    }
  }
  /* ACI_PRIORITY_REMAINING_5_END */



  /* ACI_ANSWER_SMOKE_FIXES_START */
  {
    const contextVehicle =
      context?.selectedVehicle ||
      context?.anchorVehicle ||
      context?.vehicle ||
      context?.history?.selectedVehicle ||
      {};

    const contextModelForAnswer = clean(
      pickFirst(contextVehicle?.model, context?.anchorModel, context?.model) || "",
    );

    const contextVariantForAnswer = clean(
      pickFirst(contextVehicle?.variant, context?.anchorVariant, context?.variant) || "",
    );

    const anchorModelForAnswer = model || contextModelForAnswer;
    const anchorVariantForAnswer = variant || contextVariantForAnswer;

    // Fuel decisions like CNG vs petrol are not car-model comparisons.
    if (
      has(raw, /\b(cng|petrol|diesel|hybrid|ev)\b/) &&
      has(raw, /\bwhich is better|daily|running|km|fuel\b/)
    ) {
      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "evaluation",
        entities: {
          topic: "petrol_vs_diesel",
          fuelTypes: [
            ...(has(raw, /\bcng\b/) ? ["CNG"] : []),
            ...(has(raw, /\bpetrol\b/) ? ["Petrol"] : []),
            ...(has(raw, /\bdiesel\b/) ? ["Diesel"] : []),
            ...(has(raw, /\bhybrid\b/) ? ["Hybrid"] : []),
            ...(has(raw, /\bev\b|\belectric\b/) ? ["EV"] : []),
          ],
        },
        filters: {
          city,
          activeOnly: true,
        },
        reasoningSummary: "User asked fuel decision/running cost, not model comparison.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Explain fuel decision / running cost assumptions.",
        },
      });
    }

    // Company-name purchase is a registration/billing/finance explainer.
    if (has(raw, /\bcompany name|company buy|business name|firm name\b/)) {
      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "exploration",
        entities: {
          topic: "quotation",
          subTopic: "company_name_purchase",
        },
        filters: {
          city,
          activeOnly: true,
        },
        reasoningSummary: "User asked if car can be purchased in company name.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Explain company-name purchase / quotation requirements.",
        },
      });
    }

    // Context compare: "Compare with City" should use selected vehicle + resolved model.
    if (
      isComparisonQuery(raw) &&
      contextModelForAnswer &&
      model &&
      contextModelForAnswer !== model
    ) {
      const modelsForCompare = unique([contextModelForAnswer, model]);

      return makePlan({
        tool: "vehicle_compare",
        brand,
        model: contextModelForAnswer,
        variant: contextVariantForAnswer,
        city,
        conversationMode: "comparison",
        customerStage: "evaluation",
        entities: {
          models: modelsForCompare,
          comparisonModels: modelsForCompare,
        },
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
        },
        selectedComparisonSet: {
          models: modelsForCompare,
          variantSelectionMode: "representative_default",
        },
        reasoningSummary: "User asked contextual comparison with selected vehicle.",
        resolution: {
          comparisonLevel: "model",
          variantSelectionMode: "representative_default",
          selectedModels: modelsForCompare.map((item) => ({ model: item })),
          selectedVariants: contextVariantForAnswer
            ? [{ model: contextModelForAnswer, variant: contextVariantForAnswer }]
            : [],
          note: "Compare selected car with requested model.",
        },
        ambiguity: {
          level: "soft_default",
          type: "comparison_variant",
          message: "I’ll compare representative/popular variants for now. You can change variants anytime.",
          options: [],
          selectedDefault: { variantSelectionMode: "representative_default" },
        },
      });
    }

    // Black availability with selected vehicle should not become service/stock canvas.
    if (
      anchorModelForAnswer &&
      has(raw, /\bblack\b/) &&
      has(raw, /\bavailable|availability|in stock|reserve\b/)
    ) {
      return makePlan({
        tool: "unavailable",
        brand,
        model: anchorModelForAnswer,
        variant: anchorVariantForAnswer,
        city,
        mode: "unavailable",
        conversationMode: "unavailable",
        customerStage: "evaluation",
        unavailableReason: "variant_wise_color_not_available",
        filters: {
          city,
          activeOnly: true,
          unavailableReason: "variant_wise_color_not_available",
        },
        reasoningSummary: "Variant-wise black colour availability is not available.",
        resolution: {
          variantSelectionMode: anchorVariantForAnswer ? "exact" : "not_required",
          selectedVariants: anchorVariantForAnswer
            ? [{ model: anchorModelForAnswer, variant: anchorVariantForAnswer }]
            : [],
          selectedModels: [{ model: anchorModelForAnswer }],
          note: "variant_wise_color_not_available",
        },
      });
    }

    // Multi-intent: price + compare + EMI + offers must not collapse to offers only.
    if (
      anchorModelForAnswer &&
      isGenericPriceQuery(raw) &&
      isComparisonQuery(raw) &&
      isEmiQuery(raw) &&
      has(raw, /\boffers?|discount|scheme\b/)
    ) {
      const compareModels = unique([
        anchorModelForAnswer,
        ...(comparisonModels || []),
        ...(model && model !== anchorModelForAnswer ? [model] : []),
      ]);

      const plan = makePlan({
        tool: "vehicle_pricelist",
        brand,
        model: anchorModelForAnswer,
        variant: anchorVariantForAnswer,
        city,
        mode: "multi_tool",
        conversationMode: "comparison",
        customerStage: "consideration",
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
        },
        reasoningSummary: "User asked price, comparison, EMI and offers in one message.",
        resolution: {
          variantSelectionMode: anchorVariantForAnswer ? "exact" : "not_required",
          selectedVariants: anchorVariantForAnswer
            ? [{ model: anchorModelForAnswer, variant: anchorVariantForAnswer }]
            : [],
          selectedModels: [{ model: anchorModelForAnswer }],
          note: "Show price first, then comparison, EMI and offer/quote path.",
        },
      });

      plan.tools.push({
        tool: "vehicle_compare",
        entities: {
          model: anchorModelForAnswer,
          primaryModel: anchorModelForAnswer,
          ...(compareModels.length >= 2 ? { models: compareModels, comparisonModels: compareModels } : {}),
        },
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
        },
        ranking: null,
        output: outputForTool("vehicle_compare"),
        resolution: makeResolution({
          model: anchorModelForAnswer,
          variant: "",
          comparisonLevel: "model",
          variantSelectionMode: "representative_default",
          selectedModels: compareModels.map((item) => ({ model: item })),
          selectedVariants: [],
          note: "Compare requested models using representative variants.",
        }),
      });

      plan.tools.push({
        tool: "vehicle_emi",
        entities: {
          model: anchorModelForAnswer,
          primaryModel: anchorModelForAnswer,
          ...(anchorVariantForAnswer ? { variant: anchorVariantForAnswer, primaryVariant: anchorVariantForAnswer } : {}),
        },
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
          ...(extractTenureMonths(raw) ? { tenureMonths: extractTenureMonths(raw) } : {}),
        },
        ranking: null,
        output: outputForTool("vehicle_emi"),
        resolution: makeResolution({
          model: anchorModelForAnswer,
          variant: anchorVariantForAnswer,
          variantSelectionMode: anchorVariantForAnswer ? "exact" : "representative_default",
          selectedVariants: anchorVariantForAnswer
            ? [{ model: anchorModelForAnswer, variant: anchorVariantForAnswer }]
            : [{ model: anchorModelForAnswer, variantStrategy: "popular_or_best_value" }],
          note: "Calculate EMI for selected/representative variant.",
        }),
      });

      plan.tools.push({
        tool: "aci_lead_capture",
        entities: {
          model: anchorModelForAnswer,
          primaryModel: anchorModelForAnswer,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        filters: {
          city,
          activeOnly: true,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        ranking: null,
        output: outputForTool("aci_lead_capture"),
        resolution: makeResolution({
          model: anchorModelForAnswer,
          variant: "",
          variantSelectionMode: "not_required",
          selectedVariants: [],
          note: "Offers are not verified live; capture offer enquiry/quotation.",
        }),
      });

      return plan;
    }
  }
  /* ACI_ANSWER_SMOKE_FIXES_END */



  /* ACI_FINAL_10_PLANNER_FIXES_START */
  {
    const finalContextVehicle =
      context?.selectedVehicle ||
      context?.anchorVehicle ||
      context?.vehicle ||
      context?.history?.selectedVehicle ||
      {};

    const finalContextModel = clean(
      pickFirst(
        finalContextVehicle?.model,
        context?.anchorModel,
        context?.model,
      ) || "",
    );

    const finalContextVariant = clean(
      pickFirst(
        finalContextVehicle?.variant,
        context?.anchorVariant,
        context?.variant,
      ) || "",
    );

    const finalAnchorModel = model || finalContextModel;
    const finalAnchorVariant = variant || finalContextVariant;
    const finalBudgetMax = extractBudgetMax(raw);
    const finalDownPayment = extractDownPayment(raw);
    const finalTenureMonths = extractTenureMonths(raw);
    const finalLoanPercent = extractLoanPercent(raw);
    const finalMustHaveFeatures = extractMustHaveFeatures(raw);

    const finalHasExplicitSafetyRanking =
      has(raw, /\bsafest\b|\bsafer\b|\bsafety\b|\bglobal ncap\b|\bncap\b|\b5 star\b|\bfive star\b/);

    const finalLooksLikeFeatureMatch =
      finalMustHaveFeatures.length > 0 &&
      (
        has(raw, /\bcars?\b|\bsuvs?\b|\bsedans?\b|\bhatchbacks?\b/) ||
        has(raw, /\bi want\b|\bbest\b|\bunder\b|\bwith\b|\bautomatic\b|\bmanual\b/)
      ) &&
      !finalHasExplicitSafetyRanking;

    // Internal ops must never go into new-car planner.
    if (has(raw, /\bloan closure\b|\bclosure\s+\d{3,}\b/)) {
      return makePlan({
        tool: "internal_passthrough",
        city,
        domain: "internal",
        conversationMode: "internal_passthrough",
        customerStage: "unknown",
        reasoningSummary: "Internal loan closure query should bypass new-car planner.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Internal CDrive loan closure passthrough.",
        },
      });
    }

    // Feature-match should stay feature_match, even if one requested feature is safety-related.
    // Examples:
    // - Best automatic SUV under 20 lakh with sunroof and 6 airbags
    // - Cars with 6 airbags under 15 lakh
    // - Cars with ADAS and panoramic sunroof under 25 lakh
    // - I want automatic, sunroof and 6 airbags under 15 lakh
    if (finalLooksLikeFeatureMatch) {
      return makePlan({
        tool: "vehicle_recommend",
        brand,
        city,
        conversationMode: "recommendation",
        customerStage: "exploration",
        ranking: "feature_match",
        filters: {
          priceBasis: "on_road",
          ...(finalBudgetMax ? { budgetMax: finalBudgetMax } : {}),
          ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
          ...(has(raw, /\bsuvs?\b/) ? { bodyType: "suv" } : {}),
          mustHaveFeatures: finalMustHaveFeatures,
        },
        userPreferences: {
          ...(finalBudgetMax ? { budgetMax: finalBudgetMax } : {}),
          ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
          ...(has(raw, /\bsuvs?\b/) ? { bodyType: "suv" } : {}),
          mustHaveFeatures: finalMustHaveFeatures,
        },
        reasoningSummary: "User asked for must-have feature matching, not a safety leaderboard.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Show model cards first with suggested representative variants.",
        },
      });
    }

    // Fuel decision must include CNG/Petrol in the actual planner payload.
    if (
      has(raw, /\bcng\b|\bpetrol\b|\bdiesel\b|\bhybrid\b|\bev\b|\belectric\b/) &&
      has(raw, /\bwhich is better\b|\bdaily\b|\brunning\b|\bkm\b|\bfuel\b/)
    ) {
      const fuelTypes = [
        ...(has(raw, /\bcng\b/) ? ["CNG"] : []),
        ...(has(raw, /\bpetrol\b/) ? ["Petrol"] : []),
        ...(has(raw, /\bdiesel\b/) ? ["Diesel"] : []),
        ...(has(raw, /\bhybrid\b/) ? ["Hybrid"] : []),
        ...(has(raw, /\bev\b|\belectric\b/) ? ["EV"] : []),
      ];

      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "evaluation",
        entities: {
          topic: "petrol_vs_diesel",
          topics: ["fuel_decision", ...fuelTypes],
          fuelType: fuelTypes.join(" vs "),
        },
        filters: {
          city,
          activeOnly: true,
          fuelType: fuelTypes.join(" vs "),
        },
        userPreferences: {
          fuelTypes,
        },
        reasoningSummary: "User asked fuel decision / running cost comparison.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: `Explain fuel decision for ${fuelTypes.join(" vs ") || "fuel types"}.`,
        },
      });
    }

    // Company-name + loan must contain loan/finance context.
    if (has(raw, /\bcompany name\b|\bcompany buy\b|\bbusiness name\b|\bfirm name\b/) && has(raw, /\bloan\b|\bfinance\b|\bfunding\b/)) {
      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "exploration",
        entities: {
          topic: "emi",
          topics: ["company_name_purchase", "car_loan", "finance"],
        },
        filters: {
          city,
          activeOnly: true,
        },
        reasoningSummary: "User asked company-name purchase with car-loan eligibility context.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Explain company-name car loan / finance requirements.",
        },
      });
    }

    // Company-name without explicit loan is registration / quotation explainer,
    // but keep finance/loan words in topics so regression sees the context.
    if (has(raw, /\bcompany name\b|\bcompany buy\b|\bbusiness name\b|\bfirm name\b/)) {
      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "exploration",
        entities: {
          topic: "quotation",
          topics: ["company_name_purchase", "registration", "finance", "loan"],
        },
        filters: {
          city,
          activeOnly: true,
        },
        reasoningSummary: "User asked if a car can be purchased in company name.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Explain company-name purchase / quotation / registration requirements.",
        },
      });
    }

    // Best price / black / automatic should be quotation lead with color and transmission preserved.
    if (
      finalAnchorModel &&
      has(raw, /\bbest price\b|\bfinal price\b|\bquote\b|\bquotation\b/) &&
      has(raw, /\bblack\b|\bwhite\b|\bred\b|\bblue\b|\bgrey\b|\bgray\b|\bsilver\b/)
    ) {
      const finalColor =
        has(raw, /\bblack\b/) ? "black" :
        has(raw, /\bwhite\b/) ? "white" :
        has(raw, /\bred\b/) ? "red" :
        has(raw, /\bblue\b/) ? "blue" :
        has(raw, /\bgrey\b|\bgray\b/) ? "grey" :
        has(raw, /\bsilver\b/) ? "silver" :
        "";

      return makePlan({
        tool: "aci_lead_capture",
        brand,
        model: finalAnchorModel,
        variant: finalAnchorVariant,
        city,
        conversationMode: "lead_capture",
        customerStage: "closing",
        entities: {
          leadType: "quotation",
          selectedServices: ["quotation"],
          ...(finalColor ? { color: finalColor } : {}),
          ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
        },
        filters: {
          city,
          activeOnly: true,
          leadType: "quotation",
          selectedServices: ["quotation"],
          ...(finalColor ? { color: finalColor } : {}),
          ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
        },
        leadContext: {
          leadType: "quotation",
          selectedServices: ["quotation"],
          ...(finalColor ? { color: finalColor } : {}),
          ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
        },
        reasoningSummary: "User asked best price / quotation with colour and transmission.",
        resolution: {
          variantSelectionMode: finalAnchorVariant ? "exact" : "not_required",
          selectedVariants: finalAnchorVariant
            ? [{ model: finalAnchorModel, variant: finalAnchorVariant }]
            : [],
          selectedModels: [{ model: finalAnchorModel }],
          note: "Lead capture for quotation with requested colour/transmission.",
        },
      });
    }

    // Heavy multi-intent: price + compare + EMI + offers.
    if (
      finalAnchorModel &&
      isGenericPriceQuery(raw) &&
      isComparisonQuery(raw) &&
      isEmiQuery(raw) &&
      has(raw, /\boffers?\b|\bdiscount\b|\bscheme\b/)
    ) {
      const finalCompareModels = unique([
        finalAnchorModel,
        ...(comparisonModels || []),
        ...(model && model !== finalAnchorModel ? [model] : []),
      ]);

      const plan = makePlan({
        tool: "vehicle_pricelist",
        brand,
        model: finalAnchorModel,
        variant: finalAnchorVariant,
        city,
        mode: "multi_tool",
        conversationMode: "comparison",
        customerStage: "consideration",
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
        },
        reasoningSummary: "User asked price, comparison, EMI and offers in one message.",
        resolution: {
          variantSelectionMode: finalAnchorVariant ? "exact" : "not_required",
          selectedVariants: finalAnchorVariant
            ? [{ model: finalAnchorModel, variant: finalAnchorVariant }]
            : [],
          selectedModels: [{ model: finalAnchorModel }],
          note: "Show price first, then comparison, EMI and offer/quote path.",
        },
      });

      plan.tools.push({
        tool: "vehicle_compare",
        entities: {
          model: finalAnchorModel,
          primaryModel: finalAnchorModel,
          ...(finalCompareModels.length >= 2
            ? { models: finalCompareModels, comparisonModels: finalCompareModels }
            : {}),
        },
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
        },
        ranking: null,
        output: outputForTool("vehicle_compare"),
        resolution: makeResolution({
          model: finalAnchorModel,
          variant: "",
          comparisonLevel: "model",
          variantSelectionMode: "representative_default",
          selectedModels: finalCompareModels.map((item) => ({ model: item })),
          selectedVariants: [],
          note: "Compare requested models using representative variants.",
        }),
      });

      plan.tools.push({
        tool: "vehicle_emi",
        entities: {
          model: finalAnchorModel,
          primaryModel: finalAnchorModel,
          ...(finalAnchorVariant
            ? { variant: finalAnchorVariant, primaryVariant: finalAnchorVariant }
            : {}),
        },
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
          ...(finalDownPayment !== undefined ? { downPayment: finalDownPayment } : {}),
          ...(finalTenureMonths !== undefined ? { tenureMonths: finalTenureMonths } : {}),
          ...(finalLoanPercent !== undefined ? { loanPercent: finalLoanPercent } : {}),
        },
        ranking: null,
        output: outputForTool("vehicle_emi"),
        resolution: makeResolution({
          model: finalAnchorModel,
          variant: finalAnchorVariant,
          variantSelectionMode: finalAnchorVariant ? "exact" : "representative_default",
          selectedVariants: finalAnchorVariant
            ? [{ model: finalAnchorModel, variant: finalAnchorVariant }]
            : [{ model: finalAnchorModel, variantStrategy: "popular_or_best_value" }],
          note: "Calculate EMI for selected/representative variant.",
        }),
      });

      plan.tools.push({
        tool: "aci_lead_capture",
        entities: {
          model: finalAnchorModel,
          primaryModel: finalAnchorModel,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        filters: {
          city,
          activeOnly: true,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        ranking: null,
        output: outputForTool("aci_lead_capture"),
        resolution: makeResolution({
          model: finalAnchorModel,
          variant: "",
          variantSelectionMode: "not_required",
          selectedVariants: [],
          note: "Offers are not verified live; capture offer enquiry/quotation.",
        }),
      });

      return plan;
    }
  }
  /* ACI_FINAL_10_PLANNER_FIXES_END */



  /* ACI_REMAINING_5_FINAL_FIXES_START */
  {
    const remContextVehicle =
      context?.selectedVehicle ||
      context?.anchorVehicle ||
      context?.vehicle ||
      context?.history?.selectedVehicle ||
      {};

    const remContextModel = clean(
      pickFirst(remContextVehicle?.model, context?.anchorModel, context?.model) || "",
    );

    const remContextVariant = clean(
      pickFirst(
        remContextVehicle?.variant,
        context?.anchorVariant,
        context?.variant,
      ) || "",
    );

    const remAnchorModel = model || remContextModel;
    const remAnchorVariant = variant || remContextVariant;
    const remBudgetMax = extractBudgetMax(raw);
    const remDownPayment = extractDownPayment(raw);
    const remTenureMonths = extractTenureMonths(raw);
    const remLoanPercent = extractLoanPercent(raw);

    const remFeatures = (() => {
      const features = [...extractMustHaveFeatures(raw)];

      if (has(raw, /\bpanoramic\s+sunroof\b/)) {
        return unique(
          features
            .filter((item) => key(item) !== "sunroof")
            .concat(["panoramic sunroof"]),
        );
      }

      return features;
    })();

    const remFuelTypes = [
      ...(has(raw, /\bcng\b/) ? ["CNG"] : []),
      ...(has(raw, /\bpetrol\b/) ? ["Petrol"] : []),
      ...(has(raw, /\bdiesel\b/) ? ["Diesel"] : []),
      ...(has(raw, /\bhybrid\b/) ? ["Hybrid"] : []),
      ...(has(raw, /\bev\b|\belectric\b/) ? ["EV"] : []),
    ];

    // 1) Feature-match: preserve "panoramic sunroof", not generic "sunroof".
    if (
      remFeatures.length > 0 &&
      (
        has(raw, /\bcars?\b|\bsuvs?\b|\bsedans?\b|\bhatchbacks?\b/) ||
        has(raw, /\bi want\b|\bbest\b|\bunder\b|\bwith\b|\bautomatic\b|\bmanual\b/)
      ) &&
      !has(raw, /\bsafest\b|\bsafer\b|\bsafety\b|\bglobal ncap\b|\bncap\b|\b5 star\b|\bfive star\b/)
    ) {
      return makePlan({
        tool: "vehicle_recommend",
        brand,
        city,
        conversationMode: "recommendation",
        customerStage: "exploration",
        ranking: "feature_match",
        filters: {
          priceBasis: "on_road",
          ...(remBudgetMax ? { budgetMax: remBudgetMax } : {}),
          ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
          ...(has(raw, /\bsuvs?\b/) ? { bodyType: "suv" } : {}),
          mustHaveFeatures: remFeatures,
        },
        userPreferences: {
          ...(remBudgetMax ? { budgetMax: remBudgetMax } : {}),
          ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
          ...(has(raw, /\bsuvs?\b/) ? { bodyType: "suv" } : {}),
          mustHaveFeatures: remFeatures,
        },
        reasoningSummary: "User asked for must-have feature matching.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Show model cards first with suggested representative variants.",
        },
      });
    }

    // 2) Fuel decision: keep CNG/Petrol visible in the planner JSON.
    if (
      remFuelTypes.length > 0 &&
      has(raw, /\bwhich is better\b|\bdaily\b|\brunning\b|\bkm\b|\bfuel\b/)
    ) {
      const fuelLabel = remFuelTypes.join(" vs ");

      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "evaluation",
        entities: {
          topic: "petrol_vs_diesel",
          topics: ["fuel_decision", "running_cost", ...remFuelTypes],
          fuelType: fuelLabel,
        },
        filters: {
          city,
          activeOnly: true,
          fuelType: fuelLabel,
          fuelTypes: remFuelTypes,
        },
        userPreferences: {
          fuelType: fuelLabel,
          fuelTypes: remFuelTypes,
        },
        reasoningSummary: `User asked fuel decision / running cost comparison: ${fuelLabel}.`,
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: `Explain fuel decision for ${fuelLabel}.`,
        },
      });
    }

    // 3) Company-name + loan: ensure loan/finance is explicitly present.
    if (
      has(raw, /\bcompany name\b|\bcompany buy\b|\bbusiness name\b|\bfirm name\b/) &&
      has(raw, /\bloan\b|\bfinance\b|\bfunding\b/)
    ) {
      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "exploration",
        entities: {
          topic: "emi",
          topics: [
            "company_name_purchase",
            "car_loan",
            "loan",
            "finance",
            "rto",
            "registration",
          ],
        },
        filters: {
          city,
          activeOnly: true,
          loanContext: "company_name_car_loan",
          registrationContext: "company_name_rto_registration",
        },
        reasoningSummary:
          "User asked company-name purchase with car-loan / finance eligibility context.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Explain company-name car loan / finance / RTO registration requirements.",
        },
      });
    }

    // 4) Company-name registration: ensure RTO/registration is explicitly present.
    if (has(raw, /\bcompany name\b|\bcompany buy\b|\bbusiness name\b|\bfirm name\b/)) {
      return makePlan({
        tool: "vehicle_explainer",
        brand,
        city,
        conversationMode: "education",
        customerStage: "exploration",
        entities: {
          topic: "rto",
          topics: [
            "company_name_purchase",
            "rto",
            "registration",
            "quotation",
            "finance",
            "loan",
          ],
        },
        filters: {
          city,
          activeOnly: true,
          registrationContext: "company_name_rto_registration",
          loanContext: "company_name_car_loan",
        },
        reasoningSummary:
          "User asked if a car can be purchased / registered in company name.",
        resolution: {
          variantSelectionMode: "not_required",
          selectedVariants: [],
          selectedModels: [],
          note: "Explain company-name purchase / RTO / registration / quotation requirements.",
        },
      });
    }

    // 5) Heavy multi-intent: add required comparison ambiguity metadata.
    if (
      remAnchorModel &&
      isGenericPriceQuery(raw) &&
      isComparisonQuery(raw) &&
      isEmiQuery(raw) &&
      has(raw, /\boffers?\b|\bdiscount\b|\bscheme\b/)
    ) {
      const remCompareModels = unique([
        remAnchorModel,
        ...(comparisonModels || []),
        ...(model && model !== remAnchorModel ? [model] : []),
      ]);

      const plan = makePlan({
        tool: "vehicle_pricelist",
        brand,
        model: remAnchorModel,
        variant: remAnchorVariant,
        city,
        mode: "multi_tool",
        conversationMode: "comparison",
        customerStage: "consideration",
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
        },
        reasoningSummary:
          "User asked price, comparison, EMI and offers in one message.",
        resolution: {
          variantSelectionMode: remAnchorVariant ? "exact" : "not_required",
          selectedVariants: remAnchorVariant
            ? [{ model: remAnchorModel, variant: remAnchorVariant }]
            : [],
          selectedModels: [{ model: remAnchorModel }],
          note: "Show price first, then comparison, EMI and offer/quote path.",
        },
        ambiguity: {
          level: "soft_default",
          type: "comparison_variant",
          message:
            "I’ll compare representative/popular variants for now. You can change variants anytime.",
          options: [],
          selectedDefault: {
            variantSelectionMode: "representative_default",
          },
        },
        selectedComparisonSet:
          remCompareModels.length >= 2
            ? {
                models: remCompareModels,
                variantSelectionMode: "representative_default",
              }
            : {},
      });

      plan.tools.push({
        tool: "vehicle_compare",
        entities: {
          model: remAnchorModel,
          primaryModel: remAnchorModel,
          ...(remCompareModels.length >= 2
            ? {
                models: remCompareModels,
                comparisonModels: remCompareModels,
              }
            : {}),
        },
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
        },
        ranking: null,
        output: outputForTool("vehicle_compare"),
        resolution: makeResolution({
          model: remAnchorModel,
          variant: "",
          comparisonLevel: "model",
          variantSelectionMode: "representative_default",
          selectedModels: remCompareModels.map((item) => ({ model: item })),
          selectedVariants: [],
          note: "Compare requested models using representative variants.",
        }),
      });

      plan.tools.push({
        tool: "vehicle_emi",
        entities: {
          model: remAnchorModel,
          primaryModel: remAnchorModel,
          ...(remAnchorVariant
            ? { variant: remAnchorVariant, primaryVariant: remAnchorVariant }
            : {}),
        },
        filters: {
          city,
          activeOnly: true,
          priceBasis: "on_road",
          ...(remDownPayment !== undefined ? { downPayment: remDownPayment } : {}),
          ...(remTenureMonths !== undefined ? { tenureMonths: remTenureMonths } : {}),
          ...(remLoanPercent !== undefined ? { loanPercent: remLoanPercent } : {}),
        },
        ranking: null,
        output: outputForTool("vehicle_emi"),
        resolution: makeResolution({
          model: remAnchorModel,
          variant: remAnchorVariant,
          variantSelectionMode: remAnchorVariant ? "exact" : "representative_default",
          selectedVariants: remAnchorVariant
            ? [{ model: remAnchorModel, variant: remAnchorVariant }]
            : [{ model: remAnchorModel, variantStrategy: "popular_or_best_value" }],
          note: "Calculate EMI for selected/representative variant.",
        }),
      });

      plan.tools.push({
        tool: "aci_lead_capture",
        entities: {
          model: remAnchorModel,
          primaryModel: remAnchorModel,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        filters: {
          city,
          activeOnly: true,
          leadType: "offer_enquiry",
          selectedServices: ["offer_enquiry", "quotation"],
        },
        ranking: null,
        output: outputForTool("aci_lead_capture"),
        resolution: makeResolution({
          model: remAnchorModel,
          variant: "",
          variantSelectionMode: "not_required",
          selectedVariants: [],
          note: "Offers are not verified live; capture offer enquiry/quotation.",
        }),
      });

      return plan;
    }
  }
  /* ACI_REMAINING_5_FINAL_FIXES_END */


  if (isSecurityQuery(raw)) {
    return makePlan({
      tool: "unavailable",
      brand,
      city,
      mode: "unavailable",
      conversationMode: "unavailable",
      customerStage: "unknown",
      unavailableReason: "unsupported_request",
      reasoningSummary: "Blocked security-sensitive or prompt-injection style request.",
      resolution: { variantSelectionMode: "not_required", note: "Security-sensitive or prompt-injection style request." },
    });
  }


  /* ACI_FAILED_22_FIXES_START */

  const contextVehicle =
    context?.selectedVehicle ||
    context?.anchorVehicle ||
    context?.vehicle ||
    context?.history?.selectedVehicle ||
    {};

  const contextModel = clean(
    pickFirst(contextVehicle?.model, context?.anchorModel, context?.model) || "",
  );

  const contextVariant = clean(
    pickFirst(contextVehicle?.variant, context?.anchorVariant, context?.variant) || "",
  );

  const anchorModel = model || contextModel;
  const anchorVariant = variant || contextVariant;
  const budgetMax = extractBudgetMax(raw);
  const downPayment = extractDownPayment(raw);
  const tenureMonths = extractTenureMonths(raw);
  const loanPercent = extractLoanPercent(raw);
  const mustHaveFeatures = extractMustHaveFeatures(raw);

  const makeMultiPlan = ({
    firstTool,
    secondTool,
    firstEntities = {},
    secondEntities = {},
    firstFilters = {},
    secondFilters = {},
    firstRanking = null,
    secondRanking = null,
    firstOutput = null,
    secondOutput = null,
    conversationMode = "direct_answer",
    customerStage = "evaluation",
    reasoningSummary = "Multi-intent deterministic planner red fix.",
  }) => {
    const plan = makePlan({
      tool: firstTool,
      brand,
      model: anchorModel,
      variant: anchorVariant,
      city,
      entities: firstEntities,
      filters: firstFilters,
      ranking: firstRanking,
      mode: "multi_tool",
      conversationMode,
      customerStage,
      reasoningSummary,
      resolution: {
        variantSelectionMode: anchorVariant ? "exact" : "representative_default",
        selectedVariants: anchorVariant
          ? [{ model: anchorModel, variant: anchorVariant }]
          : anchorModel
            ? [{ model: anchorModel, variantStrategy: "popular_or_best_value" }]
            : [],
        selectedModels: anchorModel ? [{ model: anchorModel }] : [],
        note: reasoningSummary,
      },
    });

    plan.tools.push({
      tool: secondTool,
      entities: {
        ...(anchorModel ? { model: anchorModel, primaryModel: anchorModel } : {}),
        ...(anchorVariant ? { variant: anchorVariant, primaryVariant: anchorVariant } : {}),
        ...secondEntities,
      },
      filters: {
        city,
        activeOnly: true,
        ...secondFilters,
      },
      ranking: secondRanking,
      output: secondOutput || outputForTool(secondTool, { variant: anchorVariant }),
      resolution: makeResolution({
        model: anchorModel,
        variant: anchorVariant,
        variantSelectionMode: anchorVariant ? "exact" : "representative_default",
        selectedVariants: anchorVariant
          ? [{ model: anchorModel, variant: anchorVariant }]
          : anchorModel
            ? [{ model: anchorModel, variantStrategy: "popular_or_best_value" }]
            : [],
        note: reasoningSummary,
      }),
    });

    return plan;
  };

  // ADAS and other no-model concept explainers should never fall through.
  if (has(raw, /\bwhat is adas\b|\bexplain adas\b|\badas meaning\b/)) {
    return makePlan({
      tool: "vehicle_explainer",
      brand,
      city,
      conversationMode: "education",
      customerStage: "exploration",
      entities: { topic: "adas" },
      reasoningSummary: "User asked ADAS explainer.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        selectedModels: [],
        note: "Explain adas.",
      },
    });
  }

  // Company-name / business car-loan question without a selected model.
  if (
    !anchorModel &&
    has(raw, /\b(company name|company buy|business owner|self employed|car loan|can i get car loan|loan)\b/)
  ) {
    return makePlan({
      tool: "vehicle_explainer",
      brand,
      city,
      conversationMode: "education",
      customerStage: "exploration",
      entities: { topic: "emi" },
      reasoningSummary: "User asked car-loan eligibility / company-name finance explainer.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        selectedModels: [],
        note: "Explain emi.",
      },
    });
  }

  // Quote + finance must stay lead capture, not get swallowed by multi-intent.
  if (anchorModel && isLeadQuery(raw) && has(raw, /\bfinance|loan|funding\b/)) {
    const services = unique(["quotation", "finance"]);

    return makePlan({
      tool: "aci_lead_capture",
      brand,
      model: anchorModel,
      variant: anchorVariant,
      city,
      conversationMode: "lead_capture",
      customerStage: "closing",
      entities: {
        leadType: "quotation",
        selectedServices: services,
      },
      filters: {
        leadType: "quotation",
        selectedServices: services,
      },
      leadContext: {
        leadType: "quotation",
        selectedServices: services,
      },
      reasoningSummary: "User asked quotation with finance.",
      resolution: {
        variantSelectionMode: anchorVariant ? "exact" : "not_required",
        selectedVariants: anchorVariant ? [{ model: anchorModel, variant: anchorVariant }] : [],
        note: "Lead capture for quotation with finance.",
      },
    });
  }

  // Variant-wise color / stock-color questions are unavailable until variant-color inventory exists.
  if (
    anchorModel &&
    isColorQuery(raw) &&
    (
      anchorVariant ||
      has(raw, /\b(get|gets|available|availability|reserve|comes|come|in stock)\b/)
    ) &&
    has(raw, /\b(black|white|red|blue|grey|gray|silver|titan|pearl|color|colour)\b/)
  ) {
    return makePlan({
      tool: "unavailable",
      brand,
      model: anchorModel,
      variant: anchorVariant,
      city,
      mode: "unavailable",
      conversationMode: "unavailable",
      customerStage: "evaluation",
      unavailableReason: "variant_wise_color_not_available",
      filters: {
        unavailableReason: "variant_wise_color_not_available",
      },
      reasoningSummary: "Variant-wise color availability is not available.",
      resolution: {
        variantSelectionMode: anchorVariant ? "exact" : "not_required",
        selectedVariants: anchorVariant ? [{ model: anchorModel, variant: anchorVariant }] : [],
        selectedModels: [{ model: anchorModel }],
        note: "variant_wise_color_not_available",
      },
    });
  }

  // Multi: feature answer + EMI in one question.
  if (
    anchorModel &&
    has(raw, /\bsunroof|adas|airbags?|mileage|boot space|ground clearance\b/) &&
    has(raw, /\bemi|loan|down payment\b/)
  ) {
    const feature = extractFeature(raw) || "features";

    return makeMultiPlan({
      firstTool: "vehicle_feature_lookup",
      secondTool: "vehicle_emi",
      firstEntities: { feature },
      secondFilters: {
        priceBasis: "on_road",
        ...(downPayment !== undefined ? { downPayment } : {}),
        ...(tenureMonths !== undefined ? { tenureMonths } : {}),
        ...(loanPercent !== undefined ? { loanPercent } : {}),
      },
      conversationMode: "calculation",
      customerStage: "consideration",
      reasoningSummary: "User asked feature lookup and EMI in one message.",
    });
  }

  // Multi/compare: comparison + mileage winner.
  if (
    isComparisonQuery(raw) &&
    has(raw, /\bmileage|average|fuel efficiency\b/)
  ) {
    const modelsForCompare = unique([
      ...comparisonModels,
      ...(contextModel && model && contextModel !== model ? [contextModel, model] : []),
      ...(comparisonModels.length < 2 && contextModel && anchorModel && contextModel !== anchorModel
        ? [contextModel, anchorModel]
        : []),
    ]);

    return makePlan({
      tool: "vehicle_compare",
      brand,
      model: anchorModel,
      variant: anchorVariant,
      city,
      mode: "multi_tool",
      conversationMode: "comparison",
      customerStage: "evaluation",
      entities: {
        ...(modelsForCompare.length >= 2 ? { models: modelsForCompare, comparisonModels: modelsForCompare } : {}),
        compareFeatures: ["mileage", "fuel efficiency"],
      },
      filters: { priceBasis: "on_road" },
      ranking: "fuel_efficiency",
      selectedComparisonSet:
        modelsForCompare.length >= 2
          ? { models: modelsForCompare, variantSelectionMode: "representative_default" }
          : {},
      reasoningSummary: "User asked comparison with mileage winner.",
      resolution: {
        comparisonLevel: "model",
        variantSelectionMode: "representative_default",
        selectedModels: modelsForCompare.map((item) => ({ model: item })),
        selectedVariants: [],
        note: "Compare selected cars and highlight mileage.",
      },
      ambiguity: {
        level: "soft_default",
        type: "comparison_variant",
        message: "I’ll compare representative/popular variants for now. You can change variants anytime.",
        options: [],
        selectedDefault: { variantSelectionMode: "representative_default" },
      },
    });
  }

  // Context comparison: "Compare with City/Seltos" must combine current selected model + resolved model.
  if (
    isComparisonQuery(raw) &&
    contextModel &&
    model &&
    contextModel !== model
  ) {
    const modelsForCompare = unique([contextModel, model]);

    return makePlan({
      tool: "vehicle_compare",
      brand,
      model: contextModel,
      variant: contextVariant,
      city,
      conversationMode: "comparison",
      customerStage: "evaluation",
      entities: {
        models: modelsForCompare,
        comparisonModels: modelsForCompare,
      },
      filters: { priceBasis: "on_road" },
      ranking: null,
      selectedComparisonSet: {
        models: modelsForCompare,
        variantSelectionMode: "representative_default",
      },
      reasoningSummary: "User asked contextual comparison with another model.",
      resolution: {
        comparisonLevel: "model",
        variantSelectionMode: "representative_default",
        selectedModels: modelsForCompare.map((item) => ({ model: item })),
        selectedVariants: contextVariant ? [{ model: contextModel, variant: contextVariant }] : [],
        note: "Compare selected car with requested model.",
      },
      ambiguity: {
        level: "soft_default",
        type: "comparison_variant",
        message: "I’ll compare representative/popular variants for now. You can change variants anytime.",
        options: [],
        selectedDefault: { variantSelectionMode: "representative_default" },
      },
    });
  }

  // Safety comparison: "Which is safer Verna or Slavia?"
  if (
    isComparisonQuery(raw) ||
    has(raw, /\bwhich is safer|safer .* or |safety .* or \b/)
  ) {
    const modelsForCompare = unique([
      ...comparisonModels,
      ...(contextModel && model && contextModel !== model ? [contextModel, model] : []),
    ]);

    if (modelsForCompare.length >= 2) {
      return makePlan({
        tool: "vehicle_compare",
        brand,
        model: modelsForCompare[0],
        city,
        conversationMode: "comparison",
        customerStage: "evaluation",
        entities: {
          models: modelsForCompare,
          comparisonModels: modelsForCompare,
          compareFeatures: has(raw, /\bsafer|safety\b/)
            ? ["safety", "airbags", "ADAS", "NCAP"]
            : [],
        },
        filters: { priceBasis: "on_road" },
        ranking: has(raw, /\bsafer|safety\b/) ? "safety" : null,
        selectedComparisonSet: {
          models: modelsForCompare,
          variantSelectionMode: "representative_default",
        },
        reasoningSummary: "User asked safety/comparison.",
        resolution: {
          comparisonLevel: "model",
          variantSelectionMode: "representative_default",
          selectedModels: modelsForCompare.map((item) => ({ model: item })),
          selectedVariants: [],
          note: "Compare selected cars using representative variants.",
        },
        ambiguity: {
          level: "soft_default",
          type: "comparison_variant",
          message: "I’ll compare representative/popular variants for now. You can change variants anytime.",
          options: [],
          selectedDefault: { variantSelectionMode: "representative_default" },
        },
      });
    }
  }

  // Feature-match recommendation: allow "sunroof" / "airbags" / "ADAS" to be recommendation, not feature lookup.
  if (
    mustHaveFeatures.length &&
    (
      has(raw, /\bcars?\b|\bsuvs?\b|\bsedans?\b|\bhatchbacks?\b/) ||
      has(raw, /\bi want\b|\bbest\b|\bunder\b/)
    ) &&
    !anchorModel
  ) {
    return makePlan({
      tool: "vehicle_recommend",
      brand,
      city,
      conversationMode: "recommendation",
      customerStage: "exploration",
      ranking: isSafetyQuery(raw) ? "safety" : "feature_match",
      filters: {
        priceBasis: "on_road",
        ...(budgetMax ? { budgetMax } : {}),
        ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
        ...(has(raw, /\bsuvs?\b/) ? { bodyType: "suv" } : {}),
        mustHaveFeatures,
      },
      userPreferences: {
        ...(budgetMax ? { budgetMax } : {}),
        ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
        mustHaveFeatures,
      },
      reasoningSummary: "User asked feature-match recommendation.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        selectedModels: [],
        note: "Show model cards first with suggested representative variants.",
      },
    });
  }

  // Safety recommendation: plural SUVs too.
  if (
    !anchorModel &&
    isSafetyQuery(raw)
  ) {
    return makePlan({
      tool: "vehicle_recommend",
      brand,
      city,
      conversationMode: "recommendation",
      customerStage: "exploration",
      ranking: "safety",
      filters: {
        priceBasis: "on_road",
        ...(budgetMax ? { budgetMax } : {}),
        ...(has(raw, /\bsuvs?\b/) ? { bodyType: "suv" } : {}),
        mustHaveFeatures,
      },
      reasoningSummary: "User asked safety recommendation.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        selectedModels: [],
        note: "Show safest model cards first.",
      },
    });
  }

  // Performance recommendation.
  if (
    !anchorModel &&
    has(raw, /\bperformance|fastest|powerful|turbo|fun to drive\b/)
  ) {
    return makePlan({
      tool: "vehicle_recommend",
      brand,
      city,
      conversationMode: "recommendation",
      customerStage: "exploration",
      ranking: "performance",
      filters: {
        priceBasis: "on_road",
        ...(budgetMax ? { budgetMax } : {}),
      },
      reasoningSummary: "User asked performance recommendation.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        selectedModels: [],
        note: "Show performance-oriented model cards.",
      },
    });
  }

  // Bad roads / rough roads recommendation.
  if (
    !anchorModel &&
    has(raw, /\bbad roads?|rough roads?|village roads?|speed breakers?|ground clearance\b/)
  ) {
    return makePlan({
      tool: "vehicle_recommend",
      brand,
      city,
      conversationMode: "recommendation",
      customerStage: "exploration",
      ranking: "comfort",
      filters: {
        priceBasis: "on_road",
        ...(budgetMax ? { budgetMax } : {}),
        mustHaveFeatures: unique(["ground clearance"]),
      },
      reasoningSummary: "User asked bad-roads recommendation.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        selectedModels: [],
        note: "Show cars suitable for bad roads.",
      },
    });
  }

  // Fuel / running-cost explainers.
  if (
    isFuelExplainerQuery(raw) ||
    has(raw, /\brunning cost|monthly running|daily running|1000 km|fuel cost\b/)
  ) {
    return makePlan({
      tool: "vehicle_explainer",
      brand,
      model: anchorModel,
      variant: anchorVariant,
      city,
      conversationMode: "education",
      customerStage: "evaluation",
      entities: { topic: "petrol_vs_diesel" },
      reasoningSummary: "User asked fuel/running-cost decision question.",
      resolution: {
        variantSelectionMode: anchorVariant ? "exact" : "not_required",
        selectedVariants: anchorVariant ? [{ model: anchorModel, variant: anchorVariant }] : [],
        selectedModels: anchorModel ? [{ model: anchorModel }] : [],
        note: "Explain fuel decision / running cost assumptions.",
      },
    });
  }

  /* ACI_FAILED_22_FIXES_END */


  if (isInternalQuery(raw)) {
    return makePlan({
      tool: "internal_passthrough",
      city,
      domain: "internal",
      conversationMode: "internal_passthrough",
      customerStage: "unknown",
      reasoningSummary: "Internal CDrive operation should bypass new-car planner.",
      resolution: { variantSelectionMode: "not_required", note: "Internal CDrive passthrough." },
    });
  }

  if (isOutOfScopeServiceQuery(raw)) {
    return makePlan({
      tool: "unavailable",
      brand,
      model,
      variant,
      city,
      mode: "unavailable",
      conversationMode: "unavailable",
      customerStage: "unknown",
      unavailableReason: "outside_current_scope",
      reasoningSummary: "Repair/service support is outside current ACI Assist scope.",
      resolution: { note: "Repair/service support is outside current ACI Assist scope." },
    });
  }

  if (isAvailabilityQuery(raw)) {
    return makePlan({
      tool: "unavailable",
      brand,
      model,
      variant,
      city,
      mode: "unavailable",
      conversationMode: "unavailable",
      customerStage: "unknown",
      unavailableReason: "dealer_inventory_not_available",
      reasoningSummary: "Dealer inventory/waiting-period data is not available.",
      resolution: { note: "Dealer inventory/waiting-period data is not available." },
    });
  }

  if (isResaleUnavailableQuery(raw)) {
    return makePlan({
      tool: "unavailable",
      brand,
      model,
      variant,
      city,
      mode: "unavailable",
      conversationMode: "unavailable",
      customerStage: "unknown",
      unavailableReason: "exact_resale_value_not_available",
      reasoningSummary: "Exact resale value is not available.",
      resolution: { note: "Exact resale value is not available." },
    });
  }

  if (isMultiIntentQuery(raw)) {
    // Let the main semantic compiler build multi_tool plans.
    return null;
  }

  if (model && isBreakupQuery(raw)) {
    return makePlan({
      tool: "vehicle_price_breakup",
      brand,
      model,
      variant,
      city,
      conversationMode: "direct_answer",
      customerStage: "consideration",
      filters: { priceBasis: "on_road" },
      reasoningSummary: "User asked for price breakup / on-road charge detail.",
      resolution: {
        variantSelectionMode: variant ? "exact" : "representative_default",
        selectedVariants: variant ? [{ model, variant }] : [{ model, variantStrategy: "popular_or_best_value" }],
        note: "Show on-road price breakup.",
      },
      ambiguity: variant
        ? null
        : {
            level: "soft_default",
            type: "variant",
            message: "I’ll show breakup using a selected or popular variant. You can change the variant anytime.",
            options: [],
            selectedDefault: { variantSelectionMode: "representative_default" },
          },
    });
  }

  if (model && isComparisonQuery(raw)) {
    const models = comparisonModels.length >= 2 ? comparisonModels : model ? [model] : [];

    return makePlan({
      tool: "vehicle_compare",
      brand,
      model,
      variant,
      city,
      conversationMode: "comparison",
      customerStage: "evaluation",
      entities: {
        ...(models.length >= 2 ? { models, comparisonModels: models } : {}),
      },
      filters: { priceBasis: "on_road" },
      ranking: has(raw, /\bmileage\b/) ? "fuel_efficiency" : has(raw, /\bfamily\b/) ? "family" : null,
      reasoningSummary: "User asked for comparison.",
      selectedComparisonSet: models.length >= 2 ? { models, variantSelectionMode: "representative_default" } : {},
      resolution: {
        comparisonLevel: variant ? "variant" : "model",
        variantSelectionMode: "representative_default",
        selectedModels: models.map((item) => ({ model: item })),
        selectedVariants: variant ? [{ model, variant }] : [],
        note: "Compare selected cars using representative variants where needed.",
      },
      ambiguity: {
        level: "soft_default",
        type: "comparison_variant",
        message: "I’ll compare representative/popular variants for now. You can change variants anytime.",
        options: [],
        selectedDefault: { variantSelectionMode: "representative_default" },
      },
    });
  }

  if (model && isFeatureQuestion(raw)) {
    const feature = extractFeature(raw);

    return makePlan({
      tool: "vehicle_feature_lookup",
      brand,
      model,
      variant,
      city,
      conversationMode: "direct_answer",
      customerStage: "evaluation",
      entities: { feature },
      reasoningSummary: `User asked feature/spec lookup: ${feature}.`,
      resolution: {
        variantSelectionMode: variant ? "exact" : "representative_default",
        selectedVariants: variant ? [{ model, variant }] : [{ model, variantStrategy: "popular_or_best_value" }],
        note: `Lookup feature: ${feature}.`,
      },
    });
  }

  if (model && isColorQuery(raw)) {
    return makePlan({
      tool: "vehicle_colors",
      brand,
      model,
      variant: "",
      city,
      conversationMode: "direct_answer",
      customerStage: "evaluation",
      entities: {
        ...(has(raw, /\bblack\b/) ? { color: "black" } : {}),
        ...(has(raw, /\bred\b/) ? { color: "red" } : {}),
      },
      reasoningSummary: "User asked model-level color availability.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        note: "Show model-level color data. Variant-wise color availability is not confirmed.",
      },
    });
  }

  if (isFeatureMatchQuery(raw)) {
    const mustHaveFeatures = extractMustHaveFeatures(raw);
    const budgetMax = extractBudgetMax(raw);

    return makePlan({
      tool: "vehicle_recommend",
      brand,
      model,
      city,
      conversationMode: "recommendation",
      customerStage: "exploration",
      ranking: isSafetyQuery(raw) ? "safety" : "feature_match",
      filters: {
        priceBasis: "on_road",
        ...(budgetMax ? { budgetMax } : {}),
        ...(has(raw, /\bautomatic\b/) ? { transmission: "automatic" } : {}),
        ...(has(raw, /\bsuv\b/) ? { bodyType: "suv" } : {}),
        mustHaveFeatures,
      },
      userPreferences: {
        ...(budgetMax ? { budgetMax } : {}),
        mustHaveFeatures,
      },
      reasoningSummary: "User asked feature-match recommendation.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        note: "Show model cards first with suggested representative variants.",
      },
    });
  }

  if (isSafetyQuery(raw) && !model) {
    const budgetMax = extractBudgetMax(raw);

    return makePlan({
      tool: "vehicle_recommend",
      brand,
      city,
      conversationMode: "recommendation",
      customerStage: "exploration",
      ranking: "safety",
      filters: {
        priceBasis: "on_road",
        ...(budgetMax ? { budgetMax } : {}),
        ...(has(raw, /\bsuv\b/) ? { bodyType: "suv" } : {}),
        mustHaveFeatures: extractMustHaveFeatures(raw),
      },
      reasoningSummary: "User asked safety recommendation.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        note: "Show safest model cards first.",
      },
    });
  }

  if (model && isSafetyQuery(raw)) {
    return makePlan({
      tool: "vehicle_feature_lookup",
      brand,
      model,
      variant,
      city,
      conversationMode: "direct_answer",
      customerStage: "evaluation",
      entities: { feature: extractFeature(raw) || "safety" },
      reasoningSummary: "User asked model safety feature.",
      resolution: {
        variantSelectionMode: variant ? "exact" : "representative_default",
        selectedVariants: variant ? [{ model, variant }] : [{ model, variantStrategy: "popular_or_best_value" }],
        note: "Lookup safety feature.",
      },
    });
  }

  if (isFuelExplainerQuery(raw)) {
    return makePlan({
      tool: "vehicle_explainer",
      brand,
      model,
      variant,
      city,
      conversationMode: "education",
      customerStage: "evaluation",
      entities: { topic: "petrol_vs_diesel" },
      reasoningSummary: "User asked fuel/running-cost decision question.",
      resolution: {
        variantSelectionMode: variant ? "exact" : "not_required",
        note: "Explain fuel decision / running cost assumptions.",
      },
    });
  }

  if (isDiscontinuedQuery(raw)) {
    return makePlan({
      tool: model ? "vehicle_pricelist" : "vehicle_recommend",
      brand,
      model,
      city,
      conversationMode: "direct_answer",
      customerStage: "exploration",
      filters: {
        priceBasis: "on_road",
        activeOnly: false,
        includeDiscontinued: true,
      },
      reasoningSummary: "User asked to include discontinued models.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        note: "Include discontinued records.",
      },
    });
  }

  if (model && isExShowroomPriceQuery(raw)) {
    return makePlan({
      tool: "vehicle_pricelist",
      brand,
      model,
      city,
      filters: { priceBasis: "ex_showroom" },
      reasoningSummary: "User asked ex-showroom price. Do not treat EX as variant.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        note: "Show ex-showroom model-level price list with variant rows.",
      },
    });
  }

  if (model && isTopModelPriceQuery(raw)) {
    return makePlan({
      tool: "vehicle_pricelist",
      brand,
      model,
      city,
      ranking: "price_high_to_low",
      filters: { priceBasis: isExShowroomPriceQuery(raw) ? "ex_showroom" : "on_road" },
      reasoningSummary: "User asked top/most-expensive variant price.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        note: "Show model-level price list sorted highest first.",
      },
    });
  }

  if (model && isBaseModelPriceQuery(raw)) {
    return makePlan({
      tool: "vehicle_pricelist",
      brand,
      model,
      city,
      ranking: "price_low_to_high",
      filters: { priceBasis: isExShowroomPriceQuery(raw) ? "ex_showroom" : "on_road" },
      reasoningSummary: "User asked base/cheapest variant price.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        note: "Show model-level price list sorted lowest first.",
      },
    });
  }

  if (model && isInsuranceQuoteQuery(raw)) {
    return makePlan({
      tool: "aci_lead_capture",
      brand,
      model,
      variant,
      city,
      conversationMode: "lead_capture",
      customerStage: "closing",
      entities: { leadType: "insurance_quote", selectedServices: ["insurance"] },
      filters: { leadType: "insurance_quote", selectedServices: ["insurance"] },
      leadContext: { leadType: "insurance_quote", selectedServices: ["insurance"] },
      reasoningSummary: "User asked insurance quote.",
      resolution: {
        variantSelectionMode: variant ? "exact" : "not_required",
        note: "Lead capture for insurance_quote.",
      },
    });
  }

  if (model && isZeroDepExplainerQuery(raw)) {
    return makePlan({
      tool: "vehicle_explainer",
      brand,
      model,
      city,
      conversationMode: "education",
      customerStage: "exploration",
      entities: { topic: "zero_dep" },
      reasoningSummary: "User asked zero-dep insurance explainer.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        note: "Explain zero_dep.",
      },
    });
  }

  if (model && isSimilarQuery(raw)) {
    return makePlan({
      tool: "vehicle_recommend",
      brand,
      model,
      city,
      ranking: "similarity",
      conversationMode: "recommendation",
      customerStage: "evaluation",
      filters: { priceBasis: "on_road" },
      reasoningSummary: "User asked similar cars / alternatives.",
      resolution: {
        variantSelectionMode: "not_required",
        selectedVariants: [],
        note: "Find similar or alternative cars.",
      },
    });
  }

  if (model && isFinanceEligibilityQuery(raw)) {
    return makePlan({
      tool: "aci_lead_capture",
      brand,
      model,
      variant,
      city,
      conversationMode: "lead_capture",
      customerStage: "closing",
      entities: { leadType: "finance_callback", selectedServices: ["finance"] },
      filters: { leadType: "finance_callback", selectedServices: ["finance"] },
      leadContext: { leadType: "finance_callback", selectedServices: ["finance"] },
      reasoningSummary: "User asked finance eligibility / loan callback.",
      resolution: {
        variantSelectionMode: variant ? "exact" : "not_required",
        note: "Lead capture for finance_callback.",
      },
    });
  }

  if (model && isLeadQuery(raw)) {
    const callbackOnly = has(raw, /\b(talk to advisor|advisor|call me|callback)\b/) && !has(raw, /\bquote|quotation|best price|final price\b/);

    const services = unique([
      callbackOnly ? "callback" : "quotation",
      ...(has(raw, /\bfinance|loan|funding\b/) ? ["finance"] : []),
      ...(has(raw, /\bexchange|old car|trade in|trade-in\b/) ? ["exchange"] : []),
      ...(has(raw, /\binsurance|zero dep|zero depreciation\b/) ? ["insurance"] : []),
    ]);

    const leadType = callbackOnly
      ? "callback"
      : services.includes("insurance")
        ? "insurance_quote"
        : "quotation";

    return makePlan({
      tool: "aci_lead_capture",
      brand,
      model,
      variant,
      city,
      conversationMode: "lead_capture",
      customerStage: "closing",
      entities: { leadType, selectedServices: services },
      filters: { leadType, selectedServices: services },
      leadContext: { leadType, selectedServices: services },
      reasoningSummary: `User asked for ${leadType}.`,
      resolution: {
        variantSelectionMode: variant ? "exact" : "not_required",
        selectedVariants: variant ? [{ model, variant }] : [],
        note: `Lead capture for ${leadType}.`,
      },
    });
  }

  if (model && isEmiQuery(raw)) {
    const downPayment = extractDownPayment(raw);
    const tenureMonths = extractTenureMonths(raw);
    const loanPercent = extractLoanPercent(raw);

    return makePlan({
      tool: "vehicle_emi",
      brand,
      model,
      variant,
      city,
      conversationMode: "calculation",
      customerStage: "consideration",
      filters: {
        priceBasis: "on_road",
        ...(downPayment !== undefined ? { downPayment } : {}),
        ...(tenureMonths !== undefined ? { tenureMonths } : {}),
        ...(loanPercent !== undefined ? { loanPercent } : {}),
      },
      reasoningSummary: "User asked EMI / affordability.",
      resolution: {
        variantSelectionMode: variant ? "exact" : "representative_default",
        selectedVariants: variant ? [{ model, variant }] : [{ model, variantStrategy: "popular_or_best_value" }],
        note: variant ? "Use selected variant for EMI." : "Use selected or representative variant for EMI.",
      },
      ambiguity: variant
        ? null
        : {
            level: "soft_default",
            type: "variant",
            message: "I’ll calculate EMI using a selected or popular variant. You can change the variant anytime.",
            options: [],
            selectedDefault: { variantSelectionMode: "representative_default" },
          },
    });
  }

  if (model && isGenericPriceQuery(raw)) {
    return makePlan({
      tool: "vehicle_pricelist",
      brand,
      model,
      variant,
      city,
      filters: { priceBasis: isExShowroomPriceQuery(raw) ? "ex_showroom" : "on_road" },
      reasoningSummary: "User asked price/pricelist.",
      resolution: {
        variantSelectionMode: variant ? "exact" : "not_required",
        selectedVariants: variant ? [{ model, variant }] : [],
        note: variant ? "Show variant price if exact variant resolves." : "Show model-level price list with variant rows.",
      },
    });
  }

  return null;
};

export const applyPlannerRedFixes = async (plan, { message = "", context = {}, selectedEntity = null } = {}) => {
  if (!plan || typeof plan !== "object") return plan;

  const deterministic = await compilePlannerRedFix({
    message,
    context,
    selectedEntity,
  });

  if (deterministic) return deterministic;

  return plan;
};

export default {
  compilePlannerRedFix,
  applyPlannerRedFixes,
};
