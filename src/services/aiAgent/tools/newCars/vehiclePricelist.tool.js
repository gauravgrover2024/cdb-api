import mongoose from "mongoose";
import { formatMoney } from "../shared/pricing.js";
import { buildV2PriceBreakup } from "./shared/priceBreakup.js";

const DEFAULT_CITY = "new-delhi";
const VEHICLE_COLORS_COLLECTION = "vehicle_colors_v2";
const ACI_MODEL_SUMMARY_COLLECTION = "aci_vehicle_model_summary";
const ACI_PRICE_ROWS_COLLECTION = "aci_vehicle_price_rows";

const asArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value];
};

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const cleanVehicleText = (value = "") =>
  cleanText(
    String(value || "")
      .replace(/[-_]+/g, " ")
      .replace(/\bn\s+line\b/gi, "N Line"),
  );

const titleCase = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());

const normalizeKey = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const slugify = (value = "", fallback = "item") =>
  normalizeKey(value).replace(/\s+/g, "-") || fallback;

const stripMakeFromModel = (model = "", make = "") => {
  const cleanModel = cleanText(model);
  const cleanMake = cleanText(make);

  if (!cleanModel || !cleanMake) return cleanModel;

  const modelKey = normalizeKey(cleanModel);
  const makeKey = normalizeKey(cleanMake);

  if (!modelKey.startsWith(`${makeKey} `)) return cleanModel;

  return (
    cleanText(cleanModel.replace(new RegExp(`^${cleanMake}\\s+`, "i"), "")) ||
    cleanModel
  );
};

const buildVehicleDisplayName = (make = "", model = "") => {
  const cleanMake = cleanText(make);
  const cleanModel = stripMakeFromModel(model, cleanMake);

  return (
    cleanText([cleanMake, cleanModel].filter(Boolean).join(" ")) ||
    cleanModel ||
    cleanMake
  );
};

const stripNormalizedPrefix = (value = "", prefix = "") => {
  const cleanValue = cleanVehicleText(value);
  const cleanPrefix = cleanVehicleText(prefix);
  const valueKey = normalizeKey(cleanValue);
  const prefixKey = normalizeKey(cleanPrefix);

  if (!cleanValue || !cleanPrefix || !prefixKey) return cleanValue;
  if (valueKey === prefixKey) return "";
  if (!valueKey.startsWith(`${prefixKey} `)) return cleanValue;

  const prefixPattern = escapeRegex(cleanPrefix).replace(/[-_\s]+/g, "[-_\\s]+");
  return cleanVehicleText(
    cleanValue.replace(new RegExp(`^${prefixPattern}\\s+`, "i"), ""),
  );
};

const stripVehicleNameFromVariant = (
  variant = "",
  { make = "", model = "", displayName = "" } = {},
) => {
  let out = cleanText(variant);
  if (!out) return out;

  const tokens = [
    displayName,
    buildVehicleDisplayName(make, model),
    `${make} ${model}`,
    model,
    make,
  ]
    .map(cleanText)
    .filter(Boolean)
    .sort((a, b) => b.length - a.length);

  for (const token of tokens) {
    const tokenKey = normalizeKey(token);
    const outKey = normalizeKey(out);

    if (tokenKey && outKey === tokenKey) return "";

    if (tokenKey && outKey.startsWith(`${tokenKey} `)) {
      out = stripNormalizedPrefix(out, token);
    }
  }

  return out;
};

const isGenericVariantCandidate = (
  value = "",
  { requestedModel = "" } = {},
) => {
  const key = normalizeKey(value);
  const modelKey = normalizeKey(requestedModel);

  if (!key) return true;
  if (modelKey && key === modelKey) return true;

  return /\b(price|prices|pricing|pricelist|price list|rate list|on road|onroad|on-road|ex showroom|exshowroom|ex-showroom|new car|variant|variants|show|list)\b/.test(
    key,
  );
};

const REQUEST_NOISE_PATTERN =
  /\b(show|find|get|open|please|pls|tell me|price list|pricelist|prices|price|pricing|rate list|on road|onroad|on-road|ex showroom|exshowroom|ex-showroom|colors|colours|color|colour|variants|variant|new car|list|for|in|new delhi|delhi)\b/gi;

const sanitizeRequestedModelText = (value = "", { requestedMake = "" } = {}) => {
  let out = cleanVehicleText(value).replace(REQUEST_NOISE_PATTERN, " ");
  out = cleanVehicleText(out);

  if (requestedMake) {
    out = stripMakeFromModel(out, requestedMake);
  }

  return cleanVehicleText(out);
};

const sanitizeRequestedVariant = (
  value = "",
  {
    requestedModel = "",
    requestedMake = "",
    requestedDisplayName = "",
  } = {},
) => {
  let text = cleanVehicleText(value);
  if (!text) return "";

  const modelWithoutMake = cleanVehicleText(
    requestedMake
      ? stripMakeFromModel(requestedModel, requestedMake)
      : requestedModel,
  );

  const prefixes = [
    requestedDisplayName,
    requestedModel,
    requestedMake && modelWithoutMake ? `${requestedMake} ${modelWithoutMake}` : "",
    modelWithoutMake,
    requestedMake,
  ]
    .map((item) => cleanVehicleText(item))
    .filter(Boolean)
    .sort((a, b) => b.length - a.length);

  const seenPrefixes = new Set();

  for (const prefix of prefixes) {
    if (!prefix || seenPrefixes.has(prefix)) continue;
    seenPrefixes.add(prefix);

    // If the candidate is only the model/make text, it is not a variant.
    if (normalizeKey(text) === normalizeKey(prefix)) return "";

    // Generic prefix strip:
    // "<make> <model> <variant>" -> "<variant>"
    // "<model> <variant>" -> "<variant>"
    const stripped = stripNormalizedPrefix(text, prefix);
    if (stripped !== text) {
      text = stripped;
      break;
    }
  }

  return cleanVehicleText(text);
};

const amount = (...values) => {
  for (const value of values) {
    if (value === null || value === undefined || value === "") continue;

    if (typeof value === "number" && Number.isFinite(value)) {
      return Math.round(value);
    }

    const text = String(value || "")
      .replace(/,/g, "")
      .trim();
    const match = text.match(/-?\d+(?:\.\d+)?/);
    if (!match) continue;

    const number = Number(match[0]);
    if (!Number.isFinite(number)) continue;

    if (/\b(cr|crore|crores)\b/i.test(text)) {
      return Math.round(number * 10000000);
    }

    if (/\b(lakh|lac|lacs|lakhs)\b/i.test(text)) {
      return Math.round(number * 100000);
    }

    return Math.round(number);
  }

  return 0;
};

const first = (...values) =>
  values.find(
    (value) => value !== undefined && value !== null && value !== "",
  ) || "";

const getEntities = (toolPlan = {}) => ({
  ...(toolPlan.entities || {}),
  ...(toolPlan.input?.entities || {}),
});

const getFilters = (toolPlan = {}) => ({
  ...(toolPlan.filters || {}),
  ...(toolPlan.input?.filters || {}),
});

const getRequestedModel = ({
  toolPlan = {},
  context = {},
  userMessage = "",
} = {}) => {
  const entities = getEntities(toolPlan);
  const filters = getFilters(toolPlan);
  const selectedVehicle = context.selectedVehicle || {};
  const requestedMake = getRequestedMake({ toolPlan, context });

  return sanitizeRequestedModelText(
    first(
      entities.model,
      entities.models?.[0],
      filters.model,
      toolPlan.model,
      toolPlan.input?.model,
      selectedVehicle.model,
      context.anchorModel,
      context.model,
      userMessage,
    ),
    { requestedMake },
  );
};

const getRequestedMake = ({ toolPlan = {}, context = {} } = {}) => {
  const entities = getEntities(toolPlan);
  const selectedVehicle = context.selectedVehicle || {};

  return cleanText(
    first(
      entities.make,
      entities.brand,
      toolPlan.make,
      toolPlan.brand,
      selectedVehicle.make,
      selectedVehicle.brand,
      context.anchorMake,
      context.make,
      context.brand,
    ),
  );
};

const getRequestedVariant = ({ toolPlan = {}, context = {} } = {}) => {
  const entities = getEntities(toolPlan);
  const filters = getFilters(toolPlan);
  const selectedVehicle = context.selectedVehicle || {};

  return cleanText(
    first(
      entities.variant,
      filters.variant,
      toolPlan.variant,
      toolPlan.input?.variant,
      selectedVehicle.variant,
      selectedVehicle.selectedVariant,
      context.anchorVariant,
      context.variant,
    ),
  );
};

const getRequestedCity = ({ toolPlan = {}, context = {} } = {}) => {
  const entities = getEntities(toolPlan);
  const filters = getFilters(toolPlan);
  const selectedVehicle = context.selectedVehicle || {};

  return cleanText(
    first(
      filters.city,
      entities.city,
      toolPlan.city,
      selectedVehicle.city,
      context.anchorCity,
      context.city,
      DEFAULT_CITY,
    ),
  );
};

const displayCity = (city = DEFAULT_CITY) =>
  titleCase(String(city || DEFAULT_CITY).replace(/[-_]+/g, " "));

const normalizeFuel = (row = {}) =>
  cleanText(
    first(
      row.fuel,
      row.fuelType,
      row.fuel_type,
      row.raw?.fuel,
      row.raw?.fuelType,
      row.raw?.fuel_type,
    ),
  );

const normalizeTransmission = (row = {}) =>
  cleanText(
    first(
      row.transmission,
      row.transmissionType,
      row.transmission_type,
      row.raw?.transmission,
      row.raw?.transmissionType,
      row.raw?.transmission_type,
    ),
  );

const normalizeModel = (row = {}) => {
  const rawModel = cleanText(
    first(
      row.modelNormalized,
      row.model_normalized,
      row.raw?.model_normalized,
      row.raw?.modelNormalized,
      row.modelName,
      row.model_name,
      row.raw?.modelName,
      row.raw?.model_name,
      row.model,
      row.raw?.model,
    ),
  );

  const make = normalizeMake(row);

  return stripMakeFromModel(rawModel, make);
};

const normalizeRawModel = (row = {}) =>
  cleanText(
    first(row.model, row.raw?.model, row.modelName, row.raw?.modelName),
  );

const normalizeMake = (row = {}) =>
  cleanText(
    first(
      row.make,
      row.brand,
      row.brandName,
      row.raw?.make,
      row.raw?.brand,
      row.raw?.brandName,
    ),
  );

const normalizeVariant = (row = {}) =>
  cleanText(
    first(
      row.variant,
      row.variantName,
      row.variant_name,
      row.variantShort,
      row.variant_short,
      row.variantNormalized,
      row.variant_normalized,
      row.raw?.variant,
      row.raw?.variantName,
      row.raw?.variant_name,
      row.raw?.variant_short,
      row.raw?.variant_normalized,
    ),
  );

const normalizeImageUrl = (row = {}) =>
  cleanText(
    first(
      row.imageUrl,
      row.image_url,
      row.heroImageUrl,
      row.vehicleImageUrl,
      row.normalizedImageUrl,
      row.cleanImageUrl,
      row.raw?.imageUrl,
      row.raw?.image_url,
      row.raw?.heroImageUrl,
      row.raw?.vehicleImageUrl,
      row.raw?.normalizedImageUrl,
      row.raw?.cleanImageUrl,
    ),
  );

const normalizeOtherItems = (row = {}) => {
  const raw = row.raw || row;

  const items = [
    ...asArray(row.otherChargeItems),
    ...asArray(row.otherItems),
    ...asArray(row.other_list),
    ...asArray(raw.otherChargeItems),
    ...asArray(raw.otherItems),
    ...asArray(raw.other_list),
    ...asArray(raw.optional_list),
  ];

  return items
    .map((item, index) => {
      const label = cleanText(
        first(
          item.label,
          item.text,
          item.name,
          item.key,
          `Other charge ${index + 1}`,
        ),
      );

      const value = amount(item.amount, item.value, item.price);

      return {
        key: slugify(`${label}-${index}`, `other-${index + 1}`),
        label,
        amount: value,
        value,
        displayValue: value ? `₹${value.toLocaleString("en-IN")}` : "",
      };
    })
    .filter((item) => item.label && item.value > 0);
};

const getExplicitRequestedMake = ({ toolPlan = {} } = {}) => {
  const entities = getEntities(toolPlan);

  return cleanVehicleText(
    first(
      entities.make,
      entities.brand,
      entities.manufacturer,
      toolPlan.make,
      toolPlan.brand,
      toolPlan.input?.make,
      toolPlan.input?.brand,
    ),
  );
};

const resolveContextSafeRequestedMake = ({
  rawRequestedMake = "",
  requestedModel = "",
  toolPlan = {},
  context = {},
} = {}) => {
  const explicitMake = getExplicitRequestedMake({ toolPlan });
  if (explicitMake) return explicitMake;

  const selectedVehicle = context.selectedVehicle || {};
  const contextModel = cleanVehicleText(
    first(selectedVehicle.model, context.anchorModel, context.model),
  );

  const requestedModelKey = normalizeKey(requestedModel);
  const contextModelKey = normalizeKey(contextModel);

  // Do not carry old selectedVehicle.make into a different model query.
  if (
    requestedModelKey &&
    contextModelKey &&
    requestedModelKey !== contextModelKey
  ) {
    return "";
  }

  return cleanVehicleText(rawRequestedMake);
};

const normalizePriceRow = (row = {}, index = 0) => {
  const raw = row.raw || row;

  const make = normalizeMake(row);
  const rawModel = cleanText(
    first(
      row.model,
      raw.model,
      row.modelName,
      raw.modelName,
      row.model_name,
      raw.model_name,
    ),
  );

  const model = stripMakeFromModel(normalizeModel(row), make);
  const displayName =
    cleanText([make, model].filter(Boolean).join(" ")) ||
    stripMakeFromModel(rawModel, make) ||
    rawModel;

  const variant = stripVehicleNameFromVariant(normalizeVariant(row), {
    make,
    model,
    displayName,
  });
  const fuel = normalizeFuel(row);
  const transmission = normalizeTransmission(row);
  const imageUrl = normalizeImageUrl(row);

  const breakup = buildV2PriceBreakup(row);

  const exShowroomPrice = breakup.exShowroom;
  const rto = breakup.rto;
  const insurance = breakup.insurance;
  const otherCharges = breakup.otherCharges;
  const onRoadPrice = breakup.onRoadPrice;
  const onRoadPriceWithoutOptional = breakup.onRoadPriceWithoutOptional;
  const onRoadPriceWithOptional = breakup.onRoadPriceWithOptional;
  const mandatoryChargesTotal = breakup.mandatoryChargesTotal;
  const optionalChargesTotal = breakup.optionalTotal;

  const id = cleanText(
    first(
      row.id,
      row._id,
      raw.id,
      raw._id,
      `${slugify(make || "brand")}-${slugify(model || "model")}-${slugify(variant || `variant-${index + 1}`)}`,
    ),
  );

  return {
    id,
    make,
    brand: make,

    // Clean canonical model for context / filters.
    model,

    // Original DB model retained for trace/debug.
    rawModel,

    // Customer-facing full name.
    displayName,
    modelDisplayName: displayName,

    variant,
    variantName: variant,

    fuel,
    fuelType: fuel,
    transmission,

    exShowroomPrice,
    exShowroomPriceValue: exShowroomPrice,
    exShowroomPriceLabel: formatMoney(exShowroomPrice),

    rto,
    rtoCharges: rto,

    insurance,

    // Visible frontend amount: optional_list + other_list.
    otherCharges,
    otherChargesTotal: otherCharges,

    // Separate detail totals.
    optionalCharges: breakup.optionalTotal,
    optionalChargeItems: breakup.optionalItems,
    otherListCharges: breakup.otherTotal,
    otherListItems: breakup.otherItems,

    // Combined individual items for the info hover.
    otherChargeItems: breakup.otherChargeItems,

    onRoadPrice,
    onRoadPriceLabel: formatMoney(onRoadPrice),
    onRoadPriceWithoutOptional,
    onRoadPriceWithoutOptionalLabel: formatMoney(onRoadPriceWithoutOptional),
    onRoadPriceWithOptional,
    onRoadPriceWithOptionalLabel: formatMoney(onRoadPriceWithOptional),
    mandatoryChargesTotal,
    mandatoryChargesTotalLabel: formatMoney(mandatoryChargesTotal),
    optionalChargesTotal,
    optionalChargesTotalLabel: formatMoney(optionalChargesTotal),

    computedOnRoadPrice: breakup.computedOnRoadPrice,
    canonicalOnRoadPrice: breakup.canonicalOnRoadPrice,
    priceIntegrity: breakup.priceIntegrity,

    priceBreakup: {
      ...breakup.contract,
      legacyExShowroom: breakup.exShowroom,
      legacyRto: breakup.rto,
      legacyInsurance: breakup.insurance,
      legacyOtherCharges: breakup.otherCharges,
      optionalTotal: breakup.optionalTotal,
      otherTotal: breakup.otherTotal,
      legacyOnRoadPrice: breakup.onRoadPrice,
      onRoadPriceWithoutOptional: breakup.onRoadPriceWithoutOptional,
      onRoadPriceWithOptional: breakup.onRoadPriceWithOptional,
      mandatoryChargesTotal: breakup.mandatoryChargesTotal,
      optionalChargesTotal: breakup.optionalTotal,
      computedOnRoadPrice: breakup.computedOnRoadPrice,
      canonicalOnRoadPrice: breakup.canonicalOnRoadPrice,
      visibleLines: breakup.visibleLines,
      detailSections: breakup.detailSections,
      otherChargeItems: breakup.otherChargeItems,
      optionalItems: breakup.optionalItems,
      otherItems: breakup.otherItems,
      hasOtherChargeDetails: breakup.otherChargeItems.length > 0,
      warnings: breakup.priceIntegrity?.warnings || [],
      priceIntegrity: breakup.priceIntegrity,
    },

    priceBreakupLines: breakup.visibleLines,

    otherChargesTooltip: {
      title: "Other charges",
      amount: otherCharges,
      displayValue: formatMoney(otherCharges),
      sections: breakup.detailSections,
      items: breakup.otherChargeItems,
    },

    price: onRoadPrice || exShowroomPrice,
    priceLabel: formatMoney(onRoadPrice || exShowroomPrice),

    imageUrl,
    recommended: Boolean(row.recommended || raw.recommended),
    raw,
  };
};

const uniqueRows = (rows = []) => {
  const seen = new Set();
  const out = [];

  for (const row of rows) {
    const key = normalizeKey(
      [
        row.make,
        row.model,
        row.variant,
        row.fuel,
        row.transmission,
        row.exShowroomPrice,
        row.onRoadPrice,
      ].join(" "),
    );

    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }

  return out;
};

const uniqueCleanStrings = (values = []) => {
  const seen = new Set();
  const out = [];

  for (const value of values) {
    const clean = cleanText(value);
    const key = normalizeKey(clean);

    if (!clean || !key || seen.has(key)) continue;
    seen.add(key);
    out.push(clean);
  }

  return out;
};

const uniqueExactCleanStrings = (values = []) => {
  const seen = new Set();
  const out = [];

  for (const value of values) {
    const clean = cleanText(value);

    if (!clean || seen.has(clean)) continue;
    seen.add(clean);
    out.push(clean);
  }

  return out;
};

const explicitNLineRequest = (value = "") =>
  /\bn\s*[- ]?\s*line\b/i.test(String(value || ""));

const isNLineModel = (value = "") => explicitNLineRequest(value);

const exactModelFilter = ({
  rows = [],
  requestedModel = "",
  userMessage = "",
} = {}) => {
  const modelKey = normalizeKey(requestedModel);
  const messageWantsNLine = explicitNLineRequest(
    `${requestedModel} ${userMessage}`,
  );

  if (!modelKey) return rows;

  // Soft ambiguity rule: a base model request should not drift into a
  // special-line model unless the user explicitly asked for that line.
  if (!messageWantsNLine) {
    return rows.filter((row) => !isNLineModel(row.model));
  }

  if (messageWantsNLine) {
    return rows.filter(
      (row) => isNLineModel(row.model) || isNLineModel(row.variant),
    );
  }

  return rows;
};

const buildVehicle = ({
  rows = [],
  requestedMake = "",
  requestedModel = "",
  city = "",
} = {}) => {
  const firstRow = rows[0] || {};
  const make = cleanText(first(requestedMake, firstRow.make, firstRow.brand));
  const model = stripMakeFromModel(
    cleanText(first(firstRow.model, requestedModel)),
    make,
  );
  const displayName = buildVehicleDisplayName(make, model || requestedModel);

  const prices = rows
    .map((row) => row.exShowroomPrice || row.onRoadPrice || 0)
    .filter((value) => Number(value || 0) > 0);

  const minPrice = prices.length ? Math.min(...prices) : 0;
  const maxPrice = prices.length ? Math.max(...prices) : 0;

  return {
    id: slugify(displayName || model || "vehicle"),
    make,
    brand: make,
    model,
    displayName,
    city: displayCity(city),
    citySlug: slugify(city || DEFAULT_CITY),
    imageUrl: firstRow.imageUrl || firstRow.normalizedImageUrl || "",
    normalizedImageUrl: firstRow.normalizedImageUrl || firstRow.imageUrl || "",
    imageFrame: firstRow.imageFrame || firstRow.vehicle?.imageFrame || null,
    selectedColor:
      firstRow.selectedColor || firstRow.vehicle?.selectedColor || null,
    visualGallery:
      firstRow.visualGallery || firstRow.vehicle?.visualGallery || [],
    variantCount: rows.length,
    priceRange:
      minPrice && maxPrice
        ? `${formatMoney(minPrice)} – ${formatMoney(maxPrice)}`
        : "",
    exShowroomPrice: minPrice ? formatMoney(minPrice) : "",
    startingOnRoadPrice: rows[0]?.onRoadPrice
      ? formatMoney(rows[0].onRoadPrice)
      : "",
    fuelText: [...new Set(rows.map((row) => row.fuel).filter(Boolean))].join(
      " / ",
    ),
    transmissionText: [
      ...new Set(rows.map((row) => row.transmission).filter(Boolean)),
    ].join(" / "),
  };
};

const makeAction = ({
  id,
  label,
  query,
  canvasType,
  intent = "vehicle_pricelist",
  vehicle,
  variant = "",
  contextPatch = {},
}) => ({
  id,
  label,
  title: label,
  query,
  intent,
  canvasType,
  vehicle,
  contextPatch: {
    selectedVehicle: vehicle,
    anchorMake: vehicle?.make || vehicle?.brand,
    anchorModel: vehicle?.model,
    anchorVariant: variant,
    anchorCity: vehicle?.city || "New Delhi",
    ...contextPatch,
  },
});

const buildActions = ({
  vehicle,
  requestedModel = "",
  requestedVariant = "",
  softAlternatives = [],
} = {}) => {
  const modelLabel = vehicle?.displayName || requestedModel || "this car";
  const variantText = requestedVariant ? ` ${requestedVariant}` : "";

  const actions = [
    makeAction({
      id: "calculate-emi",
      label: "Calculate EMI",
      query: `Calculate EMI for ${modelLabel}${variantText}`,
      intent: "vehicle_emi",
      canvasType: "emi_calculator_canvas",
      vehicle,
      variant: requestedVariant,
    }),
    makeAction({
      id: "get-quotation",
      label: "Get quotation",
      query: `Get quotation for ${modelLabel}${variantText}`,
      intent: "aci_lead_capture",
      canvasType: "aci_quotation_canvas",
      vehicle,
      variant: requestedVariant,
    }),
    makeAction({
      id: "show-colors",
      label: "Show colors",
      query: `Show colors of ${modelLabel}`,
      intent: "vehicle_colors",
      canvasType: "color_studio_canvas",
      vehicle,
    }),
  ];

  for (const alternative of softAlternatives) {
    actions.unshift(
      makeAction({
        id: `switch-${slugify(alternative.model || alternative.label)}`,
        label: `Switch to ${alternative.label || alternative.model}`,
        query: `Show ${alternative.label || alternative.model} price list`,
        intent: "vehicle_pricelist",
        canvasType: "pricelist_canvas",
        vehicle: {
          ...vehicle,
          model: alternative.model,
          displayName: buildVehicleDisplayName(
            alternative.make || vehicle?.make,
            alternative.model,
          ),
        },
        contextPatch: {
          anchorModel: alternative.model,
          exactModelOnly: true,
        },
      }),
    );
  }

  return actions;
};

const buildLeadingQuestions = ({ vehicle, requestedVariant = "" } = {}) => {
  const modelLabel = vehicle?.displayName || vehicle?.model || "this car";

  return [
    {
      id: "best-value-variant",
      label: "Which variant is best value?",
      query: `Which ${modelLabel} variant is best value?`,
      intent: "vehicle_variant_advisor",
      canvasType: "variant_advisor_canvas",
      contextPatch: {
        selectedVehicle: vehicle,
        anchorModel: vehicle?.model,
        anchorCity: vehicle?.city,
      },
    },
    {
      id: "automatic-variants",
      label: "Show automatic variants",
      query: `Show automatic variants of ${modelLabel}`,
      intent: "vehicle_pricelist",
      canvasType: "pricelist_canvas",
      contextPatch: {
        selectedVehicle: vehicle,
        anchorModel: vehicle?.model,
        anchorCity: vehicle?.city,
      },
    },
    {
      id: "price-breakup",
      label: requestedVariant
        ? "Show price breakup"
        : "Explain on-road charges",
      query: requestedVariant
        ? `Show on-road breakup of ${modelLabel} ${requestedVariant}`
        : `Explain on-road charges of ${modelLabel}`,
      intent: "vehicle_price_breakup",
      canvasType: "price_breakup_canvas",
      contextPatch: {
        selectedVehicle: vehicle,
        anchorModel: vehicle?.model,
        anchorVariant: requestedVariant,
        anchorCity: vehicle?.city,
      },
    },
  ];
};

const buildSoftAlternatives = ({
  requestedModel = "",
  requestedMake = "",
  userMessage = "",
} = {}) => {
  return [];
};

const escapeRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const looseWordsRegex = (value = "") => {
  const clean = cleanText(value);
  if (!clean) return null;

  const pattern = escapeRegex(clean).replace(/[-_\s]+/g, "[-_\\s]+");
  return new RegExp(pattern, "i");
};

const buildCityRegex = (city = "") => {
  const clean = cleanText(city || DEFAULT_CITY);
  const normal = clean.replace(/[-_]+/g, " ");
  const pattern = escapeRegex(normal).replace(/\s+/g, "[-_\\s]+");
  return new RegExp(pattern, "i");
};

const buildModelRegex = (model = "", userMessage = "") => {
  const cleanModel = cleanVehicleText(model);
  const wantsNLine = explicitNLineRequest(`${cleanModel} ${userMessage}`);
  const baseModel = cleanVehicleText(
    cleanModel.replace(/\bn\s*[- ]?\s*line\b/i, ""),
  );

  if (wantsNLine && baseModel) {
    return new RegExp(
      `${escapeRegex(baseModel)}.*n\\s*[- ]?\\s*line|n\\s*[- ]?\\s*line.*${escapeRegex(baseModel)}`,
      "i",
    );
  }

  return looseWordsRegex(baseModel || cleanModel);
};

const fetchVehiclePricelistRowsFromVehicles = async ({
  requestedMake = "",
  requestedModel = "",
  requestedVariant = "",
  requestedCity = DEFAULT_CITY,
  userMessage = "",
  limit = 240,
} = {}) => {
  if (!mongoose.connection?.db) {
    return {
      rows: [],
      records: [],
      variants: [],
      count: 0,
      matched: 0,
      source: "vehicles",
      dataSource: "vehicles",
      modulesChecked: ["vehicles:not_connected"],
    };
  }

  const collection = mongoose.connection.db.collection("vehicles");

  const modelRegex = buildModelRegex(requestedModel, userMessage);
  const makeRegex = looseWordsRegex(requestedMake);
  const cityRegex = buildCityRegex(requestedCity);
  const variantRegex = looseWordsRegex(requestedVariant);

  const modelOr = modelRegex
    ? [
        { model: modelRegex },
        { modelName: modelRegex },
        { model_name: modelRegex },
        { model_normalized: modelRegex },
        { modelNormalized: modelRegex },
      ]
    : [];

  const makeOr = makeRegex
    ? [{ make: makeRegex }, { brand: makeRegex }, { brandName: makeRegex }]
    : [];

  const cityOr = cityRegex
    ? [
        { city: cityRegex },
        { cityName: cityRegex },
        { city_name: cityRegex },
        { citySlug: cityRegex },
        { city_slug: cityRegex },
      ]
    : [];

  const variantOr = variantRegex
    ? [
        { variant: variantRegex },
        { variantName: variantRegex },
        { variant_name: variantRegex },
        { variant_normalized: variantRegex },
      ]
    : [];

  const withCity = {
    $and: [
      modelOr.length ? { $or: modelOr } : {},
      makeOr.length ? { $or: makeOr } : {},
      cityOr.length ? { $or: cityOr } : {},
      variantOr.length ? { $or: variantOr } : {},
    ].filter((item) => Object.keys(item).length),
  };

  const withoutCity = {
    $and: [
      modelOr.length ? { $or: modelOr } : {},
      makeOr.length ? { $or: makeOr } : {},
      variantOr.length ? { $or: variantOr } : {},
    ].filter((item) => Object.keys(item).length),
  };

  const queries = [withCity, withoutCity].filter((query) => query.$and?.length);

  let rows = [];
  let queryUsed = null;

  for (const query of queries) {
    rows = await collection.find(query).limit(limit).toArray();

    if (rows.length) {
      queryUsed = query;
      break;
    }
  }

  return {
    rows,
    records: rows,
    variants: rows,
    count: rows.length,
    matched: rows.length,
    source: "vehicles",
    dataSource: "vehicles",
    modulesChecked: ["vehicles"],
    queryUsed,
    summary: {
      rowCount: rows.length,
      collection: "vehicles",
    },
  };
};


const buildReadModelVisual = (summary = {}) => {
  const hero = summary?.hero || {};
  const imageUrl = hero.imageUrl || hero.normalizedImageUrl || "";

  if (!imageUrl && !hero.imageFrame) return null;

  return {
    id: `${summary.makeKey || summary.make || "aci"}-${summary.modelKey || summary.model || "model"}-display`,
    make: summary.make || "",
    brand: summary.make || "",
    model: summary.model || "",
    rawModel: summary.model || "",
    modelKey: summary.modelKey || slugify(summary.model || ""),
    colorName: hero.colorName || "Display",
    name: hero.colorName || "Display",
    hex: "",
    imageUrl,
    normalizedImageUrl: hero.normalizedImageUrl || imageUrl,
    imageFrame: hero.imageFrame || null,
    sourceImageUrl: hero.sourceImageUrl || "",
  };
};

const buildReadModelVehicleFromSummary = (summary = {}, visual = null) => ({
  id: `${summary.makeKey || slugify(summary.make || "")}-${summary.modelKey || slugify(summary.model || "")}`,
  make: summary.make || "",
  brand: summary.make || "",
  model: summary.model || "",
  fullModel: summary.fullModel || summary.displayName || buildVehicleDisplayName(summary.make, summary.model),
  displayName: summary.displayName || summary.fullModel || buildVehicleDisplayName(summary.make, summary.model),
  city: summary.city || displayCity(summary.citySlug || DEFAULT_CITY),
  citySlug: summary.citySlug || DEFAULT_CITY,
  imageUrl: visual?.imageUrl || "",
  normalizedImageUrl: visual?.normalizedImageUrl || visual?.imageUrl || "",
  imageFrame: visual?.imageFrame || null,
  selectedColor: visual || null,
  visualGallery: visual ? [visual] : [],
  variantCount: Number(summary.variantCount || 0),
  priceRange: summary.priceRangeLabel || "",
  exShowroomPrice: summary.minExShowroomPrice ? formatMoney(summary.minExShowroomPrice) : "",
  startingOnRoadPrice: summary.minOnRoadPrice ? formatMoney(summary.minOnRoadPrice) : "",
  fuelText: summary.fuelText || "",
  transmissionText: summary.transmissionText || "",
  gearboxText: summary.gearboxText || "",
  colorName: visual?.colorName || "",
  variant: "",
  selectedVariant: "",
});

const READ_MODEL_SUMMARY_PROJECTION = {
  make: 1,
  makeKey: 1,
  model: 1,
  modelKey: 1,
  fullModel: 1,
  displayName: 1,
  city: 1,
  citySlug: 1,
  variantCount: 1,
  variantsPreview: 1,
  fuelText: 1,
  transmissionText: 1,
  gearboxText: 1,
  fuels: 1,
  transmissions: 1,
  gearboxes: 1,
  minExShowroomPrice: 1,
  maxExShowroomPrice: 1,
  minOnRoadPrice: 1,
  maxOnRoadPrice: 1,
  priceRangeLabel: 1,
  onRoadPriceRangeLabel: 1,
  bodyType: 1,
  bodyTypeKey: 1,
  hero: 1,
  colorCount: 1,
};

const SOURCE_PRICE_BREAKUP_PROJECTION = {
  brand: 1,
  make: 1,
  model: 1,
  variant: 1,
  variant_short: 1,
  variant_normalized: 1,
  city: 1,
  ex_showroom: 1,
  exShowroomPrice: 1,
  ex_showroom_price_cardekho: 1,
  rto: 1,
  rto_amount_cardekho: 1,
  insurance: 1,
  insurance_amount_cardekho: 1,
  other_list: 1,
  other_totalOtherCharges: 1,
  other_totalOtherChargesInRsFormat: 1,
  otherCharges: 1,
  optional_list: 1,
  optional_total: 1,
  optional_totalAccessories: 1,
  optional_totalAccessoriesInRs: 1,
  orp_without_accessories: 1,
  ORPWithoutOptionAccessoriesDoubleType: 1,
  onRoadPrice: 1,
  on_road_price_cardekho: 1,
  total_on_road_with_accessories: 1,
};

let priceCityCatalogPromise = null;

const getPriceCityCatalog = async () => {
  if (priceCityCatalogPromise) return priceCityCatalogPromise;

  priceCityCatalogPromise = mongoose.connection.db
    .collection(ACI_PRICE_ROWS_COLLECTION)
    .aggregate([
      {
        $group: {
          _id: "$citySlug",
          citySlug: { $first: "$citySlug" },
          city: { $first: "$city" },
        },
      },
      { $match: { citySlug: { $type: "string", $ne: "" } } },
      { $limit: 1000 },
    ])
    .toArray()
    .then((rows) =>
      rows
        .map((row) => ({
          citySlug: cleanText(row.citySlug),
          city: cleanText(row.city),
        }))
        .filter((row) => row.citySlug),
    );

  return priceCityCatalogPromise;
};

const resolveRequestedCityFromMessage = async ({
  requestedCity = DEFAULT_CITY,
  userMessage = "",
} = {}) => {
  const messageKey = normalizeKey(userMessage);
  if (!messageKey) return requestedCity || DEFAULT_CITY;

  const cityCatalog = await getPriceCityCatalog();
  const matches = cityCatalog
    .flatMap((city) => {
      const cityNameKey = normalizeKey(city.city);
      const citySlugKey = normalizeKey(city.citySlug);

      return [
        { ...city, key: cityNameKey },
        { ...city, key: citySlugKey },
      ];
    })
    .filter((city) => city.key && ` ${messageKey} `.includes(` ${city.key} `))
    .sort((a, b) => b.key.length - a.key.length);

  return matches[0]?.citySlug || requestedCity || DEFAULT_CITY;
};

const slugifyStrict = (value = "") => {
  const key = normalizeKey(value).replace(/\s+/g, "-");
  return key || "";
};

const exactVehicleTextRegex = (value = "") => {
  const clean = cleanVehicleText(value);
  if (!clean) return null;

  const pattern = escapeRegex(clean).replace(/[-_\s]+/g, "[-_\\s]+");
  return new RegExp(`^${pattern}$`, "i");
};

const buildReadModelSummaryQueries = ({
  requestedMake = "",
  requestedModel = "",
  citySlug = DEFAULT_CITY,
} = {}) => {
  const makeText = cleanVehicleText(requestedMake);
  const modelText = cleanVehicleText(requestedModel);
  const modelWithoutMake = makeText
    ? stripMakeFromModel(modelText, makeText)
    : modelText;
  const makeKey = slugifyStrict(makeText);
  const baseQuery = { citySlug, ...(makeKey ? { makeKey } : {}) };
  const textCandidates = uniqueCleanStrings(
    [modelText, modelWithoutMake]
      .map((item) => cleanVehicleText(item))
      .filter(Boolean),
  );
  const modelKeys = uniqueCleanStrings(
    textCandidates
      .map((item) => slugifyStrict(item))
      .filter(Boolean),
  );

  const queries = [];

  for (const modelKey of modelKeys) {
    queries.push({ ...baseQuery, modelKey });
  }

  for (const text of textCandidates) {
    const regex = exactVehicleTextRegex(text);
    if (!regex) continue;

    queries.push({
      ...baseQuery,
      $or: [{ displayName: regex }, { fullModel: regex }, { model: regex }],
    });
  }

  if (makeKey) {
    for (const text of textCandidates) {
      const regex = exactVehicleTextRegex(text);
      if (!regex) continue;

      queries.push({
        citySlug,
        $or: [{ displayName: regex }, { fullModel: regex }, { model: regex }],
      });
    }
  }

  return queries;
};

const resolveReadModelSummary = async ({
  db,
  requestedMake = "",
  requestedModel = "",
  citySlug = DEFAULT_CITY,
} = {}) => {
  const collection = db.collection(ACI_MODEL_SUMMARY_COLLECTION);
  const queries = buildReadModelSummaryQueries({
    requestedMake,
    requestedModel,
    citySlug,
  });

  for (const query of queries) {
    const summary = await collection.findOne(query, {
      projection: READ_MODEL_SUMMARY_PROJECTION,
    });

    if (summary) return { summary, query };
  }

  return { summary: null, query: queries[0] || { citySlug } };
};

const fetchVehiclePricelistRowsFromReadModels = async ({
  requestedMake = "",
  requestedModel = "",
  requestedVariant = "",
  requestedCity = DEFAULT_CITY,
  limit = 240,
} = {}) => {
  if (!mongoose.connection?.db) {
    return {
      rows: [],
      records: [],
      variants: [],
      count: 0,
      matched: 0,
      source: "aci_vehicle_read_models",
      dataSource: "aci_vehicle_read_models",
      modulesChecked: ["aci_vehicle_read_models:not_connected"],
    };
  }

  const db = mongoose.connection.db;
  const citySlug = slugify(requestedCity || DEFAULT_CITY);
  const resolved = await resolveReadModelSummary({
    db,
    requestedMake,
    requestedModel,
    citySlug,
  });
  const resolvedSummary = resolved.summary || {};
  const resolvedMake = resolvedSummary.make || requestedMake;
  const resolvedModel =
    resolvedSummary.model ||
    (resolvedMake ? stripMakeFromModel(requestedModel, resolvedMake) : requestedModel);
  const resolvedDisplayName =
    resolvedSummary.displayName ||
    resolvedSummary.fullModel ||
    buildVehicleDisplayName(resolvedMake, resolvedModel);
  const modelKey = resolvedSummary.modelKey || slugify(resolvedModel || requestedModel);
  const makeKey = resolvedSummary.makeKey || (requestedMake ? slugify(requestedMake) : "");
  const variantText = sanitizeRequestedVariant(requestedVariant, {
    requestedModel: resolvedModel || requestedModel,
    requestedMake: resolvedMake,
    requestedDisplayName: resolvedDisplayName,
  });
  const strictVariantKeys = buildStrictRequestedVariantKeys({
    requestedVariant: variantText || requestedVariant,
    requestedModel: resolvedModel || requestedModel,
    requestedMake: resolvedMake,
    requestedDisplayName: resolvedDisplayName,
  });

  if (!modelKey) {
    return {
      rows: [],
      records: [],
      variants: [],
      count: 0,
      matched: 0,
      source: "aci_vehicle_read_models",
      dataSource: "aci_vehicle_read_models",
      modulesChecked: ["aci_vehicle_read_models:no_model_key"],
    };
  }

  const summaryQuery = {
    modelKey,
    citySlug,
    ...(makeKey ? { makeKey } : {}),
  };

  const priceQuery = {
    modelKey,
    citySlug,
    ...(makeKey ? { makeKey } : {}),
    ...(strictVariantKeys.length ? { variantKey: { $in: strictVariantKeys } } : {}),
  };

  try {
    const summary =
      resolved.summary ||
      (await db.collection(ACI_MODEL_SUMMARY_COLLECTION).findOne(summaryQuery, {
        projection: READ_MODEL_SUMMARY_PROJECTION,
      }));

    const priceRows = await db
      .collection(ACI_PRICE_ROWS_COLLECTION)
      .find(
        priceQuery,
        {
          projection: {
            make: 1,
            makeKey: 1,
            model: 1,
            modelKey: 1,
            fullModel: 1,
            variant: 1,
            variantKey: 1,
            city: 1,
            citySlug: 1,
            exShowroomPrice: 1,
            onRoadPrice: 1,
            exShowroomPriceLabel: 1,
            onRoadPriceLabel: 1,
            fuel: 1,
            fuelKey: 1,
            transmission: 1,
            transmissionKey: 1,
            transmissionSource: 1,
            gearbox: 1,
            gearboxKey: 1,
            gearboxSource: 1,
            bodyType: 1,
            bodyTypeKey: 1,
            sortOrder: 1,
            sourceVehicleId: 1,
          },
        },
      )
      .sort({ sortOrder: 1 })
      .limit(limit)
      .toArray();

    if (!priceRows.length) {
      return {
        rows: [],
        records: [],
        variants: [],
        count: 0,
        matched: 0,
        source: "aci_vehicle_read_models",
        dataSource: "aci_vehicle_read_models",
        modulesChecked: [
          ACI_MODEL_SUMMARY_COLLECTION,
          ACI_PRICE_ROWS_COLLECTION,
          "aci_vehicle_read_models:empty",
        ],
        queryUsed: { summaryQuery, priceQuery },
      };
    }

    const effectiveSummary = summary || {};
    const visual = buildReadModelVisual(effectiveSummary);
    const visualGallery = visual ? [visual] : [];
    const vehicle = buildReadModelVehicleFromSummary(effectiveSummary, visual);
    const sourceVehicleIds = [
      ...new Map(
        priceRows
          .flatMap((row) => {
            const value = row.sourceVehicleId;
            const text = cleanText(value);
            if (!value && !text) return [];

            return [
              value,
              text,
              mongoose.Types.ObjectId.isValid(text)
                ? new mongoose.Types.ObjectId(text)
                : null,
            ].filter(Boolean);
          })
          .map((value) => [String(value), value]),
      ).values(),
    ];
    const sourceVehicles = sourceVehicleIds.length
      ? await db
          .collection("vehicles")
          .find(
            { _id: { $in: sourceVehicleIds } },
            { projection: SOURCE_PRICE_BREAKUP_PROJECTION },
          )
          .toArray()
      : [];
    const sourceVehicleById = new Map(
      sourceVehicles.map((sourceRow) => [String(sourceRow._id), sourceRow]),
    );

    const rows = priceRows.map((row) => {
      const sourceVehicle = sourceVehicleById.get(String(row.sourceVehicleId)) || null;
      const normalized = normalizePriceRow({
        ...(sourceVehicle || {}),
        ...row,
        raw: sourceVehicle || row,
      });

      return {
        ...row,
        ...normalized,
        brand: row.make,
        displayName: effectiveSummary.displayName || row.fullModel || buildVehicleDisplayName(row.make, row.model),
        city: row.city || effectiveSummary.city || displayCity(citySlug),
        citySlug: row.citySlug || citySlug,
        price: normalized.onRoadPriceWithoutOptional || normalized.onRoadPrice || row.onRoadPrice || row.exShowroomPrice || 0,
        priceLabel: formatMoney(normalized.onRoadPriceWithoutOptional || normalized.onRoadPrice || row.onRoadPrice || row.exShowroomPrice || 0),
        imageUrl: visual?.imageUrl || "",
        normalizedImageUrl: visual?.normalizedImageUrl || visual?.imageUrl || "",
        imageFrame: visual?.imageFrame || null,
        colorName: visual?.colorName || "",
        selectedColor: visual || null,
        visualGallery,
        vehicle: {
          ...vehicle,
          make: row.make || vehicle.make,
          brand: row.make || vehicle.brand,
          model: row.model || vehicle.model,
          city: row.city || vehicle.city,
          citySlug: row.citySlug || vehicle.citySlug,
        },
        source: "aci_vehicle_read_models",
        dataSource: "aci_vehicle_read_models",
      };
    });

    return {
      rows,
      records: rows,
      variants: rows,
      count: rows.length,
      matched: rows.length,
      source: "aci_vehicle_read_models",
      dataSource: "aci_vehicle_read_models",
      modulesChecked: [ACI_MODEL_SUMMARY_COLLECTION, ACI_PRICE_ROWS_COLLECTION],
      queryUsed: { summaryQuery, priceQuery },
      summary: {
        rowCount: rows.length,
        collection: ACI_PRICE_ROWS_COLLECTION,
        modelSummaryCollection: ACI_MODEL_SUMMARY_COLLECTION,
        minPrice: effectiveSummary.minExShowroomPrice || rows[0]?.exShowroomPrice || 0,
        maxPrice: effectiveSummary.maxExShowroomPrice || rows.at(-1)?.exShowroomPrice || 0,
      },
      readModelSummary: effectiveSummary,
      readModelVehicle: vehicle,
      visualGallery,
      selectedVisual: visual,
    };
  } catch (error) {
    return {
      rows: [],
      records: [],
      variants: [],
      count: 0,
      matched: 0,
      source: "aci_vehicle_read_models",
      dataSource: "aci_vehicle_read_models",
      modulesChecked: [
        ACI_MODEL_SUMMARY_COLLECTION,
        ACI_PRICE_ROWS_COLLECTION,
        "aci_vehicle_read_models:error",
      ],
      error: error?.message || "Read model lookup failed",
      queryUsed: { summaryQuery, priceQuery },
    };
  }
};

const fetchVehiclePricelistRows = async (params = {}) => {
  const readModelResult = await fetchVehiclePricelistRowsFromReadModels(params);

  if (asArray(readModelResult.rows).length) {
    return readModelResult;
  }

  const fallbackResult = await fetchVehiclePricelistRowsFromVehicles(params);

  return {
    ...fallbackResult,
    readModelFallback: {
      attempted: true,
      source: readModelResult.source,
      dataSource: readModelResult.dataSource,
      modulesChecked: readModelResult.modulesChecked || [],
      error: readModelResult.error || "",
      queryUsed: readModelResult.queryUsed || null,
    },
  };
};

const exactRequestedModelRows = ({
  rows = [],
  requestedModel = "",
  userMessage = "",
} = {}) => {
  const list = Array.isArray(rows) ? rows : [];
  const requestedClean = cleanVehicleText(requestedModel);
  const requestedKey = normalizeKey(requestedClean);
  const text = `${requestedModel} ${userMessage}`;

  if (!requestedKey || !list.length) return list;

  const explicitTour = explicitTourRequest(text);
  const explicitNLine = explicitNLineRequest(text);
  

  const exactRows = list.filter((row) => {
    const rowModel =
      row.model ||
      row.model_normalized ||
      row.modelNormalized ||
      row.rawModel ||
      row.displayName ||
      "";

    const rowKey = normalizeKey(cleanVehicleText(rowModel));

    if (!rowKey) return false;

    // Exact requested model should always win.
    if (rowKey === requestedKey) return true;

    // If user did not explicitly ask for Tour/N-Line, do not allow those
    // special-series rows to become the selected model.
    if (!explicitTour && rowKey === `${requestedKey}tour`) return false;
    if (!explicitNLine && rowKey === `${requestedKey}nline`) return false;

    return false;
  });

  // If exact rows exist, use them. If not, preserve existing fuzzy behavior
  // so we don't accidentally break imperfect DB naming for other models.
  return exactRows.length ? exactRows : list;
};

const explicitTourRequest = (value = "") =>
  /\btour\b/i.test(String(value || ""));

const getRowVariantKeyCandidates = (row = {}) =>
  [
    row.variantKey,
    row.variant_key,
    row.variantNormalized,
    row.variant_normalized,
    row.variantShort,
    row.variant_short,
    row.variant,
    row.variantName,
    row.variant_name,
    row.selectedVariant,
    row.raw?.variantKey,
    row.raw?.variant_key,
    row.raw?.variantNormalized,
    row.raw?.variant_normalized,
    row.raw?.variantShort,
    row.raw?.variant_short,
    row.raw?.variant,
    row.raw?.variantName,
    row.raw?.variant_name,
  ]
    .map((item) => cleanText(item))
    .filter(Boolean);

const stripRequestedVehiclePrefixFromVariant = ({
  requestedVariant = "",
  requestedModel = "",
  requestedMake = "",
  requestedDisplayName = "",
} = {}) => {
  return stripVehicleNameFromVariant(requestedVariant, {
    make: requestedMake,
    model: requestedModel,
    displayName: requestedDisplayName,
  });
};

const buildStrictRequestedVariantKeys = ({
  requestedVariant = "",
  requestedModel = "",
  requestedMake = "",
  requestedDisplayName = "",
} = {}) => {
  const raw = cleanText(requestedVariant);
  const sanitized = sanitizeRequestedVariant(raw, {
    requestedModel,
    requestedMake,
    requestedDisplayName,
  });

  const prefixStrippedRaw = stripRequestedVehiclePrefixFromVariant({
    requestedVariant: raw,
    requestedModel,
    requestedMake,
    requestedDisplayName,
  });

  const prefixStrippedSanitized = stripRequestedVehiclePrefixFromVariant({
    requestedVariant: sanitized,
    requestedModel,
    requestedMake,
    requestedDisplayName,
  });

  return uniqueExactCleanStrings(
    [raw, sanitized, prefixStrippedRaw, prefixStrippedSanitized]
      .map((item) => cleanText(item))
      .filter(Boolean)
      .flatMap((item) => [
        slugify(item),
        normalizeKey(item),
        normalizeKey(cleanVehicleText(item)),
        item,
      ])
      .filter(Boolean),
  );
};

const preferExactRequestedVariantRows = ({
  rows = [],
  requestedVariant = "",
  requestedModel = "",
  requestedMake = "",
  requestedDisplayName = "",
  dropAmbiguousLooseMatches = false,
  ambiguousLooseMatchLimit = 3,
} = {}) => {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length || !cleanText(requestedVariant)) return list;

  const variantContexts = [
    { requestedModel, requestedMake, requestedDisplayName },
    ...list.slice(0, 8).map((row) => ({
      requestedModel: row.model || requestedModel,
      requestedMake: row.make || row.brand || requestedMake,
      requestedDisplayName:
        row.fullModel ||
        row.displayName ||
        buildVehicleDisplayName(row.make || row.brand, row.model),
    })),
  ];
  const requestedKeys = new Set(
    variantContexts.flatMap((context) =>
      buildStrictRequestedVariantKeys({
        requestedVariant,
        requestedModel: context.requestedModel,
        requestedMake: context.requestedMake,
        requestedDisplayName: context.requestedDisplayName,
      }),
    ),
  );

  if (!requestedKeys.size) return list;

  const exactRows = list.filter((row) => {
    const candidates = getRowVariantKeyCandidates(row)
      .flatMap((item) => [
        item,
        slugify(item),
        normalizeKey(item),
        normalizeKey(cleanVehicleText(item)),
      ])
      .filter(Boolean);

    return candidates.some((candidate) => requestedKeys.has(candidate));
  });

  // Exact variant rows must win over longer variants that merely share a prefix.
  if (exactRows.length) return exactRows;

  if (dropAmbiguousLooseMatches && list.length > ambiguousLooseMatchLimit) {
    return [];
  }

  return list;
};


const preferExactRequestedModelRows = ({
  rows = [],
  requestedModel = "",
  userMessage = "",
} = {}) => {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return list;

  const requestedClean = cleanVehicleText(requestedModel);
  const requestedKey = normalizeKey(requestedClean);
  if (!requestedKey) return list;

  const text = `${requestedClean} ${userMessage}`;
  const userAskedTour = explicitTourRequest(text);
  const userAskedNLine = explicitNLineRequest(text);

  const rowModelKey = (row = {}) =>
    normalizeKey(
      cleanVehicleText(
        row.model ||
          row.modelName ||
          row.model_normalized ||
          row.modelNormalized ||
          "",
      ),
    );

  // Best case: exact model rows exist. Use them.
  const exactRows = list.filter((row) => rowModelKey(row) === requestedKey);
  if (exactRows.length) return exactRows;

  // Safety: for base-model requests, remove obvious special-series rows,
  // but only if that still leaves rows.
  const filtered = list.filter((row) => {
    const key = rowModelKey(row);
    if (!key) return true;

    if (!userAskedTour && key === `${requestedKey}tour`) return false;
    if (!userAskedNLine && key === `${requestedKey}nline`) return false;

    return true;
  });

  return filtered.length ? filtered : list;
};

const pickVehicleImageUrl = (row = {}) =>
  cleanText(
    row.heroImageNormalizedUrl ||
      row.normalizedHeroImageUrl ||
      row.heroNormalizedImageUrl ||
      row.heroImageUrl ||
      row.heroImage ||
      row.displayNormalizedImageUrl ||
      row.defaultNormalizedImageUrl ||
      row.normalizedImageUrl ||
      row.cleanImageUrl ||
      row.normalized_image_url ||
      row.clean_image_url ||
      row.normalizedImagePngUrl ||
      row.displayNormalizedImagePngUrl ||
      row.displayStagedImageUrl ||
      row.stagedImageUrl ||
      row.defaultColorImageUrl ||
      row.sourceImageUrl ||
      row.imageUrl ||
      row.image_url ||
      row.car_image_url ||
      row.colorImage ||
      row.color_image ||
      row.swatchImage ||
      row.url ||
      row.src ||
      "",
  );

const normalizeFrameMeta = (frame = {}) => {
  if (!frame || typeof frame !== "object") return frame || null;

  const readNumber = (...values) => {
    for (const value of values) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
    return null;
  };

  const x = readNumber(frame.x, frame.left, frame.minX);
  const y = readNumber(frame.y, frame.top, frame.minY);
  const width = readNumber(frame.width, frame.w);
  const height = readNumber(frame.height, frame.h);
  const canvasWidth = readNumber(frame.canvas_width, frame.canvasWidth, frame.naturalWidth, frame.imageWidth, frame.sourceWidth);
  const canvasHeight = readNumber(frame.canvas_height, frame.canvasHeight, frame.naturalHeight, frame.imageHeight, frame.sourceHeight);

  if (
    !Number.isFinite(x) ||
    !Number.isFinite(y) ||
    !Number.isFinite(width) ||
    !Number.isFinite(height) ||
    !Number.isFinite(canvasWidth) ||
    !Number.isFinite(canvasHeight) ||
    width <= 0 ||
    height <= 0 ||
    canvasWidth <= 0 ||
    canvasHeight <= 0
  ) {
    return frame;
  }

  const centerX = (x + width / 2) / canvasWidth;
  const centerY = (y + height / 2) / canvasHeight;
  const widthRatio = width / canvasWidth;
  const heightRatio = height / canvasHeight;
  const scale = Math.min(
    1.3,
    Math.max(1, Math.max(0.86 / Math.max(widthRatio, 0.01), 0.58 / Math.max(heightRatio, 0.01))),
  );

  return {
    ...frame,
    naturalWidth: canvasWidth,
    naturalHeight: canvasHeight,
    bounds: { x, y, width, height },
    cssVars: {
      ...(frame.cssVars || {}),
      "--car-frame-scale": Number(scale.toFixed(3)),
      "--car-frame-x": `${Number(((0.5 - centerX) * 100).toFixed(2))}%`,
      "--car-frame-y": `${Number(((0.5 - centerY) * 100).toFixed(2))}%`,
      "--car-frame-origin": "center center",
    },
  };
};

const firstMeaningfulFrame = (...frames) =>
  frames.find((frame) => frame && typeof frame === "object" && Object.keys(frame).length) || null;

const pickImageFrame = (row = {}) =>
  normalizeFrameMeta(
    row.heroFrameMeta ||
      row.displayFrameMeta ||
      row.defaultFrameMeta ||
      row.imageFrame ||
      row.frameMeta ||
      row.image_frame ||
      row.carImageFrame ||
      row.car_image_frame ||
      row.frame ||
      row.raw?.imageFrame ||
      row.raw?.displayFrameMeta ||
      null,
  );

const normalizeSeriesKey = (value = "") =>
  cleanVehicleText(value)
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "");

const getVisualModelText = (row = {}) => {
  const make = cleanText(first(row.make, row.brand, row.brandName));
  const rawModel = cleanText(
    first(
      row.model,
      row.modelName,
      row.model_name,
      row.model_normalized,
      row.modelNormalized,
      row.rawModel,
      row.displayName,
    ),
  );

  return stripMakeFromModel(rawModel, make) || rawModel;
};

const isSpecialSeriesVisualAllowed = ({
  requestedModel = "",
  visualModel = "",
  imageUrl = "",
  sourceImageUrl = "",
  colorName = "",
} = {}) => {
  const requestedKey = normalizeSeriesKey(requestedModel);
  const visualKey = normalizeSeriesKey(visualModel);
  const searchableKey = normalizeSeriesKey(
    `${visualModel} ${imageUrl} ${sourceImageUrl} ${colorName}`,
  );

  if (!requestedKey) return true;

  // If the color document exposes a model, use that as the strongest signal.
  if (visualKey && visualKey === requestedKey) return true;
  if (visualKey) return false;

  // If model metadata exists and is clearly unrelated, reject it.
  if (
    visualKey &&
    !visualKey.includes(requestedKey) &&
    !requestedKey.includes(visualKey)
  ) {
    return false;
  }

  return true;
};

const normalizeVisualColorRow = (row = {}, index = 0) => {
  const imageUrl = pickVehicleImageUrl(row);
  if (!imageUrl) return null;

  const make = cleanText(first(row.make, row.brand, row.brandName));
  const rawModel = cleanText(
    first(
      row.model,
      row.modelName,
      row.model_name,
      row.model_normalized,
      row.modelNormalized,
    ),
  );
  const model = stripMakeFromModel(rawModel, make) || rawModel;

  const colorName = cleanText(
    row.color_name ||
      row.colorName ||
      row.name ||
      row.label ||
      row.desktopName ||
      row.mobileName ||
      `Color ${index + 1}`,
  );

  const normalizedImageUrl =
    cleanText(
      row.normalizedImageUrl ||
        row.cleanImageUrl ||
        row.normalized_image_url ||
        row.clean_image_url ||
        row.normalizedImagePngUrl ||
        imageUrl,
    ) || imageUrl;

  return {
    id: cleanText(row._id || row.id || `${colorName}-${index}`),
    make,
    brand: make,
    model,
    rawModel,
    modelKey: normalizeSeriesKey(model),
    colorName,
    name: colorName,
    hex:
      cleanText(
        row.color_hex || row.colorHex || row.hex || row.hexCode || "",
      ) || "",
    imageUrl,
    normalizedImageUrl,
    imageFrame: pickImageFrame(row),
    sourceImageUrl: cleanText(
      row.sourceImageUrl || row.image_url || row.imageUrl || "",
    ),
  };
};

const flattenVisualColorDocuments = (docs = []) =>
  docs.flatMap((doc = {}) => {
    const make = cleanText(first(doc.make, doc.brand, doc.brandName));
    const model = cleanText(first(doc.model, doc.modelName, doc.model_name));
    const topFrame = pickImageFrame(doc);
    const topImage = pickVehicleImageUrl(doc);

    if (topImage) {
      return [{
        ...doc,
        make,
        brand: doc.brand || make,
        model,
        color_name: doc.defaultColorName || doc.color_name || doc.colorName || "Display",
        colorName: doc.defaultColorName || doc.colorName || doc.color_name || "Display",
        normalizedImageUrl: topImage,
        cleanImageUrl: topImage,
        imageUrl: topImage,
        sourceImageUrl: doc.displayImageUrl || doc.defaultColorImageUrl || doc.sourceImageUrl || "",
        imageFrame: topFrame,
      }];
    }

    const fallbackColor = (Array.isArray(doc.colors) ? doc.colors : []).find((color) =>
      pickVehicleImageUrl(color),
    );
    if (!fallbackColor) return [doc];

    const imageUrl = pickVehicleImageUrl(fallbackColor);
    return [{
      ...fallbackColor,
      _id: `${doc._id || `${make}-${model}`}:hero-fallback`,
      make,
      brand: doc.brand || make,
      model,
      color_name:
        fallbackColor.name ||
        fallbackColor.color_name ||
        fallbackColor.colorName ||
        "Display",
      colorName:
        fallbackColor.name ||
        fallbackColor.colorName ||
        fallbackColor.color_name ||
        "Display",
      color_hex: fallbackColor.hex || fallbackColor.color_hex || fallbackColor.colorHex || "",
      hex: fallbackColor.hex || fallbackColor.color_hex || fallbackColor.colorHex || "",
      normalizedImageUrl: imageUrl,
      cleanImageUrl: imageUrl,
      imageUrl,
      sourceImageUrl: fallbackColor.sourceImageUrl || "",
      imageFrame: normalizeFrameMeta(
        firstMeaningfulFrame(fallbackColor.imageFrame, fallbackColor.frameMeta),
      ),
      updatedAt: fallbackColor.updatedAt || doc.updatedAt,
    }];
  });

export const sampleVehicleColorImages = async ({
  make = "",
  model = "",
  limit = 8,
} = {}) => {
  if (!mongoose.connection?.db) return [];

  const cleanMake = cleanVehicleText(make);
  const cleanModel = cleanVehicleText(model);
  if (!cleanModel) return [];

  const modelRegex = buildModelRegex(cleanModel, cleanModel);
  const makeRegex = cleanMake ? looseWordsRegex(cleanMake) : null;

  const modelOr = [
    { model: modelRegex },
    { modelName: modelRegex },
    { model_name: modelRegex },
    { model_normalized: modelRegex },
    { modelNormalized: modelRegex },
    { model_slug: slugify(stripMakeFromModel(cleanModel, cleanMake) || cleanModel) },
  ];

  const makeOr = makeRegex
    ? [{ brand: makeRegex }, { make: makeRegex }, { brandName: makeRegex }, { brand_slug: slugify(cleanMake) }]
    : [];

  const query = {
    $and: [
      { $or: modelOr },
      ...(makeOr.length ? [{ $or: makeOr }] : []),
      {
        $or: [
          { normalizedImageUrl: { $exists: true, $ne: "" } },
          { cleanImageUrl: { $exists: true, $ne: "" } },
          { displayNormalizedImageUrl: { $exists: true, $ne: "" } },
          { heroImageUrl: { $exists: true, $ne: "" } },
          { heroImage: { $exists: true, $ne: "" } },
          { defaultNormalizedImageUrl: { $exists: true, $ne: "" } },
          { displayStagedImageUrl: { $exists: true, $ne: "" } },
          { normalized_image_url: { $exists: true, $ne: "" } },
          { clean_image_url: { $exists: true, $ne: "" } },
          { normalizedImagePngUrl: { $exists: true, $ne: "" } },
          { image_url: { $exists: true, $ne: "" } },
          { imageUrl: { $exists: true, $ne: "" } },
          { "colors.normalizedImageUrl": { $exists: true, $ne: "" } },
          { "colors.stagedImageUrl": { $exists: true, $ne: "" } },
        ],
      },
    ],
  };

  try {
    const rows = await mongoose.connection.db
      .collection(VEHICLE_COLORS_COLLECTION)
      .aggregate([
        { $match: query },
        {
          $project: {
            brand: 1,
            make: 1,
            brandName: 1,
            model: 1,
            modelName: 1,
            model_name: 1,
            model_normalized: 1,
            modelNormalized: 1,
            color_name: 1,
            colorName: 1,
            name: 1,
            label: 1,
            desktopName: 1,
            mobileName: 1,
            color_hex: 1,
            colorHex: 1,
            hex: 1,
            hexCode: 1,
            heroImageNormalizedUrl: 1,
            normalizedHeroImageUrl: 1,
            heroNormalizedImageUrl: 1,
            normalizedImageUrl: 1,
            cleanImageUrl: 1,
            displayNormalizedImageUrl: 1,
            displayNormalizedImagePngUrl: 1,
            displayStagedImageUrl: 1,
            heroFrameMeta: 1,
            displayFrameMeta: 1,
            defaultFrameMeta: 1,
            heroImageUrl: 1,
            heroImage: 1,
            defaultNormalizedImageUrl: 1,
            defaultColorImageUrl: 1,
            colors: 1,
            normalized_image_url: 1,
            clean_image_url: 1,
            normalizedImagePngUrl: 1,
            stagedImageUrl: 1,
            sourceImageUrl: 1,
            image_url: 1,
            imageUrl: 1,
            imageFrame: 1,
            image_frame: 1,
            carImageFrame: 1,
            car_image_frame: 1,
            frame: 1,
          },
        },
        {
          $sort: {
            imageBackgroundRemoved: -1,
            imageProcessingMethod: -1,
            color_name: 1,
            colorName: 1,
            updatedAt: -1,
          },
        },
        { $limit: Math.max(12, limit * 4) },
      ])
      .toArray();

    return flattenVisualColorDocuments(rows)
      .map(normalizeVisualColorRow)
      .filter(Boolean)
      .filter((item) =>
        isSpecialSeriesVisualAllowed({
          requestedModel: cleanModel,
          visualModel: item.model || item.rawModel,
          imageUrl: item.imageUrl || item.normalizedImageUrl,
          sourceImageUrl: item.sourceImageUrl,
          colorName: item.colorName || item.name,
        }),
      )
      .filter((item, index, list) => {
        const key = `${item.colorName.toLowerCase()}|${item.imageUrl}`;
        return (
          list.findIndex(
            (entry) =>
              `${entry.colorName.toLowerCase()}|${entry.imageUrl}` === key,
          ) === index
        );
      })
      .slice(0, Math.max(1, limit));
  } catch {
    return [];
  }
};

const deepTextValues = (value, depth = 0, output = []) => {
  if (value === null || value === undefined || depth > 8) return output;

  if (typeof value === "string" || typeof value === "number") {
    const text = cleanText(value);
    if (text) output.push(text);
    return output;
  }

  if (Array.isArray(value)) {
    value.forEach((item) => deepTextValues(item, depth + 1, output));
    return output;
  }

  if (typeof value === "object") {
    Object.entries(value).forEach(([key, child]) => {
      const cleanKey = cleanText(key);
      if (cleanKey) output.push(cleanKey);
      deepTextValues(child, depth + 1, output);
    });
  }

  return output;
};

const primitiveText = (value = "") => {
  if (value === null || value === undefined) return "";
  if (typeof value === "string" || typeof value === "number") {
    return cleanText(value);
  }
  return "";
};

const objectLabel = (object = {}) =>
  cleanText(
    first(
      object.label,
      object.name,
      object.title,
      object.key,
      object.feature,
      object.featureName,
      object.spec,
      object.specName,
      object.heading,
    ),
  );

const objectValue = (object = {}) =>
  cleanText(
    first(
      object.value,
      object.text,
      object.description,
      object.specValue,
      object.featureValue,
      object.displayValue,
      object.answer,
      object.option,
    ),
  );

const collectPrimitiveText = (value, depth = 0, output = []) => {
  if (value === null || value === undefined || depth > 6) return output;

  if (typeof value === "string" || typeof value === "number") {
    const text = cleanText(value);
    if (text) output.push(text);
    return output;
  }

  if (Array.isArray(value)) {
    value.forEach((item) => collectPrimitiveText(item, depth + 1, output));
    return output;
  }

  if (typeof value === "object") {
    Object.values(value).forEach((child) =>
      collectPrimitiveText(child, depth + 1, output),
    );
  }

  return output;
};

const findFeatureValueByLabel = (source = {}, patterns = []) => {
  const queue = [source];

  while (queue.length) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;

    if (Array.isArray(current)) {
      queue.push(...current);
      continue;
    }

    const label = objectLabel(current).toLowerCase();

    if (label && patterns.some((pattern) => pattern.test(label))) {
      const directValue = objectValue(current);
      if (directValue) return directValue;

      const nestedValue = collectPrimitiveText(current)
        .filter((item) => normalizeKey(item) !== normalizeKey(label))
        .join(" ");

      if (nestedValue) return nestedValue;
    }

    for (const [key, value] of Object.entries(current)) {
      const keyText = cleanText(key).toLowerCase();

      if (patterns.some((pattern) => pattern.test(keyText))) {
        const direct = primitiveText(value);
        if (direct) return direct;

        const nested = collectPrimitiveText(value).join(" ");
        if (nested) return nested;
      }

      if (value && typeof value === "object") {
        queue.push(value);
      }
    }
  }

  return "";
};

const normalizeFuelValue = (value = "") => {
  const text = cleanText(value).toLowerCase();

  if (!text) return "";
  if (/\bdiesel\b/.test(text)) return "Diesel";
  if (/\bpetrol\b/.test(text)) return "Petrol";
  if (/\bcng\b/.test(text)) return "CNG";
  if (/\bhybrid\b/.test(text)) return "Hybrid";
  if (/\belectric\b|\bev\b/.test(text)) return "Electric";

  return "";
};

const normalizeTransmissionValue = (value = "") => {
  const text = cleanText(value).toLowerCase();

  if (!text) return "";

  if (/\bdct\b/.test(text)) return "DCT";
  if (/\bivt\b/.test(text)) return "IVT";
  if (/\bcvt\b/.test(text)) return "CVT";
  if (/\bamt\b/.test(text)) return "AMT";

  if (/\bautomatic\b/.test(text) || /\bauto\b/.test(text)) {
    return "Automatic";
  }

  if (/\bmanual\b/.test(text)) return "Manual";

  // Only use AT/MT when they appear as proper tokens, not inside words.
  if (/(^|\s|\W)at($|\s|\W)/i.test(value)) return "Automatic";
  if (/(^|\s|\W)mt($|\s|\W)/i.test(value)) return "Manual";

  return "";
};

const buildFeatureVariantKey = ({ variant = "", make = "", model = "" } = {}) =>
  normalizeKey(
    stripVehicleNameFromVariant(variant, {
      make,
      model,
    }) || variant,
  );

const getBestVariantMeta = (metaMap = new Map(), row = {}) => {
  if (!metaMap.size) return null;

  const rowKey = normalizeKey(row.variant || row.variantName || "");
  if (!rowKey) return null;

  // Important: exact only.
  // No includes/fuzzy matching, otherwise Safari manual rows can inherit
  // automatic from a different variant.
  return metaMap.get(rowKey) || null;
};

const extractFuelTransmissionFromFeatureRow = (row = {}) => {
  const fuelText =
    findFeatureValueByLabel(row, [/^fuel$/, /fuel\s*type/, /engine\s*type/]) ||
    row.fuel ||
    row.fuelType ||
    row.fuel_type ||
    row.engineType ||
    row.engine_type ||
    "";

  const transmissionText =
    findFeatureValueByLabel(row, [
      /^transmission$/,
      /transmission\s*type/,
      /^gearbox$/,
      /gear\s*box/,
    ]) ||
    row.transmission ||
    row.transmissionType ||
    row.transmission_type ||
    row.gearbox ||
    "";

  return {
    fuel: normalizeFuelValue(fuelText),
    transmission: normalizeTransmissionValue(transmissionText),
  };
};

const resolveVariantFeatureMeta = async ({
  make = "",
  model = "",
  rows = [],
} = {}) => {
  if (!mongoose.connection?.db || !Array.isArray(rows) || !rows.length) {
    return new Map();
  }

  const cleanMake = cleanVehicleText(make);
  const cleanModel = cleanVehicleText(model);

  if (!cleanModel) return new Map();

  const modelRegex = buildModelRegex(cleanModel, cleanModel);
  const makeRegex = cleanMake ? looseWordsRegex(cleanMake) : null;

  const modelOr = [
    { model: modelRegex },
    { modelName: modelRegex },
    { model_name: modelRegex },
    { model_normalized: modelRegex },
    { modelNormalized: modelRegex },
  ];

  const makeOr = makeRegex
    ? [{ brand: makeRegex }, { make: makeRegex }, { brandName: makeRegex }]
    : [];

  const query = {
    $and: [{ $or: modelOr }, ...(makeOr.length ? [{ $or: makeOr }] : [])],
  };

  let featureRows = [];

  try {
    featureRows = await mongoose.connection.db
      .collection("vehicle_features")
      .find(query)
      .limit(1200)
      .toArray();
  } catch {
    featureRows = [];
  }

  const byVariant = new Map();

  for (const featureRow of featureRows) {
    const variantCandidates = [
      featureRow.variant,
      featureRow.variantName,
      featureRow.variant_name,
      featureRow.name,
      featureRow.title,
      featureRow.rawVariant,
      featureRow.raw?.variant,
      featureRow.raw?.variantName,
      featureRow.raw?.variant_name,
    ].filter(Boolean);

    const meta = extractFuelTransmissionFromFeatureRow(featureRow);

    if (!meta.fuel && !meta.transmission) continue;

    for (const variantCandidate of variantCandidates) {
      const variantKey = buildFeatureVariantKey({
        variant: variantCandidate,
        make: cleanMake,
        model: cleanModel,
      });

      if (!variantKey) continue;

      const previous = byVariant.get(variantKey) || {};

      byVariant.set(variantKey, {
        fuel: previous.fuel || meta.fuel,
        transmission: previous.transmission || meta.transmission,
      });
    }
  }

  return byVariant;
};

const enrichRowsWithFeatureMeta = async ({
  rows = [],
  make = "",
  model = "",
} = {}) => {
  const list = Array.isArray(rows) ? rows : [];

  if (!list.length) return list;

  const metaMap = await resolveVariantFeatureMeta({
    make,
    model,
    rows: list,
  });

  return list.map((row) => {
    const featureMeta = getBestVariantMeta(metaMap, row) || {};
    const rowMeta = extractFuelTransmissionFromFeatureRow(row);

    const existingFuel =
      cleanText(row.fuel) && row.fuel !== "N.A." ? row.fuel : "";

    const existingTransmission =
      cleanText(row.transmission) && row.transmission !== "N.A."
        ? row.transmission
        : "";

    const fuel = existingFuel || featureMeta.fuel || rowMeta.fuel || "";

    const transmission =
      existingTransmission ||
      featureMeta.transmission ||
      rowMeta.transmission ||
      "";

    return {
      ...row,
      fuel,
      fuelType: row.fuelType || fuel,
      transmission,
      transmissionType: row.transmissionType || transmission,
      fuelTransmission: [fuel, transmission].filter(Boolean).join(" · "),
    };
  });
};

const resultAttemptedStrictVariantLookup = (result = {}) =>
  Boolean(
    result.queryUsed?.priceQuery?.variantKey ||
      result.readModelFallback?.queryUsed?.priceQuery?.variantKey,
  );

export const runVehiclePricelistNewCarsTool = async (args = {}) => {
  const { toolPlan = {}, context = {}, userMessage = "" } = args;

  const rawRequestedMake = getRequestedMake(args);
  const requestedModel = getRequestedModel(args);
  const explicitRequestedMake = getExplicitRequestedMake({ toolPlan });
  const requestedMake = resolveContextSafeRequestedMake({
    rawRequestedMake,
    requestedModel,
    toolPlan,
    context,
  });
  const requestedVariantCandidates = [
    toolPlan.entities?.primaryVariant,
    toolPlan.entities?.variant,
    toolPlan.filters?.variant,
    toolPlan.input?.variant,
    context?.selectedVehicle?.variant,
    context?.anchorVariant,
    context?.variant,
    getRequestedVariant(args),
  ]
    .map((item) => cleanText(item))
    .filter(Boolean);

  let rawRequestedVariant = "";
  let requestedVariant = "";

  for (const candidate of requestedVariantCandidates) {
    const sanitizedCandidate = sanitizeRequestedVariant(candidate, {
      requestedModel,
      requestedMake,
    });

    if (cleanText(sanitizedCandidate)) {
      rawRequestedVariant = candidate;
      requestedVariant = sanitizedCandidate;
      break;
    }
  }
  let requestedCity = getRequestedCity(args);
  requestedCity = await resolveRequestedCityFromMessage({
    requestedCity,
    userMessage,
  });
  let effectiveRequestedMake = requestedMake;

  let rawResult = await fetchVehiclePricelistRows({
    requestedMake,
    requestedModel,
    requestedVariant,
    requestedCity,
    userMessage,
    limit: toolPlan.limit || toolPlan.input?.limit || 240,
  });

  if (
    !asArray(rawResult.rows).length &&
    requestedVariant &&
    !resultAttemptedStrictVariantLookup(rawResult)
  ) {
    rawResult = await fetchVehiclePricelistRows({
      requestedMake,
      requestedModel,
      requestedVariant: "",
      requestedCity,
      userMessage,
      limit: toolPlan.limit || toolPlan.input?.limit || 240,
    });

    rawResult.variantFilterRelaxed = true;
    rawResult.originalRequestedVariant = rawRequestedVariant;
  }

  if (
    !asArray(rawResult.rows).length &&
    requestedMake &&
    !explicitRequestedMake
  ) {
    rawResult = await fetchVehiclePricelistRows({
      requestedMake: "",
      requestedModel,
      requestedVariant,
      requestedCity,
      userMessage,
      limit: toolPlan.limit || toolPlan.input?.limit || 240,
    });

    if (
      !asArray(rawResult.rows).length &&
      requestedVariant &&
      !resultAttemptedStrictVariantLookup(rawResult)
    ) {
      rawResult = await fetchVehiclePricelistRows({
        requestedMake: "",
        requestedModel,
        requestedVariant: "",
        requestedCity,
        userMessage,
        limit: toolPlan.limit || toolPlan.input?.limit || 240,
      });
      rawResult.variantFilterRelaxed = true;
      rawResult.originalRequestedVariant = rawRequestedVariant;
    }

    if (asArray(rawResult.rows).length) {
      effectiveRequestedMake = "";
      rawResult.makeFilterRelaxed = true;
      rawResult.originalRequestedMake = requestedMake;
    }
  }

  const rawRows = [
    ...asArray(rawResult.rows),
    ...asArray(rawResult.records),
    ...asArray(rawResult.variants),
  ];

  let rows = uniqueRows(rawRows.map(normalizePriceRow));

  rows = exactModelFilter({
    rows,
    requestedModel,
    userMessage,
  });

  rows = preferExactRequestedModelRows({
    rows,
    requestedModel,
    userMessage,
  });

  rows = preferExactRequestedVariantRows({
    rows,
    requestedVariant,
    requestedModel,
    requestedMake: effectiveRequestedMake || requestedMake,
    dropAmbiguousLooseMatches: true,
  });

  const softAlternatives = buildSoftAlternatives({
    requestedModel,
    requestedMake: effectiveRequestedMake,
    userMessage,
  });

  let visualGallery = [];
  let selectedVisual = null;
  let resolvedImageUrl = "";
  let resolvedImageFrame = null;

  if (rows.length) {
    const usingReadModel = rawResult.dataSource === "aci_vehicle_read_models";

    if (usingReadModel) {
      visualGallery = asArray(rawResult.visualGallery || rows[0]?.visualGallery);
      selectedVisual =
        rawResult.selectedVisual ||
        rows[0]?.selectedColor ||
        visualGallery[0] ||
        null;
    } else {
      // Enrich fuel/transmission before creating the vehicle summary.
      rows = await enrichRowsWithFeatureMeta({
        rows,
        make: rows[0]?.make || rows[0]?.brand || requestedMake,
        model: rows[0]?.model || requestedModel,
      });

      visualGallery = await sampleVehicleColorImages({
        make: rows[0]?.make || rows[0]?.brand || requestedMake,
        model: rows[0]?.model || requestedModel,
        limit: 8,
      });

      selectedVisual = visualGallery[0] || null;
    }

    resolvedImageUrl =
      selectedVisual?.imageUrl || selectedVisual?.normalizedImageUrl || rows[0]?.imageUrl || rows[0]?.normalizedImageUrl || "";
    resolvedImageFrame = selectedVisual?.imageFrame || rows[0]?.imageFrame || null;

    if (resolvedImageUrl || resolvedImageFrame || visualGallery.length) {
      rows = rows.map((row) => ({
        ...row,
        imageUrl: row.imageUrl || row.normalizedImageUrl || resolvedImageUrl,
        normalizedImageUrl:
          row.normalizedImageUrl || row.imageUrl || resolvedImageUrl,
        imageFrame: row.imageFrame || resolvedImageFrame,
        colorName: row.colorName || selectedVisual?.colorName || "",
        selectedColor: row.selectedColor || selectedVisual || null,
        visualGallery,
        vehicle: {
          ...(row.vehicle || {}),
          make: row.make,
          brand: row.brand || row.make,
          model: row.model,
          displayName: row.displayName,
          imageUrl:
            row.vehicle?.imageUrl ||
            row.imageUrl ||
            row.normalizedImageUrl ||
            resolvedImageUrl,
          normalizedImageUrl:
            row.vehicle?.normalizedImageUrl ||
            row.normalizedImageUrl ||
            row.imageUrl ||
            resolvedImageUrl,
          imageFrame:
            row.vehicle?.imageFrame || row.imageFrame || resolvedImageFrame,
          colorName: row.vehicle?.colorName || selectedVisual?.colorName || "",
          selectedColor: row.vehicle?.selectedColor || selectedVisual || null,
          visualGallery,
        },
      }));
    }
  }

  const vehicle = buildVehicle({
    rows,
    requestedMake: effectiveRequestedMake,
    requestedModel,
    city: requestedCity,
  });

  if (resolvedImageUrl || resolvedImageFrame || visualGallery.length) {
    vehicle.imageUrl = vehicle.imageUrl || resolvedImageUrl;
    vehicle.normalizedImageUrl = vehicle.normalizedImageUrl || resolvedImageUrl;
    vehicle.imageFrame = vehicle.imageFrame || resolvedImageFrame || null;
    vehicle.colorName = vehicle.colorName || selectedVisual?.colorName || "";
    vehicle.selectedColor = vehicle.selectedColor || selectedVisual || null;
    vehicle.visualGallery = visualGallery;
  }

  const requestedPriceBasis = cleanText(
    first(
      toolPlan.filters?.priceBasis,
      toolPlan.input?.priceBasis,
      toolPlan.priceBasis,
    ),
  );
  const wantsOnRoadPrice =
    normalizeKey(`${requestedPriceBasis} ${userMessage}`).includes("on road");
  const asksForVariantList =
    /\b(price\s*list|pricelist|variants?\s+price|variant[-\s]*wise|all\s+variants?)\b/i.test(
      userMessage || "",
    );
  const hasRequestedVariant = Boolean(cleanText(requestedVariant || rawRequestedVariant));
  const isExactVariantResult = rows.length === 1 && hasRequestedVariant;
  const isModelLevelListResult =
    rows.length > 1 && (!hasRequestedVariant || asksForVariantList);
  const wantsPriceBreakup =
    !isModelLevelListResult &&
    isExactVariantResult &&
    (wantsOnRoadPrice ||
      /\b(price\s*)?(break\s*up|breakup|breakdown)\b/i.test(userMessage || ""));
  const outputCanvasType = wantsPriceBreakup
    ? "price_breakup_canvas"
    : "pricelist_canvas";
  const displayVariant = cleanVehicleText(
    isExactVariantResult
      ? sanitizeRequestedVariant(rows[0]?.variant || requestedVariant, {
      requestedModel: vehicle.model || requestedModel,
      requestedMake: vehicle.make || effectiveRequestedMake || requestedMake,
      requestedDisplayName: vehicle.displayName,
        }) ||
          rows[0]?.variant ||
          requestedVariant
      : "",
  );
  const vehicleLabel =
    vehicle.displayName ||
    buildVehicleDisplayName(requestedMake, requestedModel) ||
    "Vehicle";
  const title = displayVariant
    ? `${vehicleLabel} ${displayVariant} ${wantsOnRoadPrice ? "on-road price" : "price"}`
    : `${vehicleLabel} price list`;
  const subtitle = `${vehicle.city || displayCity(requestedCity)} · ${rows.length} variants · ${wantsOnRoadPrice ? "On-road" : "Ex-showroom"}`;
  const listAnswer = `I found ${rows.length} ${vehicleLabel} variants in ${vehicle.city || displayCity(requestedCity)}. Default on-road prices exclude optional add-ons; optional add-on totals are available in each variant breakup.`;

  const actions = buildActions({
    vehicle,
    requestedModel,
    requestedVariant,
    softAlternatives,
  });

  const leadingQuestions = buildLeadingQuestions({
    vehicle,
    requestedVariant,
  });

  const contextPatch = {
    selectedVehicle: {
      ...vehicle,
      imageUrl: vehicle.imageUrl || resolvedImageUrl || "",
      normalizedImageUrl: vehicle.normalizedImageUrl || resolvedImageUrl || "",
      imageFrame: vehicle.imageFrame || resolvedImageFrame || null,
      colorName: vehicle.colorName || selectedVisual?.colorName || "",
      selectedColor: vehicle.selectedColor || selectedVisual || null,
      visualGallery,
      variant: requestedVariant || "",
      selectedVariant: requestedVariant || "",
    },
    anchorMake: vehicle.make || requestedMake,
    anchorModel: vehicle.model || requestedModel,
    anchorVariant: requestedVariant || "",
    anchorCity: vehicle.city || displayCity(requestedCity),
  };

  const widget = {
    type: "vehicle_pricelist",
    widgetType: "vehicle_pricelist",
    canvasType: outputCanvasType,
    priceBreakupCanvas: wantsPriceBreakup,
    title,
    heading: title,
    subtitle,
    answer: rows.length
      ? isModelLevelListResult
        ? listAnswer
        : `I found the ${title} for ${vehicle.city || displayCity(requestedCity)}.`
      : `I could not find live price rows for ${requestedModel || "this model"} in ${displayCity(requestedCity)}.`,
    city: vehicle.city || displayCity(requestedCity),
    citySlug: vehicle.citySlug || slugify(requestedCity || DEFAULT_CITY),
    vehicle,
    visualGallery,
    selectedColor: selectedVisual,
    imageUrl: vehicle.imageUrl || resolvedImageUrl || "",
    normalizedImageUrl: vehicle.normalizedImageUrl || resolvedImageUrl || "",
    imageFrame: vehicle.imageFrame || resolvedImageFrame || null,
    rows,
    records: rows,
    variants: rows,
    totalVariants: rows.length,
    count: rows.length,
    matched: rows.length,
    summary: {
      ...(rawResult.summary || {}),
      minPrice: rows.length
        ? Math.min(
            ...rows
              .map((row) => row.exShowroomPrice || row.onRoadPrice || 0)
              .filter(Boolean),
          )
        : 0,
      maxPrice: rows.length
        ? Math.max(
            ...rows
              .map((row) => row.exShowroomPrice || row.onRoadPrice || 0)
              .filter(Boolean),
          )
        : 0,
      rowCount: rows.length,
    },
    variantResolution: rawResult.variantResolution || null,
    candidateVariants: rawResult.candidateVariants || [],
    softAlternatives,
    actions,
    leadingQuestions,
    contextPatch,
    source: rawResult.source,
    dataSource: rawResult.dataSource,
    modulesChecked: rawResult.modulesChecked || [],
  };

  return {
    ...rawResult,
    tool: "vehicle_pricelist",
    intent: "vehicle_pricelist",
    canvasType: outputCanvasType,
    priceBreakupCanvas: wantsPriceBreakup,
    answer: widget.answer,
    title,
    subtitle,
    vehicle,
    rows,
    records: rows,
    variants: rows,
    totalVariants: rows.length,
    count: rows.length,
    matched: rows.length,
    widget,
    actions,
    leadingQuestions,
    contextPatch,
    softAlternatives,
    requested: {
      make: requestedMake,
      model: cleanVehicleText(requestedModel),
      variant: requestedVariant,
      city: requestedCity,
    },
  };
};

export default runVehiclePricelistNewCarsTool;
