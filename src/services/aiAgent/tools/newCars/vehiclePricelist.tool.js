import mongoose from "mongoose";
import { formatMoney } from "../shared/pricing.js";
import { buildV2PriceBreakup } from "./shared/priceBreakup.js";

const DEFAULT_CITY = "new-delhi";

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
      out = cleanText(
        out.replace(new RegExp(`^${escapeRegex(token)}\\s+`, "i"), ""),
      );
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

const sanitizeRequestedVariant = (
  value = "",
  { requestedModel = "", requestedMake = "" } = {},
) => {
  const stripped = stripVehicleNameFromVariant(value, {
    make: requestedMake,
    model: requestedModel,
  });

  if (isGenericVariantCandidate(stripped, { requestedModel })) return "";

  return stripped;
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
  const selectedVehicle = context.selectedVehicle || {};

  return cleanVehicleText(
    first(
      entities.model,
      entities.models?.[0],
      toolPlan.model,
      toolPlan.input?.model,
      selectedVehicle.model,
      context.anchorModel,
      context.model,
      userMessage,
    ),
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
  const selectedVehicle = context.selectedVehicle || {};

  return cleanText(
    first(
      entities.variant,
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

  // User has moved from Seltos to Safari, Verna to X5, etc.
  // Do not carry old selectedVehicle.make into the new model query.
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

    computedOnRoadPrice: breakup.computedOnRoadPrice,
    canonicalOnRoadPrice: breakup.canonicalOnRoadPrice,
    priceIntegrity: breakup.priceIntegrity,

    priceBreakup: {
      exShowroom: breakup.exShowroom,
      rto: breakup.rto,
      insurance: breakup.insurance,
      otherCharges: breakup.otherCharges,
      optionalTotal: breakup.optionalTotal,
      otherTotal: breakup.otherTotal,
      onRoadPrice: breakup.onRoadPrice,
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

  // Soft ambiguity rule:
  // "Venue" means regular Venue. "Venue N Line" means N Line.
  // Same logic also protects i20 / Creta from N Line leakage.
  if (!messageWantsNLine && /\b(venue|i20|creta)\b/.test(modelKey)) {
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
  const key = normalizeKey(`${requestedModel} ${userMessage}`);
  const explicitNLine = explicitNLineRequest(key);

  if (explicitNLine) return [];

  if (/\bvenue\b/.test(key)) {
    return [
      {
        make: requestedMake || "Hyundai",
        model: "Venue N Line",
        label: "Venue N Line",
      },
    ];
  }

  if (/\bcreta\b/.test(key)) {
    return [
      {
        make: requestedMake || "Hyundai",
        model: "Creta N Line",
        label: "Creta N Line",
      },
    ];
  }

  if (/\bi20\b/.test(key)) {
    return [
      {
        make: requestedMake || "Hyundai",
        model: "i20 N Line",
        label: "i20 N Line",
      },
    ];
  }

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
    row.normalizedImageUrl ||
      row.cleanImageUrl ||
      row.normalized_image_url ||
      row.clean_image_url ||
      row.normalizedImagePngUrl ||
      row.stagedImageUrl ||
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

const pickImageFrame = (row = {}) =>
  row.imageFrame ||
  row.image_frame ||
  row.carImageFrame ||
  row.car_image_frame ||
  row.frame ||
  row.raw?.imageFrame ||
  null;

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
  colorName = "",
} = {}) => {
  const requestedKey = normalizeSeriesKey(requestedModel);
  const visualKey = normalizeSeriesKey(visualModel);
  const searchableKey = normalizeSeriesKey(
    `${visualModel} ${imageUrl} ${colorName}`,
  );

  if (!requestedKey) return true;

  // If the color document exposes a model, use that as the strongest signal.
  if (visualKey && visualKey === requestedKey) return true;

  // Protect base models from special-series image leakage.
  if (requestedKey === "thar" && /roxx/.test(searchableKey)) return false;
  if (requestedKey === "ertiga" && /tour/.test(searchableKey)) return false;
  if (requestedKey === "venue" && /nline/.test(searchableKey)) return false;
  if (requestedKey === "creta" && /nline/.test(searchableKey)) return false;
  if (requestedKey === "i20" && /nline/.test(searchableKey)) return false;

  // For special-series requests, avoid falling back to the base model image.
  if (requestedKey === "tharroxx" && visualKey && visualKey !== "tharroxx")
    return false;
  if (requestedKey === "ertigatour" && visualKey && visualKey !== "ertigatour")
    return false;
  if (requestedKey === "venuenline" && visualKey && visualKey !== "venuenline")
    return false;
  if (requestedKey === "cretanline" && visualKey && visualKey !== "cretanline")
    return false;
  if (requestedKey === "i20nline" && visualKey && visualKey !== "i20nline")
    return false;

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

const sampleVehicleColorImages = async ({
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
  ];

  const makeOr = makeRegex
    ? [{ brand: makeRegex }, { make: makeRegex }, { brandName: makeRegex }]
    : [];

  const query = {
    $and: [
      { $or: modelOr },
      ...(makeOr.length ? [{ $or: makeOr }] : []),
      {
        $or: [
          { normalizedImageUrl: { $exists: true, $ne: "" } },
          { cleanImageUrl: { $exists: true, $ne: "" } },
          { normalized_image_url: { $exists: true, $ne: "" } },
          { clean_image_url: { $exists: true, $ne: "" } },
          { normalizedImagePngUrl: { $exists: true, $ne: "" } },
          { image_url: { $exists: true, $ne: "" } },
          { imageUrl: { $exists: true, $ne: "" } },
        ],
      },
    ],
  };

  try {
    const rows = await mongoose.connection.db
      .collection("vehicle_colors")
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
            normalizedImageUrl: 1,
            cleanImageUrl: 1,
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

    return rows
      .map(normalizeVisualColorRow)
      .filter(Boolean)
      .filter((item) =>
        isSpecialSeriesVisualAllowed({
          requestedModel: cleanModel,
          visualModel: item.model || item.rawModel,
          imageUrl: item.imageUrl || item.normalizedImageUrl,
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
  const rawRequestedVariant = getRequestedVariant(args);
  const requestedVariant = sanitizeRequestedVariant(rawRequestedVariant, {
    requestedModel,
    requestedMake,
  });
  const requestedCity = getRequestedCity(args);
  let effectiveRequestedMake = requestedMake;

  let rawResult = await fetchVehiclePricelistRowsFromVehicles({
    requestedMake,
    requestedModel,
    requestedVariant,
    requestedCity,
    userMessage,
    limit: toolPlan.limit || toolPlan.input?.limit || 240,
  });

  if (!asArray(rawResult.rows).length && requestedVariant) {
    rawResult = await fetchVehiclePricelistRowsFromVehicles({
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
    rawResult = await fetchVehiclePricelistRowsFromVehicles({
      requestedMake: "",
      requestedModel,
      requestedVariant,
      requestedCity,
      userMessage,
      limit: toolPlan.limit || toolPlan.input?.limit || 240,
    });

    if (!asArray(rawResult.rows).length && requestedVariant) {
      rawResult = await fetchVehiclePricelistRowsFromVehicles({
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
    resolvedImageUrl =
      selectedVisual?.imageUrl || selectedVisual?.normalizedImageUrl || "";
    resolvedImageFrame = selectedVisual?.imageFrame || null;

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

  const title = `${vehicle.displayName || buildVehicleDisplayName(requestedMake, requestedModel) || "Vehicle"} price list`;
  const subtitle = `${vehicle.city || displayCity(requestedCity)} · ${rows.length} variants · Ex-showroom`;

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
    canvasType: "pricelist_canvas",
    title,
    heading: title,
    subtitle,
    answer: rows.length
      ? `I found the ${title} for ${vehicle.city || displayCity(requestedCity)}.`
      : `I could not find live price rows for ${requestedModel || "this model"} in ${displayCity(requestedCity)}.`,
    city: vehicle.city || displayCity(requestedCity),
    citySlug: slugify(requestedCity || DEFAULT_CITY),
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
    canvasType: "pricelist_canvas",
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
