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

  return cleanText(cleanModel.replace(new RegExp(`^${cleanMake}\\s+`, "i"), "")) || cleanModel;
};

const buildVehicleDisplayName = (make = "", model = "") => {
  const cleanMake = cleanText(make);
  const cleanModel = stripMakeFromModel(model, cleanMake);

  return cleanText([cleanMake, cleanModel].filter(Boolean).join(" ")) || cleanModel || cleanMake;
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
      out = cleanText(out.replace(new RegExp(`^${escapeRegex(token)}\\s+`, "i"), ""));
    }
  }

  return out;
};

const isGenericVariantCandidate = (value = "", { requestedModel = "" } = {}) => {
  const key = normalizeKey(value);
  const modelKey = normalizeKey(requestedModel);

  if (!key) return true;
  if (modelKey && key === modelKey) return true;

  return /\b(price|prices|pricing|pricelist|price list|rate list|on road|onroad|on-road|ex showroom|exshowroom|ex-showroom|new car|variant|variants|show|list)\b/.test(key);
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

    const text = String(value || "").replace(/,/g, "").trim();
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
  values.find((value) => value !== undefined && value !== null && value !== "") || "";

const getEntities = (toolPlan = {}) => ({
  ...(toolPlan.entities || {}),
  ...(toolPlan.input?.entities || {}),
});

const getFilters = (toolPlan = {}) => ({
  ...(toolPlan.filters || {}),
  ...(toolPlan.input?.filters || {}),
});

const getRequestedModel = ({ toolPlan = {}, context = {}, userMessage = "" } = {}) => {
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
  cleanText(first(row.model, row.raw?.model, row.modelName, row.raw?.modelName));

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
        first(item.label, item.text, item.name, item.key, `Other charge ${index + 1}`),
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

const exactModelFilter = ({ rows = [], requestedModel = "", userMessage = "" } = {}) => {
  const modelKey = normalizeKey(requestedModel);
  const messageWantsNLine = explicitNLineRequest(`${requestedModel} ${userMessage}`);

  if (!modelKey) return rows;

  // Soft ambiguity rule:
  // "Venue" means regular Venue. "Venue N Line" means N Line.
  // Same logic also protects i20 / Creta from N Line leakage.
  if (!messageWantsNLine && /\b(venue|i20|creta)\b/.test(modelKey)) {
    return rows.filter((row) => !isNLineModel(row.model));
  }

  if (messageWantsNLine) {
    return rows.filter((row) => isNLineModel(row.model) || isNLineModel(row.variant));
  }

  return rows;
};

const buildVehicle = ({ rows = [], requestedMake = "", requestedModel = "", city = "" } = {}) => {
  const firstRow = rows[0] || {};
  const make = cleanText(first(requestedMake, firstRow.make, firstRow.brand));
  const model = stripMakeFromModel(cleanText(first(firstRow.model, requestedModel)), make);
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
    imageUrl: firstRow.imageUrl || "",
    variantCount: rows.length,
    priceRange:
      minPrice && maxPrice
        ? `${formatMoney(minPrice)} – ${formatMoney(maxPrice)}`
        : "",
    exShowroomPrice: minPrice ? formatMoney(minPrice) : "",
    startingOnRoadPrice: rows[0]?.onRoadPrice ? formatMoney(rows[0].onRoadPrice) : "",
    fuelText: [...new Set(rows.map((row) => row.fuel).filter(Boolean))].join(" / "),
    transmissionText: [...new Set(rows.map((row) => row.transmission).filter(Boolean))].join(" / "),
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

const buildActions = ({ vehicle, requestedModel = "", requestedVariant = "", softAlternatives = [] } = {}) => {
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
          displayName: buildVehicleDisplayName(alternative.make || vehicle?.make, alternative.model),
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
      label: requestedVariant ? "Show price breakup" : "Explain on-road charges",
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

const buildSoftAlternatives = ({ requestedModel = "", requestedMake = "", userMessage = "" } = {}) => {
  const key = normalizeKey(`${requestedModel} ${userMessage}`);
  const explicitNLine = explicitNLineRequest(key);

  if (explicitNLine) return [];

  if (/\bvenue\b/.test(key)) {
    return [{ make: requestedMake || "Hyundai", model: "Venue N Line", label: "Venue N Line" }];
  }

  if (/\bcreta\b/.test(key)) {
    return [{ make: requestedMake || "Hyundai", model: "Creta N Line", label: "Creta N Line" }];
  }

  if (/\bi20\b/.test(key)) {
    return [{ make: requestedMake || "Hyundai", model: "i20 N Line", label: "i20 N Line" }];
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
  const baseModel = cleanVehicleText(cleanModel.replace(/\bn\s*[- ]?\s*line\b/i, ""));

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
    ? [
        { make: makeRegex },
        { brand: makeRegex },
        { brandName: makeRegex },
      ]
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
    rows = await collection
      .find(query)
      .limit(limit)
      .toArray();

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


export const runVehiclePricelistNewCarsTool = async (args = {}) => {
  const { toolPlan = {}, context = {}, userMessage = "" } = args;

  const requestedMake = getRequestedMake(args);
  const requestedModel = getRequestedModel(args);
  const rawRequestedVariant = getRequestedVariant(args);
  const requestedVariant = sanitizeRequestedVariant(rawRequestedVariant, {
    requestedModel,
    requestedMake,
  });
  const requestedCity = getRequestedCity(args);

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

  const softAlternatives = buildSoftAlternatives({
    requestedModel,
    requestedMake,
    userMessage,
  });

  const vehicle = buildVehicle({
    rows,
    requestedMake,
    requestedModel,
    city: requestedCity,
  });

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
    rows,
    records: rows,
    variants: rows,
    totalVariants: rows.length,
    count: rows.length,
    matched: rows.length,
    summary: {
      ...(rawResult.summary || {}),
      minPrice: rows.length ? Math.min(...rows.map((row) => row.exShowroomPrice || row.onRoadPrice || 0).filter(Boolean)) : 0,
      maxPrice: rows.length ? Math.max(...rows.map((row) => row.exShowroomPrice || row.onRoadPrice || 0).filter(Boolean)) : 0,
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
