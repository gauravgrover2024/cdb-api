import mongoose from "mongoose";

import {
  normalizeSearchKey,
  normalizeText,
  normalizeCity,
} from "./aiAgent.planSchema.js";

const DEFAULT_TTL_MS = Number(
  process.env.ACI_ENTITY_INDEX_TTL_MS || 15 * 60 * 1000,
);
const DEFAULT_DOC_LIMIT = Number(
  process.env.ACI_ENTITY_INDEX_DOC_LIMIT || 30000,
);

const DEFAULT_COLLECTION_NAMES = [
  "features",
  "vehicle_features",
  "vehicleprices",
  "vehicle_prices",
  "vehicle_pricelists",
  "vehicle_price_lists",
  "vehicle_colors",
  "vehiclecolors",
  "price_history",
  "pricehistories",
];

let cache = {
  builtAt: 0,
  index: null,
  promise: null,
};

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const unique = (items = []) => [...new Set(items.filter(Boolean))];

const titleCase = (value = "") =>
  String(value || "")
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => {
      const lower = part.toLowerCase();
      if (["xuv", "cvt", "ivt", "dct", "amt", "ev", "cng"].includes(lower)) {
        return lower.toUpperCase();
      }
      if (lower === "i20") return "i20";
      if (lower === "3xo") return "3XO";
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join(" ");

const pickFirst = (...values) =>
  values.find(
    (value) =>
      value !== undefined && value !== null && String(value).trim() !== "",
  );

const stripBrandPrefix = (value = "", brand = "") => {
  const text = normalizeText(value);
  const brandText = normalizeText(brand);

  if (!text || !brandText) return text;

  const key = normalizeSearchKey(text);
  const brandKey = normalizeSearchKey(brandText);

  if (key === brandKey) return text;

  if (key.startsWith(`${brandKey} `)) {
    return normalizeText(text.slice(brandText.length));
  }

  return text;
};

const stripModelPrefix = (variant = "", brand = "", model = "") => {
  let clean = normalizeText(variant);
  const brandClean = normalizeText(brand);
  const modelClean = normalizeText(model);

  if (
    brandClean &&
    normalizeSearchKey(clean).startsWith(`${normalizeSearchKey(brandClean)} `)
  ) {
    clean = normalizeText(clean.slice(brandClean.length));
  }

  if (
    modelClean &&
    normalizeSearchKey(clean).startsWith(`${normalizeSearchKey(modelClean)} `)
  ) {
    clean = normalizeText(clean.slice(modelClean.length));
  }

  return clean || normalizeText(variant);
};

const getDb = () => {
  const db = mongoose.connection?.db;
  const readyState = mongoose.connection?.readyState;

  if (!db || readyState !== 1) return null;

  return db;
};

const envCollectionNames = () =>
  String(process.env.ACI_ENTITY_INDEX_COLLECTIONS || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

const discoverCollections = async (db) => {
  const configured = envCollectionNames();

  if (configured.length) return unique(configured);

  const collections = await db
    .listCollections(
      {},
      {
        nameOnly: true,
      },
    )
    .toArray();

  const names = collections.map((item) => item.name);

  const matched = names.filter((name) =>
    /feature|vehicle|price|color|colour|car/i.test(name),
  );

  return unique([...DEFAULT_COLLECTION_NAMES, ...matched]).filter((name) =>
    names.includes(name),
  );
};

const buildProjection = () => ({
  brand: 1,
  make: 1,
  model: 1,
  variant: 1,
  city: 1,
  fuel: 1,
  fuelType: 1,
  transmission: 1,
  price: 1,
  onRoadPrice: 1,
  on_road_price: 1,
  exShowroomPrice: 1,
  ex_showroom: 1,
  exShowroom: 1,
  color: 1,
  color_name: 1,
  colour: 1,
  colorName: 1,
  image_url: 1,
  imageUrl: 1,
  active: 1,
  discontinued: 1,
  includeDiscontinued: 1,
  date: 1,
});

const safeFindDocs = async (db, collectionName) => {
  try {
    return await db
      .collection(collectionName)
      .find(
        {
          $or: [
            { model: { $exists: true, $ne: "" } },
            { variant: { $exists: true, $ne: "" } },
            { color_name: { $exists: true, $ne: "" } },
            { color: { $exists: true, $ne: "" } },
          ],
        },
        {
          projection: buildProjection(),
          limit: DEFAULT_DOC_LIMIT,
        },
      )
      .toArray();
  } catch (error) {
    console.warn(
      `[ACI Assist] vehicle entity index skipped ${collectionName}: ${error.message}`,
    );
    return [];
  }
};

const normalizeModelRecord = (doc = {}, collectionName = "") => {
  const brand = normalizeText(pickFirst(doc.brand, doc.make));
  const rawModel = normalizeText(doc.model);
  if (!rawModel) return null;

  const model = stripBrandPrefix(rawModel, brand);
  const displayName = normalizeText(`${brand} ${model}`).trim() || model;

  const modelKey = normalizeSearchKey(`${brand} ${model}`);
  const shortModelKey = normalizeSearchKey(model);

  if (!shortModelKey) return null;

  const aliases = unique([
    model,
    rawModel,
    displayName,
    stripBrandPrefix(displayName, brand),
  ])
    .map(normalizeText)
    .filter(Boolean);

  return {
    type: "model",
    brand,
    model,
    rawModel,
    displayName,
    modelKey,
    shortModelKey,
    aliases,
    sourceCollections: [collectionName],
    variantsCount: 0,
    minPrice: null,
    maxPrice: null,
    fuelTypes: [],
    transmissions: [],
    active: doc.discontinued === true ? false : doc.active !== false,
    priority: 50,
  };
};

const normalizeVariantRecord = (
  doc = {},
  collectionName = "",
  modelRecord = null,
) => {
  const brand = normalizeText(
    pickFirst(doc.brand, doc.make, modelRecord?.brand),
  );
  const rawModel = normalizeText(
    pickFirst(doc.model, modelRecord?.rawModel, modelRecord?.model),
  );
  const model = stripBrandPrefix(rawModel, brand);
  const rawVariant = normalizeText(doc.variant);

  if (!model || !rawVariant) return null;

  const variantLabel = stripModelPrefix(rawVariant, brand, model);
  const displayName = normalizeText(`${brand} ${model} ${variantLabel}`).trim();

  const price = Number(
    pickFirst(
      doc.onRoadPrice,
      doc.on_road_price,
      doc.price,
      doc.exShowroomPrice,
      doc.exShowroom,
      doc.ex_showroom,
    ),
  );

  return {
    type: "variant",
    brand,
    model,
    rawModel,
    variant: variantLabel,
    rawVariant,
    displayName,
    modelKey: normalizeSearchKey(`${brand} ${model}`),
    shortModelKey: normalizeSearchKey(model),
    variantKey: normalizeSearchKey(`${brand} ${model} ${variantLabel}`),
    shortVariantKey: normalizeSearchKey(variantLabel),
    searchKey: normalizeSearchKey(
      `${brand} ${model} ${variantLabel} ${doc.fuelType || doc.fuel || ""} ${doc.transmission || ""}`,
    ),
    city: doc.city ? normalizeCity(doc.city) : "",
    fuelType: normalizeText(pickFirst(doc.fuelType, doc.fuel)),
    transmission: normalizeText(doc.transmission),
    price: Number.isFinite(price) ? price : null,
    active: doc.discontinued === true ? false : doc.active !== false,
    sourceCollections: [collectionName],
    priority: 40,
  };
};

const normalizeColorRecord = (
  doc = {},
  collectionName = "",
  modelRecord = null,
) => {
  const brand = normalizeText(
    pickFirst(doc.brand, doc.make, modelRecord?.brand),
  );
  const rawModel = normalizeText(
    pickFirst(doc.model, modelRecord?.rawModel, modelRecord?.model),
  );
  const model = stripBrandPrefix(rawModel, brand);
  const color = normalizeText(
    pickFirst(doc.color_name, doc.colorName, doc.color, doc.colour),
  );

  if (!model || !color) return null;

  return {
    type: "color",
    brand,
    model,
    color,
    displayName: color,
    modelKey: normalizeSearchKey(`${brand} ${model}`),
    shortModelKey: normalizeSearchKey(model),
    colorKey: normalizeSearchKey(`${brand} ${model} ${color}`),
    shortColorKey: normalizeSearchKey(color),
    imageUrl: doc.image_url || doc.imageUrl || "",
    sourceCollections: [collectionName],
    priority: 30,
  };
};

const mergeModel = (map, record) => {
  if (!record) return null;

  const key = record.modelKey || record.shortModelKey;
  const existing = map.get(key);

  if (!existing) {
    map.set(key, record);
    return record;
  }

  existing.aliases = unique([
    ...(existing.aliases || []),
    ...(record.aliases || []),
  ]);
  existing.sourceCollections = unique([
    ...(existing.sourceCollections || []),
    ...(record.sourceCollections || []),
  ]);
  existing.active = existing.active || record.active;
  existing.priority = Math.max(existing.priority || 0, record.priority || 0);

  return existing;
};

const mergeVariant = (map, record) => {
  if (!record) return null;

  const key =
    record.variantKey || `${record.modelKey}:${record.shortVariantKey}`;
  const existing = map.get(key);

  if (!existing) {
    map.set(key, record);
    return record;
  }

  existing.sourceCollections = unique([
    ...(existing.sourceCollections || []),
    ...(record.sourceCollections || []),
  ]);

  existing.price = existing.price || record.price;
  existing.city = existing.city || record.city;
  existing.fuelType = existing.fuelType || record.fuelType;
  existing.transmission = existing.transmission || record.transmission;
  existing.active = existing.active || record.active;

  return existing;
};

const mergeColor = (map, record) => {
  if (!record) return null;

  const key = record.colorKey || `${record.modelKey}:${record.shortColorKey}`;
  const existing = map.get(key);

  if (!existing) {
    map.set(key, record);
    return record;
  }

  existing.imageUrl = existing.imageUrl || record.imageUrl;
  existing.sourceCollections = unique([
    ...(existing.sourceCollections || []),
    ...(record.sourceCollections || []),
  ]);

  return existing;
};

const finalizeIndex = ({ modelMap, variantMap, colorMap }) => {
  const models = [...modelMap.values()];
  const variants = [...variantMap.values()];
  const colors = [...colorMap.values()];

  for (const model of models) {
    const relatedVariants = variants.filter(
      (variant) => variant.shortModelKey === model.shortModelKey,
    );

    model.variantsCount = relatedVariants.length;
    model.fuelTypes = unique(
      relatedVariants.map((item) => item.fuelType).filter(Boolean),
    );
    model.transmissions = unique(
      relatedVariants.map((item) => item.transmission).filter(Boolean),
    );

    const prices = relatedVariants
      .map((item) => item.price)
      .filter((price) => Number.isFinite(price) && price > 0);

    model.minPrice = prices.length ? Math.min(...prices) : null;
    model.maxPrice = prices.length ? Math.max(...prices) : null;
    model.priority =
      60 +
      Math.min(20, relatedVariants.length) +
      (model.active ? 10 : 0) +
      (prices.length ? 5 : 0);
  }

  const modelAliases = [];

  for (const model of models) {
    for (const alias of model.aliases || []) {
      const aliasKey = normalizeSearchKey(alias);
      if (!aliasKey) continue;

      modelAliases.push({
        alias,
        aliasKey,
        model,
        length: aliasKey.length,
      });
    }
  }

  modelAliases.sort(
    (a, b) => b.length - a.length || b.model.priority - a.model.priority,
  );

  const variantAliases = [];

  for (const variant of variants) {
    const aliases = unique([
      variant.variant,
      variant.rawVariant,
      variant.displayName,
      `${variant.model} ${variant.variant}`,
      `${variant.brand} ${variant.model} ${variant.variant}`,
    ]);

    for (const alias of aliases) {
      const aliasKey = normalizeSearchKey(alias);
      if (!aliasKey) continue;

      variantAliases.push({
        alias,
        aliasKey,
        variant,
        length: aliasKey.length,
      });
    }
  }

  variantAliases.sort(
    (a, b) => b.length - a.length || b.variant.priority - a.variant.priority,
  );

  const colorAliases = [];

  for (const color of colors) {
    const aliases = unique([
      color.color,
      color.displayName,
      `${color.model} ${color.color}`,
      `${color.brand} ${color.model} ${color.color}`,
    ]);

    for (const alias of aliases) {
      const aliasKey = normalizeSearchKey(alias);
      if (!aliasKey) continue;

      colorAliases.push({
        alias,
        aliasKey,
        color,
        length: aliasKey.length,
      });
    }
  }

  colorAliases.sort((a, b) => b.length - a.length);

  return {
    models,
    variants,
    colors,
    modelAliases,
    variantAliases,
    colorAliases,
    builtAt: new Date().toISOString(),
    counts: {
      models: models.length,
      variants: variants.length,
      colors: colors.length,
      modelAliases: modelAliases.length,
      variantAliases: variantAliases.length,
      colorAliases: colorAliases.length,
    },
  };
};

export const buildVehicleEntityIndex = async () => {
  const db = getDb();

  if (!db) {
    return finalizeIndex({
      modelMap: new Map(),
      variantMap: new Map(),
      colorMap: new Map(),
    });
  }

  const collectionNames = await discoverCollections(db);

  const modelMap = new Map();
  const variantMap = new Map();
  const colorMap = new Map();

  for (const collectionName of collectionNames) {
    const docs = await safeFindDocs(db, collectionName);

    for (const doc of docs) {
      const modelRecord = normalizeModelRecord(doc, collectionName);
      const mergedModel = mergeModel(modelMap, modelRecord);

      const variantRecord = normalizeVariantRecord(
        doc,
        collectionName,
        mergedModel,
      );
      mergeVariant(variantMap, variantRecord);

      const colorRecord = normalizeColorRecord(
        doc,
        collectionName,
        mergedModel,
      );
      mergeColor(colorMap, colorRecord);
    }
  }

  return finalizeIndex({
    modelMap,
    variantMap,
    colorMap,
  });
};

export const getVehicleEntityIndex = async ({ forceRefresh = false } = {}) => {
  const now = Date.now();

  if (!forceRefresh && cache.index && now - cache.builtAt < DEFAULT_TTL_MS) {
    return cache.index;
  }

  if (!forceRefresh && cache.promise) return cache.promise;

  cache.promise = buildVehicleEntityIndex()
    .then((index) => {
      cache.index = index;
      cache.builtAt = Date.now();
      cache.promise = null;
      return index;
    })
    .catch((error) => {
      cache.promise = null;
      console.error(
        "[ACI Assist] Failed to build vehicle entity index:",
        error,
      );
      return finalizeIndex({
        modelMap: new Map(),
        variantMap: new Map(),
        colorMap: new Map(),
      });
    });

  return cache.promise;
};

export const clearVehicleEntityIndexCache = () => {
  cache = {
    builtAt: 0,
    index: null,
    promise: null,
  };
};

const containsAlias = (textKey = "", aliasKey = "") => {
  if (!textKey || !aliasKey) return false;

  const escaped = aliasKey.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = new RegExp(
    `(^|\\s)${escaped.replace(/\s+/g, "\\s+")}(\\s|$)`,
    "i",
  );

  return pattern.test(textKey);
};

const isGenericCityUse = (textKey = "", aliasKey = "") => {
  if (aliasKey !== "city") return false;

  return /\b(in|my|your|current|this)\s+city\b/.test(textKey);
};

export const findModelMatches = (
  index,
  message = "",
  { includeGeneric = false } = {},
) => {
  const textKey = normalizeSearchKey(message);
  const matches = [];

  for (const item of index.modelAliases || []) {
    if (!includeGeneric && isGenericCityUse(textKey, item.aliasKey)) continue;

    if (containsAlias(textKey, item.aliasKey)) {
      matches.push({
        ...item.model,
        matchedAlias: item.alias,
        confidence: item.aliasKey === item.model.shortModelKey ? 0.94 : 0.98,
      });
    }
  }

  const seen = new Set();

  return matches.filter((item) => {
    const key = item.modelKey || item.shortModelKey;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

export const findVariantMatches = (
  index,
  message = "",
  { model = "", brand = "" } = {},
) => {
  const textKey = normalizeSearchKey(message);
  const modelKey = normalizeSearchKey(`${brand} ${model}`);
  const shortModelKey = normalizeSearchKey(model);

  const matches = [];

  for (const item of index.variantAliases || []) {
    const variant = item.variant;

    if (model || brand) {
      const sameModel =
        variant.shortModelKey === shortModelKey ||
        variant.modelKey === modelKey ||
        normalizeSearchKey(variant.model) === shortModelKey;

      if (!sameModel) continue;
    }

    if (containsAlias(textKey, item.aliasKey)) {
      matches.push({
        ...variant,
        matchedAlias: item.alias,
        confidence: item.aliasKey === variant.shortVariantKey ? 0.9 : 0.96,
      });
    }
  }

  const seen = new Set();

  return matches.filter((item) => {
    const key = item.variantKey || `${item.modelKey}:${item.shortVariantKey}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

export const findColorMatches = (
  index,
  message = "",
  { model = "", brand = "" } = {},
) => {
  const textKey = normalizeSearchKey(message);
  const modelKey = normalizeSearchKey(`${brand} ${model}`);
  const shortModelKey = normalizeSearchKey(model);
  const matches = [];

  for (const item of index.colorAliases || []) {
    const color = item.color;

    if (model || brand) {
      const sameModel =
        color.shortModelKey === shortModelKey ||
        color.modelKey === modelKey ||
        normalizeSearchKey(color.model) === shortModelKey;

      if (!sameModel) continue;
    }

    if (containsAlias(textKey, item.aliasKey)) {
      matches.push({
        ...color,
        matchedAlias: item.alias,
        confidence: item.aliasKey === color.shortColorKey ? 0.9 : 0.96,
      });
    }
  }

  const seen = new Set();

  return matches.filter((item) => {
    const key = item.colorKey || `${item.modelKey}:${item.shortColorKey}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

export const resolveVehicleEntities = async ({
  message = "",
  context = {},
  selectedEntity = null,
  forceRefresh = false,
} = {}) => {
  const index = await getVehicleEntityIndex({ forceRefresh });

  const selectedVehicle =
    selectedEntity ||
    context?.selectedVehicle ||
    context?.anchorVehicle ||
    context?.vehicle ||
    {};

  const anchorModel =
    selectedVehicle?.model || context?.anchorModel || context?.model || "";

  const anchorVariant =
    selectedVehicle?.variant ||
    context?.anchorVariant ||
    context?.variant ||
    "";

  const anchorCity =
    selectedVehicle?.city || context?.anchorCity || context?.city || "";

  const modelMatches = findModelMatches(index, message);
  const primaryModel = modelMatches[0]?.model || normalizeText(anchorModel);
  const primaryBrand =
    modelMatches[0]?.brand ||
    selectedVehicle?.brand ||
    selectedVehicle?.make ||
    "";

  let comparisonModels = modelMatches.map((item) => item.model);

  const textKey = normalizeSearchKey(message);

  if (
    anchorModel &&
    /\b(compare|vs|versus|with)\b/.test(textKey) &&
    !comparisonModels.includes(anchorModel)
  ) {
    comparisonModels.unshift(anchorModel);
  }

  comparisonModels = unique(comparisonModels);

  const variantMatches = findVariantMatches(index, message, {
    model: primaryModel,
    brand: primaryBrand,
  });

  const colorMatches = findColorMatches(index, message, {
    model: primaryModel,
    brand: primaryBrand,
  });

  const primaryVariant =
    variantMatches[0]?.variant || normalizeText(anchorVariant);

  return {
    index,
    primaryModel,
    primaryBrand,
    primaryVariant,
    primaryCity: anchorCity ? normalizeCity(anchorCity) : "",
    modelMatches,
    variantMatches,
    colorMatches,
    comparisonModels,
    selectedVehicle,
    counts: index.counts,
  };
};

export const selectRepresentativeVariant = async ({
  model = "",
  brand = "",
  preferredTransmission = "",
  preferredFuel = "",
  targetPrice = null,
  selectedVariant = "",
} = {}) => {
  const index = await getVehicleEntityIndex();

  const shortModelKey = normalizeSearchKey(model);
  const modelKey = normalizeSearchKey(`${brand} ${model}`);

  const candidates = index.variants.filter((variant) => {
    if (!variant.active) return false;

    return (
      variant.shortModelKey === shortModelKey ||
      variant.modelKey === modelKey ||
      normalizeSearchKey(variant.model) === shortModelKey
    );
  });

  if (!candidates.length) {
    return {
      model,
      variantStrategy: "representative_default",
    };
  }

  if (selectedVariant) {
    const selectedKey = normalizeSearchKey(selectedVariant);
    const exact = candidates.find(
      (variant) =>
        variant.shortVariantKey === selectedKey ||
        containsAlias(normalizeSearchKey(variant.variant), selectedKey),
    );

    if (exact) return exact;
  }

  const scored = candidates.map((variant) => {
    let score = 0;

    const transmissionKey = normalizeSearchKey(variant.transmission);
    const fuelKey = normalizeSearchKey(variant.fuelType);

    if (
      preferredTransmission &&
      transmissionKey.includes(normalizeSearchKey(preferredTransmission))
    ) {
      score += 40;
    }

    if (preferredFuel && fuelKey.includes(normalizeSearchKey(preferredFuel))) {
      score += 25;
    }

    if (targetPrice && variant.price) {
      const distance = Math.abs(Number(variant.price) - Number(targetPrice));
      score += Math.max(0, 30 - distance / 50000);
    }

    if (
      /automatic|ivt|cvt|dct|amt|at/i.test(
        `${variant.variant} ${variant.transmission}`,
      )
    ) {
      score += 10;
    }

    if (
      /sx|zx|zxi|htx|gtx|alpha|creative|accomplished|top/i.test(variant.variant)
    ) {
      score += 8;
    }

    if (variant.price) score += 4;

    return {
      ...variant,
      representativeScore: score,
    };
  });

  scored.sort((a, b) => b.representativeScore - a.representativeScore);

  return scored[0];
};

export const getAutocompleteEntityMatches = async ({
  query = "",
  context = {},
  limit = 8,
} = {}) => {
  const index = await getVehicleEntityIndex();
  const q = normalizeSearchKey(query);
  const safeLimit = Math.min(Math.max(Number(limit) || 8, 1), 12);

  if (!q) return [];

  const modelMatches = [];

  for (const model of index.models) {
    const bag = normalizeSearchKey(
      `${model.brand} ${model.model} ${(model.aliases || []).join(" ")}`,
    );

    if (bag.includes(q)) {
      modelMatches.push({
        id: `model-${model.modelKey}`,
        type: "model",
        label: model.displayName || `${model.brand} ${model.model}`.trim(),
        subLabel: [
          model.variantsCount ? `${model.variantsCount} variants` : "",
          model.fuelTypes?.slice(0, 2).join(" / "),
          model.transmissions?.slice(0, 2).join(" / "),
        ]
          .filter(Boolean)
          .join(" • "),
        icon: "car",
        autotypeText: model.displayName || model.model,
        sendOnClick: false,
        priority: model.priority || 50,
        entity: {
          brand: model.brand,
          model: model.model,
        },
      });
    }
  }

  modelMatches.sort((a, b) => b.priority - a.priority);

  const topModel = modelMatches[0];

  const querySuggestions = [];

  if (topModel) {
    const modelLabel = topModel.entity.model;

    querySuggestions.push(
      {
        id: `query-price-${normalizeSearchKey(modelLabel)}`,
        type: "query",
        label: `Show ${modelLabel} price`,
        subLabel: "Variant-wise pricelist",
        icon: "tag",
        autotypeText: `Show ${modelLabel} price`,
        sendOnClick: false,
        priority: 80,
        tool: "vehicle_pricelist",
        entity: topModel.entity,
      },
      {
        id: `query-colors-${normalizeSearchKey(modelLabel)}`,
        type: "query",
        label: `Show ${modelLabel} colors`,
        subLabel: "Exterior color gallery",
        icon: "paintbrush",
        autotypeText: `Show colors of ${modelLabel}`,
        sendOnClick: false,
        priority: 76,
        tool: "vehicle_colors",
        entity: topModel.entity,
      },
      {
        id: `query-emi-${normalizeSearchKey(modelLabel)}`,
        type: "query",
        label: `Calculate EMI for ${modelLabel}`,
        subLabel: "On-road EMI estimate",
        icon: "calculator",
        autotypeText: `Calculate EMI for ${modelLabel}`,
        sendOnClick: false,
        priority: 72,
        tool: "vehicle_emi",
        entity: topModel.entity,
      },
    );
  }

  return [...modelMatches.slice(0, safeLimit), ...querySuggestions]
    .sort((a, b) => (b.priority || 0) - (a.priority || 0))
    .slice(0, safeLimit);
};

export default getVehicleEntityIndex;
