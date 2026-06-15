import mongoose from "mongoose";
import {
  normalizeSearchKey,
  normalizeText,
  normalizeCity,
} from "./aiAgent.planSchema.js";

const DEFAULT_DOC_LIMIT = Number(
  process.env.ACI_ENTITY_INDEX_DOC_LIMIT || 10000,
);

const DEFAULT_ENTITY_CITY_SLUG =
  String(process.env.ACI_ENTITY_INDEX_CITY_SLUG || "new-delhi").trim() ||
  "new-delhi";

const DEFAULT_COLLECTION_NAMES = [
  "aci_vehicle_model_summary",
  "aci_vehicle_price_rows",
];


export const unique = (items = []) => [...new Set(items.filter(Boolean))];

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

export const getDb = () => {
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

export const discoverCollections = async () => {
  const configured = envCollectionNames();

  if (configured.length) return unique(configured);

  // Product rule:
  // Runtime entity resolution must not scan broad raw/scraper collections.
  // ACI Assist should use optimized read models by default.
  // If a deeper local audit is needed, set ACI_ENTITY_INDEX_COLLECTIONS explicitly.
  return DEFAULT_COLLECTION_NAMES;
};

const buildProjection = () => ({
  brand: 1,
  make: 1,
  makeKey: 1,
  model: 1,
  modelKey: 1,
  fullModel: 1,
  displayName: 1,
  variant: 1,
  variantKey: 1,
  city: 1,
  citySlug: 1,
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

export const safeFindDocs = async (db, collectionName) => {
  try {
    if (collectionName === "aci_vehicle_model_summary") {
      return await db
        .collection(collectionName)
        .find(
          {},
          {
            projection: {
              brand: 1,
              make: 1,
              makeKey: 1,
              model: 1,
              modelKey: 1,
              fullModel: 1,
              displayName: 1,
              city: 1,
              citySlug: 1,
              minExShowroomPrice: 1,
              maxExShowroomPrice: 1,
              fuelText: 1,
              transmissionText: 1,
              active: 1,
            },
            limit: 1200,
          },
        )
        .sort({ modelKey: 1, citySlug: 1 })
        .hint("aci_model_summary_model_city")
        .batchSize(1200)
        .toArray();
    }

    if (collectionName === "aci_vehicle_price_rows") {
      return await db
        .collection(collectionName)
        .find(
          {
            citySlug: DEFAULT_ENTITY_CITY_SLUG,
          },
          {
            projection: {
              brand: 1,
              make: 1,
              makeKey: 1,
              model: 1,
              modelKey: 1,
              fullModel: 1,
              variant: 1,
              variantKey: 1,
              city: 1,
              citySlug: 1,
              fuel: 1,
              fuelType: 1,
              transmission: 1,
              exShowroomPrice: 1,
              onRoadPrice: 1,
              active: 1,
            },
            limit: DEFAULT_DOC_LIMIT,
          },
        )
        .sort({ citySlug: 1, exShowroomPrice: 1 })
        .hint({ citySlug: 1, exShowroomPrice: 1 })
        .batchSize(DEFAULT_DOC_LIMIT)
        .toArray();
    }

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
      .batchSize(DEFAULT_DOC_LIMIT)
      .toArray();
  } catch (error) {
    console.warn(
      `[ACI Assist] vehicle entity index skipped ${collectionName}: ${error.message}`,
    );
    return [];
  }
};

export const normalizeModelRecord = (doc = {}, collectionName = "") => {
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

export const normalizeVariantRecord = (
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

export const normalizeColorRecord = (
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

export const mergeModel = (map, record) => {
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

export const mergeVariant = (map, record) => {
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

export const mergeColor = (map, record) => {
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

export const finalizeIndex = ({ modelMap, variantMap, colorMap }) => {
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
