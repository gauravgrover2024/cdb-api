import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const DRY_RUN =
  String(process.env.ACI_READ_MODEL_DRY_RUN || "true").toLowerCase() !== "false";

const DEFAULT_CITY_SLUG = "new-delhi";
const DEFAULT_CITY_LABEL = "New Delhi";

const SOURCE_VEHICLES = "vehicles";
const SOURCE_COLORS = "vehicle_colors_v2";

const MODEL_SUMMARY_COLLECTION = "aci_vehicle_model_summary";
const PRICE_ROWS_COLLECTION = "aci_vehicle_price_rows";

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const normalizeKey = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const compactKey = (value = "") => normalizeKey(value).replace(/-/g, "");

const first = (...values) => {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return value;
    }
  }
  return "";
};

const getObjectishField = (value, key = "") => {
  if (!value || !key) return "";

  if (typeof value === "object" && !Array.isArray(value)) {
    return value[key] ?? "";
  }

  const raw = String(value || "");
  if (!raw) return "";

  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  const quotedPattern = new RegExp(
    `['"]${escaped}['"]\\s*:\\s*['"]([^'"]*)['"]`,
    "i",
  );

  const quoted = raw.match(quotedPattern);
  if (quoted?.[1]) return quoted[1];

  const numericPattern = new RegExp(
    `['"]${escaped}['"]\\s*:\\s*([0-9][0-9,\\.]*)(?:\\s|,|})`,
    "i",
  );

  const numeric = raw.match(numericPattern);
  if (numeric?.[1]) return numeric[1];

  return "";
};

const getRawPriceField = (doc = {}, key = "") =>
  getObjectishField(doc.raw_price_json, key) ||
  getObjectishField(doc.raw, key);

const inferTransmissionFromText = () => {
  // Intentionally disabled.
  // Transmission must come from explicit source fields only.
  return "";
};

const toNumber = (value) => {
  if (typeof value === "number" && Number.isFinite(value)) return value;

  const raw = String(value || "").replace(/[₹,\s]/g, "").trim();
  if (!raw) return null;

  const lower = raw.toLowerCase();

  const num = Number(lower.replace(/l|lac|lakh|cr|crore/g, ""));
  if (!Number.isFinite(num)) return null;

  if (/cr|crore/.test(lower)) return Math.round(num * 10000000);
  if (/l|lac|lakh/.test(lower)) return Math.round(num * 100000);

  return Math.round(num);
};

const formatLakhs = (amount) => {
  if (!Number.isFinite(amount) || amount <= 0) return "";
  const lakhs = amount / 100000;
  if (lakhs >= 100) return `₹${(lakhs / 100).toFixed(2)} Cr`;
  return `₹${lakhs.toFixed(2)}L`;
};

const priceLabel = (min, max) => {
  if (!min && !max) return "";
  if (min && max && min !== max) return `${formatLakhs(min)} – ${formatLakhs(max)}`;
  return formatLakhs(min || max);
};

const sortByPreferredOrder = (values = [], preferred = []) => {
  const order = new Map(preferred.map((item, index) => [normalizeKey(item), index]));

  return [...values].sort((a, b) => {
    const aOrder = order.has(normalizeKey(a)) ? order.get(normalizeKey(a)) : 999;
    const bOrder = order.has(normalizeKey(b)) ? order.get(normalizeKey(b)) : 999;

    if (aOrder !== bOrder) return aOrder - bOrder;
    return String(a).localeCompare(String(b));
  });
};

const stripMakeFromModel = (model = "", make = "") => {
  const cleanModel = cleanText(model);
  const cleanMake = cleanText(make);
  if (!cleanModel || !cleanMake) return cleanModel;

  const pattern = new RegExp(`^${cleanMake.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s+`, "i");
  return cleanText(cleanModel.replace(pattern, ""));
};

const getMake = (doc = {}) =>
  cleanText(first(doc.make, doc.brand, doc.brandName, doc.manufacturer, doc.oem));

const getModel = (doc = {}) => {
  const make = getMake(doc);
  const raw = cleanText(
    first(
      doc.model,
      doc.modelName,
      doc.model_name,
      doc.displayModel,
      doc.rootModel,
      doc.fullModel,
      doc.carModel,
    ),
  );

  return stripMakeFromModel(raw, make);
};

const getVariant = (doc = {}) => {
  const make = getMake(doc);
  const model = getModel(doc);

  const raw = cleanText(
    first(
      getRawPriceField(doc, "variantShortName"),
      getRawPriceField(doc, "variantDisplayId"),
      getRawPriceField(doc, "variantDisplayName"),
      doc.variantShortName,
      doc.variantDisplayId,
      doc.variantDisplayName,
      doc.variant,
      doc.variantName,
      doc.variant_name,
      doc.displayVariant,
      doc.version,
      doc.versionName,
      doc.name,
      doc.title,
    ),
  );

  let value = raw;

  if (make) {
    value = value.replace(
      new RegExp(`^${make.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s+`, "i"),
      "",
    );
  }

  if (model) {
    value = value.replace(
      new RegExp(`^${model.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s+`, "i"),
      "",
    );
  }

  return cleanText(value);
};

const getCitySlug = (doc = {}) =>
  normalizeKey(first(doc.citySlug, doc.city_slug, doc.city, doc.cityName, DEFAULT_CITY_SLUG)) ||
  DEFAULT_CITY_SLUG;

const getCityLabel = (doc = {}) =>
  cleanText(
    first(
      doc.cityLabel,
      doc.cityName,
      getRawPriceField(doc, "variantCity"),
      getRawPriceField(doc, "cityName"),
      doc.city,
    ),
  ) || DEFAULT_CITY_LABEL;

const getExShowroom = (doc = {}) =>
  toNumber(
    first(
      doc.exShowroomPriceNumeric,
      doc.exShowroomPrice,
      doc.ex_showroom_price,
      doc.price,
      doc.priceNumeric,
      doc.exShowroom,
      getRawPriceField(doc, "exShowRoom"),
      getRawPriceField(doc, "exShowroom"),
      getRawPriceField(doc, "exShowroomPrice"),
      getRawPriceField(doc, "threeDigitExShowRoomPrice"),
      doc.raw_price_json?.exShowRoom,
      doc.raw_price_json?.ex_showroom_price,
      doc.raw_price_json?.exShowroomPrice,
      doc.raw?.exShowRoom,
      doc.raw?.exShowroomPrice,
    ),
  );

const getOnRoad = (doc = {}) =>
  toNumber(
    first(
      doc.onRoadPriceNumeric,
      doc.onRoadPrice,
      doc.on_road_price,
      doc.startingOnRoadPrice,
      getRawPriceField(doc, "onRoadPriceOfVariant"),
      getRawPriceField(doc, "ORPWithoutOptionAccessories"),
      getRawPriceField(doc, "onRoadPriceInIndianFormat"),
      getRawPriceField(doc, "threeDigitOnROadPrice"),
      doc.raw_price_json?.onRoadPrice,
      doc.raw_price_json?.on_road_price,
      doc.raw?.onRoadPrice,
    ),
  );

const getFuel = (doc = {}) =>
  cleanText(
    first(
      doc.fuel,
      doc.fuelType,
      doc.fuel_type,
      getRawPriceField(doc, "variantFuelType"),
      doc.raw?.fuel,
      doc.raw_price_json?.fuel,
    ),
  );

const getTransmission = (doc = {}) =>
  cleanText(
    first(
      doc.transmission,
      doc.transmissionType,
      doc.transmission_type,
      getRawPriceField(doc, "transmission"),
      getRawPriceField(doc, "transmissionType"),
      doc.raw?.transmission,
      doc.raw_price_json?.transmission,
    ),
  );

const getBodyType = (doc = {}) =>
  cleanText(
    first(
      doc.bodyType,
      doc.body_type,
      getRawPriceField(doc, "bodyType"),
      doc.raw?.bodyType,
      doc.raw_price_json?.bodyType,
    ),
  );

const pickHeroFromColorDoc = (doc = {}) => {
  if (!doc) return {};

  const colors = Array.isArray(doc.colors) ? doc.colors : [];
  const firstColor = colors.find(Boolean) || {};

  const imageUrl = first(
    doc.displayNormalizedImageUrl,
    doc.heroImageUrl,
    doc.heroImage,
    doc.defaultNormalizedImageUrl,
    doc.defaultColorImageUrl,
    firstColor.normalizedImageUrl,
    firstColor.cleanImageUrl,
    firstColor.imageUrl,
    firstColor.stagedImageUrl,
  );

  const frame = first(
    doc.displayFrameMeta,
    doc.heroFrameMeta,
    doc.defaultFrameMeta,
    firstColor.imageFrame,
    firstColor.frameMeta,
    firstColor.displayFrameMeta,
  );

  const colorName = cleanText(
    first(
      doc.displayColorName,
      doc.colorName,
      doc.color_name,
      firstColor.name,
      firstColor.colorName,
      firstColor.color_name,
    ),
  );

  return {
    imageUrl: imageUrl || "",
    normalizedImageUrl: imageUrl || "",
    imageFrame: frame && typeof frame === "object" ? frame : null,
    colorName: colorName || "Display",
  };
};

const loadColorHeroMap = async (db) => {
  const map = new Map();

  const docs = await db
    .collection(SOURCE_COLORS)
    .find(
      {},
      {
        projection: {
          brand: 1,
          make: 1,
          brand_slug: 1,
          model: 1,
          model_slug: 1,
          modelKey: 1,
          displayNormalizedImageUrl: 1,
          displayNormalizedImagePngUrl: 1,
          displayStagedImageUrl: 1,
          heroImageUrl: 1,
          heroImage: 1,
          defaultNormalizedImageUrl: 1,
          defaultColorImageUrl: 1,
          displayFrameMeta: 1,
          heroFrameMeta: 1,
          defaultFrameMeta: 1,
          colors: 1,
          activeColorCount: 1,
          updatedAt: 1,
        },
      },
    )
    .toArray();

  for (const doc of docs) {
    const make = cleanText(first(doc.make, doc.brand));
    const model = cleanText(first(doc.model, doc.modelName));
    const makeKey = normalizeKey(first(doc.brand_slug, make));
    const modelKey = normalizeKey(first(doc.model_slug, doc.modelKey, model));

    if (!modelKey) continue;

    const key = `${makeKey}:${modelKey}`;
    const existing = map.get(key);

    const hero = pickHeroFromColorDoc(doc);
    const score =
      (hero.imageUrl ? 10 : 0) +
      (hero.imageFrame ? 5 : 0) +
      (Number(doc.activeColorCount || 0) > 0 ? 2 : 0);

    if (!existing || score > existing.score) {
      map.set(key, {
        score,
        hero,
        colorCount: Array.isArray(doc.colors)
          ? doc.colors.length
          : Number(doc.activeColorCount || 0),
      });
    }
  }

  return map;
};

const getFeatureValue = (feature) => {
  if (!feature) return "";

  if (typeof feature === "string" || typeof feature === "number") {
    return cleanText(feature);
  }

  if (typeof feature === "object" && !Array.isArray(feature)) {
    return cleanText(
      first(
        feature.value,
        feature.displayValue,
        feature.text,
        feature.label,
        feature.name,
      ),
    );
  }

  return "";
};

const deriveVariantKeyFromFeatureDoc = (doc = {}) => {
  const direct = normalizeKey(getVariant(doc));
  if (direct) return direct;

  const rawSlug = normalizeKey(first(doc.variant_slug, doc.variantSlug));
  const modelKey = normalizeKey(first(doc.model_slug, doc.modelKey, getModel(doc)));
  const makeKey = normalizeKey(first(doc.brand_slug, doc.makeKey, getMake(doc)));

  return rawSlug
    .replace(`${makeKey}-${modelKey}-`, "")
    .replace(`${modelKey}-`, "")
    .replace(`${makeKey}-`, "");
};

const loadVehicleFeatureTransmissionMap = async (db) => {
  const map = new Map();

  const exists = await db
    .listCollections({ name: "vehicle_features" }, { nameOnly: true })
    .hasNext();

  if (!exists) return map;

  const rows = await db
    .collection("vehicle_features")
    .find(
      {
        features: { $exists: true },
      },
      {
        projection: {
          brand: 1,
          make: 1,
          model: 1,
          modelName: 1,
          model_slug: 1,
          modelKey: 1,
          variant: 1,
          variantName: 1,
          variant_slug: 1,
          variantSlug: 1,
          features: 1,
        },
      },
    )
    .toArray();

  for (const row of rows) {
    const modelKey = normalizeKey(first(row.model_slug, row.modelKey, getModel(row)));
    const variantKey = deriveVariantKeyFromFeatureDoc(row);

    if (!modelKey || !variantKey || !row.features || typeof row.features !== "object") {
      continue;
    }

    const entry = Object.entries(row.features).find(([featureKey]) =>
      String(featureKey || "").toLowerCase().includes("transmission type"),
    );

    if (!entry) continue;

    const [sourceKey, sourceValue] = entry;
    const transmission = getFeatureValue(sourceValue);

    if (!transmission) continue;

    map.set(`${modelKey}:${variantKey}`, {
      transmission,
      transmissionKey: normalizeKey(transmission),
      source: `vehicle_features.features.${sourceKey}`,
    });
  }

  return map;
};

const loadVariantGearboxMap = async (db) => {
  const map = new Map();

  const exists = await db
    .listCollections({ name: "vehicle_variant_feature_matrix_v2" }, { nameOnly: true })
    .hasNext();

  if (!exists) return map;

  const rows = await db
    .collection("vehicle_variant_feature_matrix_v2")
    .find(
      {},
      {
        projection: {
          modelKey: 1,
          variantKey: 1,
          "featuresByKey.gearbox": 1,
        },
      },
    )
    .toArray();

  for (const row of rows) {
    const modelKey = normalizeKey(row.modelKey);
    const variantKey = normalizeKey(row.variantKey);
    const gearbox = cleanText(row.featuresByKey?.gearbox?.value);

    if (!modelKey || !variantKey || !gearbox) continue;

    map.set(`${modelKey}:${variantKey}`, {
      gearbox,
      gearboxKey: normalizeKey(gearbox),
      source: "vehicle_variant_feature_matrix_v2.featuresByKey.gearbox",
    });
  }

  return map;
};

const createIndexes = async (db) => {
  await db.collection(MODEL_SUMMARY_COLLECTION).createIndexes([
    {
      key: { makeKey: 1, modelKey: 1, citySlug: 1 },
      name: "aci_model_summary_identity",
      unique: true,
      background: true,
    },
    {
      key: { modelKey: 1, citySlug: 1 },
      name: "aci_model_summary_model_city",
      background: true,
    },
    {
      key: { makeKey: 1, citySlug: 1, minExShowroomPrice: 1 },
      name: "aci_model_summary_make_city_price",
      background: true,
    },
    {
      key: { bodyTypeKey: 1, citySlug: 1, minExShowroomPrice: 1 },
      name: "aci_model_summary_body_city_price",
      background: true,
      sparse: true,
    },
    {
      key: { updatedAt: -1 },
      name: "aci_model_summary_updated",
      background: true,
    },
  ]);

  await db.collection(PRICE_ROWS_COLLECTION).createIndexes([
    {
      key: { makeKey: 1, modelKey: 1, variantKey: 1, citySlug: 1 },
      name: "aci_price_rows_identity",
      unique: true,
      background: true,
    },
    {
      key: { modelKey: 1, citySlug: 1, sortOrder: 1 },
      name: "aci_price_rows_model_city_sort",
      background: true,
    },
    {
      key: { modelKey: 1, fuelKey: 1, transmissionKey: 1, citySlug: 1 },
      name: "aci_price_rows_model_filters",
      background: true,
    },
    {
      key: { citySlug: 1, exShowroomPrice: 1 },
      name: "aci_price_rows_city_price",
      background: true,
    },
    {
      key: { updatedAt: -1 },
      name: "aci_price_rows_updated",
      background: true,
    },
  ]);
};

const build = async () => {
  await connectDB();

  const db = mongoose.connection.db;

  console.log(`DRY_RUN=${DRY_RUN}`);
  console.log("Loading color heroes...");
  const colorHeroMap = await loadColorHeroMap(db);
  console.log(`Color hero models loaded: ${colorHeroMap.size}`);

  console.log("Loading explicit transmission values from vehicle_features...");
  const vehicleFeatureTransmissionMap = await loadVehicleFeatureTransmissionMap(db);
  console.log(`Vehicle feature transmission rows loaded: ${vehicleFeatureTransmissionMap.size}`);

  console.log("Loading variant gearbox values...");
  const variantGearboxMap = await loadVariantGearboxMap(db);
  console.log(`Variant gearbox rows loaded: ${variantGearboxMap.size}`);

  console.log("Loading vehicle rows...");
  const vehicleDocs = await db
    .collection(SOURCE_VEHICLES)
    .find(
      {},
      {
        projection: {
          make: 1,
          brand: 1,
          brandName: 1,
          model: 1,
          modelName: 1,
          model_name: 1,
          fullModel: 1,
          variant: 1,
          variantName: 1,
          variant_name: 1,
          version: 1,
          versionName: 1,
          city: 1,
          cityName: 1,
          citySlug: 1,
          city_slug: 1,
          exShowroomPriceNumeric: 1,
          exShowroomPrice: 1,
          ex_showroom_price: 1,
          price: 1,
          priceNumeric: 1,
          onRoadPriceNumeric: 1,
          onRoadPrice: 1,
          on_road_price: 1,
          startingOnRoadPrice: 1,
          fuel: 1,
          fuelType: 1,
          fuel_type: 1,
          transmission: 1,
          transmissionType: 1,
          transmission_type: 1,
          gearbox: 1,
          bodyType: 1,
          bodyTypeKey: 1,
          imageUrl: 1,
          normalizedImageUrl: 1,
          displayNormalizedImageUrl: 1,
          imageFrame: 1,
          raw_price_json: 1,
          raw: 1,
          updatedAt: 1,
          createdAt: 1,
        },
      },
    )
    .toArray();

  const now = new Date();

  const modelMap = new Map();
  const priceRows = [];

  for (const doc of vehicleDocs) {
    const make = getMake(doc);
    const model = getModel(doc);
    const variant = getVariant(doc);
    const citySlug = getCitySlug(doc);
    const city = getCityLabel(doc);

    if (!model || !variant) continue;

    const makeKey = normalizeKey(make);
    const modelKey = normalizeKey(first(doc.modelKey, model));
    const variantKey = normalizeKey(first(doc.variantKey, variant));

    if (!modelKey || !variantKey) continue;

    const gearboxMeta = variantGearboxMap.get(`${modelKey}:${variantKey}`) || {};
    const gearbox = cleanText(gearboxMeta.gearbox);
    const gearboxKey = normalizeKey(gearbox);

    const fullModel = [make, model].filter(Boolean).join(" ").trim() || model;
    const identityKey = `${makeKey}:${modelKey}:${citySlug}`;
    const colorKey = `${makeKey}:${modelKey}`;
    const colorHero = colorHeroMap.get(colorKey);

    const exShowroomPrice = getExShowroom(doc);
    const onRoadPrice = getOnRoad(doc);
    const fuel = getFuel(doc);

    const transmissionMeta =
      vehicleFeatureTransmissionMap.get(`${modelKey}:${variantKey}`) || {};

    const transmission = cleanText(
      first(
        transmissionMeta.transmission,
        getTransmission(doc),
      ),
    );

    const transmissionSource =
      transmissionMeta.source ||
      (transmission ? "vehicles.explicit_transmission_field" : "");

    priceRows.push({
      make,
      makeKey,
      model,
      modelKey,
      fullModel,
      variant,
      variantKey,
      city,
      citySlug,
      exShowroomPrice,
      onRoadPrice,
      exShowroomPriceLabel: formatLakhs(exShowroomPrice),
      onRoadPriceLabel: formatLakhs(onRoadPrice),
      fuel,
      fuelKey: normalizeKey(fuel),
      transmission,
      transmissionKey: normalizeKey(transmission),
      transmissionSource,
      gearbox,
      gearboxKey,
      gearboxSource: gearboxMeta.source || "",
      bodyType: getBodyType(doc),
      bodyTypeKey: normalizeKey(first(doc.bodyTypeKey, getBodyType(doc))),
      sortOrder: Number.isFinite(exShowroomPrice) ? exShowroomPrice : 999999999,
      source: SOURCE_VEHICLES,
      sourceVehicleId: doc._id,
      updatedAt: now,
      createdAt: now,
    });

    const existing = modelMap.get(identityKey) || {
      make,
      makeKey,
      model,
      modelKey,
      fullModel,
      city,
      citySlug,
      variantCount: 0,
      variants: new Set(),
      fuels: new Set(),
      transmissions: new Set(),
      gearboxes: new Set(),
      minExShowroomPrice: null,
      maxExShowroomPrice: null,
      minOnRoadPrice: null,
      maxOnRoadPrice: null,
      bodyType: getBodyType(doc),
      bodyTypeKey: normalizeKey(first(doc.bodyTypeKey, getBodyType(doc))),
      hero: colorHero?.hero || {
        imageUrl: first(doc.displayNormalizedImageUrl, doc.normalizedImageUrl, doc.imageUrl),
        normalizedImageUrl: first(doc.displayNormalizedImageUrl, doc.normalizedImageUrl, doc.imageUrl),
        imageFrame: doc.imageFrame || null,
        colorName: "Display",
      },
      colorCount: colorHero?.colorCount || 0,
      source: SOURCE_VEHICLES,
      updatedAt: now,
      createdAt: now,
    };

    existing.variants.add(variant);
    if (fuel) existing.fuels.add(fuel);
    if (transmission) existing.transmissions.add(transmission);
    if (gearbox) existing.gearboxes.add(gearbox);

    if (Number.isFinite(exShowroomPrice)) {
      existing.minExShowroomPrice =
        existing.minExShowroomPrice === null
          ? exShowroomPrice
          : Math.min(existing.minExShowroomPrice, exShowroomPrice);
      existing.maxExShowroomPrice =
        existing.maxExShowroomPrice === null
          ? exShowroomPrice
          : Math.max(existing.maxExShowroomPrice, exShowroomPrice);
    }

    if (Number.isFinite(onRoadPrice)) {
      existing.minOnRoadPrice =
        existing.minOnRoadPrice === null
          ? onRoadPrice
          : Math.min(existing.minOnRoadPrice, onRoadPrice);
      existing.maxOnRoadPrice =
        existing.maxOnRoadPrice === null
          ? onRoadPrice
          : Math.max(existing.maxOnRoadPrice, onRoadPrice);
    }

    if ((!existing.hero?.imageUrl || !existing.hero?.imageFrame) && colorHero?.hero?.imageUrl) {
      existing.hero = colorHero.hero;
      existing.colorCount = colorHero.colorCount || existing.colorCount;
    }

    modelMap.set(identityKey, existing);
  }

  const summaries = [...modelMap.values()].map((item) => {
    const variantList = [...item.variants].sort();
    const fuelList = sortByPreferredOrder([...item.fuels], [
      "Petrol",
      "Diesel",
      "CNG",
      "Electric",
      "Hybrid",
    ]);

    const transmissionList = sortByPreferredOrder([...item.transmissions], [
      "Manual",
      "Automatic",
      "AMT",
      "iVT",
      "CVT",
      "DCT",
      "AT",
    ]);

    const gearboxList = sortByPreferredOrder([...(item.gearboxes || new Set())], [
      "5-Speed",
      "6-Speed",
      "7-Speed",
      "8-Speed",
      "9-Speed",
      "10-Speed",
    ]);

    return {
      make: item.make,
      makeKey: item.makeKey,
      model: item.model,
      modelKey: item.modelKey,
      fullModel: item.fullModel,
      displayName: item.fullModel,
      city: item.city,
      citySlug: item.citySlug,
      variantCount: variantList.length,
      variantsPreview: variantList.slice(0, 8),
      fuelText: fuelList.join(" / "),
      transmissionText: transmissionList.join(" / "),
      gearboxText: gearboxList.join(" / "),
      fuels: fuelList,
      transmissions: transmissionList,
      gearboxes: gearboxList,
      minExShowroomPrice: item.minExShowroomPrice,
      maxExShowroomPrice: item.maxExShowroomPrice,
      minOnRoadPrice: item.minOnRoadPrice,
      maxOnRoadPrice: item.maxOnRoadPrice,
      priceRangeLabel: priceLabel(item.minExShowroomPrice, item.maxExShowroomPrice),
      onRoadPriceRangeLabel: priceLabel(item.minOnRoadPrice, item.maxOnRoadPrice),
      bodyType: item.bodyType,
      bodyTypeKey: item.bodyTypeKey,
      hero: item.hero || {},
      colorCount: item.colorCount || 0,
      source: item.source,
      updatedAt: now,
      createdAt: item.createdAt || now,
    };
  });

  console.log(`Vehicle source docs: ${vehicleDocs.length}`);
  console.log(`Model summaries to upsert: ${summaries.length}`);
  console.log(`Price rows to upsert: ${priceRows.length}`);

  const sampleSummary =
    summaries.find((item) => item.modelKey === "verna" && item.citySlug === DEFAULT_CITY_SLUG) ||
    summaries.find((item) => item.modelKey === "verna") ||
    summaries[0];

  const sampleRows = priceRows
    .filter((item) => item.modelKey === "verna" && item.citySlug === DEFAULT_CITY_SLUG)
    .slice(0, 3);

  console.log("Sample summary:");
  console.log(JSON.stringify(sampleSummary, null, 2));
  console.log("Sample price rows:");
  console.log(JSON.stringify(sampleRows, null, 2));

  if (DRY_RUN) {
    console.log("Dry run enabled. No writes performed.");
    await mongoose.disconnect();
    return;
  }

  await createIndexes(db);

  if (summaries.length) {
    const ops = summaries.map((doc) => ({
      updateOne: {
        filter: {
          makeKey: doc.makeKey,
          modelKey: doc.modelKey,
          citySlug: doc.citySlug,
        },
        update: {
          $set: Object.fromEntries(
            Object.entries(doc).filter(([key]) => key !== "createdAt"),
          ),
          $setOnInsert: {
            createdAt: doc.createdAt || now,
          },
        },
        upsert: true,
      },
    }));

    const result = await db.collection(MODEL_SUMMARY_COLLECTION).bulkWrite(ops, {
      ordered: false,
    });

    console.log("Model summary bulk result:");
    console.log(JSON.stringify(result, null, 2));
  }

  if (priceRows.length) {
    const ops = priceRows.map((doc) => ({
      updateOne: {
        filter: {
          makeKey: doc.makeKey,
          modelKey: doc.modelKey,
          variantKey: doc.variantKey,
          citySlug: doc.citySlug,
        },
        update: {
          $set: Object.fromEntries(
            Object.entries(doc).filter(([key]) => key !== "createdAt"),
          ),
          $setOnInsert: {
            createdAt: doc.createdAt || now,
          },
        },
        upsert: true,
      },
    }));

    const result = await db.collection(PRICE_ROWS_COLLECTION).bulkWrite(ops, {
      ordered: false,
    });

    console.log("Price rows bulk result:");
    console.log(JSON.stringify(result, null, 2));
  }

  await mongoose.disconnect();
};

build().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
