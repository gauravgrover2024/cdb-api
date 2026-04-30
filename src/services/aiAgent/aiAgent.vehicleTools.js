import mongoose from "mongoose";
import Vehicle from "../../models/Vehicle.js";
import VehicleFeature from "../../models/VehicleFeature.js";
import { getFieldMap } from "./aiAgent.fieldMaps.js";
import {
  action,
  unavailableWidget,
  widget,
} from "./aiAgent.renderPayloads.js";
import {
  firstNumber,
  firstMeaningful,
  formatDateValue,
  normalizeCitySlug,
  pickVehiclePrice,
} from "./aiAgent.normalizers.js";
import { findLean, LIMIT, pushModuleTrace, safeId } from "./aiAgent.tools.js";
import { noteRestriction } from "./aiAgent.accessControl.js";

const titleCase = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());

const MAKE_ALIASES = ["Hyundai", "Honda", "Skoda", "Volkswagen", "Maruti", "Tata", "Mahindra", "Kia", "Toyota"];

const exactValues = (value) => {
  const clean = String(value || "").trim();
  return clean ? [...new Set([clean, clean.toLowerCase(), clean.toUpperCase(), titleCase(clean)])] : [];
};

const modelAliases = (model) => {
  const clean = String(model || "").trim();
  if (!clean) return [];
  const aliases = [clean];
  for (const make of MAKE_ALIASES) {
    const pattern = new RegExp(`^${make}\\s+`, "i");
    if (pattern.test(clean)) aliases.push(clean.replace(pattern, "").trim());
  }
  return [...new Set(aliases.filter(Boolean))];
};

const vehicleModelClause = (model, make) => {
  if (!model) return null;
  const aliases = modelAliases(model);
  const modelValues = aliases.flatMap(exactValues);
  const prefixMakes = make ? exactValues(make).filter((item) => item === titleCase(item)) : MAKE_ALIASES;
  const prefixed = prefixMakes.flatMap((brand) => aliases.flatMap((alias) => exactValues(`${brand} ${alias}`)));
  return { model: { $in: [...new Set([...modelValues, ...prefixed])] } };
};

const makeOrBrandClause = (make) => {
  if (!make) return null;
  const values = exactValues(make);
  return { $or: [{ make: { $in: values } }, { brand: { $in: values } }] };
};

const cityClause = (city) => {
  const clean = normalizeCitySlug(city || "new-delhi") || "new-delhi";
  return clean ? { city: { $regex: `^${clean.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, $options: "i" } } : null;
};

const vehicleModelQuery = (parsed, { includeCity = true } = {}) => {
  const model = parsed.entities.model || parsed.entities.models?.[0];
  const make = parsed.entities.make;
  const city = normalizeCitySlug(parsed.entities.city || "new-delhi") || "new-delhi";
  const and = [];
  if (model) and.push(vehicleModelClause(model, make));
  if (make) and.push(makeOrBrandClause(make));
  if (parsed.entities.variant) and.push({ variant: { $regex: parsed.entities.variant, $options: "i" } });
  if (includeCity && city) and.push(cityClause(city));
  return and.length ? { $and: and } : {};
};

const vehicleRow = (item) => ({
  id: safeId(item),
  make: firstMeaningful(item.make, item.brand),
  model: item.model,
  variant: item.variant,
  fuel: firstMeaningful(item.fuel, item.fuel_type),
  transmission: firstMeaningful(item.transmission, item.transmission_type),
  city: item.city,
  exShowroomPrice: item.exShowroom || item.ex_showroom,
  rto: firstMeaningful(item.rto, item.roadTax, item.other_roadTax),
  insurance: firstMeaningful(item.insurance, item.insuranceAmount, item.other_insurance),
  tcs: firstMeaningful(item.tcs, item.other_tcsCharges),
  handlingOtherCharges: firstMeaningful(item.handlingCharges, item.otherCharges, item.other_totalOtherCharges),
  optionalTotal: firstMeaningful(item.optional_total, item.optional_totalAccessories, item.optional_totalAccessoriesInRs),
  orpWithoutAccessories: firstMeaningful(item.orp_without_accessories),
  onRoadPrice: firstMeaningful(item.onRoadPrice, item.on_road_price_cardekho, item.total_on_road_with_accessories),
  year: firstMeaningful(item.year, item.activeYear),
  status: firstMeaningful(item.status, item.is_discontinued ? "Discontinued" : "Active"),
  lastUpdated: formatDateValue(firstMeaningful(item.LastPriceChangeDate, item.updatedAt, item.scrape_timestamp)),
  updatedAt: formatDateValue(firstMeaningful(item.LastPriceChangeDate, item.updatedAt, item.scrape_timestamp)),
});

const normalizeVariant = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const isFeatureYes = (value) => {
  if (value === true) return true;
  const text = String(value ?? "").trim().toLowerCase();
  return /^(yes|y|true|available|present|standard|optional|1)$/i.test(text);
};

const featureValueFor = (features = {}, featureTerm = "") => {
  const needle = normalizeVariant(featureTerm);
  if (!needle) return null;
  for (const [key, value] of Object.entries(features || {})) {
    if (normalizeVariant(key).includes(needle)) {
      return { key, value, available: isFeatureYes(value) };
    }
  }
  return null;
};

const compactVariantRows = (rows) =>
  rows.map((item) => ({
    ...vehicleRow(item),
    price: firstNumber(item.on_road_price_cardekho, item.total_on_road_with_accessories, item.onRoadPrice, item.ex_showroom, item.exShowroom),
  }));

const comparisonRowFromVariant = (row) => ({
  make: row.make,
  model: row.model,
  variant: row.variant,
  fuelOptions: row.fuel ? [row.fuel] : [],
  transmissionOptions: row.transmission ? [row.transmission] : [],
  startingPrice: row.exShowroomPrice,
  topPrice: row.onRoadPrice || row.exShowroomPrice,
  variantCount: 1,
  lastUpdated: row.lastUpdated,
  actions: [
    action("open_pricelist_prefilled", "Open Pricelist", {
      route: "/vehicles/price-list",
      query: { make: row.make, model: row.model },
    }),
  ],
});

const uniqueRows = (rows, keyFor) => {
  const seen = new Set();
  return rows.filter((row) => {
    const key = keyFor(row);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const variantGroupsForModels = async (models, trace, city = "new-delhi") => {
  const groups = await Promise.all(
    models.map(async (model) => {
      let docs = await findLean(Vehicle, { $and: [vehicleModelClause(model), cityClause(city)].filter(Boolean) }, {
        sort: { model: 1, variant: 1, city: 1 },
        limit: 80,
      });
      if (!docs.length) {
        docs = await findLean(Vehicle, vehicleModelClause(model), {
          sort: { model: 1, variant: 1, city: 1 },
          limit: 80,
        });
      }
      pushModuleTrace(trace, `Variants ${model}`, docs.length, { city: docs[0]?.city || city });
      return {
        model,
        displayModel: docs[0]?.model || model,
        make: firstMeaningful(docs[0]?.make, docs[0]?.brand),
        variants: compactVariantRows(docs),
      };
    }),
  );
  return groups;
};

const exactVariantComparison = async (variantIds, trace, context = {}) => {
  const rows = await findLean(Vehicle, { _id: { $in: variantIds } }, { limit: 12 });
  pushModuleTrace(trace, "Selected vehicle variants", rows.length);
  const compactRows = rows.length ? compactVariantRows(rows) : compactVariantRows(context.selectedVariantRows || []);
  const selectedContext = {
    selectedVariantIds: variantIds,
    selectedVariantRows: compactRows,
    selectedModels: context.selectedModels || [...new Set(compactRows.map((row) => row.model).filter(Boolean))],
    compareMode: "variants",
  };
  return {
    widgets: [
      widget("vehicle_comparison", "Selected variant comparison", {
        rows: compactRows.map(comparisonRowFromVariant),
        data: selectedContext,
        notices: rows.length ? [] : ["Using selected variants from chat context because the catalog IDs were not found in the current database query."],
      }),
    ],
    followUpSuggestions: [
      {
        label: "Show features",
        message: "Show features for selected variants",
        context: selectedContext,
        replaceContext: true,
      },
      {
        label: "Show similar cars",
        message: `Show similar cars to ${selectedContext.selectedModels?.[0] || compactRows[0]?.model || "selected model"}`,
        context: selectedContext,
        replaceContext: true,
      },
      "Open full pricelist",
    ],
  };
};

export const vehiclePricelist = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return { widgets: [unavailableWidget("Vehicle data unavailable", "You do not have catalog access.", ["Vehicles"])] };
  }
  const query = vehicleModelQuery(parsed);
  const fallbackQuery = vehicleModelQuery(parsed, { includeCity: false });
  if (!Object.keys(query).length) {
    return {
      widgets: [
        unavailableWidget(
          "Need a model",
          "Share a model such as Verna, City, or Slavia to fetch catalog data.",
          ["Vehicles"],
        ),
      ],
    };
  }
  let rows = await findLean(Vehicle, query, {
    sort: { make: 1, model: 1, variant: 1 },
    limit: 80,
  });
  const requestedCity = normalizeCitySlug(parsed.entities.city || "new-delhi") || "new-delhi";
  let usedCityFallback = false;
  if (!rows.length && JSON.stringify(query) !== JSON.stringify(fallbackQuery)) {
    rows = await findLean(Vehicle, fallbackQuery, {
      sort: { make: 1, model: 1, variant: 1, city: 1 },
      limit: 80,
    });
    usedCityFallback = true;
  }
  pushModuleTrace(trace, "Vehicles", rows.length, { city: usedCityFallback ? "available catalog rows" : requestedCity });
  if (!rows.length) {
    return { widgets: [unavailableWidget("No pricelist found", "No matching vehicle catalog records were found.", ["Vehicles"])] };
  }
  const model = parsed.entities.model || rows[0]?.model;
  const featureDocs = await findLean(VehicleFeature, vehicleModelClause(model, rows[0]?.make || rows[0]?.brand), { limit: 12 });
  pushModuleTrace(trace, "Vehicle Features", featureDocs.length);
  const wantsColors = /colors|colours/.test(parsed.lower);
  const wantsSunroof = /sunroof/.test(parsed.lower);
  const notices = [];
  if (wantsColors) notices.push("Dedicated color data was not found in the catalog fields scanned.");
  if (wantsSunroof) {
    const featureText = JSON.stringify(featureDocs).toLowerCase();
    notices.push(featureText.includes("sunroof") ? "Sunroof appears in feature data for matching variants." : "Sunroof was not found in the available feature data.");
  }
  const city = usedCityFallback ? firstMeaningful(rows[0]?.city, requestedCity) : requestedCity;
  const pricelistRows = uniqueRows(
    rows.map(vehicleRow),
    (row) => [row.city, row.make, row.model, row.variant, row.fuel, row.transmission, row.exShowroomPrice, row.onRoadPrice].join("|"),
  );
  const make = firstMeaningful(rows[0]?.make, rows[0]?.brand);
  const cities = await Vehicle.distinct("city", vehicleModelClause(model, make)).maxTimeMS(2500);
  return {
    widgets: [
      widget("vehicle_pricelist", `${model} pricelist`, {
        data: { make, model, city, cities: cities.filter(Boolean).sort(), total: pricelistRows.length, features: featureDocs, records: pricelistRows, variants: pricelistRows },
        columns: ["Make", "Model", "Variant", "Fuel", "Transmission", "City", "Ex-showroom", "RTO / road tax", "Insurance", "TCS", "Handling / other", "On-road", "Year", "Status", "Last updated"],
        rows: pricelistRows,
        records: pricelistRows,
        variants: pricelistRows,
        notices: [
          ...notices,
          usedCityFallback
            ? `${requestedCity} rows were not found. Showing available catalog rows for this model instead.`
            : "Showing new-delhi by default when city is not specified.",
          "Price breakup is shown only where stored in catalog fields.",
        ],
        actions: [
          action("open_pricelist_prefilled", "Open full pricelist", {
            route: "/vehicles/price-list",
            query: { make, model, city },
          }),
        ],
      }),
    ],
    followUpSuggestions: ["Show similar cars", "Compare with City and Slavia", "Show colors", "Show top variants", "Open full pricelist"],
  };
};

export const vehiclePriceBreakup = async (parsed, access, trace) => {
  const result = await vehiclePricelist(parsed, access, trace);
  const rows = result.widgets?.[0]?.rows || [];
  if (!rows.length) return result;
  const targetRows = rows.slice(0, LIMIT).map((row) => ({
    id: row.id,
    make: row.make,
    model: row.model,
    variant: row.variant,
    city: row.city,
    fuel: row.fuel,
    exShowroomPrice: row.exShowroomPrice,
    rto: row.rto,
    insurance: row.insurance,
    tcs: row.tcs,
    handlingOtherCharges: row.handlingOtherCharges,
    optionalTotal: row.optionalTotal,
    orpWithoutAccessories: row.orpWithoutAccessories,
    onRoadPrice: row.onRoadPrice,
    status: row.status,
    lastUpdated: row.lastUpdated,
  }));
  return {
    widgets: [
      widget("vehicle_price_breakup", "Vehicle price breakup", {
        data: {
          model: result.widgets?.[0]?.data?.model,
          city: result.widgets?.[0]?.data?.city,
          total: targetRows.length,
          availableCities: result.widgets?.[0]?.data?.cities,
        },
        rows: targetRows,
        notices: ["Only stored price fields are shown. Missing breakup values are not invented."],
      }),
    ],
    followUpSuggestions: result.followUpSuggestions,
  };
};

export const vehicleColors = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return { widgets: [unavailableWidget("Vehicle data unavailable", "You do not have catalog access.", ["Vehicles"])] };
  }
  const model = parsed.entities.model || parsed.entities.models?.[0];
  if (!model) {
    return { widgets: [unavailableWidget("Need a model", "Ask for colors with a model, for example: Show Verna colors.", ["Vehicles"])] };
  }
  const colorsMap = getFieldMap("vehicle_colors");
  const colorCollection = mongoose.connection.db.collection(colorsMap.collectionName);
  const modelRegex = new RegExp(model, "i");
  const brandRegex = parsed.entities.make ? new RegExp(parsed.entities.make, "i") : null;
  const query = {
    model: modelRegex,
    ...(brandRegex ? { brand: brandRegex } : {}),
  };
  const rows = await colorCollection
    .find(query)
    .project({ brand: 1, model: 1, color_name: 1, hex: 1, image_url: 1, last_updated: 1, scrape_timestamp: 1, source_page: 1 })
    .limit(120)
    .maxTimeMS(3500)
    .toArray();
  pushModuleTrace(trace, colorsMap.module, rows.length);
  const uniqueColors = uniqueRows(
    rows
      .map((item) => ({
        id: safeId(item),
        name: item.color_name,
        colorName: item.color_name,
        hex: item.hex,
        imageUrl: item.image_url,
        model: item.model,
        make: item.brand,
        lastUpdated: formatDateValue(firstMeaningful(item.last_updated, item.scrape_timestamp)),
        sourcePage: item.source_page,
      }))
      .filter((item) => item.name),
    (row) => `${row.make}|${row.model}|${row.name}`.toLowerCase(),
  );
  if (!uniqueColors.length) {
    return {
      widgets: [
        unavailableWidget(
          "Color data not found",
          `I checked ${colorsMap.collectionName} for ${model}, but no stored color rows matched.`,
          [colorsMap.module],
        ),
      ],
      followUpSuggestions: ["Show pricelist", "Show features", "Compare with City and Slavia"],
    };
  }
  return {
    widgets: [
      widget("vehicle_colors", `${model} colors`, {
        data: { model, total: uniqueColors.length },
        rows: uniqueColors,
        records: uniqueColors,
        notices: ["Showing only colors stored in catalog fields. No colors are inferred."],
      }),
    ],
    followUpSuggestions: ["Show pricelist", "Show features", "Compare with City and Slavia"],
  };
};

export const vehicleFeatures = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return { widgets: [unavailableWidget("Vehicle data unavailable", "You do not have catalog access.", ["Vehicles"])] };
  }
  const selectedRows = compactVariantRows(parsed.context?.selectedVariantRows || []);
  const selectedVariantNames = selectedRows.map((row) => normalizeVariant(row.variant)).filter(Boolean);
  const contextModel = parsed.context?.selectedModels?.[0] || selectedRows[0]?.model;
  const model = parsed.entities.model || parsed.entities.models?.[0] || contextModel;
  if (!model) {
    return { widgets: [unavailableWidget("Need a model", "Ask for features with a model, for example: Show features of Hyundai Verna HX8 iVT.", ["Vehicle Features"])] };
  }
  let docs = [];
  if (selectedRows.length && !parsed.entities.model) {
    const clauses = selectedRows
      .map((row) => vehicleModelClause(row.model, row.make))
      .filter(Boolean);
    docs = clauses.length
      ? await findLean(VehicleFeature, { $or: clauses }, { sort: { model: 1, variant: 1 }, limit: 240 })
      : [];
  } else {
    const query = vehicleModelClause(model, parsed.entities.make);
    docs = await findLean(VehicleFeature, query, { sort: { variant: 1 }, limit: 120 });
  }
  pushModuleTrace(trace, "Vehicle Features", docs.length);
  const variantNeedle = normalizeVariant(parsed.entities.variant);
  const matchedDocs = variantNeedle
    ? docs.filter((doc) => normalizeVariant(doc.variant).includes(variantNeedle))
    : selectedVariantNames.length
      ? docs.filter((doc) => selectedVariantNames.some((variant) => normalizeVariant(doc.variant).includes(variant) || variant.includes(normalizeVariant(doc.variant))))
    : docs;
  const rows = matchedDocs.slice(0, LIMIT).map((doc) => ({
    id: safeId(doc),
    make: doc.brand,
    model: doc.model,
    variant: doc.variant,
    bodyType: doc.body_type_bucket,
    seatingCapacity: doc.seating_capacity,
    featureGroups: Object.keys(doc.features || {}).length,
    features: doc.features || {},
    lastUpdated: formatDateValue(doc.updatedAt),
  }));
  if (!rows.length) {
    return {
      widgets: [
        unavailableWidget(
          "No feature catalogue found",
          `No feature record matched ${[model, parsed.entities.variant].filter(Boolean).join(" ")}.`,
          ["Vehicle Features"],
        ),
      ],
      followUpSuggestions: ["Show pricelist", "Show variants", "Show similar cars"],
    };
  }
  return {
    widgets: [
      widget("vehicle_features", `${model} feature catalogue`, {
        data: { model, variant: parsed.entities.variant, total: matchedDocs.length },
        rows,
        records: rows,
        notices: ["Feature values are shown only from stored feature catalogue fields."],
      }),
    ],
    followUpSuggestions: ["Show pricelist", "Compare variants", "Show similar cars"],
  };
};

export const vehicleFeatureAvailability = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return { widgets: [unavailableWidget("Vehicle data unavailable", "You do not have catalog access.", ["Vehicles"])] };
  }
  const model = parsed.entities.model || parsed.entities.models?.[0];
  const feature = parsed.entities.feature || "feature";
  if (!model || !feature) {
    return {
      widgets: [
        unavailableWidget(
          "Need a model and feature",
          "Ask something like: Does Verna have sunroof?",
          ["Vehicle Features"],
        ),
      ],
    };
  }
  const [featureDocs, priceRows] = await Promise.all([
    findLean(VehicleFeature, vehicleModelClause(model), { limit: 120 }),
    findLean(Vehicle, vehicleModelClause(model), { limit: 120 }),
  ]);
  pushModuleTrace(trace, "Vehicle Features", featureDocs.length);
  pushModuleTrace(trace, "Vehicles", priceRows.length);
  const priceByVariant = new Map(priceRows.map((item) => [normalizeVariant(item.variant), vehicleRow(item)]));
  const rows = featureDocs
    .map((item) => {
      const match = featureValueFor(item.features, feature);
      if (!match?.available) return null;
      const price = priceByVariant.get(normalizeVariant(item.variant)) || {};
      return {
        id: safeId(item),
        make: firstMeaningful(item.brand, price.make),
        model: firstMeaningful(item.model, price.model),
        variant: item.variant,
        fuel: price.fuel,
        transmission: price.transmission,
        exShowroomPrice: price.exShowroomPrice,
        onRoadPrice: price.onRoadPrice,
        feature: match.key,
        value: match.value,
        bodyType: item.body_type_bucket,
        seatingCapacity: item.seating_capacity,
        lastUpdated: formatDateValue(item.updatedAt),
      };
    })
    .filter(Boolean);

  return {
    widgets: [
      widget("vehicle_feature_answer", `${feature} availability in ${model}`, {
        data: {
          model,
          feature,
          total: rows.length,
          answer: rows.length ? "Yes" : "Not found",
          question: parsed.message,
        },
        rows,
        records: rows,
        columns: ["make", "model", "variant", "fuel", "transmission", "feature", "value", "exShowroomPrice", "onRoadPrice", "lastUpdated"],
        notices: rows.length
          ? []
          : [`${feature} was not marked as available in the feature records scanned for ${model}.`],
        actions: [
          action("open_pricelist_prefilled", "Open pricelist", {
            route: "/vehicles/price-list",
            query: { model },
          }),
        ],
      }),
    ],
    followUpSuggestions: ["Show pricelist", "Compare variants", "Show similar cars"],
  };
};

export const similarCars = async (parsed, access, trace) => {
  const selectedRows = compactVariantRows(parsed.context?.selectedVariantRows || []);
  const selectedModel = parsed.context?.selectedModels?.[0] || selectedRows[0]?.model;
  const effectiveParsed = selectedModel && !parsed.entities.model
    ? { ...parsed, entities: { ...parsed.entities, model: selectedModel, models: [selectedModel] } }
    : parsed;
  const baseResult = await vehiclePricelist(effectiveParsed, access, trace);
  const baseRows = baseResult.widgets?.[0]?.rows || [];
  if (!baseRows.length) return baseResult;
  const prices = baseRows.map((row) => Number(row.onRoadPrice || row.exShowroomPrice || 0)).filter(Boolean);
  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const anchorModel = effectiveParsed.entities.model;
  const query = {
    model: { $not: new RegExp(anchorModel, "i") },
    $or: [
      { onRoadPrice: { $gte: min * 0.85, $lte: max * 1.15 } },
      { exShowroom: { $gte: min * 0.85, $lte: max * 1.15 } },
      { ex_showroom: { $gte: min * 0.85, $lte: max * 1.15 } },
    ],
  };
  const vehicles = await findLean(Vehicle, query, { limit: 120 });
  pushModuleTrace(trace, "Vehicles similar", vehicles.length);
  const grouped = new Map();
  for (const item of vehicles) {
    const key = `${item.make || ""}:${item.model || ""}`.toLowerCase();
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(item);
  }
  const rows = [...grouped.values()].slice(0, 12).map((items) => {
    const prices = items.map(pickVehiclePrice).filter(Boolean);
    return {
      make: items[0].make,
      model: items[0].model,
      segment: firstMeaningful(items[0].segment, items[0].bodyType, items[0].body_type),
      priceRange: prices.length ? { min: Math.min(...prices), max: Math.max(...prices) } : null,
      fuelOptions: [...new Set(items.map((item) => firstMeaningful(item.fuel, item.fuel_type)).filter(Boolean))],
      transmissionOptions: [...new Set(items.map((item) => item.transmission).filter(Boolean))],
      matchingReason: "Similar catalog price band",
    };
  });
  return {
    widgets: [
      widget("similar_cars", `Similar cars to ${anchorModel}`, {
        rows,
        data: { anchorModel, priceBand: { min, max } },
        actions: rows.map((row) =>
          action("open_pricelist_prefilled", `Open ${row.model}`, {
            route: "/vehicles/price-list",
            query: { make: row.make, model: row.model },
          }),
        ),
      }),
    ],
    followUpSuggestions: ["Compare with City and Slavia", "View variants", "Open full pricelist"],
  };
};

export const vehicleComparison = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return { widgets: [unavailableWidget("Vehicle data unavailable", "You do not have catalog access.", ["Vehicles"])] };
  }
  const models = parsed.entities.models || [];
  const selectedVariantIds = parsed.context?.selectedVariantIds || parsed.filters?.selectedVariantIds;
  if (Array.isArray(selectedVariantIds) && selectedVariantIds.length >= 2) {
    return exactVariantComparison(selectedVariantIds, trace, parsed.context || {});
  }
  if (models.length < 2) {
    return { widgets: [unavailableWidget("Need models to compare", "Ask with two or more models, for example: compare Verna City Slavia.", ["Vehicles"])] };
  }
  const groups = await variantGroupsForModels(models, trace, parsed.entities.city || "Delhi");
  const rows = groups.map((group) => {
    const docs = group.variants;
    const prices = docs.map((item) => firstNumber(item.onRoadPrice, item.exShowroomPrice)).filter(Boolean);
    return {
      make: group.make,
      model: group.displayModel,
      startingPrice: prices.length ? Math.min(...prices) : null,
      topPrice: prices.length ? Math.max(...prices) : null,
      variantCount: docs.length,
      fuelOptions: [...new Set(docs.map((item) => item.fuel).filter(Boolean))],
      transmissionOptions: [...new Set(docs.map((item) => item.transmission).filter(Boolean))],
      lastUpdated: formatDateValue(docs[0]?.updatedAt),
      actions: [
        action("open_pricelist_prefilled", "Open Pricelist", {
          route: "/vehicles/price-list",
          query: { make: group.make, model: group.displayModel },
        }),
      ],
    };
  });
  return {
    widgets: [
      widget("variant_selector", "Choose variants to compare", {
        subtitle: "Pick one variant per model, then compare exact variants.",
        data: { models: groups, summary: rows },
        context: {
          comparisonModels: models,
          selectedModels: models,
          city: parsed.entities.city || groups[0]?.variants?.[0]?.city,
        },
        rows,
        actions: [
          action("show_more_inline", "Show catalogues", {
            message: `Show catalogue for ${models.join(" ")}`,
          }),
        ],
      }),
    ],
    followUpSuggestions: ["Compare top variants", "Show catalogues", "Show similar cars"],
  };
};

export const priceHistoryReport = async (parsed, access, trace) => {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);
  const query = { createdAt: { $gte: start, $lte: now } };
  if (parsed.entities.model) Object.assign(query, vehicleModelClause(parsed.entities.model));
  const rows = await findLean(Vehicle, query, { sort: { createdAt: -1 }, limit: LIMIT });
  pushModuleTrace(trace, "Vehicles", rows.length);
  return {
    widgets: [
      widget("price_history_report", "Variants added this month", {
        summary: { count: rows.length, periodStart: start.toISOString(), periodEnd: now.toISOString() },
        rows: rows.map(vehicleRow),
        notices: ["Price history is not stored as a dedicated history table. Showing records inferred from createdAt/updatedAt."],
        actions: [
          action("open_pricelist_prefilled", "Open pricelist", {
            route: "/vehicles/price-list",
            query: parsed.entities.model ? { model: parsed.entities.model } : {},
          }),
        ],
      }),
    ],
    followUpSuggestions: ["Open full pricelist", "Show top variants", "Compare with City and Slavia"],
  };
};
