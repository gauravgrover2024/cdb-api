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
import { normalizeVehicleDatasetRow } from "../../utils/vehicleDatasetNormalizer.js";

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

const escapeRegex = (value = "") =>
  String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const vehicleModelClause = (model, make) => {
  if (!model) return null;
  const aliases = modelAliases(model);
  const modelValues = aliases.flatMap(exactValues);
  const prefixMakes = make ? exactValues(make).filter((item) => item === titleCase(item)) : MAKE_ALIASES;
  const prefixed = prefixMakes.flatMap((brand) => aliases.flatMap((alias) => exactValues(`${brand} ${alias}`)));
  const normalizedModels = aliases
    .map((alias) => normalizeVehicleDatasetRow({ brand: make, make, model: alias }).model_normalized)
    .filter(Boolean);
  return {
    $or: [
      { model: { $in: [...new Set([...modelValues, ...prefixed])] } },
      ...normalizedModels.map((normalized) => ({
        model_normalized: { $regex: `^${escapeRegex(normalized)}$`, $options: "i" },
      })),
    ],
  };
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

const SYNONYM_GROUPS = [
  ["amt", "automatic"],
  ["at", "automatic"],
  ["petrol", "gasoline"],
];

const normalizeSearchToken = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const expandSynonymToken = (token) => {
  const group = SYNONYM_GROUPS.find((items) => items.includes(token));
  return group || [token];
};

const vehicleSearchTokens = (value = "") => {
  const tokens = normalizeSearchToken(value)
    .split(/\s+/)
    .filter(Boolean)
    .filter((token) => !VEHICLE_QUERY_STOPWORDS.has(token));
  return [...new Set(tokens.flatMap(expandSynonymToken))];
};

const vehicleSearchPhrase = (value = "") => vehicleSearchTokens(value).join(" ");

const orderedTokenRegex = (tokens = []) =>
  tokens.length
    ? new RegExp(tokens.map(escapeRegex).join(".*"), "i")
    : null;

const normalizedRowIdentity = (row = {}) => ({
  brand: String(row.brand_normalized || row.brand || row.make || "").trim(),
  model: String(row.model_normalized || row.model || "").trim(),
  variant: String(row.variant_normalized || row.variant || "").trim(),
  searchText: normalizeSearchToken(row.search_text),
});

const rowMatchesAllTokens = (row, tokens = []) => {
  if (!tokens.length) return false;
  const haystack = normalizeSearchToken([
    row.search_text,
    row.brand_normalized,
    row.model_normalized,
    row.variant_normalized,
    row.brand,
    row.make,
    row.model,
    row.variant,
    row.fuel,
    row.fuel_type,
    row.transmission,
  ].filter(Boolean).join(" "));
  return tokens.every((token) => haystack.includes(token));
};

const scoreVehicleMatch = (row, parsed, tokens = []) => {
  const identity = normalizedRowIdentity(row);
  const phrase = tokens.join(" ");
  const entityModel = normalizeSearchToken(parsed.entities.model || parsed.entities.models?.[0]);
  const entityVariant = normalizeSearchToken(parsed.entities.variant);
  const entityMake = normalizeSearchToken(parsed.entities.make);
  let score = 0;

  if (phrase && identity.searchText === phrase) score += 1000;
  else if (phrase && identity.searchText.startsWith(phrase)) score += 900;
  else if (phrase && identity.searchText.includes(phrase)) score += 820;

  if (entityVariant && normalizeSearchToken(identity.variant) === entityVariant) score += 780;
  else if (entityVariant && normalizeSearchToken(identity.variant).includes(entityVariant)) score += 680;
  if (phrase && normalizeSearchToken(identity.variant) === phrase) score += 760;
  else if (phrase && normalizeSearchToken(identity.variant).includes(phrase)) score += 620;

  if (entityModel && normalizeSearchToken(identity.model) === entityModel) score += 560;
  else if (entityModel && normalizeSearchToken(identity.model).includes(entityModel)) score += 460;
  if (phrase && normalizeSearchToken(identity.model) === phrase) score += 540;

  if (entityMake && normalizeSearchToken(identity.brand) === entityMake) score += 80;
  if (tokens.length && rowMatchesAllTokens(row, tokens)) score += 360 + tokens.length * 8;
  return score;
};

const catalogueSuggestionRows = (rows = []) =>
  uniqueRows(
    rows.map(vehicleRow),
    (row) => [row.make, row.model, row.variant, row.fuel, row.transmission].join("|").toLowerCase(),
  ).slice(0, 3);

const buildCatalogueQuery = (parsed, { includeCity = true } = {}) => {
  const modelQuery = vehicleModelQuery(parsed, { includeCity });
  const phrase = vehicleSearchPhrase(parsed.message || parsed.rawMessage || parsed.lower || "");
  const tokens = vehicleSearchTokens(parsed.message || parsed.rawMessage || parsed.lower || "");
  const tokenRegex = orderedTokenRegex(tokens);
  const and = [];
  if (Object.keys(modelQuery).length) and.push(modelQuery);
  if (parsed.entities.make) and.push(makeOrBrandClause(parsed.entities.make));
  if (parsed.entities.variant) {
    const normalizedVariant = normalizeVehicleDatasetRow({
      brand: parsed.entities.make,
      make: parsed.entities.make,
      model: parsed.entities.model || parsed.entities.models?.[0],
      variant: parsed.entities.variant,
    }).variant_normalized;
    and.push({
      $or: [
        { variant: { $regex: parsed.entities.variant, $options: "i" } },
        { variant_normalized: { $regex: escapeRegex(normalizedVariant || parsed.entities.variant), $options: "i" } },
      ],
    });
  }
  if (!Object.keys(modelQuery).length && tokenRegex) {
    and.push({
      $or: [
        { search_text: tokenRegex },
        { model_normalized: tokenRegex },
        { variant_normalized: tokenRegex },
        { model: tokenRegex },
        { variant: tokenRegex },
      ],
    });
  }
  if (includeCity) and.push(cityClause(parsed.entities.city || "new-delhi"));
  return {
    query: and.filter(Boolean).length ? { $and: and.filter(Boolean) } : {},
    phrase,
    tokens,
  };
};

const resolveVehicleCatalogRows = async (parsed, trace, {
  includeCity = true,
  limit = 80,
  allowCityFallback = true,
  moduleName = "Vehicles",
} = {}) => {
  const built = buildCatalogueQuery(parsed, { includeCity });
  const sort = { ex_showroom: 1, exShowroom: 1, variant_normalized: 1, variant: 1 };
  let rows = Object.keys(built.query).length
    ? await findLean(Vehicle, built.query, { sort, limit: Math.max(limit * 2, limit) })
    : [];
  let usedCityFallback = false;

  if (!rows.length && includeCity && allowCityFallback) {
    const fallback = buildCatalogueQuery(parsed, { includeCity: false });
    rows = Object.keys(fallback.query).length
      ? await findLean(Vehicle, fallback.query, { sort: { ...sort, city: 1 }, limit: Math.max(limit * 2, limit) })
      : [];
    usedCityFallback = rows.length > 0;
  }

  const scored = rows
    .map((row) => ({ row, score: scoreVehicleMatch(row, parsed, built.tokens) }))
    .filter((item) => item.score > 0 || parsed.entities.model || parsed.entities.variant)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return priceNumber(firstMeaningful(a.row.ex_showroom, a.row.exShowroom)) - priceNumber(firstMeaningful(b.row.ex_showroom, b.row.exShowroom));
    })
    .map((item) => item.row)
    .slice(0, limit);

  const suggestions = scored.length
    ? []
    : catalogueSuggestionRows(
        await findLean(
          Vehicle,
          built.tokens.length
            ? {
                $or: built.tokens.map((token) => ({
                  search_text: { $regex: escapeRegex(token), $options: "i" },
                })),
              }
            : {},
          { sort, limit: 30 },
        ),
      );

  pushModuleTrace(trace, moduleName, scored.length, {
    city: usedCityFallback ? "available catalog rows" : normalizeCitySlug(parsed.entities.city || "new-delhi") || "new-delhi",
    matchPriority: "search_text > variant_normalized > model_normalized > fuzzy",
  });

  return {
    rows: scored,
    suggestions,
    usedCityFallback,
    requestedCity: normalizeCitySlug(parsed.entities.city || "new-delhi") || "new-delhi",
    phrase: built.phrase,
    tokens: built.tokens,
  };
};

const priceNumber = (value) => firstNumber(value);

const PRICE_AMOUNT_KEYS = [
  "amount",
  "value",
  "price",
  "cost",
  "charge",
  "charges",
  "amountInRs",
  "amount_in_rs",
  "priceInRs",
  "price_in_rs",
  "inr",
  "rs",
];

const PRICE_LABEL_KEYS = [
  "label",
  "name",
  "title",
  "text",
  "description",
  "chargeName",
  "charge_name",
  "key",
  "type",
];

const priceLabelFromString = (value, fallback) =>
  String(value || "")
    .replace(/₹\s*[\d,]+(?:\.\d+)?/g, "")
    .replace(/(?:rs\.?|inr)\s*[\d,]+(?:\.\d+)?/gi, "")
    .replace(/[\d,]+(?:\.\d+)?\s*$/g, "")
    .replace(/[:=-]+\s*$/g, "")
    .trim() || fallback;

const priceLineItemsFrom = (value, fallbackLabel = "Item") => {
  if (!value) return [];
  const rows = Array.isArray(value) ? value : typeof value === "object" ? Object.entries(value) : [value];

  return rows
    .map((item, index) => {
      if (Array.isArray(item)) {
        const [label, amount] = item;
        return { label: titleCase(label || `${fallbackLabel} ${index + 1}`), amount: priceNumber(amount) };
      }

      if (item && typeof item === "object") {
        const labelKey = PRICE_LABEL_KEYS.find((key) => firstMeaningful(item[key]));
        const amountKey = PRICE_AMOUNT_KEYS.find((key) => priceNumber(item[key]) > 0);
        const fallbackEntry = Object.entries(item).find(([key, entryValue]) => !PRICE_LABEL_KEYS.includes(key) && !PRICE_AMOUNT_KEYS.includes(key) && priceNumber(entryValue) > 0);
        const amount = priceNumber(amountKey ? item[amountKey] : fallbackEntry?.[1]);
        const label = firstMeaningful(
          labelKey ? item[labelKey] : "",
          fallbackEntry?.[0],
          `${fallbackLabel} ${index + 1}`,
        );
        return { label: String(label).trim(), amount };
      }

      return {
        label: priceLabelFromString(item, `${fallbackLabel} ${index + 1}`),
        amount: priceNumber(item),
      };
    })
    .filter((row) => row.label && row.amount > 0);
};

const vehicleRow = (item) => {
  const optionalItems = priceLineItemsFrom(item.optional_list, "Optional item");
  const otherItems = priceLineItemsFrom(item.other_list, "Other charge");
  const optionalOtherItems = [
    ...optionalItems,
    ...otherItems,
  ];
  const optionalOtherTotal = optionalOtherItems.reduce((sum, row) => sum + priceNumber(row.amount), 0);
  const exShowroomPrice = priceNumber(firstMeaningful(item.ex_showroom, item.exShowroom, item.ex_showroom_price_cardekho));
  const rto = priceNumber(firstMeaningful(item.rto, item.roadTax, item.rto_amount_cardekho, item.other_roadTax));
  const insurance = priceNumber(firstMeaningful(item.insurance, item.insuranceAmount, item.insurance_amount_cardekho, item.other_insurance));
  const calculatedOnRoadPrice = exShowroomPrice + rto + insurance + optionalOtherTotal;
  const storedOnRoadPrice = priceNumber(firstMeaningful(item.onRoadPrice, item.on_road_price_cardekho, item.total_on_road_with_accessories));

  return {
    id: safeId(item),
    make: firstMeaningful(item.make, item.brand),
    brand: firstMeaningful(item.brand, item.make),
    model: item.model,
    variant: item.variant,
    brand_normalized: item.brand_normalized,
    model_normalized: item.model_normalized,
    variant_normalized: item.variant_normalized,
    search_text: item.search_text,
    variant_short: item.variant_short,
    fuel: firstMeaningful(item.fuel, item.fuel_type),
    fuel_type: firstMeaningful(item.fuel_type, item.fuel),
    transmission: firstMeaningful(item.transmission, item.transmission_type),
    price: calculatedOnRoadPrice || storedOnRoadPrice || exShowroomPrice,
    colors: item.colors_normalized || [],
    colors_normalized: item.colors_normalized || [],
    city: item.city,
    exShowroomPrice,
    exShowroom: exShowroomPrice,
    ex_showroom: exShowroomPrice,
    ex_showroom_price_cardekho: firstMeaningful(item.ex_showroom_price_cardekho),
    rto,
    rto_amount_cardekho: firstMeaningful(item.rto_amount_cardekho),
    insurance,
    insurance_amount_cardekho: firstMeaningful(item.insurance_amount_cardekho),
    optional_list: item.optional_list || [],
    other_list: item.other_list || [],
    optionalItems,
    otherItems,
    optionalOtherItems,
    optionalOtherTotal,
    optionalTotal: firstMeaningful(item.optional_total, item.optional_totalAccessories, item.optional_totalAccessoriesInRs),
    optional_total: firstMeaningful(item.optional_total),
    optional_totalAccessories: firstMeaningful(item.optional_totalAccessories),
    optional_totalAccessoriesInRs: firstMeaningful(item.optional_totalAccessoriesInRs),
    other_totalOtherCharges: firstMeaningful(item.other_totalOtherCharges),
    other_totalOtherChargesInRsFormat: firstMeaningful(item.other_totalOtherChargesInRsFormat),
    tcs: firstMeaningful(item.tcs, item.other_tcsCharges),
    other_tcsCharges: firstMeaningful(item.other_tcsCharges),
    handlingOtherCharges: firstMeaningful(item.handlingCharges, item.otherCharges, item.other_totalOtherCharges),
    orpWithoutAccessories: firstMeaningful(item.orp_without_accessories),
    orp_without_accessories: firstMeaningful(item.orp_without_accessories),
    calculatedOnRoadPrice,
    storedOnRoadPrice,
    onRoadPrice: calculatedOnRoadPrice || storedOnRoadPrice,
    on_road_price_cardekho: firstMeaningful(item.on_road_price_cardekho),
    total_on_road_with_accessories: firstMeaningful(item.total_on_road_with_accessories),
    priceFormula: {
      ex_showroom: exShowroomPrice,
      rto,
      insurance,
      optionalOtherTotal,
      calculatedOnRoadPrice,
      storedOnRoadPrice,
      difference: calculatedOnRoadPrice && storedOnRoadPrice ? calculatedOnRoadPrice - storedOnRoadPrice : 0,
    },
    year: firstMeaningful(item.year, item.activeYear),
    status: firstMeaningful(item.status, item.is_discontinued ? "Discontinued" : "Active"),
    is_discontinued: Boolean(item.is_discontinued),
    LastSeenDate: item.LastSeenDate,
    LastPriceChangeDate: item.LastPriceChangeDate,
    lastUpdated: formatDateValue(firstMeaningful(item.LastPriceChangeDate, item.LastSeenDate, item.updatedAt, item.scrape_timestamp)),
    updatedAt: formatDateValue(firstMeaningful(item.LastPriceChangeDate, item.LastSeenDate, item.updatedAt, item.scrape_timestamp)),
  };
};

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

const featureAnswerForValue = (match) => {
  if (!match) return "Not found";
  if (isFeatureYes(match.value)) return "Yes";
  const text = String(match.value ?? "").trim().toLowerCase();
  if (/^(no|n|false|not available|not applicable|na|n\/a|0|-)$/i.test(text)) return "No";
  if (/no|not available|not applicable|absent|unavailable/.test(text)) return "No";
  return text ? "Yes" : "Not found";
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
  const resolved = await resolveVehicleCatalogRows(parsed, trace, {
    includeCity: true,
    limit: 80,
    moduleName: "Vehicles",
  });
  const rows = resolved.rows;
  if (!rows.length && !resolved.tokens.length && !parsed.entities.model && !parsed.entities.variant) {
    return {
      widgets: [
        unavailableWidget(
          "Need a model",
          "Share a model or variant such as Verna, Ignis, Seltos HTE, City, or Slavia to fetch catalog data.",
          ["Vehicles"],
        ),
      ],
    };
  }
  if (!rows.length) {
    return {
      widgets: [
        widget("unavailable_notice", "No pricelist found", {
          data: {
            message: "No matching vehicle catalog records were found.",
            checked: ["Vehicles"],
            suggestions: resolved.suggestions,
            closestVariants: resolved.suggestions,
            query: resolved.phrase,
          },
          notices: ["No matching vehicle catalog records were found."],
          suggestions: resolved.suggestions,
          closestVariants: resolved.suggestions,
        }),
      ],
      followUpSuggestions: resolved.suggestions.map((row) => `Did you mean: ${[row.model, row.variant].filter(Boolean).join(" ")}?`),
    };
  }
  const firstRow = vehicleRow(rows[0]);
  const model = firstMeaningful(parsed.entities.model, firstRow.model_normalized, firstRow.model);
  const make = firstMeaningful(firstRow.make, firstRow.brand, firstRow.brand_normalized);
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
  const city = resolved.usedCityFallback ? firstMeaningful(rows[0]?.city, resolved.requestedCity) : resolved.requestedCity;
  const pricelistRows = uniqueRows(
    rows.map(vehicleRow),
    (row) => [row.city, row.make, row.model, row.variant, row.fuel, row.transmission, row.exShowroomPrice, row.onRoadPrice].join("|"),
  );
  const cities = await Vehicle.distinct("city", vehicleModelClause(model, make)).maxTimeMS(2500);
  return {
    widgets: [
      widget("vehicle_pricelist", `${model} pricelist`, {
        data: {
          make,
          model,
          city,
          cities: cities.filter(Boolean).sort(),
          total: pricelistRows.length,
          features: featureDocs,
          records: pricelistRows,
          variants: pricelistRows,
          matchPriority: "search_text > variant_normalized > model_normalized > fuzzy",
          query: resolved.phrase,
        },
        columns: ["Make", "Model", "Variant", "Fuel", "Transmission", "City", "Ex-showroom", "RTO / road tax", "Insurance", "Optional / other items", "On-road", "Year", "Status", "Last updated"],
        rows: pricelistRows,
        records: pricelistRows,
        variants: pricelistRows,
        notices: [
          ...notices,
          resolved.usedCityFallback
            ? `${resolved.requestedCity} rows were not found. Showing available catalog rows for this model instead.`
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
  const resolved = await resolveVehicleCatalogRows(parsed, trace, {
    includeCity: false,
    limit: 120,
    moduleName: "Vehicles",
  });
  const catalogRows = resolved.rows.map(vehicleRow);
  const model = firstMeaningful(parsed.entities.model, catalogRows[0]?.model_normalized, catalogRows[0]?.model, parsed.entities.models?.[0]);
  const brand = firstMeaningful(parsed.entities.make, catalogRows[0]?.make, catalogRows[0]?.brand, catalogRows[0]?.brand_normalized);
  if (!model && !resolved.tokens.length) {
    return { widgets: [unavailableWidget("Need a model", "Ask for colors with a model, for example: Show Verna colors.", ["Vehicles"])] };
  }
  const colorsMap = getFieldMap("vehicle_colors");
  const colorCollection = mongoose.connection.db.collection(colorsMap.collectionName);
  const modelRegex = new RegExp(escapeRegex(model || resolved.tokens.join(" ")), "i");
  const brandRegex = brand ? new RegExp(escapeRegex(brand), "i") : null;
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
        colorName: item.color_name,
        hex: item.hex,
        imageUrl: item.image_url,
        image_url: item.image_url,
        model: item.model,
        brand: item.brand,
        make: item.brand,
        lastUpdated: formatDateValue(firstMeaningful(item.last_updated, item.scrape_timestamp)),
        sourcePage: item.source_page,
      }))
      .filter((item) => item.colorName),
    (row) => `${row.brand}|${row.model}|${row.colorName}|${row.imageUrl || row.hex}`.toLowerCase(),
  );
  const normalizedColorNames = [
    ...new Set(
      catalogRows
        .flatMap((row) => row.colors_normalized || row.colors || [])
        .map((item) => String(item || "").trim())
        .filter(Boolean),
    ),
  ];
  const variantGroups = uniqueRows(
    catalogRows
      .filter((row) => (row.colors_normalized || row.colors || []).length)
      .map((row) => ({
        id: row.id,
        brand: row.brand || row.make,
        make: row.make || row.brand,
        model: row.model_normalized || row.model,
        variant: row.variant_normalized || row.variant,
        price: row.price,
        fuel: row.fuel,
        transmission: row.transmission,
        colors: row.colors_normalized || row.colors || [],
      })),
    (row) => [row.brand, row.model, row.variant, row.fuel, row.transmission].join("|").toLowerCase(),
  );
  const fallbackColorRows = normalizedColorNames.map((colorName, index) => ({
    id: `${model || "vehicle"}-color-${index}`,
    colorName,
    model,
    brand,
    make: brand,
  }));
  if (!uniqueColors.length && !normalizedColorNames.length) {
    return {
      widgets: [
        widget("unavailable_notice", "Color data not found", {
          data: {
            message: `I checked ${colorsMap.collectionName} and vehicle catalog colors for ${model || resolved.phrase}, but no stored color rows matched.`,
            checked: [colorsMap.module],
            suggestions: resolved.suggestions,
            closestVariants: resolved.suggestions,
          },
          notices: [`I checked ${colorsMap.collectionName} and vehicle catalog colors for ${model || resolved.phrase}, but no stored color rows matched.`],
          suggestions: resolved.suggestions,
          closestVariants: resolved.suggestions,
        }),
      ],
      followUpSuggestions: ["Show pricelist", "Show features", "Compare with City and Slavia"],
    };
  }
  const displayColors = uniqueColors.length ? uniqueColors : fallbackColorRows;
  return {
    widgets: [
      widget("vehicle_colors", `${model} colors`, {
        model,
        brand: uniqueColors[0]?.brand || brand,
        colors: displayColors,
        colors_normalized: normalizedColorNames,
        variantGroups,
        data: {
          model,
          brand: uniqueColors[0]?.brand || brand,
          total: displayColors.length,
          colors: displayColors,
          colors_normalized: normalizedColorNames,
          variantGroups,
        },
        rows: displayColors,
        records: variantGroups.length ? variantGroups : displayColors,
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
  const featureDocs = await findLean(VehicleFeature, vehicleModelClause(model), { limit: 120 });
  pushModuleTrace(trace, "Vehicle Features", featureDocs.length);
  const variantNeedle = normalizeVariant(parsed.entities.variant);
  const scopedDocs = variantNeedle
    ? featureDocs.filter((item) => normalizeVariant(item.variant).includes(variantNeedle))
    : featureDocs;
  const evidenceRows = scopedDocs.map((item) => {
    const match = featureValueFor(item.features, feature);
    const answer = featureAnswerForValue(match);
    return {
      id: safeId(item),
      brand: item.brand,
      make: item.brand,
      model: item.model,
      variant: item.variant,
      featureKey: match?.key || "",
      featureValue: match?.value ?? "",
      feature: match?.key || feature,
      value: match?.value ?? "",
      answer,
      bodyType: item.body_type_bucket,
      seatingCapacity: item.seating_capacity,
      lastUpdated: formatDateValue(item.updatedAt),
    };
  });
  const yesCount = evidenceRows.filter((row) => row.answer === "Yes").length;
  const noCount = evidenceRows.filter((row) => row.answer === "No").length;
  const notFoundCount = evidenceRows.filter((row) => row.answer === "Not found").length;
  const answer =
    yesCount > 0 && noCount === 0 && notFoundCount === 0
      ? "Yes"
      : yesCount === 0 && noCount > 0 && notFoundCount === 0
        ? "No"
        : yesCount > 0 && (noCount > 0 || notFoundCount > 0)
          ? "Mixed"
          : "Not found";

  return {
    widgets: [
      widget("vehicle_feature_answer", `${feature} availability in ${model}`, {
        question: parsed.message,
        model,
        variantQuery: parsed.entities.variant,
        feature,
        answer,
        summary: {
          totalVariantsChecked: evidenceRows.length,
          yesCount,
          noCount,
          notFoundCount,
        },
        evidenceRows,
        data: {
          model,
          variantQuery: parsed.entities.variant,
          feature,
          total: evidenceRows.length,
          totalVariantsChecked: evidenceRows.length,
          yesCount,
          noCount,
          notFoundCount,
          answer,
          question: parsed.message,
          evidenceRows,
        },
        rows: evidenceRows,
        records: evidenceRows,
        columns: ["brand", "model", "variant", "featureKey", "featureValue", "answer"],
        notices: evidenceRows.length
          ? []
          : [`No ${feature} feature records were found for ${[model, parsed.entities.variant].filter(Boolean).join(" ")}.`],
        actions: [
          action("open_features", "Open features page", {
            route: "/loans/features",
            query: { brand: evidenceRows[0]?.brand || parsed.entities.make, model, variant: parsed.entities.variant },
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
