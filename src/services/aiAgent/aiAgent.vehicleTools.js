import mongoose from "mongoose";
import Vehicle from "../../models/Vehicle.js";
import VehicleFeature from "../../models/VehicleFeature.js";
import { getFieldMap } from "./aiAgent.fieldMaps.js";
import { action, unavailableWidget, widget } from "./aiAgent.renderPayloads.js";
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
import { calculateEMI } from "./aiAgent.loanCalc.js";

const titleCase = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());

const MAKE_ALIASES = [
  "Hyundai",
  "Honda",
  "Skoda",
  "Volkswagen",
  "Maruti",
  "Tata",
  "Mahindra",
  "Kia",
  "Toyota",
];

const exactValues = (value) => {
  const clean = String(value || "").trim();
  return clean
    ? [
        ...new Set([
          clean,
          clean.toLowerCase(),
          clean.toUpperCase(),
          titleCase(clean),
        ]),
      ]
    : [];
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
  const prefixMakes = make
    ? exactValues(make).filter((item) => item === titleCase(item))
    : MAKE_ALIASES;
  const prefixed = prefixMakes.flatMap((brand) =>
    aliases.flatMap((alias) => exactValues(`${brand} ${alias}`)),
  );
  const normalizedModels = aliases
    .map(
      (alias) =>
        normalizeVehicleDatasetRow({ brand: make, make, model: alias })
          .model_normalized,
    )
    .filter(Boolean);
  return {
    $or: [
      { model: { $in: [...new Set([...modelValues, ...prefixed])] } },
      ...normalizedModels.map((normalized) => ({
        model_normalized: {
          $regex: `^${escapeRegex(normalized)}$`,
          $options: "i",
        },
      })),
    ],
  };
};

const featureModelQuery = (model, make) => {
  if (!model) return {};
  const aliases = modelAliases(model);
  const prefixMakes = make
    ? exactValues(make).filter((item) => item === titleCase(item))
    : MAKE_ALIASES;
  const modelValues = [
    ...aliases.flatMap(exactValues),
    ...prefixMakes.flatMap((brand) =>
      aliases.flatMap((alias) => exactValues(`${brand} ${alias}`)),
    ),
  ];
  const and = [{ model: { $in: [...new Set(modelValues)] } }];
  if (make) and.push({ brand: { $in: exactValues(make) } });
  return { $and: and };
};

const makeOrBrandClause = (make) => {
  if (!make) return null;
  const values = exactValues(make);
  return { $or: [{ make: { $in: values } }, { brand: { $in: values } }] };
};

const cityClause = (city) => {
  const clean = normalizeCitySlug(city || "new-delhi") || "new-delhi";
  return clean
    ? {
        city: {
          $regex: `^${clean.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`,
          $options: "i",
        },
      }
    : null;
};

const vehicleModelQuery = (parsed, { includeCity = true } = {}) => {
  const model = parsed.entities.model || parsed.entities.models?.[0];
  const make = parsed.entities.make;
  const city =
    normalizeCitySlug(parsed.entities.city || "new-delhi") || "new-delhi";
  const and = [];
  if (model) and.push(vehicleModelClause(model, make));
  if (make) and.push(makeOrBrandClause(make));
  if (parsed.entities.variant)
    and.push({ variant: { $regex: parsed.entities.variant, $options: "i" } });
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

const vehicleSearchPhrase = (value = "") =>
  vehicleSearchTokens(value).join(" ");

const orderedTokenRegex = (tokens = []) =>
  tokens.length ? new RegExp(tokens.map(escapeRegex).join(".*"), "i") : null;

const normalizedRowIdentity = (row = {}) => ({
  brand: String(row.brand_normalized || row.brand || row.make || "").trim(),
  model: String(row.model_normalized || row.model || "").trim(),
  variant: String(row.variant_normalized || row.variant || "").trim(),
  searchText: normalizeSearchToken(row.search_text),
});

const rowMatchesAllTokens = (row, tokens = []) => {
  if (!tokens.length) return false;
  const haystack = normalizeSearchToken(
    [
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
    ]
      .filter(Boolean)
      .join(" "),
  );
  return tokens.every((token) => haystack.includes(token));
};

const scoreVehicleMatch = (row, parsed, tokens = []) => {
  const identity = normalizedRowIdentity(row);
  const phrase = tokens.join(" ");
  const entityModel = normalizeSearchToken(
    parsed.entities.model || parsed.entities.models?.[0],
  );
  const entityVariant = normalizeSearchToken(parsed.entities.variant);
  const entityMake = normalizeSearchToken(parsed.entities.make);
  let score = 0;

  if (phrase && identity.searchText === phrase) score += 1000;
  else if (phrase && identity.searchText.startsWith(phrase)) score += 900;
  else if (phrase && identity.searchText.includes(phrase)) score += 820;

  if (entityVariant && normalizeSearchToken(identity.variant) === entityVariant)
    score += 780;
  else if (
    entityVariant &&
    normalizeSearchToken(identity.variant).includes(entityVariant)
  )
    score += 680;
  if (phrase && normalizeSearchToken(identity.variant) === phrase) score += 760;
  else if (phrase && normalizeSearchToken(identity.variant).includes(phrase))
    score += 620;

  if (entityModel && normalizeSearchToken(identity.model) === entityModel)
    score += 560;
  else if (
    entityModel &&
    normalizeSearchToken(identity.model).includes(entityModel)
  )
    score += 460;
  if (phrase && normalizeSearchToken(identity.model) === phrase) score += 540;

  if (entityMake && normalizeSearchToken(identity.brand) === entityMake)
    score += 80;
  if (tokens.length && rowMatchesAllTokens(row, tokens))
    score += 360 + tokens.length * 8;
  return score;
};

const catalogueSuggestionRows = (rows = []) =>
  uniqueRows(rows.map(vehicleRow), (row) =>
    [row.make, row.model, row.variant, row.fuel, row.transmission]
      .join("|")
      .toLowerCase(),
  ).slice(0, 3);

const buildCatalogueQuery = (parsed, { includeCity = true } = {}) => {
  const modelQuery = vehicleModelQuery(parsed, { includeCity });
  const phrase = vehicleSearchPhrase(
    parsed.message || parsed.rawMessage || parsed.lower || "",
  );
  const tokens = vehicleSearchTokens(
    parsed.message || parsed.rawMessage || parsed.lower || "",
  );
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
        {
          variant_normalized: {
            $regex: escapeRegex(normalizedVariant || parsed.entities.variant),
            $options: "i",
          },
        },
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

const resolveVehicleCatalogRows = async (
  parsed,
  trace,
  {
    includeCity = true,
    limit = 80,
    allowCityFallback = true,
    moduleName = "Vehicles",
  } = {},
) => {
  const built = buildCatalogueQuery(parsed, { includeCity });
  const sort = {
    ex_showroom: 1,
    exShowroom: 1,
    variant_normalized: 1,
    variant: 1,
  };
  let rows = Object.keys(built.query).length
    ? await findLean(Vehicle, built.query, {
        sort,
        limit: Math.max(limit * 2, limit),
      })
    : [];
  let usedCityFallback = false;

  let showingCity =
    normalizeCitySlug(parsed.entities.city || "new-delhi") || "new-delhi";

  if (!rows.length && includeCity && allowCityFallback) {
    // If requested city is not available, first fallback specifically to Delhi/New Delhi,
    // not to all cities. This avoids empty responses for unsupported cities like Mumbai.
    const fallbackParsed = {
      ...parsed,
      entities: {
        ...parsed.entities,
        city: "new-delhi",
      },
    };

    const fallbackDelhi = buildCatalogueQuery(fallbackParsed, {
      includeCity: true,
    });

    rows = Object.keys(fallbackDelhi.query).length
      ? await findLean(Vehicle, fallbackDelhi.query, {
          sort,
          limit: Math.max(limit * 2, limit),
        })
      : [];

    if (rows.length) {
      usedCityFallback = true;
      showingCity = "new-delhi";
    }

    // Last fallback: if even Delhi does not exist, then show available catalogue rows.
    if (!rows.length) {
      const fallbackAny = buildCatalogueQuery(parsed, { includeCity: false });
      rows = Object.keys(fallbackAny.query).length
        ? await findLean(Vehicle, fallbackAny.query, {
            sort: { ...sort, city: 1 },
            limit: Math.max(limit * 2, limit),
          })
        : [];

      if (rows.length) {
        usedCityFallback = true;
        showingCity = firstMeaningful(rows[0]?.city, "new-delhi");
      }
    }
  }

  const scored = rows
    .map((row) => ({
      row,
      score: scoreVehicleMatch(row, parsed, built.tokens),
    }))
    .filter(
      (item) =>
        item.score > 0 || parsed.entities.model || parsed.entities.variant,
    )
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return (
        priceNumber(firstMeaningful(a.row.ex_showroom, a.row.exShowroom)) -
        priceNumber(firstMeaningful(b.row.ex_showroom, b.row.exShowroom))
      );
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
    city: usedCityFallback
      ? "available catalog rows"
      : normalizeCitySlug(parsed.entities.city || "new-delhi") || "new-delhi",
    matchPriority:
      "search_text > variant_normalized > model_normalized > fuzzy",
  });

  return {
    rows: scored,
    suggestions,
    usedCityFallback,
    requestedCity:
      normalizeCitySlug(parsed.entities.city || "new-delhi") || "new-delhi",
    showingCity,
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
  const rows = Array.isArray(value)
    ? value
    : typeof value === "object"
      ? Object.entries(value)
      : [value];

  return rows
    .map((item, index) => {
      if (Array.isArray(item)) {
        const [label, amount] = item;
        return {
          label: titleCase(label || `${fallbackLabel} ${index + 1}`),
          amount: priceNumber(amount),
        };
      }

      if (item && typeof item === "object") {
        const labelKey = PRICE_LABEL_KEYS.find((key) =>
          firstMeaningful(item[key]),
        );
        const amountKey = PRICE_AMOUNT_KEYS.find(
          (key) => priceNumber(item[key]) > 0,
        );
        const fallbackEntry = Object.entries(item).find(
          ([key, entryValue]) =>
            !PRICE_LABEL_KEYS.includes(key) &&
            !PRICE_AMOUNT_KEYS.includes(key) &&
            priceNumber(entryValue) > 0,
        );
        const amount = priceNumber(
          amountKey ? item[amountKey] : fallbackEntry?.[1],
        );
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
  const optionalOtherItems = [...optionalItems, ...otherItems];
  const optionalOtherTotal = optionalOtherItems.reduce(
    (sum, row) => sum + priceNumber(row.amount),
    0,
  );
  const exShowroomPrice = priceNumber(
    firstMeaningful(
      item.ex_showroom,
      item.exShowroom,
      item.ex_showroom_price_cardekho,
    ),
  );
  const rto = priceNumber(
    firstMeaningful(
      item.rto,
      item.roadTax,
      item.rto_amount_cardekho,
      item.other_roadTax,
    ),
  );
  const insurance = priceNumber(
    firstMeaningful(
      item.insurance,
      item.insuranceAmount,
      item.insurance_amount_cardekho,
      item.other_insurance,
    ),
  );
  const calculatedOnRoadPrice =
    exShowroomPrice + rto + insurance + optionalOtherTotal;
  const storedOnRoadPrice = priceNumber(
    firstMeaningful(
      item.onRoadPrice,
      item.on_road_price_cardekho,
      item.total_on_road_with_accessories,
    ),
  );

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
    ex_showroom_price_cardekho: firstMeaningful(
      item.ex_showroom_price_cardekho,
    ),
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
    optionalTotal: firstMeaningful(
      item.optional_total,
      item.optional_totalAccessories,
      item.optional_totalAccessoriesInRs,
    ),
    optional_total: firstMeaningful(item.optional_total),
    optional_totalAccessories: firstMeaningful(item.optional_totalAccessories),
    optional_totalAccessoriesInRs: firstMeaningful(
      item.optional_totalAccessoriesInRs,
    ),
    other_totalOtherCharges: firstMeaningful(item.other_totalOtherCharges),
    other_totalOtherChargesInRsFormat: firstMeaningful(
      item.other_totalOtherChargesInRsFormat,
    ),
    tcs: firstMeaningful(item.tcs, item.other_tcsCharges),
    other_tcsCharges: firstMeaningful(item.other_tcsCharges),
    handlingOtherCharges: firstMeaningful(
      item.handlingCharges,
      item.otherCharges,
      item.other_totalOtherCharges,
    ),
    orpWithoutAccessories: firstMeaningful(item.orp_without_accessories),
    orp_without_accessories: firstMeaningful(item.orp_without_accessories),
    calculatedOnRoadPrice,
    storedOnRoadPrice,
    onRoadPrice: calculatedOnRoadPrice || storedOnRoadPrice,
    on_road_price_cardekho: firstMeaningful(item.on_road_price_cardekho),
    total_on_road_with_accessories: firstMeaningful(
      item.total_on_road_with_accessories,
    ),
    priceFormula: {
      ex_showroom: exShowroomPrice,
      rto,
      insurance,
      optionalOtherTotal,
      calculatedOnRoadPrice,
      storedOnRoadPrice,
      difference:
        calculatedOnRoadPrice && storedOnRoadPrice
          ? calculatedOnRoadPrice - storedOnRoadPrice
          : 0,
    },
    year: firstMeaningful(item.year, item.activeYear),
    status: firstMeaningful(
      item.status,
      item.is_discontinued ? "Discontinued" : "Active",
    ),
    is_discontinued: Boolean(item.is_discontinued),
    LastSeenDate: item.LastSeenDate,
    LastPriceChangeDate: item.LastPriceChangeDate,
    lastUpdated: formatDateValue(
      firstMeaningful(
        item.LastPriceChangeDate,
        item.LastSeenDate,
        item.updatedAt,
        item.scrape_timestamp,
      ),
    ),
    updatedAt: formatDateValue(
      firstMeaningful(
        item.LastPriceChangeDate,
        item.LastSeenDate,
        item.updatedAt,
        item.scrape_timestamp,
      ),
    ),
  };
};

const normalizeVariant = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const isFeatureYes = (value) => {
  if (value === true) return true;
  const text = String(value ?? "")
    .trim()
    .toLowerCase();
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
  const text = String(match.value ?? "")
    .trim()
    .toLowerCase();
  if (/^(no|n|false|not available|not applicable|na|n\/a|0|-)$/i.test(text))
    return "No";
  if (/no|not available|not applicable|absent|unavailable/.test(text))
    return "No";
  return text ? "Yes" : "Not found";
};

const compactVariantRows = (rows) =>
  rows.map((item) => ({
    ...vehicleRow(item),
    price: firstNumber(
      item.on_road_price_cardekho,
      item.total_on_road_with_accessories,
      item.onRoadPrice,
      item.ex_showroom,
      item.exShowroom,
    ),
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

const FEATURE_GROUP_ORDER = [
  "Comfort & Convenience",
  "Interior",
  "Exterior",
  "Safety",
  "Entertainment & Communication",
  "ADAS Feature",
  "Engine & Transmission",
  "Fuel & Performance",
  "Dimensions & Capacity",
];

const FEATURE_SYNONYMS = {
  sunroof: ["sunroof", "voice assisted sunroof", "panoramic sunroof"],
  "6 airbags": ["6 airbags", "airbags", "no. of airbags", "number of airbags"],
  airbags: ["airbags", "no. of airbags", "number of airbags"],
  adas: [
    "adas",
    "advanced driver assistance",
    "lane keep",
    "adaptive cruise",
    "blind spot",
  ],
  "wireless charging": ["wireless charging", "wireless charger"],
  "ventilated seats": ["ventilated seats", "ventilated front seats"],
  "360 camera": ["360 camera", "360 degree camera", "around view monitor"],
  tpms: ["tpms", "tyre pressure"],
  isofix: ["isofix"],
  mileage: ["mileage", "arai mileage"],
  "boot space": ["boot space", "boot"],
  "ground clearance": ["ground clearance"],
  transmission: ["transmission", "gearbox"],
  engine: ["engine", "displacement"],
};

const toWords = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const hasPhrase = (value, phrase) => toWords(value).includes(toWords(phrase));

const normalizedModelLabel = (row = {}) =>
  firstMeaningful(row.model_normalized, row.model);

const displayVariant = (row = {}) =>
  firstMeaningful(row.variant_normalized, row.variant_short, row.variant);

const cityFromParsed = (parsed) =>
  normalizeCitySlug(parsed.entities.city || "new-delhi") || "new-delhi";

const priceBasis = (row = {}) =>
  firstNumber(row.onRoadPrice, row.exShowroomPrice, row.price);

const transmissionMatches = (row = {}, transmission = "") => {
  const needle = toWords(transmission);
  if (!needle) return true;
  const haystack = toWords(
    [row.transmission, row.variant, row.variant_normalized, row.search_text]
      .filter(Boolean)
      .join(" "),
  );
  if (needle === "automatic")
    return /(automatic|amt|at|cvt|dct|ivt)/i.test(haystack);
  if (needle === "manual") return /\b(manual|mt)\b/i.test(haystack);
  return haystack.includes(needle);
};

const bodyTypeMatches = (bodyType = "", needle = "") => {
  const target = toWords(needle)
    .replace(/\bsuvs\b/g, "suv")
    .replace(/\bsedans\b/g, "sedan");
  if (!target) return true;
  return toWords(bodyType).includes(target.replace("7 seater", "7"));
};

const catalogueFilterReason = (row, parsed, featureDoc = null) => {
  const reasons = [];
  const price = priceBasis(row);
  if (parsed.entities.budgetMax && price && price <= parsed.entities.budgetMax)
    reasons.push("under budget");
  if (parsed.entities.budgetMin && price && price >= parsed.entities.budgetMin)
    reasons.push("above minimum budget");
  if (
    parsed.entities.bodyType &&
    bodyTypeMatches(
      featureDoc?.body_type_bucket || row.bodyType,
      parsed.entities.bodyType,
    )
  )
    reasons.push(`${parsed.entities.bodyType} body type`);
  if (parsed.entities.fuelType && hasPhrase(row.fuel, parsed.entities.fuelType))
    reasons.push(`${parsed.entities.fuelType} fuel`);
  if (
    parsed.entities.transmission &&
    transmissionMatches(row, parsed.entities.transmission)
  )
    reasons.push(`${parsed.entities.transmission} transmission`);
  return reasons;
};

const isDiscontinuedVehicleRow = (row = {}) => {
  const status = String(
    firstMeaningful(row.status, row.vehicleStatus, row.model_status) || "",
  ).toLowerCase();

  return Boolean(
    row.is_discontinued ||
    row.discontinued ||
    row.isDiscontinued ||
    status.includes("discontinued") ||
    status.includes("inactive"),
  );
};

const includeDiscontinuedRequested = (parsed = {}) =>
  Boolean(
    parsed.entities?.includeDiscontinued ||
    /\b(discontinued|old variants?|include discontinued|show discontinued|inactive)\b/i.test(
      parsed.lower || "",
    ),
  );

const applyCatalogueFilters = (rows = [], parsed = {}) => {
  const lower = parsed.lower || "";
  const wantDiscontinued =
    /\b(discontinued|old variants?|show discontinued|include discontinued)\b/i.test(
      lower,
    );
  const includeDiscontinued = includeDiscontinuedRequested(parsed);

  return rows.filter((row) => {
    const compact = vehicleRow(row);
    const price = priceBasis(compact);
    const discontinued = isDiscontinuedVehicleRow(compact);

    // DEFAULT: active cars only. Discontinued cars are shown only when explicitly requested.
    if (!includeDiscontinued && discontinued) return false;

    // If user specifically asks discontinued, show only discontinued.
    if (wantDiscontinued && !discontinued) return false;

    if (parsed.entities.budgetMax && price && price > parsed.entities.budgetMax)
      return false;
    if (parsed.entities.budgetMin && price && price < parsed.entities.budgetMin)
      return false;
    if (
      parsed.entities.fuelType &&
      !hasPhrase(compact.fuel, parsed.entities.fuelType)
    )
      return false;
    if (
      parsed.entities.transmission &&
      !transmissionMatches(compact, parsed.entities.transmission)
    )
      return false;

    return true;
  });
};

const sortCatalogueRows = (rows = [], parsed = {}) => {
  const lower = parsed.lower || "";
  const mapped = rows.map(vehicleRow);
  const sortAsc =
    /cheapest|lowest|ascending|under|between|sorted by price/.test(lower);
  const sortDesc = /top model|top variant|highest|premium/.test(lower);
  return mapped.sort((a, b) => {
    const diff = priceBasis(a) - priceBasis(b);
    return sortDesc ? -diff : sortAsc ? diff : diff;
  });
};

const summarizeCatalogueRows = (rows = []) => {
  const prices = rows.map(priceBasis).filter(Boolean);
  const exPrices = rows
    .map((row) => firstNumber(row.exShowroomPrice))
    .filter(Boolean);
  return {
    startingPrice: prices.length
      ? Math.min(...prices)
      : exPrices.length
        ? Math.min(...exPrices)
        : null,
    topPrice: prices.length
      ? Math.max(...prices)
      : exPrices.length
        ? Math.max(...exPrices)
        : null,
    variantCount: rows.length,
    fuelOptions: [
      ...new Set(
        rows
          .map((row) => firstMeaningful(row.fuelType, row.fuel))
          .filter(Boolean),
      ),
    ],
    transmissionOptions: [
      ...new Set(rows.map((row) => row.transmission).filter(Boolean)),
    ],
    activeCount: rows.filter(
      (row) => !row.is_discontinued && !/discontinued/i.test(row.status || ""),
    ).length,
    discontinuedCount: rows.filter(
      (row) => row.is_discontinued || /discontinued/i.test(row.status || ""),
    ).length,
    lastUpdated: firstMeaningful(
      ...rows.map((row) => row.lastUpdated).filter(Boolean),
    ),
  };
};

const contextPriceRowForVariant = (priceRows = [], target = {}) => {
  const targetVariant = normalizeVariant(
    firstMeaningful(
      target.variant,
      target.variant_normalized,
      target.variantName,
      target.variant_name,
    ),
  );

  if (!targetVariant) return null;

  let bestRow = null;
  let bestScore = 0;

  for (const row of priceRows || []) {
    const rowVariant = normalizeVariant(
      firstMeaningful(
        row.variant,
        row.variant_normalized,
        row.variantName,
        row.variant_name,
      ),
    );

    if (!rowVariant) continue;

    let score = 0;

    if (rowVariant === targetVariant) score = 100;
    else if (rowVariant.includes(targetVariant)) score = 82;
    else if (targetVariant.includes(rowVariant)) score = 72;
    else {
      const targetTokens = targetVariant.split(/\s+/).filter(Boolean);
      const rowTokens = new Set(rowVariant.split(/\s+/).filter(Boolean));
      const matched = targetTokens.filter((token) => rowTokens.has(token));
      const ratio = targetTokens.length
        ? matched.length / targetTokens.length
        : 0;

      if (ratio >= 0.8) score = 64;
      else if (ratio >= 0.55) score = 42;
    }

    if (score > bestScore) {
      bestScore = score;
      bestRow = row;
    }
  }

  return bestScore >= 42 ? bestRow : null;
};

const buildVehicleCanvasContext = async (
  parsed,
  trace,
  { model, make, city, limit = 180, moduleName = "Vehicles context" } = {},
) => {
  const contextParsed = {
    ...parsed,
    entities: {
      ...parsed.entities,
      model:
        model ||
        parsed.entities.model ||
        parsed.entities.models?.[0] ||
        parsed.context?.selectedModels?.[0],
      make: make || parsed.entities.make,
      city: city || parsed.entities.city || "new-delhi",
    },
  };

  const contextModel =
    contextParsed.entities.model || contextParsed.entities.models?.[0];

  if (!contextModel) {
    return {
      make,
      brand: make,
      model,
      city: cityFromParsed(parsed),
      priceRows: [],
      pricelistRows: [],
      colors: [],
      colorGallery: [],
      heroImage: "",
      imageUrl: "",
      vehicleImageUrl: "",
      availableCities: [],
      pricingSummary: {},
      priceContextAvailable: false,
      imageContextAvailable: false,
    };
  }

  const resolved = await resolveVehicleCatalogRows(contextParsed, trace, {
    includeCity: true,
    limit,
    allowCityFallback: true,
    moduleName,
  });

  const filteredRows = applyCatalogueFilters(resolved.rows, contextParsed);
  const sourceRows = filteredRows.length ? filteredRows : resolved.rows;

  const pricelistRows = uniqueRows(
    sortCatalogueRows(sourceRows, contextParsed),
    (row) =>
      [
        row.city,
        row.make,
        row.brand,
        row.model,
        row.variant,
        row.fuel,
        row.transmission,
        row.exShowroomPrice,
        row.onRoadPrice,
      ]
        .join("|")
        .toLowerCase(),
  );

  const firstRow = pricelistRows[0] || vehicleRow(sourceRows[0] || {});

  const resolvedModel = firstMeaningful(
    model,
    contextModel,
    firstRow.model_normalized,
    firstRow.model,
  );

  const resolvedMake = firstMeaningful(
    make,
    firstRow.make,
    firstRow.brand,
    firstRow.brand_normalized,
    contextParsed.entities.make,
  );

  const resolvedCity =
    resolved.showingCity ||
    firstMeaningful(
      firstRow.city,
      resolved.requestedCity,
      cityFromParsed(parsed),
    );

  let availableCities = [];

  try {
    if (resolvedModel) {
      availableCities = await Vehicle.distinct(
        "city",
        vehicleModelClause(resolvedModel, resolvedMake),
      ).maxTimeMS(2500);
    }
  } catch (error) {
    pushModuleTrace(trace, `${moduleName} cities`, 0, {
      error: error?.message || "Unable to fetch available cities",
    });
  }

  let colorGallery = [];

  try {
    const colorsMap = getFieldMap("vehicle_colors");
    const colorCollection = mongoose.connection.db.collection(
      colorsMap.collectionName,
    );

    const colorRows = await colorCollection
      .find({
        model: new RegExp(escapeRegex(resolvedModel), "i"),
        ...(resolvedMake
          ? { brand: new RegExp(escapeRegex(resolvedMake), "i") }
          : {}),
      })
      .project({
        brand: 1,
        model: 1,
        color_name: 1,
        hex: 1,
        image_url: 1,
        last_updated: 1,
        scrape_timestamp: 1,
        source_page: 1,
      })
      .limit(60)
      .maxTimeMS(3500)
      .toArray();

    pushModuleTrace(trace, `${colorsMap.module} context`, colorRows.length);

    colorGallery = uniqueRows(
      colorRows
        .map((item) => ({
          id: safeId(item),
          brand: item.brand,
          make: item.brand,
          model: item.model,
          colorName: item.color_name,
          hex: item.hex,
          imageUrl: item.image_url,
          image_url: item.image_url,
          sourcePage: item.source_page,
          lastUpdated: formatDateValue(
            firstMeaningful(item.last_updated, item.scrape_timestamp),
          ),
        }))
        .filter((item) => item.colorName || item.imageUrl),
      (row) =>
        [row.brand, row.model, row.colorName, row.imageUrl || row.hex]
          .join("|")
          .toLowerCase(),
    );
  } catch (error) {
    pushModuleTrace(trace, `${moduleName} colors`, 0, {
      error: error?.message || "Unable to fetch vehicle color context",
    });
  }

  const heroImage = firstMeaningful(
    ...colorGallery.map((item) => item.imageUrl).filter(Boolean),
  );

  return {
    make: resolvedMake,
    brand: resolvedMake,
    model: resolvedModel,
    city: resolvedCity,
    requestedCity: resolved.requestedCity,
    showingCity: resolvedCity,
    cityFallbackUsed: Boolean(resolved.usedCityFallback),

    priceRows: pricelistRows,
    pricelistRows,
    pricingSummary: summarizeCatalogueRows(pricelistRows),
    priceContextAvailable: Boolean(pricelistRows.length),

    colors: colorGallery,
    colorGallery,
    heroImage,
    imageUrl: heroImage,
    vehicleImageUrl: heroImage,
    imageContextAvailable: Boolean(heroImage),

    availableCities: availableCities.filter(Boolean).sort(),
  };
};

const enrichRowWithVehicleContext = (row = {}, contextPayload = {}) => {
  const priceRow = contextPriceRowForVariant(contextPayload.priceRows, row);

  return {
    ...row,

    priceRow,
    priceContextAvailable: Boolean(priceRow),

    exShowroomPrice: firstMeaningful(
      row.exShowroomPrice,
      priceRow?.exShowroomPrice,
      priceRow?.exShowroom,
      priceRow?.ex_showroom,
    ),
    exShowroom: firstMeaningful(
      row.exShowroom,
      priceRow?.exShowroom,
      priceRow?.exShowroomPrice,
      priceRow?.ex_showroom,
    ),
    rto: firstMeaningful(row.rto, priceRow?.rto),
    insurance: firstMeaningful(row.insurance, priceRow?.insurance),
    onRoadPrice: firstMeaningful(
      row.onRoadPrice,
      priceRow?.onRoadPrice,
      priceRow?.calculatedOnRoadPrice,
      priceRow?.storedOnRoadPrice,
    ),
    optionalItems: row.optionalItems || priceRow?.optionalItems || [],
    otherItems: row.otherItems || priceRow?.otherItems || [],
    optionalOtherItems:
      row.optionalOtherItems || priceRow?.optionalOtherItems || [],

    heroImage: contextPayload.heroImage,
    imageUrl: contextPayload.vehicleImageUrl,
    vehicleImageUrl: contextPayload.vehicleImageUrl,
    colors: contextPayload.colors || [],
    colorGallery: contextPayload.colorGallery || [],
  };
};

const modelOptionFromRows = (rows = []) => {
  const compact = rows.map(vehicleRow);
  const summary = summarizeCatalogueRows(compact);
  const first = compact[0] || {};
  return {
    id: [first.brand || first.make, first.model_normalized || first.model]
      .filter(Boolean)
      .join(":"),
    entityType: "catalogue_model",
    brand: first.brand || first.make,
    model: first.model_normalized || first.model,
    displayName: [
      first.brand || first.make,
      first.model_normalized || first.model,
    ]
      .filter(Boolean)
      .join(" "),
    variantCount: summary.variantCount,
    startingPrice: summary.startingPrice,
    topPrice: summary.topPrice,
    followUpQuery: `${[first.brand || first.make, first.model_normalized || first.model].filter(Boolean).join(" ")} pricelist`,
  };
};

const findModelAmbiguity = async (parsed, trace) => {
  const model = parsed.entities.model || parsed.entities.models?.[0];
  if (
    !model ||
    /n line|x line|hybrid|show all|include|compare/i.test(parsed.lower || "")
  )
    return null;
  const regex = new RegExp(escapeRegex(model), "i");
  const rows = await findLean(
    Vehicle,
    {
      $and: [
        { $or: [{ model_normalized: regex }, { model: regex }] },
        cityClause(parsed.entities.city || "new-delhi"),
      ],
    },
    { sort: { model_normalized: 1, ex_showroom: 1 }, limit: 180 },
  );
  const grouped = new Map();
  for (const row of rows) {
    const key =
      `${row.brand_normalized || row.brand || row.make}|${row.model_normalized || row.model}`.toLowerCase();
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  }
  const options = [...grouped.values()]
    .map(modelOptionFromRows)
    .filter((option) => option.model);
  pushModuleTrace(trace, "Vehicles model ambiguity", options.length);
  if (options.length <= 1) return null;
  return {
    widgets: [
      widget("model_ambiguity", `Which ${titleCase(model)} do you mean?`, {
        originalIntent: parsed.intent,
        originalQuery: parsed.message,
        options,
        allowShowAll: true,
        allowCompareAll: true,
      }),
    ],
  };
};

const variantOptionFromRow = (row, originalIntent, originalQuery) => ({
  id: row.id,
  entityType: "catalogue_variant",
  brand: row.brand || row.make,
  make: row.make || row.brand,
  model: row.model_normalized || row.model,
  variant: displayVariant(row),
  fuelType: row.fuel,
  transmission: row.transmission,
  exShowroom: row.exShowroomPrice,
  onRoad: row.onRoadPrice,
  city: row.city,
  displayName: [row.model_normalized || row.model, displayVariant(row)]
    .filter(Boolean)
    .join(" "),
  followUpQuery: `${[row.model_normalized || row.model, displayVariant(row)].filter(Boolean).join(" ")} ${originalIntent?.includes("breakup") ? "price breakup" : "price"}`,
  context: { originalIntent, originalQuery, selectedVariantId: row.id },
});

const maybeVariantAmbiguity = (
  parsed,
  rows = [],
  originalIntent = parsed.intent,
) => {
  if (!parsed.entities.variant) return null;
  if (
    !/\b(price|emi|breakup|rto|insurance amount|tcs|ex showroom|on road)\b/i.test(
      parsed.lower || "",
    )
  )
    return null;
  const grouped = uniqueRows(rows, (row) =>
    [row.model, row.variant, row.fuel, row.transmission, row.city]
      .join("|")
      .toLowerCase(),
  );
  const normalizedVariants = new Set(
    grouped.map((row) => toWords(displayVariant(row))),
  );
  const exact = grouped.filter(
    (row) => toWords(displayVariant(row)) === toWords(parsed.entities.variant),
  );
  if (normalizedVariants.size <= 1 || exact.length === grouped.length)
    return null;
  return {
    widgets: [
      widget(
        "variant_ambiguity",
        `Which ${parsed.entities.variant} variant do you mean?`,
        {
          originalIntent,
          originalQuery: parsed.message,
          model: parsed.entities.model || grouped[0]?.model,
          city: parsed.entities.city || grouped[0]?.city,
          options: grouped
            .slice(0, 24)
            .map((row) =>
              variantOptionFromRow(row, originalIntent, parsed.message),
            ),
          compareAllOption: true,
        },
      ),
    ],
  };
};

const flattenFeatures = (features = {}) =>
  Object.entries(features || {}).map(([key, value]) => ({
    key,
    group: key.includes("|") ? key.split("|")[0].trim() : "Features",
    name: key.includes("|") ? key.split("|").slice(1).join("|").trim() : key,
    value,
  }));

const featureTerms = (feature = "") => {
  const key = toWords(feature);
  return [
    ...new Set(
      [feature, key, ...(FEATURE_SYNONYMS[key] || [])].filter(Boolean),
    ),
  ];
};

const featureValueForAny = (features = {}, feature = "") => {
  const terms = featureTerms(feature);
  for (const term of terms) {
    const found = featureValueFor(features, term);
    if (found) return found;
  }
  return null;
};

const numericFromFeature = (value) => firstNumber(value);

const emiFor = (principal, annualRate = 9, tenureMonths = 60) => {
  const monthly = annualRate / 12 / 100;
  if (!principal || !tenureMonths) return 0;
  if (!monthly) return principal / tenureMonths;
  return (
    (principal * monthly * (1 + monthly) ** tenureMonths) /
    ((1 + monthly) ** tenureMonths - 1)
  );
};

const featureDocKey = (doc = {}) =>
  [doc.brand, doc.model, doc.variant].filter(Boolean).join("|").toLowerCase();

const catalogFeatureKey = (row = {}) =>
  [row.brand || row.make, row.model, row.variant]
    .filter(Boolean)
    .join("|")
    .toLowerCase();

const joinFeatureDocs = (docs = []) => {
  const map = new Map();
  for (const doc of docs) {
    map.set(featureDocKey(doc), doc);
    map.set(
      [doc.brand, doc.model, normalizeVariant(doc.variant)]
        .join("|")
        .toLowerCase(),
      doc,
    );
  }
  return map;
};

const normalizedKeyPart = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const stripBrandFromModel = (model = "", brand = "") => {
  const cleanModel = normalizedKeyPart(model);
  const cleanBrand = normalizedKeyPart(brand);

  if (!cleanModel || !cleanBrand) return cleanModel;

  return cleanModel.startsWith(`${cleanBrand} `)
    ? cleanModel.slice(cleanBrand.length + 1).trim()
    : cleanModel;
};

const buildFeatureLookupMaps = (docs = []) => {
  const exact = new Map();
  const model = new Map();
  const modelOnly = new Map();

  for (const doc of docs || []) {
    const brand = normalizedKeyPart(doc.brand);
    const modelName = normalizedKeyPart(doc.model);
    const strippedModel = stripBrandFromModel(doc.model, doc.brand);
    const variant = normalizedKeyPart(doc.variant);

    const exactKeys = [
      [brand, modelName, variant].join("|"),
      [brand, strippedModel, variant].join("|"),
      [modelName, variant].join("|"),
      [strippedModel, variant].join("|"),
    ].filter(Boolean);

    for (const key of exactKeys) {
      if (key.replace(/\|/g, "")) exact.set(key, doc);
    }

    const modelKeys = [
      [brand, modelName].join("|"),
      [brand, strippedModel].join("|"),
    ].filter(Boolean);

    for (const key of modelKeys) {
      if (key.replace(/\|/g, "")) {
        if (!model.has(key)) model.set(key, []);
        model.get(key).push(doc);
      }
    }

    for (const key of [modelName, strippedModel].filter(Boolean)) {
      if (!modelOnly.has(key)) modelOnly.set(key, []);
      modelOnly.get(key).push(doc);
    }
  }

  return { exact, model, modelOnly };
};

const resolveFeatureDocForCatalogueRow = (row = {}, lookup = {}) => {
  const brand = normalizedKeyPart(row.brand || row.make);
  const modelName = normalizedKeyPart(row.model_normalized || row.model);
  const strippedModel = stripBrandFromModel(modelName, brand);
  const variant = normalizedKeyPart(row.variant_normalized || row.variant);

  const exactKeys = [
    [brand, modelName, variant].join("|"),
    [brand, strippedModel, variant].join("|"),
    [modelName, variant].join("|"),
    [strippedModel, variant].join("|"),
  ];

  for (const key of exactKeys) {
    if (lookup.exact?.has(key)) return lookup.exact.get(key);
  }

  const modelKeys = [
    [brand, modelName].join("|"),
    [brand, strippedModel].join("|"),
  ];

  for (const key of modelKeys) {
    const docs = lookup.model?.get(key);
    if (docs?.length) return docs[0];
  }

  return (
    lookup.modelOnly?.get(modelName)?.[0] ||
    lookup.modelOnly?.get(strippedModel)?.[0] ||
    null
  );
};

const variantGroupsForModels = async (models, trace, city = "new-delhi") => {
  const groups = await Promise.all(
    models.map(async (model) => {
      let docs = await findLean(
        Vehicle,
        { $and: [vehicleModelClause(model), cityClause(city)].filter(Boolean) },
        {
          sort: { model: 1, variant: 1, city: 1 },
          limit: 80,
        },
      );
      if (!docs.length) {
        docs = await findLean(Vehicle, vehicleModelClause(model), {
          sort: { model: 1, variant: 1, city: 1 },
          limit: 80,
        });
      }
      pushModuleTrace(trace, `Variants ${model}`, docs.length, {
        city: docs[0]?.city || city,
      });
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
  const rows = await findLean(
    Vehicle,
    { _id: { $in: variantIds } },
    { limit: 12 },
  );
  pushModuleTrace(trace, "Selected vehicle variants", rows.length);
  const compactRows = rows.length
    ? compactVariantRows(rows)
    : compactVariantRows(context.selectedVariantRows || []);
  const selectedContext = {
    selectedVariantIds: variantIds,
    selectedVariantRows: compactRows,
    selectedModels: context.selectedModels || [
      ...new Set(compactRows.map((row) => row.model).filter(Boolean)),
    ],
    compareMode: "variants",
  };
  return {
    widgets: [
      widget("vehicle_comparison", "Selected variant comparison", {
        rows: compactRows.map(comparisonRowFromVariant),
        data: selectedContext,
        notices: rows.length
          ? []
          : [
              "Using selected variants from chat context because the catalog IDs were not found in the current database query.",
            ],
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
    return {
      widgets: [
        unavailableWidget(
          "Vehicle data unavailable",
          "You do not have catalog access.",
          ["Vehicles"],
        ),
      ],
    };
  }
  const modelAmbiguity = await findModelAmbiguity(parsed, trace);
  if (modelAmbiguity) return modelAmbiguity;
  const resolved = await resolveVehicleCatalogRows(parsed, trace, {
    includeCity: true,
    limit: 160,
    moduleName: "Vehicles",
  });
  const rows = applyCatalogueFilters(resolved.rows, parsed);
  if (
    !rows.length &&
    !resolved.tokens.length &&
    !parsed.entities.model &&
    !parsed.entities.variant
  ) {
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
      followUpSuggestions: resolved.suggestions.map(
        (row) =>
          `Did you mean: ${[row.model, row.variant].filter(Boolean).join(" ")}?`,
      ),
    };
  }
  const firstRow = vehicleRow(rows[0]);
  const model = firstMeaningful(
    parsed.entities.model,
    firstRow.model_normalized,
    firstRow.model,
  );
  const make = firstMeaningful(
    firstRow.make,
    firstRow.brand,
    firstRow.brand_normalized,
  );
  const featureDocs = await findLean(
    VehicleFeature,
    featureModelQuery(model, rows[0]?.make || rows[0]?.brand),
    { limit: 12 },
  );
  pushModuleTrace(trace, "Vehicle Features", featureDocs.length);
  const wantsColors = /colors|colours/.test(parsed.lower);
  const wantsSunroof = /sunroof/.test(parsed.lower);
  const notices = [];
  if (wantsColors)
    notices.push(
      "Dedicated color data was not found in the catalog fields scanned.",
    );
  if (wantsSunroof) {
    const featureText = JSON.stringify(featureDocs).toLowerCase();
    notices.push(
      featureText.includes("sunroof")
        ? "Sunroof appears in feature data for matching variants."
        : "Sunroof was not found in the available feature data.",
    );
  }
  const city =
    resolved.showingCity ||
    (resolved.usedCityFallback
      ? firstMeaningful(rows[0]?.city, resolved.requestedCity)
      : resolved.requestedCity);
  const pricelistRows = uniqueRows(sortCatalogueRows(rows, parsed), (row) =>
    [
      row.city,
      row.make,
      row.model,
      row.variant,
      row.fuel,
      row.transmission,
      row.exShowroomPrice,
      row.onRoadPrice,
    ].join("|"),
  );
  const variantAmbiguity = maybeVariantAmbiguity(
    parsed,
    pricelistRows,
    "vehicle_pricelist",
  );
  if (variantAmbiguity) return variantAmbiguity;
  const summary = summarizeCatalogueRows(pricelistRows);
  const cities = await Vehicle.distinct(
    "city",
    vehicleModelClause(model, make),
  ).maxTimeMS(2500);
  const colorsMap = getFieldMap("vehicle_colors");
  let colorGallery = [];

  try {
    const colorCollection = mongoose.connection.db.collection(
      colorsMap.collectionName,
    );

    const colorRows = await colorCollection
      .find({
        model: new RegExp(escapeRegex(model), "i"),
        ...(make ? { brand: new RegExp(escapeRegex(make), "i") } : {}),
      })
      .project({
        brand: 1,
        model: 1,
        color_name: 1,
        hex: 1,
        image_url: 1,
        last_updated: 1,
        scrape_timestamp: 1,
        source_page: 1,
      })
      .limit(40)
      .maxTimeMS(2500)
      .toArray();

    pushModuleTrace(
      trace,
      `${colorsMap.module} for pricelist image`,
      colorRows.length,
    );

    colorGallery = uniqueRows(
      colorRows
        .map((item) => ({
          id: safeId(item),
          brand: item.brand,
          make: item.brand,
          model: item.model,
          colorName: item.color_name,
          hex: item.hex,
          imageUrl: item.image_url,
          image_url: item.image_url,
          sourcePage: item.source_page,
          lastUpdated: formatDateValue(
            firstMeaningful(item.last_updated, item.scrape_timestamp),
          ),
        }))
        .filter((item) => item.colorName || item.imageUrl),
      (row) =>
        [row.brand, row.model, row.colorName, row.imageUrl || row.hex]
          .join("|")
          .toLowerCase(),
    );
  } catch (error) {
    pushModuleTrace(trace, `${colorsMap.module} for pricelist image`, 0, {
      error: error?.message || "Unable to fetch vehicle color images",
    });
  }

  const heroImage = firstMeaningful(
    ...colorGallery.map((item) => item.imageUrl).filter(Boolean),
  );
  return {
    widgets: [
      widget("vehicle_pricelist", `${model} pricelist`, {
        brand: make,
        model,
        city,
        heroImage,
        imageUrl: heroImage,
        colors: colorGallery,
        colorGallery,
        requestedCity: resolved.requestedCity,
        showingCity: city,
        cityFallbackUsed: Boolean(resolved.usedCityFallback),
        availableCities: cities.filter(Boolean).sort(),
        totalVariants: pricelistRows.length,
        summary,
        data: {
          make,
          brand: make,
          model,
          city,
          heroImage,
          imageUrl: heroImage,
          colors: colorGallery,
          colorGallery,
          requestedCity: resolved.requestedCity,
          showingCity: city,
          cityFallbackUsed: Boolean(resolved.usedCityFallback),
          cities: cities.filter(Boolean).sort(),
          availableCities: cities.filter(Boolean).sort(),
          total: pricelistRows.length,
          totalVariants: pricelistRows.length,
          summary,
          features: featureDocs,
          records: pricelistRows,
          variants: pricelistRows,
          matchPriority:
            "search_text > variant_normalized > model_normalized > fuzzy",
          query: resolved.phrase,
        },
        columns: [
          "Make",
          "Model",
          "Variant",
          "Fuel",
          "Transmission",
          "City",
          "Ex-showroom",
          "RTO / road tax",
          "Insurance",
          "Optional / other items",
          "On-road",
          "Year",
          "Status",
          "Last updated",
        ],
        rows: pricelistRows,
        records: pricelistRows,
        variants: pricelistRows,
        notices: [
          ...notices,
          resolved.usedCityFallback
            ? `${titleCase(resolved.requestedCity)} pricing is not available in stored catalogue. Showing ${titleCase(city)} pricing instead.`
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
    followUpSuggestions: [
      "Show similar cars",
      "Compare with City and Slavia",
      "Show colors",
      "Show top variants",
      "Open full pricelist",
    ],
  };
};

export const vehiclePriceBreakup = async (parsed, access, trace) => {
  const result = await vehiclePricelist(parsed, access, trace);
  const rows = result.widgets?.[0]?.rows || [];
  if (!rows.length) return result;
  const targetRows = rows.slice(0, LIMIT).map((row) => {
    const components = [
      { label: "Ex-showroom", value: row.exShowroomPrice },
      { label: "RTO / Road Tax", value: row.rto },
      { label: "Insurance", value: row.insurance },
      { label: "TCS", value: firstNumber(row.tcs, row.other_tcsCharges) },
      {
        label: "Handling / Other",
        value: row.otherItems?.reduce(
          (sum, item) => sum + firstNumber(item.amount),
          0,
        ),
      },
      {
        label: "Optional Accessories",
        value: row.optionalItems?.reduce(
          (sum, item) => sum + firstNumber(item.amount),
          0,
        ),
      },
    ].map((component) => ({
      ...component,
      captured: firstNumber(component.value) > 0,
    }));
    return {
      id: row.id,
      make: row.make,
      brand: row.brand || row.make,
      model: row.model,
      variant: row.variant,
      city: row.city,
      fuel: row.fuel,
      fuelType: row.fuel,
      transmission: row.transmission,
      exShowroomPrice: row.exShowroomPrice,
      exShowroom: row.exShowroomPrice,
      rto: row.rto,
      insurance: row.insurance,
      tcs: row.tcs,
      handlingOtherCharges: row.handlingOtherCharges,
      optionalTotal: row.optionalItems?.reduce(
        (sum, item) => sum + firstNumber(item.amount),
        0,
      ),
      optionalItems: row.optionalItems,
      otherItems: row.otherItems,
      components,
      totals: {
        onRoadWithoutAccessories: row.orpWithoutAccessories,
        onRoadWithAccessories: row.onRoadPrice,
      },
      orpWithoutAccessories: row.orpWithoutAccessories,
      onRoadPrice: row.onRoadPrice,
      status: row.status,
      lastUpdated: row.lastUpdated,
    };
  });
  const first = targetRows[0] || {};
  return {
    widgets: [
      widget("vehicle_price_breakup", "Vehicle price breakup", {
        brand: first.brand,
        model: first.model,
        variant: first.variant,
        city: first.city,
        components: first.components,
        totals: first.totals,
        data: {
          model: result.widgets?.[0]?.data?.model,
          city: result.widgets?.[0]?.data?.city,
          total: targetRows.length,
          availableCities: result.widgets?.[0]?.data?.cities,
          components: first.components,
          totals: first.totals,
        },
        rows: targetRows,
        notices: [
          "Only stored price fields are shown. Missing breakup values are not invented.",
        ],
      }),
    ],
    followUpSuggestions: result.followUpSuggestions,
  };
};

export const vehicleColors = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return {
      widgets: [
        unavailableWidget(
          "Vehicle data unavailable",
          "You do not have catalog access.",
          ["Vehicles"],
        ),
      ],
    };
  }
  const modelAmbiguity = await findModelAmbiguity(parsed, trace);
  if (modelAmbiguity) return modelAmbiguity;
  const resolved = await resolveVehicleCatalogRows(parsed, trace, {
    includeCity: false,
    limit: 120,
    moduleName: "Vehicles",
  });
  const catalogRows = resolved.rows.map(vehicleRow);
  const model = firstMeaningful(
    parsed.entities.model,
    catalogRows[0]?.model_normalized,
    catalogRows[0]?.model,
    parsed.entities.models?.[0],
  );
  const brand = firstMeaningful(
    parsed.entities.make,
    catalogRows[0]?.make,
    catalogRows[0]?.brand,
    catalogRows[0]?.brand_normalized,
  );
  if (!model && !resolved.tokens.length) {
    return {
      widgets: [
        unavailableWidget(
          "Need a model",
          "Ask for colors with a model, for example: Show Verna colors.",
          ["Vehicles"],
        ),
      ],
    };
  }
  const colorsMap = getFieldMap("vehicle_colors");
  const colorCollection = mongoose.connection.db.collection(
    colorsMap.collectionName,
  );
  const modelRegex = new RegExp(
    escapeRegex(model || resolved.tokens.join(" ")),
    "i",
  );
  const brandRegex = brand ? new RegExp(escapeRegex(brand), "i") : null;
  const query = {
    model: modelRegex,
    ...(brandRegex ? { brand: brandRegex } : {}),
  };
  const rows = await colorCollection
    .find(query)
    .project({
      brand: 1,
      model: 1,
      color_name: 1,
      hex: 1,
      image_url: 1,
      last_updated: 1,
      scrape_timestamp: 1,
      source_page: 1,
    })
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
        lastUpdated: formatDateValue(
          firstMeaningful(item.last_updated, item.scrape_timestamp),
        ),
        sourcePage: item.source_page,
      }))
      .filter((item) => item.colorName),
    (row) =>
      `${row.brand}|${row.model}|${row.colorName}|${row.imageUrl || row.hex}`.toLowerCase(),
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
    (row) =>
      [row.brand, row.model, row.variant, row.fuel, row.transmission]
        .join("|")
        .toLowerCase(),
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
          notices: [
            `I checked ${colorsMap.collectionName} and vehicle catalog colors for ${model || resolved.phrase}, but no stored color rows matched.`,
          ],
          suggestions: resolved.suggestions,
          closestVariants: resolved.suggestions,
        }),
      ],
      followUpSuggestions: [
        "Show pricelist",
        "Show features",
        "Compare with City and Slavia",
      ],
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
        notices: [
          "Showing only colors stored in catalog fields. No colors are inferred.",
        ],
      }),
    ],
    followUpSuggestions: [
      "Show pricelist",
      "Show features",
      "Compare with City and Slavia",
    ],
  };
};

export const vehicleFeatures = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return {
      widgets: [
        unavailableWidget(
          "Vehicle data unavailable",
          "You do not have catalog access.",
          ["Vehicles"],
        ),
      ],
    };
  }

  const selectedRows = compactVariantRows(
    parsed.context?.selectedVariantRows || [],
  );

  const selectedVariantNames = selectedRows
    .map((row) => normalizeVariant(row.variant))
    .filter(Boolean);

  const contextModel =
    parsed.context?.selectedModels?.[0] || selectedRows[0]?.model;

  const model =
    parsed.entities.model || parsed.entities.models?.[0] || contextModel;

  if (!model) {
    return {
      widgets: [
        unavailableWidget(
          "Need a model",
          "Ask for features with a model, for example: Show features of Hyundai Verna HX8 iVT.",
          ["Vehicle Features"],
        ),
      ],
    };
  }

  let docs = [];

  if (selectedRows.length && !parsed.entities.model) {
    const clauses = selectedRows
      .map((row) => vehicleModelClause(row.model, row.make))
      .filter(Boolean);

    docs = clauses.length
      ? await findLean(
          VehicleFeature,
          { $or: clauses },
          { sort: { model: 1, variant: 1 }, limit: 240 },
        )
      : [];
  } else {
    const query = featureModelQuery(model, parsed.entities.make);

    docs = await findLean(VehicleFeature, query, {
      sort: { variant: 1 },
      limit: 120,
    });
  }

  pushModuleTrace(trace, "Vehicle Features", docs.length);

  const variantNeedle = normalizeVariant(parsed.entities.variant);

  const matchedDocs = variantNeedle
    ? docs.filter((doc) =>
        normalizeVariant(doc.variant).includes(variantNeedle),
      )
    : selectedVariantNames.length
      ? docs.filter((doc) =>
          selectedVariantNames.some(
            (variant) =>
              normalizeVariant(doc.variant).includes(variant) ||
              variant.includes(normalizeVariant(doc.variant)),
          ),
        )
      : docs;

  const featureRows = matchedDocs.slice(0, LIMIT).map((doc) => ({
    id: safeId(doc),
    make: doc.brand,
    brand: doc.brand,
    model: doc.model,
    variant: doc.variant,
    bodyType: doc.body_type_bucket,
    seatingCapacity: doc.seating_capacity,
    featureGroups: Object.keys(doc.features || {}).length,
    features: doc.features || {},
    lastUpdated: formatDateValue(doc.updatedAt),
  }));

  if (!featureRows.length) {
    return {
      widgets: [
        unavailableWidget(
          "No feature catalogue found",
          `No feature record matched ${[model, parsed.entities.variant]
            .filter(Boolean)
            .join(" ")}.`,
          ["Vehicle Features"],
        ),
      ],
      followUpSuggestions: [
        "Show pricelist",
        "Show variants",
        "Show similar cars",
      ],
    };
  }

  const contextPayload = await buildVehicleCanvasContext(parsed, trace, {
    model,
    make: firstMeaningful(
      parsed.entities.make,
      featureRows[0]?.make,
      featureRows[0]?.brand,
    ),
    city: parsed.entities.city,
    moduleName: "Vehicles context for feature catalogue",
  });

  const rows = featureRows.map((row) =>
    enrichRowWithVehicleContext(row, contextPayload),
  );

  return {
    widgets: [
      widget("vehicle_features", `${model} feature catalogue`, {
        ...contextPayload,

        data: {
          ...contextPayload,

          model,
          brand: contextPayload.brand,
          make: contextPayload.make,
          city: contextPayload.city,
          variant: parsed.entities.variant,
          total: matchedDocs.length,

          priceRows: contextPayload.priceRows,
          pricelistRows: contextPayload.pricelistRows,
          colors: contextPayload.colors,
          colorGallery: contextPayload.colorGallery,
          heroImage: contextPayload.heroImage,
          imageUrl: contextPayload.imageUrl,
          vehicleImageUrl: contextPayload.vehicleImageUrl,
          pricingSummary: contextPayload.pricingSummary,
        },

        rows,
        records: rows,

        priceRows: contextPayload.priceRows,
        pricelistRows: contextPayload.pricelistRows,
        colors: contextPayload.colors,
        colorGallery: contextPayload.colorGallery,
        heroImage: contextPayload.heroImage,
        imageUrl: contextPayload.imageUrl,
        vehicleImageUrl: contextPayload.vehicleImageUrl,
        pricingSummary: contextPayload.pricingSummary,

        notices: [
          "Feature values are shown only from stored feature catalogue fields.",
          contextPayload.priceContextAvailable
            ? "Price context is attached from vehicle catalogue rows."
            : "Price context was not found for this feature response.",
          contextPayload.imageContextAvailable
            ? "Vehicle image is attached from stored color gallery data."
            : "Vehicle image was not found in stored color gallery data.",
        ],
      }),
    ],
    followUpSuggestions: [
      "Show pricelist",
      "Compare variants",
      "Show similar cars",
    ],
  };
};

export const vehicleFeatureAvailability = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return {
      widgets: [
        unavailableWidget(
          "Vehicle data unavailable",
          "You do not have catalog access.",
          ["Vehicles"],
        ),
      ],
    };
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

  const featureDocs = await findLean(
    VehicleFeature,
    featureModelQuery(model, parsed.entities.make),
    { limit: 120 },
  );

  pushModuleTrace(trace, "Vehicle Features", featureDocs.length);

  const variantNeedle = normalizeVariant(parsed.entities.variant);

  const scopedDocs = variantNeedle
    ? featureDocs.filter((item) =>
        normalizeVariant(item.variant).includes(variantNeedle),
      )
    : featureDocs;

  const baseEvidenceRows = scopedDocs.map((item) => {
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

  const contextPayload = await buildVehicleCanvasContext(parsed, trace, {
    model,
    make: firstMeaningful(
      parsed.entities.make,
      baseEvidenceRows[0]?.make,
      baseEvidenceRows[0]?.brand,
    ),
    city: parsed.entities.city,
    moduleName: "Vehicles context for feature answer",
  });

  const evidenceRows = baseEvidenceRows.map((row) =>
    enrichRowWithVehicleContext(row, contextPayload),
  );

  const yesCount = evidenceRows.filter((row) => row.answer === "Yes").length;

  const noCount = evidenceRows.filter((row) => row.answer === "No").length;

  const notFoundCount = evidenceRows.filter(
    (row) => row.answer === "Not found",
  ).length;

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
        ...contextPayload,

        question: parsed.message,
        model,
        brand: contextPayload.brand,
        make: contextPayload.make,
        city: contextPayload.city,
        variantQuery: parsed.entities.variant,
        feature,
        answer,

        priceRows: contextPayload.priceRows,
        pricelistRows: contextPayload.pricelistRows,
        colors: contextPayload.colors,
        colorGallery: contextPayload.colorGallery,
        heroImage: contextPayload.heroImage,
        imageUrl: contextPayload.imageUrl,
        vehicleImageUrl: contextPayload.vehicleImageUrl,
        pricingSummary: contextPayload.pricingSummary,

        summary: {
          totalVariantsChecked: evidenceRows.length,
          yesCount,
          noCount,
          notFoundCount,
        },

        evidenceRows,

        data: {
          ...contextPayload,

          model,
          brand: contextPayload.brand,
          make: contextPayload.make,
          city: contextPayload.city,
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
          rows: evidenceRows,
          records: evidenceRows,

          priceRows: contextPayload.priceRows,
          pricelistRows: contextPayload.pricelistRows,
          colors: contextPayload.colors,
          colorGallery: contextPayload.colorGallery,
          heroImage: contextPayload.heroImage,
          imageUrl: contextPayload.imageUrl,
          vehicleImageUrl: contextPayload.vehicleImageUrl,
          pricingSummary: contextPayload.pricingSummary,
        },

        rows: evidenceRows,
        records: evidenceRows,

        columns: [
          "brand",
          "model",
          "variant",
          "featureKey",
          "featureValue",
          "answer",
        ],

        notices: [
          ...(evidenceRows.length
            ? []
            : [
                `No ${feature} feature records were found for ${[
                  model,
                  parsed.entities.variant,
                ]
                  .filter(Boolean)
                  .join(" ")}.`,
              ]),
          contextPayload.priceContextAvailable
            ? "Price context is attached from vehicle catalogue rows."
            : "Price context was not found for this feature answer.",
          contextPayload.imageContextAvailable
            ? "Vehicle image is attached from stored color gallery data."
            : "Vehicle image was not found in stored color gallery data.",
        ],

        actions: [
          action("open_features", "Open features page", {
            route: "/loans/features",
            query: {
              brand: evidenceRows[0]?.brand || parsed.entities.make,
              model,
              variant: parsed.entities.variant,
            },
          }),
        ],
      }),
    ],
    followUpSuggestions: [
      "Show pricelist",
      "Compare variants",
      "Show similar cars",
    ],
  };
};

export const vehicleFeatureDiscovery = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return {
      widgets: [
        unavailableWidget(
          "Vehicle data unavailable",
          "You do not have catalog access.",
          ["Vehicles"],
        ),
      ],
    };
  }
  const feature =
    parsed.entities.feature ||
    (/6\s*airbags/i.test(parsed.lower) ? "6 airbags" : "feature");
  const featureQuery = parsed.entities.model
    ? featureModelQuery(parsed.entities.model, parsed.entities.make)
    : {};
  const docs = await findLean(VehicleFeature, featureQuery, {
    sort: { model: 1, variant: 1 },
    limit: 500,
  });
  pushModuleTrace(trace, "Vehicle Features", docs.length, { feature });
  const catalogRows = await findLean(
    Vehicle,
    { $and: [cityClause(parsed.entities.city || "new-delhi")].filter(Boolean) },
    { sort: { ex_showroom: 1 }, limit: 900 },
  );
  pushModuleTrace(trace, "Vehicles", catalogRows.length, {
    city: cityFromParsed(parsed),
  });
  const catalogByVariant = new Map(
    catalogRows.map((row) => [
      catalogFeatureKey(vehicleRow(row)),
      vehicleRow(row),
    ]),
  );
  const rows = [];
  for (const doc of docs) {
    const match = featureValueForAny(doc.features, feature);
    const answer = featureAnswerForValue(match);
    const compact =
      catalogByVariant.get(featureDocKey(doc)) ||
      catalogByVariant.get(
        [doc.brand, doc.model, normalizeVariant(doc.variant)]
          .join("|")
          .toLowerCase(),
      ) ||
      {};
    const price = priceBasis(compact);
    if (parsed.entities.budgetMax && price && price > parsed.entities.budgetMax)
      continue;
    if (
      parsed.entities.bodyType &&
      !bodyTypeMatches(doc.body_type_bucket, parsed.entities.bodyType)
    )
      continue;
    if (feature === "6 airbags" && numericFromFeature(match?.value) < 6)
      continue;
    rows.push({
      id: safeId(doc),
      brand: doc.brand,
      model: doc.model,
      variant: doc.variant,
      city: compact.city || cityFromParsed(parsed),
      bodyType: doc.body_type_bucket,
      onRoad: compact.onRoadPrice,
      exShowroom: compact.exShowroomPrice,
      featureKey: match?.key || "",
      featureValue: match?.value ?? "",
      answer,
      matchedReason: match
        ? `${match.key}: ${match.value}`
        : "feature not found in stored catalogue",
    });
  }
  const grouped = {
    yes: rows.filter((row) => row.answer === "Yes"),
    no: rows.filter((row) => row.answer === "No"),
    notFound: rows.filter((row) => row.answer === "Not found"),
  };
  return {
    widgets: [
      widget("vehicle_feature_discovery", `${feature} availability`, {
        title: `${feature} availability`,
        filters: {
          model: parsed.entities.model,
          feature,
          budgetMax: parsed.entities.budgetMax,
          bodyType: parsed.entities.bodyType,
          city: cityFromParsed(parsed),
        },
        grouped,
        rows: rows.slice(0, LIMIT),
        records: rows.slice(0, LIMIT),
      }),
    ],
    followUpSuggestions: [
      "Show pricelist",
      "Compare matching variants",
      "Show similar cars",
    ],
  };
};

export const vehicleColorSearch = async (parsed, access, trace) => {
  const color = parsed.entities.color;
  if (parsed.entities.model) return vehicleColors(parsed, access, trace);
  const colorsMap = getFieldMap("vehicle_colors");
  const colorCollection = mongoose.connection.db.collection(
    colorsMap.collectionName,
  );
  const colorRegex = color ? new RegExp(escapeRegex(color), "i") : /./;
  const rows = await colorCollection
    .find({ color_name: colorRegex })
    .project({
      brand: 1,
      model: 1,
      color_name: 1,
      hex: 1,
      image_url: 1,
      source_page: 1,
      last_updated: 1,
      scrape_timestamp: 1,
    })
    .limit(160)
    .maxTimeMS(3500)
    .toArray();
  pushModuleTrace(trace, colorsMap.module, rows.length, { color });
  const compactRows = uniqueRows(
    rows.map((item) => ({
      id: safeId(item),
      brand: item.brand,
      make: item.brand,
      model: item.model,
      colorName: item.color_name,
      hex: item.hex,
      imageUrl: item.image_url,
      image_url: item.image_url,
      sourcePage: item.source_page,
      lastUpdated: formatDateValue(
        firstMeaningful(item.last_updated, item.scrape_timestamp),
      ),
    })),
    (row) =>
      [row.brand, row.model, row.colorName, row.imageUrl]
        .join("|")
        .toLowerCase(),
  );
  return {
    widgets: [
      widget("vehicle_color_search", `${titleCase(color)} cars`, {
        color,
        rows: compactRows,
        colors: compactRows,
        data: { color, total: compactRows.length, colors: compactRows },
        notices: [
          "Color results come only from stored vehicle_colors records.",
        ],
      }),
    ],
    followUpSuggestions: [
      "Show pricelist",
      "Compare models",
      "Show similar cars",
    ],
  };
};

const parseNcapStars = (value) => {
  const text = String(value ?? "").trim();
  const number = firstNumber(text);
  if (number > 0) return number;
  if (/5\s*star/i.test(text)) return 5;
  if (/4\s*star/i.test(text)) return 4;
  if (/3\s*star/i.test(text)) return 3;
  if (/2\s*star/i.test(text)) return 2;
  if (/1\s*star/i.test(text)) return 1;
  return 0;
};

const yesishFeature = (value) => {
  const text = String(value ?? "")
    .trim()
    .toLowerCase();
  return Boolean(
    value === true ||
    /^(yes|available|present|standard|optional|true|1)$/i.test(text),
  );
};

const getFeatureValueByNeedles = (features = {}, needles = []) => {
  const normalizedNeedles = needles
    .map((item) => toWords(item))
    .filter(Boolean);

  for (const [key, value] of Object.entries(features || {})) {
    const normalizedKey = toWords(key);
    if (normalizedNeedles.some((needle) => normalizedKey.includes(needle))) {
      return { key, value };
    }
  }

  return null;
};

const buildSafetySummaryFromFeatureDoc = (featureDoc = {}) => {
  const features = featureDoc?.features || {};

  const adultNcapMatch = getFeatureValueByNeedles(features, [
    "global ncap safety rating",
    "adult safety rating",
    "global ncap adult",
  ]);

  const childNcapMatch = getFeatureValueByNeedles(features, [
    "global ncap child safety rating",
    "child safety rating",
    "global ncap child",
  ]);

  const airbagsMatch = getFeatureValueByNeedles(features, [
    "no of airbags",
    "number of airbags",
    "airbags",
  ]);

  const adultNcap = parseNcapStars(adultNcapMatch?.value);
  const childNcap = parseNcapStars(childNcapMatch?.value);
  const maxAirbags = firstNumber(airbagsMatch?.value);

  const safetyHighlights = flattenFeatures(features)
    .filter((item) =>
      /safety|airbag|adas|esc|esp|tpms|isofix|hill|lane|blind|collision|brake|abs|traction|stability/i.test(
        `${item.key} ${item.value}`,
      ),
    )
    .filter((item) => {
      if (/not available|no|false|0/i.test(String(item.value))) return false;
      return true;
    })
    .slice(0, 10)
    .map((item) => `${item.name}: ${item.value}`);

  const safetyFeatureCount = safetyHighlights.length;

  let score = 0;
  score += adultNcap * 100;
  score += childNcap * 70;
  score += maxAirbags * 20;
  score += safetyFeatureCount * 8;

  return {
    adultNcap: adultNcap || null,
    childNcap: childNcap || null,
    maxAirbags: maxAirbags || null,
    safetyFeatureCount,
    safetyHighlights,
    score,
  };
};

const activeVariantRows = (rows = []) =>
  rows.filter((row) => !isDiscontinuedVehicleRow(row));

const modelGroupKey = (row = {}) =>
  [firstMeaningful(row.brand, row.make), firstMeaningful(row.model)]
    .join("|")
    .toLowerCase();

const aggregateModelCards = (variantRows = [], { safetyMode = false } = {}) => {
  const groups = new Map();

  for (const row of variantRows || []) {
    const key = modelGroupKey(row);
    if (!key.trim()) continue;

    if (!groups.has(key)) {
      groups.set(key, {
        brand: firstMeaningful(row.brand, row.make),
        model: row.model,
        bodyType: row.bodyType,
        variants: [],
        scores: [],
        matchedReasonsSet: new Set(),
        fuelOptionsSet: new Set(),
        transmissionOptionsSet: new Set(),
        keyFeaturesSet: new Set(),
        safetySummaries: [],
      });
    }

    const group = groups.get(key);
    group.variants.push(row);
    group.scores.push(firstNumber(row.score));

    if (!group.bodyType && row.bodyType) group.bodyType = row.bodyType;

    for (const reason of row.matchedReasons || []) {
      if (reason) group.matchedReasonsSet.add(reason);
    }

    if (row.fuelType) group.fuelOptionsSet.add(row.fuelType);
    if (row.transmission) group.transmissionOptionsSet.add(row.transmission);

    for (const feature of row.keyFeatures || []) {
      if (feature) group.keyFeaturesSet.add(feature);
    }

    if (row.safetySummary) group.safetySummaries.push(row.safetySummary);
  }

  return [...groups.values()]
    .map((group) => {
      const prices = group.variants
        .map((row) => firstNumber(row.onRoad, row.exShowroom))
        .filter(Boolean);
      const exPrices = group.variants
        .map((row) => firstNumber(row.exShowroom))
        .filter(Boolean);
      const sortedByPrice = [...group.variants].sort(
        (a, b) =>
          firstNumber(a.onRoad, a.exShowroom) -
          firstNumber(b.onRoad, b.exShowroom),
      );
      const sortedByScore = [...group.variants].sort(
        (a, b) => firstNumber(b.score) - firstNumber(a.score),
      );

      const adultNcap =
        Math.max(
          ...group.safetySummaries.map((item) => firstNumber(item.adultNcap)),
          0,
        ) || null;
      const childNcap =
        Math.max(
          ...group.safetySummaries.map((item) => firstNumber(item.childNcap)),
          0,
        ) || null;
      const maxAirbags =
        Math.max(
          ...group.safetySummaries.map((item) => firstNumber(item.maxAirbags)),
          0,
        ) || null;
      const safetyFeatureCount =
        Math.max(
          ...group.safetySummaries.map((item) =>
            firstNumber(item.safetyFeatureCount),
          ),
          0,
        ) || 0;
      const safetyHighlights = [
        ...new Set(
          group.safetySummaries.flatMap((item) => item.safetyHighlights || []),
        ),
      ].slice(0, 8);

      const safetyScore =
        firstNumber(adultNcap) * 100 +
        firstNumber(childNcap) * 70 +
        firstNumber(maxAirbags) * 20 +
        safetyFeatureCount * 8;

      const score = safetyMode
        ? safetyScore + Math.max(...group.scores, 0)
        : Math.max(...group.scores, 0);

      const bestVariant = sortedByScore[0] || sortedByPrice[0];
      const cheapestVariant = sortedByPrice[0];
      const topVariant = sortedByPrice[sortedByPrice.length - 1];

      return {
        id: `${group.brand}-${group.model}`
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-"),
        brand: group.brand,
        make: group.brand,
        model: group.model,
        bodyType: group.bodyType,
        startingPrice: prices.length
          ? Math.min(...prices)
          : exPrices.length
            ? Math.min(...exPrices)
            : null,
        topPrice: prices.length
          ? Math.max(...prices)
          : exPrices.length
            ? Math.max(...exPrices)
            : null,
        variantCount: group.variants.length,
        activeVariantCount: activeVariantRows(group.variants).length,
        fuelOptions: [...group.fuelOptionsSet],
        transmissionOptions: [...group.transmissionOptionsSet],
        bestVariant,
        cheapestVariant,
        topVariant,
        matchedReasons: [...group.matchedReasonsSet].slice(0, 6),
        representativeFeatures: [...group.keyFeaturesSet].slice(0, 6),
        safetySummary: {
          adultNcap,
          childNcap,
          maxAirbags,
          safetyFeatureCount,
          safetyHighlights,
        },
        adultNcap,
        childNcap,
        maxAirbags,
        safetyFeatureCount,
        safetyHighlights,
        score,
        variants: group.variants,
      };
    })
    .sort((a, b) => firstNumber(b.score) - firstNumber(a.score));
};

const recommendationRows = async (
  parsed,
  trace,
  { safetyOnly = false, specMode = "", useCase = "" } = {},
) => {
  const query = {
    $and: [cityClause(parsed.entities.city || "new-delhi")].filter(Boolean),
  };
  if (parsed.entities.make)
    query.$and.push(makeOrBrandClause(parsed.entities.make));

  const raw = await findLean(Vehicle, query, {
    sort: { ex_showroom: 1 },
    limit: 1600,
  });

  const catalogRows = uniqueRows(
    applyCatalogueFilters(raw, parsed).map(vehicleRow),
    (row) =>
      [row.brand, row.model, row.variant, row.city, row.fuel, row.transmission]
        .join("|")
        .toLowerCase(),
  );

  pushModuleTrace(trace, "Vehicles", catalogRows.length, {
    city: cityFromParsed(parsed),
  });

  const featureDocs = await findLean(VehicleFeature, {}, { limit: 2500 });
  pushModuleTrace(trace, "Vehicle Features", featureDocs.length);

  const featureLookup =
    typeof buildFeatureLookupMaps === "function"
      ? buildFeatureLookupMaps(featureDocs)
      : null;
  const featureMap = joinFeatureDocs(featureDocs);

  const featureNeedles = [
    /^(automatic|manual|amt|at|mt|cvt|dct|ivt|transmission)$/i.test(
      parsed.entities.feature || "",
    )
      ? ""
      : parsed.entities.feature,
    /sunroof/i.test(parsed.lower) ? "sunroof" : "",
    /adas/i.test(parsed.lower) ? "adas" : "",
    /6\s*airbags|safest|safety/i.test(parsed.lower) ? "6 airbags" : "",
    /wireless charging/i.test(parsed.lower) ? "wireless charging" : "",
    /ventilated/i.test(parsed.lower) ? "ventilated seats" : "",
  ].filter(Boolean);

  return catalogRows
    .map((row) => {
      const featureDoc =
        featureMap.get(catalogFeatureKey(row)) ||
        featureMap.get(
          [row.brand, row.model, normalizeVariant(row.variant)]
            .join("|")
            .toLowerCase(),
        ) ||
        (featureLookup && typeof resolveFeatureDocForCatalogueRow === "function"
          ? resolveFeatureDocForCatalogueRow(row, featureLookup)
          : null);

      const resolvedBodyType = firstMeaningful(
        featureDoc?.body_type_bucket,
        row.bodyType,
        row.body_type_bucket,
        row.segment,
        row.vehicleType,
      );

      if (
        parsed.entities.bodyType &&
        !bodyTypeMatches(resolvedBodyType, parsed.entities.bodyType)
      )
        return null;

      const matchedReasons = catalogueFilterReason(row, parsed, featureDoc);
      const keyFeatures = [];

      let score = matchedReasons.length * 20;

      const allFeatureRows = flattenFeatures(featureDoc?.features || {});
      const yesFeatureCount = allFeatureRows.filter((item) =>
        yesishFeature(item.value),
      ).length;

      score += Math.min(yesFeatureCount, 120) * 0.8;

      for (const feature of featureNeedles) {
        const match = featureValueForAny(featureDoc?.features, feature);
        const answer = featureAnswerForValue(match);

        if (
          answer === "Yes" ||
          (feature === "6 airbags" && numericFromFeature(match?.value) >= 6)
        ) {
          matchedReasons.push(
            feature === "6 airbags" ? "has 6 airbags" : `has ${feature}`,
          );
          score += 35;
          keyFeatures.push(`${match.key}: ${match.value}`);
        } else if (featureNeedles.length) {
          return null;
        }
      }

      const safetySummary = buildSafetySummaryFromFeatureDoc(featureDoc);

      if (safetyOnly) {
        if (
          !safetySummary.adultNcap &&
          !safetySummary.childNcap &&
          !safetySummary.maxAirbags &&
          !safetySummary.safetyFeatureCount
        ) {
          return null;
        }

        score += safetySummary.score;
        keyFeatures.push(...(safetySummary.safetyHighlights || []).slice(0, 5));
      }

      if (specMode) {
        const specHits = allFeatureRows.filter((item) =>
          new RegExp(specMode, "i").test(`${item.key} ${item.value}`),
        );

        score += specHits.length * 8;
        keyFeatures.push(
          ...specHits.slice(0, 5).map((item) => `${item.name}: ${item.value}`),
        );
      }

      if (useCase) {
        matchedReasons.push(
          `matched ${useCase} use case using captured fields`,
        );
        score += 15;

        if (/family|parents/i.test(useCase)) {
          score += firstNumber(safetySummary.maxAirbags) * 10;
          score += firstNumber(safetySummary.adultNcap) * 25;
          score += firstNumber(safetySummary.childNcap) * 20;
        }

        if (/low emi/i.test(useCase)) {
          score += Math.max(
            0,
            40 - firstNumber(row.onRoad, row.exShowroom) / 100000,
          );
        }

        if (/feature/i.test(useCase)) {
          score += Math.min(yesFeatureCount, 100);
        }
      }

      if (!matchedReasons.length) matchedReasons.push("price band match");

      return {
        id: row.id,
        brand: row.brand || row.make,
        make: row.brand || row.make,
        model: normalizedModelLabel(row),
        variant: displayVariant(row),
        city: row.city,
        bodyType: resolvedBodyType,
        fuelType: row.fuel,
        transmission: row.transmission,
        exShowroom: row.exShowroomPrice,
        onRoad: row.onRoadPrice,
        status: row.status,
        is_discontinued: row.is_discontinued,
        keyFeatures,
        matchedReasons,
        safetySummary,
        adultNcap: safetySummary.adultNcap,
        childNcap: safetySummary.childNcap,
        maxAirbags: safetySummary.maxAirbags,
        safetyHighlights: safetySummary.safetyHighlights,
        featureCount: yesFeatureCount,
        score,
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (firstNumber(b.score) !== firstNumber(a.score))
        return firstNumber(b.score) - firstNumber(a.score);
      return (
        firstNumber(a.onRoad, a.exShowroom) -
        firstNumber(b.onRoad, b.exShowroom)
      );
    });
};

export const vehicleRecommendationSearch = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return {
      widgets: [
        unavailableWidget(
          "Vehicle data unavailable",
          "You do not have catalog access.",
          ["Vehicles"],
        ),
      ],
    };
  }

  const useCase =
    /family|parents|city driving|highway|daily running|low emi|safe|feature-loaded|feature loaded|long drives|chauffeur|rear seat|office|first car|upgrade|premium sedan/i.exec(
      parsed.lower,
    )?.[0] || parsed.intent;

  const safetyMode = parsed.intent === "vehicle_safety_expert";

  const variantRows = await recommendationRows(parsed, trace, {
    safetyOnly: safetyMode,
    specMode:
      parsed.intent === "vehicle_dimension_space_search"
        ? "boot|ground|wheelbase|fuel tank|seating|capacity|space|dimension"
        : parsed.intent === "vehicle_performance_mileage_search"
          ? "mileage|power|torque|engine|displacement|turbo|bhp|ps|nm"
          : "",
    useCase: parsed.intent === "vehicle_use_case_recommendation" ? useCase : "",
  });

  const modelCards = aggregateModelCards(variantRows, { safetyMode });
  const displayedModels = modelCards.slice(0, 12);

  const widgetType = safetyMode
    ? "vehicle_safety_results"
    : parsed.intent?.includes("dimension") ||
        parsed.intent?.includes("performance")
      ? "vehicle_spec_ranking"
      : "vehicle_recommendation_results";

  return {
    widgets: [
      widget(widgetType, parsed.message || "New car recommendations", {
        title: parsed.message,
        city: cityFromParsed(parsed),
        filters: {
          budgetMin: parsed.entities.budgetMin,
          budgetMax: parsed.entities.budgetMax,
          bodyType: parsed.entities.bodyType,
          fuelType: parsed.entities.fuelType,
          transmission: parsed.entities.transmission,
          requiredFeatures: [parsed.entities.feature].filter(Boolean),
          useCase,
          includeDiscontinued: includeDiscontinuedRequested(parsed),
        },

        // MODEL-FIRST OUTPUT
        rows: displayedModels,
        records: displayedModels,
        modelCards: displayedModels,
        groupedByModel: displayedModels,

        // VARIANT DATA STILL AVAILABLE IF UI WANTS TO DRILL DOWN
        variantRows: variantRows.slice(0, LIMIT),
        variants: variantRows.slice(0, LIMIT),

        totalMatchedVariants: variantRows.length,
        totalMatchedModels: modelCards.length,
        displayedModels: displayedModels.length,

        notices: [
          "Showing models first. Variant-level rows are available when you open a specific model.",
          safetyMode
            ? "Safety ranking is based only on stored catalogue safety fields. NCAP ratings are shown only where captured."
            : "",
        ].filter(Boolean),
      }),
    ],
    followUpSuggestions: [
      "Compare top results",
      "Show features",
      "Calculate EMI",
    ],
  };
};

const parseLoanPercentFromQuery = (lower = "") => {
  const match =
    lower.match(/\b(\d+(?:\.\d+)?)\s*%\s*(?:loan|finance|funding)\b/i) ||
    lower.match(
      /\b(\d+(?:\.\d+)?)\s*(?:percent)\s*(?:loan|finance|funding)\b/i,
    ) ||
    lower.match(
      /\b(?:loan|finance|funding)\s*(?:of|for)?\s*(\d+(?:\.\d+)?)\s*%/i,
    ) ||
    lower.match(
      /\b(?:loan|finance|funding)\s*(?:of|for)?\s*(\d+(?:\.\d+)?)\s*percent/i,
    );

  return match ? firstNumber(match[1]) : 0;
};

const parseDownPaymentPercentFromQuery = (lower = "") => {
  const match =
    lower.match(/\b(\d+(?:\.\d+)?)\s*%\s*(?:down payment|down|dp)\b/i) ||
    lower.match(
      /\b(\d+(?:\.\d+)?)\s*(?:percent)\s*(?:down payment|down|dp)\b/i,
    );

  return match ? firstNumber(match[1]) : 0;
};

export const vehicleEmiCalculator = async (parsed, access, trace) => {
  const resolved = await resolveVehicleCatalogRows(parsed, trace, {
    includeCity: true,
    limit: 80,
    moduleName: "Vehicles",
  });

  const rows = sortCatalogueRows(
    applyCatalogueFilters(resolved.rows, parsed),
    parsed,
  );
  const row = rows[0];

  const amountMatch = parsed.lower.match(
    /\bon[- ]?road\s*₹?\s*([\d,.]+)\s*(lakh|lac|l|cr|crore)?/i,
  );
  const manualAmount = amountMatch
    ? firstNumber(amountMatch[1]) *
      (/cr|crore/i.test(amountMatch[2] || "")
        ? 10000000
        : /lakh|lac|l/i.test(amountMatch[2] || "")
          ? 100000
          : 1)
    : 0;

  const compact = row ? vehicleRow(row) : {};

  const exShowroom = firstNumber(compact.exShowroomPrice);
  const onRoad =
    manualAmount || firstNumber(compact.onRoadPrice, compact.exShowroomPrice);

  if (!onRoad) {
    return {
      widgets: [
        unavailableWidget(
          "Need vehicle or amount",
          "Ask EMI with a vehicle variant or on-road amount.",
          ["Vehicles"],
        ),
      ],
    };
  }

  const annualRate = parsed.entities.annualRate || 9;
  const tenureMonths = parsed.entities.tenureMonths || 60;

  const loanPercent = parseLoanPercentFromQuery(parsed.lower || "");
  const explicitDownPaymentPercent =
    parseDownPaymentPercentFromQuery(parsed.lower || "") ||
    firstNumber(parsed.entities.downPaymentPercent);

  const loanPercentOnRoad =
    loanPercent > 0 &&
    /\b(on[- ]?road|on road price)\b/i.test(parsed.lower || "");

  let financeBasis = "on-road";
  let financeAmount = 0;
  let downPayment = 0;
  let downPaymentPercent = explicitDownPaymentPercent || null;

  if (loanPercent > 0) {
    // User asked for 90% loan. By default this means 90% of ex-showroom.
    financeBasis = loanPercentOnRoad ? "on-road" : "ex-showroom";

    const financeBase =
      financeBasis === "ex-showroom" && exShowroom ? exShowroom : onRoad;
    financeAmount = Math.round(financeBase * (loanPercent / 100));

    // Customer still pays the balance of total on-road amount.
    downPayment = Math.max(0, onRoad - financeAmount);
    downPaymentPercent = Math.round((downPayment / onRoad) * 100);
  } else if (parsed.entities.downPayment) {
    financeBasis = "on-road";
    downPayment = firstNumber(parsed.entities.downPayment);
    financeAmount = Math.max(onRoad - downPayment, 0);
    downPaymentPercent = Math.round((downPayment / onRoad) * 100);
  } else if (explicitDownPaymentPercent > 0) {
    financeBasis = "on-road";
    downPayment = Math.round(onRoad * (explicitDownPaymentPercent / 100));
    financeAmount = Math.max(onRoad - downPayment, 0);
    downPaymentPercent = explicitDownPaymentPercent;
  } else {
    financeBasis = "on-road";
    downPayment = Math.round(onRoad * 0.2);
    financeAmount = Math.max(onRoad - downPayment, 0);
    downPaymentPercent = 20;
  }

  const scenarios = [36, 48, 60, 84].map((months) => ({
    label: `${months / 12} years`,
    tenureMonths: months,
    emi: Math.round(emiFor(financeAmount, annualRate, months)),
  }));

  const emi = calculateEMI(financeAmount, annualRate, tenureMonths);

  return {
    widgets: [
      widget("vehicle_emi_calculator", "Vehicle EMI calculator", {
        vehicle: compact.id ? compact : null,
        city: resolved.showingCity || compact.city || cityFromParsed(parsed),
        requestedCity: resolved.requestedCity,
        showingCity:
          resolved.showingCity || compact.city || cityFromParsed(parsed),
        cityFallbackUsed: Boolean(resolved.usedCityFallback),

        price: {
          onRoad,
          exShowroom,
        },

        inputs: {
          financeBasis,
          loanPercent: loanPercent || null,
          downPayment,
          downPaymentPercent,
          tenureMonths,
          annualRate,
        },

        result: {
          loanAmount: financeAmount,
          financeAmount,
          emi,
          totalPayable: emi * tenureMonths + downPayment,
          totalInterest: emi * tenureMonths - financeAmount,
        },

        scenarios,

        notices: [
          loanPercent > 0
            ? `${loanPercent}% loan is calculated on ${financeBasis === "ex-showroom" ? "ex-showroom price" : "on-road price"}.`
            : "Default EMI assumes 20% down payment on on-road price when no down payment is specified.",
          resolved.usedCityFallback
            ? `${titleCase(resolved.requestedCity)} pricing is not available. EMI is calculated using ${titleCase(resolved.showingCity || "new-delhi")} pricing.`
            : "",
        ].filter(Boolean),
      }),
    ],
    followUpSuggestions: [
      "Show pricelist",
      "Compare EMI",
      "Cars with EMI under 25000",
    ],
  };
};

export const vehicleEmiBudgetSearch = async (parsed, access, trace) => {
  const emiMax = parsed.entities.emiMax || 25000;

  const variantRows = (await recommendationRows(parsed, trace))
    .map((row) => {
      const onRoad = firstNumber(row.onRoad, row.exShowroom);
      const downPayment = Math.round(onRoad * 0.2);
      const financeAmount = onRoad - downPayment;
      const emi = Math.round(emiFor(financeAmount, 9, 60));

      return {
        ...row,
        emi,
        financeAmount,
        assumptions: {
          downPaymentPercent: 20,
          annualRate: 9,
          tenureMonths: 60,
        },
      };
    })
    .filter((row) => row.emi <= emiMax);

  const modelCards = aggregateModelCards(variantRows).map((card) => {
    const bestEmiVariant = [...(card.variants || [])].sort(
      (a, b) => firstNumber(a.emi) - firstNumber(b.emi),
    )[0];

    return {
      ...card,
      bestEmiVariant,
      estimatedEmi: bestEmiVariant?.emi,
      financeAmount: bestEmiVariant?.financeAmount,
      assumptions: bestEmiVariant?.assumptions,
      matchedReasons: [
        ...(card.matchedReasons || []),
        `EMI under ₹${emiMax.toLocaleString("en-IN")}`,
      ],
    };
  });

  return {
    widgets: [
      widget("vehicle_emi_recommendations", `Cars with EMI under ${emiMax}`, {
        emiMax,
        assumptions: {
          downPaymentPercent: 20,
          annualRate: 9,
          tenureMonths: 60,
        },

        // MODEL-FIRST
        rows: modelCards.slice(0, 12),
        records: modelCards.slice(0, 12),
        modelCards: modelCards.slice(0, 12),
        groupedByModel: modelCards.slice(0, 12),

        // Drilldown
        variantRows: variantRows.slice(0, LIMIT),
        variants: variantRows.slice(0, LIMIT),

        totalMatchedVariants: variantRows.length,
        totalMatchedModels: modelCards.length,
        displayedModels: Math.min(modelCards.length, 12),
      }),
    ],
  };
};

export const similarCars = async (parsed, access, trace) => {
  const selectedRows = compactVariantRows(
    parsed.context?.selectedVariantRows || [],
  );
  const selectedModel =
    parsed.context?.selectedModels?.[0] || selectedRows[0]?.model;
  const effectiveParsed =
    selectedModel && !parsed.entities.model
      ? {
          ...parsed,
          entities: {
            ...parsed.entities,
            model: selectedModel,
            models: [selectedModel],
          },
        }
      : parsed;
  const baseResult = await vehiclePricelist(effectiveParsed, access, trace);
  const baseRows = baseResult.widgets?.[0]?.rows || [];
  if (!baseRows.length) return baseResult;
  const prices = baseRows
    .map((row) => Number(row.onRoadPrice || row.exShowroomPrice || 0))
    .filter(Boolean);
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
      segment: firstMeaningful(
        items[0].segment,
        items[0].bodyType,
        items[0].body_type,
      ),
      priceRange: prices.length
        ? { min: Math.min(...prices), max: Math.max(...prices) }
        : null,
      fuelOptions: [
        ...new Set(
          items
            .map((item) => firstMeaningful(item.fuel, item.fuel_type))
            .filter(Boolean),
        ),
      ],
      transmissionOptions: [
        ...new Set(items.map((item) => item.transmission).filter(Boolean)),
      ],
      matchedReason: "Similar catalog price band",
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
    followUpSuggestions: [
      "Compare with City and Slavia",
      "View variants",
      "Open full pricelist",
    ],
  };
};

export const vehicleComparison = async (parsed, access, trace) => {
  if (!access.canAccess("vehicles")) {
    noteRestriction(access, "Vehicles", "No vehicle catalog access");
    return {
      widgets: [
        unavailableWidget(
          "Vehicle data unavailable",
          "You do not have catalog access.",
          ["Vehicles"],
        ),
      ],
    };
  }
  const models = parsed.entities.models || [];
  const selectedVariantIds =
    parsed.context?.selectedVariantIds || parsed.filters?.selectedVariantIds;
  if (Array.isArray(selectedVariantIds) && selectedVariantIds.length >= 2) {
    return exactVariantComparison(
      selectedVariantIds,
      trace,
      parsed.context || {},
    );
  }
  if (models.length < 2) {
    return {
      widgets: [
        unavailableWidget(
          "Need models to compare",
          "Ask with two or more models, for example: compare Verna City Slavia.",
          ["Vehicles"],
        ),
      ],
    };
  }
  const groups = await variantGroupsForModels(
    models,
    trace,
    parsed.entities.city || "Delhi",
  );
  const rows = groups.map((group) => {
    const docs = group.variants;
    const prices = docs
      .map((item) => firstNumber(item.onRoadPrice, item.exShowroomPrice))
      .filter(Boolean);
    return {
      make: group.make,
      model: group.displayModel,
      startingPrice: prices.length ? Math.min(...prices) : null,
      topPrice: prices.length ? Math.max(...prices) : null,
      variantCount: docs.length,
      fuelOptions: [...new Set(docs.map((item) => item.fuel).filter(Boolean))],
      transmissionOptions: [
        ...new Set(docs.map((item) => item.transmission).filter(Boolean)),
      ],
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
      widget("vehicle_model_comparison", "Choose variants to compare", {
        subtitle: "Pick one variant per model, then compare exact variants.",
        models: rows,
        variantOptionsByModel: groups,
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
    followUpSuggestions: [
      "Compare top variants",
      "Show catalogues",
      "Show similar cars",
    ],
  };
};

const featureScoreForDoc = (featureDoc = {}) => {
  const features = flattenFeatures(featureDoc?.features || {});
  const available = features.filter((item) => yesishFeature(item.value));

  const safetySummary = buildSafetySummaryFromFeatureDoc(featureDoc);

  const comfortHits = available.filter((item) =>
    /sunroof|ventilated|climate|cruise|wireless|camera|parking|rear ac|touchscreen|speaker|android|apple/i.test(
      `${item.key} ${item.value}`,
    ),
  );

  return {
    featureCount: available.length,
    comfortFeatureCount: comfortHits.length,
    safetySummary,
    score:
      available.length * 1 +
      comfortHits.length * 3 +
      firstNumber(safetySummary.maxAirbags) * 8 +
      firstNumber(safetySummary.adultNcap) * 30 +
      firstNumber(safetySummary.childNcap) * 20 +
      firstNumber(safetySummary.safetyFeatureCount) * 4,
    highlights: [
      ...comfortHits.slice(0, 5).map((item) => `${item.name}: ${item.value}`),
      ...(safetySummary.safetyHighlights || []).slice(0, 5),
    ],
  };
};

export const vehicleVariantRecommendation = async (parsed, access, trace) => {
  const pricelistResult = await vehiclePricelist(parsed, access, trace);
  const priceWidget = pricelistResult.widgets?.find(
    (item) => item.type === "vehicle_pricelist",
  );

  if (!priceWidget?.rows?.length) return pricelistResult;

  const model =
    parsed.entities.model || priceWidget.model || priceWidget.data?.model;
  const rows = activeVariantRows(priceWidget.rows || []);

  const featureDocs = await findLean(
    VehicleFeature,
    featureModelQuery(model, parsed.entities.make),
    {
      sort: { variant: 1 },
      limit: 200,
    },
  );

  pushModuleTrace(trace, "Vehicle Features", featureDocs.length);

  const featureLookup =
    typeof buildFeatureLookupMaps === "function"
      ? buildFeatureLookupMaps(featureDocs)
      : null;

  const prices = rows
    .map((row) => firstNumber(row.onRoadPrice, row.exShowroomPrice))
    .filter(Boolean);
  const minPrice = prices.length ? Math.min(...prices) : 0;
  const maxPrice = prices.length ? Math.max(...prices) : 0;
  const spread = Math.max(maxPrice - minPrice, 1);

  const rankedVariants = rows
    .map((row) => {
      const featureDoc =
        featureLookup && typeof resolveFeatureDocForCatalogueRow === "function"
          ? resolveFeatureDocForCatalogueRow(row, featureLookup)
          : null;

      const featureScore = featureScoreForDoc(featureDoc);
      const price = firstNumber(row.onRoadPrice, row.exShowroomPrice);

      // Best-value logic: cheapest does not automatically win.
      // Mid variants with meaningful features get better value score.
      const pricePosition = price ? (price - minPrice) / spread : 1;
      const priceValueScore = Math.max(0, 70 - pricePosition * 45);

      let score = featureScore.score + priceValueScore;

      if (isDiscontinuedVehicleRow(row)) score -= 200;
      if (!price) score -= 60;

      // Penalize ultra-base variants if feature count is weak.
      if (featureScore.featureCount < 30) score -= 35;

      return {
        ...row,
        price,
        featureScore: featureScore.score,
        featureCount: featureScore.featureCount,
        comfortFeatureCount: featureScore.comfortFeatureCount,
        safetySummary: featureScore.safetySummary,
        highlights: featureScore.highlights,
        valueScore: Math.round(score),
        score: Math.round(score),
        matchedReasons: [
          `${featureScore.featureCount} captured available features`,
          `${featureScore.safetySummary?.maxAirbags || 0} airbags where captured`,
          price
            ? `priced at ₹${price.toLocaleString("en-IN")}`
            : "price not captured",
        ],
      };
    })
    .sort((a, b) => firstNumber(b.score) - firstNumber(a.score));

  const cheapestVariant = [...rankedVariants].sort(
    (a, b) => firstNumber(a.price) - firstNumber(b.price),
  )[0];
  const featureLoadedVariant = [...rankedVariants].sort(
    (a, b) => firstNumber(b.featureCount) - firstNumber(a.featureCount),
  )[0];
  const automaticRecommendation = rankedVariants.find((row) =>
    transmissionMatches(row, "automatic"),
  );
  const recommendedVariant = rankedVariants[0];

  return {
    widgets: [
      widget(
        "vehicle_variant_recommendation",
        `Best ${model || "vehicle"} variant`,
        {
          model,
          recommendedVariant,
          valueVariant: recommendedVariant,
          cheapestVariant,
          featureLoadedVariant,
          automaticRecommendation,
          topRecommendation: recommendedVariant,
          rows: rankedVariants.slice(0, 12),
          records: rankedVariants.slice(0, 12),
          rankedVariants: rankedVariants.slice(0, 24),
          alternatives: rankedVariants.slice(1, 6),
          reasons: recommendedVariant?.matchedReasons || [],
          tradeoffs: [
            cheapestVariant &&
            recommendedVariant &&
            cheapestVariant.variant !== recommendedVariant.variant
              ? `${cheapestVariant.variant} is cheaper, but ${recommendedVariant.variant} has stronger feature/safety value based on stored data.`
              : "",
          ].filter(Boolean),
          notes: [
            "Recommendation is based only on stored price and feature catalogue fields.",
            "Discontinued variants are excluded unless explicitly requested.",
          ],
        },
      ),
    ],
    followUpSuggestions: [
      "Compare top two variants",
      "Show price breakup",
      "Calculate EMI",
    ],
  };
};

const variantTokensFromMessage = (message = "") => [
  ...new Set(
    (
      message.match(
        /\b(?:sx\s?opt|s\s?opt|hte|htk|htx|gtx|sx|vx|zx|hx\d+|xza|xz|alpha|delta|sigma|base|top)(?:\s+(?:plus|opt|turbo|ivt|dct|mt|at|amt|cvt|dt))*\b/gi,
      ) || []
    ).map((item) => normalizeVariant(item)),
  ),
];

const selectRepresentativeVariantRow = (rows = [], token = "") => {
  const needle = normalizeVariant(token);

  const matches = rows.filter((row) =>
    normalizeVariant(displayVariant(row)).includes(needle),
  );

  if (!matches.length) return null;

  return [...matches].sort((a, b) => {
    const aVariant = normalizeVariant(displayVariant(a));
    const bVariant = normalizeVariant(displayVariant(b));

    const aExact = aVariant === needle ? 1 : 0;
    const bExact = bVariant === needle ? 1 : 0;

    if (bExact !== aExact) return bExact - aExact;

    // Prefer non-DT and simpler representative if multiple rows match.
    const aDt = /\bdt\b/i.test(aVariant) ? 1 : 0;
    const bDt = /\bdt\b/i.test(bVariant) ? 1 : 0;
    if (aDt !== bDt) return aDt - bDt;

    return (
      firstNumber(a.onRoadPrice, a.exShowroomPrice) -
      firstNumber(b.onRoadPrice, b.exShowroomPrice)
    );
  })[0];
};

const normalizeFeatureCompareValue = (value) => {
  const text = String(value ?? "").trim();

  if (!text) return "";
  if (/^(not available|no|false|0|-|na|n\/a)$/i.test(text)) return "No";
  if (/^(yes|available|true|1|standard)$/i.test(text)) return "Yes";

  return text;
};

const featureMapForCompare = (featureDoc = {}) => {
  const map = new Map();

  for (const item of flattenFeatures(featureDoc?.features || {})) {
    const key = toWords(item.key);
    if (!key) continue;

    map.set(key, {
      group: item.group,
      feature: item.name,
      fullKey: item.key,
      value: normalizeFeatureCompareValue(item.value),
    });
  }

  return map;
};

const resolveFeatureDocForVariantRow = async (row, featureLookup, model) => {
  if (featureLookup && typeof resolveFeatureDocForCatalogueRow === "function") {
    const resolved = resolveFeatureDocForCatalogueRow(row, featureLookup);
    if (resolved) return resolved;
  }

  const variantNeedle = normalizeVariant(displayVariant(row));

  const docs = await findLean(
    VehicleFeature,
    featureModelQuery(model || row.model, row.brand || row.make),
    {
      sort: { variant: 1 },
      limit: 200,
    },
  );

  return (
    docs.find((doc) => normalizeVariant(doc.variant) === variantNeedle) ||
    docs.find((doc) => normalizeVariant(doc.variant).includes(variantNeedle)) ||
    docs.find((doc) => variantNeedle.includes(normalizeVariant(doc.variant))) ||
    null
  );
};

export const vehicleVariantDifference = async (parsed, access, trace) => {
  const messageText = String(
    parsed.message || parsed.query || parsed.rawMessage || parsed.lower || "",
  );

  const variantTokens = [
    ...new Set(
      (
        messageText.match(
          /\b(?:sx\s?opt|s\s?opt|hte|htk|htx|gtx|sx|vx|zx|hx\d+|xza|xz|alpha|delta|sigma|base|top)(?:\s+(?:plus|opt|turbo|ivt|dct|mt|at|amt|cvt|dt))*\b/gi,
        ) || []
      ).map((item) => normalizeVariant(item)),
    ),
  ].filter(Boolean);

  if (variantTokens.length < 2) {
    return {
      widgets: [
        unavailableWidget(
          "Need two variants",
          "Ask with two variants, for example: Difference between Verna HX6 and HX8.",
          ["Vehicles", "Vehicle Features"],
        ),
      ],
    };
  }

  // IMPORTANT:
  // Do not use parsed.entities.variant here, because router only extracts the first variant.
  // For "Difference between Verna HX6 and HX8", parsed.entities.variant is HX6,
  // which filters out HX8 before comparison.
  const modelOnlyParsed = {
    ...parsed,
    entities: {
      ...parsed.entities,
      variant: "",
    },
  };

  const resolved = await resolveVehicleCatalogRows(modelOnlyParsed, trace, {
    includeCity: true,
    limit: 240,
    moduleName: "Vehicles",
  });

  const catalogueRows = sortCatalogueRows(
    applyCatalogueFilters(resolved.rows, modelOnlyParsed),
    modelOnlyParsed,
  );

  const compactRows = uniqueRows(
    catalogueRows.map((row) => vehicleRow(row)),
    (row) =>
      [row.variant, row.fuel, row.transmission, row.city]
        .join("|")
        .toLowerCase(),
  );

  const pickVariantRow = (token) => {
    const needle = normalizeVariant(token);

    const candidates = compactRows.filter((row) => {
      const variant = normalizeVariant(row.variant);
      return (
        variant === needle ||
        variant.includes(needle) ||
        needle.includes(variant)
      );
    });

    if (!candidates.length) return null;

    return [...candidates].sort((a, b) => {
      const av = normalizeVariant(a.variant);
      const bv = normalizeVariant(b.variant);

      const aExact = av === needle ? 1 : 0;
      const bExact = bv === needle ? 1 : 0;
      if (bExact !== aExact) return bExact - aExact;

      // Prefer simpler representative row if multiple city/fuel/DT rows exist.
      const aDt = /\bdt\b/i.test(av) ? 1 : 0;
      const bDt = /\bdt\b/i.test(bv) ? 1 : 0;
      if (aDt !== bDt) return aDt - bDt;

      return (
        firstNumber(a.onRoadPrice, a.exShowroomPrice) -
        firstNumber(b.onRoadPrice, b.exShowroomPrice)
      );
    })[0];
  };

  const baseRow = pickVariantRow(variantTokens[0]);
  const compareRow = pickVariantRow(variantTokens[1]);

  if (!baseRow || !compareRow) {
    const availableVariants = compactRows
      .map((row) => row.variant)
      .filter(Boolean)
      .slice(0, 20);

    return {
      widgets: [
        unavailableWidget(
          "Variants not found",
          `I could not find both variants: ${variantTokens.join(" and ")}.`,
          ["Vehicles", "Vehicle Features"],
        ),
        widget("variant_ambiguity", "Available matching variants", {
          type: "variant_ambiguity",
          model: parsed.entities.model,
          options: availableVariants.map((variant) => ({
            label: variant,
            variant,
            entityType: "catalogue_variant",
          })),
          rows: availableVariants.map((variant) => ({ variant })),
        }),
      ],
    };
  }

  const model = parsed.entities.model || baseRow.model || compareRow.model;

  const featureDocs = await findLean(
    VehicleFeature,
    featureModelQuery(
      model,
      parsed.entities.make || baseRow.brand || baseRow.make,
    ),
    {
      sort: { variant: 1 },
      limit: 260,
    },
  );

  pushModuleTrace(trace, "Vehicle Features", featureDocs.length);

  const normalizeCompareKey = (value = "") =>
    String(value || "")
      .toLowerCase()
      .replace(/&amp;/g, "&")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();

  const pickFeatureDoc = (row, token) => {
    const needle = normalizeVariant(token);
    const rowVariant = normalizeVariant(row.variant);

    return (
      featureDocs.find((doc) => normalizeVariant(doc.variant) === rowVariant) ||
      featureDocs.find((doc) =>
        normalizeVariant(doc.variant).includes(needle),
      ) ||
      featureDocs.find((doc) =>
        rowVariant.includes(normalizeVariant(doc.variant)),
      ) ||
      featureDocs.find((doc) =>
        normalizeVariant(doc.variant).includes(rowVariant),
      ) ||
      null
    );
  };

  const baseFeatureDoc = pickFeatureDoc(baseRow, variantTokens[0]);
  const compareFeatureDoc = pickFeatureDoc(compareRow, variantTokens[1]);

  const normalizeCompareValue = (value) => {
    const text = String(value ?? "").trim();
    if (!text) return "";
    if (/^(not available|no|false|0|-|na|n\/a)$/i.test(text)) return "No";
    if (/^(yes|available|true|1|standard)$/i.test(text)) return "Yes";
    return text;
  };

  const buildFeatureMap = (featureDoc = {}) => {
    const map = new Map();

    for (const item of flattenFeatures(featureDoc?.features || {})) {
      const key = normalizeCompareKey(item.key);
      if (!key) continue;

      map.set(key, {
        group:
          item.group ||
          String(item.key || "")
            .split("|")[0]
            ?.trim() ||
          "Features",
        feature:
          item.name ||
          String(item.key || "")
            .split("|")
            .pop()
            ?.trim() ||
          item.key,
        fullKey: item.key,
        value: normalizeCompareValue(item.value),
      });
    }

    return map;
  };

  const baseFeatures = buildFeatureMap(baseFeatureDoc);
  const compareFeatures = buildFeatureMap(compareFeatureDoc);

  const allKeys = [
    ...new Set([...baseFeatures.keys(), ...compareFeatures.keys()]),
  ];

  const addedFeatures = [];
  const removedFeatures = [];
  const changedFeatures = [];
  const sameImportantFeatures = [];

  for (const key of allKeys) {
    const base = baseFeatures.get(key);
    const compare = compareFeatures.get(key);

    const baseValue = base?.value || "";
    const compareValue = compare?.value || "";
    const feature = compare?.feature || base?.feature || key;
    const group = compare?.group || base?.group || "Features";

    if (!base && compare && compareValue && compareValue !== "No") {
      addedFeatures.push({ group, feature, baseValue: "", compareValue });
      continue;
    }

    if (base && !compare && baseValue && baseValue !== "No") {
      removedFeatures.push({ group, feature, baseValue, compareValue: "" });
      continue;
    }

    if (baseValue !== compareValue) {
      if (
        (baseValue === "No" || !baseValue) &&
        compareValue &&
        compareValue !== "No"
      ) {
        addedFeatures.push({ group, feature, baseValue, compareValue });
      } else if (
        baseValue &&
        baseValue !== "No" &&
        (compareValue === "No" || !compareValue)
      ) {
        removedFeatures.push({ group, feature, baseValue, compareValue });
      } else {
        changedFeatures.push({ group, feature, baseValue, compareValue });
      }
    } else if (
      /airbag|sunroof|adas|esc|tpms|isofix|climate|camera|mileage|engine|transmission|boot|ground/i.test(
        feature,
      ) &&
      baseValue
    ) {
      sameImportantFeatures.push({ group, feature, value: baseValue });
    }
  }

  const safetyDifferences = [
    ...addedFeatures,
    ...removedFeatures,
    ...changedFeatures,
  ].filter((item) =>
    /safety|airbag|adas|esc|tpms|isofix|hill|lane|blind|collision|brake|abs/i.test(
      `${item.group} ${item.feature}`,
    ),
  );

  const engineDifferences = [
    ...addedFeatures,
    ...removedFeatures,
    ...changedFeatures,
  ].filter((item) =>
    /engine|transmission|gearbox|mileage|power|torque|fuel|performance|displacement/i.test(
      `${item.group} ${item.feature}`,
    ),
  );

  const baseOnRoad = firstNumber(
    baseRow.onRoadPrice,
    baseRow.price,
    baseRow.exShowroomPrice,
  );
  const compareOnRoad = firstNumber(
    compareRow.onRoadPrice,
    compareRow.price,
    compareRow.exShowroomPrice,
  );
  const priceDifference = compareOnRoad - baseOnRoad;

  const recommendation =
    priceDifference > 0 && addedFeatures.length
      ? `${compareRow.variant} costs ₹${priceDifference.toLocaleString("en-IN")} more and adds ${addedFeatures.length} captured feature(s).`
      : priceDifference > 0
        ? `${compareRow.variant} costs ₹${priceDifference.toLocaleString("en-IN")} more, but no major extra captured features were found.`
        : `${compareRow.variant} is not costlier than ${baseRow.variant} in the stored price data.`;

  return {
    widgets: [
      widget(
        "vehicle_variant_difference",
        `${baseRow.variant} vs ${compareRow.variant}`,
        {
          type: "vehicle_variant_difference",
          model,
          city: resolved.showingCity || baseRow.city,
          requestedCity: resolved.requestedCity,
          showingCity: resolved.showingCity || baseRow.city,
          cityFallbackUsed: Boolean(resolved.usedCityFallback),

          baseVariant: baseRow,
          compareVariant: compareRow,

          price: {
            baseOnRoad,
            compareOnRoad,
            difference: priceDifference,
            baseExShowroom: firstNumber(baseRow.exShowroomPrice),
            compareExShowroom: firstNumber(compareRow.exShowroomPrice),
          },

          addedFeatures,
          removedFeatures,
          changedFeatures,
          sameImportantFeatures: sameImportantFeatures.slice(0, 20),
          safetyDifferences,
          engineDifferences,

          summary: {
            addedCount: addedFeatures.length,
            removedCount: removedFeatures.length,
            changedCount: changedFeatures.length,
            recommendation,
          },

          rows: [
            ...addedFeatures.map((item) => ({ type: "Added", ...item })),
            ...removedFeatures.map((item) => ({ type: "Removed", ...item })),
            ...changedFeatures.map((item) => ({ type: "Changed", ...item })),
          ],

          comparisonRows: [
            { label: "On-road price", values: [baseOnRoad, compareOnRoad] },
            {
              label: "Ex-showroom",
              values: [
                firstNumber(baseRow.exShowroomPrice),
                firstNumber(compareRow.exShowroomPrice),
              ],
            },
            { label: "Fuel", values: [baseRow.fuel, compareRow.fuel] },
            {
              label: "Transmission",
              values: [baseRow.transmission, compareRow.transmission],
            },
          ],

          variants: [baseRow, compareRow],

          notices: [
            "Feature difference is based only on stored feature catalogue fields.",
            !baseFeatureDoc || !compareFeatureDoc
              ? "One or both exact feature records were not found; matched by nearest variant text where possible."
              : "",
          ].filter(Boolean),
        },
      ),
    ],
    followUpSuggestions: [
      "Calculate EMI",
      "Show price breakup",
      "Compare with other variant",
    ],
  };
};

export const vehiclePriceHistory = async (parsed, access, trace) => {
  const map = getFieldMap("price_history");
  const collection = mongoose.connection.db.collection(map.collectionName);
  const clauses = [];
  if (parsed.entities.make)
    clauses.push({ brand: new RegExp(escapeRegex(parsed.entities.make), "i") });
  if (parsed.entities.model)
    clauses.push({
      model: new RegExp(escapeRegex(parsed.entities.model), "i"),
    });
  if (parsed.entities.variant)
    clauses.push({
      variant: new RegExp(escapeRegex(parsed.entities.variant), "i"),
    });
  if (parsed.entities.city)
    clauses.push({
      city: new RegExp(escapeRegex(cityFromParsed(parsed)), "i"),
    });
  const query = clauses.length ? { $and: clauses } : {};
  const rows = await collection
    .find(query)
    .sort({ date: -1, createdAt: -1 })
    .limit(120)
    .maxTimeMS(3500)
    .toArray();
  pushModuleTrace(trace, map.module, rows.length);
  if (!rows.length) {
    return {
      widgets: [
        unavailableWidget(
          "No price history found",
          "No stored price_history rows matched this request.",
          [map.module],
        ),
      ],
    };
  }
  const normalized = rows.map((row, index) => {
    const price = firstNumber(
      row.price,
      row.onRoadPrice,
      row.exShowroom,
      row.ex_showroom,
    );
    const previous = firstNumber(
      rows[index + 1]?.price,
      rows[index + 1]?.onRoadPrice,
      rows[index + 1]?.exShowroom,
      rows[index + 1]?.ex_showroom,
    );
    const changeAmount = previous ? price - previous : null;
    return {
      date: formatDateValue(
        firstMeaningful(row.date, row.createdAt, row.updatedAt),
      ),
      brand: row.brand,
      model: row.model,
      variant: row.variant,
      city: row.city,
      price,
      changeAmount,
      changePercent:
        previous && changeAmount !== null
          ? (changeAmount / previous) * 100
          : null,
    };
  });
  return {
    widgets: [
      widget("vehicle_price_history", "Vehicle price history", {
        brand: parsed.entities.make,
        model: parsed.entities.model,
        variant: parsed.entities.variant,
        city: parsed.entities.city,
        rows: normalized,
        summary: {
          latestDate: normalized[0]?.date,
          latestPrice: normalized[0]?.price,
          previousPrice: normalized[1]?.price,
          changeAmount: normalized[0]?.changeAmount,
          changePercent: normalized[0]?.changePercent,
          totalEntries: normalized.length,
        },
      }),
    ],
  };
};

export const latestCatalogueUpdates = async (parsed, access, trace) => {
  const rows = await findLean(
    Vehicle,
    {},
    { sort: { LastSeenDate: -1, createdAt: -1 }, limit: 120 },
  );
  pushModuleTrace(trace, "Vehicles", rows.length);
  const compact = rows
    .map(vehicleRow)
    .filter((row) => row.LastSeenDate || row.lastUpdated);
  if (!compact.length) {
    return {
      widgets: [
        unavailableWidget(
          "Launch status not captured",
          "Launch status is not captured in current database. Showing latest catalogue updates requires LastSeenDate/createdAt data.",
          ["Vehicles"],
        ),
      ],
    };
  }
  return {
    widgets: [
      widget("latest_catalogue_updates", "Latest catalogue updates", {
        rows: compact.slice(0, LIMIT),
        records: compact.slice(0, LIMIT),
        groupedByModel: Object.values(
          compact.reduce((acc, row) => {
            const key = `${row.brand}|${row.model}`.toLowerCase();
            if (!acc[key])
              acc[key] = {
                brand: row.brand,
                model: row.model,
                variants: 0,
                lastUpdated: row.lastUpdated,
              };
            acc[key].variants += 1;
            return acc;
          }, {}),
        ),
      }),
    ],
  };
};

export const priceHistoryReport = async (parsed, access, trace) => {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);
  const query = { createdAt: { $gte: start, $lte: now } };
  if (parsed.entities.model)
    Object.assign(query, vehicleModelClause(parsed.entities.model));
  const rows = await findLean(Vehicle, query, {
    sort: { createdAt: -1 },
    limit: LIMIT,
  });
  pushModuleTrace(trace, "Vehicles", rows.length);
  return {
    widgets: [
      widget("price_history_report", "Variants added this month", {
        summary: {
          count: rows.length,
          periodStart: start.toISOString(),
          periodEnd: now.toISOString(),
        },
        rows: rows.map(vehicleRow),
        notices: [
          "Price history is not stored as a dedicated history table. Showing records inferred from createdAt/updatedAt.",
        ],
        actions: [
          action("open_pricelist_prefilled", "Open pricelist", {
            route: "/vehicles/price-list",
            query: parsed.entities.model
              ? { model: parsed.entities.model }
              : {},
          }),
        ],
      }),
    ],
    followUpSuggestions: [
      "Open full pricelist",
      "Show top variants",
      "Compare with City and Slavia",
    ],
  };
};
