import mongoose from "mongoose";

import { parseAciFeatureRequestFromMessage } from "./aiAgent.featureRequestParser.js";

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const decodeHtml = (value = "") =>
  cleanText(value)
    .replace(/&amp;/gi, "&")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/gi, '"')
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");

const toArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value].filter(Boolean);
};

const isPlainObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value);

const firstText = (...values) => {
  for (const value of values) {
    const text = decodeHtml(value);
    if (text) return text;
  }
  return "";
};

const slugify = (value = "", fallback = "item") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "") || fallback;

const normalizeKey = (value = "") =>
  decodeHtml(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const compactKey = (value = "") => normalizeKey(value).replace(/[^a-z0-9]/g, "");

const normalizeFeatureKey = (value = "") =>
  normalizeKey(String(value || "").replace(/_/g, " ")).replace(/\s+/g, "_");

const titleCase = (value = "") =>
  decodeHtml(value)
    .split(" ")
    .map((part) => {
      if (!part) return "";
      if (/^[A-Z0-9]+$/.test(part)) return part;
      if (/^\d/.test(part)) return part.toUpperCase();
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join(" ");

const stripVehicleName = ({ value = "", brand = "", model = "" } = {}) => {
  let text = decodeHtml(value);
  const candidates = [
    [brand, model].filter(Boolean).join(" "),
    model,
    brand,
  ].filter(Boolean);

  candidates.forEach((candidate) => {
    const escaped = candidate.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    text = text.replace(new RegExp(`^\\s*${escaped}\\s+`, "i"), "");
  });

  return text || decodeHtml(value);
};

const getRowBrand = (row = {}) =>
  titleCase(firstText(row.brand, row.make, row.raw?.brand, row.raw?.make));

const getRowModel = (row = {}) =>
  titleCase(firstText(row.model, row.raw?.model, row.modelName, row.raw?.modelName));

const getRowVariantFull = (row = {}) =>
  firstText(
    row.variant,
    row.variantFull,
    row.variantName,
    row.label,
    row.name,
    row.raw?.variant,
    row.raw?.variantName,
    row.raw?.name,
  );

const getRowVariantLabel = (row = {}) => {
  const brand = getRowBrand(row);
  const model = getRowModel(row);
  const full = getRowVariantFull(row);

  return stripVehicleName({
    value: full,
    brand,
    model,
  }) || "Variant";
};

const isUnavailableValue = (value) => {
  if (value === false || value === null || value === undefined) return true;

  const text = decodeHtml(value).toLowerCase();

  if (!text) return true;

  return /^(no|na|n\/a|not available|unavailable|absent|nil|false|-)$/.test(text);
};

const isAvailableValue = (value) => !isUnavailableValue(value);

const getFeatureCategory = (section = "", name = "") => {
  const sectionKey = normalizeKey(section);
  const nameKey = normalizeKey(name);
  const text = `${sectionKey} ${nameKey}`;

  if (/adas|lane|aeb|blind|collision|autonomous|assist|departure|forward collision|driver assistance/.test(text)) {
    return "adas";
  }

  if (sectionKey.includes("safety")) return "safety";

  if (
    sectionKey.includes("comfort") ||
    sectionKey.includes("convenience") ||
    sectionKey.includes("interior")
  ) {
    return "comfort";
  }

  if (
    sectionKey.includes("entertainment") ||
    sectionKey.includes("communication")
  ) {
    return "infotainment";
  }

  if (
    sectionKey.includes("engine") ||
    sectionKey.includes("transmission") ||
    sectionKey.includes("fuel") ||
    sectionKey.includes("performance")
  ) {
    return "performance";
  }

  if (
    sectionKey.includes("dimension") ||
    sectionKey.includes("capacity")
  ) {
    return "dimensions";
  }

  if (sectionKey.includes("exterior")) return "exterior";

  if (/airbag|esc|esp|isofix|brake|tpms|hill|stability|ncap|child|abs|traction|seat belt|immobilizer|camera|sensor/.test(nameKey)) {
    return "safety";
  }

  if (/audio|speaker|touch|screen|android|apple|carplay|infotain|connected|music|jbl|display|navigation|bluetooth|usb|radio/.test(nameKey)) {
    return "infotainment";
  }

  if (/engine|transmission|torque|cylinder|fuel|tank|mileage|displacement|gearbox|drive type|emission|power bhp|max power|max torque/.test(nameKey)) {
    return "performance";
  }

  if (/length|width|height|boot space|wheel base|ground clearance|seating capacity|doors|kerb weight|gross weight/.test(nameKey)) {
    return "dimensions";
  }

  if (/headlamp|alloy|wheel cover|roof|spoiler|wiper|defogger|fog|tyre|drl|tail lamp|outside mirror|orvm/.test(nameKey)) {
    return "exterior";
  }

  if (/power steering|air conditioning|heater|seat|climate|cruise|ventilated|wireless charging|charger|keyless|start stop|parking|mirror|armrest|glove|voice|paddle|headrest|reading lamp|trunk light|accessory power outlet/.test(nameKey)) {
    return "comfort";
  }

  return "other";
};

const isSummarySection = (section = "") => {
  const key = normalizeKey(section);
  return key.startsWith("key specifications of") || key.startsWith("key features of");
};

const getSummaryKind = (section = "") => {
  const key = normalizeKey(section);
  if (key.startsWith("key specifications of")) return "quick_spec";
  if (key.startsWith("key features of")) return "highlight";
  return "";
};

const cleanSectionLabel = (section = "") => {
  const text = decodeHtml(section);
  if (/^key specifications of/i.test(text)) return "Key Specifications";
  if (/^key features of/i.test(text)) return "Key Features";
  return text || "Features";
};

const splitFeatureKey = (key = "") => {
  const text = decodeHtml(key);

  if (text.includes("|")) {
    const [section, ...rest] = text.split("|").map(decodeHtml);
    return {
      section: cleanSectionLabel(section),
      originalSection: section,
      name: rest.join(" | ") || section,
      summaryKind: getSummaryKind(section),
    };
  }

  if (text.includes(" - ")) {
    const [section, ...rest] = text.split(" - ").map(decodeHtml);
    return {
      section: cleanSectionLabel(section),
      originalSection: section,
      name: rest.join(" - ") || section,
      summaryKind: getSummaryKind(section),
    };
  }

  if (text.includes(":")) {
    const [section, ...rest] = text.split(":").map(decodeHtml);
    return {
      section: cleanSectionLabel(section),
      originalSection: section,
      name: rest.join(": ") || section,
      summaryKind: getSummaryKind(section),
    };
  }

  return {
    section: "Features",
    originalSection: "Features",
    name: text,
    summaryKind: "",
  };
};

const KNOWN_VALUE_SUFFIXES = [
  "Not Available",
  "With Guidelines",
  "With Guidedlines",
  "Front & Rear",
  "Front and Rear",
  "All Windows",
  "Driver and Passenger",
  "Android Auto, Apple CarPlay",
  "Apple CarPlay",
  "Android Auto",
  "Electronic",
  "Automatic",
  "Manual",
  "Electric",
  "Front",
  "Rear",
  "Driver",
  "Passenger",
  "Yes",
  "No",
];

const parseFeatureString = (value = "", index = 0) => {
  const text = decodeHtml(value);
  const split = splitFeatureKey(text);
  let name = split.name;
  let parsedValue = "Yes";

  const lower = name.toLowerCase();

  const suffix = KNOWN_VALUE_SUFFIXES.find((item) => {
    const itemLower = item.toLowerCase();
    return lower === itemLower || lower.endsWith(` ${itemLower}`);
  });

  if (suffix) {
    name = name.slice(0, Math.max(0, name.length - suffix.length)).trim();
    parsedValue = suffix;
  }

  if (!name) name = `Feature ${index + 1}`;

  return {
    rawKey: `${split.originalSection} | ${name}`,
    section: split.section,
    originalSection: split.originalSection,
    name,
    value: parsedValue,
    summaryKind: split.summaryKind,
  };
};

const getFeatureEntriesFromRow = (row = {}) => {
  const rawFeatureMap =
    isPlainObject(row.raw?.features)
      ? row.raw.features
      : isPlainObject(row.features)
        ? row.features
        : isPlainObject(row.featureMap)
          ? row.featureMap
          : null;

  if (rawFeatureMap) {
    return Object.entries(rawFeatureMap)
      .map(([key, value]) => {
        const split = splitFeatureKey(key);

        return {
          rawKey: decodeHtml(key),
          section: split.section,
          originalSection: split.originalSection,
          name: split.name,
          value: decodeHtml(value),
          summaryKind: split.summaryKind,
        };
      })
      .filter((item) => item.name);
  }

  if (Array.isArray(row.features)) {
    return row.features
      .map((item, index) => parseFeatureString(item, index))
      .filter((item) => item.name);
  }

  return [];
};

const FEATURE_PRIORITY = [
  "sunroof",
  "adas",
  "airbag",
  "ventilated",
  "wireless charging",
  "360",
  "camera",
  "cruise",
  "automatic climate",
  "touchscreen",
  "apple carplay",
  "android auto",
  "isofix",
  "tpms",
  "esc",
  "abs",
  "alloy wheels",
  "rear ac vents",
  "paddle shifters",
  "drive modes",
];

const QUICK_SPEC_NAMES = [
  "Fuel Type",
  "Transmission Type",
  "Engine Type",
  "Engine Displacement",
  "Displacement",
  "Max Power",
  "Max Torque",
  "Seating Capacity",
  "Body Type",
  "Reported Boot Space",
  "Boot Space",
  "Fuel Tank Capacity",
  "Drive Type",
];

const buildFeatureObject = ({ entry, row, index }) => {
  const available = isAvailableValue(entry.value);
  const category = getFeatureCategory(entry.section, entry.name);
  const brand = getRowBrand(row);
  const model = getRowModel(row);
  const variant = getRowVariantLabel(row);
  const displayValue = decodeHtml(entry.value) || (available ? "Available" : "Not available");

  return {
    id: slugify(`${variant}-${entry.section}-${entry.name}-${index}`, `feature-${index + 1}`),
    key: entry.rawKey,
    section: entry.section,
    originalSection: entry.originalSection,
    group: entry.section,
    category,
    name: decodeHtml(entry.name),
    label: decodeHtml(entry.name),
    title: decodeHtml(entry.name),
    value: displayValue,
    displayValue,
    available,
    present: available,
    included: available,
    summaryKind: entry.summaryKind,
    searchableText: normalizeKey(`${entry.section} ${entry.name} ${displayValue}`),
    brand,
    make: brand,
    model,
    variant,
  };
};

const groupFeatures = (features = []) => {
  const map = new Map();

  features.forEach((feature) => {
    const key = slugify(feature.section || "Features");
    const existing =
      map.get(key) ||
      {
        id: key,
        label: feature.section || "Features",
        name: feature.section || "Features",
        category: feature.category || "other",
        availableCount: 0,
        unavailableCount: 0,
        totalCount: 0,
        features: [],
      };

    existing.features.push(feature);
    existing.totalCount += 1;
    if (feature.available) existing.availableCount += 1;
    else existing.unavailableCount += 1;

    map.set(key, existing);
  });

  const preferredOrder = [
    "Safety",
    "Comfort & Convenience",
    "Entertainment & Communication",
    "Interior",
    "Exterior",
    "Engine & Transmission",
    "Fuel & Performance",
    "Dimensions & Capacity",
    "Suspension, Steering & Brakes",
  ];

  return [...map.values()].sort((a, b) => {
    const aIndex = preferredOrder.findIndex((item) => normalizeKey(item) === normalizeKey(a.label));
    const bIndex = preferredOrder.findIndex((item) => normalizeKey(item) === normalizeKey(b.label));

    if (aIndex >= 0 && bIndex >= 0) return aIndex - bIndex;
    if (aIndex >= 0) return -1;
    if (bIndex >= 0) return 1;

    return a.label.localeCompare(b.label);
  });
};

const uniqueByKey = (rows = [], keyGetter) => {
  const seen = new Set();
  const output = [];

  rows.forEach((row) => {
    const key = keyGetter(row);
    if (!key || seen.has(key)) return;
    seen.add(key);
    output.push(row);
  });

  return output;
};

const buildQuickSpecs = (allFeatures = [], mainFeatures = []) => {
  const summarySpecs = allFeatures
    .filter((feature) => feature.summaryKind === "quick_spec")
    .map((feature) => ({
      id: slugify(`spec-${feature.name}`),
      label: feature.name,
      value: feature.value,
      icon: getFeatureCategory(feature.section, feature.name),
    }));

  const mainSpecs = mainFeatures
    .filter((feature) =>
      QUICK_SPEC_NAMES.some((name) => normalizeKey(name) === normalizeKey(feature.name)),
    )
    .map((feature) => ({
      id: slugify(`spec-${feature.name}`),
      label: feature.name,
      value: feature.value,
      icon: getFeatureCategory(feature.section, feature.name),
    }));

  return uniqueByKey([...summarySpecs, ...mainSpecs], (item) => normalizeKey(item.label)).slice(0, 10);
};

const formatHighlight = (feature) => {
  const name = feature.name;
  const value = feature.value;

  if (/airbag/i.test(name) && /\d+/.test(value)) return `${value} airbags`;
  if (/no\.?\s*of\s*speakers|speakers/i.test(name) && /\d+/.test(value)) return `${value} speakers`;
  if (/touchscreen size/i.test(name)) return `${value} touchscreen`;
  if (/sunroof/i.test(name)) return `${name} available`;

  if (/^(yes|available)$/i.test(value)) return `${name} available`;
  if (/not available/i.test(value)) return "";

  return `${name}: ${value}`;
};

const buildHighlights = (allFeatures = [], mainFeatures = []) => {
  const keyFeatureHighlights = allFeatures
    .filter((feature) => feature.summaryKind === "highlight" && feature.available)
    .map(formatHighlight)
    .filter(Boolean);

  const priorityHighlights = mainFeatures
    .filter((feature) => {
      if (!feature.available) return false;
      const text = normalizeKey(`${feature.name} ${feature.value}`);
      return FEATURE_PRIORITY.some((needle) => text.includes(normalizeKey(needle)));
    })
    .map(formatHighlight)
    .filter(Boolean);

  return uniqueByKey(
    [...priorityHighlights, ...keyFeatureHighlights].map((label, index) => ({
      id: slugify(`highlight-${label}`, `highlight-${index + 1}`),
      label,
      text: label,
    })),
    (item) => item.label,
  ).slice(0, 8);
};

const buildStats = (features = [], groups = []) => {
  const byCategory = {};

  features.forEach((feature) => {
    const bucket =
      byCategory[feature.category] ||
      {
        available: 0,
        unavailable: 0,
        total: 0,
      };

    bucket.total += 1;
    if (feature.available) bucket.available += 1;
    else bucket.unavailable += 1;

    byCategory[feature.category] = bucket;
  });

  return {
    total: features.length,
    available: features.filter((feature) => feature.available).length,
    unavailable: features.filter((feature) => !feature.available).length,
    groups: groups.length,
    byCategory,
  };
};

const numberFromValue = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildVariantPayload = (row = {}, index = 0) => {
  const brand = getRowBrand(row);
  const model = getRowModel(row);
  const fullVariantName = getRowVariantFull(row);
  const variantLabel = getRowVariantLabel(row);

  const entries = getFeatureEntriesFromRow(row);
  const allFeatures = entries.map((entry, featureIndex) =>
    buildFeatureObject({
      entry,
      row,
      index: featureIndex,
    }),
  );

  const mainFeatures = allFeatures.filter((feature) => !feature.summaryKind);
  const featureGroups = groupFeatures(mainFeatures);
  const quickSpecs = buildQuickSpecs(allFeatures, mainFeatures);
  const highlights = buildHighlights(allFeatures, mainFeatures);
  const stats = buildStats(mainFeatures, featureGroups);

  const exShowroomPrice = numberFromValue(row.exShowroomPrice ?? row.raw?.ex_showroom);
  const onRoadPrice = numberFromValue(row.onRoadPrice ?? row.raw?.onRoadPrice ?? row.raw?.on_road_price_cardekho);

  /*
    Active/current status should come from the vehicle/pricelist-enriched row.
    If active is explicitly false, trust that.
    If active is missing, fall back to: has current price + not discontinued.
  */
  const discontinued =
    row.discontinued === true ||
    row.raw?.is_discontinued === true ||
    row.raw?.discontinued === true;

  const explicitlyActive =
    row.active === true ||
    row.raw?.active === true ||
    row.raw?.is_active === true;

  const explicitlyInactive =
    row.active === false ||
    row.raw?.active === false ||
    row.raw?.is_active === false;

  const hasCurrentPrice =
    (Number.isFinite(exShowroomPrice) && exShowroomPrice > 0) ||
    (Number.isFinite(onRoadPrice) && onRoadPrice > 0);

  const active = explicitlyInactive
    ? false
    : explicitlyActive
      ? true
      : !discontinued && hasCurrentPrice;

  return {
    id: String(row.id || row._id || row.raw?._id || slugify(`${brand}-${model}-${variantLabel}`, `variant-${index + 1}`)),
    label: variantLabel,
    name: variantLabel,
    variant: variantLabel,
    variantName: variantLabel,
    fullVariantName,
    brand,
    make: brand,
    model,
    bodyType: row.body_type_bucket || row.bodyType || row.raw?.body_type_bucket || "",
    exShowroomPrice,
    onRoadPrice,
    price: onRoadPrice || exShowroomPrice || null,
    active,
    discontinued,
    featureCount: mainFeatures.length,
    availableCount: stats.available,
    unavailableCount: stats.unavailable,
    groupCount: featureGroups.length,
    quickSpecCount: quickSpecs.length,
    highlightCount: highlights.length,
    features: mainFeatures,
    featureList: mainFeatures,
    featureGroups,
    quickSpecs,
    highlights,
    categoryStats: stats,
    rawRowId: row.id || row._id || row.raw?._id || "",
  };
};

const getSourceRows = ({ response = {}, widget = {} } = {}) => {
  const candidates = [
    response.data?.rows,
    response.rows,
    response.items,
    widget.rows,
    widget.items,
    response.features,
    widget.features,
    response.data?.features,
  ];

  for (const candidate of candidates) {
    const rows = toArray(candidate);

    if (
      rows.some(
        (row) =>
          row?.variant ||
          row?.variantName ||
          row?.raw?.variant ||
          isPlainObject(row?.raw?.features) ||
          isPlainObject(row?.features) ||
          Array.isArray(row?.features),
      )
    ) {
      return rows;
    }
  }

  return [];
};

const buildDiscoveryVariantRow = ({
  row = {},
  feature = "",
  featureKey = "",
  index = 0,
} = {}) => {
  const make = getRowBrand(row);
  const model = getRowModel(row);
  const variant = getRowVariantLabel(row);
  const normalizedFeatureKey = normalizeFeatureKey(featureKey || feature || row.feature || row.matchedFeature);
  const matrixFeature = normalizedFeatureKey ? row.featuresByKey?.[normalizedFeatureKey] : null;
  const exShowroomPrice = getDiscoveryRowPrice(row);
  const onRoadPrice = getDiscoveryRowOnRoadPrice(row);
  const displayName = firstText(
    row.displayName,
    row.fullModel,
    row.modelDisplayName,
    [make, model].filter(Boolean).join(" "),
    model,
  );

  return {
    id: String(row.id || row._id || row.raw?._id || slugify(`${make}-${model}-${variant}`, `feature-match-${index + 1}`)),
    make,
    brand: make,
    model,
    modelKey: firstText(row.modelKey, row.model_key, row.raw?.modelKey) || slugify(model, "model"),
    variantKey: firstText(row.variantKey, row.variant_key, row.raw?.variantKey) || normalizeFeatureKey(variant),
    displayName,
    fullModel: displayName,
    featureKey: normalizedFeatureKey || slugify(feature || row.feature || row.matchedFeature || "feature", "feature"),
    featureName: firstText(matrixFeature?.displayName, row.featureName, row.matchedFeature, row.feature, feature),
    matchedFeature: firstText(matrixFeature?.displayName, row.matchedFeature, row.feature, feature),
    featureAvailability: matrixFeature
      ? {
          available: matrixFeature.available === true,
          availabilityStatus: matrixFeature.availabilityStatus || "",
          value: matrixFeature.value || "",
        }
      : null,
    variant,
    variantName: variant,
    exShowroomPrice,
    onRoadPrice,
    price: exShowroomPrice || onRoadPrice || null,
    startsFromPrice: exShowroomPrice || onRoadPrice || null,
    startsFromPriceLabel:
      firstText(row.exShowroomPriceLabel, row.priceLabel, row.onRoadPriceLabel) ||
      formatMoney(exShowroomPrice || onRoadPrice),
    fuel: firstText(row.fuel, row.fuelType, row.raw?.fuel_type, getMatrixFuelType(row)),
    fuelType: firstText(row.fuelType, row.fuel, row.raw?.fuel_type, getMatrixFuelType(row)),
    transmission: firstText(row.transmission, row.transmissionType, row.raw?.transmission),
    rawRowId: String(row._id || row.raw?._id || ""),
    foundMatrixRows: matrixFeature ? 1 : 0,
    dataSource: row.dataSource || "vehicle_variant_feature_matrix_v2",
    sourceCollection: row.sourceCollection || "vehicle_variant_feature_matrix_v2",
  };
};

const buildFeatureDiscoveryModelGroups = ({
  rows = [],
  feature = "",
  featureKey = "",
  budgetMax = 0,
  source = {},
} = {}) => {
  const groups = new Map();

  rows.forEach((row, index) => {
    const variantRow = buildDiscoveryVariantRow({ row, feature, featureKey, index });
    const groupKey = compactKey(
      `${variantRow.make} ${variantRow.modelKey || variantRow.model}`,
    );

    if (!groupKey || !variantRow.model || !variantRow.variant) return;

    if (!groups.has(groupKey)) {
      groups.set(groupKey, {
        id: groupKey,
        make: variantRow.make,
        brand: variantRow.make,
        model: variantRow.model,
        modelKey: variantRow.modelKey,
        displayName: variantRow.displayName,
        fullModel: variantRow.fullModel,
        featureKey: variantRow.featureKey,
        featureName: variantRow.featureName,
        matchedFeature: variantRow.matchedFeature,
        dataSource: source.dataSource || variantRow.dataSource || "vehicle_variant_feature_matrix_v2",
        sourceCollection: source.dataSource || variantRow.sourceCollection || "vehicle_variant_feature_matrix_v2",
        foundMatrixRows: 0,
        qualifyingVariants: [],
      });
    }

    const group = groups.get(groupKey);
    group.foundMatrixRows += Number(variantRow.foundMatrixRows || 0);
    group.qualifyingVariants.push(variantRow);
  });

  return [...groups.values()]
    .map((group) => {
      const variants = sortVariantsByPrice(group.qualifyingVariants);
      const cheapest = variants[0] || {};
      const bestUnderBudget = [...variants]
        .filter((variant) => {
          const price = Number(variant.price || 0);
          return price > 0 && (!budgetMax || price <= budgetMax);
        })
        .sort((a, b) => Number(b.price || 0) - Number(a.price || 0))[0] || variants[variants.length - 1] || {};
      const prices = variants
        .map((variant) => Number(variant.price || 0))
        .filter((price) => Number.isFinite(price) && price > 0);
      const minPrice = prices.length ? Math.min(...prices) : null;
      const maxPrice = prices.length ? Math.max(...prices) : null;

      return {
        ...group,
        startsFromVariant: cheapest.variant || "",
        startsFromPrice: minPrice,
        startsFromPriceLabel: formatMoney(minPrice),
        bestUnderBudgetVariant: bestUnderBudget.variant || "",
        bestUnderBudgetPrice: Number(bestUnderBudget.price || 0) || null,
        bestUnderBudgetPriceLabel: formatMoney(bestUnderBudget.price),
        minPrice,
        maxPrice,
        priceRangeLabel:
          minPrice && maxPrice && minPrice !== maxPrice
            ? `${formatMoney(minPrice)} - ${formatMoney(maxPrice)}`
            : formatMoney(minPrice),
        priceRange:
          minPrice && maxPrice && minPrice !== maxPrice
            ? `${formatMoney(minPrice)} - ${formatMoney(maxPrice)}`
            : formatMoney(minPrice),
        qualifyingVariantCount: variants.length,
        qualifyingVariants: variants,
      };
    })
    .sort((a, b) => {
      const priceA = Number(a.startsFromPrice || Number.MAX_SAFE_INTEGER);
      const priceB = Number(b.startsFromPrice || Number.MAX_SAFE_INTEGER);
      if (priceA !== priceB) return priceA - priceB;
      return String(a.displayName || a.model).localeCompare(String(b.displayName || b.model));
    });
};

const dedupeVariants = (variants = []) => {
  const map = new Map();

  variants.forEach((variant) => {
    const key = compactKey(`${variant.brand} ${variant.model} ${variant.variant}`);
    if (!key) return;

    const existing = map.get(key);

    if (!existing || variant.featureCount >= existing.featureCount) {
      map.set(key, variant);
    }
  });

  return [...map.values()].sort((a, b) => {
    const priceA = a.price || Number.MAX_SAFE_INTEGER;
    const priceB = b.price || Number.MAX_SAFE_INTEGER;

    if (Number.isFinite(priceA) && Number.isFinite(priceB) && priceA !== priceB) {
      return priceA - priceB;
    }

    return a.variant.localeCompare(b.variant, undefined, {
      numeric: true,
      sensitivity: "base",
    });
  });
};

const getVariantPrice = (variant = {}) => {
  const price = Number(
    variant.exShowroomPrice ||
      variant.onRoadPrice ||
      variant.price ||
      0,
  );

  return Number.isFinite(price) && price > 0 ? price : Number.MAX_SAFE_INTEGER;
};

const formatMoney = (value) => {
  const number = Number(value || 0);

  if (!Number.isFinite(number) || number <= 0) return "";

  if (number >= 10000000) {
    const crore = number / 10000000;
    return `₹${crore.toFixed(crore >= 10 || Number.isInteger(crore) ? 0 : 2)}Cr`;
  }

  if (number >= 100000) {
    const lakh = number / 100000;
    return `₹${lakh.toFixed(lakh >= 10 || Number.isInteger(lakh) ? 0 : 2)}L`;
  }

  return `₹${Math.round(number).toLocaleString("en-IN")}`;
};

const isCurrentVariant = (variant = {}) => {
  const price = getVariantPrice(variant);

  return (
    variant.active !== false &&
    variant.discontinued !== true &&
    Number.isFinite(price) &&
    price !== Number.MAX_SAFE_INTEGER
  );
};

const sortVariantsByPrice = (variants = []) =>
  [...variants].sort((a, b) => {
    const priceA = getVariantPrice(a);
    const priceB = getVariantPrice(b);

    if (priceA !== priceB) return priceA - priceB;

    return String(a.variant || a.label || "").localeCompare(
      String(b.variant || b.label || ""),
      undefined,
      {
        numeric: true,
        sensitivity: "base",
      },
    );
  });

const pickMiddleVariant = (variants = []) => {
  const sorted = sortVariantsByPrice(variants);
  if (!sorted.length) return null;

  /*
    Default explorer behavior:
    open the middle current variant, not base and not top.
    This keeps the first view representative while the user can still select
    any other variant from the selector.
  */
  const index = Math.floor((sorted.length - 1) / 2);
  return sorted[index] || sorted[0];
};

const matchRequestedVariant = (variants = [], requestedVariant = "") => {
  const requested = normalizeKey(requestedVariant);
  if (!requested) return null;

  const exact = variants.find((variant) => normalizeKey(variant.variant) === requested);
  if (exact) return exact;

  const compactRequested = compactKey(requested);

  const compactExact = variants.find(
    (variant) => compactKey(variant.variant) === compactRequested,
  );
  if (compactExact) return compactExact;

  const startsWith = variants.find((variant) =>
    normalizeKey(variant.variant).startsWith(requested),
  );
  if (startsWith) return startsWith;

  return variants.find((variant) =>
    normalizeKey(variant.variant).includes(requested),
  ) || null;
};

const selectVariant = (variants = [], requestedVariant = "") => {
  if (!variants.length) return null;

  /*
    If user explicitly asks for a variant, respect that.
    Active/current status is still exposed on the selected variant so UI can
    show a notice later if needed.
  */
  const requestedMatch = matchRequestedVariant(variants, requestedVariant);
  if (requestedMatch) return requestedMatch;

  const currentVariants = variants.filter(isCurrentVariant);

  if (currentVariants.length) {
    return pickMiddleVariant(currentVariants);
  }

  return pickMiddleVariant(variants) || variants[0];
};

const getRequestedVariant = ({ response = {}, widget = {} } = {}) =>
  firstText(
    response.contextSnapshot?.anchorVariant,
    response.contextPatch?.anchorVariant,
    response.data?.variant,
    widget.variant,
  );

const getRequestedFeature = ({ response = {}, widget = {} } = {}) =>
  firstText(
    response.contextSnapshot?.feature,
    response.data?.feature,
    widget.feature,
    widget.matchedFeature,
  );

const getRequestedModel = ({ response = {}, widget = {}, selectedVariant = null } = {}) =>
  titleCase(
    firstText(
      response.contextSnapshot?.anchorModel,
      response.contextPatch?.anchorModel,
      response.data?.model,
      response.data?.filters?.model,
      widget.model,
      widget.data?.filters?.model,
      selectedVariant?.model,
    ),
  );

const getRequestedBrand = ({ response = {}, widget = {}, selectedVariant = null } = {}) =>
  titleCase(
    firstText(
      response.contextSnapshot?.anchorMake,
      response.contextPatch?.anchorMake,
      response.contextPatch?.anchorBrand,
      response.data?.brand,
      response.data?.make,
      response.data?.filters?.brand,
      response.data?.filters?.make,
      widget.brand,
      widget.make,
      widget.data?.filters?.brand,
      widget.data?.filters?.make,
      selectedVariant?.brand,
    ),
  );

const buildVehicle = ({ response = {}, widget = {}, selectedVariant = null } = {}) => {
  const brand = getRequestedBrand({ response, widget, selectedVariant });
  const model = getRequestedModel({ response, widget, selectedVariant });

  return {
    ...(response.contextPatch?.selectedVehicle || response.vehicle || response.data?.vehicle || widget.vehicle || {}),
    id: slugify(`${brand}-${model}`, "vehicle-features"),
    make: brand,
    brand,
    model,
    displayName: [brand, model].filter(Boolean).join(" ") || model || "Selected car",
    variant: selectedVariant?.variant || getRequestedVariant({ response, widget }) || "",
    selectedVariant: selectedVariant?.variant || getRequestedVariant({ response, widget }) || "",
    city:
      response.contextSnapshot?.anchorCity ||
      response.contextPatch?.anchorCity ||
      response.data?.city ||
      "new-delhi",
    citySlug:
      response.contextSnapshot?.anchorCity ||
      response.contextPatch?.anchorCity ||
      response.data?.city ||
      "new-delhi",
  };
};


const escapeRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const getDb = () => {
  if (mongoose.connection?.readyState !== 1 || !mongoose.connection?.db) {
    return null;
  }

  return mongoose.connection.db;
};

const getRequestMessage = ({ response = {}, widget = {} } = {}) =>
  firstText(
    response.message,
    response.query,
    response.userMessage,
    response.data?.message,
    response.meta?.message,
    widget.message,
    widget.data?.message,
  );

const getRequestedFeatureTerms = ({ response = {}, widget = {}, requestedFeature = "" } = {}) =>
  [
    requestedFeature,
    response.data?.feature,
    response.data?.filters?.feature,
    response.filters?.feature,
    widget.feature,
    widget.data?.feature,
    ...toArray(response.data?.filters?.mustHaveFeatures),
    ...toArray(response.filters?.mustHaveFeatures),
    ...toArray(response.data?.filters?.features),
    ...toArray(response.filters?.features),
  ]
    .map(normalizeFeatureKey)
    .filter(Boolean);

const getBudgetMax = ({ response = {}, widget = {} } = {}) => {
  const value = Number(
    response.data?.filters?.budgetMax ||
      widget.data?.filters?.budgetMax ||
      response.filters?.budgetMax ||
      0,
  );

  return Number.isFinite(value) && value > 0 ? value : 0;
};

const getRequestedFuelType = ({ response = {}, widget = {} } = {}) =>
  firstText(
    response.data?.filters?.fuelType,
    response.filters?.fuelType,
    widget.data?.filters?.fuelType,
    widget.fuelType,
  );

const getMatrixFuelType = (row = {}) =>
  firstText(
    row.fuelType,
    row.fuel,
    row.featuresByKey?.fuel_type?.value,
    ...toArray(row.fuels),
  );

const isAvailableMatrixFeature = (feature = {}) => {
  if (!feature || typeof feature !== "object") return false;
  if (feature.available === true) return true;

  const status = normalizeKey(feature.availabilityStatus || "");
  if (["available", "yes", "standard"].includes(status)) return true;

  const value = normalizeKey(feature.value || "");
  if (!value) return false;

  return !["not available", "no", "na", "n a", "n/a", "unavailable", "absent", "false"].includes(value);
};

const resolveRequestedFeatureFromMatrix = async ({
  response = {},
  widget = {},
  requestedFeature = "",
  requestedModel = "",
} = {}) => {
  const message = getRequestMessage({ response, widget });
  const termCandidates = getRequestedFeatureTerms({ response, widget, requestedFeature });

  if (message) {
    const parsed = await parseAciFeatureRequestFromMessage({
      message,
      modelEntity: {
        model: requestedModel,
        fullModel: requestedModel,
      },
    });

    const parsedFeature = toArray(parsed.requestedFeatures)[0];
    if (parsedFeature?.canonicalKey) {
      return {
        canonicalKey: parsedFeature.canonicalKey,
        displayName: parsedFeature.displayName || titleCase(parsedFeature.canonicalKey.replace(/_/g, " ")),
        parsed,
      };
    }
  }

  const canonicalKey = termCandidates[0] || normalizeFeatureKey(requestedFeature);
  if (!canonicalKey) return null;

  const db = getDb();
  const catalogDoc = db
    ? await db.collection("vehicle_feature_catalog_v2").findOne(
        { canonicalKey },
        { projection: { _id: 0, canonicalKey: 1, displayName: 1 } },
      )
    : null;

  return {
    canonicalKey,
    displayName: catalogDoc?.displayName || titleCase(canonicalKey.replace(/_/g, " ")),
    parsed: null,
  };
};

const fetchExactFeatureDiscoveryRows = async ({
  response = {},
  widget = {},
  brand = "",
  model = "",
  requestedFeature = "",
} = {}) => {
  const db = getDb();
  if (!db) return { rows: [], feature: null };

  const resolvedFeature = await resolveRequestedFeatureFromMatrix({
    response,
    widget,
    requestedFeature,
    requestedModel: model,
  });

  const featureKey = resolvedFeature?.canonicalKey || "";
  if (!featureKey) return { rows: [], feature: null };

  const budgetMax = getBudgetMax({ response, widget });
  const fuelType = getRequestedFuelType({ response, widget });
  const query = {
    [`featuresByKey.${featureKey}.available`]: true,
    $and: [
      {
        $or: [
          { activeForFeatureExplorer: { $exists: false } },
          { activeForFeatureExplorer: { $ne: false } },
        ],
      },
      {
        $or: [
          { activePricelistMatched: true },
          { priceMin: { $gt: 0 } },
          { priceMax: { $gt: 0 } },
        ],
      },
    ],
  };

  if (brand) {
    query.brand = new RegExp(`^${escapeRegex(brand)}$`, "i");
  }

  if (model) {
    query.model = new RegExp(`^${escapeRegex(model)}$`, "i");
  }

  if (budgetMax > 0) {
    query.priceMin = { $gt: 0, $lte: budgetMax };
  }

  const projection = {
    _id: 1,
    brand: 1,
    make: 1,
    model: 1,
    modelKey: 1,
    brandModelKey: 1,
    variant: 1,
    variantKey: 1,
    variantFull: 1,
    priceMin: 1,
    priceMax: 1,
    fuels: 1,
    activePricelistMatched: 1,
    activeForFeatureExplorer: 1,
    [`featuresByKey.${featureKey}`]: 1,
    "featuresByKey.fuel_type": 1,
  };

  const rows = await db
    .collection("vehicle_variant_feature_matrix_v2")
    .find(query, {
      projection,
      limit: Number(process.env.ACI_FEATURE_DISCOVERY_MATRIX_LIMIT || 1000),
    })
    .toArray();

  const fuelKey = normalizeKey(fuelType);
  const exactRows = rows
    .filter((row) => isAvailableMatrixFeature(row.featuresByKey?.[featureKey]))
    .filter((row) => {
      if (!fuelKey) return true;
      return normalizeKey(getMatrixFuelType(row)) === fuelKey;
    });

  return {
    rows: exactRows,
    feature: resolvedFeature,
    source: {
      dataSource: "vehicle_variant_feature_matrix_v2",
      featureKey,
      foundMatrixRows: exactRows.length,
      budgetMax,
      fuelType,
    },
  };
};

const getVariantPriceFromVehicleRow = (row = {}) => {
  const price = Number(
    row.ex_showroom ||
      row.exShowroomPrice ||
      row.ex_showroom_price_cardekho ||
      row.price ||
      0,
  );

  return Number.isFinite(price) && price > 0 ? price : null;
};

const getDiscoveryRowPrice = (row = {}) => {
  const price = Number(
    row.exShowroomPrice ||
      row.priceMin ||
      row.ex_showroom ||
      row.ex_showroom_price_cardekho ||
      row.price ||
      row.priceMax ||
      row.onRoadPrice ||
      row.on_road_price_cardekho ||
      row.total_on_road_with_accessories ||
      0,
  );

  return Number.isFinite(price) && price > 0 ? price : null;
};

const getDiscoveryRowOnRoadPrice = (row = {}) => {
  const price = Number(
    row.onRoadPrice ||
      row.on_road_price_cardekho ||
      row.total_on_road_with_accessories ||
      row.orp_without_accessories ||
      row.priceMax ||
      0,
  );

  return Number.isFinite(price) && price > 0 ? price : null;
};

const getOnRoadPriceFromVehicleRow = (row = {}) => {
  const price = Number(
    row.onRoadPrice ||
      row.on_road_price_cardekho ||
      row.total_on_road_with_accessories ||
      row.orp_without_accessories ||
      0,
  );

  return Number.isFinite(price) && price > 0 ? price : null;
};

const getVehicleRowVariantLabel = (row = {}, brand = "", model = "") => {
  const label = firstText(
    row.variant_short,
    row.variant_normalized,
    row.variantName,
    row.variant,
    row.name,
  );

  return stripVehicleName({
    value: label,
    brand: brand || row.brand || row.make || "",
    model: model || row.model_normalized || row.model || "",
  });
};

const getMongooseDbSafe = () => {
  if (!mongoose?.connection?.db) return null;
  if (mongoose.connection.readyState !== 1) return null;
  return mongoose.connection.db;
};

const fetchCurrentVehicleVariantMeta = async ({
  brand = "",
  model = "",
  city = "new-delhi",
} = {}) => {
  const db = getMongooseDbSafe();
  if (!db || !model) {
    return {
      hasVehicleStatus: false,
      variantMetaByKey: new Map(),
    };
  }

  const collection = db.collection("vehicles");
  const modelRegex = new RegExp(`(^|\\s)${escapeRegex(model)}$`, "i");
  const exactModelRegex = new RegExp(`^\\s*${escapeRegex(model)}\\s*$`, "i");
  const brandRegex = brand ? new RegExp(`^\\s*${escapeRegex(brand)}\\s*$`, "i") : null;

  const and = [
    {
      $or: [
        { model_normalized: exactModelRegex },
        { model: exactModelRegex },
        { model: modelRegex },
      ],
    },
    {
      $or: [
        { city },
        { city: normalizeKey(city) },
        { city: "new-delhi" },
        { city: { $exists: false } },
      ],
    },
  ];

  if (brandRegex) {
    and.push({
      $or: [
        { brand: brandRegex },
        { make: brandRegex },
        { brand_normalized: brandRegex },
      ],
    });
  }

  const rows = await collection
    .find(
      { $and: and },
      {
        projection: {
          _id: 1,
          brand: 1,
          make: 1,
          brand_normalized: 1,
          model: 1,
          model_normalized: 1,
          variant: 1,
          variant_short: 1,
          variant_normalized: 1,
          fuel_type: 1,
          transmission: 1,
          city: 1,
          ex_showroom: 1,
          exShowroomPrice: 1,
          ex_showroom_price_cardekho: 1,
          onRoadPrice: 1,
          on_road_price_cardekho: 1,
          total_on_road_with_accessories: 1,
          orp_without_accessories: 1,
          is_discontinued: 1,
          discontinued: 1,
          active: 1,
          is_active: 1,
          LastSeenDate: 1,
          updatedAt: 1,
        },
      },
    )
    .limit(250)
    .toArray();

  const variantMetaByKey = new Map();

  rows.forEach((row) => {
    const exShowroomPrice = getVariantPriceFromVehicleRow(row);
    const onRoadPrice = getOnRoadPriceFromVehicleRow(row);
    const discontinued =
      row.is_discontinued === true ||
      row.discontinued === true;

    const explicitlyInactive =
      row.active === false ||
      row.is_active === false;

    const explicitlyActive =
      row.active === true ||
      row.is_active === true;

    const current =
      !explicitlyInactive &&
      !discontinued &&
      Boolean(exShowroomPrice || onRoadPrice || explicitlyActive);

    if (!current) return;

    const variantLabel = getVehicleRowVariantLabel(row, brand, model);
    const key = compactKey(variantLabel);
    if (!key) return;

    const meta = {
      vehicleRowId: String(row._id || ""),
      variant: variantLabel,
      active: true,
      current: true,
      discontinued: false,
      exShowroomPrice,
      onRoadPrice,
      price: exShowroomPrice || onRoadPrice || null,
      city: row.city || city,
      fuel: row.fuel_type || row.fuel || "",
      transmission: row.transmission || "",
      lastSeenDate: row.LastSeenDate || "",
    };

    const existing = variantMetaByKey.get(key);
    if (!existing || Number(meta.price || 0) < Number(existing.price || Number.MAX_SAFE_INTEGER)) {
      variantMetaByKey.set(key, meta);
    }
  });

  return {
    hasVehicleStatus: variantMetaByKey.size > 0,
    variantMetaByKey,
  };
};

const enrichVariantsWithCurrentVehicleRows = async ({
  variants = [],
  brand = "",
  model = "",
  city = "new-delhi",
} = {}) => {
  const { hasVehicleStatus, variantMetaByKey } =
    await fetchCurrentVehicleVariantMeta({ brand, model, city });

  if (!hasVehicleStatus) {
    return {
      variants,
      hasVehicleStatus: false,
      activeVariantCount: variants.filter(isCurrentVariant).length,
    };
  }

  const enriched = variants.map((variant) => {
    const meta = variantMetaByKey.get(compactKey(variant.variant));

    if (!meta) {
      return {
        ...variant,
        active: false,
        current: false,
        currentPricelistMatched: false,
      };
    }

    return {
      ...variant,
      ...meta,
      active: true,
      current: true,
      currentPricelistMatched: true,
      exShowroomPrice: meta.exShowroomPrice || variant.exShowroomPrice,
      onRoadPrice: meta.onRoadPrice || variant.onRoadPrice,
      price: meta.price || variant.price,
    };
  });

  return {
    variants: enriched,
    hasVehicleStatus: true,
    activeVariantCount: enriched.filter((variant) => variant.active === true).length,
  };
};


export const buildFeatureExplorerPayload = async ({ response = {}, widget = {} } = {}) => {
  const sourceRows = getSourceRows({ response, widget });
  const rawVariants = dedupeVariants(sourceRows.map(buildVariantPayload)).filter(
    (variant) => variant.featureCount > 0,
  );

  if (!rawVariants.length) return null;

  const requestedVariant = getRequestedVariant({ response, widget });
  const city =
    response.contextSnapshot?.anchorCity ||
    response.contextPatch?.anchorCity ||
    response.data?.city ||
    "new-delhi";

  const requestedBrand = getRequestedBrand({
    response,
    widget,
    selectedVariant: rawVariants[0],
  });

  const requestedModel = getRequestedModel({
    response,
    widget,
    selectedVariant: rawVariants[0],
  });

  const enrichment = await enrichVariantsWithCurrentVehicleRows({
    variants: rawVariants,
    brand: requestedBrand,
    model: requestedModel,
    city,
  });

  const variants = enrichment.variants;
  const selectedVariant = selectVariant(variants, requestedVariant);

  const currentVariants = variants.filter(isCurrentVariant);
  const baseVariantOptions = sortVariantsByPrice(
    currentVariants.length ? currentVariants : variants,
  );

  const selectedIsInOptions = baseVariantOptions.some(
    (variant) => variant.id === selectedVariant?.id,
  );

  const variantOptions =
    selectedVariant && !selectedIsInOptions
      ? [selectedVariant, ...baseVariantOptions]
      : baseVariantOptions;

  const sortedAllVariants = sortVariantsByPrice(variants);

  const vehicle = buildVehicle({ response, widget, selectedVariant });

  const payload = {
    type: "vehicle_features",
    tool: "vehicle_features",
    intent: "vehicle_model_features_explorer",
    canvasType: "features_explorer_canvas",
    title: `${vehicle.displayName}${selectedVariant?.variant ? ` ${selectedVariant.variant}` : ""} features`,
    answer: `I found ${selectedVariant?.featureCount || 0} features for ${vehicle.displayName}${selectedVariant?.variant ? ` ${selectedVariant.variant}` : ""}.`,
    vehicle,
    selectedVariant: selectedVariant?.variant || "",
    selectedVariantId: selectedVariant?.id || "",
    variants: variantOptions,
    variantOptions,
    allVariants: sortedAllVariants,
    activeVariantCount: currentVariants.length,
    totalRawVariantCount: variants.length,
    selectedVariantIsActive: selectedVariant ? isCurrentVariant(selectedVariant) : false,
    activeStatusSource: enrichment.hasVehicleStatus ? "vehicles" : "feature_rows",
    currentPricelistMatched: selectedVariant?.currentPricelistMatched === true,
    featureGroups: selectedVariant?.featureGroups || [],
    features: selectedVariant?.features || [],
    featureList: selectedVariant?.features || [],
    rows: selectedVariant?.features || [],
    items: selectedVariant?.features || [],
    quickSpecs: selectedVariant?.quickSpecs || [],
    highlights: selectedVariant?.highlights || [],
    categoryStats: selectedVariant?.categoryStats || {},
    featureStats: selectedVariant?.categoryStats || {},
    searchableFeatures: selectedVariant?.features || [],
    totalVariantCount: variants.length,
    totalFeatureCount: selectedVariant?.featureCount || 0,
    availableFeatureCount: selectedVariant?.availableCount || 0,
    data: {
      vehicle,
      selectedVariant: selectedVariant?.variant || "",
      variants: variantOptions,
      variantOptions,
      allVariants: sortedAllVariants,
      activeVariantCount: currentVariants.length,
      totalRawVariantCount: variants.length,
      selectedVariantIsActive: selectedVariant ? isCurrentVariant(selectedVariant) : false,
      activeStatusSource: enrichment.hasVehicleStatus ? "vehicles" : "feature_rows",
      currentPricelistMatched: selectedVariant?.currentPricelistMatched === true,
      featureGroups: selectedVariant?.featureGroups || [],
      features: selectedVariant?.features || [],
      quickSpecs: selectedVariant?.quickSpecs || [],
      highlights: selectedVariant?.highlights || [],
      categoryStats: selectedVariant?.categoryStats || {},
    },
  };

  return payload;
};

const featureMatchesTerm = (feature = {}, term = "") => {
  const normalizedTerm = normalizeKey(term);
  if (!normalizedTerm || normalizedTerm === "features") return true;

  const text = normalizeKey(`${feature.section} ${feature.name} ${feature.value}`);

  if (text.includes(normalizedTerm)) return true;

  if (/airbags?/.test(normalizedTerm) && /airbags?/.test(text)) return true;
  if (/360/.test(normalizedTerm) && /camera/.test(text)) return true;
  if (/carplay/.test(normalizedTerm) && /apple carplay/.test(text)) return true;
  if (/android/.test(normalizedTerm) && /android auto/.test(text)) return true;

  return false;
};

export const buildFeatureDiscoveryPayload = async ({ response = {}, widget = {} } = {}) => {
  const sourceRows = getSourceRows({ response, widget });
  const rawVariants = dedupeVariants(sourceRows.map(buildVariantPayload)).filter(
    (variant) => variant.featureCount > 0,
  );

  const city =
    response.contextSnapshot?.anchorCity ||
    response.contextPatch?.anchorCity ||
    response.data?.city ||
    "new-delhi";

  const requestedBrand = getRequestedBrand({
    response,
    widget,
    selectedVariant: rawVariants[0],
  });

  const requestedModel = getRequestedModel({
    response,
    widget,
    selectedVariant: rawVariants[0],
  });
  const explicitRequestedModel = getRequestedModel({
    response,
    widget,
    selectedVariant: null,
  });

  const enrichment = await enrichVariantsWithCurrentVehicleRows({
    variants: rawVariants,
    brand: requestedBrand,
    model: requestedModel,
    city,
  });

  const allVariants = enrichment.hasVehicleStatus
    ? enrichment.variants.filter((variant) => variant.active === true)
    : enrichment.variants;

  const requestedFeature = getRequestedFeature({ response, widget });
  const exactMatrixResult = await fetchExactFeatureDiscoveryRows({
    response,
    widget,
    brand: requestedBrand,
    model: explicitRequestedModel,
    requestedFeature,
  });
  const exactMatrixRows = exactMatrixResult.rows || [];
  const exactFeatureKey = exactMatrixResult.feature?.canonicalKey || normalizeFeatureKey(requestedFeature);
  const exactFeatureName =
    exactMatrixResult.feature?.displayName ||
    titleCase((exactFeatureKey || requestedFeature || "feature").replace(/_/g, " "));

  const matchedRows = [];

  allVariants.forEach((variant) => {
    const matches = variant.features.filter((feature) =>
      featureMatchesTerm(feature, requestedFeature),
    );

    const availableMatches = matches.filter((feature) => feature.available);
    const best = availableMatches[0] || matches[0];

    if (!best || !best.available) return;

    matchedRows.push({
      id: `${variant.id}-${slugify(best.name)}`,
      variant: variant.variant,
      variantName: variant.variant,
      label: variant.variant,
      brand: variant.brand,
      make: variant.make,
      model: variant.model,
      feature: best.name,
      matchedFeature: best.name,
      section: best.section,
      value: best.value,
      displayValue: best.displayValue,
      available: true,
      present: true,
      included: true,
      exShowroomPrice: variant.exShowroomPrice,
      onRoadPrice: variant.onRoadPrice,
      price: variant.price,
      quickSpecs: variant.quickSpecs,
      highlights: variant.highlights,
      featureCount: variant.featureCount,
      availableCount: variant.availableCount,
      variantId: variant.id,
    });
  });

  const model =
    getRequestedModel({
      response,
      widget,
      selectedVariant: allVariants[0],
    }) || allVariants[0]?.model || "";

  const brand =
    getRequestedBrand({
      response,
      widget,
      selectedVariant: allVariants[0],
    }) || allVariants[0]?.brand || "";

  const vehicle = {
    ...(response.contextPatch?.selectedVehicle || widget.vehicle || {}),
    id: slugify(`${brand}-${model}`, "feature-discovery"),
    make: brand,
    brand,
    model,
    displayName: [brand, model].filter(Boolean).join(" ") || model || "Selected car",
    city:
      response.contextSnapshot?.anchorCity ||
      response.contextPatch?.anchorCity ||
      response.data?.city ||
      "new-delhi",
  };

  const runtimeMatchedCount = Math.max(
    Number(response.matched || 0),
    Number(response.data?.matched || 0),
    Number(response.meta?.matched || 0),
    Number(widget.matched || 0),
    Number(response.executor?.runtimeResultsMeta?.[0]?.matched || 0),
    Number(response.runtimeResultsMeta?.[0]?.matched || 0),
  );

  const hasExactMatrixTruth = Boolean(exactMatrixResult.feature);
  const effectiveMatchedRows = hasExactMatrixTruth
    ? exactMatrixRows
    : matchedRows.length > 0
      ? matchedRows
      : [];

  const effectiveMatchedCount =
    effectiveMatchedRows.length || (hasExactMatrixTruth ? 0 : runtimeMatchedCount || matchedRows.length);
  const fallbackRowsForGrouping = effectiveMatchedRows;
  const budgetMax = getBudgetMax({ response, widget });
  const modelGroups = buildFeatureDiscoveryModelGroups({
    rows: fallbackRowsForGrouping,
    feature: exactFeatureName,
    featureKey: exactFeatureKey,
    budgetMax,
    source: exactMatrixResult.source,
  });
  const responseRows = modelGroups.length ? modelGroups : effectiveMatchedRows;
  const budgetLabel =
    Number.isFinite(budgetMax) && budgetMax > 0
      ? ` under ${formatMoney(budgetMax)}`
      : "";
  const makeLabel = brand ? `${brand} ` : "";
  const featureLabel = exactFeatureName;
  const modelFirstAnswer = modelGroups.length
    ? `I found ${makeLabel}models with at least one ${featureLabel} variant${budgetLabel}. I’ll show where the feature starts and the best qualifying variant within your budget.`
    : effectiveMatchedCount
      ? `I found ${effectiveMatchedCount} qualifying variant${effectiveMatchedCount === 1 ? "" : "s"} with ${featureLabel}${budgetLabel}.`
      : `I could not find variants with ${requestedFeature}.`;

  return {
    type: "vehicle_feature_discovery",
    tool: "vehicle_feature_discovery",
    intent: "vehicle_feature_discovery",
    canvasType: response.canvasType || widget.canvasType || "feature_match_builder_canvas",
    title: `${requestedFeature || "Feature"} matches`,
    answer: modelFirstAnswer,
    vehicle,
    feature: exactFeatureName,
    featureKey: exactFeatureKey,
    matchedFeature: exactFeatureName,
    variants: hasExactMatrixTruth ? exactMatrixRows : allVariants,
    matchedVariants: effectiveMatchedRows,
    modelGroups,
    rows: responseRows,
    items: responseRows,
    features: effectiveMatchedRows,
    featureList: effectiveMatchedRows,
    totalVariantCount: hasExactMatrixTruth ? exactMatrixRows.length : allVariants.length,
    matchedVariantCount: effectiveMatchedCount,
    modelGroupCount: modelGroups.length,
    rowCount: responseRows.length,
    activeStatusSource: hasExactMatrixTruth
      ? "vehicle_variant_feature_matrix_v2"
      : enrichment.hasVehicleStatus ? "vehicles" : "feature_rows",
    activeVariantCount: hasExactMatrixTruth
      ? exactMatrixRows.length
      : allVariants.filter((variant) => variant.active === true || variant.current === true).length,
    totalRawVariantCount: rawVariants.length,
    currentPricelistMatched: hasExactMatrixTruth || enrichment.hasVehicleStatus,
    dataSource: exactMatrixResult.source?.dataSource || "vehicle_variant_feature_matrix_v2",
    sourceCollection: exactMatrixResult.source?.dataSource || "vehicle_variant_feature_matrix_v2",
    foundMatrixRows: exactMatrixRows.length,
    sourceTransparency: {
      responseTool: "vehicle_feature_discovery",
      modulesChecked: ["vehicle_feature_catalog_v2", "vehicle_variant_feature_matrix_v2"],
      dataSource: exactMatrixResult.source?.dataSource || "vehicle_variant_feature_matrix_v2",
      recordCount: effectiveMatchedCount,
      matched: effectiveMatchedCount,
      featureKey: exactFeatureKey,
      foundMatrixRows: exactMatrixRows.length,
    },
    data: {
      vehicle,
      feature: exactFeatureName,
      featureKey: exactFeatureKey,
      variants: hasExactMatrixTruth ? exactMatrixRows : allVariants,
      matchedVariants: effectiveMatchedRows,
      modelGroups,
      rows: responseRows,
      items: responseRows,
      rowCount: responseRows.length,
      modelGroupCount: modelGroups.length,
      matchedVariantCount: effectiveMatchedCount,
      activeStatusSource: hasExactMatrixTruth
        ? "vehicle_variant_feature_matrix_v2"
        : enrichment.hasVehicleStatus ? "vehicles" : "feature_rows",
      activeVariantCount: hasExactMatrixTruth
        ? exactMatrixRows.length
        : allVariants.filter((variant) => variant.active === true || variant.current === true).length,
      totalRawVariantCount: rawVariants.length,
      currentPricelistMatched: hasExactMatrixTruth || enrichment.hasVehicleStatus,
      dataSource: exactMatrixResult.source?.dataSource || "vehicle_variant_feature_matrix_v2",
      sourceCollection: exactMatrixResult.source?.dataSource || "vehicle_variant_feature_matrix_v2",
      foundMatrixRows: exactMatrixRows.length,
    },
  };
};

export default {
  buildFeatureExplorerPayload,
  buildFeatureDiscoveryPayload,
};
