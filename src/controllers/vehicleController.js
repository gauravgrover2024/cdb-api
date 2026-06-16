import asyncHandler from "express-async-handler";
import mongoose from "mongoose";
import Vehicle from "../models/Vehicle.js";
import VehicleRecord from "../models/VehicleRecord.js";
import VehicleFeature from "../models/VehicleFeature.js";
import {
  normalizeVehicleDatasetRow,
  vehicleNormalizationFields,
} from "../utils/vehicleDatasetNormalizer.js";
import {
  buildSearchTokenFilter,
  escapeSearchRegex,
} from "../utils/searchTokens.js";

const VEHICLE_LIST_PROJECTION = {
  make: 1,
  brand: 1,
  model: 1,
  variant: 1,
  fuel: 1,
  fuel_type: 1,
  city: 1,
  exShowroom: 1,
  ex_showroom: 1,
  rto: 1,
  insurance: 1,
  otherCharges: 1,
  other_totalOtherCharges: 1,
  other_totalOtherChargesInRsFormat: 1,
  other_tcsCharges: 1,
  other_otherCharges: 1,
  other_mcdCharges: 1,
  other_numberPlateCharges: 1,
  other_smartCardcharges: 1,
  other_list: 1,
  optional_totalAccessories: 1,
  optional_totalAccessoriesInRs: 1,
  optional_total: 1,
  optional_accessoriesCharges: 1,
  optional_extendedWarrantyCharges: 1,
  optional_zeroDepInsuranceCharges: 1,
  optional_amcCharges: 1,
  optional_miscellaneouscharges: 1,
  optional_list: 1,
  orp_without_accessories: 1,
  ex_showroom_price_cardekho: 1,
  insurance_amount_cardekho: 1,
  rto_amount_cardekho: 1,
  onRoadPrice: 1,
  on_road_price_cardekho: 1,
  total_on_road_with_accessories: 1,
  LastSeenDate: 1,
  LastPriceChangeDate: 1,
  IsDiscontinued: 1,
  status: 1,
  is_discontinued: 1,
  isDiscontinued: 1,
  discontinued_date: 1,
  discontinuedDate: 1,
  image_url: 1,
  imageUrl: 1,
  sourceImageUrl: 1,
  normalizedImageUrl: 1,
  cleanImageUrl: 1,
  normalized_image_url: 1,
  clean_image_url: 1,
  normalizedImagePngUrl: 1,
  color_name: 1,
  color_hex: 1,
  hex: 1,
  brand_normalized: 1,
  model_normalized: 1,
  variant_normalized: 1,
  search_text: 1,
  colors_normalized: 1,
  createdAt: 1,
  updatedAt: 1,
};

const parseAmount = (value) => {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  if (typeof value === "string") {
    const cleaned = value.replace(/[^0-9.-]/g, "");
    if (!cleaned) return 0;
    const parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
};

const firstPositiveAmount = (...values) => {
  for (const value of values) {
    const amount = parseAmount(value);
    if (amount > 0) return amount;
  }
  return 0;
};

const numbersNearlyEqual = (a, b) =>
  Math.abs(parseAmount(a) - parseAmount(b)) <= 1;

const normalizeVehiclePricing = (raw = {}) => {
  const tcs = firstPositiveAmount(raw.tcs, raw.other_tcsCharges);
  const optionalTotal = firstPositiveAmount(
    raw.optional_total,
    raw.optional_totalAccessories,
    raw.optional_accessoriesCharges,
  );
  const orpWithoutAccessories = firstPositiveAmount(
    raw.orp_without_accessories,
  );
  const totalOnRoad = firstPositiveAmount(
    raw.total_on_road_with_accessories,
    raw.on_road_price_cardekho,
    raw.onRoadPrice,
    orpWithoutAccessories && optionalTotal
      ? orpWithoutAccessories + optionalTotal
      : 0,
  );

  const explicitOtherCharges = firstPositiveAmount(
    raw.handlingCharges,
    raw.other_otherCharges,
    raw.other_handlingCharges,
  );
  const rawOtherTotal = firstPositiveAmount(
    raw.other_totalOtherCharges,
    raw.otherCharges,
  );
  const nonTcsOtherCharges =
    explicitOtherCharges ||
    (rawOtherTotal && tcs
      ? rawOtherTotal > tcs && !numbersNearlyEqual(rawOtherTotal, tcs)
        ? Math.max(rawOtherTotal - tcs, 0)
        : numbersNearlyEqual(rawOtherTotal, tcs)
          ? 0
          : rawOtherTotal
      : rawOtherTotal || 0);

  const onRoadWithoutAccessories =
    orpWithoutAccessories ||
    (totalOnRoad && optionalTotal && totalOnRoad >= optionalTotal
      ? totalOnRoad - optionalTotal
      : totalOnRoad);

  return {
    tcs,
    optionalTotal,
    otherCharges: nonTcsOtherCharges,
    orp_without_accessories: onRoadWithoutAccessories,
    onRoadPrice: totalOnRoad,
    on_road_price_cardekho: totalOnRoad,
    total_on_road_with_accessories: totalOnRoad,
  };
};

const toVehicleListItem = (doc) => {
  const normalized = normalizeVehicleRecord(doc);
  const { rawVariant, rawModel, ...normalizedWithoutRaw } = normalized;
  const discontinued = isVehicleDiscontinued(normalized);
  const pricing = normalizeVehiclePricing(normalized);
  const rto = parseAmount(normalized.rto ?? normalized.roadTax ?? 0);

  return {
    ...normalizedWithoutRaw,
    _id: normalized._id,
    make: normalized.make,
    brand: normalized.brand,
    model: normalized.model,
    variant: normalized.variant,
    city: normalized.city,
    fuel: normalized.fuel,
    fuel_type: normalized.fuel_type,
    exShowroom: normalized.exShowroom,
    ex_showroom: parseAmount(
      normalized.ex_showroom ?? normalized.exShowroom ?? 0,
    ),
    rto,
    roadTax: rto,
    insurance: normalized.insurance,
    otherCharges: pricing.otherCharges,
    other_totalOtherCharges: pricing.otherCharges,
    tcs: pricing.tcs,
    other_tcsCharges: pricing.tcs,
    optional_total: pricing.optionalTotal,
    optional_totalAccessories: pricing.optionalTotal,
    orp_without_accessories: pricing.orp_without_accessories,
    onRoadPrice: pricing.onRoadPrice,
    on_road_price_cardekho: pricing.on_road_price_cardekho,
    total_on_road_with_accessories: pricing.total_on_road_with_accessories,
    status: normalized.status,
    is_discontinued: discontinued,
    isDiscontinued: discontinued,
    discontinued_date: normalized.discontinued_date ?? null,
    discontinuedDate: normalized.discontinuedDate ?? null,
    sourceImageUrl:
      normalized.sourceImageUrl ||
      normalized.image_url ||
      normalized.imageUrl ||
      "",
    normalizedImageUrl: normalized.normalizedImageUrl || "",
    cleanImageUrl:
      normalized.cleanImageUrl || normalized.normalizedImageUrl || "",
    image_url:
      normalized.image_url ||
      normalized.sourceImageUrl ||
      normalized.imageUrl ||
      "",
    imageUrl:
      normalized.imageUrl ||
      normalized.normalizedImageUrl ||
      normalized.cleanImageUrl ||
      normalized.image_url ||
      "",
    color_name: normalized.color_name || "",
    color_hex: normalized.color_hex || normalized.hex || "",
    hex: normalized.hex || normalized.color_hex || "",
    createdAt: normalized.createdAt,
    updatedAt: normalized.updatedAt,
  };
};

const normalizeText = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const canonicalizeMake = (value) => {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[-_]+/g, " ")
    .replace(/\s+/g, " ");
  const aliases = {
    mercedes: "mercedes benz",
    "mercedes benz": "mercedes benz",
    benz: "mercedes benz",
    maruti: "maruti suzuki",
    "maruti suzuki": "maruti suzuki",
  };
  return aliases[normalized] || normalized;
};

const escapeRegex = (value) =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const normalizeRegNo = (value) =>
  String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

const trimLeading = (value, prefix) => {
  const source = String(value || "").trim();
  const leader = String(prefix || "").trim();
  if (!source || !leader) return source;
  const escaped = escapeRegex(leader);
  return source.replace(new RegExp(`^${escaped}\\s*`, "i"), "").trim();
};

const toCityToken = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-");

const buildCityCandidates = (city) => {
  const token = toCityToken(city);
  if (!token) return [];

  const aliases = {
    delhi: ["new-delhi"],
    "new-delhi": ["delhi"],
    gurugram: ["gurgaon"],
    gurgaon: ["gurugram"],
  };

  return [...new Set([token, ...(aliases[token] || [])])];
};

const slugTokens = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .split("-")
    .filter(Boolean);

const mediaUrlMatchesMakeModel = (url, make, model) => {
  const raw = String(url || "")
    .trim()
    .toLowerCase();
  if (!raw) return false;

  const makeParts = slugTokens(make);
  const modelParts = slugTokens(model);
  if (!makeParts.length || !modelParts.length) return false;

  const normalized = raw.replace(/[^a-z0-9]+/g, "-");
  const hasMake = makeParts.some(
    (part) =>
      normalized.includes(`-${part}-`) ||
      normalized.endsWith(`-${part}`) ||
      normalized.startsWith(`${part}-`),
  );
  const hasModel = modelParts.some(
    (part) =>
      normalized.includes(`-${part}-`) ||
      normalized.endsWith(`-${part}`) ||
      normalized.startsWith(`${part}-`),
  );

  return hasMake && hasModel;
};

const normalizeHex = (value) =>
  String(value || "")
    .trim()
    .replace(/^#/, "")
    .toLowerCase();

const parseTimestampValue = (value) => {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime() || 0;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
};

const rowLatestTimestamp = (row) =>
  Math.max(
    parseTimestampValue(row?.scrape_timestamp),
    parseTimestampValue(row?.updatedAt),
    parseTimestampValue(row?.last_updated),
  );

const dedupeMediaRowsByHexLatest = (rows = []) => {
  const byHex = new Map();
  const withoutHex = [];

  rows.forEach((row) => {
    const hex = normalizeHex(
      row?.hex || row?.color_hex || row?.colour_hex || "",
    );
    if (!hex) {
      withoutHex.push(row);
      return;
    }

    const existing = byHex.get(hex);
    if (!existing) {
      byHex.set(hex, row);
      return;
    }

    const existingTs = rowLatestTimestamp(existing);
    const candidateTs = rowLatestTimestamp(row);
    if (candidateTs >= existingTs) {
      byHex.set(hex, row);
    }
  });

  return [...withoutHex, ...byHex.values()].sort((a, b) =>
    String(a?.color_name || "").localeCompare(String(b?.color_name || "")),
  );
};

const resolveDisplayImageUrl = (row = {}) =>
  String(
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
      row.defaultColorImageUrl ||
      row.imageUrl ||
      row.image_url ||
      row.sourceImageUrl ||
      row.car_image_url ||
      "",
  ).trim();

const normalizeImageFrameMeta = (frame = {}) => {
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
  const canvasWidth = readNumber(
    frame.canvas_width,
    frame.canvasWidth,
    frame.naturalWidth,
    frame.imageWidth,
    frame.sourceWidth,
  );
  const canvasHeight = readNumber(
    frame.canvas_height,
    frame.canvasHeight,
    frame.naturalHeight,
    frame.imageHeight,
    frame.sourceHeight,
  );

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
    Math.max(
      1,
      Math.max(
        0.86 / Math.max(widthRatio, 0.01),
        0.58 / Math.max(heightRatio, 0.01),
      ),
    ),
  );
  const translateX = (0.5 - centerX) * 100;
  const translateY = (0.5 - centerY) * 100;

  return {
    ...frame,
    naturalWidth: canvasWidth,
    naturalHeight: canvasHeight,
    bounds: { x, y, width, height },
    cssVars: {
      ...(frame.cssVars || {}),
      "--car-frame-scale": Number(scale.toFixed(3)),
      "--car-frame-x": `${Number(translateX.toFixed(2))}%`,
      "--car-frame-y": `${Number(translateY.toFixed(2))}%`,
      "--car-frame-origin": "center center",
    },
  };
};

const firstMeaningfulFrame = (...frames) =>
  frames.find(
    (frame) => frame && typeof frame === "object" && Object.keys(frame).length,
  ) || null;

const flattenVehicleColorDocuments = (docs = []) =>
  docs.flatMap((doc = {}) => {
    const make = doc.make || doc.brand || doc.brandName || "";
    const model = doc.model || doc.modelName || doc.model_name || "";
    const topFrame = normalizeImageFrameMeta(
      doc.heroFrameMeta ||
        doc.displayFrameMeta ||
        doc.defaultFrameMeta ||
        doc.imageFrame ||
        doc.frameMeta ||
        doc.image_frame ||
        doc.carImageFrame ||
        doc.car_image_frame ||
        doc.frame ||
        null,
    );
    const topImage =
      doc.heroImageNormalizedUrl ||
      doc.normalizedHeroImageUrl ||
      doc.heroNormalizedImageUrl ||
      doc.heroImageUrl ||
      doc.heroImage ||
      doc.displayNormalizedImageUrl ||
      doc.defaultNormalizedImageUrl ||
      doc.displayStagedImageUrl ||
      doc.normalizedImageUrl ||
      doc.cleanImageUrl ||
      doc.imageUrl ||
      doc.image_url ||
      "";

    const rows = [];
    if (topImage) {
      rows.push({
        ...doc,
        make,
        brand: doc.brand || make,
        model,
        color_name:
          doc.defaultColorName || doc.color_name || doc.colorName || "Display",
        colorName:
          doc.defaultColorName || doc.colorName || doc.color_name || "Display",
        normalizedImageUrl: topImage,
        cleanImageUrl: topImage,
        imageUrl: topImage,
        imageFrame: topFrame,
        sourceImageUrl:
          doc.displayImageUrl ||
          doc.defaultColorImageUrl ||
          doc.sourceImageUrl ||
          "",
        source: doc.source || VEHICLE_COLORS_COLLECTION,
      });
    }

    (Array.isArray(doc.colors) ? doc.colors : []).forEach((color, index) => {
      const colorImage =
        color.normalizedImageUrl ||
        color.stagedImageUrl ||
        color.normalizedImagePngUrl ||
        color.cleanImageUrl ||
        color.imageUrl ||
        color.sourceImageUrl ||
        "";
      if (!colorImage) return;

      rows.push({
        ...color,
        _id: `${doc._id || `${make}-${model}`}:${index}`,
        make,
        brand: doc.brand || make,
        model,
        color_name:
          color.name ||
          color.color_name ||
          color.colorName ||
          `Color ${index + 1}`,
        colorName:
          color.name ||
          color.colorName ||
          color.color_name ||
          `Color ${index + 1}`,
        color_hex: color.hex || color.color_hex || color.colorHex || "",
        hex: color.hex || color.color_hex || color.colorHex || "",
        normalizedImageUrl: colorImage,
        cleanImageUrl: colorImage,
        imageUrl: colorImage,
        sourceImageUrl: color.sourceImageUrl || "",
        imageFrame: normalizeImageFrameMeta(
          firstMeaningfulFrame(color.imageFrame, color.frameMeta, topFrame),
        ),
        updatedAt: color.updatedAt || doc.updatedAt,
        source: doc.source || VEHICLE_COLORS_COLLECTION,
      });
    });

    return rows.length ? rows : [doc];
  });

const normalizeVehicleRecord = (doc) => {
  const raw = doc?.toObject ? doc.toObject() : { ...(doc || {}) };
  const pricing = normalizeVehiclePricing(raw);
  const make = String(raw.make || raw.brand || "").trim();
  const rawModel = String(raw.model || "").trim();
  const rawVariant = String(raw.variant || "").trim();
  const model = trimLeading(rawModel, make) || rawModel;
  const variant =
    trimLeading(rawVariant, `${make} ${rawModel}`.trim()) ||
    trimLeading(rawVariant, rawModel) ||
    trimLeading(rawVariant, `${make} ${model}`.trim()) ||
    trimLeading(rawVariant, make) ||
    rawVariant;
  const sourceImageUrl = String(
    raw.sourceImageUrl ||
      raw.image_url ||
      raw.imageUrl ||
      raw.car_image_url ||
      "",
  ).trim();
  const normalizedImageUrl = String(
    raw.normalizedImageUrl ||
      raw.cleanImageUrl ||
      raw.normalized_image_url ||
      raw.clean_image_url ||
      raw.normalizedImagePngUrl ||
      "",
  ).trim();
  const imageUrl = resolveDisplayImageUrl({
    normalizedImageUrl,
    imageUrl: sourceImageUrl,
  });

  return {
    ...raw,
    make,
    brand: String(raw.brand || make).trim(),
    model,
    rawModel,
    variant,
    rawVariant,
    fuel: raw.fuel || raw.fuel_type || "",
    fuel_type: raw.fuel_type || raw.fuel || "",
    exShowroom: parseAmount(raw.exShowroom ?? raw.ex_showroom ?? 0),
    onRoadPrice: pricing.onRoadPrice,
    on_road_price_cardekho: pricing.on_road_price_cardekho,
    total_on_road_with_accessories: pricing.total_on_road_with_accessories,
    orp_without_accessories: pricing.orp_without_accessories,
    insurance: parseAmount(raw.insurance ?? 0),
    rto: parseAmount(raw.rto ?? raw.rto_amount_cardekho ?? 0),
    tcs: pricing.tcs,
    other_tcsCharges: pricing.tcs,
    otherCharges: pricing.otherCharges,
    other_totalOtherCharges: pricing.otherCharges,
    optional_total: pricing.optionalTotal,
    optional_totalAccessories: pricing.optionalTotal,
    sourceImageUrl,
    normalizedImageUrl,
    cleanImageUrl: normalizedImageUrl,
    imageUrl,
    image_url: sourceImageUrl,
    ...vehicleNormalizationFields({
      ...raw,
      brand: raw.brand || make,
      make,
      model: rawModel,
      variant: rawVariant,
    }),
  };
};

const withCanonicalVehiclePricing = (payload = {}) => {
  const pricing = normalizeVehiclePricing(payload);
  const next = { ...payload };

  if (pricing.onRoadPrice > 0) {
    next.onRoadPrice = pricing.onRoadPrice;
    next.on_road_price_cardekho = pricing.on_road_price_cardekho;
    next.total_on_road_with_accessories =
      pricing.total_on_road_with_accessories;
    next.orp_without_accessories = pricing.orp_without_accessories;
  }

  if (
    payload.tcs !== undefined ||
    payload.other_tcsCharges !== undefined ||
    pricing.tcs > 0
  ) {
    next.tcs = pricing.tcs;
    next.other_tcsCharges = pricing.tcs;
  }

  if (
    payload.optional_total !== undefined ||
    payload.optional_totalAccessories !== undefined ||
    payload.optional_accessoriesCharges !== undefined ||
    pricing.optionalTotal > 0
  ) {
    next.optional_total = pricing.optionalTotal;
    next.optional_totalAccessories = pricing.optionalTotal;
  }

  if (
    payload.otherCharges !== undefined ||
    payload.other_totalOtherCharges !== undefined ||
    payload.other_tcsCharges !== undefined ||
    payload.tcs !== undefined
  ) {
    next.otherCharges = pricing.otherCharges;
    next.other_totalOtherCharges = pricing.otherCharges;
  }

  return {
    ...next,
    ...vehicleNormalizationFields(next),
  };
};

const matchesExact = (actual, expected) => {
  if (!expected) return true;
  return (
    canonicalizeMake(actual) === canonicalizeMake(expected) ||
    normalizeText(actual).replace(/[-_]+/g, " ").replace(/\s+/g, " ") ===
      normalizeText(expected).replace(/[-_]+/g, " ").replace(/\s+/g, " ")
  );
};

const matchesVehicleFilters = (vehicle, filters = {}) => {
  const normalized = normalizeVehicleRecord(vehicle);
  if (!matchesExact(normalized.make, filters.make)) return false;
  if (!matchesExact(normalized.model, filters.model)) return false;
  if (!matchesExact(normalized.variant, filters.variant)) return false;
  if (!matchesExact(normalized.city, filters.city)) return false;
  if (!matchesExact(normalized.fuel, filters.fuel)) return false;
  return true;
};

const buildMakeMatch = (make) => {
  const value = String(make || "").trim();
  const normalized = canonicalizeMake(value);
  const candidates = [
    ...new Set(
      [
        value,
        normalized,
        normalized.replace(/ /g, "-"),
        normalized.replace(/ /g, ""),
      ].filter(Boolean),
    ),
  ];
  return {
    $or: [
      { make: { $in: candidates } },
      { brand: { $in: candidates } },
      {
        brand_normalized: new RegExp(
          `^${escapeRegex(value || normalized)}$`,
          "i",
        ),
      },
    ],
  };
};

const buildModelCandidates = (make, model) => {
  const makeValue = String(make || "").trim();
  const modelValue = String(model || "").trim();
  return [
    ...new Set(
      [modelValue, `${makeValue} ${modelValue}`.trim()].filter(Boolean),
    ),
  ];
};

const buildExactMakeCandidates = (make) => {
  const value = String(make || "").trim();
  const canonical = canonicalizeMake(value);
  const titleCanonical = canonical
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");

  return [
    ...new Set(
      [
        value,
        value.toUpperCase(),
        titleCanonical,
        titleCanonical.toUpperCase(),
        canonical,
        canonical.toUpperCase(),
        value.replace(/-/g, " "),
        value.replace(/\s+/g, "-"),
      ].filter(Boolean),
    ),
  ];
};

const buildVariantCandidates = (make, model, variant) => {
  const makeValue = String(make || "").trim();
  const modelValue = String(model || "").trim();
  const variantValue = String(variant || "").trim();
  return [
    ...new Set(
      [
        variantValue,
        `${makeValue} ${variantValue}`.trim(),
        `${modelValue} ${variantValue}`.trim(),
        `${makeValue} ${modelValue} ${variantValue}`.trim(),
      ].filter(Boolean),
    ),
  ];
};

const mergeAndCondition = (query, condition) => {
  if (
    !condition ||
    typeof condition !== "object" ||
    !Object.keys(condition).length
  )
    return;
  query.$and = [...(query.$and || []), condition];
};

const buildVehicleQuery = ({
  q,
  make,
  model,
  variant,
  city,
  fuel,
  useSearchTokens = true,
}) => {
  const query = {};

  if (q) {
    const tokenFilter = useSearchTokens ? buildSearchTokenFilter(q) : null;
    const escapedQ = escapeSearchRegex(q);
    mergeAndCondition(
      query,
      tokenFilter || {
        $or: [
          { make: new RegExp(escapedQ, "i") },
          { brand: new RegExp(escapedQ, "i") },
          { model: new RegExp(escapedQ, "i") },
          { variant: new RegExp(escapedQ, "i") },
          { search_text: new RegExp(escapedQ, "i") },
        ],
      },
    );
  }

  if (make) mergeAndCondition(query, buildMakeMatch(make));
  if (model) {
    const normalizedModel = normalizeVehicleDatasetRow({
      brand: make,
      make,
      model,
    }).model_normalized;
    mergeAndCondition(query, {
      $or: [
        { model: { $in: buildModelCandidates(make, model) } },
        {
          model_normalized: new RegExp(
            `^${escapeRegex(normalizedModel || model)}$`,
            "i",
          ),
        },
      ],
    });
  }
  if (variant) {
    const normalizedVariant = normalizeVehicleDatasetRow({
      brand: make,
      make,
      model,
      variant,
    }).variant_normalized;
    mergeAndCondition(query, {
      $or: [
        { variant: { $in: buildVariantCandidates(make, model, variant) } },
        {
          variant_normalized: new RegExp(
            `^${escapeRegex(normalizedVariant || variant)}$`,
            "i",
          ),
        },
      ],
    });
  }

  if (city) {
    const cityCandidates = buildCityCandidates(city);
    if (cityCandidates.length === 1) query.city = cityCandidates[0];
    else if (cityCandidates.length > 1) query.city = { $in: cityCandidates };
  }

  if (fuel) {
    const fuelRegex = new RegExp(`^${escapeRegex(String(fuel).trim())}$`, "i");
    mergeAndCondition(query, {
      $or: [{ fuel: fuelRegex }, { fuel_type: fuelRegex }],
    });
  }

  return query;
};

const buildMakeRegex = (make) =>
  new RegExp(`^${escapeRegex(String(make || "").trim())}$`, "i");

const parseBoolean = (value) => {
  const raw = String(value ?? "")
    .trim()
    .toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes";
};

const hasDiscontinuedDate = (value) => {
  if (value === undefined || value === null) return false;
  const raw = String(value).trim();
  if (!raw) return false;
  return raw.toLowerCase() !== "null";
};

const isVehicleDiscontinued = (vehicle) =>
  parseBoolean(vehicle?.is_discontinued ?? vehicle?.isDiscontinued) ||
  hasDiscontinuedDate(vehicle?.discontinued_date ?? vehicle?.discontinuedDate);

const ACTIVE_VARIANT_FILTER = {
  $and: [
    {
      // Keep this branch cast-safe for the schema-typed boolean field.
      $or: [
        { is_discontinued: { $exists: false } },
        { is_discontinued: false },
        { is_discontinued: 0 },
        { is_discontinued: null },
      ],
    },
    {
      $nor: [
        { isDiscontinued: true },
        { isDiscontinued: 1 },
        { isDiscontinued: "true" },
        { isDiscontinued: "True" },
        {
          discontinued_date: {
            $exists: true,
            $nin: [null, "", "null", "NULL"],
          },
        },
        {
          discontinuedDate: { $exists: true, $nin: [null, "", "null", "NULL"] },
        },
      ],
    },
  ],
};

const DISTINCT_CACHE_TTL_MS = 5 * 60 * 1000;
const DISTINCT_CACHE = new Map();

const VEHICLE_LIST_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes cache
const VEHICLE_LIST_CACHE = new Map();

const getCacheKey = (prefix, params = {}) =>
  JSON.stringify({
    prefix,
    ...params,
  });

const readCache = (cacheMap, ttl, prefix, params = {}) => {
  const key = getCacheKey(prefix, params);
  const entry = cacheMap.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > ttl) {
    cacheMap.delete(key);
    return null;
  }
  return entry.data;
};

const writeCache = (cacheMap, prefix, params = {}, data = []) => {
  const key = getCacheKey(prefix, params);
  cacheMap.set(key, { ts: Date.now(), data });
};

const readDistinctCache = (scope, params = {}) => {
  return readCache(DISTINCT_CACHE, DISTINCT_CACHE_TTL_MS, scope, params);
};

const writeDistinctCache = (scope, params = {}, data = []) => {
  writeCache(DISTINCT_CACHE, scope, params, data);
};

const SIMILAR_BASE_CACHE_TTL_MS = 5 * 60 * 1000;
const FEATURE_META_CACHE_TTL_MS = 10 * 60 * 1000;
const SIMILAR_BASE_CACHE = new Map();
let FEATURE_META_CACHE = { ts: 0, data: new Map() };
const VEHICLE_MEDIA_CACHE_TTL_MS = 10 * 60 * 1000;
const VEHICLE_MEDIA_CACHE = new Map();
const POPULAR_CARS_CACHE_TTL_MS = 10 * 60 * 1000;
const POPULAR_CARS_CACHE = new Map();
const POPULAR_CARS_IN_FLIGHT = new Map();
const VEHICLE_COLORS_COLLECTION = "vehicle_colors_v2";
const R2_PUBLIC_IMAGE_PREFIX =
  "https://pub-8504a10fc1c04f02ac8760cb90462ae3.r2.dev/";

const normalizeLooseToken = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");

const normalizeModelNameForKey = (make, model) =>
  normalizeLooseToken(trimLeading(model, make) || model);

const buildMakeModelJoinKey = (make, model) => {
  const mk = normalizeLooseToken(make);
  const mdl = normalizeModelNameForKey(make, model);
  if (!mk || !mdl) return "";
  return `${mk}|${mdl}`;
};

const extractFeatureValueByKeywords = (featuresObj = {}, keywords = []) => {
  if (!featuresObj || typeof featuresObj !== "object") return "";
  const needles = (keywords || [])
    .map((value) => normalizeText(value))
    .filter(Boolean);
  if (!needles.length) return "";

  for (const [key, value] of Object.entries(featuresObj)) {
    const hay = normalizeText(key);
    if (!hay) continue;
    if (!needles.some((needle) => hay.includes(needle))) continue;
    const raw = String(value ?? "").trim();
    if (!raw) continue;
    const normalized = raw.toLowerCase();
    if (
      ["not available", "na", "n/a", "-", "null", "undefined"].includes(
        normalized,
      )
    ) {
      continue;
    }
    return raw;
  }
  return "";
};

const parseSeatCount = (value) => {
  if (value == null) return null;
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return Math.round(value);
  }
  const raw = String(value).trim();
  if (!raw) return null;
  const match = raw.match(/(\d{1,2})/);
  if (!match) return null;
  const parsed = Number(match[1]);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const normalizeBodyTypeBucket = (value) => {
  const text = normalizeText(value);
  if (!text) return "";
  if (
    text.includes("suv") ||
    text.includes("crossover") ||
    text.includes("sport utility")
  ) {
    return "suv";
  }
  if (text.includes("sedan")) return "sedan";
  if (text.includes("hatch")) return "hatchback";
  if (
    text.includes("muv") ||
    text.includes("mpv") ||
    text.includes("people mover")
  )
    return "mpv";
  if (text.includes("coupe")) return "coupe";
  if (text.includes("convertible") || text.includes("cabriolet"))
    return "convertible";
  if (text.includes("pickup")) return "pickup";
  return text;
};

const formatBodyType = (value) => {
  const text = String(value || "")
    .trim()
    .replace(/\s+/g, " ");
  if (!text) return "";
  return text
    .split(" ")
    .map((part) =>
      part ? `${part[0].toUpperCase()}${part.slice(1).toLowerCase()}` : "",
    )
    .join(" ");
};

const buildBaseModelRowsSnapshot = async ({
  city = "",
  includeDiscontinued = false,
} = {}) => {
  const query = buildVehicleQuery({ city });
  if (!includeDiscontinued) mergeAndCondition(query, ACTIVE_VARIANT_FILTER);

  const pipeline = [
    { $match: query },
    {
      $project: {
        _id: 1,
        make: { $ifNull: ["$make", "$brand"] },
        model: "$model",
        variant: "$variant",
        city: "$city",
        basePrice: {
          $ifNull: ["$ex_showroom", "$exShowroom"],
        },
      },
    },
    { $match: { basePrice: { $gt: 0 } } },
    { $sort: { basePrice: 1 } },
    {
      $group: {
        _id: { make: "$make", model: "$model" },
        make: { $first: "$make" },
        model: { $first: "$model" },
        city: { $first: "$city" },
        basePrice: { $first: "$basePrice" },
        variant: { $first: "$variant" },
        vehicleId: { $first: "$_id" },
      },
    },
  ];

  const rows = await Vehicle.aggregate(pipeline);
  return rows
    .map((row) => {
      const make = String(row?.make || "").trim();
      const modelRaw = String(row?.model || "").trim();
      const model = trimLeading(modelRaw, make) || modelRaw;
      const key = buildMakeModelJoinKey(make, model);
      if (!make || !model || !key) return null;
      return {
        key,
        make,
        model,
        city: row?.city || "",
        basePrice: Number(row?.basePrice || 0),
        variant: String(row?.variant || "").trim(),
        vehicleId: String(row?.vehicleId || ""),
      };
    })
    .filter((row) => row && row.basePrice > 0);
};

const getBaseModelRowsCached = async ({
  city = "",
  includeDiscontinued = false,
} = {}) => {
  const key = JSON.stringify({
    city: toCityToken(city || ""),
    includeDiscontinued: Boolean(includeDiscontinued),
  });
  const cached = SIMILAR_BASE_CACHE.get(key);
  if (cached && Date.now() - cached.ts <= SIMILAR_BASE_CACHE_TTL_MS) {
    return cached.data;
  }
  const data = await buildBaseModelRowsSnapshot({ city, includeDiscontinued });
  SIMILAR_BASE_CACHE.set(key, { ts: Date.now(), data });
  return data;
};

const buildFeatureMetaSnapshot = async () => {
  const docs = await VehicleFeature.find({})
    .select({
      _id: 1,
      brand: 1,
      model: 1,
      body_type_bucket: 1,
      seating_capacity: 1,
    })
    .lean();

  const byModel = new Map();

  docs.forEach((doc) => {
    const make = String(doc?.brand || "").trim();
    const modelRaw = String(doc?.model || "").trim();
    const model = trimLeading(modelRaw, make) || modelRaw;
    const key = buildMakeModelJoinKey(make, model);
    if (!key) return;

    const rawBody = String(doc?.body_type_bucket || "").trim();
    const bodyBucket = normalizeBodyTypeBucket(rawBody);
    const bodyLabel = formatBodyType(rawBody || bodyBucket);
    const seatValue = doc?.seating_capacity;
    const seatCount = parseSeatCount(seatValue);

    if (!byModel.has(key)) {
      byModel.set(key, {
        make,
        model,
        bodyCounts: new Map(),
        bodyLabels: new Map(),
        seatCounts: new Map(),
      });
    }
    const entry = byModel.get(key);
    if (bodyBucket) {
      entry.bodyCounts.set(
        bodyBucket,
        (entry.bodyCounts.get(bodyBucket) || 0) + 1,
      );
      if (bodyLabel && !entry.bodyLabels.has(bodyBucket)) {
        entry.bodyLabels.set(bodyBucket, bodyLabel);
      }
    }
    if (seatCount) {
      entry.seatCounts.set(
        seatCount,
        (entry.seatCounts.get(seatCount) || 0) + 1,
      );
    }
  });

  const out = new Map();
  byModel.forEach((entry, key) => {
    const bodyBucket =
      [...entry.bodyCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || "";
    const seatingCapacity =
      [...entry.seatCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ||
      null;
    out.set(key, {
      make: entry.make,
      model: entry.model,
      bodyTypeBucket: bodyBucket,
      bodyType: entry.bodyLabels.get(bodyBucket) || formatBodyType(bodyBucket),
      seatingCapacity: seatingCapacity ? Number(seatingCapacity) : null,
    });
  });

  return out;
};

const loadModelMetaOnDemand = async (make, model) => {
  if (!make || !model) return null;
  const brandRegex = new RegExp(`^${escapeRegex(String(make).trim())}$`, "i");
  const modelRegex = new RegExp(escapeRegex(String(model).trim()), "i");
  const docs = await VehicleFeature.find({
    brand: brandRegex,
    model: modelRegex,
  })
    .select({
      body_type_bucket: 1,
      seating_capacity: 1,
      features: 1,
      brand: 1,
      model: 1,
    })
    .limit(200)
    .lean();

  if (!docs.length) return null;
  const bodyCounts = new Map();
  const seatCounts = new Map();
  let displayBody = "";

  docs.forEach((doc) => {
    const rawBody =
      String(doc?.body_type_bucket || "").trim() ||
      extractFeatureValueByKeywords(doc?.features, [
        "body type",
        "bodytype",
        "vehicle type",
        "body style",
        "segment",
      ]);
    const bodyBucket = normalizeBodyTypeBucket(rawBody);
    if (bodyBucket) {
      bodyCounts.set(bodyBucket, (bodyCounts.get(bodyBucket) || 0) + 1);
      if (!displayBody) displayBody = formatBodyType(rawBody || bodyBucket);
    }

    const seatValue =
      doc?.seating_capacity ??
      extractFeatureValueByKeywords(doc?.features, [
        "seating capacity",
        "seat capacity",
        "seating",
        "number of seats",
        "no of seats",
        "no. of seats",
        "seats",
      ]);
    const seatCount = parseSeatCount(seatValue);
    if (seatCount) {
      seatCounts.set(seatCount, (seatCounts.get(seatCount) || 0) + 1);
    }
  });

  const topBody =
    [...bodyCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || "";
  const topSeat =
    [...seatCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || null;
  if (!topBody || !topSeat) return null;
  return {
    bodyTypeBucket: topBody,
    bodyType: displayBody || formatBodyType(topBody),
    seatingCapacity: Number(topSeat),
  };
};

const getFeatureMetaMapCached = async () => {
  const isFresh =
    Date.now() - FEATURE_META_CACHE.ts <= FEATURE_META_CACHE_TTL_MS;
  if (isFresh && FEATURE_META_CACHE.data?.size) {
    return FEATURE_META_CACHE.data;
  }
  const data = await buildFeatureMetaSnapshot();
  FEATURE_META_CACHE = { ts: Date.now(), data };
  return data;
};

const getVehicles = asyncHandler(async (req, res) => {
  const { q, make, model, variant, city, fuel } = req.query;
  const pageSize = req.query.limit ? Number(req.query.limit) : null;
  const skip = Number(req.query.skip) || 0;
  const includeFullPayload =
    String(req.query.full || "").toLowerCase() === "true" ||
    String(req.query.full || "") === "1";

  const cacheParams = {
    q,
    make,
    model,
    variant,
    city,
    fuel,
    pageSize,
    skip,
    includeFullPayload,
  };
  const cached = readCache(
    VEHICLE_LIST_CACHE,
    VEHICLE_LIST_CACHE_TTL_MS,
    "list",
    cacheParams,
  );
  if (cached) {
    return res.json({ ...cached, fromCache: true });
  }

  let query = buildVehicleQuery({ q, make, model, variant, city, fuel });
  let cursor = Vehicle.find(query).sort({ make: 1, model: 1, variant: 1 });

  if (!includeFullPayload) cursor.select(VEHICLE_LIST_PROJECTION);
  if (skip > 0) cursor.skip(skip);
  if (pageSize) cursor.limit(pageSize);

  const shouldCountSeparately = Boolean(pageSize || skip > 0);
  let [docs, count] = shouldCountSeparately
    ? await Promise.all([cursor.lean(), Vehicle.countDocuments(query)])
    : [await cursor.lean(), null];

  if (q && docs.length === 0) {
    query = buildVehicleQuery({
      q,
      make,
      model,
      variant,
      city,
      fuel,
      useSearchTokens: false,
    });
    cursor = Vehicle.find(query).sort({ make: 1, model: 1, variant: 1 });
    if (!includeFullPayload) cursor.select(VEHICLE_LIST_PROJECTION);
    if (skip > 0) cursor.skip(skip);
    if (pageSize) cursor.limit(pageSize);
    [docs, count] = shouldCountSeparately
      ? await Promise.all([cursor.lean(), Vehicle.countDocuments(query)])
      : [await cursor.lean(), null];
  }

  const data = includeFullPayload
    ? docs.map(normalizeVehicleRecord)
    : docs.map(toVehicleListItem);

  const response = { success: true, count: count ?? data.length, data };
  writeCache(VEHICLE_LIST_CACHE, "list", cacheParams, response);

  res.json(response);
});

const searchVehicleRecords = asyncHandler(async (req, res) => {
  const rawQ = String(req.query.q || req.query.search || "").trim();
  const q = normalizeRegNo(rawQ);
  const isFourDigitSuffixSearch = /^\d{4}$/.test(rawQ) || /^\d{4}$/.test(q);
  const requestedLimit = Number(req.query.limit);
  const defaultLimit = isFourDigitSuffixSearch ? 5000 : 20;
  const limit = Math.min(
    Math.max(
      Number.isFinite(requestedLimit) ? requestedLimit : defaultLimit,
      1,
    ),
    10000,
  );

  const fetchAll = req.query.all === "true";

  if (!fetchAll && rawQ.length < 2 && q.length < 2) {
    return res.json({ success: true, count: 0, data: [] });
  }

  const rawEscaped = escapeRegex(rawQ);
  const regEscaped = escapeRegex(q);
  const suffix = (q || normalizeRegNo(rawQ)).slice(-4);

  // If fetchAll requested, skip clause building and return all records
  if (fetchAll) {
    const rows = await VehicleRecord.find({})
      .select({
        _id: 1,
        registrationNumber: 1,
        customerName: 1,
        primaryMobile: 1,
        make: 1,
        model: 1,
        variant: 1,
        yearOfManufacture: 1,
        fuelType: 1,
        typesOfVehicle: 1,
        chassisNumber: 1,
        engineNumber: 1,
        registrationCity: 1,
        registrationDate: 1,
        loanId: 1,
        updatedAt: 1,
        createdAt: 1,
      })
      .sort({ updatedAt: -1 })
      .limit(limit)
      .lean();
    return res.json({ success: true, count: rows.length, data: rows });
  }

  const clauses = [];
  if (q.length >= 2) {
    if (isFourDigitSuffixSearch) {
      clauses.push({ registrationNumberLast4: suffix });
      clauses.push({
        registrationNumberNormalized: new RegExp(`${regEscaped}$`, "i"),
      });
    } else {
      clauses.push({
        registrationNumberNormalized: new RegExp(`^${regEscaped}`, "i"),
      });
      clauses.push({
        registrationNumberNormalized: new RegExp(regEscaped, "i"),
      });
      if (suffix.length === 4) {
        clauses.push({ registrationNumberLast4: suffix });
      }
    }
  }
  if (rawQ.length >= 2) {
    clauses.push({ customerName: new RegExp(rawEscaped, "i") });
    clauses.push({ primaryMobile: new RegExp(rawEscaped, "i") });
    clauses.push({ make: new RegExp(rawEscaped, "i") });
    clauses.push({ model: new RegExp(rawEscaped, "i") });
    clauses.push({ variant: new RegExp(rawEscaped, "i") });
  }
  if (!clauses.length) {
    return res.json({ success: true, count: 0, data: [] });
  }

  const fetchLimit = isFourDigitSuffixSearch ? limit : Math.max(limit * 4, 40);

  const rows = await VehicleRecord.find({ $or: clauses })
    .select({
      _id: 1,
      registrationNumber: 1,
      registrationNumberNormalized: 1,
      registrationNumberLast4: 1,
      customerName: 1,
      primaryMobile: 1,
      make: 1,
      model: 1,
      variant: 1,
      yearOfManufacture: 1,
      manufactureMonth: 1,
      engineNumber: 1,
      chassisNumber: 1,
      registrationDate: 1,
      regAuthority: 1,
      registrationCity: 1,
      hypothecation: 1,
      fuelType: 1,
      typesOfVehicle: 1,
      batteryNumber: 1,
      chargerNumber: 1,
      cubicCapacityCc: 1,
      updatedAt: 1,
      createdAt: 1,
    })
    .sort({ updatedAt: -1, createdAt: -1 })
    .limit(fetchLimit)
    .lean();

  const scored = rows
    .map((row) => {
      const normalized =
        normalizeRegNo(
          row?.registrationNumberNormalized || row?.registrationNumber,
        ) || "";

      let score = 0;
      if (q && normalized) {
        if (normalized === q) score += 150;
        if (normalized.startsWith(q)) score += 110;
        if (normalized.includes(q)) score += 50;
      }
      if (isFourDigitSuffixSearch && row?.registrationNumberLast4 === suffix)
        score += 220;
      if (isFourDigitSuffixSearch && normalized && normalized.endsWith(suffix))
        score += 170;
      if (
        !isFourDigitSuffixSearch &&
        suffix.length === 4 &&
        row?.registrationNumberLast4 === suffix
      )
        score += 80;
      if (
        !isFourDigitSuffixSearch &&
        suffix.length === 4 &&
        normalized &&
        normalized.endsWith(suffix)
      )
        score += 40;
      const customerName = String(row?.customerName || "");
      const primaryMobile = String(row?.primaryMobile || "");
      const make = String(row?.make || "");
      const model = String(row?.model || "");
      const variant = String(row?.variant || "");
      if (rawQ && new RegExp(rawEscaped, "i").test(customerName)) score += 90;
      if (rawQ && new RegExp(rawEscaped, "i").test(primaryMobile)) score += 70;
      if (rawQ && new RegExp(rawEscaped, "i").test(make)) score += 55;
      if (rawQ && new RegExp(rawEscaped, "i").test(model)) score += 45;
      if (rawQ && new RegExp(rawEscaped, "i").test(variant)) score += 35;
      if (!score) return null;

      return {
        ...row,
        registrationNumber:
          String(row?.registrationNumber || "").trim() || normalized,
        registrationNumberNormalized: normalized,
        score,
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      const aTs = Date.parse(a.updatedAt || a.createdAt || "") || 0;
      const bTs = Date.parse(b.updatedAt || b.createdAt || "") || 0;
      return bTs - aTs;
    });

  const deduped = [];
  const seen = new Set();
  for (const row of scored) {
    const key = row.registrationNumberNormalized || row.registrationNumber;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    deduped.push({
      registrationNumber: row.registrationNumber,
      registrationNumberNormalized: row.registrationNumberNormalized,
      customerName: row.customerName || "",
      primaryMobile: row.primaryMobile || "",
      make: row.make || "",
      model: row.model || "",
      variant: row.variant || "",
      yearOfManufacture: row.yearOfManufacture || "",
      manufactureMonth: row.manufactureMonth || "",
      engineNumber: row.engineNumber || "",
      chassisNumber: row.chassisNumber || "",
      registrationDate: row.registrationDate || null,
      regAuthority: row.regAuthority || "",
      registrationCity: row.registrationCity || "",
      hypothecation: row.hypothecation || "",
      fuelType: row.fuelType || "",
      typesOfVehicle: row.typesOfVehicle || "",
      batteryNumber: row.batteryNumber || "",
      chargerNumber: row.chargerNumber || "",
      cubicCapacityCc: row.cubicCapacityCc,
    });
    if (deduped.length >= limit) break;
  }

  res.json({ success: true, count: deduped.length, data: deduped });
});

const getVehicleById = asyncHandler(async (req, res) => {
  if (!req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
    res.status(400);
    throw new Error("Invalid vehicle ID format");
  }

  const vehicle = await Vehicle.findById(req.params.id);
  if (vehicle) {
    res.json({ success: true, data: normalizeVehicleRecord(vehicle) });
  } else {
    res.status(404);
    throw new Error("Vehicle not found");
  }
});

const createVehicle = asyncHandler(async (req, res) => {
  const make = String(req.body.make || req.body.brand || "").trim();
  const model = String(req.body.model || "").trim();
  const variant = String(req.body.variant || "").trim();
  const fuel = req.body.fuel || req.body.fuel_type;
  const city = req.body.city;

  if (!make || !model || !variant) {
    res.status(400);
    throw new Error("Please include Make, Model, and Variant");
  }

  const payload = withCanonicalVehiclePricing({
    ...req.body,
    make,
    brand: req.body.brand || make,
    model,
    variant,
  });
  const existingDocs = await Vehicle.find(
    buildVehicleQuery({ make, model, variant, city, fuel }),
  )
    .select({
      make: 1,
      brand: 1,
      model: 1,
      variant: 1,
      fuel: 1,
      fuel_type: 1,
      city: 1,
    })
    .lean();
  const duplicate = existingDocs.find((doc) =>
    matchesVehicleFilters(doc, { make, model, variant, city, fuel }),
  );
  if (duplicate) {
    res.status(400);
    throw new Error(
      "Vehicle variant already exists for this city/fuel combination",
    );
  }

  const vehicle = await Vehicle.create(payload);
  res
    .status(201)
    .json({ success: true, data: normalizeVehicleRecord(vehicle) });
});

const updateVehicle = asyncHandler(async (req, res) => {
  const vehicle = await Vehicle.findById(req.params.id);

  if (vehicle) {
    const nextMake = String(
      req.body.make || req.body.brand || vehicle.make || vehicle.brand || "",
    ).trim();
    Object.assign(vehicle, {
      ...req.body,
      make: nextMake,
      brand: req.body.brand || vehicle.brand || nextMake,
    });
    Object.assign(vehicle, withCanonicalVehiclePricing(vehicle.toObject()));
    const updatedVehicle = await vehicle.save();
    res.json({ success: true, data: normalizeVehicleRecord(updatedVehicle) });
  } else {
    res.status(404);
    throw new Error("Vehicle not found");
  }
});

const deleteVehicle = asyncHandler(async (req, res) => {
  const vehicle = await Vehicle.findById(req.params.id);
  if (vehicle) {
    await vehicle.deleteOne();
    res.json({ success: true, message: "Vehicle removed" });
  } else {
    res.status(404);
    throw new Error("Vehicle not found");
  }
});

const deleteVehicleRecord = asyncHandler(async (req, res) => {
  const record = await VehicleRecord.findById(req.params.id);
  if (record) {
    await record.deleteOne();
    res.json({ success: true, message: "Vehicle record removed" });
  } else {
    res.status(404);
    throw new Error("Vehicle record not found");
  }
});

const bulkUploadVehicles = asyncHandler(async (req, res) => {
  const vehiclesData = req.body;

  if (!Array.isArray(vehiclesData)) {
    res.status(400);
    throw new Error("Expected an array of vehicle objects");
  }

  const results = { inserted: 0, updated: 0, errors: [] };

  for (const item of vehiclesData) {
    try {
      const make = String(item.make || item.brand || "").trim();
      const model = String(item.model || "").trim();
      const variant = String(item.variant || "").trim();
      const fuel = item.fuel || item.fuel_type;
      const city = item.city;
      if (!make || !model || !variant) continue;

      const payload = withCanonicalVehiclePricing({
        ...item,
        make,
        brand: item.brand || make,
        model,
        variant,
      });
      const existingDocs = await Vehicle.find(
        buildVehicleQuery({ make, model, variant, city, fuel }),
      )
        .select({
          make: 1,
          brand: 1,
          model: 1,
          variant: 1,
          fuel: 1,
          fuel_type: 1,
          city: 1,
        })
        .lean();
      const duplicate = existingDocs.find((doc) =>
        matchesVehicleFilters(doc, { make, model, variant, city, fuel }),
      );

      if (duplicate) {
        await Vehicle.findByIdAndUpdate(duplicate._id, payload, {
          returnDocument: "after",
        });
        results.updated++;
      } else {
        await Vehicle.create(payload);
        results.inserted++;
      }
    } catch (error) {
      results.errors.push({ item, error: error.message });
    }
  }

  res.json({ success: true, data: results });
});

const getUniqueMakes = asyncHandler(async (req, res) => {
  const { city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);
  const cached = readDistinctCache("makes", { city, includeDiscontinued });
  if (cached) {
    return res.json({ success: true, data: cached, cached: true });
  }
  const cityQuery = city ? buildVehicleQuery({ city }) : {};
  const baseQuery = includeDiscontinued
    ? cityQuery
    : { ...cityQuery, ...ACTIVE_VARIANT_FILTER };

  const [makeValues, brandValues] = await Promise.all([
    Vehicle.distinct("make", {
      ...baseQuery,
      make: { $exists: true, $ne: null },
    }),
    Vehicle.distinct("brand", {
      ...baseQuery,
      brand: { $exists: true, $ne: null },
    }),
  ]);

  const makes = [
    ...new Set(
      [...makeValues, ...brandValues]
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  ].sort((a, b) => a.localeCompare(b));
  writeDistinctCache("makes", { city, includeDiscontinued }, makes);

  res.json({ success: true, data: makes });
});

const getUniqueModels = asyncHandler(async (req, res) => {
  const { make, city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);

  if (!make) {
    res.status(400);
    throw new Error("Make parameter is required");
  }
  const cached = readDistinctCache("models", {
    make,
    city,
    includeDiscontinued,
  });
  if (cached) {
    return res.json({ success: true, data: cached, cached: true });
  }

  const query = buildVehicleQuery({ make, city });
  if (!includeDiscontinued) mergeAndCondition(query, ACTIVE_VARIANT_FILTER);
  const rawModels = await Vehicle.distinct("model", {
    ...query,
    model: { $exists: true, $ne: null },
  });
  const models = [
    ...new Set(
      rawModels
        .map((value) => String(value || "").trim())
        .map((value) => trimLeading(value, make) || value)
        .filter(Boolean),
    ),
  ].sort((a, b) => a.localeCompare(b));
  writeDistinctCache("models", { make, city, includeDiscontinued }, models);

  res.json({ success: true, data: models });
});

const getUniqueVariants = asyncHandler(async (req, res) => {
  const { make, model, city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);

  if (!make || !model) {
    res.status(400);
    throw new Error("Make and Model parameters are required");
  }
  const cached = readDistinctCache("variants", {
    make,
    model,
    city,
    includeDiscontinued,
  });
  if (cached) {
    return res.json({ success: true, data: cached, cached: true });
  }

  const query = buildVehicleQuery({ make, model, city });
  if (!includeDiscontinued) mergeAndCondition(query, ACTIVE_VARIANT_FILTER);
  const rawVariants = await Vehicle.distinct("variant", {
    ...query,
    variant: { $exists: true, $ne: null },
  });
  const variants = [
    ...new Set(
      rawVariants
        .map((value) => String(value || "").trim())
        .map(
          (value) =>
            trimLeading(value, `${make} ${model}`.trim()) ||
            trimLeading(value, model) ||
            trimLeading(value, make) ||
            value,
        )
        .filter(Boolean),
    ),
  ].sort((a, b) => a.localeCompare(b));
  writeDistinctCache(
    "variants",
    { make, model, city, includeDiscontinued },
    variants,
  );

  res.json({ success: true, data: variants });
});

const getVariantOptionsByModel = asyncHandler(async (req, res) => {
  const { make, model, city } = req.query;
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);

  if (!make || !model) {
    res.status(400);
    throw new Error("Make and model are required");
  }

  const cacheParams = { make, model, city, includeDiscontinued };
  const cached = readCache(
    VEHICLE_LIST_CACHE,
    VEHICLE_LIST_CACHE_TTL_MS,
    "variants-options",
    cacheParams,
  );
  if (cached) {
    return res.json({ ...cached, fromCache: true });
  }

  const baseQuery = buildVehicleQuery({ make, model });
  const cityQuery = city ? buildVehicleQuery({ make, model, city }) : null;
  if (!includeDiscontinued) {
    mergeAndCondition(baseQuery, ACTIVE_VARIANT_FILTER);
    if (cityQuery) mergeAndCondition(cityQuery, ACTIVE_VARIANT_FILTER);
  }

  const cityDocs = cityQuery
    ? await Vehicle.find(cityQuery).select(VEHICLE_LIST_PROJECTION).lean()
    : [];
  const docs = cityDocs.length
    ? cityDocs
    : await Vehicle.find(baseQuery).select(VEHICLE_LIST_PROJECTION).lean();

  const variants = docs
    .map((doc) => normalizeVehicleRecord(doc))
    .sort((a, b) => (a.onRoadPrice || 0) - (b.onRoadPrice || 0));

  const byVariant = new Map();
  variants.forEach((doc) => {
    const key = normalizeText(doc.variant);
    if (!key || byVariant.has(key)) return;
    byVariant.set(key, {
      _id: doc._id,
      id: doc._id,
      make: doc.make,
      model: doc.model,
      variant: doc.variant,
      city: doc.city,
      fuel: doc.fuel,
      exShowroom: doc.exShowroom,
      onRoadPrice:
        doc.onRoadPrice ||
        parseAmount(
          doc.total_on_road_with_accessories || doc.on_road_price_cardekho || 0,
        ),
      insurance: doc.insurance,
      rto: doc.rto,
      tcs: parseAmount(
        doc.tcs || doc.other_tcsCharges || doc.otherCharges || 0,
      ),
      ...doc,
    });
  });

  const response = { success: true, data: Array.from(byVariant.values()) };
  writeCache(VEHICLE_LIST_CACHE, "variants-options", cacheParams, response);

  res.json(response);
});

const getVehicleByDetails = asyncHandler(async (req, res) => {
  const { make, model, variant, fuel, city } = req.query;

  if (!make || !model || !variant) {
    res.status(400);
    throw new Error("Make, model and variant are required");
  }

  const baseQuery = buildVehicleQuery({ make, model, variant, fuel });
  const cityQuery = city
    ? buildVehicleQuery({ make, model, variant, fuel, city })
    : null;

  const docsWithCity = cityQuery
    ? await Vehicle.find(cityQuery).select(VEHICLE_LIST_PROJECTION).lean()
    : [];
  const docs = docsWithCity.length
    ? docsWithCity
    : await Vehicle.find(baseQuery).select(VEHICLE_LIST_PROJECTION).lean();

  const match = docs
    .map((doc) => normalizeVehicleRecord(doc))
    .find(
      (doc) =>
        matchesExact(doc.make, make) &&
        matchesExact(doc.model, model) &&
        matchesExact(doc.variant, variant) &&
        matchesExact(doc.fuel, fuel),
    );

  if (!match) {
    res.status(404);
    throw new Error("Vehicle not found");
  }

  res.json({ success: true, data: match });
});

const getVehicleMedia = asyncHandler(async (req, res) => {
  const { make, model, variant } = req.query;

  if (!make || !model) {
    res.status(400);
    throw new Error("Make and model are required");
  }

  const cached = readCache(
    VEHICLE_MEDIA_CACHE,
    VEHICLE_MEDIA_CACHE_TTL_MS,
    "media",
    { make, model, variant },
  );
  if (cached) {
    return res.json({ ...cached, fromCache: true });
  }

  const collection = mongoose.connection.db.collection(
    VEHICLE_COLORS_COLLECTION,
  );
  const makeValue = String(make || "").trim();
  const modelCandidates = buildModelCandidates(make, model);
  const variantCandidates = variant
    ? buildVariantCandidates(make, model, variant)
    : [];

  // Fast exact path first (index-friendly).
  let docs = await collection
    .find({
      brand: makeValue,
      model: { $in: modelCandidates },
      ...(variantCandidates.length
        ? { variant: { $in: variantCandidates } }
        : {}),
    })
    .toArray();

  // Case-insensitive fallback for rows with casing drift.
  if (!docs.length) {
    docs = await collection
      .find(
        {
          brand: makeValue,
          model: { $in: modelCandidates },
          ...(variantCandidates.length
            ? { variant: { $in: variantCandidates } }
            : {}),
        },
        {
          collation: { locale: "en", strength: 2 },
        },
      )
      .toArray();
  }

  // Backward-compatible fallback for legacy rows where model/variant text is noisy.
  if (!docs.length) {
    docs = await collection.find({ brand: buildMakeRegex(make) }).toArray();
  }

  const colorRows = flattenVehicleColorDocuments(docs);

  const rows = colorRows
    .map((doc) => normalizeVehicleRecord(doc))
    .filter(
      (doc) =>
        matchesExact(doc.make, make) &&
        matchesExact(doc.model, model) &&
        matchesExact(doc.variant, variant),
    )
    .filter((doc) => {
      const sourceUrl =
        doc.sourceImageUrl || doc.image_url || doc.imageUrl || "";
      if (!sourceUrl) return true;
      return mediaUrlMatchesMakeModel(sourceUrl, make, model);
    })
    .sort((a, b) =>
      String(a.color_name || "").localeCompare(String(b.color_name || "")),
    );

  const fallbackRows = rows.length
    ? rows
    : colorRows
        .map((doc) => normalizeVehicleRecord(doc))
        .filter(
          (doc) =>
            matchesExact(doc.make, make) && matchesExact(doc.model, model),
        )
        .filter((doc) => {
          const sourceUrl =
            doc.sourceImageUrl || doc.image_url || doc.imageUrl || "";
          if (!sourceUrl) return true;
          return mediaUrlMatchesMakeModel(sourceUrl, make, model);
        })
        .sort((a, b) =>
          String(a.color_name || "").localeCompare(String(b.color_name || "")),
        );

  const payload = {
    success: true,
    data: dedupeMediaRowsByHexLatest(fallbackRows),
  };
  writeCache(VEHICLE_MEDIA_CACHE, "media", { make, model, variant }, payload);
  res.json(payload);
});

const toSlug = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const normalizeSalesDisplayName = (value = "") =>
  String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .replace(/^Maruti\s+(?:Arena|Nexa)\s+/i, "Maruti ");

const POPULAR_CAR_KNOWN_MAKES = [
  "Mercedes Benz",
  "Mercedes-Benz",
  "Land Rover",
  "Maruti Suzuki",
  "Volkswagen",
  "Mahindra",
  "Hyundai",
  "Toyota",
  "Citroen",
  "Renault",
  "Skoda",
  "Honda",
  "Nissan",
  "Maruti",
  "Tata",
  "Kia",
  "MG",
  "Jeep",
  "BYD",
  "BMW",
  "Audi",
  "Volvo",
  "Lexus",
  "Porsche",
  "Mini",
  "Force",
  "Isuzu",
];

const parseSalesVehicleName = (displayName = "", knownMakes = []) => {
  const cleaned = normalizeSalesDisplayName(displayName);
  const normalized = normalizeText(cleaned)
    .replace(/[-_]+/g, " ")
    .replace(/\s+/g, " ");
  const make = [...new Set([...knownMakes, ...POPULAR_CAR_KNOWN_MAKES])]
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .sort((a, b) => b.length - a.length)
    .find((candidate) => {
      const candidateKey = normalizeText(candidate)
        .replace(/[-_]+/g, " ")
        .replace(/\s+/g, " ");
      return (
        normalized === candidateKey || normalized.startsWith(`${candidateKey} `)
      );
    });

  if (make) {
    const model = trimLeading(cleaned, make) || cleaned;
    return {
      make,
      model,
      displayName: [make, model].filter(Boolean).join(" "),
    };
  }

  const [fallbackMake = "", ...modelParts] = cleaned.split(" ");
  const model = modelParts.join(" ").trim();
  return {
    make: fallbackMake,
    model: model || cleaned,
    displayName: [fallbackMake, model].filter(Boolean).join(" ") || cleaned,
  };
};

const parseSalesMonth = (value = "") => {
  const raw = String(value || "").trim();
  const match = raw.match(/^(\d{4})-(\d{1,2})/);
  if (!match) {
    return { monthLabel: raw || "", year: null, previousMonthLabel: "" };
  }

  const year = Number(match[1]);
  const monthIndex = Number(match[2]) - 1;
  const currentDate = new Date(Date.UTC(year, monthIndex, 1));
  const previousDate = new Date(Date.UTC(year, monthIndex - 1, 1));
  const formatter = new Intl.DateTimeFormat("en-IN", {
    month: "long",
    timeZone: "UTC",
  });

  return {
    monthLabel: formatter.format(currentDate),
    year,
    previousMonthLabel: formatter.format(previousDate),
  };
};

const formatExShowroomRange = (minPrice, maxPrice) => {
  const format = (value) => {
    const amount = Number(value || 0);
    if (!amount) return "";
    if (amount >= 10000000) return `₹${(amount / 10000000).toFixed(2)}Cr`;
    return `₹${(amount / 100000).toFixed(2)}L`;
  };

  const min = format(minPrice);
  const max = format(maxPrice);
  if (min && max && min !== max) return `${min} – ${max}`;
  return min || max || "";
};

const normalizeSalesNumber = (value) => {
  if (typeof value === "number" && Number.isFinite(value))
    return Math.round(value);
  const parsed = parseAmount(value);
  return parsed > 0 ? Math.round(parsed) : 0;
};

const normalizeSalesChange = (value) => {
  const number = Number(value);
  return Number.isFinite(number) ? Number(number.toFixed(2)) : 0;
};

const cleanR2ImageUrl = (value) => {
  const url = String(value || "").trim();
  if (!url || !url.startsWith(R2_PUBLIC_IMAGE_PREFIX)) return "";
  return url;
};

const compactVehicleMediaKey = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "")
    .trim();

const hasVehicleImageModelConflict = ({
  requestedModel = "",
  row = {},
  imageUrl = "",
} = {}) => {
  const requestedKey = compactVehicleMediaKey(requestedModel);
  if (!requestedKey) return false;

  const mediaKey = compactVehicleMediaKey(
    [
      imageUrl,
      row.normalizedImageUrl,
      row.cleanImageUrl,
      row.displayNormalizedImageUrl,
      row.displayStagedImageUrl,
      row.heroImageUrl,
      row.heroImage,
      row.defaultNormalizedImageUrl,
      row.sourceImageUrl,
      row.source_image_url,
      row.imageUrl,
      row.image_url,
      row.colorName,
      row.color_name,
      row.name,
      ...(Array.isArray(row.colors)
        ? row.colors.flatMap((color) => [
            color.normalizedImageUrl,
            color.stagedImageUrl,
            color.cleanImageUrl,
            color.imageUrl,
            color.sourceImageUrl,
            color.source_image_url,
          ])
        : []),
    ]
      .filter(Boolean)
      .join(" "),
  );

  const rules = [
    { requested: "innovahycross", forbidden: ["innovacrysta", "crysta"] },
    { requested: "innovacrysta", forbidden: ["innovahycross", "hycross"] },
    { requested: "creta", forbidden: ["cretaelectric", "electric", "nline"] },
    { requested: "fortuner", forbidden: ["legender"] },
    { requested: "thar", forbidden: ["tharroxx", "roxx"] },
  ];
  const rule = rules.find((item) => item.requested === requestedKey);

  return Boolean(rule?.forbidden.some((token) => mediaKey.includes(token)));
};

const pickVehicleColorImageUrl = (row = {}) =>
  cleanR2ImageUrl(row.heroImageNormalizedUrl) ||
  cleanR2ImageUrl(row.normalizedHeroImageUrl) ||
  cleanR2ImageUrl(row.heroNormalizedImageUrl) ||
  cleanR2ImageUrl(row.heroImageUrl) ||
  cleanR2ImageUrl(row.heroImage) ||
  cleanR2ImageUrl(row.displayNormalizedImageUrl) ||
  cleanR2ImageUrl(row.defaultNormalizedImageUrl) ||
  cleanR2ImageUrl(row.normalizedImageUrl) ||
  cleanR2ImageUrl(row.cleanImageUrl) ||
  cleanR2ImageUrl(row.displayStagedImageUrl) ||
  cleanR2ImageUrl(row.normalizedImagePngUrl) ||
  cleanR2ImageUrl(row.displayNormalizedImagePngUrl) ||
  cleanR2ImageUrl(row.defaultColorImageUrl) ||
  cleanR2ImageUrl(row.imageUrl) ||
  cleanR2ImageUrl(row.image_url) ||
  cleanR2ImageUrl(
    row.colors?.find?.((color) => cleanR2ImageUrl(color.normalizedImageUrl))
      ?.normalizedImageUrl,
  ) ||
  cleanR2ImageUrl(
    row.colors?.find?.((color) => cleanR2ImageUrl(color.stagedImageUrl))
      ?.stagedImageUrl,
  ) ||
  "";

const pickPopularImageFrame = (row = {}) =>
  normalizeImageFrameMeta(
    row.heroFrameMeta ||
      row.displayFrameMeta ||
      row.defaultFrameMeta ||
      row.imageFrame ||
      row.frameMeta ||
      row.image_frame ||
      row.carImageFrame ||
      row.car_image_frame ||
      row.frame ||
      firstMeaningfulFrame(
        row.colors?.find?.((color) =>
          firstMeaningfulFrame(color.frameMeta, color.imageFrame),
        )?.frameMeta,
        row.colors?.find?.((color) =>
          firstMeaningfulFrame(color.frameMeta, color.imageFrame),
        )?.imageFrame,
      ) ||
      null,
  ) || {};

const buildExactModelRegexes = (make, model) =>
  buildModelCandidates(make, model)
    .map((value) => String(value || "").trim())
    .filter(Boolean)
    .map((value) => new RegExp(`^${escapeRegex(value)}$`, "i"));

const buildPopularVehicleIdentity = (make, model) => {
  const normalized = normalizeVehicleDatasetRow({ brand: make, make, model });
  return {
    makeCandidates: buildExactMakeCandidates(make),
    modelCandidates: buildModelCandidates(make, model),
    brandNormalized: normalized.brand_normalized,
    modelNormalized: normalized.model_normalized,
  };
};

const popularIdentityKey = (make, model) => {
  const identity = buildPopularVehicleIdentity(make, model);
  return [identity.brandNormalized, identity.modelNormalized]
    .map((value) =>
      normalizeText(value).replace(/[-_]+/g, " ").replace(/\s+/g, " "),
    )
    .join("|");
};

const getPopularVehiclePriceRangeMap = async ({ vehicles = [], city }) => {
  const identities = vehicles
    .map((vehicle) => ({
      ...vehicle,
      identity: buildPopularVehicleIdentity(vehicle.make, vehicle.model),
      key: popularIdentityKey(vehicle.make, vehicle.model),
    }))
    .filter(
      (item) => item.identity.brandNormalized && item.identity.modelNormalized,
    );

  if (!identities.length) return new Map();

  const identityOr = identities.map((item) => ({
    brand_normalized: item.identity.brandNormalized,
    model_normalized: item.identity.modelNormalized,
  }));
  const projection = {
    make: 1,
    brand: 1,
    model: 1,
    variant: 1,
    city: 1,
    ex_showroom: 1,
    exShowroom: 1,
  };
  const cityCandidates = buildCityCandidates(city);
  const buildQuery = (withCity = true) => {
    const query = {
      $and: [
        { $or: identityOr },
        ...(withCity && cityCandidates.length
          ? [
              cityCandidates.length === 1
                ? { city: cityCandidates[0] }
                : { city: { $in: cityCandidates } },
            ]
          : []),
        ACTIVE_VARIANT_FILTER,
      ],
    };
    return query;
  };

  const docsByKey = new Map();
  const addDocs = (docs = []) => {
    docs.forEach((doc) => {
      const normalized = normalizeVehicleDatasetRow(doc);
      const key = popularIdentityKey(normalized.brand, normalized.model);
      if (!docsByKey.has(key)) docsByKey.set(key, []);
      docsByKey.get(key).push(doc);
    });
  };

  addDocs(await Vehicle.find(buildQuery(true)).select(projection).lean());

  const missingIdentities = identities.filter(
    (item) => !docsByKey.has(item.key),
  );
  if (missingIdentities.length) {
    const fallbackQuery = {
      $and: [
        {
          $or: missingIdentities.map((item) => ({
            brand_normalized: item.identity.brandNormalized,
            model_normalized: item.identity.modelNormalized,
          })),
        },
        ACTIVE_VARIANT_FILTER,
      ],
    };
    addDocs(await Vehicle.find(fallbackQuery).select(projection).lean());
  }

  const rangeMap = new Map();
  identities.forEach((item) => {
    const docs = docsByKey.get(item.key) || [];
    const prices = docs
      .map((doc) => normalizeVehicleRecord(doc))
      .filter(
        (doc) =>
          matchesExact(doc.make, item.make) &&
          matchesExact(doc.model, item.model),
      )
      .map((doc) =>
        firstPositiveAmount(
          doc.ex_showroom,
          doc.exShowroom,
          doc.exShowroomPrice,
        ),
      )
      .filter((amount) => amount > 0)
      .sort((a, b) => a - b);

    rangeMap.set(item.key, {
      minExShowroomPrice: prices[0] || 0,
      maxExShowroomPrice: prices[prices.length - 1] || prices[0] || 0,
    });
  });

  return rangeMap;
};

const getPopularVehicleImageMap = async (vehicles = []) => {
  const identities = vehicles
    .map((vehicle) => ({
      ...vehicle,
      identity: buildPopularVehicleIdentity(vehicle.make, vehicle.model),
      key: popularIdentityKey(vehicle.make, vehicle.model),
    }))
    .filter((item) => item.identity.modelCandidates.length);

  if (!identities.length) return new Map();

  const collection = mongoose.connection.db.collection(
    VEHICLE_COLORS_COLLECTION,
  );
  const activeScopeFilter = {
    $or: [
      { scopeStatus: { $exists: false } },
      { scopeStatus: { $ne: "rejected" } },
    ],
  };
  const projection = {
    brand: 1,
    make: 1,
    brandName: 1,
    model: 1,
    modelName: 1,
    model_name: 1,
    color_name: 1,
    colorName: 1,
    normalizedImageUrl: 1,
    cleanImageUrl: 1,
    heroImageNormalizedUrl: 1,
    normalizedHeroImageUrl: 1,
    heroNormalizedImageUrl: 1,
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
    sourceImageUrl: 1,
    source_image_url: 1,
    colors: 1,
    imageFrame: 1,
    image_frame: 1,
    carImageFrame: 1,
    car_image_frame: 1,
    frame: 1,
    scopeStatus: 1,
    isSelected: 1,
    selected: 1,
    isDefault: 1,
    default: 1,
    imageBackgroundRemoved: 1,
    updatedAt: 1,
  };
  const sort = {
    isSelected: -1,
    selected: -1,
    isDefault: -1,
    default: -1,
    imageBackgroundRemoved: -1,
    color_name: 1,
    colorName: 1,
    updatedAt: -1,
  };
  const rows = await collection
    .find(
      {
        $and: [
          {
            $or: identities.map((item) => ({
              brand: { $in: item.identity.makeCandidates },
              model: { $in: item.identity.modelCandidates },
            })),
          },
          activeScopeFilter,
        ],
      },
      { projection },
    )
    .sort(sort)
    .limit(Math.max(120, identities.length * 12))
    .toArray();

  const imageMap = new Map();
  identities.forEach((item) => {
    const row = rows.find((candidate) => {
      const rowMake =
        candidate.brand || candidate.make || candidate.brandName || "";
      const rowModel = String(
        candidate.model || candidate.modelName || candidate.model_name || "",
      ).trim();
      const normalizedImageUrl = pickVehicleColorImageUrl(candidate);
      return (
        normalizedImageUrl &&
        matchesExact(rowMake, item.make) &&
        matchesExact(
          trimLeading(rowModel, item.make) || rowModel,
          item.model,
        ) &&
        !hasVehicleImageModelConflict({
          requestedModel: item.model,
          row: candidate,
          imageUrl: normalizedImageUrl,
        })
      );
    });
    const normalizedImageUrl = pickVehicleColorImageUrl(row);
    imageMap.set(item.key, {
      imageUrl: normalizedImageUrl || "",
      normalizedImageUrl: normalizedImageUrl || "",
      imageFrame: row ? pickPopularImageFrame(row) : {},
    });
  });

  return imageMap;
};

const getPopularVehiclePriceRange = async ({ make, model, city }) => {
  const identity = buildPopularVehicleIdentity(make, model);
  const projection = {
    make: 1,
    brand: 1,
    model: 1,
    variant: 1,
    city: 1,
    ex_showroom: 1,
    exShowroom: 1,
  };
  const normalizedBaseQuery =
    identity.brandNormalized && identity.modelNormalized
      ? {
          brand_normalized: identity.brandNormalized,
          model_normalized: identity.modelNormalized,
        }
      : null;

  let docs = [];
  if (normalizedBaseQuery) {
    const cityCandidates = buildCityCandidates(city);
    const normalizedCityQuery = {
      ...normalizedBaseQuery,
      ...(cityCandidates.length === 1
        ? { city: cityCandidates[0] }
        : cityCandidates.length > 1
          ? { city: { $in: cityCandidates } }
          : {}),
    };
    mergeAndCondition(normalizedCityQuery, ACTIVE_VARIANT_FILTER);
    docs = city
      ? await Vehicle.find(normalizedCityQuery).select(projection).lean()
      : [];

    if (!docs.length) {
      const normalizedFallbackQuery = { ...normalizedBaseQuery };
      mergeAndCondition(normalizedFallbackQuery, ACTIVE_VARIANT_FILTER);
      docs = await Vehicle.find(normalizedFallbackQuery)
        .select(projection)
        .lean();
    }
  }

  if (!docs.length) {
    const cityQuery = buildVehicleQuery({ make, model, city });
    mergeAndCondition(cityQuery, ACTIVE_VARIANT_FILTER);

    const baseQuery = buildVehicleQuery({ make, model });
    mergeAndCondition(baseQuery, ACTIVE_VARIANT_FILTER);

    docs = city ? await Vehicle.find(cityQuery).select(projection).lean() : [];
    if (!docs.length) {
      docs = await Vehicle.find(baseQuery).select(projection).lean();
    }
  }

  const prices = docs
    .map((doc) => normalizeVehicleRecord(doc))
    .filter(
      (doc) => matchesExact(doc.make, make) && matchesExact(doc.model, model),
    )
    .map((doc) =>
      firstPositiveAmount(doc.ex_showroom, doc.exShowroom, doc.exShowroomPrice),
    )
    .filter((amount) => amount > 0)
    .sort((a, b) => a - b);

  return {
    minExShowroomPrice: prices[0] || 0,
    maxExShowroomPrice: prices[prices.length - 1] || prices[0] || 0,
  };
};

const getPopularVehicleImage = async ({ make, model }) => {
  const makeRegex = buildMakeRegex(make);
  const modelRegexes = buildExactModelRegexes(make, model);
  if (!modelRegexes.length)
    return { imageUrl: "", normalizedImageUrl: "", imageFrame: {} };

  const collection = mongoose.connection.db.collection(
    VEHICLE_COLORS_COLLECTION,
  );
  const identity = buildPopularVehicleIdentity(make, model);
  const projection = {
    brand: 1,
    make: 1,
    brandName: 1,
    model: 1,
    modelName: 1,
    model_name: 1,
    color_name: 1,
    colorName: 1,
    normalizedImageUrl: 1,
    cleanImageUrl: 1,
    heroImageNormalizedUrl: 1,
    normalizedHeroImageUrl: 1,
    heroNormalizedImageUrl: 1,
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
    sourceImageUrl: 1,
    source_image_url: 1,
    colors: 1,
    imageFrame: 1,
    image_frame: 1,
    carImageFrame: 1,
    car_image_frame: 1,
    frame: 1,
    scopeStatus: 1,
    isSelected: 1,
    selected: 1,
    isDefault: 1,
    default: 1,
    imageBackgroundRemoved: 1,
    updatedAt: 1,
  };
  const sort = {
    isSelected: -1,
    selected: -1,
    isDefault: -1,
    default: -1,
    imageBackgroundRemoved: -1,
    color_name: 1,
    colorName: 1,
    updatedAt: -1,
  };
  const activeScopeFilter = {
    $or: [
      { scopeStatus: { $exists: false } },
      { scopeStatus: { $ne: "rejected" } },
    ],
  };
  const exactQuery = {
    $and: [
      {
        $or: [
          { brand: { $in: identity.makeCandidates } },
          { make: { $in: identity.makeCandidates } },
          { brandName: { $in: identity.makeCandidates } },
        ],
      },
      { model: { $in: identity.modelCandidates } },
      activeScopeFilter,
    ],
  };

  let rows = await collection
    .find(exactQuery, { projection })
    .sort(sort)
    .limit(12)
    .toArray();

  rows = rows.filter((row) => {
    const normalizedImageUrl = pickVehicleColorImageUrl(row);
    return (
      normalizedImageUrl &&
      !hasVehicleImageModelConflict({
        requestedModel: model,
        row,
        imageUrl: normalizedImageUrl,
      })
    );
  });

  if (!rows.length) {
    rows = await collection
      .find(
        {
          $and: [
            {
              $or: [
                { brand: makeRegex },
                { make: makeRegex },
                { brandName: makeRegex },
                { manufacturer: makeRegex },
              ],
            },
            {
              $or: [
                { model: { $in: modelRegexes } },
                { modelName: { $in: modelRegexes } },
                { model_name: { $in: modelRegexes } },
              ],
            },
            activeScopeFilter,
            {
              $or: [
                {
                  normalizedImageUrl: {
                    $regex: `^${escapeRegex(R2_PUBLIC_IMAGE_PREFIX)}`,
                    $options: "i",
                  },
                },
                {
                  cleanImageUrl: {
                    $regex: `^${escapeRegex(R2_PUBLIC_IMAGE_PREFIX)}`,
                    $options: "i",
                  },
                },
              ],
            },
          ],
        },
        { projection },
      )
      .sort(sort)
      .limit(8)
      .toArray();
  }

  const exactRow = rows.find((row) => {
    const rowModel = String(
      row.model || row.modelName || row.model_name || "",
    ).trim();
    return matchesExact(trimLeading(rowModel, make) || rowModel, model);
  });
  const row = exactRow || rows[0] || null;
  const normalizedImageUrl = pickVehicleColorImageUrl(row);

  return {
    imageUrl: normalizedImageUrl,
    normalizedImageUrl,
    imageFrame: row ? pickPopularImageFrame(row) : {},
  };
};

const buildPopularCarsPayload = async ({
  city = "new-delhi",
  limit = 25,
} = {}) => {
  const startedAt = Date.now();
  const salesCollection =
    mongoose.connection.db.collection("monthly_car_sales");
  const latest = await salesCollection
    .find({ source: "v3cars" })
    .sort({ month: -1, rank: 1 })
    .limit(1)
    .toArray();
  const month = latest[0]?.month || "";

  if (!month) {
    return {
      ok: true,
      source: "v3cars",
      month: "",
      year: null,
      city,
      count: 0,
      rows: [],
      meta: { queryMs: Date.now() - startedAt, reason: "monthly_sales_empty" },
    };
  }

  const salesRows = await salesCollection
    .find({ month, source: "v3cars" })
    .sort({ rank: 1 })
    .limit(limit)
    .toArray();
  const knownMakes = POPULAR_CAR_KNOWN_MAKES;
  const monthInfo = parseSalesMonth(month);
  const parsedRows = salesRows
    .map((row) => {
      const rawName = normalizeSalesDisplayName(
        row.model || row.displayName || row.name,
      );
      const parsed = parseSalesVehicleName(rawName, knownMakes);
      if (!parsed.make || !parsed.model) return null;
      return {
        row,
        parsed,
        key: popularIdentityKey(parsed.make, parsed.model),
      };
    })
    .filter(Boolean);
  const parsedVehicles = parsedRows.map((item) => item.parsed);
  const [priceRangeMap, imageMap] = await Promise.all([
    getPopularVehiclePriceRangeMap({ vehicles: parsedVehicles, city }),
    getPopularVehicleImageMap(parsedVehicles),
  ]);

  const rows = parsedRows
    .map(({ row, parsed, key }) => {
      const { minExShowroomPrice = 0, maxExShowroomPrice = 0 } =
        priceRangeMap.get(key) || {};
      const image = imageMap.get(key) || {};

      return {
        id: toSlug(`${parsed.make}-${parsed.model}`),
        rank: Number(row.rank || 0) || 0,
        make: parsed.make,
        brand: parsed.make,
        rawBrand: parsed.make,
        model: parsed.model,
        displayName: parsed.displayName,
        bodyStyle: String(row.bodyStyle || "").trim(),
        segment: String(row.segment || "").trim(),
        priceRange: formatExShowroomRange(
          minExShowroomPrice,
          maxExShowroomPrice,
        ),
        minExShowroomPrice,
        maxExShowroomPrice,
        currentMonth: monthInfo.monthLabel,
        previousMonth: monthInfo.previousMonthLabel,
        currentMonthSales: normalizeSalesNumber(
          row.sales || row.currentMonthSales,
        ),
        previousMonthSales: normalizeSalesNumber(row.previousMonthSales),
        salesChangePercent: normalizeSalesChange(
          row.percentChange ?? row.salesChangePercent,
        ),
        salesTrend:
          normalizeSalesChange(row.percentChange ?? row.salesChangePercent) > 0
            ? "up"
            : normalizeSalesChange(
                  row.percentChange ?? row.salesChangePercent,
                ) < 0
              ? "down"
              : "flat",
        imageUrl: image.imageUrl,
        normalizedImageUrl: image.normalizedImageUrl,
        imageFrame: image.imageFrame || {},
        city,
        source: "v3cars",
      };
    })
    .filter(Boolean);

  const payload = {
    ok: true,
    source: "v3cars",
    month: monthInfo.monthLabel,
    year: monthInfo.year,
    city,
    count: rows.length,
    rows,
    meta: { queryMs: Date.now() - startedAt },
  };
  return payload;
};

const warmPopularCarsCache = async ({
  city = "new-delhi",
  limit = 25,
} = {}) => {
  if (!mongoose.connection?.db) {
    throw new Error(
      "MongoDB connection is not ready for popular cars cache warm-up",
    );
  }

  const normalizedCity = toCityToken(city || "new-delhi") || "new-delhi";
  const normalizedLimit = Math.min(Math.max(Number(limit) || 25, 1), 25);
  const cacheKey = getCacheKey("popular-cars", {
    city: normalizedCity,
    limit: normalizedLimit,
  });
  const cached = readCache(
    POPULAR_CARS_CACHE,
    POPULAR_CARS_CACHE_TTL_MS,
    "popular-cars",
    {
      city: normalizedCity,
      limit: normalizedLimit,
    },
  );
  if (cached) return cached;

  if (POPULAR_CARS_IN_FLIGHT.has(cacheKey)) {
    return POPULAR_CARS_IN_FLIGHT.get(cacheKey);
  }

  const warmPromise = buildPopularCarsPayload({
    city: normalizedCity,
    limit: normalizedLimit,
  })
    .then((payload) => {
      writeCache(
        POPULAR_CARS_CACHE,
        "popular-cars",
        { city: normalizedCity, limit: normalizedLimit },
        payload,
      );
      return payload;
    })
    .finally(() => {
      POPULAR_CARS_IN_FLIGHT.delete(cacheKey);
    });

  POPULAR_CARS_IN_FLIGHT.set(cacheKey, warmPromise);
  return warmPromise;
};

const getPopularCars = asyncHandler(async (req, res) => {
  const city = toCityToken(req.query.city || "new-delhi") || "new-delhi";
  const limitRaw = Number(req.query.limit);
  const limit =
    Number.isFinite(limitRaw) && limitRaw > 0
      ? Math.min(Math.round(limitRaw), 25)
      : 25;

  const cached = readCache(
    POPULAR_CARS_CACHE,
    POPULAR_CARS_CACHE_TTL_MS,
    "popular-cars",
    {
      city,
      limit,
    },
  );
  if (cached) {
    res.set("Cache-Control", "public, max-age=300, stale-while-revalidate=600");
    return res.json({ ...cached, cached: true });
  }

  const payload = await warmPopularCarsCache({ city, limit });
  res.set("Cache-Control", "public, max-age=300, stale-while-revalidate=600");
  return res.json(payload);
});

const getSimilarModels = asyncHandler(async (req, res) => {
  const startedAt = Date.now();
  const make = String(req.query.make || "").trim();
  const model = String(req.query.model || "").trim();
  const city = "new-delhi";
  const includeDiscontinued = parseBoolean(req.query.includeDiscontinued);
  const toleranceRaw = Number(req.query.tolerance);
  const tolerance =
    Number.isFinite(toleranceRaw) && toleranceRaw > 0 && toleranceRaw <= 0.5
      ? toleranceRaw
      : 0.15;
  const limitRaw = Number(req.query.limit);
  const limit =
    Number.isFinite(limitRaw) && limitRaw > 0
      ? Math.min(Math.round(limitRaw), 20)
      : 5;

  if (!make || !model) {
    res.status(400);
    throw new Error("Make and model are required");
  }

  const selectedKey = buildMakeModelJoinKey(make, model);
  if (!selectedKey) {
    return res.json({
      success: true,
      baseModel: null,
      data: [],
      meta: { queryMs: Date.now() - startedAt, reason: "invalid_make_model" },
    });
  }

  const [baseRows, featureMetaMap] = await Promise.all([
    getBaseModelRowsCached({ city, includeDiscontinued }),
    getFeatureMetaMapCached(),
  ]);

  const selectedBase = baseRows.find((row) => row?.key === selectedKey);

  if (!selectedBase) {
    return res.json({
      success: true,
      baseModel: null,
      data: [],
      meta: {
        queryMs: Date.now() - startedAt,
        rowsScanned: baseRows?.length || 0,
        reason: "base_variant_not_found",
      },
    });
  }

  let selectedMeta = featureMetaMap.get(selectedKey) || null;
  if (!selectedMeta) {
    const ondemand = await loadModelMetaOnDemand(
      selectedBase.make,
      selectedBase.model,
    );
    if (ondemand) {
      selectedMeta = {
        make: selectedBase.make,
        model: selectedBase.model,
        ...ondemand,
      };
      featureMetaMap.set(selectedKey, selectedMeta);
    }
  }

  const selectedBodyKey = normalizeText(selectedMeta?.bodyTypeBucket || "");
  const selectedSeat = Number(selectedMeta?.seatingCapacity || 0) || null;
  const metadataReady = Boolean(selectedBodyKey && selectedSeat);

  const baseModel = {
    make: selectedBase.make,
    model: selectedBase.model,
    basePrice: selectedBase.basePrice,
    baseVariant: selectedBase.variant,
    city: selectedBase.city || city || "",
    bodyType: selectedMeta?.bodyType || "",
    bodyTypeBucket: selectedMeta?.bodyTypeBucket || "",
    seatingCapacity: selectedSeat,
    metadataReady,
  };

  if (!metadataReady) {
    return res.json({
      success: true,
      baseModel,
      data: [],
      meta: {
        queryMs: Date.now() - startedAt,
        rowsScanned: baseRows?.length || 0,
        reason: "body_or_seating_missing",
      },
    });
  }

  const minPrice = selectedBase.basePrice * (1 - tolerance);
  const maxPrice = selectedBase.basePrice * (1 + tolerance);

  const allSimilar = (baseRows || [])
    .filter((row) => row?.key && row.key !== selectedKey)
    .filter((row) => {
      const price = Number(row?.basePrice || 0);
      if (!price || price < minPrice || price > maxPrice) return false;
      const meta = featureMetaMap.get(row.key);
      if (!meta) return false;
      if (normalizeText(meta?.bodyTypeBucket || "") !== selectedBodyKey)
        return false;
      if ((Number(meta?.seatingCapacity || 0) || null) !== selectedSeat)
        return false;
      return true;
    })
    .map((row) => {
      const meta = featureMetaMap.get(row.key);
      return {
        make: row.make,
        model: row.model,
        startingPrice: Number(row.basePrice) || 0,
        baseVariant: row.variant,
        city: row.city || "",
        bodyType: meta?.bodyType || "",
        bodyTypeBucket: meta?.bodyTypeBucket || "",
        seatingCapacity: Number(meta?.seatingCapacity || 0) || null,
        priceDelta: (Number(row.basePrice) || 0) - selectedBase.basePrice,
      };
    })
    .sort(
      (a, b) =>
        Math.abs(Number(a?.priceDelta || 0)) -
        Math.abs(Number(b?.priceDelta || 0)),
    );
  const similar = allSimilar.slice(0, limit);

  return res.json({
    success: true,
    baseModel,
    data: similar,
    meta: {
      queryMs: Date.now() - startedAt,
      rowsScanned: baseRows?.length || 0,
      tolerance,
      city: city || null,
      includeDiscontinued: Boolean(includeDiscontinued),
      totalMatches: allSimilar.length,
    },
  });
});

let vehicleCacheWarmupStarted = false;

const warmVehicleCachesWhenConnected = (attempt = 0) => {
  if (!mongoose.connection?.db) {
    if (attempt < 24) {
      setTimeout(() => warmVehicleCachesWhenConnected(attempt + 1), 500);
    }
    return;
  }

  if (vehicleCacheWarmupStarted) return;
  vehicleCacheWarmupStarted = true;

  void warmPopularCarsCache({ city: "new-delhi", limit: 25 }).catch(() => {
    vehicleCacheWarmupStarted = false;
  });

  setTimeout(() => {
    void getBaseModelRowsCached({
      city: "new-delhi",
      includeDiscontinued: false,
    }).catch(() => {});
    void getFeatureMetaMapCached().catch(() => {});
  }, 1500);
};

// Warm vehicle discovery caches in background so first UI open is fast once Mongo is ready.
setTimeout(() => warmVehicleCachesWhenConnected(), 500);

export {
  getVehicles,
  searchVehicleRecords,
  getVehicleById,
  createVehicle,
  updateVehicle,
  deleteVehicle,
  deleteVehicleRecord,
  bulkUploadVehicles,
  getUniqueMakes,
  getUniqueModels,
  getUniqueVariants,
  getVariantOptionsByModel,
  getVehicleByDetails,
  getVehicleMedia,
  getPopularCars,
  getSimilarModels,
};
