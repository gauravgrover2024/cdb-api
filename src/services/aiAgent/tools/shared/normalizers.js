import { normalizeSearchKey } from "../../aiAgent.planSchema.js";

/**
 * Shared normalizers for ACI Assist V2 tools.
 * These helpers are data-only. They do not build canvases or answers.
 */

export const DEFAULT_CITY = "new-delhi";

export const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

export const searchKey = (value = "") => normalizeSearchKey(value || "");

export const isPlainObject = (value) =>
  Boolean(value) &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  !(value instanceof Date);

export const asArray = (value) => {
  if (Array.isArray(value)) {
    return value.filter(
      (item) => item !== undefined && item !== null && item !== "",
    );
  }

  if (value === undefined || value === null || value === "") return [];

  return [value];
};

export const unique = (items = []) => [...new Set(asArray(items).filter(Boolean))];

export const uniqueBy = (items = [], getKey = (item) => item) => {
  const seen = new Set();
  const output = [];

  for (const item of asArray(items)) {
    const key = searchKey(getKey(item));
    if (!key || seen.has(key)) continue;

    seen.add(key);
    output.push(item);
  }

  return output;
};

export const firstMeaningful = (...values) =>
  values.find(
    (value) =>
      value !== undefined &&
      value !== null &&
      String(value).trim() !== "",
  ) || "";

export const displayName = (value = "") => {
  const text = cleanText(value);
  if (!text) return "";

  return text
    .split(" ")
    .map((part) => {
      if (!part) return "";
      if (/[A-Z0-9()]/.test(part)) return part;
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join(" ");
};

export const normalizeCity = (value = "") => {
  const key = searchKey(value);

  if (!key) return DEFAULT_CITY;

  if (["delhi", "new delhi", "ncr", "new delhi ncr"].includes(key)) {
    return DEFAULT_CITY;
  }

  return key.replace(/\s+/g, "-");
};

export const getToolEntities = (toolPlan = {}) => toolPlan.entities || {};
export const getToolFilters = (toolPlan = {}) => toolPlan.filters || {};

export const getToolModel = (toolPlan = {}, context = {}) =>
  displayName(
    firstMeaningful(
      toolPlan.entities?.model,
      toolPlan.entities?.primaryModel,
      toolPlan.filters?.model,
      context?.selectedVehicle?.model,
      context?.anchorModel,
      context?.model,
    ),
  );

export const getToolVariant = (toolPlan = {}, context = {}) =>
  displayName(
    firstMeaningful(
      toolPlan.entities?.variant,
      toolPlan.entities?.primaryVariant,
      toolPlan.filters?.variant,
      context?.selectedVehicle?.variant,
      context?.anchorVariant,
      context?.variant,
    ),
  );

export const getToolBrand = (toolPlan = {}, context = {}) =>
  displayName(
    firstMeaningful(
      toolPlan.entities?.brand,
      toolPlan.entities?.make,
      toolPlan.filters?.brand,
      toolPlan.filters?.make,
      context?.selectedVehicle?.brand,
      context?.anchorBrand,
    ),
  );

export const getToolCity = (toolPlan = {}, context = {}) =>
  normalizeCity(
    firstMeaningful(
      toolPlan.filters?.city,
      toolPlan.entities?.city,
      context?.selectedVehicle?.city,
      context?.anchorCity,
      DEFAULT_CITY,
    ),
  );

export const getToolFeature = (toolPlan = {}) =>
  cleanText(
    firstMeaningful(
      toolPlan.entities?.feature,
      asArray(toolPlan.entities?.features)[0],
      asArray(toolPlan.filters?.mustHaveFeatures)[0],
      asArray(toolPlan.filters?.compareFeatures)[0],
    ),
  );

export const getToolModels = (toolPlan = {}, context = {}) => {
  const entities = getToolEntities(toolPlan);
  const filters = getToolFilters(toolPlan);

  return uniqueBy(
    [
      ...asArray(entities.models),
      ...asArray(entities.comparisonModels),
      ...asArray(filters.models),
      entities.model,
      filters.model,
      context?.selectedVehicle?.model,
      context?.anchorModel,
    ]
      .filter(Boolean)
      .map(displayName),
  );
};

export const numberFromValue = (value) => {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;

  const text = String(value ?? "")
    .replace(/,/g, "")
    .trim();

  if (!text) return null;

  const match = text.match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;

  const number = Number(match[0]);
  return Number.isFinite(number) ? number : null;
};

export const amountFromValue = (value) => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? Math.round(value) : 0;
  }

  const text = String(value ?? "").toLowerCase();
  const number = numberFromValue(value);

  if (number === null) return 0;

  if (/\b(cr|crore|crores)\b/.test(text) && number <= 100) {
    return Math.round(number * 10000000);
  }

  if (/\b(lakh|lakhs|lac|lacs)\b/.test(text) && number <= 300) {
    return Math.round(number * 100000);
  }

  return Math.round(number);
};

export const firstNumber = (...values) => {
  for (const value of values) {
    const number = amountFromValue(value);
    if (number > 0) return number;
  }

  return 0;
};

export const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      if (isPlainObject(value)) return Object.keys(value).length > 0;
      return value !== undefined && value !== null && value !== "";
    }),
  );

export const safeJsonText = (value = {}) => {
  try {
    return searchKey(JSON.stringify(value || {}));
  } catch {
    return "";
  }
};

export const normalizeColors = (row = {}) => {
  const rawColors = firstMeaningful(
    row.colors,
    row.colours,
    row.availableColors,
    row.availableColours,
    row.colorOptions,
    row.exteriorColors,
    row.exteriorColours,
    row.color,
    row.colour,
  );

  if (!rawColors) return [];

  if (Array.isArray(rawColors)) {
    return unique(
      rawColors
        .map((item) => {
          if (typeof item === "string") return displayName(item);

          return displayName(
            firstMeaningful(item.name, item.color, item.colour, item.title),
          );
        })
        .filter(Boolean),
    );
  }

  if (typeof rawColors === "string") {
    return unique(
      rawColors
        .split(/[,/|]+/)
        .map(displayName)
        .filter(Boolean),
    );
  }

  if (isPlainObject(rawColors)) {
    return unique(
      Object.values(rawColors)
        .flat()
        .map((item) =>
          typeof item === "string"
            ? displayName(item)
            : displayName(firstMeaningful(item?.name, item?.color, item?.colour)),
        )
        .filter(Boolean),
    );
  }

  return [];
};

export const normalizeFeatures = (row = {}) => {
  const buckets = [
    row.features,
    row.keyFeatures,
    row.key_features,
    row.specs,
    row.specifications,
    row.equipment,
    row.featureList,
    row.feature_list,
  ].filter(Boolean);

  const features = [];

  for (const bucket of buckets) {
    if (Array.isArray(bucket)) {
      for (const item of bucket) {
        if (typeof item === "string") {
          features.push(displayName(item));
        } else if (isPlainObject(item)) {
          features.push(
            displayName(
              firstMeaningful(
                item.name,
                item.label,
                item.title,
                item.feature,
                item.key,
              ),
            ),
          );
        }
      }
    } else if (isPlainObject(bucket)) {
      for (const [key, value] of Object.entries(bucket)) {
        if (typeof value === "boolean") {
          if (value) features.push(displayName(key));
        } else if (typeof value === "string" || typeof value === "number") {
          features.push(displayName(`${key} ${value}`));
        } else if (Array.isArray(value)) {
          features.push(displayName(key));

          value.forEach((item) => {
            if (typeof item === "string") {
              features.push(displayName(item));
            } else if (isPlainObject(item)) {
              features.push(displayName(firstMeaningful(item.name, item.label)));
            }
          });
        } else {
          features.push(displayName(key));
        }
      }
    } else if (typeof bucket === "string") {
      features.push(...bucket.split(/[,/|]+/).map(displayName));
    }
  }

  return unique(features.filter(Boolean));
};

export const normalizeVehicleRow = (row = {}) => {
  const model = displayName(
    firstMeaningful(
      row.model,
      row.modelName,
      row.model_name,
      row.vehicleModel,
      row.carModel,
      row.displayModel,
      row.rootModel,
    ),
  );

  const variant = displayName(
    firstMeaningful(
      row.variant_short,
      row.variantShort,
      row.variant_normalized,
      row.variant,
      row.variantName,
      row.variant_name,
      row.vehicleVariant,
      row.trim,
      row.name,
      row.title,
    ),
  );

  const brand = displayName(
    firstMeaningful(row.brand, row.make, row.makeName, row.manufacturer),
  );

  const fuelType = displayName(
    firstMeaningful(row.fuelType, row.fuel, row.fuel_type, row.engineFuel),
  );

  const transmission = displayName(
    firstMeaningful(row.transmission, row.gearbox, row.transmissionType),
  );

  const bodyType = displayName(
    firstMeaningful(row.bodyType, row.body_type, row.segment, row.category),
  );

  const exShowroomPrice = firstNumber(
    row.exShowroomPrice,
    row.ex_showroom_price,
    row.exShowroom,
    row.ex_showroom,
    row.exshowroom,
    row.price,
    row.basePrice,
  );

  const onRoadPrice = firstNumber(
    row.onRoadPrice,
    row.on_road_price,
    row.onRoad,
    row.on_road,
    row.finalPrice,
    row.totalPrice,
  );

  const rto = firstNumber(row.rto, row.rtoAmount, row.roadTax, row.road_tax);

  const insurance = firstNumber(
    row.insurance,
    row.insuranceAmount,
    row.insuranceCost,
  );

  const tcs = firstNumber(row.tcs, row.tcsAmount);
  const handling = firstNumber(row.handling, row.handlingCharges);
  const fastag = firstNumber(row.fastag, row.fastTag);
  const accessories = firstNumber(row.accessories, row.optionalAccessories);

  const colors = normalizeColors(row);
  const features = normalizeFeatures(row);

  const discontinued =
    Boolean(row.discontinued) ||
    Boolean(row.isDiscontinued) ||
    row.status === "discontinued" ||
    row.active === false ||
    row.isActive === false;
  const sourceImageUrl = firstMeaningful(
    row.sourceImageUrl,
    row.image_url,
    row.imageUrl,
    row.car_image_url,
  );
  const normalizedImageUrl = firstMeaningful(
    row.normalizedImageUrl,
    row.cleanImageUrl,
    row.normalized_image_url,
    row.clean_image_url,
    row.normalizedImagePngUrl,
  );
  const imageUrl = firstMeaningful(normalizedImageUrl, sourceImageUrl);

  return compactObject({
    id: String(row._id || row.id || ""),
    brand,
    make: brand,
    model,
    variant,
    fuelType,
    transmission,
    bodyType,
    exShowroomPrice,
    onRoadPrice,
    rto,
    insurance,
    tcs,
    handling,
    fastag,
    accessories,
    colors,
    features,
    variantShort: row.variant_short || row.variantShort || "",
    variantNormalized: row.variant_normalized || "",
    modelNormalized: row.model_normalized || "",
    brandNormalized: row.brand_normalized || "",
    sourceImageUrl,
    normalizedImageUrl,
    cleanImageUrl: normalizedImageUrl || "",
    imageUrl,
    image_url: sourceImageUrl || "",
    searchText: row.search_text || "",
    discontinued,
    active: !discontinued,
    raw: row,
  });
};
