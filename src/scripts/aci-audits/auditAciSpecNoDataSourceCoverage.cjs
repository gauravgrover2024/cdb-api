#!/usr/bin/env node

require("dotenv").config();

const mongoose = require("mongoose");

const CASES = [
  {
    id: "D71",
    message: "baleno mileage",
    brand: "Maruti",
    model: "Baleno",
    attributeKey: "mileage",
  },
  {
    id: "D72",
    message: "scorpio n mileage",
    brand: "Mahindra",
    model: "Scorpio N",
    attributeKey: "mileage",
  },
  {
    id: "D76",
    message: "creta ground clearance",
    brand: "Hyundai",
    model: "Creta",
    attributeKey: "ground_clearance",
  },
  {
    id: "D78",
    message: "verna power",
    brand: "Hyundai",
    model: "Verna",
    attributeKey: "power",
  },
  {
    id: "D82",
    message: "boot space",
    brand: "Hyundai",
    model: "Creta",
    attributeKey: "boot_space",
    context: "selectedVehicle=Hyundai Creta",
  },
  {
    id: "J167",
    message: "iska mileage kya hai",
    brand: "Maruti",
    model: "Baleno",
    attributeKey: "mileage",
    context: "selectedVehicle=Maruti Baleno",
  },
];

const ATTRIBUTE_DEFINITIONS = {
  mileage: {
    key: "mileage",
    label: "mileage",
    fields: ["mileage", "fuelEfficiency", "araiMileage"],
    relatedPatterns: [/mileage/i, /\barai\b/i, /fuel.*efficiency/i],
    sourcePatterns: [/mileage/i, /\barai\b/i, /fuel.*efficiency/i],
  },
  ground_clearance: {
    key: "ground_clearance",
    label: "ground clearance",
    fields: ["groundClearance"],
    relatedPatterns: [/ground.*clearance/i, /clearance.*unladen/i, /reported.*ground/i],
    sourcePatterns: [/ground.*clearance/i, /clearance.*unladen/i, /reported.*ground/i],
  },
  power: {
    key: "power",
    label: "power",
    fields: ["power", "maxPower", "maximumPower", "bhp"],
    relatedPatterns: [/max[_\s-]*power/i, /maximum.*power/i, /\bbhp\b/i],
    sourcePatterns: [/max\s*power/i, /maximum.*power/i, /\bbhp\b/i],
  },
  boot_space: {
    key: "boot_space",
    label: "boot space",
    fields: ["bootSpace", "bootCapacity", "luggageSpace"],
    relatedPatterns: [/boot.*space/i, /boot.*capacity/i, /luggage/i, /reported.*boot/i],
    sourcePatterns: [/boot.*space/i, /boot.*capacity/i, /luggage/i, /reported.*boot/i],
  },
};

const text = (value = "") => String(value || "").replace(/\s+/g, " ").trim();

const escapeRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const rxExact = (value = "") => new RegExp(`^${escapeRegex(text(value))}$`, "i");
const rxContains = (value = "") => new RegExp(escapeRegex(text(value)), "i");

const normalize = (value = "") =>
  text(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const slugify = (value = "") => normalize(value).replace(/\s+/g, "-");

const unique = (items = []) => [...new Set(items.filter(Boolean))];

const firstMeaningful = (...values) =>
  values.find((value) => value !== undefined && value !== null && text(value) !== "") || "";

const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      if (value && typeof value === "object") return Object.keys(value).length > 0;
      return value !== undefined && value !== null && value !== "";
    }),
  );

const getNestedValue = (object = {}, path = "") => {
  const direct = object?.[path];
  if (direct !== undefined && direct !== null && direct !== "") return direct;

  return path.split(".").reduce((current, part) => {
    if (current === undefined || current === null) return undefined;
    return current[part];
  }, object);
};

const valueFromFeatureCell = (cell = {}) => {
  if (!cell || typeof cell !== "object") return "";
  if (cell.available === false || cell.availabilityStatus === "not_available") return "";
  return firstMeaningful(cell.value, cell.displayValue, cell.text, cell.label);
};

const collectDirectValues = ({ row = {}, definition = {}, source = "" } = {}) => {
  const candidates = [];
  for (const field of definition.fields || []) {
    candidates.push([field, getNestedValue(row, field)]);
    candidates.push([`specs.${field}`, getNestedValue(row, `specs.${field}`)]);
    candidates.push([`specifications.${field}`, getNestedValue(row, `specifications.${field}`)]);
    candidates.push([`attributes.${field}`, getNestedValue(row, `attributes.${field}`)]);
  }

  return candidates
    .flatMap(([field, value]) => {
      if (Array.isArray(value)) return value.map((item) => [field, item]);
      return [[field, value]];
    })
    .map(([field, value]) => {
      if (value === undefined || value === null || value === "") return null;
      if (typeof value === "object") {
        const displayValue = firstMeaningful(value.value, value.displayValue, value.text, value.label);
        const unit = firstMeaningful(value.unit, value.units);
        return displayValue
          ? compactObject({
              field,
              value: unit ? `${displayValue} ${unit}` : String(displayValue),
              source,
            })
          : null;
      }
      return compactObject({ field, value: String(value), source });
    })
    .filter(Boolean);
};

const collectFeatureMapMatches = ({
  features = {},
  patterns = [],
  source = "",
  variant = "",
  exactKey = "",
} = {}) => {
  const entries = Object.entries(features || {});

  return entries
    .filter(([key, cell]) => {
      const value = typeof cell === "object"
        ? [cell.displayName, cell.value, cell.text, cell.label].map(text).join(" ")
        : text(cell);
      const haystack = `${key} ${value}`;
      if (exactKey && normalize(key) === normalize(exactKey)) return true;
      return patterns.some((pattern) => pattern.test(haystack));
    })
    .map(([key, cell]) => {
      const value = typeof cell === "object"
        ? valueFromFeatureCell(cell) || firstMeaningful(cell.displayName, cell.value, cell.text, cell.label)
        : cell;
      return compactObject({
        field: key,
        value: text(value),
        displayName: typeof cell === "object" ? text(cell.displayName) : "",
        variant,
        source,
      });
    })
    .filter((item) => item.value || item.displayName || item.field);
};

const dedupeSamples = (items = []) => {
  const seen = new Set();
  const out = [];

  for (const item of items) {
    const key = [item.source, item.variant, item.field, item.value].join("|").toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }

  return out;
};

const vehiclesQueryForCase = ({ brand = "", model = "" } = {}) => ({
  brand: rxExact(brand),
  model: rxContains(model),
});

const sourceFeatureQueryForCase = ({ brand = "", model = "" } = {}) => ({
  brand: rxExact(brand),
  model: rxExact(model),
});

const summaryQueryForCase = ({ brand = "", model = "" } = {}) => {
  const fullModel = [brand, model].filter(Boolean).join(" ");
  const modelKey = slugify(model);
  const brandModelKey = slugify(fullModel);

  return {
    $or: [
      { fullModel: rxExact(fullModel) },
      {
        $and: [
          { $or: [{ make: rxExact(brand) }, { brand: rxExact(brand) }] },
          { model: rxExact(model) },
        ],
      },
      { modelKey: rxExact(modelKey) },
      { shortModelKey: rxExact(modelKey) },
      { modelKey: rxExact(brandModelKey) },
    ],
  };
};

const matrixQueryForCase = ({ brand = "", model = "" } = {}) => ({
  brand: rxExact(brand),
  model: rxExact(model),
});

const activeVehicleSignal = (rows = []) => {
  const activeRows = rows.filter(
    (row = {}) =>
      row.is_discontinued !== true &&
      !/discontinued|inactive/i.test(text(row.lifecycleStatus)),
  );
  const discontinuedRows = rows.filter(
    (row = {}) => row.is_discontinued === true || /discontinued/i.test(text(row.lifecycleStatus)),
  );
  const inactiveRows = rows.filter((row = {}) => /inactive/i.test(text(row.lifecycleStatus)));
  const latestSeen =
    rows
      .map((row = {}) => text(row.LastSeenDate || row.lastSeenAt || row.updatedAt))
      .filter(Boolean)
      .sort()
      .pop() || "";

  return {
    status: activeRows.length
      ? "active"
      : discontinuedRows.length
        ? "discontinued"
        : inactiveRows.length
          ? "inactive"
          : rows.length
            ? "unknown"
            : "missing",
    activeRows: activeRows.length,
    discontinuedRows: discontinuedRows.length,
    inactiveRows: inactiveRows.length,
    totalRows: rows.length,
    latestSeen,
    sample: rows.slice(0, 5).map((row = {}) => ({
      brand: row.brand || row.make || "",
      model: row.model || "",
      variant: row.variantDisplayName || row.variant || "",
      city: row.city || row.cityName || "",
      isDiscontinued: Boolean(row.is_discontinued),
      lastSeen: row.LastSeenDate || row.lastSeenAt || "",
    })),
  };
};

const inspectCase = async ({ db, testCase }) => {
  const definition = ATTRIBUTE_DEFINITIONS[testCase.attributeKey];
  if (!definition) throw new Error(`Unknown attribute ${testCase.attributeKey}`);

  const vehicleRows = await db.collection("vehicles")
    .find(vehiclesQueryForCase(testCase), {
      projection: {
        brand: 1,
        make: 1,
        model: 1,
        variant: 1,
        variantDisplayName: 1,
        variantShortName: 1,
        city: 1,
        cityName: 1,
        is_discontinued: 1,
        LastSeenDate: 1,
        lastSeenAt: 1,
        lifecycleStatus: 1,
        updatedAt: 1,
        specs: 1,
        specifications: 1,
        attributes: 1,
        mileage: 1,
        fuelEfficiency: 1,
        araiMileage: 1,
        groundClearance: 1,
        bootSpace: 1,
        bootCapacity: 1,
        luggageSpace: 1,
        power: 1,
        maxPower: 1,
        maximumPower: 1,
        bhp: 1,
      },
    })
    .limit(120)
    .toArray();

  const sourceFeatureRows = await db.collection("vehicle_features")
    .find(sourceFeatureQueryForCase(testCase), {
      projection: {
        brand: 1,
        model: 1,
        variant: 1,
        features: 1,
        updatedAt: 1,
        scrape_timestamp: 1,
      },
    })
    .limit(30)
    .toArray();

  const summaryRows = await db.collection("aci_vehicle_model_summary")
    .find(summaryQueryForCase(testCase), {
      projection: {
        make: 1,
        brand: 1,
        model: 1,
        fullModel: 1,
        modelKey: 1,
        shortModelKey: 1,
        specs: 1,
        specifications: 1,
        attributes: 1,
        ...Object.fromEntries(definition.fields.map((field) => [field, 1])),
      },
    })
    .limit(20)
    .toArray();

  const matrixRows = await db.collection("vehicle_variant_feature_matrix_v2")
    .find(matrixQueryForCase(testCase), {
      projection: {
        brand: 1,
        make: 1,
        model: 1,
        modelKey: 1,
        brandModelKey: 1,
        variant: 1,
        variantFull: 1,
        variantName: 1,
        variantKey: 1,
        lifecycleStatus: 1,
        activeForFeatureExplorer: 1,
        featuresByKey: 1,
        decisionSignals: 1,
      },
    })
    .limit(120)
    .toArray();

  const vehicleSignal = activeVehicleSignal(vehicleRows);

  const sourceVehicleValues = dedupeSamples(
    vehicleRows.flatMap((row) =>
      collectDirectValues({
        row,
        definition,
        source: "vehicles",
      }).map((item) => ({
        ...item,
        variant: row.variantDisplayName || row.variant || "",
      })),
    ),
  );

  const sourceFeatureValues = dedupeSamples(
    sourceFeatureRows.flatMap((row) =>
      collectFeatureMapMatches({
        features: row.features || {},
        patterns: definition.sourcePatterns,
        source: "vehicle_features",
        variant: row.variant || "",
      }),
    ),
  );

  const summaryExactValues = dedupeSamples(
    summaryRows.flatMap((row) =>
      collectDirectValues({
        row,
        definition,
        source: "aci_vehicle_model_summary",
      }).map((item) => ({
        ...item,
        variant: row.fullModel || row.model || "",
      })),
    ),
  );

  const matrixExactValues = dedupeSamples(
    matrixRows.flatMap((row) =>
      collectFeatureMapMatches({
        features: row.featuresByKey || {},
        patterns: [],
        exactKey: definition.key,
        source: "vehicle_variant_feature_matrix_v2.featuresByKey",
        variant: row.variantFull || row.variant || "",
      }).concat(
        collectFeatureMapMatches({
          features: row.decisionSignals?.featuresByKey || {},
          patterns: [],
          exactKey: definition.key,
          source: "vehicle_variant_feature_matrix_v2.decisionSignals",
          variant: row.variantFull || row.variant || "",
        }),
      ),
    ),
  );

  const matrixRelatedValues = dedupeSamples(
    matrixRows.flatMap((row) =>
      collectFeatureMapMatches({
        features: row.featuresByKey || {},
        patterns: definition.relatedPatterns,
        source: "vehicle_variant_feature_matrix_v2.featuresByKey",
        variant: row.variantFull || row.variant || "",
      }).concat(
        collectFeatureMapMatches({
          features: row.decisionSignals?.featuresByKey || {},
          patterns: definition.relatedPatterns,
          source: "vehicle_variant_feature_matrix_v2.decisionSignals",
          variant: row.variantFull || row.variant || "",
        }),
      ),
    ),
  );

  const sourceHasData = sourceVehicleValues.length > 0 || sourceFeatureValues.length > 0;
  const runtimeExactHasData = summaryExactValues.length > 0 || matrixExactValues.length > 0;
  const readModelHasRelatedData = runtimeExactHasData || matrixRelatedValues.length > 0;

  const classification = (() => {
    if (vehicleSignal.status !== "active") return "source_missing_or_inactive";
    if (!sourceHasData && !readModelHasRelatedData) return "source_missing_or_inactive";
    if (sourceHasData && !readModelHasRelatedData) return "source_has_data_read_model_missing";
    if (readModelHasRelatedData) return "read_model_has_data_tool_gap";
    if (!testCase.model || !testCase.attributeKey) return "audit_fixture_replace";
    return "needs_manual_review";
  })();

  return {
    id: testCase.id,
    message: testCase.message,
    model: testCase.model,
    brand: testCase.brand,
    requestedAttribute: definition.label,
    attributeKey: definition.key,
    context: testCase.context || "",
    activeDiscontinuedSignal: vehicleSignal,
    source: {
      vehiclesRows: vehicleRows.length,
      vehicleFeaturesRows: sourceFeatureRows.length,
      sourceValueCount: sourceVehicleValues.length + sourceFeatureValues.length,
      vehiclesValueSample: sourceVehicleValues.slice(0, 8),
      vehicleFeaturesValueSample: sourceFeatureValues.slice(0, 8),
    },
    derived: {
      aciVehicleModelSummaryRows: summaryRows.length,
      vehicleVariantFeatureMatrixRows: matrixRows.length,
      runtimeExactValueCount: summaryExactValues.length + matrixExactValues.length,
      relatedReadModelValueCount: matrixRelatedValues.length,
      summaryExactSample: summaryExactValues.slice(0, 8),
      matrixExactSample: matrixExactValues.slice(0, 8),
      matrixRelatedSample: matrixRelatedValues.slice(0, 10),
    },
    runtimeRelatedCollectionCounts: {
      aci_vehicle_model_summary: summaryRows.length,
      vehicle_variant_feature_matrix_v2: matrixRows.length,
    },
    classification,
  };
};

const printResult = (result = {}) => {
  console.log(`\n[${result.id}] ${result.message}`);
  console.log(`model=${result.brand} ${result.model} attribute=${result.requestedAttribute} context=${result.context || "none"}`);
  console.log(
    `vehicleStatus=${result.activeDiscontinuedSignal.status} activeRows=${result.activeDiscontinuedSignal.activeRows} discontinuedRows=${result.activeDiscontinuedSignal.discontinuedRows} inactiveRows=${result.activeDiscontinuedSignal.inactiveRows} totalVehicleRows=${result.activeDiscontinuedSignal.totalRows} latestSeen=${result.activeDiscontinuedSignal.latestSeen || "n/a"}`,
  );
  console.log(`vehicleSample=${JSON.stringify(result.activeDiscontinuedSignal.sample)}`);
  console.log(`sourceCounts=${JSON.stringify({
    vehiclesRows: result.source.vehiclesRows,
    vehicleFeaturesRows: result.source.vehicleFeaturesRows,
    sourceValueCount: result.source.sourceValueCount,
  })}`);
  console.log(`sourceSample=${JSON.stringify({
    vehicles: result.source.vehiclesValueSample,
    vehicle_features: result.source.vehicleFeaturesValueSample,
  })}`);
  console.log(`derivedCounts=${JSON.stringify({
    aciVehicleModelSummaryRows: result.derived.aciVehicleModelSummaryRows,
    vehicleVariantFeatureMatrixRows: result.derived.vehicleVariantFeatureMatrixRows,
    runtimeExactValueCount: result.derived.runtimeExactValueCount,
    relatedReadModelValueCount: result.derived.relatedReadModelValueCount,
  })}`);
  console.log(`derivedSample=${JSON.stringify({
    summaryExact: result.derived.summaryExactSample,
    matrixExact: result.derived.matrixExactSample,
    matrixRelated: result.derived.matrixRelatedSample,
  })}`);
  console.log(`runtimeRelatedCollectionCounts=${JSON.stringify(result.runtimeRelatedCollectionCounts)}`);
  console.log(`classification=${result.classification}`);
};

const main = async () => {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  if (typeof connectDB !== "function") {
    throw new Error("connectDB export not found");
  }

  await connectDB();
  const db = mongoose.connection.db;
  const results = [];

  for (const testCase of CASES) {
    const result = await inspectCase({ db, testCase });
    results.push(result);
    printResult(result);
  }

  const counts = results.reduce((acc, result) => {
    acc[result.classification] = (acc[result.classification] || 0) + 1;
    return acc;
  }, {});

  console.log(`\nclassificationSummary=${JSON.stringify(counts)}`);
};

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (mongoose.connection?.readyState) {
      await mongoose.disconnect();
    }
  });
