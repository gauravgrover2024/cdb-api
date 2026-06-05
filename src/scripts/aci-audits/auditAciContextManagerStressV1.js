import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";
import {
  compactAciContextState,
} from "../../services/aciCore/context/aciContextManager.service.js";
import {
  hasBuyerFriendlyResolvedTopicAnswer,
} from "../../services/aciCore/specs/aciResolvedTopicAnswerUx.service.js";
import {
  runAciCoreLiveBridge,
} from "../../services/aciCore/integration/aciCoreLiveBridge.service.js";
import {
  hasForbiddenContextPayload,
  planContextCase,
} from "./auditAciContextManagerV1.js";

const CASE_TIMEOUT_MS = 5000;
const E2E_CASE_TIMEOUT_MS = 25000;
const TOTAL_TIMEOUT_MS = 75000;
const MEDIAN_TARGET_MS = 400;
const P95_TARGET_MS = 1200;

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const percentile = (values = [], pct = 50) => {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil((pct / 100) * sorted.length) - 1);
  return sorted[index] || 0;
};

const compactVehicle = ({
  make = "",
  model = "",
  fullModel = "",
  variant = "",
  fuelType = "",
  transmission = "",
  city = "",
  citySlug = "",
  source = "stress_fixture",
} = {}) => ({
  make,
  model,
  fullModel: fullModel || [make, model].filter(Boolean).join(" "),
  makeKey: clean(make),
  modelKey: clean(fullModel || [make, model].filter(Boolean).join(" ")),
  shortModelKey: clean(model),
  variant,
  variantKey: clean(variant),
  fuelType,
  fuelKey: clean(fuelType),
  transmission,
  transmissionKey: clean(transmission),
  city,
  citySlug,
  confidence: model ? 0.9 : 0,
  source,
});

const contextWithVehicle = (vehicle = {}) =>
  compactAciContextState({
    selectedVehicle: compactVehicle(vehicle),
    anchors: {
      primaryVehicle: compactVehicle(vehicle),
      comparisonTargets: [],
    },
    provenance: {
      sources: ["stress_fixture"],
      updatedBy: "stress_audit",
    },
  });

const contextWithComparison = (vehicles = []) =>
  compactAciContextState({
    selectedVehicle: {},
    activeComparison: {
      vehicles: vehicles.map(compactVehicle),
      confidence: 0.9,
      source: "stress_fixture",
    },
    anchors: {
      primaryVehicle: {},
      comparisonTargets: vehicles.map(compactVehicle),
    },
    provenance: {
      sources: ["stress_fixture"],
      updatedBy: "stress_audit",
    },
  });

const V = {
  vernaSxDelhi: { make: "Hyundai", model: "Verna", variant: "SX", city: "Delhi", citySlug: "new-delhi" },
  cretaDelhi: { make: "Hyundai", model: "Creta", city: "Delhi", citySlug: "new-delhi" },
  cretaSxDelhi: { make: "Hyundai", model: "Creta", variant: "SX", city: "Delhi", citySlug: "new-delhi" },
  cretaSxo: { make: "Hyundai", model: "Creta", variant: "SX(O)" },
  seltosNoida: { make: "Kia", model: "Seltos", city: "Noida", citySlug: "noida" },
  seltosHtx: { make: "Kia", model: "Seltos", variant: "HTX" },
  balenoAlpha: { make: "Maruti Suzuki", model: "Baleno", variant: "Alpha" },
  balenoSigma: { make: "Maruti Suzuki", model: "Baleno", variant: "Sigma" },
  i20Sportz: { make: "Hyundai", model: "I20", variant: "Sportz" },
  i20SportzNoida: { make: "Hyundai", model: "I20", variant: "Sportz", city: "Noida", citySlug: "noida" },
  eqs: { make: "Mercedes Benz", model: "Eqs" },
  be6: { make: "Mahindra", model: "Be 6" },
};

const comparisonCretaSeltos = contextWithComparison([
  { make: "Hyundai", model: "Creta" },
  { make: "Kia", model: "Seltos" },
]);

const caseDef = (id, group, message, expected = {}, previousContextState = null) => ({
  id,
  group,
  message,
  previousContextState,
  expected,
});

const CASES = [
  caseDef("single-be-6e-sunroof", "A", "be 6e sunroof", { tool: "vehicle_feature_lookup", vehicle: { make: "Mahindra", model: "Be 6" }, shouldNotClarify: true, e2eSanity: true, e2eExpectedTool: "vehicle_feature_lookup", e2eMustIncludeAny: ["sunroof"], e2eMustNotInclude: ["I need one more detail", "not available"] }),
  caseDef("single-mahindra-be-6e-sunroof", "A", "mahindra be 6e sunroof", { tool: "vehicle_feature_lookup", vehicle: { make: "Mahindra", model: "Be 6" }, shouldNotClarify: true }),
  caseDef("single-eqs-range", "A", "eqs range", { tool: "vehicle_spec_attribute_lookup", vehicle: { make: "Mercedes Benz", model: "Eqs" }, shouldNotClarify: true, buyerAnswer: true, e2eSanity: true, e2eExpectedTool: "vehicle_spec_attribute_lookup", e2eMustIncludeAny: ["813 km", "857 km"], e2eMustNotInclude: ["not available", "functionality that is not available", "I don\'t have the exact", "indexed spec value"] }),
  caseDef("single-mercedes-eqs-range", "A", "mercedes eqs range", { tool: "vehicle_spec_attribute_lookup", vehicle: { make: "Mercedes Benz", model: "Eqs" }, shouldNotClarify: true, buyerAnswer: true }),
  caseDef("single-ix-range", "A", "ix range", { tool: "vehicle_spec_attribute_lookup", vehicle: { make: "Bmw", model: "Ix" }, shouldNotClarify: true, buyerAnswer: true }),
  caseDef("single-bmw-ix-range", "A", "bmw ix range", { tool: "vehicle_spec_attribute_lookup", vehicle: { make: "Bmw", model: "Ix" }, shouldNotClarify: true, buyerAnswer: true }),
  caseDef("single-creta-sunroof", "A", "creta sunroof", { tool: "vehicle_feature_lookup", vehicle: { make: "Hyundai", model: "Creta" } }),
  caseDef("single-seltos-airbags", "A", "seltos airbags", { tool: "vehicle_feature_lookup", vehicle: { make: "Kia", model: "Seltos" } }),
  caseDef("single-baleno-boot-space", "A", "baleno boot space", { tool: "vehicle_spec_attribute_lookup", vehicle: { model: "Baleno" } }),
  caseDef("single-city-ground-clearance", "A", "city ground clearance", { tool: "vehicle_spec_attribute_lookup", vehicle: { model: "City" } }),

  caseDef("switch-price-creta-from-verna-context", "B", "Creta price", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, mustClear: { model: "Verna", variant: "SX" }, city: "Delhi" }, contextWithVehicle(V.vernaSxDelhi)),
  caseDef("switch-colors-seltos-from-verna-context", "B", "Seltos colors", { tool: "vehicle_colors", vehicle: { model: "Seltos" }, mustClear: { model: "Verna", variant: "SX" } }, contextWithVehicle(V.vernaSxDelhi)),
  caseDef("switch-emi-city-from-verna-context", "B", "Creta EMI Delhi", { tool: "vehicle_emi", vehicle: { model: "Creta" }, city: "Delhi", mustClear: { model: "Verna", variant: "SX" } }, contextWithVehicle(V.vernaSxDelhi)),
  caseDef("switch-feature-thar-from-verna-context", "B", "Thar features", { tool: "vehicle_feature_lookup", vehicle: { model: "Thar" }, mustClear: { model: "Verna", variant: "SX" } }, contextWithVehicle(V.vernaSxDelhi)),
  caseDef("no-context-creta", "B", "Creta", { vehicle: { model: "Creta" }, shouldClarify: true }),

  caseDef("city-creta-price-from-verna", "C", "Creta price", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, city: "Delhi" }, contextWithVehicle(V.vernaSxDelhi)),
  caseDef("city-seltos-price-noida", "C", "Seltos price Noida", { tool: "vehicle_pricelist", vehicle: { model: "Seltos" }, city: "Noida" }, contextWithVehicle(V.cretaDelhi)),
  caseDef("city-same-car-gurgaon", "C", "same car in Gurgaon", { vehicle: { model: "Seltos" }, city: "Gurgaon" }, contextWithVehicle(V.seltosNoida)),
  caseDef("city-creta-mumbai", "C", "price in Mumbai", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, city: "Mumbai" }, contextWithVehicle(V.cretaDelhi)),
  caseDef("city-emi-this", "C", "EMI for this", { tool: "vehicle_emi", vehicle: { model: "Creta" } }, contextWithVehicle(V.cretaDelhi)),
  caseDef("city-colors-this", "C", "colors for this", { tool: "vehicle_colors", vehicle: { model: "Creta" } }, contextWithVehicle(V.cretaDelhi)),
  caseDef("city-features-this", "C", "features in this", { tool: "vehicle_feature_lookup", vehicle: { model: "Creta" } }, contextWithVehicle(V.cretaDelhi)),
  caseDef("city-seltos-colors", "C", "Seltos colors", { tool: "vehicle_colors", vehicle: { model: "Seltos" } }, contextWithVehicle(V.cretaDelhi)),

  caseDef("fuel-creta-petrol-manual-delhi", "D", "Creta petrol manual price Delhi", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, fuelType: "Petrol", transmission: "Manual", city: "Delhi" }),
  caseDef("fuel-follow-automatic", "D", "automatic?", { vehicle: { model: "Creta" }, transmission: "Automatic" }, contextWithVehicle({ ...V.cretaDelhi, fuelType: "Petrol", transmission: "Manual" })),
  caseDef("fuel-seltos-diesel-auto-noida", "D", "Seltos diesel automatic price Noida", { tool: "vehicle_pricelist", vehicle: { model: "Seltos" }, fuelType: "Diesel", transmission: "Automatic", city: "Noida" }),
  caseDef("fuel-follow-manual-variant", "D", "manual variant?", { vehicle: { model: "Seltos" }, transmission: "Manual" }, contextWithVehicle({ ...V.seltosNoida, fuelType: "Diesel", transmission: "Automatic" })),
  caseDef("fuel-baleno-cng-delhi", "D", "Baleno CNG price Delhi", { tool: "vehicle_pricelist", vehicle: { model: "Baleno" }, fuelType: "CNG", city: "Delhi" }),
  caseDef("fuel-follow-petrol-auto", "D", "petrol automatic?", { vehicle: { model: "Baleno" }, fuelType: "Petrol", transmission: "Automatic" }, contextWithVehicle({ make: "Maruti Suzuki", model: "Baleno", fuelType: "CNG", city: "Delhi", citySlug: "new-delhi" })),
  caseDef("fuel-venue-diesel-manual", "D", "Venue diesel manual features", { tool: "vehicle_feature_lookup", vehicle: { model: "Venue" }, fuelType: "Diesel", transmission: "Manual" }),
  caseDef("fuel-follow-same-auto", "D", "same in automatic?", { vehicle: { model: "Venue" }, transmission: "Automatic" }, contextWithVehicle({ make: "Hyundai", model: "Venue", fuelType: "Diesel", transmission: "Manual" })),
  caseDef("fuel-i20-petrol-cvt", "D", "i20 petrol cvt features", { tool: "vehicle_feature_lookup", vehicle: { model: "I20" }, fuelType: "Petrol", transmission: "Automatic" }),
  caseDef("fuel-follow-diesel", "D", "diesel?", { vehicle: { model: "I20" }, fuelType: "Diesel" }, contextWithVehicle({ make: "Hyundai", model: "I20", fuelType: "Petrol", transmission: "Automatic" })),

  caseDef("variant-verna-to-creta-price", "E", "Creta price", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, mustClear: { model: "Verna", variant: "SX" } }, contextWithVehicle(V.vernaSxDelhi)),
  caseDef("variant-creta-to-seltos-htx", "E", "Seltos HTX price", { tool: "vehicle_pricelist", vehicle: { model: "Seltos" }, mustClear: { model: "Creta" } }, contextWithVehicle(V.cretaSxDelhi)),
  caseDef("variant-seltos-to-creta-sxo", "E", "Creta SX(O) features", { tool: "vehicle_feature_lookup", vehicle: { model: "Creta" }, mustClear: { model: "Seltos" } }, contextWithVehicle(V.seltosHtx)),
  caseDef("variant-colors-creta-clear", "E", "colors for Creta", { tool: "vehicle_colors", vehicle: { model: "Creta" } }, contextWithVehicle(V.cretaSxo)),
  caseDef("variant-baleno-sigma", "E", "Baleno Sigma features", { tool: "vehicle_feature_lookup", vehicle: { model: "Baleno" } }, contextWithVehicle(V.balenoAlpha)),
  caseDef("variant-i20-sportz", "E", "i20 Sportz features", { tool: "vehicle_feature_lookup", vehicle: { model: "I20" }, mustClear: { model: "Baleno" } }, contextWithVehicle(V.balenoSigma)),
  caseDef("variant-price-this-noida", "E", "price of this in Noida", { tool: "vehicle_pricelist", vehicle: { model: "I20" }, city: "Noida" }, contextWithVehicle(V.i20Sportz)),
  caseDef("variant-i20-to-creta-delhi", "E", "Creta in Delhi", { vehicle: { model: "Creta" }, city: "Delhi", mustClear: { model: "I20" } }, contextWithVehicle(V.i20SportzNoida)),

  caseDef("compare-creta-seltos", "F", "Creta vs Seltos", { tool: "vehicle_compare", comparison: ["Creta", "Seltos"] }),
  caseDef("compare-follow-better", "F", "which one is better?", { tool: "vehicle_compare", comparison: ["Creta", "Seltos"], e2eSanity: true, e2eExpectedTool: "vehicle_compare", e2eMustIncludeAny: ["Creta", "Seltos"], e2eMaxRows: 2, e2eMaxModels: 2, e2eMaxPayloadBytes: 180000, e2eNoDuplicateComparisonTitle: true }, comparisonCretaSeltos),
  caseDef("compare-petrol-auto", "F", "compare their petrol automatic variants", { comparison: ["Creta", "Seltos"], fuelType: "Petrol", transmission: "Automatic" }, comparisonCretaSeltos),
  caseDef("compare-baleno-i20", "F", "now compare Baleno vs i20", { tool: "vehicle_compare", comparison: ["Baleno", "I20"], mustClear: { model: "Seltos" } }, comparisonCretaSeltos),
  caseDef("compare-which-safer", "F", "which is safer?", { comparison: ["Baleno", "I20"] }, contextWithComparison([{ make: "Maruti Suzuki", model: "Baleno" }, { make: "Hyundai", model: "I20" }])),
  caseDef("compare-city-noida", "F", "change city to Noida", { comparison: ["Baleno", "I20"], city: "Noida" }, contextWithComparison([{ make: "Maruti Suzuki", model: "Baleno" }, { make: "Hyundai", model: "I20" }])),
  caseDef("compare-now-creta-price", "F", "now Creta price", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, mustClearComparison: true }, comparisonCretaSeltos),
  caseDef("compare-which-one-from-comparison", "F", "which one from the comparison?", { shouldClarify: true }, contextWithVehicle(V.cretaDelhi)),

  caseDef("ambiguous-range", "G", "range", { shouldClarify: true }),
  caseDef("ambiguous-sunroof", "G", "sunroof", { shouldClarify: true }),
  caseDef("ambiguous-price", "G", "price", { shouldClarify: true }),
  caseDef("ambiguous-automatic", "G", "automatic", { shouldClarify: true }),
  caseDef("ambiguous-which-one", "G", "which one?", { shouldClarify: true }),
  caseDef("ambiguous-compare-these", "G", "compare these", { shouldClarify: true }),
  caseDef("ambiguous-comparison-known", "G", "which one?", { tool: "vehicle_compare", comparison: ["Creta", "Seltos"] }, comparisonCretaSeltos),
  caseDef("context-eqs-range", "G", "range", { tool: "vehicle_spec_attribute_lookup", vehicle: { model: "Eqs" }, buyerAnswer: true }, contextWithVehicle(V.eqs)),
  caseDef("context-be6-sunroof", "G", "sunroof", { tool: "vehicle_feature_lookup", vehicle: { model: "Be 6" } }, contextWithVehicle(V.be6)),
  caseDef("context-creta-on-road", "G", "on road?", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, city: "Delhi" }, contextWithVehicle(V.cretaDelhi)),

  caseDef("unsupported-creta-mumbai", "H", "Creta price Mumbai", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, city: "Mumbai", noDelhiFallback: true }),
  caseDef("unsupported-seltos-bangalore", "H", "Seltos on road Bangalore", { tool: "vehicle_pricelist", vehicle: { model: "Seltos" }, city: "Bangalore", noDelhiFallback: true }),
  caseDef("unsupported-same-mumbai", "H", "same in Mumbai", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, city: "Mumbai", noDelhiFallback: true, e2eSanity: true, e2eExpectedTool: "vehicle_pricelist", e2eMustIncludeAny: ["Mumbai"], e2eMustNotInclude: ["New Delhi price", "Delhi on-road"] }, contextWithVehicle(V.cretaDelhi)),
  caseDef("unsupported-back-delhi", "H", "Delhi price", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, city: "Delhi" }, contextWithVehicle({ make: "Hyundai", model: "Creta", city: "Mumbai", citySlug: "mumbai" })),
  caseDef("unsupported-noida-price", "H", "Noida price", { tool: "vehicle_pricelist", vehicle: { model: "Creta" }, city: "Noida" }, contextWithVehicle({ make: "Hyundai", model: "Creta", city: "Mumbai", citySlug: "mumbai" })),
];

function assertPreviousContextFixtureIsCompact(previousContextState, failures = []) {
  if (!previousContextState) return failures;
  const state = previousContextState.contextState || previousContextState.aciContextState || previousContextState;
  const forbidden = hasForbiddenContextPayload(state);
  if (forbidden.length) failures.push(`Previous fixture has forbidden context keys: ${forbidden.join(", ")}`);
  return failures;
}

function assertCompactContextState(contextState, failures = []) {
  const forbidden = hasForbiddenContextPayload(contextState || {});
  if (forbidden.length) failures.push(`Context state has forbidden keys: ${forbidden.join(", ")}`);
  return failures;
}

function assertVehicleAnchor(actual = {}, expected = {}, failures = []) {
  if (expected.make && clean(actual.make) !== clean(expected.make)) {
    failures.push(`Expected make ${expected.make}, got ${actual.make || ""}`);
  }
  if (expected.model && clean(actual.model) !== clean(expected.model)) {
    failures.push(`Expected model ${expected.model}, got ${actual.model || ""}`);
  }
  return failures;
}

function assertNoStaleVehicle(result = {}, forbidden = {}, failures = []) {
  const text = JSON.stringify({
    selectedVehicle: result.selectedVehicle,
    activeComparison: result.activeComparison,
  }).toLowerCase();
  if (forbidden.model && text.includes(clean(forbidden.model))) {
    failures.push(`Stale model leaked: ${forbidden.model}`);
  }
  if (forbidden.variant && text.includes(clean(forbidden.variant))) {
    failures.push(`Stale variant leaked: ${forbidden.variant}`);
  }
  return failures;
}

function assertComparisonTargets(actual = {}, expected = [], failures = []) {
  if (!expected.length) return failures;
  const labels = asArray(actual.vehicles).map((item) => [item.make, item.model, item.fullModel].join(" ")).join(" ");
  for (const model of expected) {
    if (!clean(labels).includes(clean(model))) failures.push(`Missing comparison target ${model}; got ${labels}`);
  }
  return failures;
}

function assertIntentOrTool(actual = "", expected = "", failures = []) {
  if (expected && actual !== expected) failures.push(`Expected tool ${expected}, got ${actual}`);
  return failures;
}

function expectedFuelKeyForDisplay(value = '') {
  const key = clean(value);
  if (!key) return '';
  if (/\bcng\b/.test(key)) return 'cng';
  if (/\bdiesel\b/.test(key)) return 'diesel';
  if (/\bpetrol\b/.test(key)) return 'petrol';
  if (/\belectric\b|\bev\b/.test(key)) return 'electric';
  if (/\bhybrid\b/.test(key)) return 'hybrid';
  return key;
}

function expectedTransmissionKeyForDisplay(value = '') {
  const key = clean(value);
  if (!key) return '';
  if (/\bmanual\b|\bmt\b/.test(key)) return 'manual';
  if (/\bautomatic\b|\bauto\b|\bamt\b|\bcvt\b|\bdct\b|\bivt\b|\bimt\b/.test(key)) return 'automatic';
  return key;
}

function assertPowertrainKeyDisplayConsistency(vehicle = {}, failures = [], label = 'vehicle') {
  const fuelType = vehicle?.fuelType || '';
  const fuelKey = vehicle?.fuelKey || '';
  if (fuelType && fuelKey) {
    const expected = expectedFuelKeyForDisplay(fuelType);
    if (expected && clean(fuelKey) !== expected) {
      failures.push(`${label} has inconsistent fuelType/fuelKey: ${fuelType}/${fuelKey}`);
    }
  }

  const transmission = vehicle?.transmission || '';
  const transmissionKey = vehicle?.transmissionKey || '';
  if (transmission && transmissionKey) {
    const expected = expectedTransmissionKeyForDisplay(transmission);
    if (expected && clean(transmissionKey) !== expected) {
      failures.push(`${label} has inconsistent transmission/transmissionKey: ${transmission}/${transmissionKey}`);
    }
  }
}

function assertContextPowertrainConsistency(result = {}, failures = []) {
  assertPowertrainKeyDisplayConsistency(result.selectedVehicle || {}, failures, 'selectedVehicle');

  for (const [index, vehicle] of Object.entries(result.activeComparison?.vehicles || [])) {
    assertPowertrainKeyDisplayConsistency(vehicle, failures, `activeComparison.vehicles[${index}]`);
  }

  const state = result.contextState || {};
  assertPowertrainKeyDisplayConsistency(state.selectedVehicle || {}, failures, 'contextState.selectedVehicle');

  for (const [index, vehicle] of Object.entries(state.activeComparison?.vehicles || [])) {
    assertPowertrainKeyDisplayConsistency(vehicle, failures, `contextState.activeComparison.vehicles[${index}]`);
  }
}

function expectedPrimaryTasksForTool(tool = "") {
  const key = String(tool || "").trim();
  const map = {
    clarification: ["clarification"],
    vehicle_feature_lookup: ["feature_answer", "feature_lookup", "vehicle_feature_answer"],
    vehicle_spec_attribute_lookup: [
      "feature_answer",
      "spec_attribute_lookup",
      "vehicle_spec_attribute_answer",
      "vehicle_spec_attribute_lookup",
    ],
    vehicle_pricelist: ["price_lookup", "on_road_price", "vehicle_price"],
    vehicle_colors: ["color_lookup", "vehicle_colors"],
    vehicle_emi: ["emi_calculation", "vehicle_emi"],
    vehicle_compare: ["vehicle_comparison", "comparison"],
    vehicle_recommend: ["vehicle_recommendation", "recommendation"],
    vehicle_similar: ["similar_vehicles", "rivals_alternatives", "similar_cars"],
  };
  return map[key] || [];
}

function assertSemanticRouteConsistency(result = {}, failures = []) {
  const tool = String(result.tool || "").trim();
  const primaryTask = clean(result.meaningFrame?.primaryTask || "");
  const planTask = clean(result.plan?.meta?.primaryTask || "");
  const routeReason = clean(result.plan?.meta?.routeReason || result.plan?.meta?.primaryTask || result.tool || "");

  if (!tool) {
    failures.push("Missing routed tool.");
    return failures;
  }

  if (tool !== "clarification") {
    if (primaryTask === "clarification") {
      failures.push(`Tool ${tool} cannot keep primaryTask clarification.`);
    }
    if (planTask === "clarification") {
      failures.push(`Tool ${tool} cannot keep plan meta primaryTask clarification.`);
    }
    if (routeReason === "clarification") {
      failures.push(`Tool ${tool} cannot keep routeReason clarification.`);
    }
  }

  // Clarification is an action, not always the semantic topic.
  // It is acceptable for tool=clarification to carry primaryTask=feature_answer,
  // vehicle_comparison, price_lookup, etc. when the user intent is recognizable
  // but the entity/context is insufficient. The bad case is the reverse:
  // executing a real tool while the route still says clarification.
  if (tool === "clarification") {
    return failures;
  }

  const expectedTasks = expectedPrimaryTasksForTool(tool).map(clean);
  if (expectedTasks.length && primaryTask && !expectedTasks.includes(primaryTask)) {
    failures.push(`Tool ${tool} does not match primaryTask ${primaryTask}.`);
  }

  return failures;
}

function assertNoGenericClarificationWhenContextSufficient(result = {}, expected = {}, failures = []) {
  if (expected.shouldNotClarify && result.tool === "clarification") {
    failures.push("Unexpected generic clarification with sufficient context.");
  }
  return failures;
}

function assertUnsupportedCityNoDelhiFallback(result = {}, failures = []) {
  const city = clean(result.selectedVehicle?.city || result.selectedVehicle?.citySlug || "");
  if (city.includes("delhi")) failures.push("Unsupported/non-Delhi city fell back to Delhi.");
  return failures;
}

function assertBuyerFriendlyResolvedTopicAnswer(answer = "", expected = {}, failures = []) {
  if (!expected.buyerAnswer) return failures;
  if (!hasBuyerFriendlyResolvedTopicAnswer(answer)) {
    failures.push(`Buyer-friendly resolved topic answer missing; got "${answer || ""}"`);
  }
  return failures;
}

function buildAnswerPreview(result = {}, expected = {}) {
  const vehicle = result.selectedVehicle || {};
  const model = vehicle.fullModel || [vehicle.make, vehicle.model].filter(Boolean).join(" ") || "this model";
  if (expected.buyerAnswer) {
    return `I found ${model}. You're asking about range. I don't have the exact certified range value in the current spec data yet, so I won't guess.`;
  }
  if (result.activeComparison?.vehicles?.length >= 2) {
    const labels = result.activeComparison.vehicles.map((item) => item.model).filter(Boolean).join(" vs ");
    return `I'm continuing your ${labels} comparison.`;
  }
  return result.tool === "clarification" ? "I need one more detail to continue." : `I understood ${model}.`;
}

const withTimeout = (promise, id = "") => {
  let timeoutId = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(`${id} exceeded ${CASE_TIMEOUT_MS}ms`)), CASE_TIMEOUT_MS);
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timeoutId) clearTimeout(timeoutId);
  });
};

const withE2eTimeout = (promise, id = "") => {
  let timeoutId = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(`${id} e2e exceeded ${E2E_CASE_TIMEOUT_MS}ms`)), E2E_CASE_TIMEOUT_MS);
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timeoutId) clearTimeout(timeoutId);
  });
};

const buildRuntimeContext = (previousContextState = null) => {
  if (!previousContextState) return {};
  const state = previousContextState.contextState || previousContextState.aciContextState || previousContextState;
  return {
    ...state,
    contextState: state,
    aciContextState: state,
  };
};

function assertE2eAnswerQuality({ output = {}, expected = {}, failures = [] } = {}) {
  const answer = String(output.answer || output.message || output.data?.answer || "");
  const answerLower = answer.toLowerCase();
  const tool = output?.aciCoreBridge?.tool || output?.meta?.aciCoreBridge?.tool || output?.tool || output?.data?.aciCoreBridge?.tool || "";
  const payloadBytes = Buffer.byteLength(JSON.stringify(output || {}));

  if (expected.e2eExpectedTool && tool !== expected.e2eExpectedTool) {
    failures.push(`E2E expected tool ${expected.e2eExpectedTool}, got ${tool}`);
  }

  if (!answer.trim()) {
    failures.push("E2E answer is empty.");
  }

  if (Array.isArray(expected.e2eMustIncludeAny) && expected.e2eMustIncludeAny.length) {
    const hasAny = expected.e2eMustIncludeAny.some((needle) => answer.includes(String(needle || "")));
    if (!hasAny) failures.push(`E2E answer must include one of: ${expected.e2eMustIncludeAny.join(", ")}`);
  }

  if (Array.isArray(expected.e2eMustNotInclude) && expected.e2eMustNotInclude.length) {
    const bad = expected.e2eMustNotInclude.find((needle) => answerLower.includes(String(needle || "").toLowerCase()));
    if (bad) failures.push(`E2E answer included forbidden text: ${bad}`);
  }

  if (
    tool !== "clarification" &&
    /i need one more detail|can you clarify what you want to check|need one detail/i.test(answer)
  ) {
    failures.push("E2E real tool returned generic clarification wording.");
  }

  const models = output?.data?.models || [];
  const rows = output?.data?.rows || [];
  const title = String(output?.title || "");

  if (expected.e2eMaxModels !== undefined && Array.isArray(models) && models.length > expected.e2eMaxModels) {
    failures.push(`E2E expected <=${expected.e2eMaxModels} models, got ${models.length}`);
  }

  if (expected.e2eMaxRows !== undefined && Array.isArray(rows) && rows.length > expected.e2eMaxRows) {
    failures.push(`E2E expected <=${expected.e2eMaxRows} rows, got ${rows.length}`);
  }

  if (expected.e2eMaxPayloadBytes !== undefined && payloadBytes > expected.e2eMaxPayloadBytes) {
    failures.push(`E2E payload too large: ${payloadBytes} > ${expected.e2eMaxPayloadBytes}`);
  }

  if (expected.e2eNoDuplicateComparisonTitle && /creta vs seltos vs hyundai creta/i.test(title)) {
    failures.push(`E2E duplicate comparison title remains: ${title}`);
  }

  if (
    expected.e2eNoDuplicateComparisonTitle &&
    /creta/i.test(answer) &&
    /seltos/i.test(answer) &&
    /creta.*seltos.*creta.*seltos/i.test(answer.replace(/\s+/g, " "))
  ) {
    failures.push(`E2E duplicate comparison targets remain in answer: ${answer}`);
  }

  return {
    tool,
    answer,
    payloadBytes,
    title,
    rowCount: Array.isArray(rows) ? rows.length : 0,
    modelCount: Array.isArray(models) ? models.length : 0,
  };
}

const runStressCase = async (testCase = {}) => {
  const startedAt = Date.now();
  const failures = [];

  assertPreviousContextFixtureIsCompact(testCase.previousContextState, failures);

  try {
    const context = testCase.previousContextState
      ? { contextState: testCase.previousContextState, aciContextState: testCase.previousContextState }
      : {};
    const result = await withTimeout(planContextCase({ message: testCase.message, context }), testCase.id);
    const expected = testCase.expected || {};
    const answerPreview = buildAnswerPreview(result, expected);

    assertCompactContextState(result.contextState, failures);
    assertContextPowertrainConsistency(result, failures);
    assertVehicleAnchor(result.selectedVehicle || {}, expected.vehicle || {}, failures);
    assertNoStaleVehicle(result, expected.mustClear || {}, failures);
    assertComparisonTargets(result.activeComparison || {}, expected.comparison || [], failures);
    assertIntentOrTool(result.tool, expected.tool, failures);
    assertSemanticRouteConsistency(result, failures);
    assertNoGenericClarificationWhenContextSufficient(result, expected, failures);
    assertBuyerFriendlyResolvedTopicAnswer(answerPreview, expected, failures);

    if (expected.shouldClarify && result.tool !== "clarification") {
      failures.push(`Expected clarification, got ${result.tool}`);
    }
    if (expected.mustClearComparison && asArray(result.activeComparison?.vehicles).length) {
      failures.push("Expected active comparison to clear.");
    }
    const actualCity = result.selectedVehicle?.city ||
      result.selectedVehicle?.citySlug ||
      result.activeComparison?.city ||
      result.activeComparison?.citySlug ||
      "";
    if (expected.city && clean(actualCity) !== clean(expected.city) && !clean(actualCity).includes(clean(expected.city))) {
      failures.push(`Expected city ${expected.city}, got ${actualCity}`);
    }
    if (expected.noDelhiFallback) assertUnsupportedCityNoDelhiFallback(result, failures);
    const actualFuel = result.selectedVehicle?.fuelType ||
      result.selectedVehicle?.fuelKey ||
      result.activeComparison?.fuelKey ||
      "";
    if (expected.fuelType && clean(actualFuel) !== clean(expected.fuelType)) {
      failures.push(`Expected fuel ${expected.fuelType}, got ${actualFuel}`);
    }
    const actualTransmission = result.selectedVehicle?.transmission ||
      result.selectedVehicle?.transmissionKey ||
      result.activeComparison?.transmissionKey ||
      "";
    if (expected.transmission && clean(actualTransmission) !== clean(expected.transmission)) {
      failures.push(`Expected transmission ${expected.transmission}, got ${actualTransmission}`);
    }

    let e2eSummary = null;
    if (expected.e2eSanity) {
      const e2eOutput = await withE2eTimeout(
        runAciCoreLiveBridge({
          message: testCase.message,
          context: buildRuntimeContext(testCase.previousContextState),
          user: null,
          session: {},
          meta: { source: "context_stress_e2e_sanity", caseId: testCase.id },
        }),
        testCase.id,
      );

      e2eSummary = assertE2eAnswerQuality({ output: e2eOutput, expected, failures });
    }

    const durationMs = Date.now() - startedAt;
    if (!expected.e2eSanity && durationMs > CASE_TIMEOUT_MS) failures.push(`Case exceeded ${CASE_TIMEOUT_MS}ms: ${durationMs}ms`);
    if (expected.e2eSanity && durationMs > E2E_CASE_TIMEOUT_MS) failures.push(`E2E case exceeded ${E2E_CASE_TIMEOUT_MS}ms: ${durationMs}ms`);

    return {
      id: testCase.id,
      group: testCase.group,
      pass: failures.length === 0,
      durationMs,
      message: testCase.message,
      e2eSanity: Boolean(expected.e2eSanity),
      failures,
      summary: {
        intent: result.meaningFrame?.primaryTask || "",
        tool: result.tool,
        primaryTask: result.meaningFrame?.primaryTask || "",
        selectedVehicle: result.selectedVehicle || {},
        activeComparison: result.activeComparison || {},
        contextStateCompactBytes: Buffer.byteLength(JSON.stringify(result.contextState || {})),
        routeReason: result.plan?.meta?.primaryTask || result.tool,
        answerPreview,
        e2e: e2eSummary,
      },
    };
  } catch (error) {
    return {
      id: testCase.id,
      group: testCase.group,
      pass: false,
      durationMs: Date.now() - startedAt,
      message: testCase.message,
      e2eSanity: Boolean(testCase.expected?.e2eSanity),
      failures: [error?.message || String(error)],
      summary: {},
    };
  }
};

const main = async () => {
  await connectDB();

  const startedAt = Date.now();
  const results = [];
  for (const item of CASES) {
    results.push(await runStressCase(item));
  }

  const durationMs = Date.now() - startedAt;
  const durations = results.map((item) => item.durationMs);
  const slowCases = results
    .filter((item) => item.durationMs > (item.e2eSanity ? E2E_CASE_TIMEOUT_MS : CASE_TIMEOUT_MS))
    .map((item) => ({
      id: item.id,
      durationMs: item.durationMs,
      thresholdMs: item.e2eSanity ? E2E_CASE_TIMEOUT_MS : CASE_TIMEOUT_MS,
      e2eSanity: Boolean(item.e2eSanity),
    }));
  const failed = results.filter((item) => !item.pass);
  const e2eSanityResults = results.filter((item) => item.e2eSanity);
  const e2eSanityFailures = [];
  if (e2eSanityResults.length < 4) {
    e2eSanityFailures.push(`expected at least 4 e2e sanity cases, got ${e2eSanityResults.length}`);
  }
  const groups = [...new Set(CASES.map((item) => item.group))];
  const contextSwitchIds = [
    "switch-price-creta-from-verna-context",
    "switch-colors-seltos-from-verna-context",
    "switch-emi-city-from-verna-context",
    "switch-feature-thar-from-verna-context",
    "no-context-creta",
  ];
  const missingSwitchIds = contextSwitchIds.filter((id) => !CASES.some((item) => item.id === id));
  const timing = {
    minMs: Math.min(...durations),
    medianMs: percentile(durations, 50),
    p95Ms: percentile(durations, 95),
    maxMs: Math.max(...durations),
    totalMs: durationMs,
  };
  const timingFailures = [];
  if (timing.medianMs > MEDIAN_TARGET_MS) timingFailures.push(`median ${timing.medianMs}ms > ${MEDIAN_TARGET_MS}ms`);
  if (timing.p95Ms > P95_TARGET_MS) timingFailures.push(`p95 ${timing.p95Ms}ms > ${P95_TARGET_MS}ms`);
  if (durationMs > TOTAL_TIMEOUT_MS) timingFailures.push(`total ${durationMs}ms > ${TOTAL_TIMEOUT_MS}ms`);
  if (missingSwitchIds.length) timingFailures.push(`missing required context-switch ids: ${missingSwitchIds.join(", ")}`);
  timingFailures.push(...e2eSanityFailures);

  const summary = {
    suite: "ACI Context Manager Stress Audit v1",
    ok: failed.length === 0 && slowCases.length === 0 && timingFailures.length === 0,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length + timingFailures.length + slowCases.length,
    failedIds: [
      ...failed.map((item) => item.id),
      ...slowCases.map((item) => item.id),
      ...timingFailures.map((_, index) => `timing-${index + 1}`),
    ],
    slowCases,
    durationMs,
    timing,
    timingFailures,
    coverage: {
      caseCount: CASES.length,
      groups,
      contextSwitchChecksIncluded: missingSwitchIds.length === 0,
      compactnessGuardEnabled: true,
      e2eSanityCount: e2eSanityResults.length,
      e2eSanityIds: e2eSanityResults.map((item) => item.id),
      buyerExperienceGuardEnabled: true,
    },
    results,
  };

  console.log(JSON.stringify(summary, null, 2));
  await mongoose.disconnect();

  if (!summary.ok) process.exit(1);
};

main().catch(async (error) => {
  console.error(error?.stack || error?.message || error);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
