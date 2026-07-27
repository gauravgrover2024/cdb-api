#!/usr/bin/env node
"use strict";

require("dotenv").config();

const assert = require("assert");
const mongoose = require("mongoose");

const clean = (value = "") => String(value || "").replace(/\s+/g, " ").trim();
const lower = (value = "") => clean(value).toLowerCase();
const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const mongoUri = () =>
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.MONGODB_URL ||
  process.env.MONGO_URL ||
  process.env.DATABASE_URL ||
  "";

const vehicleLabel = (vehicle = {}) =>
  lower([vehicle.make || vehicle.brand, vehicle.model || vehicle.fullModel, vehicle.variant]
    .filter(Boolean)
    .join(" "));

const comparisonVehicles = (response = {}) =>
  asArray(
    response.contextPatch?.activeComparison?.vehicles ||
      response.contextPatch?.selectedComparisonSet?.vehicles ||
      response.contextPatch?.contextState?.activeComparison?.vehicles,
  );

const comparisonRows = (response = {}) =>
  asArray(response.data?.rows || response.rows);

const selectedVehicle = (response = {}) =>
  response.contextPatch?.selectedVehicle ||
  response.contextPatch?.contextState?.selectedVehicle ||
  response.selectedVehicle ||
  response.vehicle ||
  {};

const compactStateForFrontendTransport = (state = {}) => ({
  schemaVersion: state.schemaVersion || "aci_context_state_v1",
  selectedVehicle: state.selectedVehicle || {},
  activeComparison: state.activeComparison || {},
  requested: state.requested || {},
  buyerContext: state.buyerContext || {},
  contextLedger: state.contextLedger || {},
  buyerGuidanceContext: state.buyerGuidanceContext || {},
  anchors: state.anchors || {},
  confidence: state.confidence || {},
  provenance: state.provenance || {},
});

const buildNextFrontendContext = (previous = {}, response = {}) => {
  const patch = response.contextPatch || {};
  const state = compactStateForFrontendTransport(
    patch.contextState || patch.aciContextState || {},
  );

  return {
    contextState: state,
    aciContextState: state,
    selectedVehicle: patch.selectedVehicle || state.selectedVehicle || previous.selectedVehicle || {},
    anchorMake: patch.anchorMake || state.selectedVehicle?.make || previous.anchorMake || "",
    anchorModel: patch.anchorModel || state.selectedVehicle?.model || previous.anchorModel || "",
    anchorVariant: Object.prototype.hasOwnProperty.call(patch, "anchorVariant")
      ? patch.anchorVariant || ""
      : state.selectedVehicle?.variant || "",
    anchorCity:
      patch.anchorCity || state.selectedVehicle?.citySlug || previous.anchorCity || "new-delhi",
    selectedComparisonSet:
      patch.selectedComparisonSet === null ? null : patch.selectedComparisonSet || {},
    activeComparison: patch.activeComparison || state.activeComparison || {},
  };
};

const percentile = (values = [], ratio = 0.95) => {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * ratio) - 1)];
};

const hasModel = (vehicles = [], modelPattern) =>
  vehicles.some((vehicle) => modelPattern.test(vehicleLabel(vehicle)));

async function main() {
  const uri = mongoUri();
  assert(uri, "Mongo URI is required for the roller-coaster smoke.");

  await mongoose.connect(uri);

  const [{ chatWithAgent }, { prewarmAciAssistRuntime }, { prewarmAciCoreRuntime }] =
    await Promise.all([
      import("../../services/aiAgent/aiAgent.service.js"),
      import("../../services/aiAgent/aiAgent.runtimePrewarm.js"),
      import("../../services/aciCore/aciCore.prewarm.js"),
    ]);

  await Promise.all([
    prewarmAciAssistRuntime({ force: true }),
    prewarmAciCoreRuntime({ force: true, mode: "light", background: false }),
  ]);

  const steps = [
    {
      id: "thar-colors",
      message: "thar colors",
      check(response) {
        assert(/\bthar\b/.test(vehicleLabel(selectedVehicle(response))));
        assert.strictEqual(comparisonVehicles(response).length, 0);
      },
    },
    {
      id: "thar-abs-follow-up",
      message: "abs",
      check(response) {
        assert(/feature/.test(response.intent || ""));
        assert(/\babs\b|anti-lock/i.test(response.answer || ""));
        assert(!/anti-lock Braking/.test(response.answer || ""));
        assert(!/older .* variant|pick a current variant/i.test(response.answer || ""));
        assert(!/\babs\b/.test(lower(selectedVehicle(response).variant || "")));
      },
    },
    {
      id: "thar-sunroof-follow-up",
      message: "sunroof",
      check(response) {
        assert(/\bthar\b/.test(vehicleLabel(selectedVehicle(response))));
        assert(/sunroof/i.test(response.answer || ""));
      },
    },
    {
      id: "contextual-vs-creta",
      message: "vs creta",
      check(response) {
        const vehicles = comparisonVehicles(response);
        assert(hasModel(vehicles, /\bthar\b/));
        assert(hasModel(vehicles, /\bcreta\b/));
        assert(!/\bindexed\b|\bi compared\b|listed features|share \d+ highlights/i.test(response.answer || ""));
      },
    },
    {
      id: "comparison-abs-follow-up",
      message: "which has abs",
      check(response) {
        assert(/feature_comparison/.test(response.intent || ""));
        assert(/\babs\b|anti-lock/i.test(response.answer || ""));
        assert(hasModel(comparisonVehicles(response), /\bthar\b/));
        assert(hasModel(comparisonVehicles(response), /\bcreta\b/));
      },
    },
    {
      id: "explicit-creta-sunroof",
      message: "creta sunroof",
      check(response) {
        assert(/\bcreta\b/.test(vehicleLabel(selectedVehicle(response))));
        assert.strictEqual(comparisonVehicles(response).length, 0);
      },
    },
    {
      id: "creta-abs-follow-up",
      message: "abs",
      check(response) {
        assert(/\bcreta\b/.test(vehicleLabel(selectedVehicle(response))));
        assert(!/anti-lock Braking/.test(response.answer || ""));
        assert(!/older .* variant/i.test(response.answer || ""));
        assert(!/\babs\b/.test(lower(selectedVehicle(response).variant || "")));
      },
    },
    {
      id: "creta-vs-thar-matched-powertrain",
      message: "vs thar",
      check(response) {
        const rows = comparisonRows(response);
        const vehicles = comparisonVehicles(response);
        assert.strictEqual(rows.length, 2);
        assert(hasModel(vehicles, /\bcreta\b/));
        assert(hasModel(vehicles, /\bthar\b(?! roxx)/));
        assert(rows.every((row) => /petrol/i.test(row.fuel || row.fuelType || "")));
        assert(rows.every((row) => /manual/i.test(row.transmission || "")));
        assert(!/AXT RWD Diesel/i.test(response.answer || ""));
        assert(/like-for-like|both are petrol manual/i.test(response.answer || ""));
        assert(!/DB-backed|indexed|current structured data|variants shown/i.test(response.answer || ""));
      },
    },
    {
      id: "explicit-cross-fuel-variant-honesty",
      message: "compare Hyundai Creta E vs Mahindra Thar AXT RWD Diesel",
      check(response) {
        assert.strictEqual(
          response.data?.comparisonPairing?.status,
          "explicit_powertrain_mismatch",
        );
        assert(/powertrains differ/i.test(response.answer || ""));
        assert(/petrol manual/i.test(response.answer || ""));
        assert(/diesel manual/i.test(response.answer || ""));
        assert(/like-for-like pair/i.test(response.answer || ""));
      },
    },
    {
      id: "switch-punch-ev-colors",
      message: "tata punch ev colors",
      check(response) {
        assert(/\bpunch ev\b/.test(vehicleLabel(selectedVehicle(response))));
        assert.strictEqual(comparisonVehicles(response).length, 0);
      },
    },
    {
      id: "punch-ev-vs-thar-fallback-honesty",
      message: "vs thar",
      check(response) {
        const fallback = asArray(response.data?.variantSelection).find(
          (selection) => selection.fallbackReason === "no_matching_fuel_variant",
        );
        assert(fallback);
        assert(/could not find an? electric .*Thar variant/i.test(response.answer || ""));
        assert(/nearest available option/i.test(response.answer || ""));
        assert(/does not work for you|switch it/i.test(response.answer || ""));
        assert.strictEqual(response.data?.comparisonPairing?.requiresConfirmation, true);
        assert.strictEqual(response.data?.comparisonPairing?.choices?.length, 2);
        assert(asArray(response.actions).some((action) => action.id === "accept-comparison-fallback"));
        assert(asArray(response.actions).some((action) => action.id === "change-comparison-variants"));
      },
    },
    {
      id: "switch-thar-roxx-colors",
      message: "thar roxx colors",
      check(response) {
        assert(/\bthar roxx\b/.test(vehicleLabel(selectedVehicle(response))));
      },
    },
    {
      id: "thar-roxx-sunroof-follow-up",
      message: "sunroof",
      check(response) {
        assert(/\bthar roxx\b/.test(vehicleLabel(selectedVehicle(response))));
      },
    },
    {
      id: "thar-roxx-vs-thar",
      message: "vs thar",
      check(response) {
        const vehicles = comparisonVehicles(response);
        assert(hasModel(vehicles, /\bthar roxx\b/));
        assert(hasModel(vehicles, /\bthar\b(?! roxx)/));
      },
    },
    {
      id: "comparison-sunroof-follow-up",
      message: "which has sunroof",
      check(response) {
        assert(/feature_comparison/.test(response.intent || ""));
        assert(!/could not confidently match/i.test(response.answer || ""));
        assert(!/which has sunroof/i.test(vehicleLabel(selectedVehicle(response))));
      },
    },
    {
      id: "explicit-creta-abs-clears-comparison",
      message: "creta abs",
      check(response) {
        assert(/\bcreta\b/.test(vehicleLabel(selectedVehicle(response))));
        assert.strictEqual(comparisonVehicles(response).length, 0);
        assert(!/older .* variant/i.test(response.answer || ""));
      },
    },
    {
      id: "relative-last-vs-thar-roxx",
      message: "last vs thar roxx",
      check(response) {
        const vehicles = comparisonVehicles(response);
        assert(hasModel(vehicles, /\bcreta\b/));
        assert(hasModel(vehicles, /\bthar roxx\b/));
        assert(!/comparison between last|\blast and\b/i.test(response.answer || ""));
        assert.strictEqual(response.contextPatch?.contextTrace?.relativeReferenceResolved, true);
      },
    },
  ];

  let context = { anchorCity: "new-delhi" };
  const results = [];

  try {
    for (const step of steps) {
      const startedAt = Date.now();
      try {
        const response = await chatWithAgent({ message: step.message, context });
        const durationMs = Date.now() - startedAt;
        step.check(response);
        context = buildNextFrontendContext(context, response);
        results.push({
          id: step.id,
          message: step.message,
          ok: true,
          durationMs,
          intent: response.intent || "",
          isolation: response.meta?.aciCoreBridge?.contextIsolation || "",
          answer: response.answer || "",
        });
      } catch (error) {
        results.push({
          id: step.id,
          message: step.message,
          ok: false,
          durationMs: Date.now() - startedAt,
          error: error.message,
        });
      }
    }
  } finally {
    await mongoose.disconnect();
  }

  const durations = results.map((result) => result.durationMs);
  const medianMs = percentile(durations, 0.5);
  const p95Ms = percentile(durations, 0.95);
  const maxMs = Math.max(...durations);
  const functionalFailures = results.filter((result) => !result.ok).map((result) => result.id);
  const timingFailures = [
    medianMs > 900 ? `median ${medianMs}ms > 900ms` : "",
    p95Ms > 1600 ? `p95 ${p95Ms}ms > 1600ms` : "",
    maxMs > 2500 ? `max ${maxMs}ms > 2500ms` : "",
  ].filter(Boolean);
  const failedIds = [...functionalFailures, ...timingFailures.map((_, index) => `timing-${index + 1}`)];

  const output = {
    suite: "ACI frontend-contract context roller-coaster smoke v1",
    ok: failedIds.length === 0,
    total: results.length,
    passed: results.length - functionalFailures.length,
    failed: failedIds.length,
    failedIds,
    timing: { medianMs, p95Ms, maxMs },
    timingFailures,
    results,
  };

  console.log(JSON.stringify(output, null, 2));
  if (!output.ok) process.exit(1);
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI frontend-contract context roller-coaster smoke v1",
    ok: false,
    error: error.message,
  }, null, 2));
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
