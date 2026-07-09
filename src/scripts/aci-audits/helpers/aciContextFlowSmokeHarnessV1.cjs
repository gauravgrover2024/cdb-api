"use strict";

require("dotenv").config();

const mongoose = require("mongoose");

const mongoUri = () =>
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.MONGODB_URL ||
  process.env.MONGO_URL ||
  process.env.DATABASE_URL ||
  "";

const vehicleLabel = (vehicle = {}) =>
  [
    vehicle.make || vehicle.brand,
    vehicle.model || vehicle.fullModel || vehicle.displayName,
    vehicle.variant || vehicle.variantName || vehicle.selectedVariant,
  ]
    .filter(Boolean)
    .join(" ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const comparisonVehicles = (response = {}) => {
  const vehicles =
    response.contextPatch?.selectedComparisonSet?.vehicles ||
    response.contextPatch?.activeComparison?.vehicles ||
    response.contextPatch?.contextState?.activeComparison?.vehicles ||
    response.contextPatch?.aciContextState?.activeComparison?.vehicles ||
    response.selectedComparisonSet?.vehicles ||
    response.activeComparison?.vehicles ||
    [];

  return Array.isArray(vehicles) ? vehicles : [];
};

const comparisonLabels = (response = {}) =>
  comparisonVehicles(response).map(vehicleLabel).filter(Boolean);

const selectedLabel = (response = {}) =>
  vehicleLabel(
    response.contextPatch?.selectedVehicle ||
      response.contextPatch?.contextState?.selectedVehicle ||
      response.contextPatch?.aciContextState?.selectedVehicle ||
      response.selectedVehicle ||
      {},
  );

const mergeContext = (previous = {}, patch = {}) => ({
  ...previous,
  ...(patch || {}),
  selectedVehicle:
    patch.selectedVehicle ||
    patch.contextState?.selectedVehicle ||
    previous.selectedVehicle ||
    {},
  selectedComparisonSet:
    patch.selectedComparisonSet === null
      ? null
      : patch.selectedComparisonSet || previous.selectedComparisonSet || null,
  activeComparison:
    patch.activeComparison ||
    patch.contextState?.activeComparison ||
    patch.aciContextState?.activeComparison ||
    previous.activeComparison ||
    {},
  contextLedger:
    patch.contextLedger ||
    patch.contextState?.contextLedger ||
    patch.aciContextState?.contextLedger ||
    previous.contextLedger ||
    {},
  contextState: {
    ...(previous.contextState || {}),
    ...(patch.contextState || {}),
  },
  aciContextState: {
    ...(previous.aciContextState || {}),
    ...(patch.aciContextState || {}),
  },
});

const getBridgeRunner = async () => {
  const mod = await import(
    "../../../services/aciCore/integration/aciCoreLiveBridge.service.js"
  );
  if (typeof mod.runAciCoreLiveBridge !== "function") {
    throw new Error("runAciCoreLiveBridge export is unavailable.");
  }
  return mod.runAciCoreLiveBridge;
};

const responseSummary = ({ scenarioId = "", step = {}, response = {} } = {}) => ({
  id: `${scenarioId}:${step.id}`,
  scenarioId,
  stepId: step.id,
  message: step.message,
  ok: true,
  intent: response.intent || "",
  tool: response.tool || response.meta?.aciCoreBridge?.tool || "",
  contextIsolation: response.meta?.aciCoreBridge?.contextIsolation || "",
  selectedVehicle: selectedLabel(response),
  comparisonVehicles: comparisonLabels(response),
  contextTrace: response.contextPatch?.contextTrace || {},
});

async function runAciContextFlowSmokeSuite({
  suite = "ACI context flow smoke",
  scenarios = [],
} = {}) {
  const uri = mongoUri();
  if (!uri) {
    throw new Error(
      "Mongo URI is required. Set MONGO_URI, MONGODB_URI, MONGODB_URL, MONGO_URL, or DATABASE_URL.",
    );
  }

  await mongoose.connect(uri);
  const runBridge = await getBridgeRunner();
  const results = [];

  try {
    for (const scenario of scenarios) {
      let context = {};

      for (const step of scenario.steps || []) {
        try {
          const response = await runBridge({
            message: step.message,
            context,
            session: {
              source: "aci_context_flow_smoke",
              scenarioId: scenario.id,
            },
          });

          if (!response || typeof response !== "object") {
            throw new Error("Live bridge returned no response object.");
          }

          if (typeof step.assert === "function") {
            step.assert({
              response,
              context,
              selectedLabel: selectedLabel(response),
              comparisonLabels: comparisonLabels(response),
            });
          }

          context = mergeContext(context, response.contextPatch || {});
          results.push(responseSummary({ scenarioId: scenario.id, step, response }));
        } catch (error) {
          results.push({
            id: `${scenario.id}:${step.id}`,
            scenarioId: scenario.id,
            stepId: step.id,
            message: step.message,
            ok: false,
            error: error.message,
          });
        }
      }
    }
  } finally {
    await mongoose.disconnect();
  }

  const failedStepIds = results
    .filter((result) => !result.ok)
    .map((result) => result.id);

  return {
    suite,
    ok: failedStepIds.length === 0,
    total: results.length,
    passed: results.length - failedStepIds.length,
    failed: failedStepIds.length,
    failedStepIds,
    results,
  };
}

module.exports = {
  comparisonLabels,
  mergeContext,
  runAciContextFlowSmokeSuite,
  selectedLabel,
  vehicleLabel,
};
