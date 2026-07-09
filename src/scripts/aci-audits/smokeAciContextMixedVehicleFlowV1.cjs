#!/usr/bin/env node
"use strict";

require("dotenv").config();

const assert = require("assert");
const mongoose = require("mongoose");

const vehicleLabel = (vehicle = {}) =>
  [
    vehicle.make || vehicle.brand,
    vehicle.model || vehicle.fullModel || vehicle.displayName,
    vehicle.variant || vehicle.variantName || vehicle.selectedVariant,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

const vehicleListFromResponse = (response = {}) => {
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

const comparisonLabels = (response = {}) => vehicleListFromResponse(response).map(vehicleLabel).filter(Boolean);

const selectedLabel = (response = {}) =>
  vehicleLabel(
    response.contextPatch?.selectedVehicle ||
      response.contextPatch?.contextState?.selectedVehicle ||
      response.contextPatch?.aciContextState?.selectedVehicle ||
      response.selectedVehicle ||
      {}
  );

const mergeContext = (previous = {}, patch = {}) => ({
  ...previous,
  ...(patch || {}),
  selectedVehicle: patch.selectedVehicle || patch.contextState?.selectedVehicle || previous.selectedVehicle || {},
  selectedComparisonSet:
    patch.selectedComparisonSet === null
      ? null
      : patch.selectedComparisonSet || previous.selectedComparisonSet || null,
  activeComparison:
    patch.activeComparison || patch.contextState?.activeComparison || patch.aciContextState?.activeComparison || previous.activeComparison || {},
  contextLedger:
    patch.contextLedger || patch.contextState?.contextLedger || patch.aciContextState?.contextLedger || previous.contextLedger || {},
  contextState: {
    ...(previous.contextState || {}),
    ...(patch.contextState || {}),
  },
  aciContextState: {
    ...(previous.aciContextState || {}),
    ...(patch.aciContextState || {}),
  },
});

async function getBridgeRunner() {
  const mod = await import("../../services/aciCore/integration/aciCoreLiveBridge.service.js");

  const runner =
    mod.runAciCoreLiveBridge ||
    mod.runAciCoreBridge ||
    mod.handleAciCoreLiveBridge ||
    mod.executeAciCoreLiveBridge ||
    mod.default;

  if (typeof runner !== "function") {
    throw new Error(
      `Could not find live bridge runner export in aciCoreLiveBridge.service.js. Exports: ${Object.keys(mod).join(", ")}`
    );
  }

  return runner;
}

async function callBridge(runBridge, message, context) {
  const attempts = [
    () => runBridge({ message, context, session: { source: "aci_context_mixed_vehicle_flow_smoke" } }),
    () => runBridge({ rawMessage: message, message, context, session: { source: "aci_context_mixed_vehicle_flow_smoke" } }),
    () => runBridge(message, context),
  ];

  let lastError;

  for (const attempt of attempts) {
    try {
      const response = await attempt();
      if (response && typeof response === "object") return response;
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("Live bridge returned no response object");
}

(async () => {
  const mongoUri =
    process.env.MONGODB_URI ||
    process.env.MONGO_URI ||
    process.env.MONGODB_URL ||
    process.env.MONGO_URL ||
    process.env.DATABASE_URL;

  if (!mongoUri) {
    throw new Error(
      "Mongo URI is required. Set MONGO_URI, MONGODB_URI, MONGODB_URL, MONGO_URL, or DATABASE_URL."
    );
  }

  await mongoose.connect(mongoUri);

  const runBridge = await getBridgeRunner();

  let context = {};
  const results = [];

  const steps = [
    {
      id: "thar-price",
      message: "thar price",
      assert(response) {
        const selected = selectedLabel(response);
        assert(/thar/.test(selected), `Expected Thar selected after price query, got: ${selected}`);
      },
    },
    {
      id: "creta-sunroof",
      message: "creta sunroof",
      assert(response) {
        const selected = selectedLabel(response);
        assert(/creta/.test(selected), `Expected Creta selected after sunroof query, got: ${selected}`);

        const labels = comparisonLabels(response);
        assert.strictEqual(labels.length, 0, `Creta sunroof must not preserve comparison context: ${labels.join(" | ")}`);
      },
    },
    {
      id: "thar-abs",
      message: "thar abs",
      assert(response) {
        const selected = selectedLabel(response);
        assert(/thar/.test(selected), `Expected Thar selected after ABS query, got: ${selected}`);
        assert(!/creta/.test(selected), `Creta contaminated Thar ABS context: ${selected}`);

        const labels = comparisonLabels(response);
        assert.strictEqual(labels.length, 0, `Thar ABS must clear comparison context: ${labels.join(" | ")}`);
      },
    },
    {
      id: "last-vs-thar-roxx",
      message: "last vs thar roxx",
      assert(response) {
        const labels = comparisonLabels(response);
        const joined = labels.join(" | ");

        assert(labels.length >= 2, `Expected at least two comparison vehicles, got: ${joined}`);
        assert(/thar/.test(joined), `Expected Thar in comparison, got: ${joined}`);
        assert(/roxx/.test(joined), `Expected Thar Roxx in comparison, got: ${joined}`);
        assert(!/creta/.test(joined), `Creta must not leak into last vs Thar Roxx comparison: ${joined}`);
        assert(
          labels.some((label) => /\bthar\b/.test(label) && !/\broxx\b/.test(label)),
          `Expected base Thar to remain distinct from Thar Roxx, got: ${joined}`
        );
      },
    },
  ];

  for (const step of steps) {
    try {
      const response = await callBridge(runBridge, step.message, context);
      step.assert(response);
      context = mergeContext(context, response.contextPatch || {});

      results.push({
        id: step.id,
        message: step.message,
        ok: true,
        intent: response.intent,
        tool: response.tool || response.meta?.aciCoreBridge?.tool || "",
        title: response.title,
        selectedVehicle: selectedLabel(response),
        comparisonVehicles: comparisonLabels(response),
        contextLedgerVersion:
          context.contextLedger?.version ||
          context.contextState?.contextLedger?.version ||
          context.aciContextState?.contextLedger?.version ||
          "",
        contextTrace: response.contextPatch?.contextTrace || {},
      });
    } catch (error) {
      results.push({
        id: step.id,
        message: step.message,
        ok: false,
        error: error.message,
      });
    }
  }

  await mongoose.disconnect();

  const failedStepIds = results.filter((result) => !result.ok).map((result) => result.id);
  const output = {
    suite: "ACI mixed vehicle customer-chat context flow v1",
    ok: failedStepIds.length === 0,
    total: steps.length,
    passed: steps.length - failedStepIds.length,
    failed: failedStepIds.length,
    failedStepIds,
    results,
  };

  console.log(JSON.stringify(output, null, 2));
  if (!output.ok) process.exitCode = 1;
})().catch(async (error) => {
  try {
    if (mongoose.connection.readyState !== 0) await mongoose.disconnect();
  } catch (_) {}

  console.error(JSON.stringify({
    suite: "ACI mixed vehicle customer-chat context flow v1",
    ok: false,
    passed: 0,
    failed: 1,
    failedStepIds: ["suite_setup"],
    error: error.message,
    stack: error.stack,
  }, null, 2));
  process.exit(1);
});
