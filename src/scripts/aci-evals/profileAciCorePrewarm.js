import "dotenv/config";
import mongoose from "mongoose";
import { performance } from "node:perf_hooks";

import connectDB from "../../config/db.js";
import {
  prewarmAciCoreRuntime,
  getAciCoreRuntimePrewarmState,
  normalizePrewarmMode,
} from "../../services/aciCore/aciCore.prewarm.js";
import {
  runAciCoreLiveBridge,
} from "../../services/aciCore/integration/aciCoreLiveBridge.service.js";

const now = () => performance.now();

async function timeStep(label, fn) {
  const startedAt = now();
  try {
    const value = await fn();
    return {
      label,
      ok: true,
      durationMs: Math.round(now() - startedAt),
      value,
    };
  } catch (error) {
    return {
      label,
      ok: false,
      durationMs: Math.round(now() - startedAt),
      error: error?.stack || error?.message || String(error),
    };
  }
}

const summarizeResponse = (response = {}) => ({
  intent: response.intent,
  canvasType: response.canvasType,
  title: response.title,
  matched: response.matched || response.sourceTransparency?.recordCount || 0,
  rowCount: (response.rows || response.data?.rows || []).length,
  modelGroupCount: (response.modelGroups || response.data?.modelGroups || []).length,
  budgetDiscovery: response.budgetDiscovery || response.data?.budgetDiscovery || null,
  cache: response.cache || response.data?.cache || null,
  bridge: response.aciCoreBridge || response.meta?.aciCoreBridge || null,
});

async function main() {
  await connectDB();
  const mode = normalizePrewarmMode();
  const backgroundWarm = process.env.ACI_CORE_PREWARM_BACKGROUND === "true";
  const heavyCachesEnabled = mode === "full";

  const output = {
    suite: "ACI Core prewarm profile",
    startedAt: new Date().toISOString(),
    config: {
      mode,
      heavyCachesEnabled,
      backgroundWarmRequested: backgroundWarm,
      budgetDiscoveryProfile: process.env.ACI_BUDGET_DISCOVERY_PROFILE === "true",
    },
    steps: [],
    warmQueries: [],
  };

  output.steps.push(
    await timeStep(`aci_core_${mode}_prewarm`, () =>
      prewarmAciCoreRuntime({
        force: true,
        mode,
        background: backgroundWarm,
      }),
    ),
  );

  output.prewarmState = getAciCoreRuntimePrewarmState();
  output.config.backgroundWarmTriggered = Boolean(output.prewarmState.backgroundWarmTriggered);
  output.config.heavyStepLabels = (output.prewarmState.results || [])
    .filter((item) => !item.cache?.skipped)
    .map((item) => item.label);

  const queries = [
    "cars under 20 lakhs",
    "Hyundai cars with sunroof under 20 lakh",
    "Creta SX on-road price Delhi",
    "turbocharged SUVs under 8 lakhs",
    "Verna HX8 iVT vs City ZX CVT",
  ];

  for (const message of queries) {
    const result = await timeStep(`warm_query:${message}`, () =>
      runAciCoreLiveBridge({ message, context: {} }),
    );

    output.warmQueries.push({
      message,
      ok: result.ok,
      durationMs: result.durationMs,
      error: result.error,
      summary: result.ok ? summarizeResponse(result.value) : null,
    });
  }

  const firstBudgetQuery = output.warmQueries.find((item) => item.message === "cars under 20 lakhs");
  output.firstBudgetQuery = {
    durationMs: firstBudgetQuery?.durationMs ?? null,
    stayedFast: Number(firstBudgetQuery?.durationMs || 0) < 3000,
    rowSource:
      firstBudgetQuery?.summary?.budgetDiscovery?.rowSource ||
      firstBudgetQuery?.summary?.cache?.rowSource ||
      null,
  };

  output.finishedAt = new Date().toISOString();
  output.ok =
    output.steps.every((step) => step.ok) &&
    output.warmQueries.every((item) => item.ok);

  console.log(JSON.stringify(output, null, 2));

  await mongoose.disconnect();
  process.exitCode = output.ok ? 0 : 1;
}

main().catch(async (error) => {
  console.error(error);
  await mongoose.disconnect().catch(() => {});
  process.exitCode = 1;
});
