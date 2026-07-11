import mongoose from "mongoose";

import {
  loadVehicleModelIndex,
  loadVehicleVariantIndexByModelKey,
} from "./aiAgent.vehicleModelResolver.js";
import { getVehicleEntityIndex } from "./aiAgent.vehicleEntityIndex.js";
import { loadAciFeatureRequestCatalog } from "./aiAgent.featureRequestParser.js";
import { refreshVehicleHintsFromDb } from "./aiAgent.intentParser.js";
import { prewarmFeatureResolverV2 } from "./aiAgent.featureResolverV2.js";
import { prewarmAciFeatureExplainers } from "../aciCore/features/aciFeatureExplainer.service.js";
import { prewarmAciFinalRecommendationEvidence } from "../aciCore/recommendations/aciFinalRecommendation.service.js";

const DEFAULT_PREWARM_TTL_MS = Number(
  process.env.ACI_RUNTIME_PREWARM_TTL_MS || 10 * 60 * 1000,
);

let prewarmState = {
  startedAt: 0,
  completedAt: 0,
  durationMs: 0,
  status: "idle",
  error: "",
  promise: null,
  results: [],
};

const isMongoReady = () =>
  Boolean(mongoose.connection?.readyState === 1 && mongoose.connection?.db);

const shouldSkipPrewarm = ({ force = false } = {}) => {
  if (force) return false;
  if (prewarmState.status === "running" && prewarmState.promise) return true;
  if (!prewarmState.completedAt) return false;

  return Date.now() - prewarmState.completedAt < DEFAULT_PREWARM_TTL_MS;
};

const normalizeSettled = (item, label) => ({
  label,
  ok: item.status === "fulfilled",
  error: item.status === "rejected" ? item.reason?.message || String(item.reason || "") : "",
});

export const prewarmAciAssistRuntime = async ({ force = false } = {}) => {
  if (!isMongoReady()) {
    return {
      ...prewarmState,
      status: "skipped",
      error: "mongoose_not_connected",
    };
  }

  if (shouldSkipPrewarm({ force })) {
    return prewarmState.promise || prewarmState;
  }

  const db = mongoose.connection.db;
  const startedAt = Date.now();

  prewarmState = {
    ...prewarmState,
    startedAt,
    status: "running",
    error: "",
    promise: null,
  };

  prewarmState.promise = (async () => {
    const tasks = [
      ["vehicle_model_index", loadVehicleModelIndex({ db, force })],
      ["vehicle_variant_index", loadVehicleVariantIndexByModelKey({ db, force })],
      ["vehicle_entity_index", getVehicleEntityIndex({ forceRefresh: force })],
      ["feature_request_catalog", loadAciFeatureRequestCatalog({ forceRefresh: force })],
      ["feature_answer_resolver", prewarmFeatureResolverV2()],
      ["feature_explainer_catalog", prewarmAciFeatureExplainers({ force })],
      ["final_recommendation_evidence", prewarmAciFinalRecommendationEvidence({ force })],
      ["vehicle_hints", refreshVehicleHintsFromDb()],
    ];

    const settled = await Promise.allSettled(tasks.map(([, promise]) => promise));
    const results = settled.map((item, index) => normalizeSettled(item, tasks[index][0]));

    const failed = results.filter((item) => !item.ok);
    const completedAt = Date.now();

    prewarmState = {
      startedAt,
      completedAt,
      durationMs: completedAt - startedAt,
      status: failed.length ? "partial" : "ready",
      error: failed.map((item) => `${item.label}: ${item.error}`).join(" | "),
      promise: null,
      results,
    };

    if (process.env.ACI_RUNTIME_PREWARM_LOG === "true") {
      const summary = results.map((item) => `${item.label}:${item.ok ? "ok" : "failed"}`).join(", ");
      console.log(
        `[ACI Assist] runtime prewarm ${prewarmState.status} in ${prewarmState.durationMs}ms (${summary})`,
      );

      if (prewarmState.error) {
        console.warn(`[ACI Assist] runtime prewarm warnings: ${prewarmState.error}`);
      }
    }

    return prewarmState;
  })();

  return prewarmState.promise;
};

export const triggerAciAssistRuntimePrewarm = ({ force = false } = {}) => {
  if (!isMongoReady()) return null;

  if (shouldSkipPrewarm({ force })) {
    return prewarmState.promise || null;
  }

  const promise = prewarmAciAssistRuntime({ force }).catch((error) => {
    prewarmState = {
      ...prewarmState,
      completedAt: Date.now(),
      durationMs: Date.now() - (prewarmState.startedAt || Date.now()),
      status: "failed",
      error: error?.message || String(error || ""),
      promise: null,
    };

    return prewarmState;
  });

  return promise;
};

export const getAciAssistRuntimePrewarmState = () => ({
  ...prewarmState,
  promise: prewarmState.promise ? "[in-flight]" : null,
});
