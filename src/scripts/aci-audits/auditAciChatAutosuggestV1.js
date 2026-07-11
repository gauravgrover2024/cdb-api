#!/usr/bin/env node

import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";
import { getAiAutocompleteSuggestions } from "../../services/aiAgent/aiAgent.autocomplete.js";

const failures = [];
const results = [];

const runCase = async (id, input, check) => {
  const startedAt = Date.now();
  const response = await getAiAutocompleteSuggestions(input);
  const suggestions = response.suggestions || [];
  const caseFailures = [];
  check({ response, suggestions, failures: caseFailures });
  failures.push(...caseFailures.map((failure) => `${id}: ${failure}`));
  results.push({
    id,
    pass: caseFailures.length === 0,
    durationMs: Date.now() - startedAt,
    failures: caseFailures,
    suggestions: suggestions.map((item) => ({
      id: item.id,
      type: item.type,
      label: item.label,
      tool: item.tool || "",
    })),
  });
};

await connectDB();

try {
  await runCase("brand-prefix", { q: "hyu", limit: 8 }, ({ suggestions, failures: found }) => {
    if (!suggestions.some((item) => item.type === "brand" && /hyundai/i.test(item.label))) {
      found.push("Expected Hyundai brand suggestion");
    }
  });

  await runCase("model-no-generic-templates", { q: "creta", limit: 8 }, ({ suggestions, failures: found }) => {
    if (!suggestions.some((item) => item.type === "model" && /creta/i.test(item.label))) {
      found.push("Expected Creta model suggestion");
    }
    if (suggestions.some((item) => item.type === "query")) {
      found.push("Generic question/query templates must not be returned");
    }
  });

  await runCase(
    "context-variant",
    {
      q: "sx",
      limit: 8,
      context: { selectedVehicle: { make: "Hyundai", model: "Creta" } },
    },
    ({ suggestions, failures: found }) => {
      if (!suggestions.some((item) => item.type === "variant" && /creta.*sx/i.test(item.label))) {
        found.push("Expected a Creta SX variant suggestion");
      }
    },
  );

  await runCase("feature-prefix", { q: "sunr", limit: 8 }, ({ suggestions, failures: found }) => {
    if (!suggestions.some((item) => item.type === "feature" && /sunroof/i.test(item.label))) {
      found.push("Expected Sunroof feature suggestion");
    }
  });

  await runCase(
    "context-action",
    {
      q: "price",
      limit: 8,
      context: { selectedVehicle: { make: "Hyundai", model: "Creta", fullModel: "Hyundai Creta" } },
    },
    ({ suggestions, failures: found }) => {
      const action = suggestions.find(
        (item) => item.type === "context_action" && item.tool === "vehicle_pricelist",
      );
      if (!action || !/creta/i.test(action.label)) {
        found.push("Expected context-aware Creta price action");
      }
    },
  );

  await runCase("warm-latency", { q: "creta", limit: 8 }, ({ response, suggestions, failures: found }) => {
    if (!suggestions.length) found.push("Expected warm suggestions");
    if (Number(response.meta?.queryMs || 0) > 75) {
      found.push(`Warm query took ${response.meta.queryMs}ms; expected <= 75ms`);
    }
    if (new Set(suggestions.map((item) => item.id)).size !== suggestions.length) {
      found.push("Suggestion IDs must be unique");
    }
  });

  const ok = failures.length === 0;
  console.log(
    JSON.stringify(
      {
        suite: "ACI chat autosuggest audit v1",
        ok,
        total: results.length,
        passed: results.filter((item) => item.pass).length,
        failed: results.filter((item) => !item.pass).length,
        failures,
        results,
      },
      null,
      2,
    ),
  );
  if (!ok) process.exitCode = 1;
} finally {
  await mongoose.disconnect();
}
