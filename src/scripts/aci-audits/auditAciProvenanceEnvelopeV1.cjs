#!/usr/bin/env node

require("dotenv/config");
const assert = require("assert");

const CASE_TIMEOUT_MS = Number(process.env.ACI_PROVENANCE_CASE_TIMEOUT_MS || 12000);

const cases = [
  {
    id: "price-exact",
    message: "Creta SX on-road price Delhi",
    expectedStatus: "available",
    expectedCollections: ["aci_vehicle_price_rows"],
  },
  {
    id: "colors",
    message: "Creta colors",
    expectedStatus: "available",
    expectedCollections: ["vehicle_colors_v2"],
  },
  {
    id: "feature-exact",
    message: "Does Creta SX have 6 airbags?",
    expectedStatus: "available",
    expectedCollections: ["vehicle_feature_catalog_v2", "vehicle_variant_feature_matrix_v2"],
  },
  {
    id: "feature-discovery",
    message: "which Punch variants have sunroof",
    expectedStatus: "available",
    expectedCollections: ["vehicle_feature_catalog_v2", "vehicle_variant_feature_matrix_v2"],
  },
  {
    id: "unsupported-city",
    message: "Seltos price Mumbai",
    expectedStatus: "unsupported_city",
    expectedCollections: ["aci_vehicle_price_rows"],
  },
  {
    id: "comparison",
    message: "Creta vs Seltos",
    expectedStatus: "available",
    expectedCollections: ["aci_vehicle_price_rows", "vehicle_variant_feature_matrix_v2"],
  },
  {
    id: "budget-discovery",
    message: "cars under 20 lakhs in Delhi",
    expectedStatus: "available",
    expectedCollections: ["aci_vehicle_model_summary", "aci_vehicle_price_rows"],
  },
];

const asArray = (value) => Array.isArray(value) ? value : value ? [value] : [];

const withTimeout = async (promise, ms, label) =>
  Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)),
  ]);

const getRows = (response = {}) =>
  asArray(response.rows || response.items || response.colors || response.data?.rows || response.data?.items || response.data?.colors);

const hasAll = (haystack = [], needles = []) =>
  needles.every((needle) => haystack.includes(needle));

async function main() {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  await connectDB();

  const { chatWithAgent } = await import("../../services/aiAgent/aiAgent.service.js");

  const startedAt = Date.now();
  const results = [];

  for (const testCase of cases) {
    const caseStartedAt = Date.now();
    const failures = [];

    try {
      const response = await withTimeout(
        chatWithAgent({
          message: testCase.message,
          context: {},
          user: null,
          session: {},
          meta: { source: "auditAciProvenanceEnvelopeV1", caseId: testCase.id },
        }),
        CASE_TIMEOUT_MS,
        testCase.id,
      );

      const rows = getRows(response);
      const sourceCollections = asArray(response.sourceCollections);
      const dataStatus = response.dataStatus || "";
      const provenance = response.provenance || {};
      const trace = response.trace || {};

      try { assert(sourceCollections.length > 0, "top-level sourceCollections missing"); } catch (e) { failures.push(e.message); }
      try { assert(hasAll(sourceCollections, testCase.expectedCollections), `missing expected collections: ${testCase.expectedCollections.join(", ")}`); } catch (e) { failures.push(e.message); }
      try { assert.strictEqual(dataStatus, testCase.expectedStatus, `expected dataStatus ${testCase.expectedStatus}, got ${dataStatus}`); } catch (e) { failures.push(e.message); }
      try { assert(provenance && typeof provenance === "object" && provenance.tool, "provenance.tool missing"); } catch (e) { failures.push(e.message); }
      try { assert(provenance.dataStatus === dataStatus, "provenance.dataStatus mismatch"); } catch (e) { failures.push(e.message); }
      try { assert(Array.isArray(provenance.sourceCollections) && provenance.sourceCollections.length > 0, "provenance.sourceCollections missing"); } catch (e) { failures.push(e.message); }
      try { assert(trace && typeof trace === "object" && trace.tool, "trace.tool missing"); } catch (e) { failures.push(e.message); }
      try { assert(Boolean(response.meta?.provenance), "meta.provenance missing"); } catch (e) { failures.push(e.message); }
      try { assert(Boolean(response.data?.provenance), "data.provenance missing"); } catch (e) { failures.push(e.message); }
      try {
        assert(!sourceCollections.includes("unsupported_city_fast_path"), "sourceCollections should not include unsupported_city_fast_path");
        assert(!sourceCollections.includes("aci_vehicle_read_models"), "sourceCollections should not include aci_vehicle_read_models");
      } catch (e) { failures.push(e.message); }

      results.push({
        id: testCase.id,
        pass: failures.length === 0,
        durationMs: Date.now() - caseStartedAt,
        failures,
        summary: {
          message: testCase.message,
          intent: response.intent || "",
          tool: response.tool || "",
          canvasType: response.canvasType || "",
          title: response.title || "",
          rowCount: rows.length,
          sourceCollections,
          dataStatus,
          provenanceTool: provenance.tool || "",
          provenanceDataSource: provenance.dataSource || "",
          traceTool: trace.tool || "",
        },
      });
    } catch (error) {
      results.push({
        id: testCase.id,
        pass: false,
        durationMs: Date.now() - caseStartedAt,
        failures: [error?.message || String(error)],
      });
    }
  }

  const failed = results.filter((item) => !item.pass);
  const output = {
    suite: "ACI Provenance Envelope Audit v1",
    ok: failed.length === 0,
    backendOnly: true,
    frontendEvaluated: false,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    results,
    durationMs: Date.now() - startedAt,
  };

  console.log(JSON.stringify(output, null, 2));

  const mongoose = require("mongoose");
  if (mongoose.connection?.readyState) await mongoose.disconnect();

  if (!output.ok) process.exit(1);
}

main().catch(async (error) => {
  console.error(error);
  const mongoose = require("mongoose");
  if (mongoose.connection?.readyState) await mongoose.disconnect();
  process.exit(1);
});
