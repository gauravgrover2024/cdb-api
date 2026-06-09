#!/usr/bin/env node

require("dotenv/config");
const assert = require("assert");

const CASE_TIMEOUT_MS = Number(process.env.ACI_COLOR_ROUTING_CASE_TIMEOUT_MS || 5000);

const cases = [
  {
    id: "creta-colors",
    message: "Creta colors",
    expectedModel: "creta",
    minRows: 1,
  },
  {
    id: "show-colors-of-creta",
    message: "Show colors of Creta",
    expectedModel: "creta",
    minRows: 1,
  },
  {
    id: "seltos-colors",
    message: "Which colors are available in Seltos?",
    expectedModel: "seltos",
    minRows: 1,
  },
  {
    id: "creta-black-color",
    message: "Creta black color",
    expectedModel: "creta",
    minRows: 1,
    expectedDataStatus: "available",
    requiredAnswerPattern: /\bblack\b/i,
  },
  {
    id: "seltos-dual-tone-negative",
    message: "dual tone colors in seltos",
    expectedModel: "seltos",
    minRows: 0,
    maxRows: 0,
    expectedDataStatus: "not_available",
    requiredAnswerPattern: /\bno\b.*\bdual[-\s]?tone\b/i,
  },
];

const asArray = (value) => Array.isArray(value) ? value : value ? [value] : [];
const normalize = (value = "") => String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();

const withTimeout = async (promise, ms, label) =>
  Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)),
  ]);

const getRows = (response = {}) =>
  asArray(response.rows || response.items || response.colors || response.data?.rows || response.data?.items || response.data?.colors);

const getSelectedVehicle = (response = {}) =>
  response.contextPatch?.selectedVehicle || response.data?.selectedVehicle || response.selectedVehicle || {};

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
          meta: { source: "auditAciColorRoutingV1", caseId: testCase.id },
        }),
        CASE_TIMEOUT_MS,
        testCase.id,
      );

      const rows = getRows(response);
      const selectedVehicle = getSelectedVehicle(response);
      const bridge = response.aciCoreBridge || response.meta?.aciCoreBridge || {};
      const answerText = `${response.title || ""} ${response.answer || ""}`;

      try { assert.strictEqual(response.intent, "vehicle_colors", `expected intent vehicle_colors, got ${response.intent || ""}`); } catch (e) { failures.push(e.message); }
      try { assert.strictEqual(response.canvasType, "color_studio_canvas", `expected color_studio_canvas, got ${response.canvasType || ""}`); } catch (e) { failures.push(e.message); }
      try { assert(rows.length >= testCase.minRows, `expected at least ${testCase.minRows} color rows, got ${rows.length}`); } catch (e) { failures.push(e.message); }
      if (Number.isFinite(testCase.maxRows)) {
        try { assert(rows.length <= testCase.maxRows, `expected at most ${testCase.maxRows} color rows, got ${rows.length}`); } catch (e) { failures.push(e.message); }
      }
      if (testCase.expectedDataStatus) {
        try { assert.strictEqual(response.dataStatus, testCase.expectedDataStatus, `expected dataStatus ${testCase.expectedDataStatus}, got ${response.dataStatus || ""}`); } catch (e) { failures.push(e.message); }
      }
      if (testCase.requiredAnswerPattern) {
        try { assert(testCase.requiredAnswerPattern.test(response.answer || ""), `answer did not match ${testCase.requiredAnswerPattern}`); } catch (e) { failures.push(e.message); }
      }
      try {
        assert(
          normalize(selectedVehicle.model || selectedVehicle.fullModel || response.title).includes(testCase.expectedModel),
          `expected model ${testCase.expectedModel}`,
        );
      } catch (e) { failures.push(e.message); }
      try {
        assert(!/\bremote climate|airbags?|sunroof|adas package\b/i.test(answerText), "color query returned feature-answer content");
      } catch (e) { failures.push(e.message); }
      try {
        assert(bridge.tool === "vehicle_colors" || response.tool === "vehicle_colors", `expected vehicle_colors bridge/tool, got bridge=${bridge.tool || ""}, response=${response.tool || ""}`);
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
          dataStatus: response.dataStatus || "",
          selectedVehicle: {
            make: selectedVehicle.make || selectedVehicle.brand || "",
            model: selectedVehicle.model || "",
            fullModel: selectedVehicle.fullModel || selectedVehicle.displayName || "",
            colorCount: selectedVehicle.colorCount || "",
          },
          bridge: {
            tool: bridge.tool || "",
            contextIsolation: bridge.contextIsolation || "",
            routingReason: bridge.routingReason || "",
          },
          answerPreview: String(response.answer || "").slice(0, 220),
        },
      });
    } catch (error) {
      results.push({ id: testCase.id, pass: false, durationMs: Date.now() - caseStartedAt, failures: [error?.message || String(error)] });
    }
  }

  const failed = results.filter((item) => !item.pass);
  const output = {
    suite: "ACI Color Routing Audit v1",
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
