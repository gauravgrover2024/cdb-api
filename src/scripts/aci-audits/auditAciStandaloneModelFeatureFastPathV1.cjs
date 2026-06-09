require("dotenv/config");

const CASE_TIMEOUT_MS = Number(process.env.ACI_STANDALONE_MODEL_FEATURE_CASE_TIMEOUT_MS || 8000);

const cases = [
  {
    id: "punch-which-variants-sunroof",
    message: "which Punch variants have sunroof",
    expectedIntent: "vehicle_feature_discovery",
    expectedCanvasType: "feature_match_builder_canvas",
    expectedContextIsolation: "standalone_model_feature_fast_path",
    expectedRoutingReason: "standalone_model_feature_discovery",
    expectedRows: 10,
  },
  {
    id: "punch-direct-sunroof",
    message: "Punch sunroof",
    expectedIntent: "vehicle_feature_answer",
    expectedCanvasType: "",
    expectedContextIsolation: "standalone_model_feature_fast_path",
    expectedRoutingReason: "standalone_model_feature_lookup",
    minRows: 20,
  },
  {
    id: "punch-hinglish-sunroof",
    message: "Punch me sunroof hai",
    expectedIntent: "vehicle_feature_answer",
    expectedCanvasType: "",
    expectedContextIsolation: "standalone_model_feature_fast_path",
    expectedRoutingReason: "standalone_model_feature_lookup",
    minRows: 20,
  },
  {
    id: "punch-hinglish-which-variants-sunroof",
    message: "Punch ke kaunse variants me sunroof hai",
    expectedIntent: "vehicle_feature_discovery",
    expectedCanvasType: "feature_match_builder_canvas",
    expectedContextIsolation: "standalone_model_feature_fast_path",
    expectedRoutingReason: "standalone_model_feature_discovery",
    expectedRows: 10,
  },
  {
    id: "punch-sunroof-variants",
    message: "Punch sunroof variants",
    expectedIntent: "vehicle_feature_discovery",
    expectedCanvasType: "feature_match_builder_canvas",
    expectedContextIsolation: "standalone_model_feature_fast_path",
    expectedRoutingReason: "standalone_model_feature_discovery",
    expectedRows: 10,
  },
  {
    id: "creta-sx-sunroof-stays-variant-path",
    message: "Creta SX sunroof",
    expectedIntent: "vehicle_feature_answer",
    expectedCanvasType: "",
    expectedContextIsolation: "exact_single_feature_fast_path",
    expectedRows: 1,
  },
  {
    id: "creta-vs-seltos-sunroof-stays-comparison",
    message: "Creta vs Seltos sunroof",
    expectedIntent: "vehicle_feature_comparison",
    expectedCanvasType: "feature_comparison_canvas",
    expectedContextIsolation: "explicit_comparison_targets",
    minRows: 1,
  },
];

const withTimeout = (promise, timeoutMs, label) =>
  Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs),
    ),
  ]);

const getRows = (response = {}) =>
  response.rows || response.items || response.data?.rows || response.data?.items || [];

const check = ({ response = {}, rows = [], bridge = {}, testCase = {} }) => {
  const failures = [];

  if (response.intent !== testCase.expectedIntent) {
    failures.push(`intent expected ${testCase.expectedIntent}, got ${response.intent}`);
  }

  if ((response.canvasType || "") !== testCase.expectedCanvasType) {
    failures.push(`canvasType expected ${testCase.expectedCanvasType}, got ${response.canvasType || ""}`);
  }

  if ((bridge.contextIsolation || "") !== testCase.expectedContextIsolation) {
    failures.push(`contextIsolation expected ${testCase.expectedContextIsolation}, got ${bridge.contextIsolation || ""}`);
  }

  if (
    testCase.expectedRoutingReason &&
    (bridge.routingReason || "") !== testCase.expectedRoutingReason
  ) {
    failures.push(`routingReason expected ${testCase.expectedRoutingReason}, got ${bridge.routingReason || ""}`);
  }

  if (Number.isFinite(testCase.expectedRows) && rows.length !== testCase.expectedRows) {
    failures.push(`rowCount expected ${testCase.expectedRows}, got ${rows.length}`);
  }

  if (Number.isFinite(testCase.minRows) && rows.length < testCase.minRows) {
    failures.push(`rowCount expected >= ${testCase.minRows}, got ${rows.length}`);
  }

  if (response.usedGemini === true || bridge.usedGemini === true) {
    failures.push("usedGemini should be false for this deterministic fast path");
  }

  return failures;
};

(async () => {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  await connectDB();

  const { chatWithAgent } = await import("../../services/aiAgent/aiAgent.service.js");

  const results = [];

  for (const testCase of cases) {
    const startedAt = Date.now();

    try {
      const response = await withTimeout(
        chatWithAgent({
          message: testCase.message,
          context: {},
          user: null,
          session: {},
          meta: {
            source: "aci_standalone_model_feature_fast_path_audit_v1",
            caseId: testCase.id,
          },
        }),
        CASE_TIMEOUT_MS,
        testCase.id,
      );

      const durationMs = Date.now() - startedAt;
      const rows = getRows(response);
      const bridge = response.aciCoreBridge || response.meta?.aciCoreBridge || {};
      const failures = check({ response, rows, bridge, testCase });

      results.push({
        id: testCase.id,
        pass: failures.length === 0,
        durationMs,
        failures,
        summary: {
          message: testCase.message,
          intent: response.intent,
          tool: response.tool,
          canvasType: response.canvasType || "",
          title: response.title || "",
          rowCount: rows.length,
          contextIsolation: bridge.contextIsolation || "",
          routingReason: bridge.routingReason || "",
          usedGemini: Boolean(response.usedGemini || bridge.usedGemini),
          answerPreview: String(response.answer || "").slice(0, 180),
        },
      });
    } catch (error) {
      results.push({
        id: testCase.id,
        pass: false,
        durationMs: Date.now() - startedAt,
        failures: [error.message],
        summary: {
          message: testCase.message,
        },
      });
    }
  }

  const failed = results.filter((result) => !result.pass);

  const payload = {
    suite: "ACI Standalone Model Feature Fast Path Audit v1",
    ok: failed.length === 0,
    backendOnly: true,
    frontendEvaluated: false,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((result) => result.id),
    results,
  };

  console.log(JSON.stringify(payload, null, 2));

  const mongoose = require("mongoose");
  if (mongoose.connection?.readyState) await mongoose.disconnect();

  if (failed.length) process.exit(1);
})();
