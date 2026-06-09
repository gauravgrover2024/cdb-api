require("dotenv/config");

const CASE_TIMEOUT_MS = Number(process.env.ACI_STANDALONE_FEATURE_COMPARISON_TIMEOUT_MS || 9000);
const MAX_FAST_PATH_DURATION_MS = Number(process.env.ACI_STANDALONE_FEATURE_COMPARISON_MAX_MS || 6000);

const withTimeout = (promise, ms, label) =>
  Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms),
    ),
  ]);

const getRows = (response = {}) =>
  response.rows || response.items || response.data?.rows || response.data?.items || [];

const getBridge = (response = {}) =>
  response.aciCoreBridge || response.meta?.aciCoreBridge || {};

(async () => {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  await connectDB();

  const bridgeMod = await import("../../services/aciCore/integration/aciCoreLiveBridge.service.js");
  const runAciCoreLiveBridge = bridgeMod.runAciCoreLiveBridge;

  // Warm the DB/entity/feature resolver path once. This audit validates the
  // deterministic fast path's warm routing/performance, not Node process cold start.
  await withTimeout(
    runAciCoreLiveBridge({
      message: "Creta vs Seltos sunroof",
      context: {},
      meta: {
        audit: "aci_standalone_feature_comparison_fast_path_v1",
        warmup: true,
      },
    }),
    CASE_TIMEOUT_MS,
    "warmup-creta-vs-seltos-sunroof",
  );

  const cases = [
    {
      id: "creta-vs-seltos-sunroof",
      message: "Creta vs Seltos sunroof",
      expectedIntent: "vehicle_feature_comparison",
      expectedCanvasType: "feature_comparison_canvas",
      expectedIsolation: "standalone_model_feature_comparison_fast_path",
      minRows: 1,
      maxDurationMs: MAX_FAST_PATH_DURATION_MS,
    },
    {
      id: "creta-vs-seltos-six-airbags",
      message: "Creta vs Seltos 6 airbags",
      expectedIntent: "vehicle_feature_comparison",
      expectedCanvasType: "feature_comparison_canvas",
      expectedIsolation: "standalone_model_feature_comparison_fast_path",
      minRows: 1,
      maxDurationMs: MAX_FAST_PATH_DURATION_MS,
    },
    {
      id: "creta-and-seltos-sunroof-comparison",
      message: "Creta and Seltos sunroof comparison",
      expectedIntent: "vehicle_feature_comparison",
      expectedCanvasType: "feature_comparison_canvas",
      expectedIsolation: "standalone_model_feature_comparison_fast_path",
      minRows: 1,
      maxDurationMs: MAX_FAST_PATH_DURATION_MS,
    },
    {
      id: "compare-creta-seltos-on-sunroof",
      message: "compare Creta and Seltos on sunroof",
      expectedIntent: "vehicle_feature_comparison",
      expectedCanvasType: "feature_comparison_canvas",
      expectedIsolation: "standalone_model_feature_comparison_fast_path",
      minRows: 1,
      maxDurationMs: MAX_FAST_PATH_DURATION_MS,
    },
    {
      id: "plain-creta-vs-seltos-not-feature-fast-path",
      message: "Creta vs Seltos",
      expectedNotIsolation: "standalone_model_feature_comparison_fast_path",
      minRows: 1,
    },
  ];

  const results = [];

  for (const testCase of cases) {
    const startedAt = Date.now();
    const failures = [];

    let response = null;
    try {
      response = await withTimeout(
        runAciCoreLiveBridge({
          message: testCase.message,
          context: {},
          meta: {
            audit: "aci_standalone_feature_comparison_fast_path_v1",
          },
        }),
        CASE_TIMEOUT_MS,
        testCase.id,
      );
    } catch (error) {
      failures.push(error.message || String(error));
    }

    const durationMs = Date.now() - startedAt;
    const rows = getRows(response || {});
    const bridge = getBridge(response || {});

    if (testCase.expectedIntent && response?.intent !== testCase.expectedIntent) {
      failures.push(`expected intent ${testCase.expectedIntent}, got ${response?.intent || ""}`);
    }

    if (testCase.expectedCanvasType && response?.canvasType !== testCase.expectedCanvasType) {
      failures.push(`expected canvasType ${testCase.expectedCanvasType}, got ${response?.canvasType || ""}`);
    }

    if (testCase.expectedIsolation && bridge.contextIsolation !== testCase.expectedIsolation) {
      failures.push(`expected contextIsolation ${testCase.expectedIsolation}, got ${bridge.contextIsolation || ""}`);
    }

    if (testCase.expectedNotIsolation && bridge.contextIsolation === testCase.expectedNotIsolation) {
      failures.push(`plain comparison incorrectly used ${testCase.expectedNotIsolation}`);
    }

    if (typeof testCase.minRows === "number" && rows.length < testCase.minRows) {
      failures.push(`expected at least ${testCase.minRows} rows, got ${rows.length}`);
    }

    if (testCase.maxDurationMs && durationMs > testCase.maxDurationMs) {
      failures.push(`duration ${durationMs}ms exceeded ${testCase.maxDurationMs}ms`);
    }

    if (bridge.usedGemini !== false) {
      failures.push(`expected usedGemini false, got ${bridge.usedGemini}`);
    }

    if (
      testCase.expectedIsolation &&
      !String(response?.sourceCollections || "").includes("vehicle_variant_feature_matrix_v2")
    ) {
      failures.push("missing vehicle_variant_feature_matrix_v2 source collection");
    }

    results.push({
      id: testCase.id,
      pass: failures.length === 0,
      durationMs,
      failures,
      summary: {
        message: testCase.message,
        intent: response?.intent || "",
        tool: response?.tool || "",
        canvasType: response?.canvasType || "",
        title: response?.title || "",
        rowCount: rows.length,
        contextIsolation: bridge.contextIsolation || "",
        routingReason: bridge.routingReason || "",
        usedGemini: bridge.usedGemini,
        sourceCollections: response?.sourceCollections || [],
        dataStatus: response?.dataStatus || "",
        answerPreview: String(response?.answer || "").slice(0, 180),
      },
    });
  }

  const failed = results.filter((item) => !item.pass);

  console.log(JSON.stringify({
    suite: "ACI Standalone Feature Comparison Fast Path Audit v1",
    ok: failed.length === 0,
    backendOnly: true,
    frontendEvaluated: false,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    results,
  }, null, 2));

  const mongoose = require("mongoose");
  if (mongoose.connection?.readyState) await mongoose.disconnect();

  if (failed.length) process.exit(1);
})();
