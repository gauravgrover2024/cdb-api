#!/usr/bin/env node

require("dotenv/config");

const assert = require("assert");

const CASE_TIMEOUT_MS = Number(process.env.ACI_VARIANT_RESOLVER_CASE_TIMEOUT_MS || 6000);
const EXACT_PRICE_MAX_DURATION_MS = Number(process.env.ACI_VARIANT_RESOLVER_EXACT_PRICE_MAX_MS || 3000);
const FEATURE_MAX_DURATION_MS = Number(process.env.ACI_VARIANT_RESOLVER_FEATURE_MAX_MS || 3500);
const FOLLOWUP_MAX_DURATION_MS = Number(process.env.ACI_VARIANT_RESOLVER_FOLLOWUP_MAX_MS || 3500);

const cases = [
  {
    id: "creta-diesel-variant-list",
    messages: ["Show Creta diesel variants"],
    expectedModel: "creta",
    expectedIntent: "vehicle_pricelist",
    expectedRowCountMin: 2,
    expectSelectedVariantEmpty: true,
    expectedFuel: "diesel",
  },
  {
    id: "creta-sx-price",
    messages: ["Creta SX on-road price Delhi"],
    expectedModel: "creta",
    expectedVariant: "sx",
    expectedIntent: "vehicle_pricelist",
    requireSelectedVariantExact: true,
  },
  {
    id: "creta-sx-o-price",
    messages: ["Creta SX(O) on-road price Delhi"],
    expectedModel: "creta",
    expectedIntent: "clarification",
    allowNoRows: true,
    expectSelectedVariantEmpty: true,
    expectedAnswerIncludes: ["SX(O)", "does not match an exact current variant"],
    forbiddenSelectedVariants: ["sx", "sx o", "sx(o)"],
  },
  {
    id: "creta-sx-o-show-model-price-followup",
    messages: ["Creta SX(O) on-road price Delhi", "show model price"],
    expectedModel: "creta",
    expectedIntent: "vehicle_pricelist",
    expectedRowCountMin: 2,
    expectSelectedVariantEmpty: true,
    forbiddenSelectedVariants: ["sx", "sx o", "sx(o)"],
    maxFinalDurationMs: FOLLOWUP_MAX_DURATION_MS,
    stepExpectations: [
      {
        index: 0,
        expectedIntent: "clarification",
        expectSelectedVariantEmpty: true,
        expectedAnswerIncludes: ["SX(O)", "does not match an exact current variant"],
      },
    ],
  },
  {
    id: "creta-sx-o-price-in-delhi-followup",
    messages: ["Creta SX(O) on-road price Delhi", "price in Delhi"],
    expectedModel: "creta",
    expectedIntent: "vehicle_pricelist",
    expectedRowCountMin: 2,
    expectSelectedVariantEmpty: true,
    forbiddenSelectedVariants: ["sx", "sx o", "sx(o)"],
    maxFinalDurationMs: FOLLOWUP_MAX_DURATION_MS,
    stepExpectations: [
      {
        index: 0,
        expectedIntent: "clarification",
        expectSelectedVariantEmpty: true,
        expectedAnswerIncludes: ["SX(O)", "does not match an exact current variant"],
      },
    ],
  },
  {
    id: "seltos-hte-price",
    messages: ["Seltos HTE on-road price Delhi"],
    expectedModel: "seltos",
    expectedVariant: "hte",
    expectedIntent: "vehicle_pricelist",
    requireSelectedVariantExact: true,
  },
  {
    id: "punch-adventure-s-price",
    messages: ["Tata Punch Adventure S price Delhi"],
    expectedModel: "punch",
    expectedVariant: "adventure s",
    expectedIntent: "vehicle_pricelist",
    requireSelectedVariantExact: true,
  },
  {
    id: "punch-adventure-s-features",
    messages: ["Does Tata Punch Adventure S have sunroof and ADAS?"],
    expectedModel: "punch",
    expectedVariant: "adventure s",
    expectedIntent: "vehicle_multi_feature_answer",
    requireSelectedVariantExact: true,
  },
  {
    id: "creta-sx-airbags",
    messages: ["Does Creta SX have 6 airbags?"],
    expectedModel: "creta",
    expectedVariant: "sx",
    expectedIntent: "vehicle_feature_answer",
    requireSelectedVariantExact: true,
  },
  {
    id: "seltos-htx-sunroof",
    messages: ["Does Seltos HTX have sunroof?"],
    expectedModel: "seltos",
    expectedVariant: "htx",
    expectedIntent: "vehicle_feature_answer",
    requireSelectedVariantExact: true,
  },
  {
    id: "variant-context-price-followup",
    messages: ["Does Tata Punch Adventure S have sunroof?", "price in Delhi"],
    expectedModel: "punch",
    expectedVariant: "adventure s",
    expectedIntent: "vehicle_pricelist",
    requireSelectedVariantExact: true,
  },
  {
    id: "variant-context-feature-followup",
    messages: ["Creta SX on-road price Delhi", "does this have sunroof?"],
    expectedModel: "creta",
    expectedVariant: "sx",
    requireSelectedVariantExact: true,
  },
  {
    id: "broad-feature-discovery-clears-variant-context",
    messages: ["Does Tata Punch Adventure S have sunroof?", "which variants have sunroof"],
    expectedModel: "punch",
    expectedIntent: "vehicle_feature_discovery",
    expectedRowCountMin: 2,
    expectSelectedVariantEmpty: true,
    maxFinalDurationMs: FOLLOWUP_MAX_DURATION_MS,
    stepExpectations: [
      {
        index: 0,
        expectedIntent: "vehicle_feature_answer",
        expectedVariant: "adventure s",
        requireSelectedVariantExact: true,
      },
    ],
  },
  {
    id: "creta-sx-ambiguous",
    messages: ["Creta SX price"],
    expectedModel: "creta",
    expectedVariant: "sx",
    expectedIntent: "vehicle_pricelist",
  },
  {
    id: "punch-adventure-ambiguous",
    messages: ["Punch Adventure price Delhi"],
    expectedModel: "punch",
    expectedVariant: "adventure",
    expectedIntent: "vehicle_pricelist",
  },
];

const asArray = (value) => Array.isArray(value) ? value : value ? [value] : [];

const normalize = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const hasVehicle = (vehicle = {}) =>
  Boolean(vehicle.make || vehicle.brand || vehicle.model || vehicle.fullModel || vehicle.displayName);

const getRows = (response = {}) =>
  asArray(response.rows || response.items || response.data?.rows || response.data?.items);

const getSelectedVehicle = (response = {}) =>
  response.contextPatch?.selectedVehicle ||
  response.data?.selectedVehicle ||
  response.selectedVehicle ||
  {};

const summarizeStep = ({ message, response, durationMs }) => {
  const rows = getRows(response);
  const selectedVehicle = getSelectedVehicle(response);

  return {
    message,
    durationMs,
    intent: response.intent || "",
    canvasType: response.canvasType || "",
    title: response.title || "",
    rowCount: rows.length,
    selectedVehicle: {
      make: selectedVehicle.make || selectedVehicle.brand || "",
      model: selectedVehicle.model || "",
      fullModel: selectedVehicle.fullModel || selectedVehicle.displayName || "",
      variant: selectedVehicle.variant || selectedVehicle.variantName || selectedVehicle.selectedVariant || "",
      selectedVariant: selectedVehicle.selectedVariant || "",
      variantKey: selectedVehicle.variantKey || "",
      variantResolutionStatus: selectedVehicle.variantResolutionStatus || "",
      unresolvedVariant: selectedVehicle.unresolvedVariant || "",
      city: selectedVehicle.city || "",
      citySlug: selectedVehicle.citySlug || "",
      source: selectedVehicle.source || "",
      confidence: selectedVehicle.confidence ?? "",
    },
    rows: rows.slice(0, 8).map((row) => ({
      make: row.make || row.brand || "",
      model: row.model || row.fullModel || row.displayName || "",
      variant: row.variant || row.variantName || row.selectedVariant || "",
      fuel: row.fuelType || row.fuel || row.fuelKey || "",
      transmission: row.transmission || row.transmissionType || "",
      city: row.city || row.citySlug || "",
    })),
    answerPreview: String(response.answer || "").slice(0, 300),
  };
};

const mergeContext = (context = {}, response = {}) => {
  const patch = response.contextPatch && typeof response.contextPatch === "object" ? response.contextPatch : {};
  const selectedVehicle = getSelectedVehicle(response);

  return {
    ...context,
    ...patch,
    selectedVehicle: hasVehicle(selectedVehicle) ? selectedVehicle : context.selectedVehicle || {},
    ...(patch.selectedComparisonSet ? { selectedComparisonSet: patch.selectedComparisonSet } : {}),
    ...(patch.activeComparison ? { activeComparison: patch.activeComparison } : {}),
  };
};

const withTimeout = async (promise, ms, label) =>
  Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms),
    ),
  ]);

const exactVariantMatches = (actual = "", expected = "") =>
  normalize(actual) === normalize(expected);

const selectedVariantValues = (step = {}) => [
  step.selectedVehicle?.variant,
  step.selectedVehicle?.selectedVariant,
  step.selectedVehicle?.variantKey,
].filter(Boolean);

const rowVariantBlob = (step = {}) =>
  normalize((step.rows || []).map((row) => row.variant).filter(Boolean).join(" "));

const validateStep = ({ expectation = {}, step = {}, label = "step" }) => {
  const failures = [];
  const selectedVehicleBlob = normalize(JSON.stringify(step.selectedVehicle || {}));
  const rowBlob = rowVariantBlob(step);

  const check = (fn) => {
    try {
      fn();
    } catch (error) {
      failures.push(`${label}: ${error?.message || String(error)}`);
    }
  };

  if (expectation.expectedIntent) {
    check(() =>
      assert.strictEqual(
        step.intent,
        expectation.expectedIntent,
        `expected intent ${expectation.expectedIntent}, got ${step.intent || ""}`,
      ),
    );
  }

  if (expectation.expectedModel) {
    check(() =>
      assert(
        selectedVehicleBlob.includes(normalize(expectation.expectedModel)) ||
          normalize(step.title || "").includes(normalize(expectation.expectedModel)),
        `expected selected model ${expectation.expectedModel}`,
      ),
    );
  }

  if (expectation.expectedVariant) {
    if (expectation.requireSelectedVariantExact) {
      check(() =>
        assert(
          selectedVariantValues(step).some((value) => exactVariantMatches(value, expectation.expectedVariant)),
          `expected selected variant exactly ${expectation.expectedVariant}, got ${JSON.stringify(step.selectedVehicle || {})}`,
        ),
      );
    } else {
      check(() =>
        assert(
          selectedVariantValues(step).some((value) => normalize(value).includes(normalize(expectation.expectedVariant))) ||
            rowBlob.includes(normalize(expectation.expectedVariant)),
          `expected variant ${expectation.expectedVariant}`,
        ),
      );
    }
  }

  if (expectation.expectSelectedVariantEmpty) {
    check(() =>
      assert(
        selectedVariantValues(step).length === 0,
        `expected selected variant to be empty, got ${JSON.stringify(step.selectedVehicle || {})}`,
      ),
    );
  }

  for (const forbidden of expectation.forbiddenSelectedVariants || []) {
    check(() =>
      assert(
        !selectedVariantValues(step).some((value) => exactVariantMatches(value, forbidden)),
        `must not silently resolve to ${forbidden}`,
      ),
    );
  }

  if (expectation.expectedRowCountMin !== undefined) {
    check(() =>
      assert(
        step.rowCount >= expectation.expectedRowCountMin,
        `expected rowCount >= ${expectation.expectedRowCountMin}, got ${step.rowCount}`,
      ),
    );
  }

  if (expectation.expectedFuel) {
    check(() =>
      assert(
        step.rows.length > 0 &&
          step.rows.every((row) =>
            normalize(row.fuel).includes(normalize(expectation.expectedFuel)),
          ),
        `expected every returned row to use ${expectation.expectedFuel}`,
      ),
    );
  }

  for (const snippet of expectation.expectedAnswerIncludes || []) {
    check(() =>
      assert(
        normalize(`${step.answerPreview || ""} ${step.title || ""}`).includes(normalize(snippet)),
        `expected answer/title to include ${snippet}`,
      ),
    );
  }

  if (expectation.maxFinalDurationMs !== undefined) {
    check(() =>
      assert(
        step.durationMs <= expectation.maxFinalDurationMs,
        `duration ${step.durationMs}ms exceeds ${expectation.maxFinalDurationMs}ms`,
      ),
    );
  }

  return failures;
};

const validateFinalStep = ({ testCase, steps }) => {
  const final = steps[steps.length - 1] || {};
  const finalMessage = testCase.messages[testCase.messages.length - 1] || "";
  const expectation = {
    expectedIntent: testCase.expectedIntent,
    expectedModel: testCase.expectedModel,
    expectedVariant: testCase.expectedVariant,
    requireSelectedVariantExact: testCase.requireSelectedVariantExact,
    expectSelectedVariantEmpty: testCase.expectSelectedVariantEmpty,
    forbiddenSelectedVariants: [
      ...(testCase.mustNotResolveVariant ? [testCase.mustNotResolveVariant] : []),
      ...(testCase.forbiddenSelectedVariants || []),
    ],
    expectedRowCountMin:
      testCase.expectedRowCountMin ??
      (/price/i.test(finalMessage) && !testCase.allowNoRows ? 1 : undefined),
    expectedFuel: testCase.expectedFuel,
    expectedAnswerIncludes: testCase.expectedAnswerIncludes,
    maxFinalDurationMs: testCase.maxFinalDurationMs,
  };

  const failures = validateStep({ expectation, step: final, label: "final" });

  const isSingleStepExactPrice =
    testCase.messages.length === 1 &&
    /price|on-road|on road/i.test(finalMessage) &&
    !testCase.allowNoRows;

  if (isSingleStepExactPrice && final.durationMs > EXACT_PRICE_MAX_DURATION_MS) {
    failures.push(`final: exact price resolver duration ${final.durationMs}ms exceeds ${EXACT_PRICE_MAX_DURATION_MS}ms`);
  }

  if (/does .*have|sunroof|airbags|adas/i.test(finalMessage) && final.durationMs > FEATURE_MAX_DURATION_MS) {
    failures.push(`final: feature resolver duration ${final.durationMs}ms exceeds ${FEATURE_MAX_DURATION_MS}ms`);
  }

  if (/\bundefined\b|\bnull\b/i.test(`${final.title || ""} ${final.answerPreview || ""}`)) {
    failures.push("final: response label must not contain undefined/null");
  }

  if (/\b([A-Z][a-z]+)\s+\1\s+[A-Z][A-Za-z0-9-]+\b/.test(`${final.title || ""} ${final.answerPreview || ""}`)) {
    failures.push("final: response label must not contain duplicate make prefix");
  }

  for (const stepExpectation of testCase.stepExpectations || []) {
    const step = steps[stepExpectation.index] || {};
    failures.push(
      ...validateStep({
        expectation: stepExpectation,
        step,
        label: `step[${stepExpectation.index}]`,
      }),
    );
  }

  return failures;
};

async function main() {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  if (typeof connectDB !== "function") throw new Error("connectDB export not found");
  await connectDB();

  const { chatWithAgent } = await import("../../services/aiAgent/aiAgent.service.js");

  const startedAt = Date.now();
  const results = [];

  for (const testCase of cases) {
    let context = {};
    const steps = [];
    const caseStartedAt = Date.now();
    let failures = [];

    try {
      for (const message of testCase.messages) {
        const stepStartedAt = Date.now();

        const response = await withTimeout(
          chatWithAgent({
            message,
            context,
            user: null,
            session: {},
            meta: { source: "auditAciVariantResolverAccuracyV1", caseId: testCase.id },
          }),
          CASE_TIMEOUT_MS,
          `${testCase.id}:${message}`,
        );

        steps.push(summarizeStep({
          message,
          response,
          durationMs: Date.now() - stepStartedAt,
        }));

        context = mergeContext(context, response);
      }

      failures = validateFinalStep({ testCase, steps });
    } catch (error) {
      failures = [error?.message || String(error)];
    }

    results.push({
      id: testCase.id,
      pass: failures.length === 0,
      durationMs: Date.now() - caseStartedAt,
      failures,
      steps,
    });
  }

  const failed = results.filter((item) => !item.pass);

  const output = {
    suite: "ACI Variant Resolver Accuracy Audit v1",
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
  if (mongoose.connection?.readyState) {
    await mongoose.disconnect();
  }

  if (!output.ok) process.exit(1);
}

main().catch(async (error) => {
  console.error(error);
  const mongoose = require("mongoose");
  if (mongoose.connection?.readyState) {
    await mongoose.disconnect();
  }
  process.exit(1);
});
