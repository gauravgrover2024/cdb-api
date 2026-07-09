#!/usr/bin/env node
"use strict";

const assert = require("assert");
const {
  runAciContextFlowSmokeSuite,
} = require("./helpers/aciContextFlowSmokeHarnessV1.cjs");

const hasBaseThar = (labels = []) =>
  labels.some((label) => /\bthar\b/.test(label) && !/\broxx\b/.test(label));

const scenarios = [
  {
    id: "selected-pronoun-carry",
    steps: [
      {
        id: "creta-price",
        message: "creta price",
        assert({ selectedLabel }) {
          assert(/\bcreta\b/.test(selectedLabel), `Expected Creta, got: ${selectedLabel}`);
        },
      },
      {
        id: "what-about-sunroof",
        message: "what about sunroof",
        assert({ response, selectedLabel, comparisonLabels }) {
          assert(/\bcreta\b/.test(selectedLabel), `Expected Creta carry, got: ${selectedLabel}`);
          assert.strictEqual(comparisonLabels.length, 0, "Pronoun fact follow-up must not create comparison.");
          assert.strictEqual(
            response.contextPatch?.contextTrace?.explicitSingleVehicleTurn,
            false,
            "Pronoun carry must not be recorded as a new explicit vehicle turn.",
          );
        },
      },
    ],
  },
  {
    id: "relative-last-comparison",
    steps: [
      { id: "creta-price", message: "creta price" },
      { id: "thar-abs", message: "thar abs" },
      {
        id: "compare-last-with-thar-roxx",
        message: "compare last with thar roxx",
        assert({ response, comparisonLabels }) {
          const joined = comparisonLabels.join(" | ");
          assert(comparisonLabels.length >= 2, `Expected two vehicles, got: ${joined}`);
          assert(hasBaseThar(comparisonLabels), `Expected base Thar, got: ${joined}`);
          assert(/\broxx\b/.test(joined), `Expected Thar Roxx, got: ${joined}`);
          assert(!/\bcreta\b/.test(joined), `Creta leaked into comparison: ${joined}`);
          assert.strictEqual(
            response.contextPatch?.contextTrace?.relativeReferenceResolved,
            true,
            "Expected deterministic relative-reference trace.",
          );
        },
      },
    ],
  },
  {
    id: "comparison-follow-up",
    steps: [
      { id: "verna-vs-city", message: "verna vs city" },
      {
        id: "which-has-sunroof",
        message: "which has sunroof",
        assert({ response, selectedLabel, comparisonLabels }) {
          const joined = comparisonLabels.join(" | ");
          assert(comparisonLabels.length >= 2, `Comparison was not preserved: ${joined}`);
          assert(/\bverna\b/.test(joined), `Verna missing: ${joined}`);
          assert(/\bcity\b/.test(joined), `City missing: ${joined}`);
          assert(
            !/\b(which|sunroof)\b/.test(selectedLabel),
            `Follow-up words leaked into selected-vehicle metadata: ${selectedLabel}`,
          );
          assert(
            /comparison/.test(response.intent || ""),
            `Expected comparison intent, got: ${response.intent}`,
          );
        },
      },
    ],
  },
  {
    id: "explicit-switch-clears-comparison",
    steps: [
      { id: "verna-vs-city", message: "verna vs city" },
      {
        id: "creta-price",
        message: "creta price",
        assert({ response, selectedLabel, comparisonLabels }) {
          assert(/\bcreta\b/.test(selectedLabel), `Expected Creta, got: ${selectedLabel}`);
          assert.strictEqual(comparisonLabels.length, 0, "Explicit Creta query must clear comparison.");
          assert.strictEqual(
            response.contextPatch?.contextTrace?.comparisonCleared,
            true,
            "Expected comparison-cleared trace.",
          );
        },
      },
    ],
  },
];

runAciContextFlowSmokeSuite({
  suite: "ACI context relative-reference smoke v1",
  scenarios,
})
  .then((output) => {
    console.log(JSON.stringify(output, null, 2));
    if (!output.ok) process.exitCode = 1;
  })
  .catch((error) => {
    console.error(
      JSON.stringify(
        {
          suite: "ACI context relative-reference smoke v1",
          ok: false,
          passed: 0,
          failed: 1,
          failedStepIds: ["suite_setup"],
          error: error.message,
        },
        null,
        2,
      ),
    );
    process.exit(1);
  });
