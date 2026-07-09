#!/usr/bin/env node
"use strict";

const assert = require("assert");
const {
  runAciContextFlowSmokeSuite,
} = require("./helpers/aciContextFlowSmokeHarnessV1.cjs");

const switchStep = ({ id, message, expectedModel }) => ({
  id,
  message,
  assert({ response, selectedLabel, comparisonLabels }) {
    assert(
      new RegExp(`\\b${expectedModel}\\b`, "i").test(selectedLabel),
      `Expected ${expectedModel}, got: ${selectedLabel}`,
    );
    assert.strictEqual(
      comparisonLabels.length,
      0,
      `Stale comparison survived ${message}: ${comparisonLabels.join(" | ")}`,
    );
    assert.strictEqual(
      response.contextPatch?.contextTrace?.comparisonCleared,
      true,
      `Missing comparison-cleared trace for ${message}.`,
    );
  },
});

const scenarios = [
  {
    id: "comparison-to-price",
    steps: [
      { id: "verna-vs-city", message: "verna vs city" },
      switchStep({
        id: "creta-price",
        message: "creta price",
        expectedModel: "creta",
      }),
    ],
  },
  {
    id: "comparison-to-feature",
    steps: [
      { id: "verna-vs-city", message: "verna vs city" },
      switchStep({
        id: "thar-abs",
        message: "thar abs",
        expectedModel: "thar",
      }),
    ],
  },
  {
    id: "comparison-to-color",
    steps: [
      { id: "creta-vs-seltos", message: "creta vs seltos" },
      switchStep({
        id: "venue-colors",
        message: "venue colors",
        expectedModel: "venue",
      }),
    ],
  },
];

runAciContextFlowSmokeSuite({
  suite: "ACI context no-stale-comparison smoke v1",
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
          suite: "ACI context no-stale-comparison smoke v1",
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
