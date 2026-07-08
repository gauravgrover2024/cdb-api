#!/usr/bin/env node
require("dotenv/config");

const mongoose = require("mongoose");

const knownRows = [
  { fullModel: "Kia Sonet", model: "Kia Sonet", modelKey: "sonet", rank: 1 },
  { fullModel: "Nissan Gravite", model: "Nissan Gravite", modelKey: "gravite", rank: 2 },
];

const unknownRows = [
  {
    fullModel: "Imaginary Phantom X",
    model: "Imaginary Phantom X",
    modelKey: "imaginary-phantom-x",
    rank: 1,
  },
  {
    fullModel: "Madeup Rocket SUV",
    model: "Madeup Rocket SUV",
    modelKey: "madeup-rocket-suv",
    rank: 2,
  },
];

const mixedRows = [...knownRows, ...unknownRows];

const summarizeRow = (row = {}) => ({
  model: row.fullModel || row.displayName || row.model,
  modelKey: row.modelKey,
  provenance: row.candidateSourceProvenance
    ? {
        status: row.candidateSourceProvenance.status,
        band: row.candidateSourceProvenance.band,
        diagnosticUseAllowed: row.candidateSourceProvenance.diagnosticUseAllowed,
        finalUseAllowed: row.candidateSourceProvenance.finalUseAllowed,
        vehicleCount: row.candidateSourceProvenance.evidence?.vehicleCount || 0,
        priceRowCount: row.candidateSourceProvenance.evidence?.priceRowCount || 0,
        featureRowCount: row.candidateSourceProvenance.evidence?.featureRowCount || 0,
      }
    : null,
});

const runCase = async ({ id, rows, expectations }) => {
  const { evaluateCandidateSourceProvenance } = await import(
    "../../services/aciCore/candidates/aciCandidateSourceProvenance.service.js"
  );

  const summary = await evaluateCandidateSourceProvenance({
    rows,
    buyerContext: {
      citySlug: "new-delhi",
      budgetMaxLakh: 18,
      transmissionPreference: "automatic",
      usage: ["city", "family"],
    },
  });

  const outputRows = Array.isArray(summary.rows) ? summary.rows : [];

  const result = {
    id,
    summary: {
      status: summary.status,
      candidateCount: summary.candidateCount,
      strongCount: summary.strongCount,
      goodCount: summary.goodCount,
      limitedCount: summary.limitedCount,
      weakCount: summary.weakCount,
      diagnosticAllowedCount: summary.diagnosticAllowedCount,
      finalEligibleCount: summary.finalEligibleCount,
      finalRecommendationEnabled: summary.finalRecommendationEnabled,
      canUseForDiagnosticShortlist: summary.canUseForDiagnosticShortlist,
      canUseForFinalRecommendation: summary.canUseForFinalRecommendation,
    },
    rows: outputRows.map(summarizeRow),
    ok: true,
    failReasons: [],
  };

  if (summary.status !== "evaluated") {
    result.failReasons.push("summary_not_evaluated");
  }

  if (summary.finalRecommendationEnabled !== false) {
    result.failReasons.push("final_recommendation_not_disabled");
  }

  if (summary.finalEligibleCount !== 0) {
    result.failReasons.push("unexpected_final_eligible_candidates");
  }

  if (typeof expectations.goodCount === "number" && summary.goodCount !== expectations.goodCount) {
    result.failReasons.push(`good_count_expected_${expectations.goodCount}_got_${summary.goodCount}`);
  }

  if (typeof expectations.weakCount === "number" && summary.weakCount !== expectations.weakCount) {
    result.failReasons.push(`weak_count_expected_${expectations.weakCount}_got_${summary.weakCount}`);
  }

  if (
    typeof expectations.diagnosticAllowedCount === "number" &&
    summary.diagnosticAllowedCount !== expectations.diagnosticAllowedCount
  ) {
    result.failReasons.push(
      `diagnostic_allowed_expected_${expectations.diagnosticAllowedCount}_got_${summary.diagnosticAllowedCount}`,
    );
  }

  if (expectations.unknownRowsBlocked) {
    const unknownOutputRows = outputRows.filter((row) =>
      unknownRows.some((unknown) => unknown.modelKey === row.modelKey),
    );

    for (const row of unknownOutputRows) {
      const provenance = row.candidateSourceProvenance || {};

      if (provenance.band !== "weak") {
        result.failReasons.push(`${row.modelKey}_not_weak`);
      }

      if (provenance.status !== "identity_unverified") {
        result.failReasons.push(`${row.modelKey}_not_identity_unverified`);
      }

      if (provenance.diagnosticUseAllowed !== false) {
        result.failReasons.push(`${row.modelKey}_diagnostic_allowed`);
      }

      if (provenance.finalUseAllowed !== false) {
        result.failReasons.push(`${row.modelKey}_final_allowed`);
      }
    }
  }

  result.ok = result.failReasons.length === 0;

  return result;
};

const main = async () => {
  const connectModule = await import("../../config/db.js");
  const connectDB = connectModule.default || connectModule.connectDB;

  if (typeof connectDB !== "function") {
    throw new Error("connectDB export not found");
  }

  await connectDB();

  const cases = [
    {
      id: "known-candidates-usable",
      rows: knownRows,
      expectations: {
        goodCount: 2,
        weakCount: 0,
        diagnosticAllowedCount: 2,
        unknownRowsBlocked: false,
      },
    },
    {
      id: "unknown-candidates-blocked",
      rows: unknownRows,
      expectations: {
        goodCount: 0,
        weakCount: 2,
        diagnosticAllowedCount: 0,
        unknownRowsBlocked: true,
      },
    },
    {
      id: "mixed-known-and-unknown-candidates",
      rows: mixedRows,
      expectations: {
        goodCount: 2,
        weakCount: 2,
        diagnosticAllowedCount: 2,
        unknownRowsBlocked: true,
      },
    },
  ];

  const results = [];

  for (const testCase of cases) {
    results.push(await runCase(testCase));
  }

  const ok = results.every((result) => result.ok);

  console.log(
    JSON.stringify(
      {
        suite: "ACI candidate source provenance contract smoke v1",
        ok,
        results,
      },
      null,
      2,
    ),
  );

  await mongoose.disconnect();

  if (!ok) {
    process.exit(1);
  }
};

main().catch(async (error) => {
  console.error(error);

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
