#!/usr/bin/env node
require("dotenv/config");

const mongoose = require("mongoose");

const knownRows = [
  {
    fullModel: "Kia Sonet",
    model: "Kia Sonet",
    modelKey: "sonet",
    rank: 1,
    positiveSignals: ["city-use suitability strong", "family practicality good"],
  },
  {
    fullModel: "Nissan Gravite",
    model: "Nissan Gravite",
    modelKey: "gravite",
    rank: 2,
    positiveSignals: ["city-use suitability strong", "family practicality good"],
  },
];

const fakeRows = [
  {
    fullModel: "Imaginary Phantom X",
    model: "Imaginary Phantom X",
    modelKey: "imaginary-phantom-x",
    rank: 3,
    positiveSignals: ["city-use suitability strong", "family practicality good"],
  },
  {
    fullModel: "Madeup Rocket SUV",
    model: "Madeup Rocket SUV",
    modelKey: "madeup-rocket-suv",
    rank: 4,
    positiveSignals: ["city-use suitability strong", "family practicality good"],
  },
];

const buyerContext = {
  citySlug: "new-delhi",
  budgetMaxLakh: 18,
  transmissionPreference: "automatic",
  useCase: ["city", "family"],
  familySize: 4,
};

const fakeModelKeys = new Set(fakeRows.map((row) => row.modelKey));
const fakeNames = fakeRows.map((row) => row.fullModel.toLowerCase());

const rowView = (row = {}) => ({
  model: row.fullModel || row.displayName || row.model,
  modelKey: row.modelKey,
  rank: row.rank || row.diagnosticRanking?.rank,
  sourceBand: row.candidateSourceProvenance?.band,
  sourceStatus: row.candidateSourceProvenance?.status,
  sourceDiagnosticUseAllowed: row.candidateSourceProvenance?.diagnosticUseAllowed,
});

const main = async () => {
  const connectModule = await import("../../config/db.js");
  const sourceModule = await import("../../services/aciCore/candidates/aciCandidateSourceProvenance.service.js");
  const rankingModule = await import("../../services/aciCore/candidates/aciCandidateDiagnosticRanking.service.js");
  const readinessModule = await import("../../services/aciCore/candidates/aciCandidateEvidenceReadiness.service.js");
  const composerModule = await import("../../services/aciCore/candidates/aciDiagnosticShortlistComposer.service.js");

  const connectDB = connectModule.default || connectModule.connectDB;
  const { evaluateCandidateSourceProvenance, filterDiagnosticSourceProvenanceRows } = sourceModule;
  const { buildCandidateDiagnosticRanking, summarizeCandidateDiagnosticRanking } = rankingModule;
  const { buildCandidateEvidenceReadinessContract, summarizeCandidateEvidenceReadiness } = readinessModule;
  const { buildDiagnosticShortlistComposer } = composerModule;

  if (typeof connectDB !== "function") throw new Error("connectDB export not found");

  await connectDB();

  const sourceSummary = await evaluateCandidateSourceProvenance({
    rows: [...knownRows, ...fakeRows],
    buyerContext,
  });

  const sourceRows = Array.isArray(sourceSummary.rows) ? sourceSummary.rows : [];
  const filteredRows = filterDiagnosticSourceProvenanceRows(sourceRows);

  const ranking = buildCandidateDiagnosticRanking({
    rows: filteredRows,
    buyerContext,
    bridge: {},
    response: {},
  });

  const rankedRows = Array.isArray(ranking.rows) ? ranking.rows : [];

  const readiness = buildCandidateEvidenceReadinessContract({
    rows: rankedRows,
    buyerContext,
    bridge: {},
    response: {},
  });

  const readinessRows = Array.isArray(readiness.rows) ? readiness.rows : [];
  const rankingSummary = summarizeCandidateDiagnosticRanking(ranking);
  const readinessSummary = summarizeCandidateEvidenceReadiness(readiness);

  const composer = buildDiagnosticShortlistComposer({
    response: {},
    rows: readinessRows,
    buyerContext,
    candidateDiagnosticRanking: rankingSummary,
    candidateEvidenceReadiness: readinessSummary,
    candidateSourceProvenance: sourceSummary,
  });

  const answer = String(composer.answer || "").toLowerCase();

  const result = {
    suite: "ACI candidate source provenance downstream guard smoke v1",
    sourceSummary: {
      candidateCount: sourceSummary.candidateCount,
      goodCount: sourceSummary.goodCount,
      weakCount: sourceSummary.weakCount,
      diagnosticAllowedCount: sourceSummary.diagnosticAllowedCount,
      finalEligibleCount: sourceSummary.finalEligibleCount,
      finalRecommendationEnabled: sourceSummary.finalRecommendationEnabled,
    },
    sourceRows: sourceRows.map(rowView),
    filteredRows: filteredRows.map(rowView),
    rankedRows: rankedRows.map(rowView),
    readinessRows: readinessRows.map(rowView),
    composer: {
      status: composer.status,
      title: composer.title,
      answer: composer.answer,
      topModels: composer.topModels || [],
      safety: composer.safety || {},
    },
    ok: true,
    failReasons: [],
  };

  if (sourceSummary.goodCount !== 2) result.failReasons.push(`expected_2_good_got_${sourceSummary.goodCount}`);
  if (sourceSummary.weakCount !== 2) result.failReasons.push(`expected_2_weak_got_${sourceSummary.weakCount}`);

  for (const row of filteredRows) {
    if (fakeModelKeys.has(row.modelKey)) result.failReasons.push(`${row.modelKey}_survived_source_filter`);
  }

  for (const row of rankedRows) {
    if (fakeModelKeys.has(row.modelKey)) result.failReasons.push(`${row.modelKey}_survived_ranking`);
  }

  for (const row of readinessRows) {
    if (fakeModelKeys.has(row.modelKey)) result.failReasons.push(`${row.modelKey}_survived_readiness`);
  }

  for (const fakeName of fakeNames) {
    if (answer.includes(fakeName)) result.failReasons.push(`${fakeName}_leaked_into_composer_answer`);
  }

  if (composer.safety?.hasUnsafeFinalLanguage) result.failReasons.push("composer_unsafe_final_language");

  result.ok = result.failReasons.length === 0;

  console.log(JSON.stringify(result, null, 2));

  await mongoose.disconnect();

  if (!result.ok) process.exit(1);
};

main().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
