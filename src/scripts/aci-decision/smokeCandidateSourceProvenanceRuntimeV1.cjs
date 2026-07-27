#!/usr/bin/env node
require("dotenv/config");

const mongoose = require("mongoose");

const getRows = (response = {}) =>
  response.rows ||
  response.data?.rows ||
  response.items ||
  response.data?.items ||
  [];

const getSummary = (response = {}) =>
  response.candidateSourceProvenance ||
  response.data?.candidateSourceProvenance ||
  response.meta?.candidateSourceProvenance ||
  response.contextPatch?.candidateSourceProvenance ||
  null;

const cases = [
  {
    id: "family-auto-suv-source-provenance",
    message: "Recommend automatic SUV for family city use under 18 lakh in Delhi",
  },
  {
    id: "final-choice-source-provenance",
    message:
      "I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?",
  },
];

const forbiddenBuyerTerms = [
  "candidateSourceProvenance",
  "source_provenance",
  "db_current_usable",
  "identity_unverified",
  "derived_only",
];

const main = async () => {
  const connectModule = await import("../../config/db.js");
  const aiAgentModule = await import("../../services/aiAgent/aiAgent.service.js");

  const connectDB = connectModule.default || connectModule.connectDB;
  const chatWithAgent = aiAgentModule.chatWithAgent;

  if (typeof connectDB !== "function") {
    throw new Error("connectDB export not found");
  }

  if (typeof chatWithAgent !== "function") {
    throw new Error("chatWithAgent export not found");
  }

  await connectDB();

  const results = [];

  for (const testCase of cases) {
    const response = await chatWithAgent({
      message: testCase.message,
      context: {},
    });

    const rows = getRows(response);
    const summary = getSummary(response);

    const rowProvenance = rows
      .slice(0, 8)
      .map((row) => row.candidateSourceProvenance || null)
      .filter(Boolean);

    const answer = String(response.answer || "");

    const result = {
      id: testCase.id,
      title: response.title,
      answer,
      summary: summary
        ? {
            version: summary.version,
            status: summary.status,
            candidateCount: summary.candidateCount,
            strongCount: summary.strongCount,
            goodCount: summary.goodCount,
            limitedCount: summary.limitedCount,
            weakCount: summary.weakCount,
            diagnosticAllowedCount: summary.diagnosticAllowedCount,
            finalEligibleCount: summary.finalEligibleCount,
            finalRecommendationEnabled: summary.finalRecommendationEnabled,
          }
        : null,
      topRows: rows.slice(0, 5).map((row) => ({
        model: row.fullModel || row.displayName || row.model,
        modelKey: row.modelKey,
        provenance: row.candidateSourceProvenance
          ? {
              status: row.candidateSourceProvenance.status,
              band: row.candidateSourceProvenance.band,
              diagnosticUseAllowed: row.candidateSourceProvenance.diagnosticUseAllowed,
              bodyTypes: row.candidateSourceProvenance.evidence?.bodyTypes || [],
              vehicleCount: row.candidateSourceProvenance.evidence?.vehicleCount || 0,
              priceRowCount: row.candidateSourceProvenance.evidence?.priceRowCount || 0,
              featureRowCount: row.candidateSourceProvenance.evidence?.featureRowCount || 0,
            }
          : null,
      })),
      safety: {
        hasSummary: Boolean(summary),
        hasRowProvenance: rowProvenance.length > 0,
        hasBuyerLeakLanguage: forbiddenBuyerTerms.some((term) =>
          answer.toLowerCase().includes(term.toLowerCase()),
        ),
      },
    };

    const failReasons = [];

    if (!result.safety.hasSummary) {
      failReasons.push("missing_source_provenance_summary");
    }

    if (!result.safety.hasRowProvenance) {
      failReasons.push("missing_row_source_provenance");
    }

    if (result.safety.hasBuyerLeakLanguage) {
      failReasons.push("source_provenance_internal_language_leaked");
    }

    if (summary?.finalRecommendationEnabled !== false) {
      failReasons.push("final_recommendation_not_disabled");
    }

    results.push({
      ...result,
      ok: failReasons.length === 0,
      failReasons,
    });
  }

  const ok = results.every((item) => item.ok);

  console.log(
    JSON.stringify(
      {
        suite: "ACI candidate source provenance runtime smoke v1",
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
