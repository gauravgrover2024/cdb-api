#!/usr/bin/env node
"use strict";

require("dotenv").config();

const assert = require("assert");
const mongoose = require("mongoose");

const CATALOG_COLLECTION = "vehicle_feature_catalog_v2";
const EXPLAINER_COLLECTION = "aci_feature_explainers_v1";

const mongoUri = () =>
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.MONGODB_URL ||
  process.env.MONGO_URL ||
  process.env.DATABASE_URL ||
  "";

const clean = (value = "") => String(value ?? "").replace(/\s+/g, " ").trim();
const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const requiredTextFields = [
  "buyerSummary",
  "howItWorks",
  "whenItMattersSummary",
  "limitationsSummary",
  "buyerAdvice",
];
const requiredImportanceFields = [
  "safety",
  "cityUse",
  "highwayUse",
  "familyUse",
  "offRoadUse",
  "chauffeurUse",
  "firstTimeBuyer",
];

const hasUnqualifiedScopeClaim = (text = "", pattern) =>
  clean(text)
    .split(/(?<=[.!?])\s+/)
    .some((sentence) =>
      pattern.test(sentence) &&
      !/\b(?:does not|doesn't|do not|don't|may not|might not|not necessarily|not automatically|cannot be assumed|verify|depends|varies|can vary|where supported|if equipped)\b/i.test(
        sentence,
      ));

async function main() {
  const uri = mongoUri();
  assert(uri, "Mongo URI is required for the feature explainer coverage audit");
  await mongoose.connect(uri);

  try {
    const db = mongoose.connection.db;
    const [catalog, explainers] = await Promise.all([
      db.collection(CATALOG_COLLECTION)
        .find({}, { projection: { _id: 0, canonicalKey: 1, groupKey: 1 } })
        .toArray(),
      db.collection(EXPLAINER_COLLECTION)
        .find({ status: "published" })
        .toArray(),
    ]);

    const catalogKeys = new Set(catalog.map((item) => item.canonicalKey));
    const explainersByKey = new Map();
    const duplicateKeys = [];
    for (const row of explainers) {
      if (explainersByKey.has(row.canonicalKey)) duplicateKeys.push(row.canonicalKey);
      explainersByKey.set(row.canonicalKey, row);
    }

    const missingKeys = [...catalogKeys].filter((key) => !explainersByKey.has(key));
    const orphanKeys = [...explainersByKey.keys()].filter((key) => !catalogKeys.has(key));
    const invalidRows = [];

    for (const [key, row] of explainersByKey) {
      const issues = [];
      for (const field of requiredTextFields) {
        if (clean(row[field]).length < 15) issues.push(`${field}_missing_or_thin`);
      }
      if (asArray(row.whenItMatters).length < 2) issues.push("when_it_matters_too_thin");
      if (!row.importance || typeof row.importance !== "object") issues.push("importance_missing");
      if (row.contentOrigin === "offline_structured_generation") {
        if (row.publishable !== true) issues.push("generated_not_publishable");
        if (Number(row.qualityScore || 0) < 0.88) issues.push("generated_quality_below_0_88");
        if (clean(row.qualityStatus) !== "offline_model_reviewed") issues.push("generated_quality_status_invalid");
        if (!clean(row.featureType)) issues.push("generated_feature_type_missing");
        if (!clean(row.decisionCategory)) issues.push("generated_decision_category_missing");
        if (!asArray(row.decisionSignals).length) issues.push("generated_decision_signals_missing");
        for (const field of requiredImportanceFields) {
          if (!clean(row.importance?.[field])) issues.push(`generated_importance_${field}_missing`);
        }
        if (!clean(row.generation?.writerModel) || !clean(row.generation?.reviewerModel)) {
          issues.push("generated_two_pass_provenance_missing");
        }
        if (clean(row.sourceCatalogCollection) !== CATALOG_COLLECTION) {
          issues.push("generated_catalog_provenance_missing");
        }
      }
      const text = requiredTextFields.map((field) => clean(row[field])).join(" ");
      if (/\b(always prevents|guarantees|zero risk|will prevent every)\b/i.test(text)) {
        issues.push("unsafe_absolute_claim");
      }
      if (/\b(this car|this model|this variant|all variants|standard on)\b/i.test(text)) {
        issues.push("vehicle_availability_claim");
      }
      if (
        key === "lane_keep_assist" &&
        hasUnqualifiedScopeClaim(
          text,
          /\bcontinuous(?:ly)?\s+(?:steer|steering|lane)|\blane\s+cent(?:er(?:ing|ed)?|re(?:d|ing)?)/i,
        )
      ) {
        issues.push("lane_keep_scope_overreach");
      }
      if (
        /(?:warning|monitor)$/.test(key) &&
        hasUnqualifiedScopeClaim(
          text,
          /\b(?:automatically\s+)?(?:applies?|controls?|provides?)\s+(?:the\s+)?(?:brak|steer)|\bactive\s+(?:braking|steering)\b/i,
        )
      ) {
        issues.push("warning_or_monitor_scope_overreach");
      }
      if (
        key === "curtain_airbag" &&
        hasUnqualifiedScopeClaim(text, /\brollover\b/i)
      ) {
        issues.push("curtain_airbag_rollover_scope_overreach");
      }
      if (
        /ncap_(?:child_)?safety_rating$/.test(key) &&
        !/\b(?:protocol|test year|assessment year)\b/i.test(text)
      ) {
        issues.push("safety_rating_protocol_caveat_missing");
      }
      if (
        /(?:number_of_airbags|six_airbags)$/.test(key) &&
        !/\b(?:position|placement|coverage|crash performance)\b/i.test(text)
      ) {
        issues.push("airbag_count_coverage_caveat_missing");
      }
      if (issues.length) invalidRows.push({ key, issues });
    }

    const groupCoverage = await db.collection(CATALOG_COLLECTION).aggregate([
      {
        $lookup: {
          from: EXPLAINER_COLLECTION,
          localField: "canonicalKey",
          foreignField: "canonicalKey",
          as: "explainers",
        },
      },
      {
        $group: {
          _id: "$groupKey",
          catalog: { $sum: 1 },
          explained: {
            $sum: {
              $cond: [
                {
                  $gt: [
                    {
                      $size: {
                        $filter: {
                          input: "$explainers",
                          as: "explainer",
                          cond: { $eq: ["$$explainer.status", "published"] },
                        },
                      },
                    },
                    0,
                  ],
                },
                1,
                0,
              ],
            },
          },
        },
      },
      { $sort: { _id: 1 } },
    ]).toArray();

    const failures = [];
    if (missingKeys.length) failures.push(`${missingKeys.length} catalog features missing explainers`);
    if (orphanKeys.length) failures.push(`${orphanKeys.length} explainer keys missing from catalog`);
    if (duplicateKeys.length) failures.push(`${duplicateKeys.length} duplicate explainer keys`);
    if (invalidRows.length) failures.push(`${invalidRows.length} invalid explainer rows`);

    const summary = {
      suite: "ACI Feature Explainer complete coverage audit v1",
      ok: failures.length === 0,
      total: catalog.length,
      passed: catalog.length - missingKeys.length - invalidRows.length,
      failed: failures.length,
      failedIds: failures.map((_, index) => `feature-explainer-coverage-${index + 1}`),
      coveragePercent: catalog.length ? Number(((explainersByKey.size / catalog.length) * 100).toFixed(2)) : 0,
      catalogCount: catalog.length,
      publishedExplainerCount: explainersByKey.size,
      missingKeys,
      orphanKeys,
      duplicateKeys,
      invalidRows: invalidRows.slice(0, 100),
      groupCoverage,
      failures,
    };

    console.log(JSON.stringify(summary, null, 2));
    if (!summary.ok) process.exit(1);
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI Feature Explainer complete coverage audit v1",
    ok: false,
    error: error.message,
  }, null, 2));
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
