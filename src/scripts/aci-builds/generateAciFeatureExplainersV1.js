#!/usr/bin/env node

import "dotenv/config";

import fs from "fs";
import path from "path";
import mongoose from "mongoose";
import { z } from "zod";

import {
  generateAciStructuredObject,
} from "../../services/aciCore/llm/aciGeminiClient.js";

const CATALOG_COLLECTION = "vehicle_feature_catalog_v2";
const EXPLAINER_COLLECTION = "aci_feature_explainers_v1";
const SCHEMA_VERSION = "aci_feature_explainer_v1";
const PROMPT_VERSION = "aci_feature_explainer_generation_v1.2";
const CHECKPOINT_PATH = "/tmp/aci_feature_explainer_generation_v1.json";

const args = process.argv.slice(2);
const hasArg = (name) => args.includes(name);
const argValue = (name, fallback = "") => {
  const prefix = `${name}=`;
  const match = args.find((arg) => arg.startsWith(prefix));
  return match ? match.slice(prefix.length) : fallback;
};

const SHOULD_WRITE = hasArg("--write");
const SHOULD_PUBLISH = hasArg("--publish");
const FORCE = hasArg("--force");
const LIMIT = Math.max(0, Number(argValue("--limit", 0)) || 0);
const GROUP = String(argValue("--group", "")).trim();
const KEYS = String(argValue("--keys", ""))
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const BATCH_SIZE = Math.min(10, Math.max(1, Number(argValue("--batch-size", 8)) || 8));
const WORKERS = Math.min(4, Math.max(1, Number(argValue("--workers", 1)) || 1));
const MODEL_CALL_INTERVAL_MS = Math.max(
  1_000,
  Number(argValue("--call-interval-ms", 3_500)) || 3_500,
);
const MODEL_CALL_MAX_ATTEMPTS = Math.max(
  1,
  Number(argValue("--call-max-attempts", 8)) || 8,
);
const MODEL = String(
  argValue("--model", process.env.ACI_FEATURE_EXPLAINER_GENERATION_MODEL || "gemini-3.1-flash-lite"),
).trim();
const FALLBACK_MODEL = String(
  process.env.ACI_FEATURE_EXPLAINER_GENERATION_FALLBACK_MODEL ||
  "gemini-2.5-flash-lite",
).trim();

const ImportanceLevel = z.enum(["critical", "high", "medium", "low", "not_applicable"]);
const FeatureType = z.enum([
  "active_safety",
  "passive_safety",
  "driver_assistance",
  "comfort",
  "convenience",
  "infotainment",
  "connectivity",
  "exterior",
  "interior",
  "engine_drivetrain",
  "chassis_control",
  "performance_metric",
  "dimension_metric",
  "charging_metric",
  "ownership_utility",
  "other",
]);
const DecisionCategory = z.enum([
  "must_have_safety",
  "high_value_safety",
  "daily_comfort",
  "family_practicality",
  "highway_convenience",
  "city_convenience",
  "off_road_capability",
  "performance_preference",
  "ev_ownership",
  "technology_preference",
  "cosmetic_preference",
  "specification_context",
]);

const GeneratedEntrySchema = z.object({
  canonicalKey: z.string().min(1),
  buyerSummary: z.string().min(20).max(360),
  howItWorks: z.string().min(20).max(500),
  whenItMattersSummary: z.string().min(15).max(360),
  whenItMatters: z.array(z.string().min(3).max(120)).min(2).max(5),
  limitationsSummary: z.string().min(15).max(420),
  buyerAdvice: z.string().min(15).max(360),
  featureType: FeatureType,
  decisionCategory: DecisionCategory,
  decisionSignals: z.array(z.string().min(3).max(100)).min(1).max(5),
  importance: z.object({
    safety: ImportanceLevel,
    cityUse: ImportanceLevel,
    highwayUse: ImportanceLevel,
    familyUse: ImportanceLevel,
    offRoadUse: ImportanceLevel,
    chauffeurUse: ImportanceLevel,
    firstTimeBuyer: ImportanceLevel,
  }),
  qualityScore: z.number().min(0).max(1),
  qualityNotes: z.array(z.string().max(180)).max(5),
  publishable: z.boolean(),
});

const BatchSchema = z.object({
  entries: z.array(GeneratedEntrySchema).min(1).max(10),
});

const clean = (value = "") => String(value ?? "").replace(/\s+/g, " ").trim();
const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const chunk = (items = [], size = 8) => {
  const output = [];
  for (let index = 0; index < items.length; index += size) {
    output.push(items.slice(index, index + size));
  }
  return output;
};
const sleep = (durationMs = 0) =>
  new Promise((resolve) => setTimeout(resolve, Math.max(0, durationMs)));

let lastModelCallStartedAt = 0;
let modelCallStartQueue = Promise.resolve();

const reserveModelCallStart = () => {
  const reservation = modelCallStartQueue.then(async () => {
    const sinceLastCall = Date.now() - lastModelCallStartedAt;
    if (sinceLastCall < MODEL_CALL_INTERVAL_MS) {
      await sleep(MODEL_CALL_INTERVAL_MS - sinceLastCall);
    }
    lastModelCallStartedAt = Date.now();
  });
  modelCallStartQueue = reservation.catch(() => {});
  return reservation;
};

const retryDelayMs = (error = {}, attempt = 1) => {
  const message = clean(error?.message);
  const retrySeconds = Number(message.match(/retry in\s+([\d.]+)s/i)?.[1] || 0);
  if (retrySeconds > 0) {
    const reportedDelayMs = Math.ceil((retrySeconds + 3) * 1_000);
    return /quota|rate.?limit|429/i.test(message)
      ? Math.max(70_000, reportedDelayMs)
      : reportedDelayMs;
  }
  if (/high demand|temporar(?:y|ily)|unavailable|429|quota|rate.?limit/i.test(message)) {
    return Math.min(75_000, 10_000 * attempt);
  }
  return 0;
};

const runPacedModelCall = async (operation, label = "model_call") => {
  for (let attempt = 1; attempt <= MODEL_CALL_MAX_ATTEMPTS; attempt += 1) {
    await reserveModelCallStart();

    try {
      return await operation();
    } catch (error) {
      const delayMs = retryDelayMs(error, attempt);
      if (!delayMs || attempt === MODEL_CALL_MAX_ATTEMPTS) throw error;
      console.error(JSON.stringify({
        event: "feature_explainer_model_retry",
        label,
        attempt,
        delayMs,
        error: clean(error.message).slice(0, 500),
      }));
      await sleep(delayMs);
    }
  }
  throw new Error(`${label} exhausted retries`);
};

const mongoUri = () =>
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.MONGODB_URL ||
  process.env.MONGO_URL ||
  process.env.DATABASE_URL ||
  "";

const SYSTEM_PROMPT = `
You are the senior automotive feature educator for ACI Assist, an Indian new-car decision product.

Create buyer-facing explanations for the exact canonical feature records supplied. These explanations are educational content only. Vehicle availability, prices, variants and specifications come from separate database read models.

Hard rules:
- Never claim that any make, model or variant has a feature.
- Never invent a price, measurement, legal mandate, test result, safety rating or market fact.
- Never name a car brand, model, portal or manufacturer.
- Explain the exact feature or metric, not a vaguely related feature.
- Do not infer a more advanced sub-feature from a broad name. Adaptive cruise control does not automatically imply stop-and-go operation or a full halt; high-beam assist does not automatically imply matrix-light selective shading; sunroof does not automatically imply panoramic operation; a camera does not automatically imply recording.
- Lane Keep Assist does not automatically imply continuous lane centring; describe corrective steering support unless the exact name says lane centring. Warning and monitor features alert the driver and do not imply steering or braking intervention. Collision-avoidance features may intervene, but the exact action and operating range vary by implementation.
- Curtain airbags primarily protect heads in side impacts; do not claim rollover deployment unless the exact system says so. Airbag count alone does not prove coverage or crash performance. Safety-rating explanations must tell buyers to check the exact testing programme, protocol year and adult/child scope before comparing scores. Camera activation, image stitching and recording capability vary; a camera name never implies recording.
- Where implementations can differ, say so plainly and tell the buyer to verify the exact variant behavior.
- Use clear Indian English that a first-time buyer can understand.
- Be useful for a purchase decision: explain what it does, where it matters, its limits and whether it is worth prioritising.
- Safety systems assist the driver; never say they guarantee prevention of a crash.
- Driver-assistance systems do not replace an attentive driver.
- For dimensions and performance metrics, explain the trade-off and avoid saying bigger or higher is always better.
- For charging-time records, explain that the shown charger power is the test/input context and actual time depends on the vehicle, battery state, temperature and available charging power.
- For battery warranty, explain what the metric covers and tell the buyer to verify terms; never invent a duration, distance limit or coverage term.
- For cosmetic features, say they are preference-led and should not displace core safety or use-case needs.
- Do not use markdown, emoji, headings, bullets or internal database language inside text fields.
- Keep each field concise and non-repetitive.
- Self-review every entry. Mark publishable false if the feature name is too ambiguous to explain safely.
`;

const REVIEW_SYSTEM_PROMPT = `
You are the adversarial quality editor for ACI Assist automotive feature explanations.

Review every draft against the supplied canonical feature record. Correct it before returning it.

Reject or repair:
- capabilities that are more advanced than the exact feature name;
- lane-keeping described as continuous lane centring, or warning/monitor features described as active intervention;
- safety guarantees or claims that an assistance system replaces the driver;
- vehicle, model, variant, price, legal-mandate or availability claims;
- vague marketing language, repetition and internal jargon;
- advice that does not help an Indian buyer decide whether to prioritise the feature;
- missing implementation caveats where feature behavior can vary by vehicle;
- claims that bigger, faster, more powerful or more screens are automatically better.

Return the same schema and canonicalKey. Set qualityScore independently. Use publishable true only at 0.88 or above after your corrections.
`;

const buildPrompt = (features = []) => `
Generate one explanation for every feature below and return the same canonicalKey unchanged.

Feature records:
${JSON.stringify(features.map((feature) => ({
  canonicalKey: feature.canonicalKey,
  displayName: feature.displayName,
  groupKey: feature.groupKey,
  groupLabel: feature.groupLabel,
  aliases: asArray(feature.aliases).slice(0, 8),
  sections: asArray(feature.sections).slice(0, 5),
  exampleValues: asArray(feature.examples)
    .map((example) => clean(example?.value))
    .filter(Boolean)
    .slice(0, 5),
})), null, 2)}

Quality scoring:
- 0.95-1.00: precise, clear, decision-useful and safely caveated.
- 0.88-0.94: publishable with no material factual or clarity issue.
- below 0.88: not publishable.

Return exactly ${features.length} entries.
`;

const buildReviewPrompt = (features = [], drafts = []) => `
Feature records:
${JSON.stringify(features.map((feature) => ({
  canonicalKey: feature.canonicalKey,
  displayName: feature.displayName,
  groupKey: feature.groupKey,
  groupLabel: feature.groupLabel,
  aliases: asArray(feature.aliases).slice(0, 8),
  exampleValues: asArray(feature.examples)
    .map((example) => clean(example?.value))
    .filter(Boolean)
    .slice(0, 5),
})), null, 2)}

Draft explanations:
${JSON.stringify(drafts, null, 2)}

Return exactly ${features.length} corrected, independently scored entries.
`;

const forbiddenPatterns = [
  /\b(always prevents|guarantees|eliminates all|zero risk|will prevent every)\b/i,
  /\b(carwale|cardekho)\b/i,
  /\b(indexed|database row|mongodb|canonical key)\b/i,
  /₹|\b(?:lakh|crore)\b/i,
];

const hasUnqualifiedScopeClaim = (text = "", pattern) =>
  clean(text)
    .split(/(?<=[.!?])\s+/)
    .some((sentence) =>
      pattern.test(sentence) &&
      !/\b(?:does not|doesn't|do not|don't|may not|might not|not necessarily|not automatically|cannot be assumed|verify|depends|varies|can vary|where supported|if equipped)\b/i.test(
        sentence,
      ));

const auditEntry = (entry = {}, expected = {}) => {
  const issues = [];
  if (entry.canonicalKey !== expected.canonicalKey) issues.push("canonical_key_mismatch");
  if (entry.publishable !== true) issues.push("model_marked_not_publishable");
  if (Number(entry.qualityScore || 0) < 0.88) issues.push("quality_score_below_0_88");

  const text = [
    entry.buyerSummary,
    entry.howItWorks,
    entry.whenItMattersSummary,
    entry.limitationsSummary,
    entry.buyerAdvice,
    ...asArray(entry.whenItMatters),
    ...asArray(entry.decisionSignals),
  ].join(" ");
  if (forbiddenPatterns.some((pattern) => pattern.test(text))) issues.push("forbidden_wording");
  if (/\b(?:this car|this model|this variant|all variants|standard on)\b/i.test(text)) {
    issues.push("vehicle_availability_claim");
  }
  if (
    expected.canonicalKey === "adaptive_cruise_control" &&
    hasUnqualifiedScopeClaim(text, /\bstop(?:-|\s)and(?:-|\s)go\b|\btraffic jam assist\b/i)
  ) {
    issues.push("adaptive_cruise_scope_overreach");
  }
  if (
    expected.canonicalKey === "adaptive_high_beam_assist" &&
    hasUnqualifiedScopeClaim(
      text,
      /selective(?:ly)?\s+(?:shade|shading)|matrix\s+(?:beam|light)/i,
    )
  ) {
    issues.push("high_beam_scope_overreach");
  }
  if (
    expected.canonicalKey === "lane_keep_assist" &&
    hasUnqualifiedScopeClaim(
      text,
      /\bcontinuous(?:ly)?\s+(?:steer|steering|lane)|\blane\s+cent(?:er(?:ing|ed)?|re(?:d|ing)?)/i,
    )
  ) {
    issues.push("lane_keep_scope_overreach");
  }
  if (
    /(?:warning|monitor)$/.test(expected.canonicalKey) &&
    hasUnqualifiedScopeClaim(
      text,
      /\b(?:automatically\s+)?(?:applies?|controls?|provides?)\s+(?:the\s+)?(?:brak|steer)|\bactive\s+(?:braking|steering)\b/i,
    )
  ) {
    issues.push("warning_or_monitor_scope_overreach");
  }
  if (
    expected.canonicalKey === "curtain_airbag" &&
    hasUnqualifiedScopeClaim(text, /\brollover\b/i)
  ) {
    issues.push("curtain_airbag_rollover_scope_overreach");
  }
  if (
    /ncap_(?:child_)?safety_rating$/.test(expected.canonicalKey) &&
    !/\b(?:protocol|test year|assessment year)\b/i.test(text)
  ) {
    issues.push("safety_rating_protocol_caveat_missing");
  }
  if (
    /(?:number_of_airbags|six_airbags)$/.test(expected.canonicalKey) &&
    !/\b(?:position|placement|coverage|crash performance)\b/i.test(text)
  ) {
    issues.push("airbag_count_coverage_caveat_missing");
  }
  if (text.length < 180) issues.push("explanation_too_thin");
  if (expected.canonicalKey === "battery_warranty" && /\b\d+(?:[.,]\d+)?\b/.test(text)) {
    issues.push("battery_warranty_invented_number");
  }
  return issues;
};

const loadCheckpoint = () => {
  try {
    const parsed = JSON.parse(fs.readFileSync(CHECKPOINT_PATH, "utf8"));
    return parsed && typeof parsed === "object" ? parsed : { entries: {} };
  } catch {
    return { entries: {} };
  }
};

const saveCheckpoint = (checkpoint = {}) => {
  fs.writeFileSync(CHECKPOINT_PATH, JSON.stringify(checkpoint, null, 2));
};

async function generateBatch(features = [], modelName = MODEL) {
  const result = await runPacedModelCall(
    () => generateAciStructuredObject({
      schema: BatchSchema,
      system: SYSTEM_PROMPT,
      prompt: buildPrompt(features),
      modelName,
      timeoutMs: 60_000,
      temperature: 0.15,
      maxRetries: 0,
    }),
    `writer:${modelName}`,
  );
  return {
    entries: result.object.entries,
    trace: result.trace,
  };
}

async function generateWithFallback(features = []) {
  try {
    return await generateBatch(features, MODEL);
  } catch (primaryError) {
    if (!FALLBACK_MODEL || FALLBACK_MODEL === MODEL) throw primaryError;
    const fallback = await generateBatch(features, FALLBACK_MODEL);
    return {
      ...fallback,
      primaryError: primaryError.message,
    };
  }
}

async function reviewBatch(features = [], drafts = [], modelName = MODEL) {
  const result = await runPacedModelCall(
    () => generateAciStructuredObject({
      schema: BatchSchema,
      system: REVIEW_SYSTEM_PROMPT,
      prompt: buildReviewPrompt(features, drafts),
      modelName,
      timeoutMs: 60_000,
      temperature: 0,
      maxRetries: 0,
    }),
    `reviewer:${modelName}`,
  );
  return {
    entries: result.object.entries,
    trace: result.trace,
  };
}

async function reviewWithFallback(features = [], drafts = []) {
  try {
    return await reviewBatch(features, drafts, MODEL);
  } catch (primaryError) {
    if (!FALLBACK_MODEL || FALLBACK_MODEL === MODEL) throw primaryError;
    const fallback = await reviewBatch(features, drafts, FALLBACK_MODEL);
    return {
      ...fallback,
      primaryError: primaryError.message,
    };
  }
}

async function generateValidatedBatch(features = []) {
  const first = await generateWithFallback(features);
  const reviewed = await reviewWithFallback(features, first.entries);
  const byKey = new Map(reviewed.entries.map((entry) => [entry.canonicalKey, entry]));
  const accepted = [];
  const retryFeatures = [];

  for (const feature of features) {
    const entry = byKey.get(feature.canonicalKey);
    const issues = entry ? auditEntry(entry, feature) : ["missing_generated_entry"];
    if (!issues.length) {
      accepted.push({
        entry,
        feature,
        trace: {
          writer: first.trace,
          reviewer: reviewed.trace,
        },
      });
    } else {
      retryFeatures.push({ feature, initialIssues: issues });
    }
  }

  for (const retry of retryFeatures) {
    const second = await generateWithFallback([retry.feature]);
    const secondReview = await reviewWithFallback([retry.feature], second.entries);
    const entry = secondReview.entries.find((item) => item.canonicalKey === retry.feature.canonicalKey);
    const issues = entry ? auditEntry(entry, retry.feature) : ["missing_generated_entry_after_retry"];
    if (issues.length) {
      const error = new Error(`${retry.feature.canonicalKey}: ${issues.join(", ")}`);
      error.feature = retry.feature;
      throw error;
    }
    accepted.push({
      entry,
      feature: retry.feature,
      trace: {
        writer: second.trace,
        reviewer: secondReview.trace,
      },
    });
  }

  return accepted;
}

async function main() {
  const uri = mongoUri();
  if (!uri) throw new Error("Mongo URI is required");
  await mongoose.connect(uri);

  const startedAt = Date.now();
  const generationRunId = `feature-explainer-${new Date().toISOString()}`;
  const checkpoint = FORCE ? { entries: {} } : loadCheckpoint();
  checkpoint.entries ||= {};

  try {
    const db = mongoose.connection.db;
    const query = {
      ...(GROUP ? { groupKey: GROUP } : {}),
      ...(KEYS.length ? { canonicalKey: { $in: KEYS } } : {}),
    };
    let catalog = await db.collection(CATALOG_COLLECTION)
      .find(query, {
        projection: {
          _id: 0,
          canonicalKey: 1,
          displayName: 1,
          groupKey: 1,
          groupLabel: 1,
          aliases: 1,
          sections: 1,
          examples: 1,
          modelsCount: 1,
          variantsCount: 1,
          rows: 1,
          availableRows: 1,
          synthetic: 1,
        },
      })
      .sort({ groupKey: 1, canonicalKey: 1 })
      .toArray();

    if (!FORCE) {
      const existing = await db.collection(EXPLAINER_COLLECTION)
        .find(
          { canonicalKey: { $in: catalog.map((item) => item.canonicalKey) }, status: "published" },
          { projection: { _id: 0, canonicalKey: 1 } },
        )
        .toArray();
      const existingKeys = new Set(existing.map((item) => item.canonicalKey));
      catalog = catalog.filter((item) => !existingKeys.has(item.canonicalKey));
    }

    catalog = catalog.filter((item) => !checkpoint.entries[item.canonicalKey]);
    if (LIMIT) catalog = catalog.slice(0, LIMIT);

    const batches = chunk(catalog, BATCH_SIZE);
    const failures = [];
    let generated = 0;

    let nextBatchIndex = 0;
    const processBatch = async (index) => {
      const batch = batches[index];
      try {
        const accepted = await generateValidatedBatch(batch);
        const now = new Date();
        const docs = accepted.map(({ entry, feature, trace }) => ({
          schemaVersion: SCHEMA_VERSION,
          contentVersion: `${new Date().toISOString().slice(0, 10)}.generated`,
          canonicalKey: feature.canonicalKey,
          displayName: feature.displayName,
          groupKey: feature.groupKey,
          groupLabel: feature.groupLabel,
          aliases: asArray(feature.aliases),
          buyerSummary: clean(entry.buyerSummary),
          howItWorks: clean(entry.howItWorks),
          whenItMattersSummary: clean(entry.whenItMattersSummary),
          whenItMatters: asArray(entry.whenItMatters).map(clean),
          limitationsSummary: clean(entry.limitationsSummary),
          buyerAdvice: clean(entry.buyerAdvice),
          featureType: entry.featureType,
          decisionCategory: entry.decisionCategory,
          decisionSignals: asArray(entry.decisionSignals).map(clean),
          importance: entry.importance,
          qualityScore: Number(entry.qualityScore),
          qualityNotes: asArray(entry.qualityNotes).map(clean),
          qualityStatus: "offline_model_reviewed",
          publishable: true,
          status: SHOULD_PUBLISH ? "published" : "draft",
          contentOrigin: "offline_structured_generation",
          generation: {
            runId: generationRunId,
            promptVersion: PROMPT_VERSION,
            writerModel: trace.writer.model,
            writerLatencyMs: trace.writer.latencyMs,
            reviewerModel: trace.reviewer.model,
            reviewerLatencyMs: trace.reviewer.latencyMs,
          },
          catalogStats: {
            modelsCount: feature.modelsCount || 0,
            variantsCount: feature.variantsCount || 0,
            rows: feature.rows || 0,
            availableRows: feature.availableRows || 0,
            synthetic: feature.synthetic === true,
          },
          sourceCatalogCollection: CATALOG_COLLECTION,
          sourceRefs: [],
          reviewedAt: now,
          updatedAt: now,
        }));

        if (SHOULD_WRITE && docs.length) {
          await db.collection(EXPLAINER_COLLECTION).bulkWrite(
            docs.map((doc) => ({
              updateOne: {
                filter: { canonicalKey: doc.canonicalKey },
                update: { $set: doc, $setOnInsert: { createdAt: now } },
                upsert: true,
              },
            })),
            { ordered: true },
          );
        }

        for (const doc of docs) checkpoint.entries[doc.canonicalKey] = doc;
        saveCheckpoint(checkpoint);
        generated += docs.length;
        console.log(JSON.stringify({
          batch: index + 1,
          batches: batches.length,
          generated: docs.length,
          totalGenerated: generated,
          keys: docs.map((doc) => doc.canonicalKey),
        }));
      } catch (error) {
        failures.push({
          batch: index + 1,
          keys: batch.map((item) => item.canonicalKey),
          error: error.message,
        });
        console.error(JSON.stringify(failures[failures.length - 1]));
      }
    };

    const workers = Array.from(
      { length: Math.min(WORKERS, batches.length || 1) },
      async () => {
        while (nextBatchIndex < batches.length) {
          const index = nextBatchIndex;
          nextBatchIndex += 1;
          await processBatch(index);
        }
      },
    );
    await Promise.all(workers);

    if (SHOULD_WRITE) {
      const collection = db.collection(EXPLAINER_COLLECTION);
      await collection.createIndex({ canonicalKey: 1 }, { unique: true });
      await collection.createIndex({ aliases: 1 });
      await collection.createIndex({ status: 1, groupKey: 1 });
      await collection.createIndex({ qualityStatus: 1, qualityScore: -1 });
    }

    const summary = {
      suite: "ACI complete feature explainer generation v1",
      ok: failures.length === 0,
      mode: SHOULD_WRITE ? (SHOULD_PUBLISH ? "write_publish" : "write_draft") : "generate_only",
      model: MODEL,
      fallbackModel: FALLBACK_MODEL,
      workers: WORKERS,
      requestedFeatures: catalog.length,
      generated,
      failed: failures.length,
      failures,
      checkpointPath: CHECKPOINT_PATH,
      durationMs: Date.now() - startedAt,
    };
    console.log(JSON.stringify(summary, null, 2));
    if (!summary.ok) process.exitCode = 1;
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI complete feature explainer generation v1",
    ok: false,
    error: error.message,
  }, null, 2));
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
