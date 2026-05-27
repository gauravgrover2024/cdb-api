import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";

import {
  retrieveAciDbCandidates,
  clearAciCandidateRetrieverCaches,
} from "../../services/aciCore/candidates/aciDbCandidateRetriever.js";

import {
  parseDeterministicMeaningFrame,
} from "../../services/aciCore/understanding/deterministicMeaningFrame.parser.js";

const DEFAULT_WARM_RUNS = Number(process.env.ACI_MEANING_PROFILE_RUNS || 8);

const cases = [
  {
    id: "broad-hyundai-sunroof-budget",
    message: "Hyundai cars with sunroof under 20 lakh",
  },
  {
    id: "variant-comparison",
    message: "Verna HX8 iVT vs City ZX CVT",
  },
  {
    id: "extreme-multi-intent",
    message: "Punch and Nexon CNG sunroof ABS ADAS",
  },
  {
    id: "onroad-price",
    message: "Creta SX on-road price Delhi",
  },
];

const nowMs = () => Number(process.hrtime.bigint() / 1000000n);

const percentile = (values = [], p = 50) => {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((p / 100) * sorted.length) - 1),
  );
  return sorted[index];
};

const summarize = (values = []) => ({
  minMs: values.length ? Math.min(...values) : null,
  p50Ms: percentile(values, 50),
  p95Ms: percentile(values, 95),
  maxMs: values.length ? Math.max(...values) : null,
  avgMs: values.length
    ? Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(2))
    : null,
});

async function runSingle({ message }) {
  const totalStart = nowMs();

  const candidateStart = nowMs();
  const candidateSnapshot = await retrieveAciDbCandidates({
    rawMessage: message,
    normalizedMessage: message,
    activeContext: null,
  });
  const candidateMs = nowMs() - candidateStart;

  const parserStart = nowMs();
  const parserResult = await parseDeterministicMeaningFrame({
    rawMessage: message,
    normalizedMessage: message,
    activeContext: null,
    candidateSnapshot,
  });
  const parserMs = nowMs() - parserStart;

  return {
    totalMs: nowMs() - totalStart,
    candidateMs,
    parserMs,
    primaryTask: parserResult.meaningFrame?.primaryTask || null,
    modelCount: parserResult.meaningFrame?.filters?.models?.length || 0,
    variantCount: parserResult.meaningFrame?.filters?.variants?.length || 0,
    featureCount: parserResult.meaningFrame?.filters?.features?.length || 0,
    confidence: parserResult.meaningFrame?.confidence || null,
    candidateCounts: candidateSnapshot?.trace?.counts || null,
  };
}

async function main() {
  await connectDB();

  const results = [];

  for (const item of cases) {
    clearAciCandidateRetrieverCaches();

    const cold = await runSingle({ message: item.message });

    const warmRuns = [];
    for (let i = 0; i < DEFAULT_WARM_RUNS; i += 1) {
      warmRuns.push(await runSingle({ message: item.message }));
    }

    const warmTotal = warmRuns.map((run) => run.totalMs);
    const warmCandidate = warmRuns.map((run) => run.candidateMs);
    const warmParser = warmRuns.map((run) => run.parserMs);

    results.push({
      id: item.id,
      message: item.message,
      cold,
      warmRuns: DEFAULT_WARM_RUNS,
      warm: {
        total: summarize(warmTotal),
        candidate: summarize(warmCandidate),
        parser: summarize(warmParser),
      },
      latestFrameShape: {
        primaryTask: warmRuns[warmRuns.length - 1]?.primaryTask || cold.primaryTask,
        modelCount: warmRuns[warmRuns.length - 1]?.modelCount ?? cold.modelCount,
        variantCount: warmRuns[warmRuns.length - 1]?.variantCount ?? cold.variantCount,
        featureCount: warmRuns[warmRuns.length - 1]?.featureCount ?? cold.featureCount,
        confidence: warmRuns[warmRuns.length - 1]?.confidence || cold.confidence,
      },
    });
  }

  const allWarmTotals = results.flatMap((item) => [
    item.warm.total.p50Ms,
    item.warm.total.p95Ms,
  ]).filter((value) => typeof value === "number");

  const summary = {
    suite: "ACI deterministic meaning baseline profiler",
    ok: true,
    warmRunsPerCase: DEFAULT_WARM_RUNS,
    targetNotes: {
      parser: "Parser should stay near 0-5ms.",
      warmUnderstanding: "Warm candidate + parser should ideally trend below 100-250ms for obvious queries.",
      cold: "Cold values include cache rebuild and are expected to be higher; production should prewarm.",
    },
    aggregateWarm: summarize(allWarmTotals),
    results,
  };

  console.log(JSON.stringify(summary, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI deterministic meaning baseline profiler",
    ok: false,
    error: error.message,
    stack: error.stack,
  }, null, 2));

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
