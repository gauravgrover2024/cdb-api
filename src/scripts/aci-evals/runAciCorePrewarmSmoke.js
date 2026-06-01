import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";

import {
  retrieveAciDbCandidates,
  clearAciCandidateRetrieverCaches,
} from "../../services/aciCore/candidates/aciDbCandidateRetriever.js";

import {
  prewarmAciCoreRuntime,
  getAciCoreRuntimePrewarmState,
} from "../../services/aciCore/aciCore.prewarm.js";

const cases = [
  "Hyundai cars with sunroof under 20 lakh",
  "Verna HX8 iVT vs City ZX CVT",
  "Punch and Nexon CNG sunroof ABS ADAS",
  "Creta SX on-road price Delhi",
];

const nowMs = () => Number(process.hrtime.bigint() / 1000000n);

async function timedCandidateRetrieval(message) {
  const startedAt = nowMs();

  const snapshot = await retrieveAciDbCandidates({
    rawMessage: message,
    normalizedMessage: message,
    activeContext: null,
  });

  return {
    message,
    durationMs: nowMs() - startedAt,
    counts: snapshot?.trace?.counts || null,
    cache: snapshot?.trace?.cache || null,
  };
}

async function main() {
  await connectDB();

  clearAciCandidateRetrieverCaches();

  const prewarm = await prewarmAciCoreRuntime({ force: true, mode: "full" });

  const firstQueryResults = [];
  for (const message of cases) {
    firstQueryResults.push(await timedCandidateRetrieval(message));
  }

  const failures = [];

  if (prewarm.status !== "ready") {
    failures.push(`prewarm status expected ready, got ${prewarm.status}: ${prewarm.error || ""}`);
  }

  const candidateCache = prewarm.results?.find((item) => item.label === "candidate_retriever_catalogs")?.cache || {};

  if (!candidateCache.vehicleEntityIndex?.models) failures.push("vehicle entity model index was not prewarmed");
  if (!candidateCache.vehicleEntityIndex?.variants) failures.push("vehicle entity variant index was not prewarmed");
  if (!candidateCache.makes) failures.push("candidate make catalog was not prewarmed");
  if (!candidateCache.features) failures.push("candidate feature catalog was not prewarmed");
  if (!candidateCache.priceVariants) failures.push("candidate price variant catalog was not prewarmed");

  const slowQueries = firstQueryResults.filter((item) => item.durationMs > 500);
  if (slowQueries.length) {
    failures.push(
      `post-prewarm candidate retrieval expected <=500ms, slow cases: ${
        slowQueries.map((item) => `${item.message}:${item.durationMs}ms`).join(", ")
      }`,
    );
  }

  const response = {
    suite: "ACI Core prewarm smoke",
    ok: failures.length === 0,
    failures,
    prewarm,
    state: getAciCoreRuntimePrewarmState(),
    firstQueryResults,
  };

  console.log(JSON.stringify(response, null, 2));

  await mongoose.disconnect();

  if (failures.length) process.exit(1);
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI Core prewarm smoke",
    ok: false,
    error: error.message,
    stack: error.stack,
  }, null, 2));

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
