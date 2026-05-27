import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";

import {
  runAciUnderstandingEngine,
} from "../../services/aciCore/understanding/aciUnderstandingEngine.js";

import {
  retrieveAciDbCandidates,
  clearAciCandidateRetrieverCaches,
} from "../../services/aciCore/candidates/aciDbCandidateRetriever.js";

import {
  parseHybridMeaningFrame,
} from "../../services/aciCore/understanding/hybridMeaningFrame.parser.js";

// Smoke must not burn Gemini quota. It proves high-confidence cases route deterministically.
process.env.ACI_MEANING_PARSER_ENABLED = "false";

const cases = [
  {
    id: "broad-hyundai-sunroof-budget",
    message: "Hyundai cars with sunroof under 20 lakh",
    expected: {
      primaryTask: "vehicle_discovery",
      selectedParser: "deterministic",
      usedGemini: false,
      models: [],
      makes: ["Hyundai"],
      features: ["sunroof"],
      maxBudget: 2000000,
      broadDiscovery: true,
    },
  },
  {
    id: "variant-comparison",
    message: "Verna HX8 iVT vs City ZX CVT",
    expected: {
      primaryTask: "vehicle_comparison",
      selectedParser: "deterministic",
      usedGemini: false,
      models: ["Hyundai Verna", "Honda City"],
      variants: ["Hyundai Verna HX8 iVT", "Honda City ZX CVT"],
      comparisonTargetCount: 2,
    },
  },
  {
    id: "extreme-multi-intent",
    message: "Punch and Nexon CNG sunroof ABS ADAS",
    expected: {
      primaryTask: "vehicle_comparison",
      selectedParser: "deterministic",
      usedGemini: false,
      models: ["Tata Punch", "Tata Nexon"],
      features: ["sunroof", "adas_package", "anti_lock_braking_system_abs"],
      fuelTypes: ["cng"],
      comparisonTargetCount: 2,
    },
  },
  {
    id: "onroad-price",
    message: "Creta SX on-road price Delhi",
    expected: {
      primaryTask: "on_road_estimate",
      selectedParser: "deterministic",
      usedGemini: false,
      models: ["Hyundai Creta"],
      variants: ["Hyundai Creta SX"],
      requestedFactsTrue: ["price", "onRoad"],
    },
  },
];

const lower = (value) => String(value || "").toLowerCase();

const includesAll = (actual = [], expected = []) => {
  const text = actual.map((item) => lower(item)).join(" | ");
  return expected.every((item) => text.includes(lower(item)));
};

const getFrameModelTexts = (frame) => [
  ...(frame.filters?.models || []),
  frame.anchors?.primaryVehicle?.fullModel,
  frame.anchors?.primaryVehicle?.model,
  ...(frame.anchors?.comparisonTargets || []).map((item) => item.fullModel || item.model),
].filter(Boolean);

const getFrameVariantTexts = (frame) => [
  ...(frame.filters?.variants || []),
  frame.anchors?.primaryVehicle?.fullVariant,
  frame.anchors?.primaryVehicle?.variant,
  ...(frame.anchors?.comparisonTargets || []).map((item) => item.fullVariant || item.variant),
].filter(Boolean);

const checkCase = ({ frame, parserResult, expected }) => {
  const failures = [];

  if (expected.primaryTask && frame.primaryTask !== expected.primaryTask) {
    failures.push(`primaryTask expected ${expected.primaryTask}, got ${frame.primaryTask}`);
  }

  const router = parserResult?.trace?.router || {};

  if (expected.selectedParser && router.selectedParser !== expected.selectedParser) {
    failures.push(`router.selectedParser expected ${expected.selectedParser}, got ${router.selectedParser}`);
  }

  if (typeof expected.usedGemini === "boolean" && Boolean(router.usedGemini) !== expected.usedGemini) {
    failures.push(`router.usedGemini expected ${expected.usedGemini}, got ${router.usedGemini}`);
  }

  if (expected.makes && !includesAll(frame.filters?.makes || [], expected.makes)) {
    failures.push(`makes expected ${JSON.stringify(expected.makes)}, got ${JSON.stringify(frame.filters?.makes || [])}`);
  }

  if (expected.models && !includesAll(getFrameModelTexts(frame), expected.models)) {
    failures.push(`models expected ${JSON.stringify(expected.models)}, got ${JSON.stringify(getFrameModelTexts(frame))}`);
  }

  if (expected.variants && !includesAll(getFrameVariantTexts(frame), expected.variants)) {
    failures.push(`variants expected ${JSON.stringify(expected.variants)}, got ${JSON.stringify(getFrameVariantTexts(frame))}`);
  }

  if (expected.features && !includesAll(frame.filters?.features || [], expected.features)) {
    failures.push(`features expected ${JSON.stringify(expected.features)}, got ${JSON.stringify(frame.filters?.features || [])}`);
  }

  if (expected.fuelTypes && !includesAll(frame.filters?.fuelTypes || [], expected.fuelTypes)) {
    failures.push(`fuelTypes expected ${JSON.stringify(expected.fuelTypes)}, got ${JSON.stringify(frame.filters?.fuelTypes || [])}`);
  }

  if (expected.maxBudget && Number(frame.filters?.budget?.max || 0) !== expected.maxBudget) {
    failures.push(`maxBudget expected ${expected.maxBudget}, got ${frame.filters?.budget?.max}`);
  }

  if (typeof expected.broadDiscovery === "boolean" && Boolean(frame.discovery?.isBroadDiscovery) !== expected.broadDiscovery) {
    failures.push(`broadDiscovery expected ${expected.broadDiscovery}, got ${frame.discovery?.isBroadDiscovery}`);
  }

  if (typeof expected.comparisonTargetCount === "number" && (frame.anchors?.comparisonTargets || []).length !== expected.comparisonTargetCount) {
    failures.push(`comparisonTargetCount expected ${expected.comparisonTargetCount}, got ${(frame.anchors?.comparisonTargets || []).length}`);
  }

  for (const fact of expected.requestedFactsTrue || []) {
    if (!frame.requestedFacts?.[fact]) {
      failures.push(`requestedFacts.${fact} expected true`);
    }
  }

  if (frame.clarification?.needed) {
    failures.push(`clarification should not be needed for clear high-confidence smoke case`);
  }

  return failures;
};

async function main() {
  await connectDB();
  clearAciCandidateRetrieverCaches();

  const results = [];
  const failures = [];

  for (const item of cases) {
    const startedAt = Date.now();

    const output = await runAciUnderstandingEngine({
      message: item.message,
      candidateRetriever: retrieveAciDbCandidates,
      parser: parseHybridMeaningFrame,
    });

    const frame = output.meaningFrame;
    const parserResult = output.parserResult;
    const caseFailures = checkCase({ frame, parserResult, expected: item.expected });

    const summary = {
      id: item.id,
      message: item.message,
      pass: caseFailures.length === 0,
      durationMs: Date.now() - startedAt,
      failures: caseFailures,
      primaryTask: frame.primaryTask,
      confidence: frame.confidence,
      filters: frame.filters,
      primaryVehicle: frame.anchors?.primaryVehicle,
      comparisonTargets: frame.anchors?.comparisonTargets,
      requestedFacts: frame.requestedFacts,
      discovery: frame.discovery,
      routerTrace: parserResult?.trace?.router,
      parserTrace: parserResult?.trace,
    };

    results.push(summary);

    if (caseFailures.length) {
      failures.push(summary);
    }
  }

  console.log(JSON.stringify({
    suite: "ACI hybrid meaning-frame router smoke",
    ok: failures.length === 0,
    total: cases.length,
    passed: cases.length - failures.length,
    failed: failures.length,
    failedIds: failures.map((item) => item.id),
    failures,
    results,
  }, null, 2));

  await mongoose.disconnect();

  if (failures.length) process.exit(1);
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI hybrid meaning-frame router smoke",
    ok: false,
    error: error.message,
    stack: error.stack,
  }, null, 2));

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
