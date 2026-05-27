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
  parseDeterministicMeaningFrame,
} from "../../services/aciCore/understanding/deterministicMeaningFrame.parser.js";

const cases = [
  {
    id: "broad-hyundai-sunroof-budget-answer-readiness",
    message: "Hyundai cars with sunroof under 20 lakh",
    expected: {
      primaryTask: "vehicle_discovery",
      broadDiscovery: true,
      makes: ["Hyundai"],
      features: ["sunroof"],
      maxBudget: 2000000,
      requestedFactsTrue: ["features", "price"],
      minConfidence: 0.75,
      forbiddenText: [
        "i cannot",
        "not enough information",
      ],
    },
  },
  {
    id: "variant-comparison-exact-targets-answer-readiness",
    message: "Verna HX8 iVT vs City ZX CVT",
    expected: {
      primaryTask: "vehicle_comparison",
      models: ["Hyundai Verna", "Honda City"],
      variants: ["Hyundai Verna HX8 iVT", "Honda City ZX CVT"],
      comparisonTargetCount: 2,
      resultGranularity: "vehicle_targets",
      requestedFactsTrue: ["comparison"],
      minConfidence: 0.75,
      forbiddenText: [
        "Honda City Hybrid",
        "City Hybrid ZX CVT",
        "Hyundai Verna SX",
        "Verna SX IVT",
      ],
    },
  },
  {
    id: "extreme-multi-intent-answer-readiness",
    message: "Punch and Nexon CNG sunroof ABS ADAS",
    expected: {
      primaryTask: "vehicle_comparison",
      models: ["Tata Punch", "Tata Nexon"],
      features: ["sunroof", "adas_package", "anti_lock_braking_system"],
      fuelTypes: ["cng"],
      comparisonTargetCount: 2,
      resultGranularity: "vehicle_targets",
      requestedFactsTrue: ["features", "comparison"],
      minConfidence: 0.75,
      forbiddenText: [
        "autonomous_parking",
        "lane_keep_assist",
        "forward_collision_warning",
        "blind_spot_monitor",
        "Honda",
        "Hyundai",
      ],
    },
  },
  {
    id: "onroad-price-specific-variant-answer-readiness",
    message: "Creta SX on-road price Delhi",
    expected: {
      primaryTask: "on_road_estimate",
      models: ["Hyundai Creta"],
      variants: ["Hyundai Creta SX"],
      requestedFactsTrue: ["price", "onRoad"],
      resultGranularity: "variant",
      minConfidence: 0.75,
      forbiddenText: [
        "test drive",
        "City Hybrid",
        "Verna",
      ],
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

const getFrameQualityText = (frame) => {
  const values = [];

  const collect = (value) => {
    if (value === null || value === undefined) return;

    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      values.push(String(value));
      return;
    }

    if (Array.isArray(value)) {
      value.forEach(collect);
      return;
    }

    if (typeof value === "object") {
      Object.values(value).forEach(collect);
    }
  };

  collect({
    primaryTask: frame.primaryTask,
    filters: frame.filters,
    anchors: frame.anchors,
    requestedFacts: frame.requestedFacts,
    discovery: frame.discovery,
    clarificationReason: frame.clarification?.reason,
    clarificationQuestion: frame.clarification?.question,
    clarificationOptions: frame.clarification?.options,
    refusalReason: frame.safety?.refusalReason,
    unsupportedReason: frame.safety?.unsupportedReason,
    consentReason: frame.safety?.consentReason,
  });

  return values.join(" | ").toLowerCase();
};

const assertCase = ({ frame, expected }) => {
  const failures = [];

  if (expected.primaryTask && frame.primaryTask !== expected.primaryTask) {
    failures.push(`primaryTask expected ${expected.primaryTask}, got ${frame.primaryTask}`);
  }

  if (typeof expected.broadDiscovery === "boolean" && Boolean(frame.discovery?.isBroadDiscovery) !== expected.broadDiscovery) {
    failures.push(`broadDiscovery expected ${expected.broadDiscovery}, got ${frame.discovery?.isBroadDiscovery}`);
  }

  if (expected.resultGranularity && frame.discovery?.resultGranularity !== expected.resultGranularity) {
    failures.push(`resultGranularity expected ${expected.resultGranularity}, got ${frame.discovery?.resultGranularity}`);
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

  if (typeof expected.comparisonTargetCount === "number" && (frame.anchors?.comparisonTargets || []).length !== expected.comparisonTargetCount) {
    failures.push(`comparisonTargetCount expected ${expected.comparisonTargetCount}, got ${(frame.anchors?.comparisonTargets || []).length}`);
  }

  for (const fact of expected.requestedFactsTrue || []) {
    if (!frame.requestedFacts?.[fact]) {
      failures.push(`requestedFacts.${fact} expected true`);
    }
  }

  if (expected.minConfidence && Number(frame.confidence?.overall || 0) < expected.minConfidence) {
    failures.push(`overall confidence expected >= ${expected.minConfidence}, got ${frame.confidence?.overall}`);
  }

  if (frame.clarification?.needed) {
    failures.push(`clarification should not be needed for this clear query: ${frame.clarification?.reason || ""}`);
  }

  if (frame.safety?.shouldRefuse) {
    failures.push(`safety refusal should not be triggered: ${frame.safety?.refusalReason || ""}`);
  }

  const qualityText = getFrameQualityText(frame);
  for (const forbidden of expected.forbiddenText || []) {
    if (qualityText.includes(lower(forbidden))) {
      failures.push(`forbidden text/entity present: "${forbidden}"`);
    }
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
      parser: parseDeterministicMeaningFrame,
    });

    const frame = output.meaningFrame;
    const caseFailures = assertCase({ frame, expected: item.expected });

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
      clarification: frame.clarification,
      safety: frame.safety,
      parserTrace: output.parserResult?.trace,
    };

    results.push(summary);

    if (caseFailures.length) {
      failures.push(summary);
    }
  }

  const response = {
    suite: "ACI answer quality smoke",
    scope: "meaning-frame answer-readiness; final customer response text gate comes after tool/executor integration",
    ok: failures.length === 0,
    total: cases.length,
    passed: cases.length - failures.length,
    failed: failures.length,
    failedIds: failures.map((item) => item.id),
    failures,
    results,
  };

  console.log(JSON.stringify(response, null, 2));

  await mongoose.disconnect();

  if (failures.length) process.exit(1);
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI answer quality smoke",
    ok: false,
    error: error.message,
    stack: error.stack,
  }, null, 2));

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
