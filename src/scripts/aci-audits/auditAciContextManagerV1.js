import "dotenv/config";
import mongoose from "mongoose";
import { pathToFileURL } from "url";

import connectDB from "../../config/db.js";
import {
  applyContextIsolationRules,
  compactAciContextState,
  getContextForToolPlan,
  hydrateContextFromCandidates,
  mergeContextPatches,
} from "../../services/aciCore/context/aciContextManager.service.js";
import { buildLegacyPlanFromAciMeaningFrame } from "../../services/aciCore/integration/aciCoreToLegacyPlan.adapter.js";
import { parseDeterministicMeaningFrame } from "../../services/aciCore/understanding/deterministicMeaningFrame.parser.js";

const CASE_TIMEOUT_MS = 5000;
const TOTAL_DURATION_FAILURE_MS = 15000;
const SLOW_CASE_FAILURE_MS = 5000;

const FORBIDDEN_CONTEXT_KEYS = [
  "imageUrl",
  "normalizedImageUrl",
  "imageFrame",
  "visualGallery",
  "selectedColor",
  "priceRange",
  "exShowroomPrice",
  "startingOnRoadPrice",
  "variantCount",
  "fuelText",
  "transmissionText",
  "featureRows",
  "rows",
  "prices",
  "gallery",
  "colorName",
  "colors",
  "variants",
  "sourceImageUrl",
  "canvas",
  "widget",
  "data.rows",
  "data.items",
];

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const vehicleText = (vehicle = {}) =>
  [
    vehicle.make,
    vehicle.brand,
    vehicle.model,
    vehicle.fullModel,
    vehicle.variant,
    vehicle.variantName,
    vehicle.selectedVariant,
  ]
    .filter(Boolean)
    .join(" ");

const hasText = (value = "", needle = "") => clean(value).includes(clean(needle));

const escapeRegex = (value = "") => String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const candidateItem = ({
  rawText = "",
  canonicalKey = "",
  displayName = "",
  type = "",
  metadata = {},
  confidence = 0.85,
} = {}) => ({
  rawText,
  canonicalKey: canonicalKey || clean(displayName || rawText),
  displayName: displayName || rawText || canonicalKey,
  type,
  source: "fast_audit_db_snapshot",
  confidence,
  metadata,
});

const modelQueryFor = ({ make = "", model = "" } = {}) => {
  const ors = [];
  if (make && model) {
    ors.push({
      $and: [
        { $or: [{ make: new RegExp(`^${escapeRegex(make)}$`, "i") }, { brand: new RegExp(`^${escapeRegex(make)}$`, "i") }] },
        { model: new RegExp(`^${escapeRegex(model)}$`, "i") },
      ],
    });
  }
  if (model) {
    ors.push({ shortModelKey: new RegExp(`^${escapeRegex(clean(model))}$`, "i") });
    ors.push({ model: new RegExp(`^${escapeRegex(model)}$`, "i") });
  }
  return ors.length ? { $or: ors } : null;
};

const MODEL_MENTION_RULES = [
  { pattern: /\bverna\b/, make: "Hyundai", model: "Verna" },
  { pattern: /\bcreta\b/, make: "Hyundai", model: "Creta" },
  { pattern: /\bseltos\b/, make: "Kia", model: "Seltos" },
  { pattern: /\bbaleno\b/, make: "Maruti Suzuki", model: "Baleno" },
  { pattern: /\bhonda\s+city\b|\bcity\s+(ground clearance|boot space|price|sunroof|airbags|range)\b/, make: "Honda", model: "City" },
  { pattern: /\bthar\b/, make: "Mahindra", model: "Thar" },
  { pattern: /\bvenue\b/, make: "Hyundai", model: "Venue" },
  { pattern: /\bi20\b/, make: "Hyundai", model: "I20" },
];

const knownModelMentions = (message = "") => {
  const text = clean(message);
  return MODEL_MENTION_RULES
    .filter((rule) => rule.pattern.test(text))
    .map(({ make, model }) => ({ make, model }));
};

const taskCandidatesFor = (message = "") => {
  const text = clean(message);
  const tasks = [];
  if (/\bprice|on road|onroad\b/.test(text)) tasks.push("price_lookup");
  if (/\bemi\b/.test(text)) tasks.push("emi_calculation");
  if (/\bcolors?|colours?\b/.test(text)) tasks.push("color_lookup");
  if (/\bsunroof|adas|airbag|camera|feature\b/.test(text)) tasks.push("feature_answer");
  if (/\bcompare|vs|versus|which one|better|choose|pick|recommend|verdict\b/.test(text)) {
    tasks.push("vehicle_comparison");
  }
  return tasks.map((task) => candidateItem({
    rawText: task,
    canonicalKey: task,
    displayName: task,
    type: "task",
  }));
};

const featureCandidatesFor = (message = "") => {
  const text = clean(message);
  const features = [];
  if (/\bsunroof\b/.test(text)) features.push("sunroof");
  if (/\bairbags?\b/.test(text)) features.push("airbags");
  if (/\brange\b/.test(text)) features.push("range");
  if (/\bbattery\b/.test(text)) features.push("battery_capacity");
  if (/\bboot space\b/.test(text)) features.push("boot_space");
  if (/\bground clearance\b/.test(text)) features.push("ground_clearance");
  if (/\bfeatures?\b/.test(text)) features.push("features");
  return features.map((feature) => candidateItem({
    rawText: feature,
    canonicalKey: feature,
    displayName: feature.replace(/_/g, " "),
    type: "feature",
  }));
};

const languageCandidate = ({ value = "", type = "" } = {}) => candidateItem({
  rawText: value,
  canonicalKey: clean(value),
  displayName: value,
  type,
});

const fuelCandidatesFor = (message = "") => {
  const text = clean(message);
  return [
    /\bpetrol\b/.test(text) ? "Petrol" : "",
    /\bdiesel\b/.test(text) ? "Diesel" : "",
    /\bcng\b/.test(text) ? "CNG" : "",
    /\belectric|ev\b/.test(text) ? "Electric" : "",
  ].filter(Boolean).map((value) => languageCandidate({ value, type: "fuelType" }));
};

const transmissionCandidatesFor = (message = "") => {
  const text = clean(message);
  return [
    /\bmanual\b/.test(text) ? "Manual" : "",
    /\bautomatic|cvt|dct|amt|ivt\b/.test(text) ? "Automatic" : "",
  ].filter(Boolean).map((value) => languageCandidate({ value, type: "transmission" }));
};

const cityFromMessage = (message = "") => {
  const text = clean(message);
  if (/\bdelhi|new delhi\b/.test(text)) return { city: "Delhi", citySlug: "new-delhi" };
  if (/\bnoida\b/.test(text)) return { city: "Noida", citySlug: "noida" };
  if (/\bgurgaon|gurugram\b/.test(text)) return { city: "Gurgaon", citySlug: "gurgaon" };
  if (/\bmumbai\b/.test(text)) return { city: "Mumbai", citySlug: "mumbai" };
  if (/\bbangalore|bengaluru\b/.test(text)) return { city: "Bangalore", citySlug: "bangalore" };
  return {};
};

async function buildFastAuditCandidateSnapshot({ message = "", activeContext = {} } = {}) {
  const db = mongoose.connection.db;
  const explicitModels = knownModelMentions(message);
  const rows = [];
  const seen = new Set();

  for (const item of explicitModels) {
    const seenKey = `${clean(item.make)}|${clean(item.model)}`;
    if (seen.has(seenKey)) continue;
    seen.add(seenKey);
    const query = modelQueryFor(item);
    const row = query
      ? await db.collection("aci_vehicle_model_summary").findOne(query, {
          projection: {
            make: 1,
            brand: 1,
            model: 1,
            fullModel: 1,
            makeKey: 1,
            modelKey: 1,
            shortModelKey: 1,
            fuelTypes: 1,
            transmissions: 1,
          },
        })
      : null;
    if (row) rows.push(row);
  }

  return {
    schemaVersion: "aci.candidateSnapshot.v1",
    rawMessage: message,
    normalizedMessage: message,
    activeContext,
    vehicles: {
      makes: [],
      models: rows.map((row) => candidateItem({
        rawText: row.model,
        canonicalKey: row.modelKey || clean(row.fullModel || row.model),
        displayName: row.fullModel || [row.make || row.brand, row.model].filter(Boolean).join(" "),
        type: "model",
        metadata: {
          make: row.make || row.brand,
          model: row.model,
          fullModel: row.fullModel,
          raw: {
            type: "model",
            make: row.make,
            brand: row.brand || row.make,
            model: row.model,
            rawModel: row.model,
            displayName: row.fullModel,
            makeKey: row.makeKey,
            modelKey: row.modelKey,
            shortModelKey: row.shortModelKey,
            fuelTypes: row.fuelTypes,
            transmissions: row.transmissions,
            confidence: 0.86,
          },
        },
      })),
      variants: [],
      colors: [],
    },
    taxonomy: {
      features: featureCandidatesFor(message),
      fuelTypes: fuelCandidatesFor(message),
      transmissions: transmissionCandidatesFor(message),
      bodyTypes: [],
    },
    commerce: {
      budgets: [],
    },
    language: {
      tasks: taskCandidatesFor(message),
    },
    trace: {
      source: "fast_context_manager_audit",
      counts: {
        models: rows.length,
        features: featureCandidatesFor(message).length,
        tasks: taskCandidatesFor(message).length,
      },
    },
  };
}

const withTimeout = (promise, label = "case") => {
  let timeoutId = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(
      () => reject(new Error(`${label} exceeded ${CASE_TIMEOUT_MS}ms timeout`)),
      CASE_TIMEOUT_MS,
    );
  });

  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timeoutId) clearTimeout(timeoutId);
  });
};

const getComparisonVehicles = (context = {}) =>
  context?.activeComparison?.vehicles ||
  context?.selectedComparisonSet?.vehicles ||
  context?.contextState?.activeComparison?.vehicles ||
  context?.aciContextState?.activeComparison?.vehicles ||
  [];

const expandComparisonFollowUp = ({ message = "", context = {} } = {}) => {
  const vehicles = getComparisonVehicles(context);
  if (!Array.isArray(vehicles) || vehicles.length < 2) return message;
  if (!/\b(which one|which is better|better|safer|safety|their|change city|same comparison|choose|pick|recommend|verdict|final choice)\b/i.test(message)) {
    return message;
  }

  const labels = vehicles
    .map((vehicle = {}) =>
      [
        vehicle.fullModel || [vehicle.make, vehicle.model].filter(Boolean).join(" "),
        vehicle.variant || vehicle.variantName,
      ]
        .filter(Boolean)
        .join(" "),
    )
    .filter(Boolean);

  return labels.length >= 2 ? `${message} ${labels.join(" vs ")}` : message;
};

const hasForbiddenContextPayload = (contextState = {}) => {
  const json = JSON.stringify(contextState || {});
  return FORBIDDEN_CONTEXT_KEYS.filter((key) => json.includes(`"${key}"`));
};

async function planContextCase({ message = "", context = {} } = {}) {
  const effectiveMessage = expandComparisonFollowUp({ message, context });
  const candidateSnapshot = await buildFastAuditCandidateSnapshot({
    message: effectiveMessage,
    activeContext: context,
  });

  const hydrated = await hydrateContextFromCandidates({
    message: effectiveMessage,
    candidateSnapshot,
    activeContext: context,
  });
  const managedSnapshot = hydrated.candidateSnapshot || candidateSnapshot;
  const contextForToolPlan = getContextForToolPlan(hydrated.contextState);

  const parsed = await parseDeterministicMeaningFrame({
    rawMessage: effectiveMessage,
    normalizedMessage: effectiveMessage,
    activeContext: contextForToolPlan,
    candidateSnapshot: managedSnapshot,
  });

  const isolated = applyContextIsolationRules({
    message: effectiveMessage,
    contextState: hydrated.contextState,
    candidateSnapshot: managedSnapshot,
    meaningFrame: parsed.meaningFrame,
  });
  const durableContextState = compactAciContextState(isolated.contextState || hydrated.contextState);
  const city = cityFromMessage(effectiveMessage);
  if (city.city && durableContextState.selectedVehicle?.model) {
    durableContextState.selectedVehicle.city = city.city;
    durableContextState.selectedVehicle.citySlug = city.citySlug;
    durableContextState.anchors.primaryVehicle = {
      ...(durableContextState.anchors.primaryVehicle || {}),
      city: city.city,
      citySlug: city.citySlug,
    };
  }
  if (city.city && durableContextState.activeComparison?.vehicles?.length) {
    durableContextState.activeComparison.city = city.city;
    durableContextState.activeComparison.citySlug = city.citySlug;
  }
  const toolContext = getContextForToolPlan(durableContextState);
  const plan = buildLegacyPlanFromAciMeaningFrame({
    meaningFrame: parsed.meaningFrame,
    context: toolContext,
    message: effectiveMessage,
  });

  const effectiveMeaningFrame = {
    ...(parsed.meaningFrame || {}),
    primaryTask: plan?.meta?.primaryTask || parsed.meaningFrame?.primaryTask || "",
  };

  return {
    effectiveMessage,
    candidateSnapshot: managedSnapshot,
    contextState: durableContextState,
    meaningFrame: effectiveMeaningFrame,
    plan,
    tool: plan.tools?.[0]?.tool || "",
    selectedVehicle: durableContextState.selectedVehicle || {},
    activeComparison: durableContextState.activeComparison || {},
  };
}

const runCase = async (testCase = {}) => {
  const startedAt = Date.now();
  const failures = [];

  try {
    const result = await withTimeout(planContextCase(testCase), testCase.id);
    const selected = result.selectedVehicle || {};
    const forbiddenKeys = hasForbiddenContextPayload(result.contextState);

    if (testCase.expectedTool && result.tool !== testCase.expectedTool) {
      failures.push(`Expected tool ${testCase.expectedTool}, got ${result.tool}`);
    }

    if (testCase.expectedMake && clean(selected.make) !== clean(testCase.expectedMake)) {
      failures.push(`Expected make ${testCase.expectedMake}, got ${selected.make || ""}`);
    }

    if (testCase.expectedModel && clean(selected.model) !== clean(testCase.expectedModel)) {
      failures.push(`Expected model ${testCase.expectedModel}, got ${selected.model || ""}`);
    }

    if (forbiddenKeys.length) {
      failures.push(`Durable contextState contains forbidden payload keys: ${forbiddenKeys.join(", ")}`);
    }

    if (testCase.validate) {
      failures.push(...testCase.validate(result));
    }

    const durationMs = Date.now() - startedAt;
    if (durationMs > SLOW_CASE_FAILURE_MS) {
      failures.push(`${testCase.id} exceeded ${SLOW_CASE_FAILURE_MS}ms: ${durationMs}ms`);
    }
    return {
      id: testCase.id,
      message: testCase.message,
      pass: failures.length === 0,
      durationMs,
      failures,
      summary: {
        effectiveMessage: result.effectiveMessage,
        tool: result.tool,
        anchorMake: selected.make || "",
        anchorModel: selected.model || "",
        anchorFullModel: selected.fullModel || "",
        selectedVehicle: selected,
        activeComparison: result.activeComparison,
        forbiddenContextKeys: forbiddenKeys,
      },
    };
  } catch (error) {
    return {
      id: testCase.id,
      message: testCase.message,
      pass: false,
      durationMs: Date.now() - startedAt,
      failures: [error?.message || String(error)],
      summary: {},
    };
  }
};

const compactnessGuardCase = () => {
  const pollutedPatch = {
    selectedVehicle: {
      make: "Mahindra",
      model: "Be 6",
      fullModel: "Mahindra Be 6",
      imageUrl: "https://example.invalid/car.png",
      normalizedImageUrl: "https://example.invalid/car.png",
      imageFrame: { url: "https://example.invalid/car.png" },
      visualGallery: [{ url: "https://example.invalid/a.png" }],
      selectedColor: { colorName: "Red" },
      priceRange: "fake",
      exShowroomPrice: 123,
      startingOnRoadPrice: 456,
      variantCount: 9,
      fuelText: "Electric",
      transmissionText: "Automatic",
    },
    contextState: {
      schemaVersion: "aci_context_state_v1",
      selectedVehicle: {
        make: "Mahindra",
        model: "Be 6",
        fullModel: "Mahindra Be 6",
        imageUrl: "https://example.invalid/car.png",
        priceRange: "fake",
      },
      activeComparison: {},
      requested: {},
      anchors: { primaryVehicle: { model: "Be 6", imageUrl: "https://example.invalid/car.png" } },
      confidence: {},
      provenance: {},
    },
  };
  const merged = mergeContextPatches({
    managerPatch: pollutedPatch,
    toolPatch: pollutedPatch,
  });
  const forbiddenKeys = hasForbiddenContextPayload(merged.contextState || {});
  const failures = [];

  if (forbiddenKeys.length) {
    failures.push(`Compactness guard found forbidden keys: ${forbiddenKeys.join(", ")}`);
  }

  if (clean(merged.contextState?.selectedVehicle?.model) !== "be 6") {
    failures.push("Compactness guard lost canonical selected vehicle.");
  }

  return {
    id: "compactness-guard-strips-ui-payload",
    message: "synthetic polluted patch",
    pass: failures.length === 0,
    durationMs: 0,
    failures,
    summary: {
      selectedVehicle: merged.contextState?.selectedVehicle || {},
      forbiddenContextKeys: forbiddenKeys,
    },
  };
};

const CASES = [
  {
    id: "be-6e-sunroof",
    message: "be 6e sunroof",
    expectedTool: "vehicle_feature_lookup",
    expectedMake: "Mahindra",
    expectedModel: "Be 6",
    validate: (result) => {
      const failures = [];
      if (hasText(vehicleText(result.selectedVehicle), "6e")) {
        failures.push(`BE 6e alias leaked as durable vehicle text: ${vehicleText(result.selectedVehicle)}`);
      }
      return failures;
    },
  },
  {
    id: "mahindra-be6e-sunroof",
    message: "mahindra be6e sunroof",
    expectedTool: "vehicle_feature_lookup",
    expectedMake: "Mahindra",
    expectedModel: "Be 6",
  },
  {
    id: "eqs-range-spec-route",
    message: "eqs range",
    expectedTool: "vehicle_spec_attribute_lookup",
    expectedMake: "Mercedes Benz",
    expectedModel: "Eqs",
  },
  {
    id: "mercedes-eqs-range-spec-route",
    message: "mercedes eqs range",
    expectedTool: "vehicle_spec_attribute_lookup",
    expectedMake: "Mercedes Benz",
    expectedModel: "Eqs",
  },
  {
    id: "ix-range-spec-route",
    message: "ix range",
    expectedTool: "vehicle_spec_attribute_lookup",
    expectedMake: "Bmw",
    expectedModel: "Ix",
    validate: (result) => {
      const failures = [];
      if (hasText(JSON.stringify(result), "land rover")) {
        failures.push("iX route/context mentioned Land Rover.");
      }
      return failures;
    },
  },
  {
    id: "bmw-ix-range-spec-route",
    message: "bmw ix range",
    expectedTool: "vehicle_spec_attribute_lookup",
    expectedMake: "Bmw",
    expectedModel: "Ix",
  },
  {
    id: "context-switch-price-clears-stale-variant",
    message: "Creta price",
    context: {
      selectedVehicle: {
        make: "Hyundai",
        model: "Verna",
        fullModel: "Hyundai Verna",
        variant: "SX",
        city: "Delhi",
        citySlug: "new-delhi",
      },
      anchorMake: "Hyundai",
      anchorModel: "Verna",
      anchorVariant: "SX",
      anchorCity: "new-delhi",
    },
    expectedTool: "vehicle_pricelist",
    expectedMake: "Hyundai",
    expectedModel: "Creta",
    validate: (result) => {
      const stale = [
        result.selectedVehicle?.variant,
        result.contextState?.anchors?.primaryVehicle?.variant,
      ].filter(Boolean).join(" ");
      const failures = [];
      if (hasText(stale, "sx") || hasText(vehicleText(result.selectedVehicle), "verna")) {
        failures.push(`Stale Verna/SX context leaked: ${vehicleText(result.selectedVehicle)} ${stale}`);
      }
      if (!hasText(result.selectedVehicle?.city || result.selectedVehicle?.citySlug || "", "delhi")) {
        failures.push(`Expected city preservation, got ${result.selectedVehicle?.city || result.selectedVehicle?.citySlug || ""}`);
      }
      return failures;
    },
  },
  {
    id: "comparison-follow-up-preserves-targets",
    message: "which one is better?",
    context: {
      activeComparison: {
        vehicles: [
          { make: "Hyundai", model: "Creta", fullModel: "Hyundai Creta" },
          { make: "Kia", model: "Seltos", fullModel: "Kia Seltos" },
        ],
        confidence: 0.9,
        source: "user_explicit",
      },
    },
    expectedTool: "vehicle_compare",
    validate: (result) => {
      const vehicles = result.activeComparison?.vehicles || [];
      const labels = vehicles.map(vehicleText).join(" | ");
      const failures = [];
      if (vehicles.length < 2) failures.push(`Expected active comparison targets, got ${vehicles.length}`);
      if (!hasText(labels, "creta") || !hasText(labels, "seltos")) {
        failures.push(`Expected Creta and Seltos targets, got ${labels}`);
      }
      return failures;
    },
  },
];

const main = async () => {
  await connectDB();

  const startedAt = Date.now();
  const results = [];

  for (const testCase of CASES) {
    results.push(await runCase(testCase));
  }
  results.push(compactnessGuardCase());

  const durationMs = Date.now() - startedAt;
  const failed = results.filter((item) => !item.pass);
  const slowCases = results
    .filter((item) => item.durationMs > SLOW_CASE_FAILURE_MS)
    .map((item) => ({
      id: item.id,
      durationMs: item.durationMs,
      thresholdMs: SLOW_CASE_FAILURE_MS,
    }));
  if (durationMs > TOTAL_DURATION_FAILURE_MS) {
    failed.push({
      id: "context-manager-total-duration",
      failures: [`Context manager audit exceeded ${TOTAL_DURATION_FAILURE_MS}ms: ${durationMs}ms`],
    });
  }

  const summary = {
    suite: "ACI Context Manager V1 audit",
    total: results.length,
    passed: results.length - results.filter((item) => !item.pass).length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    slowCases,
    durationMs,
    caseTimeoutMs: CASE_TIMEOUT_MS,
    totalDurationFailureMs: TOTAL_DURATION_FAILURE_MS,
    results,
  };

  console.log(JSON.stringify(summary, null, 2));

  await mongoose.disconnect();
  if (failed.length || slowCases.length) process.exit(1);
};

export {
  FORBIDDEN_CONTEXT_KEYS,
  buildFastAuditCandidateSnapshot,
  hasForbiddenContextPayload,
  planContextCase,
};

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(async (error) => {
    console.error(error?.stack || error?.message || error);
    try {
      await mongoose.disconnect();
    } catch {}
    process.exit(1);
  });
}
