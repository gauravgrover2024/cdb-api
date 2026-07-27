#!/usr/bin/env node

require("dotenv/config");

const assert = require("assert");

const text = (value = "") => String(value || "").trim();
const lower = (value = "") => text(value).toLowerCase();
const asArray = (value) => (Array.isArray(value) ? value : value ? [value] : []);
const uniq = (items = []) => [...new Set(items.filter(Boolean))];

const FAST_MODE = process.env.ACI_BUYER_JOURNEY_AUDIT_FAST === "1";
const FAST_MODE_EXCLUDED_JOURNEY_IDS = new Set([
  "budget-must-have-journey",
]);

const getRows = (response = {}) =>
  asArray(
    response.rows ||
      response.items ||
      response.results ||
      response.data?.rows ||
      response.data?.items ||
      response.data?.results ||
      response.data?.modelGroups ||
      response.modelGroups,
  );

const getFilters = (response = {}) => response.data?.filters || response.filters || {};

const getBudgetDiscovery = (response = {}) =>
  response.budgetDiscovery ||
  response.data?.budgetDiscovery ||
  response.meta?.budgetDiscovery ||
  {};

const getFeatureResolution = (response = {}) =>
  response.featureResolution ||
  response.data?.featureResolution ||
  getBudgetDiscovery(response).featureResolution ||
  {};

const getNoResultRecovery = (response = {}) =>
  response.noResultRecovery ||
  response.data?.noResultRecovery ||
  response.meta?.noResultRecovery ||
  getBudgetDiscovery(response).noResultRecovery ||
  null;

const getUnsupportedCity = (response = {}) =>
  response.unsupportedCity ||
  response.data?.unsupportedCity ||
  response.meta?.unsupportedCity ||
  null;

const getAnswerBlob = (response = {}) =>
  [
    response.intent,
    response.canvasType,
    response.title,
    response.answer,
    response.data?.title,
    response.data?.answer,
    JSON.stringify(response.data || {}),
  ].join(" ");

const vehicleText = (response = {}) =>
  [
    response.contextPatch?.anchorMake,
    response.contextPatch?.anchorModel,
    response.contextPatch?.anchorVariant,
    response.contextPatch?.selectedVehicle?.make,
    response.contextPatch?.selectedVehicle?.model,
    response.contextPatch?.selectedVehicle?.variant,
    response.contextPatch?.selectedVehicle?.variantName,
    response.data?.anchorMake,
    response.data?.anchorModel,
    response.data?.anchorVariant,
    response.data?.model,
    response.data?.variant,
  ].join(" ");

const comparisonText = (response = {}) => {
  const vehicles = [
    ...asArray(response.data?.selectedComparisonSet?.vehicles),
    ...asArray(response.selectedComparisonSet?.vehicles),
    ...asArray(response.contextPatch?.selectedComparisonSet?.vehicles),
    ...asArray(response.contextPatch?.activeComparison?.vehicles),
    ...asArray(response.data?.rows),
    ...asArray(response.rows),
  ];

  return [
    response.title,
    response.answer,
    response.data?.title,
    response.data?.answer,
    JSON.stringify(vehicles),
  ].join(" ");
};

const getFeatureKeys = (response = {}) => {
  const resolution = getFeatureResolution(response);
  const features = [
    ...asArray(response.features),
    ...asArray(response.data?.features),
    ...asArray(response.rows),
    ...asArray(response.data?.rows),
    ...asArray(response.items),
    ...asArray(response.data?.items),
    ...asArray(resolution.resolvedFeatures),
  ];

  return uniq([
    ...asArray(response.featureKeys),
    ...asArray(response.data?.featureKeys),
    ...asArray(resolution.featureKeys),
    ...features.map((item) => item?.featureKey || item?.canonicalKey || item?.key || ""),
  ].map((item) => lower(item)));
};

const hasTurboFeatureFilter = (response = {}) => {
  const filters = getFilters(response);
  const resolution = getFeatureResolution(response);

  const blob = [
    ...asArray(filters.mustHaveFeatures),
    ...asArray(filters.compareFeatures),
    ...asArray(resolution.requestedFeatures),
    ...asArray(resolution.featureKeys),
    ...asArray(resolution.resolvedFeatures).map((feature) => feature?.featureKey),
    ...asArray(resolution.resolvedFeatures).map((feature) => feature?.displayName),
  ].join(" ");

  return /turbo[_\s-]*charger|turbo/i.test(blob);
};

const hasMeaningfulVehicle = (vehicle = {}) =>
  Boolean(
    text(vehicle.make || vehicle.brand) ||
      text(vehicle.model) ||
      text(vehicle.fullModel || vehicle.displayName) ||
      text(vehicle.variant || vehicle.variantName || vehicle.selectedVariant),
  );

const mergeVehicles = (previous = {}, next = {}) => {
  if (!hasMeaningfulVehicle(next)) return previous || {};
  if (!hasMeaningfulVehicle(previous)) return next || {};

  const nextVariant = text(next.variant || next.variantName || next.selectedVariant);
  const prevVariant = text(previous.variant || previous.variantName || previous.selectedVariant);

  return {
    ...previous,
    ...next,
    make: text(next.make || next.brand) || previous.make || previous.brand || "",
    brand: text(next.brand || next.make) || previous.brand || previous.make || "",
    model: text(next.model) || previous.model || "",
    fullModel: text(next.fullModel || next.displayName) || previous.fullModel || previous.displayName || "",
    displayName: text(next.displayName || next.fullModel) || previous.displayName || previous.fullModel || "",
    variant: nextVariant || prevVariant || "",
    variantName: text(next.variantName || next.variant || next.selectedVariant) || prevVariant || "",
    selectedVariant: text(next.selectedVariant || next.variant || next.variantName) || prevVariant || "",
    city: text(next.city || next.citySlug) || previous.city || previous.citySlug || "",
    citySlug: text(next.citySlug || next.city) || previous.citySlug || previous.city || "",
  };
};

const mergeContextFromResponse = (context = {}, response = {}) => {
  const patch = response.contextPatch && typeof response.contextPatch === "object"
    ? response.contextPatch
    : {};

  const priorVehicle = context.selectedVehicle || {};
  const responseVehicle =
    patch.selectedVehicle ||
    response.data?.selectedVehicle ||
    response.selectedVehicle ||
    {};

  const selectedVehicle = mergeVehicles(priorVehicle, responseVehicle);

  const unsupportedCity = getUnsupportedCity(response);
  if (unsupportedCity?.requestedCity && hasMeaningfulVehicle(selectedVehicle)) {
    selectedVehicle.unsupportedCity = unsupportedCity.requestedCity;
  }

  const selectedComparisonSet =
    patch.selectedComparisonSet ||
    response.data?.selectedComparisonSet ||
    response.selectedComparisonSet ||
    context.selectedComparisonSet ||
    null;

  const activeComparison =
    patch.activeComparison ||
    response.data?.activeComparison ||
    response.activeComparison ||
    (selectedComparisonSet?.vehicles ? selectedComparisonSet : context.activeComparison) ||
    null;

  return {
    ...context,
    ...patch,
    selectedVehicle,
    ...(selectedComparisonSet ? { selectedComparisonSet } : {}),
    ...(activeComparison ? { activeComparison } : {}),
  };
};

const summarize = (response = {}) => ({
  intent: response.intent || "",
  canvasType: response.canvasType || "",
  title: response.title || response.data?.title || "",
  answerPreview: text(response.answer || response.data?.answer || "").slice(0, 260),
  rowCount: getRows(response).length,
  unsupportedCity: getUnsupportedCity(response),
  noResultRecovery: getNoResultRecovery(response),
  filters: getFilters(response),
  featureKeys: getFeatureKeys(response),
  hasTurboFeatureFilter: hasTurboFeatureFilter(response),
  selectedVehicle: response.contextPatch?.selectedVehicle || response.data?.selectedVehicle || {},
  selectedComparisonSet: response.contextPatch?.selectedComparisonSet || response.data?.selectedComparisonSet || null,
});

const assertIncludes = (blob, pattern, label) => {
  assert(
    pattern.test(blob),
    `${label}: expected ${JSON.stringify(blob).slice(0, 500)} to match ${pattern}`,
  );
};

const assertNotIncludes = (blob, pattern, label) => {
  assert(
    !pattern.test(blob),
    `${label}: expected ${JSON.stringify(blob).slice(0, 500)} not to match ${pattern}`,
  );
};

const assertSupportedPrice = (response, label) => {
  assert.notStrictEqual(response.canvasType, "unsupported_city_canvas", `${label}: should not be unsupported city`);
  assert(getRows(response).length > 0 || Number(response.matched || response.count || 0) > 0, `${label}: expected price rows/matches`);
  assertIncludes(getAnswerBlob(response), /price|₹|lakh|on-road|on road|ex-showroom|ex showroom/i, `${label}: price wording`);
};

const assertUnsupportedCity = (response, city, label) => {
  assert.strictEqual(response.canvasType, "unsupported_city_canvas", `${label}: expected unsupported_city_canvas`);
  assertIncludes(getAnswerBlob(response), new RegExp(city, "i"), `${label}: requested city mention`);
  assertIncludes(getAnswerBlob(response), /new delhi|noida|gurgaon/i, `${label}: supported city recovery`);
  assert(getUnsupportedCity(response), `${label}: expected unsupportedCity object`);
};

const assertComparison = (response, label) => {
  assert.strictEqual(response.canvasType, "comparison_canvas", `${label}: expected comparison_canvas`);
  const blob = comparisonText(response);
  assertIncludes(blob, /creta/i, `${label}: Creta context`);
  assertIncludes(blob, /seltos/i, `${label}: Seltos context`);
  assertNotIncludes(getAnswerBlob(response), /can you clarify|which car are you asking/i, `${label}: should not clarify`);
};

const assertPendingHonesty = (response, topic, label) => {
  const blob = getAnswerBlob(response);
  assertIncludes(blob, new RegExp(topic, "i"), `${label}: topic mention`);
  assertIncludes(blob, /not available|not invent|verified|do not have|not currently|cannot confirm|unavailable/i, `${label}: honest unavailable wording`);
  assertNotIncludes(blob, /₹\s?\d|discount\s+is\s+\d|offer\s+is\s+\d|insurance\s+premium\s+is\s+\d/i, `${label}: should not invent numeric facts`);
};

async function main() {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  if (typeof connectDB !== "function") throw new Error("connectDB export not found");
  await connectDB();

  const prewarmMod = await import("../../services/aciCore/aciCore.prewarm.js");
  if (typeof prewarmMod.prewarmAciCoreRuntime === "function") {
    await prewarmMod.prewarmAciCoreRuntime({ force: false });
  }

  const serviceMod = await import("../../services/aiAgent/aiAgent.service.js");
  const chatWithAgent = serviceMod.chatWithAgent || serviceMod.default;
  if (typeof chatWithAgent !== "function") throw new Error("chatWithAgent export not found");

  const journeys = [
    {
      id: "price-city-trust-journey",
      title: "Price trust journey",
      steps: [
        {
          id: "delhi-price",
          message: "Creta SX on-road price Delhi",
          assert: (response) => {
            assertSupportedPrice(response, "delhi-price");
            assertIncludes(vehicleText(response), /creta/i, "delhi-price vehicle");
            assertIncludes(vehicleText(response), /sx/i, "delhi-price variant");
          },
        },
        {
          id: "same-in-mumbai",
          message: "same in Mumbai",
          assert: (response) => {
            assertUnsupportedCity(response, "Mumbai", "same-in-mumbai");
            assertNotIncludes(getAnswerBlob(response), /delhi on-road price|new delhi price/i, "same-in-mumbai no Delhi fallback");
          },
        },
        {
          id: "same-in-noida",
          message: "same in Noida",
          assert: (response) => {
            assertSupportedPrice(response, "same-in-noida");
            assertIncludes(getAnswerBlob(response), /noida/i, "same-in-noida city mention");
          },
        },
      ],
    },

    {
      id: "unsupported-first-price-recovery-journey",
      title: "Unsupported-first price recovery journey",
      steps: [
        {
          id: "creta-mumbai-price",
          message: "creta price in mumbai",
          assert: (response) => {
            assertUnsupportedCity(response, "Mumbai", "creta-mumbai-price");
            assertIncludes(getAnswerBlob(response), /creta/i, "creta-mumbai-price model mention");
            assertIncludes(vehicleText(response) + JSON.stringify(response.unsupportedCity || response.data?.unsupportedCity || {}), /creta/i, "creta-mumbai-price context model");
          },
        },
        {
          id: "delhi-price-after-unsupported",
          message: "delhi price",
          assert: (response) => {
            assertSupportedPrice(response, "delhi-price-after-unsupported");
            assertIncludes(getAnswerBlob(response), /delhi|new delhi/i, "delhi-price-after-unsupported city mention");
            assertIncludes(vehicleText(response) + getAnswerBlob(response), /creta/i, "delhi-price-after-unsupported model");
          },
        },
      ],
    },

    {
      id: "compare-to-decision-journey",
      title: "Compare-to-decision journey",
      steps: [
        {
          id: "compare-creta-seltos",
          message: "Creta vs Seltos",
          assert: (response) => assertComparison(response, "compare-creta-seltos"),
        },
        {
          id: "price-difference",
          message: "price difference",
          assert: (response) => {
            assertComparison(response, "price-difference");
            assertIncludes(getAnswerBlob(response), /price|cheaper|cost|difference|₹/i, "price-difference wording");
          },
        },
        {
          id: "which-one",
          message: "which one?",
          assert: (response) => {
            assertComparison(response, "which-one");
            assertNotIncludes(getAnswerBlob(response), /I can.?t decide|not enough context|can you clarify/i, "which-one should preserve comparison");
          },
        },
      ],
    },

    {
      id: "feature-spec-context-journey",
      title: "Feature/spec context journey",
      steps: [
        {
          id: "punch-sunroof-adas",
          message: "Does the Tata Punch have a sunroof and ADAS?",
          assert: (response) => {
            assert.strictEqual(response.intent, "vehicle_multi_feature_answer", "punch-sunroof-adas: expected multi-feature intent");
            const keys = getFeatureKeys(response).join(" ");
            assertIncludes(keys, /sunroof/i, "punch-sunroof-adas sunroof key");
            assertIncludes(keys, /adas/i, "punch-sunroof-adas adas key");
            assertIncludes(vehicleText(response) + getAnswerBlob(response), /punch/i, "punch-sunroof-adas vehicle");
          },
        },
        {
          id: "punch-adventure-s-sunroof-adas",
          message: "Does Tata Punch Adventure S have sunroof and ADAS?",
          assert: (response) => {
            assert.strictEqual(response.intent, "vehicle_multi_feature_answer", "punch-adventure-s: expected multi-feature intent");
            const blob = vehicleText(response) + getAnswerBlob(response);
            assertIncludes(blob, /punch/i, "punch-adventure-s model");
            assertIncludes(blob, /adventure\s+s/i, "punch-adventure-s variant");
            const keys = getFeatureKeys(response).join(" ");
            assertIncludes(keys, /sunroof/i, "punch-adventure-s sunroof key");
            assertIncludes(keys, /adas/i, "punch-adventure-s adas key");
          },
        },
        {
          id: "punch-mileage",
          message: "Tata Punch mileage",
          assert: (response) => {
            assertIncludes(getAnswerBlob(response), /mileage|kmpl/i, "punch-mileage wording");
            assertIncludes(vehicleText(response) + getAnswerBlob(response), /punch/i, "punch-mileage model");
          },
        },
      ],
    },

    {
      id: "budget-must-have-journey",
      title: "Budget + must-have feature journey",
      steps: [
        {
          id: "plain-suvs-under-8l",
          message: "SUVs under 8 lakhs",
          assert: (response) => {
            assert.strictEqual(response.canvasType, "recommendation_results_canvas", "plain-suvs: expected recommendation canvas");
            assert(!hasTurboFeatureFilter(response), "plain-suvs: must not inject turbo filter");
          },
        },
        {
          id: "turbo-suvs-under-8l",
          message: "turbocharged SUVs under 8 lakhs",
          assert: (response) => {
            assert.strictEqual(response.canvasType, "feature_match_builder_canvas", "turbo-suvs: expected feature match canvas");
            assert(hasTurboFeatureFilter(response), "turbo-suvs: expected turbo filter");
          },
        },
        {
          id: "turbo-cars-under-12l",
          message: "cars with turbo under 12 lakhs",
          assert: (response) => {
            assert.strictEqual(response.canvasType, "feature_match_builder_canvas", "turbo-cars: expected feature match canvas");
            assert(hasTurboFeatureFilter(response), "turbo-cars: expected turbo filter");
          },
        },
      ],
    },

    {
      id: "pending-module-honesty-journey",
      title: "Pending module honesty journey",
      steps: [
        {
          id: "offers-creta",
          message: "are there offers on Creta",
          assert: (response) => assertPendingHonesty(response, "offer", "offers-creta"),
        },
        {
          id: "insurance-creta",
          message: "insurance price for Creta",
          assert: (response) => assertPendingHonesty(response, "insurance", "insurance-creta"),
        },
        {
          id: "service-cost-creta",
          message: "service cost of Creta",
          assert: (response) => assertPendingHonesty(response, "service", "service-cost-creta"),
        },
      ],
    },
  ];

  const selectedJourneys = FAST_MODE
    ? journeys.filter((journey) => !FAST_MODE_EXCLUDED_JOURNEY_IDS.has(journey.id))
    : journeys;
  const skippedJourneyIds = journeys
    .filter((journey) => !selectedJourneys.some((selected) => selected.id === journey.id))
    .map((journey) => journey.id);

  const results = [];
  const startedAt = Date.now();

  for (const journey of selectedJourneys) {
    let context = {};
    const stepResults = [];

    for (const step of journey.steps) {
      const stepStartedAt = Date.now();
      let response = null;
      const failures = [];

      try {
        response = await chatWithAgent({
          message: step.message,
          context,
          user: null,
          session: {},
          meta: {
            source: "auditAciCoreBuyerJourneyBackendV1",
            journeyId: journey.id,
            stepId: step.id,
            backendOnly: true,
            frontendEvaluated: false,
          },
        });

        step.assert(response);
      } catch (error) {
        failures.push(error?.message || String(error));
      }

      stepResults.push({
        id: step.id,
        message: step.message,
        pass: failures.length === 0,
        durationMs: Date.now() - stepStartedAt,
        failures,
        summary: summarize(response || {}),
      });

      if (response) {
        context = mergeContextFromResponse(context, response);
      }
    }

    const failedSteps = stepResults.filter((step) => !step.pass);
    results.push({
      id: journey.id,
      title: journey.title,
      pass: failedSteps.length === 0,
      failedStepIds: failedSteps.map((step) => step.id),
      steps: stepResults,
    });
  }

  const failed = results.filter((journey) => !journey.pass);

  const output = {
    suite: "ACI Core Buyer Journey Backend Audit v1",
    ok: failed.length === 0,
    backendOnly: true,
    frontendEvaluated: false,
    fastMode: FAST_MODE,
    skippedJourneyIds,
    totalJourneys: results.length,
    passedJourneys: results.length - failed.length,
    failedJourneys: failed.length,
    failedIds: failed.map((journey) => journey.id),
    totalSteps: results.reduce((sum, journey) => sum + journey.steps.length, 0),
    failedSteps: results.flatMap((journey) =>
      journey.steps
        .filter((step) => !step.pass)
        .map((step) => `${journey.id}:${step.id}`),
    ),
    durationMs: Date.now() - startedAt,
    results,
  };

  console.log(JSON.stringify(output, null, 2));
  return output.ok;
}

main()
  .then(async (ok) => {
    const mongoose = require("mongoose");
    if (mongoose.connection?.readyState) await mongoose.disconnect();
    process.exit(ok ? 0 : 1);
  })
  .catch(async (error) => {
    console.error(error?.stack || error?.message || String(error));
    const mongoose = require("mongoose");
    if (mongoose.connection?.readyState) await mongoose.disconnect();
    process.exit(1);
  });
