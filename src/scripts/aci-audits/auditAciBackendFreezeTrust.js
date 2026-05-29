import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";
import { prewarmAciCoreRuntime } from "../../services/aciCore/aciCore.prewarm.js";
import { runAciCoreLiveBridge } from "../../services/aciCore/integration/aciCoreLiveBridge.service.js";

const getRows = (response = {}) =>
  response.rows || response.data?.rows || response.items || response.data?.items || [];

const getUnsupportedCity = (response = {}) =>
  response.unsupportedCity ||
  response.data?.unsupportedCity ||
  response.widget?.unsupportedCity ||
  response.meta?.unsupportedCity ||
  null;

const getNoResultRecovery = (response = {}) =>
  response.noResultRecovery ||
  response.data?.noResultRecovery ||
  response.meta?.noResultRecovery ||
  response.budgetDiscovery?.noResultRecovery ||
  response.data?.budgetDiscovery?.noResultRecovery ||
  null;

const getAnchorVariant = (response = {}) =>
  response.contextPatch?.anchorVariant ||
  response.data?.contextPatch?.anchorVariant ||
  response.widget?.contextPatch?.anchorVariant ||
  "";

const hasTurboFeatureFilter = (response = {}) => {
  const filters = response.data?.filters || response.filters || {};
  const budgetDiscovery = response.budgetDiscovery || response.data?.budgetDiscovery || {};
  const featureResolution =
    response.featureResolution ||
    response.data?.featureResolution ||
    budgetDiscovery.featureResolution ||
    {};

  const filterText = [
    ...(filters.mustHaveFeatures || []),
    ...(filters.compareFeatures || []),
    ...(featureResolution.featureKeys || []),
    ...(featureResolution.resolvedFeatures || []).map((feature) => feature.featureKey),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  return filterText.includes("turbo_charger") || filterText.includes("turbo charger");
};

const runCase = async ({ id, message, check }) => {
  const response = await runAciCoreLiveBridge({
    message,
    context: {},
  });
  const failures = check(response);

  return {
    id,
    message,
    pass: failures.length === 0,
    failures,
    summary: {
      intent: response.intent,
      canvasType: response.canvasType,
      matched: response.matched,
      count: response.count,
      rowCount: getRows(response).length,
      unsupportedCity: getUnsupportedCity(response),
      noResultRecovery: getNoResultRecovery(response),
      hasTurboFeatureFilter: hasTurboFeatureFilter(response),
      anchorVariant: getAnchorVariant(response),
    },
  };
};

const unsupportedCityCases = [
  {
    id: "unsupported-city-mumbai",
    message: "Creta SX on-road price Mumbai",
    check: (response) => {
      const failures = [];
      const unsupportedCity = getUnsupportedCity(response);
      const rows = getRows(response);

      if (response.canvasType !== "unsupported_city_canvas") {
        failures.push(`expected unsupported_city_canvas, got ${response.canvasType}`);
      }
      if (!unsupportedCity) failures.push("missing unsupportedCity metadata");
      if (unsupportedCity?.requestedCity !== "Mumbai") {
        failures.push(`expected requestedCity Mumbai, got ${unsupportedCity?.requestedCity || ""}`);
      }
      if (unsupportedCity?.reason !== "pricing_city_not_supported") {
        failures.push(`expected pricing_city_not_supported, got ${unsupportedCity?.reason || ""}`);
      }
      if (!unsupportedCity?.canRetryWithSupportedCity) {
        failures.push("expected canRetryWithSupportedCity=true");
      }
      if (rows.length) failures.push(`expected no fabricated rows, got ${rows.length}`);

      return failures;
    },
  },
  ...["Delhi", "Noida", "Gurgaon"].map((city) => ({
    id: `supported-city-${city.toLowerCase()}`,
    message: `Creta SX on-road price ${city}`,
    check: (response) => {
      const failures = [];
      const rows = getRows(response);

      if (getUnsupportedCity(response)) failures.push("supported city returned unsupportedCity");
      if (!["price_breakup_canvas", "pricelist_canvas"].includes(response.canvasType)) {
        failures.push(`expected price canvas, got ${response.canvasType}`);
      }
      if (!rows.length) failures.push(`expected at least one ${city} price row`);

      return failures;
    },
  })),
];

const featureFilterCases = [
  {
    id: "turbocharged-suvs-under-8l",
    message: "turbocharged SUVs under 8 lakhs",
    check: (response) => {
      const failures = [];
      const rows = getRows(response);
      const noResultRecovery = getNoResultRecovery(response);

      if (!hasTurboFeatureFilter(response)) {
        failures.push("turbocharged was not retained as a must-have feature filter");
      }
      if (getAnchorVariant(response)) {
        failures.push(`expected empty anchorVariant, got ${getAnchorVariant(response)}`);
      }
      if (response.canvasType !== "feature_match_builder_canvas" && !noResultRecovery) {
        failures.push(`expected feature_match_builder_canvas or honest no-result, got ${response.canvasType}`);
      }
      if (!rows.length && !noResultRecovery) {
        failures.push("empty result is missing noResultRecovery metadata");
      }

      return failures;
    },
  },
  {
    id: "cars-with-turbo-under-12l",
    message: "cars with turbo under 12 lakhs",
    check: (response) => {
      const failures = [];
      const rows = getRows(response);
      const noResultRecovery = getNoResultRecovery(response);

      if (!hasTurboFeatureFilter(response)) {
        failures.push("turbo was not retained as a must-have feature filter");
      }
      if (getAnchorVariant(response)) {
        failures.push(`expected empty anchorVariant, got ${getAnchorVariant(response)}`);
      }
      if (!rows.length && !noResultRecovery) {
        failures.push("empty result is missing noResultRecovery metadata");
      }

      return failures;
    },
  },
  {
    id: "plain-suvs-under-8l",
    message: "SUVs under 8 lakhs",
    check: (response) => {
      const failures = [];
      const rows = getRows(response);

      if (hasTurboFeatureFilter(response)) {
        failures.push("plain SUV budget discovery unexpectedly applied turbo filter");
      }
      if (!rows.length) failures.push("plain SUV budget discovery returned no rows");

      return failures;
    },
  },
];

const main = async () => {
  await connectDB();
  await prewarmAciCoreRuntime({ force: false });

  const cases = [...unsupportedCityCases, ...featureFilterCases];
  const results = [];

  for (const testCase of cases) {
    results.push(await runCase(testCase));
  }

  const failed = results.filter((result) => !result.pass);
  const output = {
    suite: "ACI backend freeze trust audit",
    ok: failed.length === 0,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((result) => result.id),
    results,
  };

  console.log(JSON.stringify(output, null, 2));

  await mongoose.disconnect();
  return failed.length ? 1 : 0;
};

main()
  .then((code) => {
    process.exitCode = code;
  })
  .catch(async (error) => {
    console.error(error);
    await mongoose.disconnect().catch(() => {});
    process.exitCode = 1;
  });
