import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";
import {
  clearVehicleEntityIndexCache,
  findColorMatches,
  findModelMatches,
  findVariantMatches,
  getAutocompleteEntityMatches,
  getVehicleEntityIndex,
  resolveVehicleEntities,
} from "../../services/aiAgent/aiAgent.vehicleEntityIndex.js";

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const includesClean = (value = "", expected = "") =>
  clean(value).includes(clean(expected));

const cases = [];

const addCase = (id, run) => {
  cases.push({ id, run });
};

addCase("entity-index-builds-read-model-backed-counts", async () => {
  clearVehicleEntityIndexCache();
  const index = await getVehicleEntityIndex({ forceRefresh: true });

  const failures = [];

  if (!index?.counts) failures.push("Missing index.counts");
  if (!Number(index?.counts?.models || 0)) failures.push("Expected model count > 0");
  if (!Number(index?.counts?.variants || 0)) failures.push("Expected variant count > 0");
  if (!Array.isArray(index?.modelAliases) || !index.modelAliases.length) {
    failures.push("Expected modelAliases to be populated");
  }

  return {
    failures,
    debug: {
      counts: index?.counts || {},
      builtAt: index?.builtAt || "",
    },
  };
});

addCase("find-model-creta", async () => {
  const index = await getVehicleEntityIndex();
  const matches = findModelMatches(index, "Show Creta pricelist");
  const first = matches[0] || {};

  const failures = [];
  if (!includesClean(first.brand, "Hyundai")) {
    failures.push(`Expected Hyundai, got "${first.brand || ""}"`);
  }
  if (!includesClean(first.model, "Creta")) {
    failures.push(`Expected Creta, got "${first.model || ""}"`);
  }

  return { failures, debug: { first } };
});

addCase("find-model-honda-city-not-generic-city", async () => {
  const index = await getVehicleEntityIndex();

  const hondaCity = findModelMatches(index, "EMI for Honda City");
  const genericCity = findModelMatches(index, "Show price in my city");

  const failures = [];
  if (!includesClean(hondaCity[0]?.brand || "", "Honda")) {
    failures.push(`Expected Honda City match, got "${hondaCity[0]?.displayName || ""}"`);
  }
  if (genericCity.some((item) => clean(item.model) === "city")) {
    failures.push("Generic phrase 'my city' should not match Honda City");
  }

  return {
    failures,
    debug: {
      hondaCityFirst: hondaCity[0] || null,
      genericCityMatches: genericCity.map((item) => item.displayName || item.model),
    },
  };
});

addCase("resolve-entities-uses-context-for-compare-with-city", async () => {
  const result = await resolveVehicleEntities({
    message: "Compare with City",
    context: {
      anchorMake: "Hyundai",
      anchorModel: "Verna",
      anchorVariant: "SX IVT",
      selectedVehicle: {
        make: "Hyundai",
        model: "Verna",
        variant: "SX IVT",
      },
    },
  });

  const failures = [];
  if (!result.comparisonModels.includes("Verna")) {
    failures.push(`Expected comparisonModels to include Verna, got ${JSON.stringify(result.comparisonModels)}`);
  }
  if (!result.comparisonModels.includes("City")) {
    failures.push(`Expected comparisonModels to include City, got ${JSON.stringify(result.comparisonModels)}`);
  }
  if (clean(result.primaryVariant) !== "sx ivt") {
    failures.push(`Expected primaryVariant SX IVT, got "${result.primaryVariant}"`);
  }

  return {
    failures,
    debug: {
      primaryModel: result.primaryModel,
      primaryBrand: result.primaryBrand,
      primaryVariant: result.primaryVariant,
      comparisonModels: result.comparisonModels,
    },
  };
});

addCase("find-variant-verna-db-backed-scoped", async () => {
  const index = await getVehicleEntityIndex();

  const vernaAutomatic =
    index.variants.find(
      (variant) =>
        clean(variant.brand) === "hyundai" &&
        clean(variant.model) === "verna" &&
        clean(variant.transmission).includes("automatic"),
    ) ||
    index.variants.find(
      (variant) =>
        clean(variant.brand) === "hyundai" &&
        clean(variant.model) === "verna",
    );

  const failures = [];

  if (!vernaAutomatic?.variant) {
    failures.push("Expected at least one DB-backed Verna variant in entity index");
    return { failures, debug: { vernaAutomatic: null, first: null } };
  }

  const query = `Verna ${vernaAutomatic.variant}`;
  const matches = findVariantMatches(index, query, {
    model: "Verna",
    brand: "Hyundai",
  });

  const first = matches[0] || {};

  if (!includesClean(first.model, "Verna")) {
    failures.push(`Expected Verna variant, got model "${first.model || ""}"`);
  }

  if (clean(first.variant) !== clean(vernaAutomatic.variant)) {
    failures.push(
      `Expected variant "${vernaAutomatic.variant}", got "${first.variant || ""}"`,
    );
  }

  return {
    failures,
    debug: {
      query,
      expectedVariant: vernaAutomatic.variant,
      first,
    },
  };
});

addCase("color-black-does-not-match-without-model-scope-as-selected-car", async () => {
  const index = await getVehicleEntityIndex();
  const matches = findColorMatches(index, "black available?");

  const failures = [];
  if (matches.length > 20) {
    failures.push(`Unscoped black color query returned too many matches: ${matches.length}`);
  }

  return {
    failures,
    debug: {
      count: matches.length,
      firstFive: matches.slice(0, 5).map((item) => ({
        brand: item.brand,
        model: item.model,
        color: item.color,
      })),
    },
  };
});

addCase("autocomplete-verna-returns-model-without-generic-query-templates", async () => {
  const matches = await getAutocompleteEntityMatches({
    query: "verna",
    limit: 8,
  });

  const failures = [];

  if (!matches.some((item) => item.type === "model" && includesClean(item.label, "Verna"))) {
    failures.push("Expected model suggestion for Verna");
  }

  if (matches.some((item) => item.type === "query")) {
    failures.push("Generic query-template suggestions must stay excluded");
  }

  return {
    failures,
    debug: {
      matches: matches.map((item) => ({
        type: item.type,
        label: item.label,
        tool: item.tool || "",
      })),
    },
  };
});

const main = async () => {
  await connectDB();

  const results = [];

  for (const testCase of cases) {
    const startedAt = Date.now();
    let payload = { failures: [], debug: {} };

    try {
      payload = await testCase.run();
    } catch (err) {
      payload = {
        failures: [err?.stack || err?.message || String(err)],
        debug: {},
      };
    }

    results.push({
      id: testCase.id,
      pass: payload.failures.length === 0,
      durationMs: Date.now() - startedAt,
      failures: payload.failures,
      debug: payload.debug,
    });
  }

  const failed = results.filter((item) => !item.pass);

  console.log(JSON.stringify({
    suite: "ACI vehicle entity index audit",
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    results,
  }, null, 2));

  await mongoose.disconnect();

  if (failed.length) process.exit(1);
};

main().catch(async (err) => {
  console.error(err?.stack || err?.message || err);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
