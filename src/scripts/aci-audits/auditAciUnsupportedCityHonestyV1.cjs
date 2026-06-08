#!/usr/bin/env node
require("dotenv").config();

const mongoose = require("mongoose");

const STRICT = String(process.env.ACI_UNSUPPORTED_CITY_AUDIT_STRICT || "1") === "1";

const SUPPORTED = [
  { city: "New Delhi", slug: "new-delhi", messages: ["Creta price Delhi", "Creta price New Delhi"] },
  { city: "Noida", slug: "noida", messages: ["Creta price Noida"] },
  { city: "Gurgaon", slug: "gurgaon", messages: ["Creta price Gurgaon", "Creta price Gurugram"] },
];

const UNSUPPORTED = [
  ["Mumbai", ["Creta price Mumbai", "same in Mumbai", "price in Mumbai"]],
  ["Bangalore", ["Seltos price Bangalore", "Seltos price Bengaluru"]],
  ["Jaipur", ["Baleno price Jaipur"]],
  ["Pune", ["Creta price Pune", "now Pune"]],
  ["Chennai", ["Seltos price Chennai"]],
  ["Hyderabad", ["Creta price Hyderabad"]],
  ["Kolkata", ["Seltos price Kolkata"]],
  ["Ahmedabad", ["Baleno price Ahmedabad"]],
  ["Chandigarh", ["Creta price Chandigarh"]],
  ["Faridabad", ["Creta price Faridabad"]],
  ["Ghaziabad", ["Creta price Ghaziabad"]],
];

const selectedVehicle = ({ make = "", model = "", variant = "", city = "new-delhi", citySlug = city } = {}) => ({
  make,
  model,
  fullModel: [make, model].filter(Boolean).join(" "),
  variant,
  variantName: variant,
  fullVariant: [make, model, variant].filter(Boolean).join(" "),
  city,
  citySlug,
});

const contextCretaDelhi = {
  selectedVehicle: selectedVehicle({ make: "Hyundai", model: "Creta", city: "new-delhi", citySlug: "new-delhi" }),
};

const activeComparison = (...vehicles) => ({
  vehicles: vehicles.map((vehicle) => ({
    make: vehicle.make,
    model: vehicle.model,
    fullModel: vehicle.fullModel || [vehicle.make, vehicle.model].filter(Boolean).join(" "),
    variant: vehicle.variant || "",
  })),
});

const contextCretaSeltos = {
  activeComparison: activeComparison(
    selectedVehicle({ make: "Hyundai", model: "Creta" }),
    selectedVehicle({ make: "Kia", model: "Seltos" }),
  ),
};

const text = (value) => String(value || "");
const lower = (value) => text(value).toLowerCase();
const asArray = (value) => (Array.isArray(value) ? value : []);

const getCanvasType = (body = {}) =>
  body.canvasType ||
  body.data?.canvasType ||
  body.widget?.canvasType ||
  "";

const getTool = (body = {}) =>
  body.aciCoreBridge?.tool ||
  body.meta?.aciCoreBridge?.tool ||
  body.tool ||
  body.responseTool ||
  body.data?.tool ||
  "";

const getRows = (body = {}) =>
  asArray(body.rows || body.data?.rows || body.records || body.data?.records || body.variants || body.data?.variants || []);

const getTitle = (body = {}) => body.title || body.data?.title || body.widget?.title || "";
const getAnswer = (body = {}) => text(body.answer || body.message || body.data?.answer || "");

const hasUnsupportedSignal = (body = {}, cityLabel = "") => {
  const blob = JSON.stringify(body || {});
  const answer = getAnswer(body);
  const canvas = getCanvasType(body);

  return (
    canvas === "unsupported_city_canvas" ||
    /pricing unavailable|not supported|do not have live|cannot quote|not available yet|unsupported/i.test(`${answer} ${blob}`) ||
    new RegExp(cityLabel.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(`${answer} ${blob}`)
  );
};

const hasFakeSupportedPriceSignal = (body = {}, unsupportedCity = "") => {
  const blob = JSON.stringify(body || {});
  const rows = getRows(body);
  const answer = getAnswer(body);

  if (getCanvasType(body) === "unsupported_city_canvas") return false;

  const hasPriceRows = rows.length > 0 && rows.some((row) =>
    Number(row.exShowroomPrice || row.onRoadPrice || row.price || 0) > 0,
  );

  const mentionsUnsupported = new RegExp(unsupportedCity.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(blob);
  const delhiFallbackLanguage = /new delhi|delhi on-road|delhi price|in delhi|new-delhi/i.test(answer);
  const moneySignal = /₹|rs\.?|lakh|on-road|ex-showroom/i.test(answer);

  return hasPriceRows || (moneySignal && delhiFallbackLanguage && !mentionsUnsupported);
};

const hasSupportedPriceSignal = (body = {}) => {
  const rows = getRows(body);
  const answer = getAnswer(body);
  return (
    getCanvasType(body) !== "unsupported_city_canvas" &&
    rows.length > 0 &&
    /₹|lakh|on-road|ex-showroom|price/i.test(answer)
  );
};

const caseDefs = [
  ...UNSUPPORTED.flatMap(([city, messages]) =>
    messages.map((message) => ({
      id: `unsupported-${lower(city).replace(/[^a-z0-9]+/g, "-")}-${lower(message).replace(/[^a-z0-9]+/g, "-").slice(0, 45)}`,
      type: "unsupported",
      city,
      message,
      context: /\bsame|now|there|price in\b/i.test(message) ? contextCretaDelhi : {},
    })),
  ),
  {
    id: "unsupported-context-price-there-mumbai",
    type: "unsupported",
    city: "Mumbai",
    message: "price there",
    context: { ...contextCretaDelhi, anchorCity: "mumbai" },
  },
  {
    id: "unsupported-comparison-same-in-mumbai",
    type: "unsupported",
    city: "Mumbai",
    message: "same in Mumbai",
    context: contextCretaSeltos,
  },
  ...SUPPORTED.flatMap((city) =>
    city.messages.map((message) => ({
      id: `supported-${city.slug}-${lower(message).replace(/[^a-z0-9]+/g, "-")}`,
      type: "supported",
      city: city.city,
      citySlug: city.slug,
      message,
      context: {},
    })),
  ),
  {
    id: "supported-context-change-city-noida",
    type: "supported",
    city: "Noida",
    citySlug: "noida",
    message: "change city to Noida",
    context: contextCretaDelhi,
  },
];

async function callBridgeCase(runAciCoreLiveBridge, testCase) {
  return runAciCoreLiveBridge({
    message: testCase.message,
    context: testCase.context || {},
    user: null,
    session: {},
    meta: { source: "auditAciUnsupportedCityHonestyV1", caseId: testCase.id },
  });
}

function classify(testCase, body = {}) {
  const failures = [];
  const canvasType = getCanvasType(body);
  const tool = getTool(body);
  const rows = getRows(body);
  const title = getTitle(body);
  const answer = getAnswer(body);

  if (testCase.type === "unsupported") {
    if (!hasUnsupportedSignal(body, testCase.city)) {
      failures.push("missing_unsupported_city_signal");
    }
    if (hasFakeSupportedPriceSignal(body, testCase.city)) {
      failures.push("possible_hidden_supported_city_price_fallback");
    }
    if (/new delhi|new-delhi/i.test(answer) && !/currently available for|supported.*cities|New Delhi, Noida, and Gurgaon/i.test(answer)) {
      failures.push("unsafe_delhi_language_without_supported_city_context");
    }
  }

  if (testCase.type === "supported") {
    if (!hasSupportedPriceSignal(body)) {
      failures.push("supported_city_missing_price_rows_or_price_answer");
    }
    if (canvasType === "unsupported_city_canvas") {
      failures.push("supported_city_marked_unsupported");
    }
  }

  return {
    id: testCase.id,
    type: testCase.type,
    city: testCase.city,
    citySlug: testCase.citySlug || "",
    message: testCase.message,
    pass: failures.length === 0,
    failures,
    response: {
      tool,
      canvasType,
      title,
      rowsCount: rows.length,
      answerPreview: answer.replace(/\s+/g, " ").slice(0, 260),
    },
  };
}

async function main() {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  if (typeof connectDB !== "function") throw new Error("connectDB export not found");
  await connectDB();

  const bridgeMod = await import("../../services/aciCore/integration/aciCoreLiveBridge.service.js");
  const runAciCoreLiveBridge = bridgeMod.runAciCoreLiveBridge || bridgeMod.default;
  if (typeof runAciCoreLiveBridge !== "function") throw new Error("runAciCoreLiveBridge export not found");

  const results = [];
  for (const testCase of caseDefs) {
    const body = await callBridgeCase(runAciCoreLiveBridge, testCase);
    results.push(classify(testCase, body));
  }

  const failed = results.filter((row) => !row.pass);
  const summary = {
    suite: "ACI Unsupported City Honesty Audit v1",
    ok: failed.length === 0,
    strict: STRICT,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((row) => row.id),
    results,
  };

  console.log(JSON.stringify(summary, null, 2));
  return !STRICT || failed.length === 0;
}

let finalExitCode = 0;

main()
  .then((ok) => {
    finalExitCode = ok ? 0 : 1;
  })
  .catch((error) => {
    console.error(error);
    finalExitCode = 1;
  })
  .finally(async () => {
    if (mongoose.connection?.readyState) {
      await mongoose.disconnect();
    }
    process.exit(finalExitCode);
  });
