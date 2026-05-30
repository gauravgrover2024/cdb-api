import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";
import { prewarmAciCoreRuntime } from "../../services/aciCore/aciCore.prewarm.js";
import { chatWithAgent } from "../../services/aiAgent/aiAgent.service.js";

const CORPUS_PATH = path.resolve(
  process.cwd(),
  "docs/aci-assist/customer-query-corpus-v1.md",
);

const EXPECTED_COUNT = Number(process.env.ACI_CUSTOMER_CORPUS_EXPECTED_COUNT || 40);
const WORKERS = Math.max(1, Number(process.env.ACI_CUSTOMER_CORPUS_WORKERS || 8));
const PRINT_FULL_ANSWERS = process.env.ACI_CUSTOMER_CORPUS_PRINT_FULL !== "false";
const SLOW_MS = Number(process.env.ACI_CUSTOMER_CORPUS_SLOW_MS || 5000);
const VERY_SLOW_MS = Number(process.env.ACI_CUSTOMER_CORPUS_VERY_SLOW_MS || 10000);

const cleanCell = (value = "") =>
  String(value || "")
    .replace(/^["'“”]+|["'“”]+$/g, "")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/`/g, "")
    .replace(/\*\*/g, "")
    .replace(/\s+/g, " ")
    .trim();

const parseCorpus = () => {
  const markdown = fs.readFileSync(CORPUS_PATH, "utf8");
  const lines = markdown.split(/\r?\n/);
  const items = [];

  for (const line of lines) {
    const trimmed = line.trim();
    const match = trimmed.match(/^(?:Q\s*)?(\d+)[\).:-]\s*(.+)$/i);
    if (!match) continue;

    const number = Number(match[1]);
    if (!Number.isFinite(number) || number < 1 || number > EXPECTED_COUNT) continue;

    const query = cleanCell(match[2]);
    if (!query) continue;

    items.push({
      id: `q${number}`,
      index: number,
      query,
    });
  }

  return items.sort((a, b) => a.index - b.index);
};

const rowsOf = (response = {}) =>
  response.rows ||
  response.data?.rows ||
  response.widget?.rows ||
  response.records ||
  response.variants ||
  [];

const bridgeOf = (response = {}) =>
  response.aciCoreBridge || response.meta?.aciCoreBridge || {};

const getUnsupportedCity = (response = {}) =>
  response.unsupportedCity || response.data?.unsupportedCity || null;

const getContextPatch = (response = {}) =>
  response.contextPatch ||
  response.data?.contextPatch ||
  response.widget?.contextPatch ||
  {};

const mergeContext = (base = {}, patch = {}) => ({
  ...base,
  ...patch,
});

const firstRowsPreview = (rows = [], limit = 8) =>
  rows.slice(0, limit).map((row) => ({
    make: row.make || row.brand || "",
    model: row.model || row.fullModel || row.displayName || "",
    variant: row.variant || row.variantName || "",
    fuel: row.fuel || row.fuelType || "",
    transmission: row.transmission || "",
    price:
      row.onRoadPriceLabel ||
      row.onRoadPriceWithoutOptionalLabel ||
      row.priceLabel ||
      row.exShowroomPriceLabel ||
      "",
    feature: row.featureName || row.label || "",
    availability: row.availability || row.status || "",
  }));

const validateResponse = ({ query = "", response = {}, durationMs = 0 }) => {
  const failures = [];
  const rows = rowsOf(response);
  const canvasType = response.canvasType || response.data?.canvasType || "";
  const intent = response.intent || "";
  const q = String(query || "").toLowerCase();

  if (!intent) failures.push("missing intent");
  if (!canvasType && !response.answer) failures.push("missing canvasType/answer");

  const isArrowFollowUp = q.includes("->");
  const finalUserMessage = isArrowFollowUp
    ? q.split("->").slice(-1)[0].trim()
    : q;

  const isComparison =
    /\b(compare|vs|versus)\b/.test(finalUserMessage) ||
    (isArrowFollowUp && /\bwhich one|more features|better\b/.test(finalUserMessage));

  const isUnsupportedCityPrice =
    /\b(on road|on-road|onroad|price|pricing|breakup|quotation|quote)\b/.test(q) &&
    /\b(mumbai|bombay|bangalore|bengaluru|pune|chennai|hyderabad|kolkata|ahmedabad)\b/.test(q);

  const isSupportedExactPrice =
    /\b(on road|on-road|onroad|price|pricing|breakup)\b/.test(q) &&
    /\b(delhi|new delhi|noida|gurgaon|gurugram)\b/.test(q) &&
    !/\bunder\s+\d+/.test(q) &&
    !/\bvs\b|\bversus\b/.test(q);

  const isBudgetDiscovery =
    /\bunder\s+\d+\s*(lakh|lakhs|lac|lacs|l)\b/.test(q) &&
    !/\b(on road|on-road|onroad|price list|pricelist|breakup)\b/.test(q);

  const isCheapestCng =
    /\bcheapest\b/.test(q) && /\bcng\b/.test(q) && /\bcars\b/.test(q);

  const isFeatureDiscovery =
    /\b(sunroof|adas|abs|airbags?|turbo|turbocharged|cng|diesel|petrol|automatic|ventilated)\b/.test(q) &&
    /\b(cars|suv|suvs|models|variants|with|under|cheapest)\b/.test(q) &&
    !/\bvs\b|\bversus\b/.test(q);

  if (isUnsupportedCityPrice) {
    const unsupported = getUnsupportedCity(response);
    if (canvasType !== "unsupported_city_canvas") {
      failures.push(`expected unsupported_city_canvas, got ${canvasType || "blank"}`);
    }
    if (rows.length !== 0) failures.push(`unsupported city returned rows: ${rows.length}`);
    if (unsupported?.reason !== "pricing_city_not_supported") {
      failures.push("unsupported city missing pricing_city_not_supported reason");
    }
  }

  if (isSupportedExactPrice && !isUnsupportedCityPrice) {
    if (!["price_breakup_canvas", "pricelist_canvas"].includes(canvasType)) {
      failures.push(`expected price canvas, got ${canvasType || "blank"}`);
    }
    if (!rows.length) failures.push("supported price query returned no rows");

    if (/\bcreta\b/.test(q) && rows[0]) {
      const rowModel = String(rows[0].model || rows[0].fullModel || "").toLowerCase();
      if (!rowModel.includes("creta")) failures.push(`expected Creta row, got ${rowModel}`);
    }

    if (/\bsx\b/.test(q) && rows[0]) {
      const variants = rows.map((row) => String(row.variant || row.variantName || "").toLowerCase());
      if (!variants.some((variant) => variant.includes("sx"))) {
        failures.push(`expected SX variant in rows, got ${variants.slice(0, 5).join(", ")}`);
      }
    }
  }

  if (isComparison) {
    if (!/comparison/.test(canvasType) && !/comparison/.test(intent)) {
      failures.push(`expected comparison route, got intent=${intent}, canvasType=${canvasType}`);
    }

    // For feature-comparison canvases, rows are feature rows, so 3-4 rows can be correct.
    // For normal vehicle comparison canvases, top rows should be exactly 2.
    if (canvasType === "comparison_canvas" && rows.length && rows.length !== 2) {
      failures.push(`comparison_canvas should return exactly 2 top rows, got ${rows.length}`);
    }
  }

  if (isBudgetDiscovery || isCheapestCng) {
    if (!["recommendation_results_canvas", "feature_match_builder_canvas"].includes(canvasType)) {
      failures.push(`expected discovery canvas, got ${canvasType || "blank"}`);
    }
    if (response.contextPatch?.anchorVariant) {
      failures.push(`budget/feature discovery leaked anchorVariant=${response.contextPatch.anchorVariant}`);
    }
  }

  if (isFeatureDiscovery && !isSupportedExactPrice && !isComparison) {
    if (
      ![
        "feature_match_builder_canvas",
        "recommendation_results_canvas",
        "feature_comparison_canvas",
        "pricelist_canvas",
      ].includes(canvasType)
    ) {
      failures.push(`expected feature/discovery canvas, got ${canvasType || "blank"}`);
    }
  }

  if (/mercedes|eqs/i.test(JSON.stringify(rows[0] || {})) && /\bcreta\b/.test(q)) {
    failures.push("Creta query returned Mercedes/EQS-looking row");
  }

  if (durationMs > 30000) failures.push(`very slow response >30s: ${durationMs}ms`);

  return {
    pass: failures.length === 0,
    failures,
  };
};

const runOneMessage = async ({ message, context = {} }) => {
  const started = performance.now();
  const response = await chatWithAgent({ message, context });
  const durationMs = Math.round(performance.now() - started);
  return { response, durationMs };
};

const runCorpusItem = async (item) => {
  const started = performance.now();
  let context = {};
  let setup = null;
  let finalMessage = item.query;
  let response = null;
  let error = "";

  try {
    if (item.query.includes("->")) {
      const [setupMessage, followUpMessage] = item.query
        .split("->")
        .map((part) => cleanCell(part));

      finalMessage = followUpMessage;

      const setupRun = await runOneMessage({ message: setupMessage, context: {} });
      setup = {
        message: setupMessage,
        durationMs: setupRun.durationMs,
        intent: setupRun.response?.intent || "",
        canvasType: setupRun.response?.canvasType || "",
        title: setupRun.response?.title || "",
        answer: setupRun.response?.answer || "",
        rowCount: rowsOf(setupRun.response).length,
        bridge: bridgeOf(setupRun.response),
      };

      context = mergeContext(context, getContextPatch(setupRun.response));

      const finalRun = await runOneMessage({ message: followUpMessage, context });
      response = finalRun.response;
    } else {
      const finalRun = await runOneMessage({ message: item.query, context: {} });
      response = finalRun.response;
    }
  } catch (err) {
    error = err?.stack || err?.message || String(err);
  }

  const durationMs = Math.round(performance.now() - started);
  const rows = rowsOf(response);
  const bridge = bridgeOf(response);
  const validation = error
    ? { pass: false, failures: [`threw: ${error}`] }
    : validateResponse({ query: item.query, response, durationMs });

  return {
    id: item.id,
    index: item.index,
    query: item.query,
    finalMessage,
    durationMs,
    pass: validation.pass,
    failures: validation.failures,
    setup,
    intent: response?.intent || "",
    canvasType: response?.canvasType || response?.data?.canvasType || "",
    title: response?.title || "",
    answer: response?.answer || "",
    rowCount: rows.length,
    firstRows: firstRowsPreview(rows, 10),
    unsupportedCity: getUnsupportedCity(response),
    bridge,
    contextPatch: getContextPatch(response),
    rawResponse: response,
  };
};

const runWithWorkers = async (items = [], workerCount = 8) => {
  const results = new Array(items.length);
  let cursor = 0;

  const workers = Array.from({ length: Math.min(workerCount, items.length) }, async (_, workerIndex) => {
    while (cursor < items.length) {
      const currentIndex = cursor;
      cursor += 1;

      const item = items[currentIndex];
      const result = await runCorpusItem(item);
      results[currentIndex] = result;

      const status = result.pass ? "✅" : "❌";
      const speed =
        result.durationMs > VERY_SLOW_MS ? "VERY_SLOW" :
        result.durationMs > SLOW_MS ? "SLOW" :
        "OK";

      console.log(`\n${status} [w${workerIndex + 1}] ${result.index}/${items.length} ${result.durationMs}ms ${speed}`);
      console.log(`Q: ${result.query}`);
      if (result.setup) {
        console.log(`SETUP: ${result.setup.message}`);
        console.log(`SETUP ANSWER: ${result.setup.answer}`);
      }
      console.log(`INTENT: ${result.intent}`);
      console.log(`CANVAS: ${result.canvasType}`);
      console.log(`TITLE: ${result.title}`);
      console.log(`ANSWER: ${result.answer}`);
      console.log(`ROWS: ${result.rowCount}`);
      if (result.firstRows.length) {
        console.log(`FIRST ROWS: ${JSON.stringify(result.firstRows, null, 2)}`);
      }
      if (result.unsupportedCity) {
        console.log(`UNSUPPORTED CITY: ${JSON.stringify(result.unsupportedCity, null, 2)}`);
      }
      if (result.bridge?.contextIsolation) {
        console.log(`BRIDGE: ${JSON.stringify(result.bridge, null, 2)}`);
      }
      if (result.failures.length) {
        console.log(`FAILURES: ${JSON.stringify(result.failures, null, 2)}`);
      }
    }
  });

  await Promise.all(workers);
  return results;
};

const writeMarkdownReport = ({ report, markdownPath }) => {
  const lines = [];

  lines.push(`# ACI Customer Query Corpus v1 Live Eval`);
  lines.push("");
  lines.push(`- Total: ${report.total}`);
  lines.push(`- Passed: ${report.passed}`);
  lines.push(`- Failed: ${report.failed}`);
  lines.push(`- Slow > ${SLOW_MS}ms: ${report.slowOver5s}`);
  lines.push(`- Workers: ${WORKERS}`);
  lines.push("");

  if (report.failedResults.length) {
    lines.push(`## Failed`);
    lines.push("");
    for (const item of report.failedResults) {
      lines.push(`### ${item.index}. ${item.query}`);
      lines.push(`- Duration: ${item.durationMs}ms`);
      lines.push(`- Intent: ${item.intent}`);
      lines.push(`- Canvas: ${item.canvasType}`);
      lines.push(`- Failures: ${item.failures.join("; ")}`);
      lines.push(`- Answer: ${item.answer}`);
      lines.push("");
    }
  }

  if (report.slow.length) {
    lines.push(`## Slow Queries`);
    lines.push("");
    for (const item of report.slow) {
      lines.push(`- ${item.index}. ${item.durationMs}ms — ${item.query} [${item.intent}/${item.canvasType}]`);
    }
    lines.push("");
  }

  lines.push(`## Full Answers`);
  lines.push("");

  for (const item of report.results) {
    lines.push(`### ${item.index}. ${item.query}`);
    lines.push("");
    lines.push(`- Pass: ${item.pass ? "YES" : "NO"}`);
    lines.push(`- Duration: ${item.durationMs}ms`);
    lines.push(`- Intent: ${item.intent}`);
    lines.push(`- Canvas: ${item.canvasType}`);
    lines.push(`- Title: ${item.title}`);
    lines.push(`- Context isolation: ${item.bridge?.contextIsolation || ""}`);
    if (item.setup) {
      lines.push(`- Setup answer: ${item.setup.answer}`);
    }
    if (item.failures.length) {
      lines.push(`- Failures: ${item.failures.join("; ")}`);
    }
    lines.push("");
    lines.push(`**Answer:** ${item.answer || ""}`);
    lines.push("");
    if (item.firstRows.length) {
      lines.push(`**First rows:**`);
      lines.push("");
      lines.push("```json");
      lines.push(JSON.stringify(item.firstRows, null, 2));
      lines.push("```");
      lines.push("");
    }
  }

  fs.writeFileSync(markdownPath, lines.join("\n"));
};

const main = async () => {
  const corpus = parseCorpus();

  console.log(JSON.stringify({
    suite: "ACI customer query corpus v1 extractor",
    path: CORPUS_PATH,
    extracted: corpus.length,
    expected: EXPECTED_COUNT,
    workers: WORKERS,
    preview: corpus.slice(0, 8),
  }, null, 2));

  if (corpus.length !== EXPECTED_COUNT) {
    console.error(`Expected ${EXPECTED_COUNT} queries but extracted ${corpus.length}.`);
    process.exit(2);
  }

  await connectDB();
  await prewarmAciCoreRuntime({ force: true, mode: "light", background: false });

  const results = await runWithWorkers(corpus, WORKERS);

  await mongoose.disconnect();

  const failedResults = results.filter((item) => !item.pass);
  const slow = results
    .filter((item) => item.durationMs > SLOW_MS)
    .map((item) => ({
      id: item.id,
      index: item.index,
      query: item.query,
      durationMs: item.durationMs,
      intent: item.intent,
      canvasType: item.canvasType,
      title: item.title,
    }));

  const outDir = path.resolve(process.cwd(), "aci_eval_outputs");
  fs.mkdirSync(outDir, { recursive: true });

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const jsonPath = path.join(outDir, `aci_customer_query_corpus_v1_${stamp}.json`);
  const markdownPath = path.join(outDir, `aci_customer_query_corpus_v1_${stamp}.md`);

  const report = {
    suite: "ACI customer query corpus v1 live chat eval",
    ok: failedResults.length === 0,
    total: results.length,
    passed: results.length - failedResults.length,
    failed: failedResults.length,
    failedIds: failedResults.map((item) => item.id),
    slowOver5s: slow.length,
    slow,
    failedResults,
    results,
  };

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  writeMarkdownReport({ report, markdownPath });

  console.log("\n==============================");
  console.log("ACI CUSTOMER QUERY CORPUS V1 SUMMARY");
  console.log("==============================");
  console.log(JSON.stringify({
    ok: report.ok,
    total: report.total,
    passed: report.passed,
    failed: report.failed,
    failedIds: report.failedIds,
    slowOver5s: report.slowOver5s,
    slow,
    jsonPath,
    markdownPath,
  }, null, 2));

  if (failedResults.length) process.exit(1);
};

main().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
