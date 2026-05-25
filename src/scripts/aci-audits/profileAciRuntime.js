import "dotenv/config";
import fs from "fs";
import path from "path";
import mongoose from "mongoose";
import { performance } from "perf_hooks";

const TS = new Date().toISOString().replace(/[:.]/g, "-");
const OUT_DIR = path.resolve(`aci_runtime_profile_${TS}`);
const OUT_JSON = path.join(OUT_DIR, "aci_runtime_profile.json");
const OUT_MD = path.join(OUT_DIR, "aci_runtime_profile.md");

const TESTS = [
  {
    id: "runtime-price",
    message: "Verna pricelist",
    context: {},
    user: null,
  },
  {
    id: "runtime-feature",
    message: "Does Verna SX have sunroof?",
    context: {},
    user: null,
  },
  {
    id: "runtime-colors",
    message: "Show Verna colors",
    context: {},
    user: null,
  },
  {
    id: "runtime-compare",
    message: "Compare Verna SX IVT with City",
    context: {
      selectedVehicle: {
        model: "Verna",
        variant: "SX IVT",
        city: "new-delhi",
      },
      anchorModel: "Verna",
      anchorVariant: "SX IVT",
      anchorCity: "new-delhi",
    },
    user: null,
  },
  {
    id: "runtime-emi",
    message: "EMI for Verna SX IVT with 2 lakh down payment",
    context: {},
    user: null,
  },
  {
    id: "runtime-multi",
    message:
      "Show Verna price in Delhi, compare with City, tell EMI for 5 years and check offers",
    context: {},
    user: null,
  },
  {
    id: "runtime-lead",
    message: "Best price for black Verna SX automatic",
    context: {},
    user: null,
  },
  {
    id: "runtime-internal",
    message: "Loan closure 7077",
    context: {},
    user: { id: "runtime-profiler-user", role: "admin" },
  },
];

let activeTestId = "";
let captureCommands = true;
const startedCommands = new Map();
const commandRecords = [];

const ensureDir = (dir) => fs.mkdirSync(dir, { recursive: true });

const sanitizeValue = (value, depth = 0) => {
  if (depth > 6) return "[MaxDepth]";
  if (value === null || value === undefined) return value;

  if (typeof value === "string") {
    if (/^\d{10}$/.test(value)) return "[mobile]";
    if (value.length > 250) return `${value.slice(0, 250)}…`;
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean") return value;

  if (value instanceof Date) return value.toISOString();

  if (Array.isArray(value)) {
    return value.slice(0, 30).map((item) => sanitizeValue(item, depth + 1));
  }

  if (typeof value === "object") {
    if (value._bsontype === "ObjectId") return "[ObjectId]";

    const out = {};
    for (const [key, val] of Object.entries(value).slice(0, 80)) {
      if (/password|token|secret|apiKey|authorization/i.test(key)) {
        out[key] = "[redacted]";
      } else {
        out[key] = sanitizeValue(val, depth + 1);
      }
    }
    return out;
  }

  return String(value);
};

const summarizeResponse = (response = {}) => ({
  intent: response.intent || "",
  mode: response.mode || "",
  displayMode: response.displayMode || "",
  canvasType: response.canvasType || "",
  inlineType: response.inlineType || "",
  title: response.title || "",
  plannerTools: Array.isArray(response.plannerTools) ? response.plannerTools : [],
  secondaryCount: Array.isArray(response.secondaryResponses)
    ? response.secondaryResponses.length
    : 0,
  runtimeResultsMeta: Array.isArray(response.runtimeResultsMeta)
    ? response.runtimeResultsMeta.map((item) => ({
        tool: item.tool,
        matched: item.matched,
        source: item.source,
        modulesChecked: item.modulesChecked,
        error: item.error || "",
      }))
    : [],
  service: response.service || {},
});

const compactExplain = (explain = {}) => {
  const stats = explain.executionStats || {};
  const queryPlanner = explain.queryPlanner || {};
  const stages = [];

  const walk = (stage, depth = 0) => {
    if (!stage || typeof stage !== "object" || depth > 10) return;

    if (stage.stage) {
      stages.push({
        stage: stage.stage,
        indexName: stage.indexName || "",
        direction: stage.direction || "",
        keyPattern: stage.keyPattern || undefined,
      });
    }

    for (const key of ["inputStage", "inputStages", "outerStage", "innerStage", "shards"]) {
      const child = stage[key];
      if (Array.isArray(child)) child.forEach((item) => walk(item, depth + 1));
      else if (child && typeof child === "object") walk(child, depth + 1);
    }
  };

  walk(queryPlanner.winningPlan || {});

  return {
    executionTimeMillis: stats.executionTimeMillis ?? null,
    nReturned: stats.nReturned ?? null,
    totalKeysExamined: stats.totalKeysExamined ?? null,
    totalDocsExamined: stats.totalDocsExamined ?? null,
    usesIndex: stages.some((s) => s.stage === "IXSCAN"),
    usesCollscan: stages.some((s) => s.stage === "COLLSCAN"),
    stages,
  };
};

const runExplainForFind = async (db, record) => {
  if (record.commandName !== "find") return null;
  if (!record.rawCommand?.find) return null;

  const collectionName = record.rawCommand.find;
  const filter = record.rawCommand.filter || {};
  const projection = record.rawCommand.projection || undefined;
  const sort = record.rawCommand.sort || undefined;
  const limit = Math.min(Number(record.rawCommand.limit || 20), 50);

  try {
    captureCommands = false;

    let cursor = db.collection(collectionName).find(filter);
    if (projection) cursor = cursor.project(projection);
    if (sort) cursor = cursor.sort(sort);
    cursor = cursor.limit(limit);

    const explain = await cursor.explain("executionStats");
    return compactExplain(explain);
  } catch (error) {
    return {
      error: error?.message || "Explain failed",
    };
  } finally {
    captureCommands = true;
  }
};

const connectWithMonitoring = async () => {
  const uri = process.env.MONGO_URI;
  if (!uri) {
    throw new Error("MONGO_URI is not configured.");
  }

  mongoose.set("strictQuery", false);

  await mongoose.connect(uri, {
    monitorCommands: true,
    serverSelectionTimeoutMS: 10000,
  });

  const client = mongoose.connection.getClient();

  client.on("commandStarted", (event) => {
    if (!captureCommands || !activeTestId) return;

    startedCommands.set(event.requestId, {
      activeTestId,
      requestId: event.requestId,
      commandName: event.commandName,
      databaseName: event.databaseName,
      collectionName:
        event.command?.find ||
        event.command?.aggregate ||
        event.command?.count ||
        event.command?.distinct ||
        event.command?.insert ||
        event.command?.update ||
        event.command?.delete ||
        "",
      rawCommand: event.command,
      command: sanitizeValue(event.command),
      startedAt: performance.now(),
    });
  });

  client.on("commandSucceeded", (event) => {
    if (!captureCommands) return;

    const started = startedCommands.get(event.requestId);
    if (!started) return;

    startedCommands.delete(event.requestId);

    commandRecords.push({
      ...started,
      ok: true,
      durationMs:
        typeof event.duration === "number"
          ? event.duration
          : Number((performance.now() - started.startedAt).toFixed(2)),
    });
  });

  client.on("commandFailed", (event) => {
    if (!captureCommands) return;

    const started = startedCommands.get(event.requestId);
    if (!started) return;

    startedCommands.delete(event.requestId);

    commandRecords.push({
      ...started,
      ok: false,
      error: event.failure?.message || "Mongo command failed",
      durationMs:
        typeof event.duration === "number"
          ? event.duration
          : Number((performance.now() - started.startedAt).toFixed(2)),
    });
  });
};

const groupBy = (items, getKey) => {
  const map = new Map();
  for (const item of items) {
    const key = getKey(item);
    map.set(key, [...(map.get(key) || []), item]);
  }
  return [...map.entries()].map(([key, values]) => ({ key, values }));
};

const summarizeCommands = (records = []) => {
  const totalDbMs = records.reduce((sum, item) => sum + (item.durationMs || 0), 0);

  const byCollection = groupBy(records, (item) => item.collectionName || item.commandName)
    .map(({ key, values }) => ({
      collection: key,
      count: values.length,
      totalMs: Number(values.reduce((sum, item) => sum + (item.durationMs || 0), 0).toFixed(2)),
      methods: [...new Set(values.map((item) => item.commandName))],
    }))
    .sort((a, b) => b.totalMs - a.totalMs);

  const byMethod = groupBy(records, (item) => item.commandName)
    .map(({ key, values }) => ({
      method: key,
      count: values.length,
      totalMs: Number(values.reduce((sum, item) => sum + (item.durationMs || 0), 0).toFixed(2)),
    }))
    .sort((a, b) => b.totalMs - a.totalMs);

  const slowCommands = [...records]
    .filter((item) => (item.durationMs || 0) >= 20)
    .sort((a, b) => (b.durationMs || 0) - (a.durationMs || 0))
    .slice(0, 20)
    .map((item) => ({
      commandName: item.commandName,
      collectionName: item.collectionName,
      durationMs: item.durationMs,
      ok: item.ok,
      command: item.command,
      explain: item.explain || null,
    }));

  return {
    commandCount: records.length,
    totalDbMs: Number(totalDbMs.toFixed(2)),
    byCollection,
    byMethod,
    slowCommands,
  };
};

const run = async () => {
  ensureDir(OUT_DIR);

  await connectWithMonitoring();

  const { chatWithAgent } = await import("../../services/aiAgent/aiAgent.service.js");
  const db = mongoose.connection.db;

  const results = [];

  for (const test of TESTS) {
    console.log(`Profiling: ${test.id} — ${test.message}`);

    activeTestId = test.id;
    const beforeCount = commandRecords.length;
    const start = performance.now();

    let response;
    let error = "";

    try {
      response = await chatWithAgent({
        message: test.message,
        context: test.context,
        user: test.user,
      });
    } catch (err) {
      error = err?.stack || err?.message || "Unknown error";
    }

    const totalMs = Number((performance.now() - start).toFixed(2));
    activeTestId = "";

    const records = commandRecords.slice(beforeCount).filter((item) => item.activeTestId === test.id);

    const explainTargets = records
      .filter((item) => item.commandName === "find")
      .sort((a, b) => (b.durationMs || 0) - (a.durationMs || 0))
      .slice(0, 10);

    for (const record of explainTargets) {
      record.explain = await runExplainForFind(db, record);
    }

    const commandSummary = summarizeCommands(records);
    const nonDbMs = Number(Math.max(0, totalMs - commandSummary.totalDbMs).toFixed(2));

    results.push({
      id: test.id,
      message: test.message,
      totalMs,
      dbMs: commandSummary.totalDbMs,
      nonDbMs,
      error,
      response: summarizeResponse(response),
      commandSummary,
      commands: records.map((item) => ({
        commandName: item.commandName,
        collectionName: item.collectionName,
        durationMs: item.durationMs,
        ok: item.ok,
        error: item.error || "",
        command: item.command,
        explain: item.explain || null,
      })),
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    databaseName: mongoose.connection.db.databaseName,
    results,
    summary: {
      totalTests: results.length,
      slowestTotal: [...results].sort((a, b) => b.totalMs - a.totalMs).slice(0, 5),
      slowestDb: [...results].sort((a, b) => b.dbMs - a.dbMs).slice(0, 5),
      slowestNonDb: [...results].sort((a, b) => b.nonDbMs - a.nonDbMs).slice(0, 5),
    },
  };

  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(OUT_MD, renderMarkdown(report));

  console.log("");
  console.log("✅ Runtime profile complete");
  console.log(OUT_MD);
  console.log(OUT_JSON);

  await mongoose.disconnect();
};

const mdRow = (items = []) =>
  `| ${items.map((item) => String(item ?? "").replace(/\n/g, " ")).join(" | ")} |`;

function renderMarkdown(report) {
  const lines = [];

  lines.push("# ACI Assist Runtime Profile");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Database: ${report.databaseName}`);
  lines.push("");

  lines.push("## Summary");
  lines.push("");
  lines.push(mdRow(["Test", "Total ms", "DB ms", "Non-DB ms", "DB commands", "Intent", "Canvas", "Error"]));
  lines.push(mdRow(["---", "---:", "---:", "---:", "---:", "---", "---", "---"]));

  for (const r of report.results) {
    lines.push(
      mdRow([
        r.id,
        r.totalMs,
        r.dbMs,
        r.nonDbMs,
        r.commandSummary.commandCount,
        r.response.intent,
        r.response.canvasType || r.response.inlineType || "",
        r.error ? "YES" : "",
      ]),
    );
  }

  lines.push("");
  lines.push("## Interpretation rules");
  lines.push("");
  lines.push("- If total ms is high but DB ms is low, latency is likely planner/model/JS shaping.");
  lines.push("- If DB ms is high, inspect slow commands and explain plans.");
  lines.push("- Any COLLSCAN in frequent public queries is a launch blocker.");
  lines.push("- Queries with large docs examined need indexes or read-model restructuring.");
  lines.push("");

  for (const r of report.results) {
    lines.push(`## ${r.id}: ${r.message}`);
    lines.push("");
    lines.push(`- Total: ${r.totalMs} ms`);
    lines.push(`- DB: ${r.dbMs} ms`);
    lines.push(`- Non-DB: ${r.nonDbMs} ms`);
    lines.push(`- Intent: ${r.response.intent}`);
    lines.push(`- Display: ${r.response.displayMode} / ${r.response.canvasType || r.response.inlineType || ""}`);
    lines.push("");

    lines.push("### Collections touched");
    lines.push("");
    if (!r.commandSummary.byCollection.length) {
      lines.push("No Mongo commands captured.");
    } else {
      lines.push(mdRow(["Collection", "Count", "Total ms", "Methods"]));
      lines.push(mdRow(["---", "---:", "---:", "---"]));
      for (const item of r.commandSummary.byCollection) {
        lines.push(mdRow([item.collection, item.count, item.totalMs, item.methods.join(", ")]));
      }
    }
    lines.push("");

    lines.push("### Slow commands");
    lines.push("");
    if (!r.commandSummary.slowCommands.length) {
      lines.push("No commands >= 20 ms.");
    } else {
      lines.push(mdRow(["Command", "Collection", "ms", "Index?", "COLLSCAN?", "Docs examined", "Keys examined"]));
      lines.push(mdRow(["---", "---", "---:", "---", "---", "---:", "---:"]));

      for (const cmd of r.commandSummary.slowCommands) {
        const explain = cmd.explain || {};
        lines.push(
          mdRow([
            cmd.commandName,
            cmd.collectionName,
            cmd.durationMs,
            explain.usesIndex ? "yes" : explain.error ? "n/a" : "no",
            explain.usesCollscan ? "YES" : explain.error ? "n/a" : "no",
            explain.totalDocsExamined ?? "",
            explain.totalKeysExamined ?? "",
          ]),
        );
      }
    }
    lines.push("");

    lines.push("### Runtime tool meta");
    lines.push("");
    if (!r.response.runtimeResultsMeta.length) {
      lines.push("No runtimeResultsMeta returned.");
    } else {
      for (const meta of r.response.runtimeResultsMeta) {
        lines.push(
          `- ${meta.tool}: matched=${meta.matched}, source=${meta.source}, modules=${(meta.modulesChecked || []).join(", ")}, error=${meta.error || ""}`,
        );
      }
    }
    lines.push("");
  }

  lines.push("## Next decisions");
  lines.push("");
  lines.push("- If DB time is low but response is slow, optimize planner/model path and early status streaming.");
  lines.push("- If DB time is high, add indexes or build lightweight read models.");
  lines.push("- If heavy collections are touched for simple queries, add projections or read-model collections.");
  lines.push("");

  return lines.join("\n");
}

run().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
