import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import mongoose from "mongoose";

const DEFAULT_SCRIPTS_DIR = path.resolve(process.cwd(), "scripts/vehicle-scrapers");
const SCRIPTS_DIR =
  process.env.SCRAPER_SCRIPTS_DIR || DEFAULT_SCRIPTS_DIR;
const PYTHON_BIN = process.env.SCRAPER_PYTHON_BIN || "python3";

const SCRIPT_CATALOG = {
  prices: {
    key: "prices",
    label: "Cardekho NCR Pricelist",
    filename: "cardekho_ncr_scraper.py",
    description: "Fetches make/model/variant city-wise prices and updates vehicles collection.",
    targetCollections: ["vehicles"],
  },
  colors: {
    key: "colors",
    label: "Color Names + Photos",
    filename: "cardekho_color_names_photos.py",
    description: "Fetches vehicle color names and image URLs into vehicle_colors collection.",
    targetCollections: ["vehicle_colors"],
  },
  features: {
    key: "features",
    label: "Variant Enrichment",
    filename: "variant_enrichment.py",
    description: "Fetches variant feature set into vehicle_features collection.",
    targetCollections: ["vehicle_features"],
  },
};

const RUN_HISTORY_LIMIT = 100;
const LOG_LIMIT_PER_RUN = 5000;
const runs = [];
const runsById = new Map();
const activeByScript = new Map();

const nowIso = () => new Date().toISOString();
const runId = () => `job_${Date.now()}_${Math.floor(Math.random() * 100000)}`;

const safeNum = (value) => (Number.isFinite(Number(value)) ? Number(value) : 0);

const getCollection = (name) => mongoose.connection?.db?.collection(name);

const getCollectionCount = async (name) => {
  try {
    const col = getCollection(name);
    if (!col) return 0;
    return await col.countDocuments();
  } catch {
    return 0;
  }
};

const getCollectionLatest = async (name) => {
  try {
    const col = getCollection(name);
    if (!col) return null;
    const latest = await col
      .find(
        {},
        {
          projection: {
            _id: 1,
            updatedAt: 1,
            createdAt: 1,
            scrape_timestamp: 1,
            last_updated: 1,
          },
        },
      )
      .sort({ _id: -1 })
      .limit(1)
      .toArray();

    if (!latest.length) return null;
    const item = latest[0];
    return {
      id: String(item._id),
      scrapeTimestamp: item.scrape_timestamp || null,
      lastUpdated: item.last_updated || null,
      updatedAt: item.updatedAt || null,
      createdAt: item.createdAt || null,
    };
  } catch {
    return null;
  }
};

const getCollectionsSnapshot = async () => {
  const targets = ["vehicles", "vehicle_colors", "vehicle_features"];
  const collections = {};
  for (const name of targets) {
    const [count, latest] = await Promise.all([
      getCollectionCount(name),
      getCollectionLatest(name),
    ]);
    collections[name] = { count, latest };
  }
  return { capturedAt: nowIso(), collections };
};

const scriptPath = (filename) => path.join(SCRIPTS_DIR, filename);
const scriptExists = (filename) => fs.existsSync(scriptPath(filename));

const isExecutionSupported = () => {
  if (process.env.VERCEL) return false;
  return true;
};

const tailList = (lines = [], max = 250) =>
  Array.isArray(lines) ? lines.slice(Math.max(0, lines.length - max)) : [];

const pushLog = (run, stream, data) => {
  const lines = String(data || "")
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0);
  lines.forEach((line) => run.logs.push(`[${stream}] ${line}`));
  if (run.logs.length > LOG_LIMIT_PER_RUN) {
    run.logs = run.logs.slice(run.logs.length - LOG_LIMIT_PER_RUN);
  }
};

const toRunResponse = (run, { includeLogs = false } = {}) => ({
  id: run.id,
  scriptKey: run.scriptKey,
  label: run.label,
  filename: run.filename,
  status: run.status,
  startedAt: run.startedAt,
  endedAt: run.endedAt,
  durationMs: run.durationMs || null,
  exitCode: run.exitCode,
  error: run.error || null,
  pid: run.pid || null,
  summaryBefore: run.summaryBefore || null,
  summaryAfter: run.summaryAfter || null,
  delta: run.delta || null,
  logsTail: tailList(run.logs, 200),
  logsCount: run.logs.length,
  logs: includeLogs ? run.logs : undefined,
});

const buildDelta = (before, after) => {
  const names = new Set([
    ...Object.keys(before?.collections || {}),
    ...Object.keys(after?.collections || {}),
  ]);
  const collections = {};
  names.forEach((name) => {
    const b = safeNum(before?.collections?.[name]?.count);
    const a = safeNum(after?.collections?.[name]?.count);
    collections[name] = { before: b, after: a, delta: a - b };
  });
  return { collections };
};

export const getVehicleScraperCatalog = () =>
  Object.values(SCRIPT_CATALOG).map((item) => ({
    ...item,
    scriptPath: scriptPath(item.filename),
    exists: scriptExists(item.filename),
    runnable: isExecutionSupported() && scriptExists(item.filename),
  }));

export const getVehicleScraperRuns = () => runs.map((run) => toRunResponse(run));

export const getVehicleScraperRun = (id, options = {}) => {
  const run = runsById.get(id);
  return run ? toRunResponse(run, options) : null;
};

export const getVehicleScraperSummary = async () => {
  const snapshot = await getCollectionsSnapshot();
  return {
    executionSupported: isExecutionSupported(),
    scriptsDir: SCRIPTS_DIR,
    pythonBin: PYTHON_BIN,
    activeRuns: Array.from(activeByScript.values()).map((run) =>
      toRunResponse(run),
    ),
    snapshot,
  };
};

export const startVehicleScraperRun = async (scriptKey) => {
  const script = SCRIPT_CATALOG[scriptKey];
  if (!script) {
    const err = new Error(`Unsupported script key: ${scriptKey}`);
    err.statusCode = 400;
    throw err;
  }
  if (!isExecutionSupported()) {
    const err = new Error(
      "Script execution is not supported on this deployment target. Run from local backend environment.",
    );
    err.statusCode = 400;
    throw err;
  }
  if (!scriptExists(script.filename)) {
    const err = new Error(`Script not found: ${scriptPath(script.filename)}`);
    err.statusCode = 404;
    throw err;
  }
  if (activeByScript.size > 0) {
    const active = Array.from(activeByScript.values())[0];
    const err = new Error(
      `Another script is already running (${active.label}). Please wait for it to complete.`,
    );
    err.statusCode = 409;
    throw err;
  }

  const before = await getCollectionsSnapshot();
  const run = {
    id: runId(),
    scriptKey: script.key,
    label: script.label,
    filename: script.filename,
    status: "running",
    startedAt: nowIso(),
    endedAt: null,
    durationMs: null,
    exitCode: null,
    error: null,
    pid: null,
    logs: [],
    summaryBefore: before,
    summaryAfter: null,
    delta: null,
  };

  runs.unshift(run);
  runsById.set(run.id, run);
  if (runs.length > RUN_HISTORY_LIMIT) {
    const removed = runs.splice(RUN_HISTORY_LIMIT);
    removed.forEach((item) => runsById.delete(item.id));
  }
  activeByScript.set(script.key, run);

  const child = spawn(PYTHON_BIN, [script.filename], {
    cwd: SCRIPTS_DIR,
    env: process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  run.pid = child.pid;

  child.stdout.on("data", (data) => pushLog(run, "stdout", data));
  child.stderr.on("data", (data) => pushLog(run, "stderr", data));

  child.on("error", async (error) => {
    run.status = "failed";
    run.error = error.message;
    run.exitCode = -1;
    run.endedAt = nowIso();
    run.durationMs = new Date(run.endedAt).getTime() - new Date(run.startedAt).getTime();
    run.summaryAfter = await getCollectionsSnapshot();
    run.delta = buildDelta(run.summaryBefore, run.summaryAfter);
    activeByScript.delete(script.key);
  });

  child.on("close", async (code) => {
    run.exitCode = code;
    run.status = code === 0 ? "completed" : "failed";
    run.endedAt = nowIso();
    run.durationMs = new Date(run.endedAt).getTime() - new Date(run.startedAt).getTime();
    run.summaryAfter = await getCollectionsSnapshot();
    run.delta = buildDelta(run.summaryBefore, run.summaryAfter);
    activeByScript.delete(script.key);
  });

  return toRunResponse(run, { includeLogs: true });
};
