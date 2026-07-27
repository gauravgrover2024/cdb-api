import { spawn } from "child_process";
import fs from "fs";
import path from "path";

const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const outDir = path.resolve(`aci_safety_gate_${stamp}`);
fs.mkdirSync(outDir, { recursive: true });

const logPath = path.join(outDir, "aci_safety_gate_full.log");
fs.writeFileSync(logPath, "", { flag: "a" });

const SAFETY_MODE = String(process.env.ACI_SAFETY_MODE || "full").toLowerCase();
const SAFETY_WORKERS = Math.max(
  1,
  Number(process.env.ACI_SAFETY_WORKERS || (SAFETY_MODE === "fast" ? 6 : 4)),
);

const tasks = [
  {
    key: "executor",
    label: "Executor smoke",
    command: "node",
    args: ["src/scripts/testAiExecutor.js"],
  },
  {
    key: "v2",
    label: "V2 service smoke",
    command: "node",
    args: ["src/scripts/testAiAssist.v2.js"],
  },
  {
    key: "contract",
    label: "Contract foundation",
    command: "node",
    args: ["src/scripts/testAciContractFoundation.js"],
  },
  {
    key: "modelResolver",
    label: "Model resolver audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciModelResolver.js"],
  },
  {
    key: "modelContextResolver",
    label: "Model context resolver audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciModelContextResolver.js"],
  },
  {
    key: "contextPriority",
    label: "Context priority audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciContextPriority.js"],
  },
  {
    key: "vehicleEntityIndex",
    label: "Vehicle entity index audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciVehicleEntityIndex.js"],
  },
  {
    key: "multiFeatureQueries",
    label: "Multi-feature query audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciMultiFeatureQueries.js"],
  },
  {
    key: "variantMultiFeatureQueries",
    label: "Variant multi-feature query audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciVariantMultiFeatureQueries.js"],
  },
  {
    key: "featureComparisonQueries",
    label: "Feature comparison query audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciFeatureComparisonQueries.js"],
  },
  {
    key: "modelAliasFeatureQueries",
    label: "Model alias feature query audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciModelAliasFeatureQueries.js"],
  },
  {
    key: "contextManager",
    label: "Context manager audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciContextManagerV1.js"],
  },
  {
    key: "embarrassmentQueries",
    label: "Embarrassment query audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciEmbarrassmentQueries.js"],
  },
  {
    key: "contextSwitch",
    label: "Context switch audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciContextSwitch.js"],
  },
  {
    key: "backendFreezeTrust",
    label: "Backend freeze trust audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciBackendFreezeTrust.js"],
  },
  {
    key: "noDataBaselineFreezeGate",
    label: "No-data baseline freeze gate",
    command: "node",
    args: ["src/scripts/aci-audits/runAciNoDataBaselineFreezeGateV1.cjs"],
    env: {
      ACI_NO_DATA_BASELINE_STRICT: "1",
      ACI_DEEP_AUDIT_WORKERS: "10",
    },
  },
  {
    key: "answerLanguage",
    label: "Answer language registry audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciAnswerLanguageRegistryV1.js"],
  },
  {
    key: "noHardcodedVehicleFacts",
    label: "No-hardcoded vehicle facts audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciNoHardcodedVehicleFacts.cjs"],
  },
  {
    key: "featureExplainer",
    label: "Feature explainer smoke",
    command: "node",
    args: ["src/scripts/aci-audits/smokeAciFeatureExplainerV1.cjs"],
  },
  {
    key: "factualTraceMetadata",
    label: "Factual trace metadata audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciFactualTraceMetadataV1.cjs"],
    env: {
      ACI_TRACE_AUDIT_MODE: "bridge",
      ACI_TRACE_AUDIT_STRICT: "1",
    },
  },
  {
    key: "unsupportedCityHonesty",
    label: "Unsupported city honesty audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciUnsupportedCityHonestyV1.cjs"],
    env: {
      ACI_UNSUPPORTED_CITY_AUDIT_STRICT: "1",
    },
  },
  {
    key: "buyerAnswerQuality",
    label: "Buyer answer quality audit",
    command: "node",
    args: ["src/scripts/aci-audits/auditAciBuyerAnswerQualityV1.cjs"],
  },
];


const filterTasksForSafetyMode = (allTasks = []) => {
  if (SAFETY_MODE === "fast") {
    const fastKeys = new Set([
      "modelResolver",
      "modelContextResolver",
      "contextPriority",
      "contextManager",
      "modelAliasFeatureQueries",
      "embarrassmentQueries",
      "answerLanguage",
      "noHardcodedVehicleFacts",
      "featureExplainer",
      "factualTraceMetadata",
      "unsupportedCityHonesty",
      "buyerAnswerQuality",
    ]);

    return allTasks.filter((task) => fastKeys.has(task.key));
  }

  if (SAFETY_MODE === "freeze") {
    const freezeKeys = new Set([
      "contract",
      "contextSwitch",
      "backendFreezeTrust",
      "noDataBaselineFreezeGate",
      "vehicleEntityIndex",
      "featureComparisonQueries",
      "multiFeatureQueries",
      "variantMultiFeatureQueries",
    ]);

    return allTasks.filter((task) => freezeKeys.has(task.key));
  }

  return allTasks;
};

const runTasksWithLimit = async (selectedTasks = [], workerCount = 4) => {
  const results = new Array(selectedTasks.length);
  let cursor = 0;

  const workers = Array.from(
    { length: Math.min(workerCount, selectedTasks.length || 1) },
    async () => {
      while (cursor < selectedTasks.length) {
        const index = cursor;
        cursor += 1;
        results[index] = await runTask(selectedTasks[index]);
      }
    },
  );

  await Promise.all(workers);
  return results;
};

const selectedTasks = filterTasksForSafetyMode(tasks);

const runSelectedTasks = async () => {
  if (SAFETY_MODE !== "fast") {
    return runTasksWithLimit(selectedTasks, SAFETY_WORKERS);
  }

  const contextManagerTask = selectedTasks.find((task) => task.key === "contextManager");
  const remainingTasks = selectedTasks.filter((task) => task.key !== "contextManager");
  const contextResults = contextManagerTask ? [await runTask(contextManagerTask)] : [];
  const remainingResults = await runTasksWithLimit(remainingTasks, SAFETY_WORKERS);

  return [...contextResults, ...remainingResults];
};

const runTask = (task) =>
  new Promise((resolve) => {
    console.log(`\n▶ ${task.label}`);

    const startedAt = Date.now();
    const child = spawn(task.command, task.args, {
      shell: false,
      env: { ...process.env, ...(task.env || {}) },
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (error) => {
      const durationMs = Date.now() - startedAt;
      const output = `${stdout}${stderr}\n${error?.stack || error?.message || String(error)}`;

      resolve({
        ...task,
        ok: false,
        status: 1,
        durationMs,
        output,
      });
    });

    child.on("close", (code, signal) => {
      const durationMs = Date.now() - startedAt;
      const output = `${stdout}${stderr}${signal ? `\nProcess terminated by signal: ${signal}` : ""}`;

      resolve({
        ...task,
        ok: code === 0,
        status: Number.isInteger(code) ? code : 1,
        signal: signal || "",
        durationMs,
        output,
      });
    });
  });

const startedAt = Date.now();
const results = await runSelectedTasks();
const totalDurationMs = Date.now() - startedAt;

for (const result of results) {
  fs.appendFileSync(
    logPath,
    `\n\n===== ${result.label} (${result.durationMs}ms) =====\n${result.output}`,
  );

  const icon = result.ok ? "✅" : "❌";
  console.log(`${icon} ${result.label} finished in ${result.durationMs}ms`);
}

const byKey = Object.fromEntries(results.map((result) => [result.key, result]));
const allOutput = results.map((result) => result.output).join("\n");

const extractSuite = (name) => {
  const regex = new RegExp(
    `"suite"\\s*:\\s*"${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"[\\s\\S]*?"failedIds"\\s*:\\s*\\[[\\s\\S]*?\\]`,
    "m",
  );
  const match = allOutput.match(regex);
  return match ? `{ ${match[0]} }` : "";
};

const count = (pattern) => (allOutput.match(pattern) || []).length;

const contextChecks = [
  "switch-price-creta-from-verna-context",
  "switch-colors-seltos-from-verna-context",
  "switch-emi-city-from-verna-context",
  "switch-feature-thar-from-verna-context",
  "no-context-creta",
];

const selectedTaskKeys = new Set(selectedTasks.map((task) => task.key));
const missingContextChecks = selectedTaskKeys.has("contextSwitch")
  ? contextChecks.filter((key) => !allOutput.includes(key))
  : [];
const durationFor = (key) => byKey[key]?.durationMs ?? 0;

const parseTaskJson = (key) => {
  const output = byKey[key]?.output || "";
  const start = output.indexOf("{");
  const end = output.lastIndexOf("}");

  if (start < 0 || end < 0 || end <= start) return null;

  try {
    return JSON.parse(output.slice(start, end + 1));
  } catch {
    return null;
  }
};

const reportedDurationFor = (key) => {
  const parsed = parseTaskJson(key);
  const durationMs = Number(parsed?.durationMs);
  return Number.isFinite(durationMs) && durationMs >= 0 ? durationMs : null;
};

const slowCheckDurationFor = (key) => reportedDurationFor(key) ?? durationFor(key);

const foundationKeys = ["executor", "v2", "contract"];
const selectedFoundationKeys = foundationKeys.filter((key) => selectedTaskKeys.has(key));
const foundationOk =
  selectedFoundationKeys.length === 0 ||
  selectedFoundationKeys.every((key) => Boolean(byKey[key]?.ok));

const foundationDurationMs =
  (byKey.executor?.durationMs || 0) +
  (byKey.v2?.durationMs || 0) +
  (byKey.contract?.durationMs || 0);

const slowThresholdsMs = {
  contextManager: 15000,
  modelAliasFeatureQueries: 60000,
  embarrassmentQueries: 60000,
  noHardcodedVehicleFacts: 60000,
  featureExplainer: 60000,
  factualTraceMetadata: 60000,
  unsupportedCityHonesty: 60000,
  noDataBaselineFreezeGate: 120000,
  total: SAFETY_MODE === "fast" ? 90000 : 180000,
};

const slowSuites = [
  ...Object.entries(slowThresholdsMs)
    .filter(([key]) => key !== "total" && selectedTaskKeys.has(key))
    .map(([key, thresholdMs]) => ({
      key,
      label: byKey[key]?.label || key,
      durationMs: slowCheckDurationFor(key),
      wrapperDurationMs: durationFor(key),
      suiteReportedDurationMs: reportedDurationFor(key),
      thresholdMs,
    }))
    .filter((item) => item.durationMs > item.thresholdMs),
  ...(totalDurationMs > slowThresholdsMs.total
    ? [{
        key: "total",
        label: "Fast safety total",
        durationMs: totalDurationMs,
        thresholdMs: slowThresholdsMs.total,
      }]
    : []),
];

const hasFailures =
  !foundationOk ||
  results.some((result) => !result.ok) ||
  /"failed"\s*:\s*[1-9]/.test(allOutput) ||
  /"pass"\s*:\s*false/.test(allOutput) ||
  /ReferenceError|TypeError|SyntaxError|MongoServerError|contractErrors"\s*:\s*\[[^\]]*\{/.test(allOutput) ||
  missingContextChecks.length > 0 ||
  slowSuites.length > 0;

const exitCodeFor = (key) => {
  if (!selectedTaskKeys.has(key)) return 0;
  return byKey[key]?.status ?? 1;
};

const selectedFoundationExitCode = () => {
  if (!selectedFoundationKeys.length) return 0;
  return selectedFoundationKeys.some((key) => exitCodeFor(key) !== 0) ? 1 : 0;
};

const summary = {
  mode: SAFETY_MODE,
  workers: SAFETY_WORKERS,

  ok: !hasFailures,

  foundationExitCode: selectedFoundationExitCode(),
  executorExitCode: exitCodeFor("executor"),
  v2ExitCode: exitCodeFor("v2"),
  contractExitCode: exitCodeFor("contract"),

  modelResolverExitCode: exitCodeFor("modelResolver"),
  modelContextResolverExitCode: exitCodeFor("modelContextResolver"),
  contextPriorityExitCode: exitCodeFor("contextPriority"),
  vehicleEntityIndexExitCode: exitCodeFor("vehicleEntityIndex"),
  multiFeatureQueriesExitCode: exitCodeFor("multiFeatureQueries"),
  variantMultiFeatureQueriesExitCode: exitCodeFor("variantMultiFeatureQueries"),
  featureComparisonQueriesExitCode: exitCodeFor("featureComparisonQueries"),
  modelAliasFeatureQueriesExitCode: exitCodeFor("modelAliasFeatureQueries"),
  contextManagerExitCode: exitCodeFor("contextManager"),
  embarrassmentQueriesExitCode: exitCodeFor("embarrassmentQueries"),
  contextSwitchExitCode: exitCodeFor("contextSwitch"),
  backendFreezeTrustExitCode: exitCodeFor("backendFreezeTrust"),
  noDataBaselineFreezeGateExitCode: exitCodeFor("noDataBaselineFreezeGate"),
  noHardcodedVehicleFactsExitCode: exitCodeFor("noHardcodedVehicleFacts"),
  featureExplainerExitCode: exitCodeFor("featureExplainer"),
  factualTraceMetadataExitCode: exitCodeFor("factualTraceMetadata"),
  unsupportedCityHonestyExitCode: exitCodeFor("unsupportedCityHonesty"),

  passedMentions: count(/"pass"\s*:\s*true/g),
  failedMentions: count(/"pass"\s*:\s*false/g),

  reportedDurationsMs: {
    executor: reportedDurationFor("executor"),
    v2: reportedDurationFor("v2"),
    contract: reportedDurationFor("contract"),
    modelResolver: reportedDurationFor("modelResolver"),
    modelContextResolver: reportedDurationFor("modelContextResolver"),
    contextPriority: reportedDurationFor("contextPriority"),
    vehicleEntityIndex: reportedDurationFor("vehicleEntityIndex"),
    multiFeatureQueries: reportedDurationFor("multiFeatureQueries"),
    variantMultiFeatureQueries: reportedDurationFor("variantMultiFeatureQueries"),
    featureComparisonQueries: reportedDurationFor("featureComparisonQueries"),
    modelAliasFeatureQueries: reportedDurationFor("modelAliasFeatureQueries"),
    contextManager: reportedDurationFor("contextManager"),
    embarrassmentQueries: reportedDurationFor("embarrassmentQueries"),
    contextSwitch: reportedDurationFor("contextSwitch"),
    backendFreezeTrust: reportedDurationFor("backendFreezeTrust"),
    noDataBaselineFreezeGate: reportedDurationFor("noDataBaselineFreezeGate"),
    noHardcodedVehicleFacts: reportedDurationFor("noHardcodedVehicleFacts"),
    featureExplainer: reportedDurationFor("featureExplainer"),
    factualTraceMetadata: reportedDurationFor("factualTraceMetadata"),
    unsupportedCityHonesty: reportedDurationFor("unsupportedCityHonesty"),
  },

  durationsMs: {
    executor: durationFor("executor"),
    v2: durationFor("v2"),
    contract: durationFor("contract"),
    foundationSequentialEquivalent: durationFor("executor") + durationFor("v2") + durationFor("contract"),
    modelResolver: durationFor("modelResolver"),
    modelContextResolver: durationFor("modelContextResolver"),
    contextPriority: durationFor("contextPriority"),
    vehicleEntityIndex: durationFor("vehicleEntityIndex"),
    multiFeatureQueries: durationFor("multiFeatureQueries"),
    variantMultiFeatureQueries: durationFor("variantMultiFeatureQueries"),
    featureComparisonQueries: durationFor("featureComparisonQueries"),
    modelAliasFeatureQueries: durationFor("modelAliasFeatureQueries"),
    contextManager: durationFor("contextManager"),
    embarrassmentQueries: durationFor("embarrassmentQueries"),
    contextSwitch: durationFor("contextSwitch"),
    backendFreezeTrust: durationFor("backendFreezeTrust"),
    noDataBaselineFreezeGate: durationFor("noDataBaselineFreezeGate"),
    noHardcodedVehicleFacts: durationFor("noHardcodedVehicleFacts"),
    featureExplainer: durationFor("featureExplainer"),
    factualTraceMetadata: durationFor("factualTraceMetadata"),
    unsupportedCityHonesty: durationFor("unsupportedCityHonesty"),
    total: totalDurationMs,
  },

  suites: {
    executor: extractSuite("ACI Assist executor smoke"),
    v2: extractSuite("ACI Assist V2 service smoke"),
    contract: extractSuite("ACI Assist V2 official contract foundation"),
    modelResolver: extractSuite("ACI model resolver audit"),
    modelContextResolver: extractSuite("ACI model context resolver audit"),
    contextPriority: extractSuite("ACI context priority audit"),
    vehicleEntityIndex: extractSuite("ACI vehicle entity index audit"),
    multiFeatureQueries: extractSuite("ACI multi-feature query audit"),
    variantMultiFeatureQueries: extractSuite("ACI variant multi-feature query audit"),
    featureComparisonQueries: extractSuite("ACI feature comparison query audit"),
    embarrassmentQueries: extractSuite("ACI embarrassment query audit"),
    modelAliasFeatureQueries: extractSuite("ACI model alias feature query audit"),
    contextManager: extractSuite("ACI Context Manager V1 audit"),
    noHardcodedVehicleFacts: extractSuite("ACI No-Hardcoded Vehicle Facts Audit"),
    featureExplainer: extractSuite("ACI Feature Explainer smoke v1"),
    factualTraceMetadata: extractSuite("ACI Factual Trace Metadata Audit v1"),
    unsupportedCityHonesty: extractSuite("ACI Unsupported City Honesty Audit v1"),
    noDataBaselineFreezeGate: extractSuite("ACI No-Data Baseline Freeze Gate v1"),
    noDataBaseline: extractSuite("ACI No-Data Baseline Audit v1"),
  },

  slowThresholdsMs,
  slowSuites,

  contextSwitch: {
    expectedChecks: contextChecks,
    missingChecks: missingContextChecks,
  },

  fullLogPath: logPath,
};

console.log("\n==============================");
console.log("ACI SAFETY GATE SUMMARY");
console.log("==============================");
console.log(JSON.stringify(summary, null, 2));

if (summary.ok) {
  console.log("\n✅ Safety gate passed. You only need to paste this summary unless I ask for the full log.");
} else {
  console.log("\n❌ Safety gate failed. Paste this summary plus the failed section from the full log.");
}

process.exitCode = summary.ok ? 0 : 1;
