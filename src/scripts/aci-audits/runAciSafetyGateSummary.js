import { spawn } from "child_process";
import fs from "fs";
import path from "path";

const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const outDir = path.resolve(`aci_safety_gate_${stamp}`);
fs.mkdirSync(outDir, { recursive: true });

const logPath = path.join(outDir, "aci_safety_gate_full.log");
fs.writeFileSync(logPath, "", { flag: "a" });

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
];

const runTask = (task) =>
  new Promise((resolve) => {
    console.log(`\n▶ ${task.label}`);

    const startedAt = Date.now();
    const child = spawn(task.command, task.args, {
      shell: false,
      env: process.env,
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
const results = await Promise.all(tasks.map(runTask));
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

const missingContextChecks = contextChecks.filter((key) => !allOutput.includes(key));

const foundationOk =
  Boolean(byKey.executor?.ok) &&
  Boolean(byKey.v2?.ok) &&
  Boolean(byKey.contract?.ok);

const hasFailures =
  !foundationOk ||
  results.some((result) => !result.ok) ||
  /"failed"\s*:\s*[1-9]/.test(allOutput) ||
  /"pass"\s*:\s*false/.test(allOutput) ||
  /ReferenceError|TypeError|SyntaxError|MongoServerError|contractErrors"\s*:\s*\[[^\]]*\{/.test(allOutput) ||
  missingContextChecks.length > 0;

const foundationDurationMs =
  (byKey.executor?.durationMs || 0) +
  (byKey.v2?.durationMs || 0) +
  (byKey.contract?.durationMs || 0);

const summary = {
  ok: !hasFailures,

  foundationExitCode: foundationOk ? 0 : 1,
  executorExitCode: byKey.executor?.status ?? 1,
  v2ExitCode: byKey.v2?.status ?? 1,
  contractExitCode: byKey.contract?.status ?? 1,

  modelResolverExitCode: byKey.modelResolver?.status ?? 1,
  modelContextResolverExitCode: byKey.modelContextResolver?.status ?? 1,
  contextPriorityExitCode: byKey.contextPriority?.status ?? 1,
  vehicleEntityIndexExitCode: byKey.vehicleEntityIndex?.status ?? 1,
  multiFeatureQueriesExitCode: byKey.multiFeatureQueries?.status ?? 1,
  variantMultiFeatureQueriesExitCode: byKey.variantMultiFeatureQueries?.status ?? 1,
  featureComparisonQueriesExitCode: byKey.featureComparisonQueries?.status ?? 1,
  modelAliasFeatureQueriesExitCode: byKey.modelAliasFeatureQueries?.status ?? 1,
  embarrassmentQueriesExitCode: byKey.embarrassmentQueries?.status ?? 1,
  contextSwitchExitCode: byKey.contextSwitch?.status ?? 1,
  backendFreezeTrustExitCode: byKey.backendFreezeTrust?.status ?? 1,

  passedMentions: count(/"pass"\s*:\s*true/g),
  failedMentions: count(/"pass"\s*:\s*false/g),

  durationsMs: {
    executor: byKey.executor?.durationMs ?? 0,
    v2: byKey.v2?.durationMs ?? 0,
    contract: byKey.contract?.durationMs ?? 0,
    foundationSequentialEquivalent: foundationDurationMs,
    modelResolver: byKey.modelResolver?.durationMs ?? 0,
    modelContextResolver: byKey.modelContextResolver?.durationMs ?? 0,
    contextPriority: byKey.contextPriority?.durationMs ?? 0,
    vehicleEntityIndex: byKey.vehicleEntityIndex?.durationMs ?? 0,
    multiFeatureQueries: byKey.multiFeatureQueries?.durationMs ?? 0,
    variantMultiFeatureQueries: byKey.variantMultiFeatureQueries?.durationMs ?? 0,
    featureComparisonQueries: byKey.featureComparisonQueries?.durationMs ?? 0,
    modelAliasFeatureQueries: byKey.modelAliasFeatureQueries?.durationMs ?? 0,
    embarrassmentQueries: byKey.embarrassmentQueries?.durationMs ?? 0,
    contextSwitch: byKey.contextSwitch?.durationMs ?? 0,
    backendFreezeTrust: byKey.backendFreezeTrust?.durationMs ?? 0,
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
  },

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

if (hasFailures) {
  console.log("\n❌ Safety gate failed. Paste this summary plus the failed section from the full log.");
  process.exit(1);
}

console.log("\n✅ Safety gate passed. You only need to paste this summary unless I ask for the full log.");
