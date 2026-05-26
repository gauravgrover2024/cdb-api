import { spawnSync } from "child_process";
import fs from "fs";
import path from "path";

const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const outDir = path.resolve(`aci_safety_gate_${stamp}`);
fs.mkdirSync(outDir, { recursive: true });

const logPath = path.join(outDir, "aci_safety_gate_full.log");

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(logPath, "", { flag: "a" });

const run = (label, command, args) => {
  console.log(`\n▶ ${label}`);
  const result = spawnSync(command, args, {
    encoding: "utf8",
    shell: false,
  });

  const output = `${result.stdout || ""}${result.stderr || ""}`;

  fs.mkdirSync(path.dirname(logPath), { recursive: true });
  fs.appendFileSync(logPath, `\n\n===== ${label} =====\n${output}`);

  return {
    label,
    ok: result.status === 0,
    status: result.status,
    output,
  };
};

const foundation = run("Foundation", "npm", ["run", "test:aci:foundation"]);

const modelResolver = run("Model resolver audit", "node", [
  "src/scripts/aci-audits/auditAciModelResolver.js",
]);

const modelContextResolver = run("Model context resolver audit", "node", [
  "src/scripts/aci-audits/auditAciModelContextResolver.js",
]);

const contextPriority = run("Context priority audit", "node", [
  "src/scripts/aci-audits/auditAciContextPriority.js",
]);

const vehicleEntityIndex = run("Vehicle entity index audit", "node", [
  "src/scripts/aci-audits/auditAciVehicleEntityIndex.js",
]);

const contextSwitch = run("Context switch audit", "node", [
  "src/scripts/aci-audits/auditAciContextSwitch.js",
]);

const allOutput = `${foundation.output}\n${modelResolver.output}\n${modelContextResolver.output}\n${contextPriority.output}\n${vehicleEntityIndex.output}\n${contextSwitch.output}`;

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

const hasFailures =
  !foundation.ok ||
  !modelResolver.ok ||
  !modelContextResolver.ok ||
  !contextPriority.ok ||
  !vehicleEntityIndex.ok ||
  !contextSwitch.ok ||
  /"failed"\s*:\s*[1-9]/.test(allOutput) ||
  /"pass"\s*:\s*false/.test(allOutput) ||
  /ReferenceError|TypeError|SyntaxError|MongoServerError|contractErrors"\s*:\s*\[[^\]]*\{/.test(allOutput) ||
  missingContextChecks.length > 0;

const summary = {
  ok: !hasFailures,
  foundationExitCode: foundation.status,
  modelResolverExitCode: modelResolver.status,
  modelContextResolverExitCode: modelContextResolver.status,
  contextPriorityExitCode: contextPriority.status,
  vehicleEntityIndexExitCode: vehicleEntityIndex.status,
  contextSwitchExitCode: contextSwitch.status,
  passedMentions: count(/"pass"\s*:\s*true/g),
  failedMentions: count(/"pass"\s*:\s*false/g),
  suites: {
    executor: extractSuite("ACI Assist executor smoke"),
    v2: extractSuite("ACI Assist V2 service smoke"),
    contract: extractSuite("ACI Assist V2 official contract foundation"),
    modelResolver: extractSuite("ACI model resolver audit"),
    modelContextResolver: extractSuite("ACI model context resolver audit"),
    contextPriority: extractSuite("ACI context priority audit"),
    vehicleEntityIndex: extractSuite("ACI vehicle entity index audit"),
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
