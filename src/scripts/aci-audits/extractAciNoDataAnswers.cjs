#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const JSON_START = "__ACI_DEEP_AUDIT_JSON_START__";
const JSON_END = "__ACI_DEEP_AUDIT_JSON_END__";
const TMP_DIR = "/tmp";
const FULL_AUDIT_LOG_PATTERNS = [
  /^aci_full_185.*\.log$/,
  /^aci_full_185_after_green.*\.log$/,
];

const RUN_FULL_AUDIT_COMMAND =
  "ACI_DEEP_AUDIT_WORKERS=10 npm run aci:buyer-answer:deep-audit | tee /tmp/aci_full_185_after_green_$(date +%Y%m%d_%H%M%S).log";

const text = (value = "") => String(value || "");

const timestamp = () => {
  const date = new Date();
  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    pad(date.getMonth() + 1),
    pad(date.getDate()),
    "_",
    pad(date.getHours()),
    pad(date.getMinutes()),
    pad(date.getSeconds()),
  ].join("");
};

const compactRow = (entry = {}) => ({
  id: entry.id || "",
  groupKey: entry.groupKey || text(entry.group).slice(0, 1).toUpperCase() || "",
  message: entry.message || "",
  tool: entry.tool || entry.response?.tool || "",
  title: entry.title || entry.response?.title || "",
  answerPreview: entry.answerPreview || entry.response?.answerPreview || "",
});

const findLatestAuditLog = () => {
  let entries = [];

  try {
    entries = fs.readdirSync(TMP_DIR, { withFileTypes: true })
      .filter((entry) => entry.isFile())
      .filter((entry) => FULL_AUDIT_LOG_PATTERNS.some((pattern) => pattern.test(entry.name)))
      .map((entry) => {
        const filePath = path.join(TMP_DIR, entry.name);
        const stat = fs.statSync(filePath);
        return {
          path: filePath,
          mtimeMs: stat.mtimeMs,
        };
      });
  } catch {
    return "";
  }

  entries.sort((left, right) => right.mtimeMs - left.mtimeMs || right.path.localeCompare(left.path));
  return entries[0]?.path || "";
};

const parseDeepAuditSummary = (sourcePath = "") => {
  const raw = fs.readFileSync(sourcePath, "utf8");
  const start = raw.lastIndexOf(JSON_START);
  if (start < 0) {
    throw new Error(`Could not find ${JSON_START} in ${sourcePath}`);
  }

  const jsonStart = start + JSON_START.length;
  const end = raw.indexOf(JSON_END, jsonStart);
  if (end < 0) {
    throw new Error(`Could not find ${JSON_END} after latest JSON start in ${sourcePath}`);
  }

  const jsonText = raw.slice(jsonStart, end).trim();
  try {
    return JSON.parse(jsonText);
  } catch (error) {
    throw new Error(`Could not parse deep-audit JSON from ${sourcePath}: ${error?.message || String(error)}`);
  }
};

const classifyNoDataAnswer = (entry = {}) => {
  const message = text(entry.message).toLowerCase();
  const answer = text(entry.answerPreview || entry.response?.answerPreview).toLowerCase();
  const title = text(entry.title || entry.response?.title).toLowerCase();
  const tool = text(entry.tool || entry.response?.tool).toLowerCase();
  const blob = `${message} ${answer} ${title} ${tool}`;

  if (
    /\b(mumbai|bangalore|bengaluru|jaipur)\b/.test(blob) &&
    /\b(price|on road|on-road|ex showroom|ex-showroom|pricelist)\b/.test(blob)
  ) {
    return "expectedUnsupportedCity";
  }

  if (
    /\b(offer|offers|discount|deal|benefit|service cost|maintenance|insurance|bank|finance scheme|waiting period|stock|availability|delivery|service center|service centre)\b/.test(blob) ||
    /\bnot available yet|not available in this system yet|until .* collection|pending module\b/.test(blob)
  ) {
    return "expectedPendingModule";
  }

  if (tool === "vehicle_compare") {
    return "likelyComparisonEvidenceGap";
  }

  if (
    tool === "vehicle_colors" &&
    /\bexact\b.*\b(?:color|colour|shade)\b.*\bnot found|could not find an exact\b.*\b(?:color|colour|shade)\b/.test(blob) &&
    /\bavailable colors include|available colours include\b/.test(blob)
  ) {
    return "validNegativeResult";
  }

  if (tool === "vehicle_feature_lookup") {
    return "likelyFeatureReadModelGap";
  }

  if (tool === "vehicle_spec_attribute_lookup") {
    return "likelySpecReadModelGap";
  }

  if (tool === "vehicle_score_insight") {
    return "likelyScoreDataGap";
  }

  return "needsManualReview";
};

const bucketNoDataAnswers = (noDataAnswers = []) => {
  const buckets = {
    expectedUnsupportedCity: [],
    expectedPendingModule: [],
    validNegativeResult: [],
    likelyComparisonEvidenceGap: [],
    likelyFeatureReadModelGap: [],
    likelySpecReadModelGap: [],
    likelyScoreDataGap: [],
    needsManualReview: [],
  };

  noDataAnswers.forEach((entry = {}) => {
    const bucket = classifyNoDataAnswer(entry);
    buckets[bucket].push(entry);
  });

  return buckets;
};

const printBucket = (name = "", rows = []) => {
  console.log(`\n${name} (${rows.length})`);
  rows.map(compactRow).forEach((row) => {
    console.log(
      `- ${row.id} group=${row.groupKey} tool=${row.tool} message=${JSON.stringify(row.message)} title=${JSON.stringify(row.title)} answer=${JSON.stringify(row.answerPreview)}`,
    );
  });
};

const failWithMissingLogHelp = () => {
  console.error("No full deep-audit log path was provided and no /tmp/aci_full_185*.log file was found.");
  console.error("Run:");
  console.error(`  ${RUN_FULL_AUDIT_COMMAND}`);
  process.exitCode = 1;
};

const main = () => {
  const sourcePath = process.argv[2] ? path.resolve(process.argv[2]) : findLatestAuditLog();

  if (!sourcePath) {
    failWithMissingLogHelp();
    return;
  }

  if (!fs.existsSync(sourcePath)) {
    throw new Error(`Audit log not found: ${sourcePath}`);
  }

  const summary = parseDeepAuditSummary(sourcePath);
  const noDataAnswers = Array.isArray(summary.noDataAnswers) ? summary.noDataAnswers : [];
  const buckets = bucketNoDataAnswers(noDataAnswers);
  const bucketCounts = Object.fromEntries(
    Object.entries(buckets).map(([key, rows]) => [key, rows.length]),
  );
  const output = {
    sourceLog: sourcePath,
    extractedAt: new Date().toISOString(),
    auditSummary: {
      ok: Boolean(summary.ok),
      totalCases: Number(summary.totalCases || 0),
      selected: Number(summary.selected || 0),
      executed: Number(summary.executed || 0),
      passed: Number(summary.passed || 0),
      failed: Number(summary.failed || 0),
      hardFailureCount: Number(summary.hardFailureCount || 0),
      softFailureCount: Number(summary.softFailureCount || 0),
      noDataAnswerCount: Number(summary.noDataAnswerCount || noDataAnswers.length || 0),
    },
    bucketCounts,
    buckets,
    noDataAnswers,
  };
  const outputPath = path.join(TMP_DIR, `aci_no_data_answers_${timestamp()}.json`);

  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));

  console.log(`source log: ${sourcePath}`);
  console.log(`output file: ${outputPath}`);
  console.log(
    `summary: executed=${output.auditSummary.executed} passed=${output.auditSummary.passed} failed=${output.auditSummary.failed} hard=${output.auditSummary.hardFailureCount} soft=${output.auditSummary.softFailureCount} noData=${noDataAnswers.length}`,
  );
  console.log(`bucket counts: ${JSON.stringify(bucketCounts)}`);

  Object.entries(buckets).forEach(([name, rows]) => printBucket(name, rows));
};

try {
  main();
} catch (error) {
  console.error(error?.message || String(error));
  process.exitCode = 1;
}
