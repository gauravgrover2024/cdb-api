#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const JSON_START = "__ACI_DEEP_AUDIT_JSON_START__";
const JSON_END = "__ACI_DEEP_AUDIT_JSON_END__";
const TMP_DIR = "/tmp";

const STRICT = String(process.env.ACI_NO_DATA_BASELINE_STRICT || "1") === "1";

const EXPECTED_NO_DATA = {
  "A22-seltos-price-bangalore": {
    bucket: "expectedUnsupportedCity",
    requiredText: [/bangalore/i, /new delhi/i, /noida/i, /gurgaon/i],
  },
  "A24-baleno-price-jaipur": {
    bucket: "expectedUnsupportedCity",
    requiredText: [/jaipur/i, /new delhi/i, /noida/i, /gurgaon/i],
  },
  "F105-same-in-mumbai": {
    bucket: "expectedUnsupportedCity",
    requiredText: [/mumbai/i, /new delhi/i, /noida/i, /gurgaon/i],
  },
  "J169-mumbai-me-price-batao": {
    bucket: "expectedUnsupportedCity",
    requiredText: [/mumbai/i, /new delhi/i, /noida/i, /gurgaon/i],
  },
  "C64-dual-tone-colors-in-seltos": {
    bucket: "validNegativeResult",
    requiredText: [/could not find an exact/i, /available colors include/i],
  },
  "G122-how-good-is-scorpio-n-overall": {
    bucket: "expectedKnownScoreDataGap",
    requiredText: [/could not find enough diagnostic score data/i, /diagnostic/i],
  },
  "I157-should-i-wait-for-discount": {
    bucket: "expectedPendingModule",
    requiredText: [/discount|offer/i, /not invent|verified|not available/i],
  },
  "I158-are-there-offers-on-creta": {
    bucket: "expectedPendingModule",
    requiredText: [/offer/i, /not invent|verified|not available/i],
  },
  "I159-service-cost-of-creta": {
    bucket: "expectedPendingModule",
    requiredText: [/service/i, /not invent|verified|not available/i],
  },
  "I160-insurance-price-for-creta": {
    bucket: "expectedPendingModule",
    requiredText: [/insurance/i, /not invent|verified|not available/i],
  },
};

const BLOCKED_BUCKET_PATTERNS = [
  "likelyFeatureReadModelGap",
  "likelySpecReadModelGap",
  "likelyComparisonEvidenceGap",
  "needsManualReview",
];

const FULL_AUDIT_LOG_PATTERNS = [
  /^aci_full_185.*\.log$/,
  /^aci_full_185_after_green.*\.log$/,
  /^aci_full_185_no_data_review.*\.log$/,
];

const text = (value = "") => String(value || "");

const compactRow = (entry = {}) => ({
  id: entry.id || "",
  groupKey: entry.groupKey || text(entry.group).slice(0, 1).toUpperCase() || "",
  message: entry.message || "",
  tool: entry.tool || entry.response?.tool || "",
  title: entry.title || entry.response?.title || "",
  answerPreview: entry.answerPreview || entry.response?.answerPreview || "",
});

const isCompleteDeepAuditLog = (filePath = "") => {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    const start = raw.lastIndexOf(JSON_START);
    if (start < 0) return false;
    return raw.indexOf(JSON_END, start + JSON_START.length) >= 0;
  } catch {
    return false;
  }
};

const findLatestAuditLog = () => {
  let entries = [];

  try {
    entries = fs.readdirSync(TMP_DIR, { withFileTypes: true })
      .filter((entry) => entry.isFile())
      .filter((entry) => FULL_AUDIT_LOG_PATTERNS.some((pattern) => pattern.test(entry.name)))
      .map((entry) => {
        const filePath = path.join(TMP_DIR, entry.name);
        const stat = fs.statSync(filePath);
        return { path: filePath, mtimeMs: stat.mtimeMs };
      })
      .filter((entry) => isCompleteDeepAuditLog(entry.path));
  } catch {
    return "";
  }

  entries.sort((left, right) => right.mtimeMs - left.mtimeMs || right.path.localeCompare(left.path));
  return entries[0]?.path || "";
};

const parseDeepAuditSummary = (sourcePath = "") => {
  const raw = fs.readFileSync(sourcePath, "utf8");
  const start = raw.lastIndexOf(JSON_START);
  if (start < 0) throw new Error(`Could not find ${JSON_START} in ${sourcePath}`);

  const jsonStart = start + JSON_START.length;
  const end = raw.indexOf(JSON_END, jsonStart);
  if (end < 0) throw new Error(`Could not find ${JSON_END} after latest JSON start in ${sourcePath}`);

  return JSON.parse(raw.slice(jsonStart, end).trim());
};

const classifyNoDataAnswer = (entry = {}) => {
  const row = compactRow(entry);
  const message = row.message.toLowerCase();
  const answer = row.answerPreview.toLowerCase();
  const title = row.title.toLowerCase();
  const tool = row.tool.toLowerCase();
  const blob = `${message} ${answer} ${title} ${tool}`;

  if (
    /\b(mumbai|bangalore|bengaluru|jaipur|pune|chennai|hyderabad|kolkata|ahmedabad|chandigarh|faridabad|ghaziabad)\b/.test(blob) &&
    /\b(price|pricing|on road|on-road|ex showroom|ex-showroom|pricelist)\b/.test(blob)
  ) {
    return "expectedUnsupportedCity";
  }

  if (
    /\b(offer|offers|discount|deal|benefit|service cost|maintenance|insurance|bank|finance scheme|waiting period|stock|availability|delivery|service center|service centre)\b/.test(blob) ||
    /\bnot available yet|not available in this system yet|pending module\b/.test(blob)
  ) {
    return "expectedPendingModule";
  }

  if (
    tool === "vehicle_colors" &&
    /\bexact\b.*\b(?:color|colour|shade)\b.*\bnot found|could not find an exact\b.*\b(?:color|colour|shade)\b/.test(blob) &&
    /\bavailable colors include|available colours include\b/.test(blob)
  ) {
    return "validNegativeResult";
  }

  if (tool === "vehicle_score_insight") return "expectedKnownScoreDataGap";
  if (tool === "vehicle_compare") return "likelyComparisonEvidenceGap";
  if (tool === "vehicle_feature_lookup") return "likelyFeatureReadModelGap";
  if (tool === "vehicle_spec_attribute_lookup") return "likelySpecReadModelGap";

  return "needsManualReview";
};

const validateExpectedRow = (entry = {}) => {
  const row = compactRow(entry);
  const expected = EXPECTED_NO_DATA[row.id];
  const failures = [];

  if (!expected) {
    failures.push("unexpected_no_data_id");
    return { ...row, expectedBucket: "", actualBucket: classifyNoDataAnswer(entry), failures };
  }

  const actualBucket = classifyNoDataAnswer(entry);
  if (actualBucket !== expected.bucket) {
    failures.push(`bucket_mismatch_expected_${expected.bucket}_got_${actualBucket}`);
  }

  const blob = `${row.message} ${row.title} ${row.answerPreview} ${row.tool}`;
  for (const pattern of expected.requiredText || []) {
    if (!pattern.test(blob)) {
      failures.push(`missing_required_text_${String(pattern)}`);
    }
  }

  return {
    ...row,
    expectedBucket: expected.bucket,
    actualBucket,
    failures,
  };
};

const main = () => {
  const sourcePath = process.argv[2] ? path.resolve(process.argv[2]) : findLatestAuditLog();
  if (!sourcePath) {
    throw new Error("No full deep-audit log path was provided and no /tmp/aci_full_185*.log file was found.");
  }

  const summary = parseDeepAuditSummary(sourcePath);
  const noDataAnswers = Array.isArray(summary.noDataAnswers) ? summary.noDataAnswers : [];
  const expectedIds = Object.keys(EXPECTED_NO_DATA).sort();
  const actualIds = noDataAnswers.map((entry) => entry.id).sort();

  const rows = noDataAnswers.map(validateExpectedRow);
  const rowFailures = rows.filter((row) => row.failures.length > 0);

  const missingExpectedIds = expectedIds.filter((id) => !actualIds.includes(id));
  const unexpectedIds = actualIds.filter((id) => !EXPECTED_NO_DATA[id]);

  const bucketCounts = {};
  for (const entry of noDataAnswers) {
    const bucket = classifyNoDataAnswer(entry);
    bucketCounts[bucket] = (bucketCounts[bucket] || 0) + 1;
  }

  const blockedBucketHits = Object.entries(bucketCounts)
    .filter(([bucket, count]) => BLOCKED_BUCKET_PATTERNS.includes(bucket) && count > 0)
    .map(([bucket]) => bucket);

  const failures = [
    ...rowFailures.map((row) => ({ id: row.id, failures: row.failures })),
    ...missingExpectedIds.map((id) => ({ id, failures: ["missing_expected_no_data_id"] })),
    ...unexpectedIds.map((id) => ({ id, failures: ["unexpected_no_data_id"] })),
    ...blockedBucketHits.map((bucket) => ({ id: bucket, failures: ["blocked_no_data_bucket_present"] })),
  ];

  if (!summary.ok || Number(summary.failed || 0) !== 0 || Number(summary.hardFailureCount || 0) !== 0 || Number(summary.softFailureCount || 0) !== 0) {
    failures.push({ id: "deep_audit_summary", failures: ["deep_audit_not_clean"] });
  }

  const output = {
    suite: "ACI No-Data Baseline Audit v1",
    ok: failures.length === 0,
    strict: STRICT,
    sourceLog: sourcePath,
    auditSummary: {
      ok: Boolean(summary.ok),
      totalCases: Number(summary.totalCases || 0),
      executed: Number(summary.executed || 0),
      passed: Number(summary.passed || 0),
      failed: Number(summary.failed || 0),
      hardFailureCount: Number(summary.hardFailureCount || 0),
      softFailureCount: Number(summary.softFailureCount || 0),
      auditWarningCount: Number(summary.auditWarningCount || 0),
      noDataAnswerCount: Number(summary.noDataAnswerCount || noDataAnswers.length || 0),
    },
    expectedCount: expectedIds.length,
    actualCount: actualIds.length,
    bucketCounts,
    failed: failures.length,
    failedIds: failures.map((failure) => failure.id),
    failures,
    rows,
  };

  console.log(JSON.stringify(output, null, 2));
  return !STRICT || failures.length === 0;
};

let ok = false;
try {
  ok = main();
} catch (error) {
  console.error(error?.stack || error?.message || String(error));
  ok = false;
}

process.exit(ok ? 0 : 1);
