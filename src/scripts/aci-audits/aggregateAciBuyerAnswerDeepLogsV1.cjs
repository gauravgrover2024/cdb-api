#!/usr/bin/env node

const fs = require("fs");

const JSON_START = "__ACI_DEEP_AUDIT_JSON_START__";
const JSON_END = "__ACI_DEEP_AUDIT_JSON_END__";

const readInput = () => {
  const files = process.argv.slice(2);
  if (files.length) {
    return files.map((file) => ({
      source: file,
      text: fs.readFileSync(file, "utf8"),
    }));
  }

  if (!process.stdin.isTTY) {
    return [{
      source: "stdin",
      text: fs.readFileSync(0, "utf8"),
    }];
  }

  return [];
};

const extractJsonBlocks = (source = "", raw = "") => {
  const blocks = [];
  let offset = 0;

  while (offset < raw.length) {
    const start = raw.indexOf(JSON_START, offset);
    if (start < 0) break;

    const jsonStart = start + JSON_START.length;
    const end = raw.indexOf(JSON_END, jsonStart);
    if (end < 0) {
      blocks.push({
        source,
        ok: false,
        error: `missing ${JSON_END}`,
      });
      break;
    }

    const jsonText = raw.slice(jsonStart, end).trim();
    try {
      blocks.push({
        source,
        ok: true,
        summary: JSON.parse(jsonText),
      });
    } catch (error) {
      blocks.push({
        source,
        ok: false,
        error: error?.message || String(error),
        jsonPreview: jsonText.slice(0, 500),
      });
    }

    offset = end + JSON_END.length;
  }

  return blocks;
};

const sum = (items = [], key = "") =>
  items.reduce((total, item) => total + (Number(item?.[key] || 0) || 0), 0);

const main = () => {
  const inputs = readInput();
  const blocks = inputs.flatMap((input) => extractJsonBlocks(input.source, input.text));
  const parsed = blocks.filter((block) => block.ok).map((block) => block.summary);
  const parseErrors = blocks.filter((block) => !block.ok);
  const allResults = parsed.flatMap((summary) => Array.isArray(summary.results) ? summary.results : []);
  const hardFailures = parsed.flatMap((summary) => Array.isArray(summary.hardFailures) ? summary.hardFailures : []);
  const softFailures = parsed.flatMap((summary) => Array.isArray(summary.softFailures) ? summary.softFailures : []);
  const noDataAnswers = parsed.flatMap((summary) => Array.isArray(summary.noDataAnswers) ? summary.noDataAnswers : []);
  const auditWarnings = [
    ...parseErrors,
    ...parsed.flatMap((summary) => Array.isArray(summary.auditWarnings) ? summary.auditWarnings : []),
  ];

  const byGroup = {};
  allResults.forEach((result = {}) => {
    const key = result.groupKey || String(result.group || "").slice(0, 1).toUpperCase() || "?";
    byGroup[key] ||= { group: result.group || "", total: 0, passed: 0, failed: 0, hard: 0, soft: 0, warnings: 0 };
    byGroup[key].total += 1;
    if (result.pass) byGroup[key].passed += 1;
    else byGroup[key].failed += 1;
    if (Array.isArray(result.hardFailures) && result.hardFailures.length) byGroup[key].hard += 1;
    if (Array.isArray(result.softFailures) && result.softFailures.length) byGroup[key].soft += 1;
    if (Array.isArray(result.auditWarnings) && result.auditWarnings.length) byGroup[key].warnings += 1;
  });

  const summary = {
    suite: "ACI Buyer Answer Deep Audit Aggregate v1",
    ok: parseErrors.length === 0 && hardFailures.length === 0 && softFailures.length === 0,
    sources: inputs.map((input) => input.source),
    parsedBlocks: parsed.length,
    parseErrorCount: parseErrors.length,
    totalCases: parsed.length ? Math.max(...parsed.map((item) => Number(item.totalCases || 0))) : 0,
    selected: sum(parsed, "selected"),
    executed: sum(parsed, "executed"),
    passed: sum(parsed, "passed"),
    failed: sum(parsed, "failed"),
    failedIds: allResults.filter((result) => !result.pass).map((result) => result.id),
    hardFailureCount: hardFailures.length,
    softFailureCount: softFailures.length,
    noDataAnswerCount: noDataAnswers.length,
    auditWarningCount: auditWarnings.length,
    hardFailures,
    softFailures,
    noDataAnswers,
    auditWarnings,
    byGroup,
  };

  console.log(JSON.stringify(summary, null, 2));
  process.exitCode = summary.ok ? 0 : 1;
};

main();
