import fs from "fs";

const BASE = "/tmp/aci_feature_kb_v2";
const conflicts = JSON.parse(
  fs.readFileSync(`${BASE}/feature_conflicts_sample.json`, "utf8"),
);

const normalize = (v = "") =>
  String(v ?? "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();

const classifyConflict = (row) => {
  const all = [];

  for (const c of row.conflicts || []) {
    if (c.existing) all.push(c.existing);
    if (c.incoming) all.push(c.incoming);
  }

  const values = [...new Set(all.map((x) => normalize(x.value)))];
  const sections = [...new Set(all.map((x) => normalize(x.rawSection)))];
  const names = [...new Set(all.map((x) => normalize(x.rawFeatureName)))];

  const hasOnlySummarySection = sections.every((s) =>
    ["key features", "key specifications", ""].includes(s),
  );

  const hasDetailedAndSummary =
    sections.some((s) => ["key features", "key specifications"].includes(s)) &&
    sections.some((s) => !["key features", "key specifications", ""].includes(s));

  const yesNoOnly = values.every((v) =>
    ["yes", "no", "not available", ""].includes(v),
  );

  const numericOnly = values.every((v) => /^[-+]?\d+(\.\d+)?(\s*[a-z/%.-]+)?$/.test(v));

  if (hasDetailedAndSummary) return "summary_vs_detailed_duplicate";
  if (hasOnlySummarySection) return "summary_only_conflict";
  if (yesNoOnly) return "availability_conflict";
  if (numericOnly) return "numeric_value_conflict";
  if (sections.length > 1) return "multi_section_value_conflict";
  if (names.length > 1) return "multi_name_value_conflict";

  return "same_feature_value_conflict";
};

const byKey = {};
const byType = {};
const examples = {};

for (const row of conflicts) {
  const type = classifyConflict(row);
  byType[type] = (byType[type] || 0) + 1;

  byKey[row.canonicalKey] ||= {
    count: 0,
    types: {},
    examples: [],
  };

  byKey[row.canonicalKey].count += 1;
  byKey[row.canonicalKey].types[type] = (byKey[row.canonicalKey].types[type] || 0) + 1;

  if (byKey[row.canonicalKey].examples.length < 5) {
    byKey[row.canonicalKey].examples.push({
      model: row.model,
      variant: row.variant,
      displayName: row.displayName,
      type,
      conflicts: row.conflicts,
    });
  }

  examples[type] ||= [];
  if (examples[type].length < 8) {
    examples[type].push({
      canonicalKey: row.canonicalKey,
      displayName: row.displayName,
      model: row.model,
      variant: row.variant,
      conflicts: row.conflicts,
    });
  }
}

const topKeys = Object.entries(byKey)
  .sort((a, b) => b[1].count - a[1].count)
  .slice(0, 30)
  .map(([canonicalKey, data]) => ({
    canonicalKey,
    count: data.count,
    types: data.types,
    examples: data.examples,
  }));

console.log("=== CONFLICT SUMMARY ===");
console.log({
  totalConflictDocs: conflicts.length,
  byType,
});

console.log("\n=== TOP CONFLICT KEYS ===");
console.dir(topKeys, { depth: 8 });

console.log("\n=== EXAMPLES BY TYPE ===");
console.dir(examples, { depth: 8 });

fs.writeFileSync(
  `${BASE}/conflict_deep_audit.json`,
  JSON.stringify(
    {
      totalConflictDocs: conflicts.length,
      byType,
      topKeys,
      examples,
    },
    null,
    2,
  ),
);

console.log(`\nWritten: ${BASE}/conflict_deep_audit.json`);
