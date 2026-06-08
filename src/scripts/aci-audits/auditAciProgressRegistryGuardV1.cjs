#!/usr/bin/env node

const assert = require("assert");

const REQUIRED_MODULE_IDS = [
  "intelligence-core",
  "pricing",
  "feature-answers",
  "comparison",
  "testing-evals",
  "aci-assist-premortem-guardrails",
  "local-gemma-language-intent-layer",
  "backend_completion_timeline"
];

const VALID_STATUSES = new Set([
  "ready",
  "mostly_ready",
  "partial",
  "planned",
  "deferred",
  "pending",
  "blocked",
  "in_progress"
]);

const text = (value = "") => String(value || "").trim();

function asModules(snapshot = {}) {
  return snapshot.modules || snapshot.data?.modules || snapshot.progressModules || snapshot.items || [];
}

function assertNoDuplicates(values = [], label = "values") {
  const duplicates = values.filter((value, index) => values.indexOf(value) !== index);
  assert.deepStrictEqual([...new Set(duplicates)], [], `duplicate ${label}: ${[...new Set(duplicates)].join(", ")}`);
}

function validateSnapshot(snapshot = {}) {
  assert(snapshot && typeof snapshot === "object", "snapshot must be an object");
  assert.strictEqual(snapshot.ok, true, "snapshot.ok must be true");
  assert.strictEqual(snapshot.source, "live_registry", "snapshot.source must be live_registry");
  assert.strictEqual(snapshot.registrySource, "live_registry", "snapshot.registrySource must be live_registry");
  assert.strictEqual(snapshot.fallbackUsed, false, "snapshot.fallbackUsed must be false");
  assert(text(snapshot.generatedAt), "snapshot.generatedAt is required");

  const modules = asModules(snapshot);
  assert(Array.isArray(modules), "modules must be an array");
  assert(modules.length >= REQUIRED_MODULE_IDS.length, `expected at least ${REQUIRED_MODULE_IDS.length} modules, got ${modules.length}`);

  const moduleIds = modules.map((module) => text(module.id)).filter(Boolean);
  assertNoDuplicates(moduleIds, "module ids");

  for (const requiredId of REQUIRED_MODULE_IDS) {
    assert(moduleIds.includes(requiredId), `missing required module ${requiredId}`);
  }

  modules.forEach((module, moduleIndex) => {
    assert(text(module.id), `module[${moduleIndex}] missing id`);
    assert(text(module.title), `${module.id || moduleIndex} missing title`);
    assert(text(module.status), `${module.id || moduleIndex} missing status`);
    assert(VALID_STATUSES.has(module.status), `${module.id} invalid status ${module.status}`);

    assert(Array.isArray(module.items), `${module.id} items must be an array`);
    assert(module.items.length > 0, `${module.id} items must not be empty`);

    const itemKeys = [];

    module.items.forEach((item, itemIndex) => {
      assert(item && typeof item === "object", `${module.id}.items[${itemIndex}] must be an object`);
      assert(text(item.key), `${module.id}.items[${itemIndex}] missing key`);
      assert(text(item.name || item.title), `${module.id}.${item.key || itemIndex} missing name/title`);
      assert(text(item.status), `${module.id}.${item.key || itemIndex} missing status`);
      assert(VALID_STATUSES.has(item.status), `${module.id}.${item.key || itemIndex} invalid status ${item.status}`);
      itemKeys.push(item.key);
    });

    assertNoDuplicates(itemKeys, `${module.id} item keys`);
  });

  const integrity = snapshot.meta?.registryIntegrity;
  assert(integrity && typeof integrity === "object", "meta.registryIntegrity is required");
  assert.strictEqual(integrity.ok, true, "meta.registryIntegrity.ok must be true");
  assert.strictEqual(integrity.source, "live_registry", "meta.registryIntegrity.source must be live_registry");
  assert.strictEqual(integrity.fallbackUsed, false, "meta.registryIntegrity.fallbackUsed must be false");
  assert.strictEqual(integrity.errorCount, 0, `registry integrity errors found: ${(integrity.errors || []).join("; ")}`);

  const premortem = modules.find((module) => module.id === "aci-assist-premortem-guardrails");
  assert(premortem, "pre-mortem guardrail module missing");

  const progressGuard = premortem.items.find((item) => item.key === "progress_registry_guard");
  assert(progressGuard, "progress_registry_guard item missing");
  assert(
    ["planned", "in_progress", "ready"].includes(progressGuard.status),
    `unexpected progress_registry_guard status ${progressGuard.status}`
  );
}

async function main() {
  const { getAciProgressSnapshot } = require("../../services/aciProgress/aciProgress.service.cjs");

  const startedAt = Date.now();
  const snapshot = getAciProgressSnapshot();

  const failures = [];
  try {
    validateSnapshot(snapshot);
  } catch (error) {
    failures.push(error?.message || String(error));
  }

  const modules = asModules(snapshot);

  const output = {
    suite: "ACI Progress Registry Guard v1",
    ok: failures.length === 0,
    backendOnly: true,
    frontendEvaluated: false,
    source: snapshot.source || "",
    registrySource: snapshot.registrySource || "",
    fallbackUsed: snapshot.fallbackUsed,
    modulesCount: modules.length,
    requiredModuleIds: REQUIRED_MODULE_IDS,
    registryIntegrity: snapshot.meta?.registryIntegrity || null,
    failed: failures.length,
    failedIds: failures.length ? ["progress_registry_integrity"] : [],
    failures,
    durationMs: Date.now() - startedAt
  };

  console.log(JSON.stringify(output, null, 2));
  process.exit(output.ok ? 0 : 1);
}

main().catch((error) => {
  console.error(error?.stack || error?.message || String(error));
  process.exit(1);
});
