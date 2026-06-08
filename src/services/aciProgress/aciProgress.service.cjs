const fs = require("fs");
const path = require("path");
const { ACI_PROGRESS_MODULES } = require("./aciProgress.registry.cjs");

const REPORT_DIR = path.resolve(process.cwd(), "reports/aci");
const REGISTRY_SOURCE = "live_registry";

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

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

const normalizeText = (value = "") => String(value || "").trim();

const normalizeProgressModule = (module) => {
  if (!module || typeof module !== "object" || typeof module.id !== "string") {
    return null;
  }

  const items = Array.isArray(module.items)
    ? module.items
    : Array.isArray(module.milestones)
      ? module.milestones
      : null;

  if (!items) return null;

  return {
    ...module,
    group: module.group || module.area || "Roadmap",
    items
  };
};

const getRawProgressModules = () =>
  Array.isArray(ACI_PROGRESS_MODULES) ? ACI_PROGRESS_MODULES : [];

const getSafeProgressModules = () =>
  getRawProgressModules()
    .map(normalizeProgressModule)
    .filter(Boolean);

function validateProgressRegistry({ rawModules = [], modules = [] } = {}) {
  const errors = [];
  const warnings = [];

  if (!Array.isArray(rawModules)) {
    errors.push("ACI_PROGRESS_MODULES is not an array");
  }

  if (!modules.length) {
    errors.push("No valid progress modules available from registry");
  }

  const invalidRawModuleCount = rawModules.length - modules.length;
  if (invalidRawModuleCount > 0) {
    errors.push(`${invalidRawModuleCount} registry module(s) were invalid and would have been silently filtered`);
  }

  const moduleIds = modules.map((module) => normalizeText(module.id)).filter(Boolean);
  const duplicateModuleIds = moduleIds.filter((id, index) => moduleIds.indexOf(id) !== index);
  const uniqueDuplicateModuleIds = [...new Set(duplicateModuleIds)];

  if (uniqueDuplicateModuleIds.length) {
    errors.push(`Duplicate module IDs: ${uniqueDuplicateModuleIds.join(", ")}`);
  }

  const missingRequiredModuleIds = REQUIRED_MODULE_IDS.filter((id) => !moduleIds.includes(id));
  if (missingRequiredModuleIds.length) {
    errors.push(`Missing required module IDs: ${missingRequiredModuleIds.join(", ")}`);
  }

  const moduleIssues = [];

  modules.forEach((module) => {
    const moduleLabel = module.id || module.title || "unknown_module";

    if (!normalizeText(module.id)) moduleIssues.push(`${moduleLabel}: missing id`);
    if (!normalizeText(module.title)) moduleIssues.push(`${moduleLabel}: missing title`);
    if (!normalizeText(module.status)) moduleIssues.push(`${moduleLabel}: missing status`);
    if (module.status && !VALID_STATUSES.has(module.status)) {
      moduleIssues.push(`${moduleLabel}: invalid status ${module.status}`);
    }

    if (!Array.isArray(module.items) || !module.items.length) {
      moduleIssues.push(`${moduleLabel}: missing/empty items`);
      return;
    }

    const itemKeys = [];

    module.items.forEach((item, index) => {
      const itemLabel = item?.key || item?.name || `${moduleLabel}.items[${index}]`;

      if (!item || typeof item !== "object") {
        moduleIssues.push(`${moduleLabel}.items[${index}]: invalid item`);
        return;
      }

      if (!normalizeText(item.key)) moduleIssues.push(`${moduleLabel}.${itemLabel}: missing key`);
      if (!normalizeText(item.name || item.title)) moduleIssues.push(`${moduleLabel}.${itemLabel}: missing name/title`);
      if (!normalizeText(item.status)) moduleIssues.push(`${moduleLabel}.${itemLabel}: missing status`);
      if (item.status && !VALID_STATUSES.has(item.status)) {
        moduleIssues.push(`${moduleLabel}.${itemLabel}: invalid status ${item.status}`);
      }

      if (normalizeText(item.key)) itemKeys.push(item.key);
    });

    const duplicateItemKeys = itemKeys.filter((key, index) => itemKeys.indexOf(key) !== index);
    const uniqueDuplicateItemKeys = [...new Set(duplicateItemKeys)];

    if (uniqueDuplicateItemKeys.length) {
      moduleIssues.push(`${moduleLabel}: duplicate item keys ${uniqueDuplicateItemKeys.join(", ")}`);
    }
  });

  if (moduleIssues.length) {
    errors.push(...moduleIssues);
  }

  return {
    ok: errors.length === 0,
    source: REGISTRY_SOURCE,
    fallbackUsed: false,
    moduleCount: modules.length,
    rawModuleCount: rawModules.length,
    requiredModuleIds: REQUIRED_MODULE_IDS,
    missingRequiredModuleIds,
    duplicateModuleIds: uniqueDuplicateModuleIds,
    invalidRawModuleCount,
    errorCount: errors.length,
    warningCount: warnings.length,
    errors,
    warnings
  };
}

function safeReadJson(filePath) {
  try {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    return {
      __readError: error.message
    };
  }
}

function listReports() {
  if (!fs.existsSync(REPORT_DIR)) return [];

  return fs
    .readdirSync(REPORT_DIR)
    .filter((fileName) => fileName.endsWith(".json"))
    .map((fileName) => {
      const filePath = path.join(REPORT_DIR, fileName);
      const stat = fs.statSync(filePath);
      return {
        fileName,
        filePath,
        modifiedAt: stat.mtime.toISOString(),
        json: safeReadJson(filePath)
      };
    })
    .sort((a, b) => b.modifiedAt.localeCompare(a.modifiedAt));
}

function reportLooksPassed(report) {
  if (!report || report.__readError) return false;

  if (report.success === true || report.ok === true || report.pass === true) return true;
  if (report.failed === 0 && Number(report.passed || 0) > 0) return true;
  if (report.failedCount === 0 && Number(report.passedCount || 0) > 0) return true;
  if (report.exitCode === 0) return true;
  if (report.foundationExitCode === 0 && report.contextSwitchExitCode === 0) return true;

  if (Array.isArray(report.results) && report.results.length) {
    return report.results.every((item) => item.pass === true || item.passed === true);
  }

  return false;
}

function reportHasName(reports, patterns) {
  return reports.find((report) => {
    const name = report.fileName.toLowerCase();
    return patterns.some((pattern) => name.includes(pattern));
  });
}

function setItemStatus(modules, moduleId, itemKey, status) {
  const module = modules.find((entry) => entry.id === moduleId);
  if (!module || !Array.isArray(module.items)) return;

  const item = module.items.find((entry) => entry.key === itemKey || entry.name === itemKey);
  if (item) item.status = status;
}

function recomputeModuleStatus(module) {
  if (!Array.isArray(module.items) || !module.items.length) return module.status;

  const scoreMap = {
    ready: 100,
    mostly_ready: 80,
    partial: 50,
    in_progress: 50,
    planned: 25,
    deferred: 10,
    pending: 0,
    blocked: 0
  };

  const avg =
    module.items.reduce((sum, item) => sum + (scoreMap[item.status] || 0), 0) /
    module.items.length;

  if (avg >= 92) return "ready";
  if (avg >= 70) return "mostly_ready";
  if (avg >= 40) return "partial";
  if (avg >= 15) return "planned";
  return "pending";
}

function applyReportSignals(modules, reports) {
  const safety = reportHasName(reports, ["safety"]);
  const understanding = reportHasName(reports, ["understanding"]);
  const router = reportHasName(reports, ["router"]);
  const answerQuality = reportHasName(reports, ["answer-quality", "answer_quality"]);
  const prewarm = reportHasName(reports, ["prewarm"]);
  const contextSwitch = reportHasName(reports, ["context-switch", "context_switch"]);
  const multiFeature = reportHasName(reports, ["multi-feature", "multi_feature"]);
  const featureComparison = reportHasName(reports, ["feature-comparison", "feature_comparison"]);

  if (safety && reportLooksPassed(safety.json)) {
    setItemStatus(modules, "testing-evals", "foundation_safety", "ready");
  }

  if (understanding && reportLooksPassed(understanding.json)) {
    setItemStatus(modules, "testing-evals", "understanding_workers", "ready");
    setItemStatus(modules, "intelligence-core", "deterministic_parser", "ready");
  }

  if (router && reportLooksPassed(router.json)) {
    setItemStatus(modules, "intelligence-core", "hybrid_router", "ready");
  }

  if (answerQuality && reportLooksPassed(answerQuality.json)) {
    setItemStatus(modules, "testing-evals", "answer_quality_smoke", "ready");
    setItemStatus(modules, "intelligence-core", "answer_composer", "partial");
  }

  if (prewarm && reportLooksPassed(prewarm.json)) {
    setItemStatus(modules, "performance-scale", "candidate_prewarm", "ready");
    setItemStatus(modules, "performance-scale", "startup_prewarm", "ready");
    setItemStatus(modules, "performance-scale", "warm_path_speed", "mostly_ready");
  }

  if (contextSwitch && reportLooksPassed(contextSwitch.json)) {
    setItemStatus(modules, "testing-evals", "context_switch_audit", "ready");
    setItemStatus(modules, "chat-concierge", "explicit_context_switching", "mostly_ready");
  }

  if (multiFeature && reportLooksPassed(multiFeature.json)) {
    setItemStatus(modules, "feature-answers", "same_car_multi_feature", "ready");
    setItemStatus(modules, "feature-answers", "variant_multi_feature", "ready");
    setItemStatus(modules, "testing-evals", "multi_feature_audits", "ready");
  }

  if (featureComparison && reportLooksPassed(featureComparison.json)) {
    setItemStatus(modules, "feature-answers", "two_car_feature_comparison", "ready");
    setItemStatus(modules, "comparison", "feature_specific_comparison", "mostly_ready");
  }

  modules
    .filter((module) => module && Array.isArray(module.items))
    .forEach((module) => {
      module.status = recomputeModuleStatus(module);
    });
}

function getAciProgressSnapshot() {
  const rawModules = clone(getRawProgressModules());
  const modules = clone(getSafeProgressModules());
  const reports = listReports();

  applyReportSignals(modules, reports);

  const registryIntegrity = validateProgressRegistry({
    rawModules,
    modules
  });

  const generatedAt = new Date().toISOString();

  return {
    ok: registryIntegrity.ok,
    source: REGISTRY_SOURCE,
    registrySource: REGISTRY_SOURCE,
    fallbackUsed: false,
    generatedAt,
    lastUpdated: new Date().toLocaleString("en-IN", {
      timeZone: "Asia/Kolkata",
      hour12: false
    }),
    modules,
    meta: {
      source: "live registry + latest reports/aci JSON reports",
      registrySource: REGISTRY_SOURCE,
      fallbackUsed: false,
      reportDir: REPORT_DIR,
      reportsFound: reports.length,
      registryIntegrity,
      latestReports: reports.slice(0, 8).map((report) => ({
        fileName: report.fileName,
        modifiedAt: report.modifiedAt,
        passed: reportLooksPassed(report.json),
        readError: report.json && report.json.__readError ? report.json.__readError : null
      }))
    }
  };
}

module.exports = {
  getAciProgressSnapshot,
  validateProgressRegistry
};
