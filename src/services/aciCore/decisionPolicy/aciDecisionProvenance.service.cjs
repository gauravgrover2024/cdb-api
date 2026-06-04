const {
  SOURCE_CLASSES,
} = require('./aciDecisionPolicy.constants.cjs');

const DEFAULT_REQUIRED_PROVENANCE_FIELDS = Object.freeze([
  'buildVersion',
  'builtAt',
  'sourceClass',
]);

const VALID_SOURCE_CLASSES = new Set(Object.values(SOURCE_CLASSES));

const asObject = (value) => (
  value && typeof value === 'object' && !Array.isArray(value) ? value : {}
);

function parseDateMs(value) {
  if (!value || typeof value !== 'string') return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

function computeStalenessDays({ builtAt, now = new Date() } = {}) {
  const builtAtMs = parseDateMs(builtAt);
  const nowMs = now instanceof Date ? now.getTime() : parseDateMs(String(now));

  if (!Number.isFinite(builtAtMs) || !Number.isFinite(nowMs)) {
    return null;
  }

  return Math.max(0, Math.floor((nowMs - builtAtMs) / 86400000));
}

function getMissingProvenanceFields(provenance = {}, requiredFields = DEFAULT_REQUIRED_PROVENANCE_FIELDS) {
  const value = asObject(provenance);

  return requiredFields.filter((field) => {
    const fieldValue = value[field];
    return fieldValue === undefined || fieldValue === null || fieldValue === '';
  });
}

function evaluateDecisionProvenance(provenance = {}, options = {}) {
  const value = asObject(provenance);
  const requiredFields = Array.isArray(options.requiredFields)
    ? options.requiredFields
    : DEFAULT_REQUIRED_PROVENANCE_FIELDS;

  const missingFields = getMissingProvenanceFields(value, requiredFields);
  const sourceClassValid = value.sourceClass ? VALID_SOURCE_CLASSES.has(value.sourceClass) : false;

  const computedStalenessDays = Number.isFinite(Number(value.stalenessDays))
    ? Number(value.stalenessDays)
    : computeStalenessDays({
        builtAt: value.builtAt,
        now: options.now,
      });

  const hasExplicitMaxAge = Number.isFinite(Number(options.maxStalenessDays));
  const staleByThreshold =
    hasExplicitMaxAge &&
    Number.isFinite(computedStalenessDays) &&
    computedStalenessDays > Number(options.maxStalenessDays);

  const needsRebuild = value.needsRebuild === true || staleByThreshold;

  const issues = [];
  if (missingFields.length > 0) {
    issues.push('provenance_missing_required_fields');
  }

  if (value.sourceClass && !sourceClassValid) {
    issues.push('provenance_invalid_source_class');
  }

  if (staleByThreshold) {
    issues.push('provenance_stale_by_threshold');
  }

  if (value.needsRebuild === true) {
    issues.push('provenance_declared_needs_rebuild');
  }

  const ok =
    missingFields.length === 0 &&
    sourceClassValid &&
    needsRebuild !== true;

  let status = 'fresh';
  if (missingFields.length > 0 || !sourceClassValid) {
    status = 'missing_or_invalid';
  } else if (needsRebuild) {
    status = 'stale_or_rebuild_required';
  }

  return {
    ok,
    status,
    buildVersion: value.buildVersion || '',
    builtAt: value.builtAt || '',
    sourceClass: value.sourceClass || '',
    sourceClassValid,
    stalenessDays: computedStalenessDays,
    needsRebuild,
    staleByThreshold,
    missingFields,
    issues,
  };
}

module.exports = {
  DEFAULT_REQUIRED_PROVENANCE_FIELDS,
  computeStalenessDays,
  getMissingProvenanceFields,
  evaluateDecisionProvenance,
};
