#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const REPO_ROOT = path.resolve(__dirname, '../../..');

const SCAN_PATHS = [
  'src/services/aciCore/decisionPolicy',
  'src/services/aciCore/decisionProfiles',
  'src/services/aciCore/scoreProfiles',
  'src/scripts/aci-decision',
];

const SERVICE_FILE_PREFIXES = [
  'src/services/aciCore/decisionPolicy',
  'src/services/aciCore/decisionProfiles',
  'src/services/aciCore/scoreProfiles',
];

const isServiceFile = (file = '') =>
  SERVICE_FILE_PREFIXES.some((prefix) => file.startsWith(prefix));

const BANNED_RULES = [
  {
    category: 'vehicle_specific_fact_in_core_decision_service',
    serviceOnly: true,
    patterns: [
      /\bhyundai\b/i,
      /\bkia\b/i,
      /\bmaruti\b/i,
      /\btata\b/i,
      /\bhonda\b/i,
      /\btoyota\b/i,
      /\bmahindra\b/i,
      /\bcreta\b/i,
      /\bseltos\b/i,
      /\bbaleno\b/i,
      /\bnexon\b/i,
      /\bverna\b/i,
      /\bcity zx\b/i,
    ],
  },
  {
    category: 'hardcoded_market_or_persona_judgement',
    serviceOnly: false,
    patterns: [
      /\burban premium\b/i,
      /\bmarket leader\b/i,
      /\bmust buy\b/i,
      /\bbuy this\b/i,
      /\bgo for this\b/i,
      /\bavoid this\b/i,
      /\bbest choice\b/i,
      /\bbest pick\b/i,
      /\bbest family\b/i,
      /\bfamily buyer\b/i,
      /\bpoor resale\b/i,
      /\bstrong resale\b/i,
      /\bservice network\b/i,
      /\bdirect rival\b/i,
      /\brival pair\b/i,
      /\bpremium feel\b/i,
      /\benthusiast\b/i,
      /\bchauffeur\b/i,
      /\brecommended car\b/i,
      /\brecommended buy\b/i,
      /\boverall winner\b/i,
      /\bfinal winner\b/i,
      /\bclear winner\b/i,
      /\bvalue pick\b/i,
      /\bstrongest value pick\b/i,
      /\bstrongest same-family value pick\b/i,
    ],
  },
  {
    category: 'final_recommendation_activation_or_placeholder',
    serviceOnly: false,
    patterns: [
      /\bcanUseForFinalRecommendation\s*:\s*true\b/,
      /\bFINAL_RECOMMENDATION_ALLOWED\b/,
      /\brecommendationScore\b/,
      /\bbuyerSegment\b/,
      /\bidealFor\b/,
      /\bskipIf\b/,
    ],
  },
  {
    category: 'sponsored_influence_surface',
    serviceOnly: false,
    patterns: [
      /\bsponsoredInfluenceDetected\b/,
      /\bsponsored_influence_not_allowed\b/i,
      /\badInfluence\b/i,
      /\bpaid placement\b/i,
      /\bpaid recommendation\b/i,
    ],
  },
];

const SAFE_CONTEXT_PATTERNS = [
  /\bnot a final recommendation\b/i,
  /\bnot final recommendation\b/i,
  /\bdiagnostic-only\b/i,
  /\bdiagnostic only\b/i,
  /\bdiagnostic module scores\b/i,
  /\bNo final overall winner is computed\b/i,
  /\bfinal recommendation needs\b/i,
  /\bfinal recommendation requires\b/i,
  /\bfinal recommendation is blocked\b/i,
  /\bfinal recommendation policy\b/i,
  /\bcannot final recommend\b/i,
  /\bblockedReasons\b/,
  /\bSPONSORED_INFLUENCE_NOT_ALLOWED\b/,
];

function isTestOrAuditFile(file = '') {
  const base = path.basename(file);
  return (
    file.startsWith('src/scripts/aci-decision/') &&
    /(?:audit|smoke|eval|corpus|fixture|runDecision|runCrossModel|language)/i.test(base)
  );
}

function walk(dir) {
  const absDir = path.join(REPO_ROOT, dir);
  if (!fs.existsSync(absDir)) return [];

  const out = [];
  for (const entry of fs.readdirSync(absDir, { withFileTypes: true })) {
    const abs = path.join(absDir, entry.name);
    if (entry.isDirectory()) {
      out.push(...walk(path.relative(REPO_ROOT, abs)));
    } else if (entry.isFile() && /\.(cjs|js|json|md)$/.test(entry.name)) {
      out.push(path.relative(REPO_ROOT, abs));
    }
  }
  return out;
}

function getWindow(lines, index, radius = 3) {
  const start = Math.max(0, index - radius);
  const end = Math.min(lines.length, index + radius + 1);
  return lines.slice(start, end).join('\n');
}

function hasSafeContext(text = '') {
  return SAFE_CONTEXT_PATTERNS.some((pattern) => pattern.test(text));
}

function getAllowedReason({ file, line, window, rule }) {
  const normalizedLine = String(line || '').trim();

  if (isTestOrAuditFile(file)) {
    return 'test_or_audit_guardrail_fixture';
  }

  if (
    file === 'src/services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs' &&
    (
      /\bFINAL_RECOMMENDATION_ALLOWED\b/.test(line) ||
      /\bSPONSORED_INFLUENCE_NOT_ALLOWED\b/.test(line) ||
      /\bsponsored_influence_not_allowed\b/i.test(line)
    )
  ) {
    return 'central_policy_constant';
  }

  if (
    file === 'src/services/aciCore/decisionPolicy/aciDecisionPolicy.service.cjs' &&
    (
      /\bFINAL_RECOMMENDATION_ALLOWED\b/.test(line) ||
      /\bcanUseForFinalRecommendation\s*=\s*true\b/.test(line) ||
      /\bcanUseForFinalRecommendation\s*:\s*true\b/.test(line) ||
      /\bsponsoredInfluenceDetected\b/.test(line)
    )
  ) {
    return 'central_policy_gate';
  }

  if (
    file === 'src/services/aciCore/decisionPolicy/aciFinalRecommendationEligibility.service.cjs' &&
    (
      /\bFINAL_RECOMMENDATION_ALLOWED\b/.test(line) ||
      /\bcanUseForFinalRecommendation\s*:\s*finalRecommendationReady\b/.test(line) ||
      /\bfinalRecommendationEnabled\s*:\s*finalRecommendationReady\b/.test(line)
    )
  ) {
    return 'evidence_gated_final_policy';
  }

  if (
    file === 'src/services/aciCore/decisionPolicy/aciDecisionModulePolicyProfiles.service.cjs' &&
    (
      /\bFINAL_RECOMMENDATION_ALLOWED\b/.test(line) ||
      /\bcanEverUseForFinalRecommendation\b/.test(line) ||
      /\bcanUseForFinalRecommendation\b/.test(line)
    )
  ) {
    return 'module_policy_gate';
  }

  if (
    file === 'src/services/aciCore/decisionProfiles/contracts/aciVariantDecisionProfile.manifest.cjs' &&
    (
      /path:\s*'buyerSegment'/.test(line) ||
      /path:\s*'scores\.recommendationScore'/.test(line) ||
      /RECOMMENDATION_SCORE/.test(line)
    )
  ) {
    return 'manifest_future_field_placeholder';
  }

  if (file === 'src/services/aciCore/decisionProfiles/aciVariantDecisionProfile.builder.cjs') {
    if (
      /\brecommendationScore\b/.test(line) &&
      (
        normalizedLine === "'recommendationScore'," ||
        /recommendationScore\s*:\s*null/.test(line) ||
        /SCORE_FIELDS/.test(window)
      )
    ) {
      return 'score_field_null_placeholder';
    }

    if (/\bbuyerSegment\b/.test(line) && /getFirst\(modelSummary/.test(line)) {
      return 'source_field_passthrough_not_hardcoded';
    }

    if (/\bidealFor\b/.test(line) && /idealFor:\s*\[\]/.test(line)) {
      return 'empty_future_persona_placeholder';
    }

    if (/\bskipIf\b/.test(line) && /skipIf:\s*\[\]/.test(line)) {
      return 'empty_future_persona_placeholder';
    }
  }

  if (hasSafeContext(window)) {
    return 'safe_diagnostic_or_blocking_context';
  }

  return '';
}

const files = [...new Set(SCAN_PATHS.flatMap(walk))].sort();
const violations = [];
const allowedMatches = [];

for (const file of files) {
  const abs = path.join(REPO_ROOT, file);
  const lines = fs.readFileSync(abs, 'utf8').split(/\r?\n/);

  lines.forEach((line, index) => {
    const window = getWindow(lines, index);

    for (const rule of BANNED_RULES) {
      if (rule.serviceOnly && !isServiceFile(file)) continue;

      for (const pattern of rule.patterns) {
        if (!pattern.test(line)) continue;

        const allowedReason = getAllowedReason({ file, line, window, rule });

        const hit = {
          file,
          line: index + 1,
          category: rule.category,
          pattern: String(pattern),
          text: line.trim(),
        };

        if (allowedReason) {
          allowedMatches.push({
            ...hit,
            allowedReason,
          });
          continue;
        }

        violations.push(hit);
      }
    }
  });
}

const summary = {
  suite: 'ACI Decision Market-Judgement Isolation Audit v1',
  ok: violations.length === 0,
  scannedPaths: SCAN_PATHS,
  scannedFileCount: files.length,
  scannedFiles: files,
  bannedRuleCount: BANNED_RULES.length,
  bannedPatternCount: BANNED_RULES.reduce((sum, rule) => sum + rule.patterns.length, 0),
  allowedMatchCount: allowedMatches.length,
  allowedMatches: allowedMatches.slice(0, 120),
  violationCount: violations.length,
  violations,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) {
  process.exit(1);
}
