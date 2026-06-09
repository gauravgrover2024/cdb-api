'use strict';

/**
 * Hybrid ACI meaning-frame parser.
 *
 * Production-shaped parser router:
 * - Deterministic parser first.
 * - Gemini only when deterministic frame is weak/ambiguous and enabled.
 * - Safe fallback to deterministic if Gemini fails, times out, or quota fails.
 *
 * This file does not touch live chat routing.
 */

import {
  parseDeterministicMeaningFrame,
} from './deterministicMeaningFrame.parser.js';

import {
  parseGeminiMeaningFrame,
} from './geminiMeaningFrame.parser.js';

import {
  getGeminiApiKey,
  isAciMeaningParserEnabled,
} from '../llm/aciGeminiClient.js';

const ROUTER_VERSION = '0.1.0';

const DEFAULT_CONFIDENCE_THRESHOLD = 0.75;

const safeNumber = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const getConfidenceThreshold = () => {
  const parsed = Number(process.env.ACI_MEANING_ROUTER_CONFIDENCE_THRESHOLD || DEFAULT_CONFIDENCE_THRESHOLD);
  return Number.isFinite(parsed) && parsed > 0 && parsed <= 1
    ? parsed
    : DEFAULT_CONFIDENCE_THRESHOLD;
};

const isWeakDeterministicFrame = (parserResult = {}) => {
  const frame = parserResult.meaningFrame || {};
  const confidence = safeNumber(frame.confidence?.overall, 0);
  const threshold = getConfidenceThreshold();

  const reasons = [];

  if (confidence < threshold) {
    reasons.push(`low_confidence:${confidence}`);
  }

  if (frame.clarification?.needed) {
    reasons.push('clarification_needed');
  }

  if (frame.primaryTask === 'clarification') {
    reasons.push('primary_task_clarification');
  }

  if (frame.primaryTask === 'unsupported') {
    reasons.push('primary_task_unsupported');
  }

  if (Array.isArray(frame.context?.ambiguity) && frame.context.ambiguity.length) {
    reasons.push('context_ambiguity');
  }

  return {
    weak: reasons.length > 0,
    reasons,
    confidence,
    threshold,
  };
};

const decorateParserResult = (parserResult = {}, routerTrace = {}, extra = {}) => ({
  ...parserResult,
  warnings: [
    ...(parserResult.warnings || []),
    ...(extra.warnings || []),
  ],
  errors: [
    ...(parserResult.errors || []),
    ...(extra.errors || []),
  ],
  trace: {
    ...(parserResult.trace || {}),
    router: {
      version: ROUTER_VERSION,
      ...routerTrace,
    },
  },
});

async function parseHybridMeaningFrame({
  rawMessage = '',
  normalizedMessage = '',
  activeContext = null,
  candidateSnapshot = null,
} = {}) {
  const startedAt = Date.now();

  const deterministicResult = await parseDeterministicMeaningFrame({
    rawMessage,
    normalizedMessage,
    activeContext,
    candidateSnapshot,
  });

  if (candidateSnapshot?.vehicles?.variantResolution?.status === 'exact_unavailable') {
    return decorateParserResult(deterministicResult, {
      selectedParser: 'deterministic',
      usedGemini: false,
      reason: 'exact_variant_unavailable_from_model_scoped_resolver',
      latencyMs: Date.now() - startedAt,
    });
  }

  const weakness = isWeakDeterministicFrame(deterministicResult);

  if (!weakness.weak) {
    return decorateParserResult(deterministicResult, {
      selectedParser: 'deterministic',
      usedGemini: false,
      reason: 'high_confidence_deterministic',
      deterministicConfidence: weakness.confidence,
      threshold: weakness.threshold,
      latencyMs: Date.now() - startedAt,
    });
  }

  const geminiEnabled = isAciMeaningParserEnabled();
  const hasGeminiKey = Boolean(getGeminiApiKey());

  if (!geminiEnabled || !hasGeminiKey) {
    return decorateParserResult(
      deterministicResult,
      {
        selectedParser: 'deterministic',
        usedGemini: false,
        reason: !geminiEnabled ? 'gemini_disabled' : 'gemini_key_missing',
        deterministicWeakReasons: weakness.reasons,
        deterministicConfidence: weakness.confidence,
        threshold: weakness.threshold,
        latencyMs: Date.now() - startedAt,
      },
      {
        warnings: [
          !geminiEnabled
            ? 'Hybrid router kept deterministic frame because Gemini parser is disabled.'
            : 'Hybrid router kept deterministic frame because Gemini API key is missing.',
        ],
      },
    );
  }

  try {
    const geminiResult = await parseGeminiMeaningFrame({
      rawMessage,
      normalizedMessage,
      activeContext,
      candidateSnapshot,
    });

    const geminiFrame = geminiResult.meaningFrame || {};
    const geminiConfidence = safeNumber(geminiFrame.confidence?.overall, 0);

    const useGemini = (
      !geminiFrame.clarification?.needed &&
      geminiFrame.primaryTask !== 'clarification' &&
      geminiConfidence >= weakness.confidence
    );

    if (useGemini) {
      return decorateParserResult(geminiResult, {
        selectedParser: 'gemini',
        usedGemini: true,
        reason: 'gemini_improved_or_equal_low_confidence_frame',
        deterministicWeakReasons: weakness.reasons,
        deterministicConfidence: weakness.confidence,
        geminiConfidence,
        threshold: weakness.threshold,
        latencyMs: Date.now() - startedAt,
      });
    }

    return decorateParserResult(
      deterministicResult,
      {
        selectedParser: 'deterministic',
        usedGemini: true,
        reason: 'gemini_not_better_than_deterministic',
        deterministicWeakReasons: weakness.reasons,
        deterministicConfidence: weakness.confidence,
        geminiConfidence,
        threshold: weakness.threshold,
        latencyMs: Date.now() - startedAt,
      },
      {
        warnings: [
          'Hybrid router tried Gemini but kept deterministic frame because Gemini did not improve confidence/readiness.',
        ],
      },
    );
  } catch (error) {
    return decorateParserResult(
      deterministicResult,
      {
        selectedParser: 'deterministic',
        usedGemini: true,
        reason: 'gemini_failed_fallback_to_deterministic',
        deterministicWeakReasons: weakness.reasons,
        deterministicConfidence: weakness.confidence,
        threshold: weakness.threshold,
        latencyMs: Date.now() - startedAt,
      },
      {
        warnings: [
          'Hybrid router tried Gemini but fell back to deterministic frame.',
        ],
        errors: [
          {
            parser: 'hybridMeaningFrameParser',
            message: error?.message || String(error || ''),
          },
        ],
      },
    );
  }
}

export {
  ROUTER_VERSION,
  isWeakDeterministicFrame,
  parseHybridMeaningFrame,
};

export default parseHybridMeaningFrame;
