'use strict';

/**
 * Gemini meaning-frame parser.
 *
 * Non-live parser for ACI Core evals.
 */

import {
  generateAciStructuredObject,
  getAciMeaningParserModelName,
  getAciMeaningParserFallbackModelName,
  getAciMeaningParserTimeoutMs,
} from '../llm/aciGeminiClient.js';

import {
  createEmptyMeaningFrame,
  ACI_MESSAGE_TYPES,
  ACI_TASKS,
  ACI_CONTEXT_ACTIONS,
  assertMeaningFrameShape,
} from './aciMeaningFrame.schema.js';

import {
  PARSER_TYPES,
  createParserResult,
  assertParserResultShape,
} from './aciParserResult.schema.js';

import {
  GeminiMeaningFrameSchema,
} from './geminiMeaningFrame.schema.js';

import {
  PROMPT_VERSION,
  systemPrompt,
  buildGeminiMeaningFramePrompt,
} from './geminiMeaningFrame.prompt.js';

import {
  repairGeminiMeaningFrame,
} from './geminiMeaningFrame.repair.js';

const PARSER_VERSION = '0.1.0';

const createErrorMeaningFrame = ({
  rawMessage = '',
  normalizedMessage = '',
  error,
} = {}) => createEmptyMeaningFrame({
  messageType: ACI_MESSAGE_TYPES.AUTOMOTIVE_QUERY,
  primaryTask: ACI_TASKS.CLARIFICATION,
  rawMessage,
  normalizedMessage,
  context: {
    ...createEmptyMeaningFrame().context,
    action: ACI_CONTEXT_ACTIONS.ASK_CLARIFICATION,
  },
  clarification: {
    needed: true,
    reason: 'meaning_parser_failed',
    question: 'I could not understand this clearly. What would you like to check?',
    options: [],
  },
  confidence: {
    overall: 0,
    entityResolution: 0,
    taskUnderstanding: 0,
    toolReadiness: 0,
  },
  trace: {
    parser: 'geminiMeaningFrameParser',
    parserVersion: PARSER_VERSION,
    error: error?.message || String(error || ''),
    createdAt: new Date().toISOString(),
  },
});

async function runGeminiAttempt({
  rawMessage,
  normalizedMessage,
  candidateSnapshot,
  modelName,
  timeoutMs,
} = {}) {
  const prompt = buildGeminiMeaningFramePrompt({
    rawMessage,
    normalizedMessage,
    candidateSnapshot,
  });

  const result = await generateAciStructuredObject({
    schema: GeminiMeaningFrameSchema,
    system: systemPrompt,
    prompt,
    modelName,
    timeoutMs,
    temperature: 0,
  });

  const meaningFrame = repairGeminiMeaningFrame({
    frame: result.object,
    rawMessage,
    normalizedMessage,
    candidateSnapshot,
    parserName: 'geminiMeaningFrameParser',
    parserVersion: PARSER_VERSION,
  });

  assertMeaningFrameShape(meaningFrame);

  return {
    meaningFrame,
    rawParserOutput: result.object,
    trace: result.trace,
  };
}

async function parseGeminiMeaningFrame({
  rawMessage = '',
  normalizedMessage = '',
  activeContext = null,
  candidateSnapshot = null,
  timeoutMs = getAciMeaningParserTimeoutMs(),
} = {}) {
  const startedAt = Date.now();
  const primaryModel = getAciMeaningParserModelName();
  const fallbackModel = getAciMeaningParserFallbackModelName();
  const errors = [];

  for (const modelName of [primaryModel, fallbackModel].filter(Boolean)) {
    try {
      const attempt = await runGeminiAttempt({
        rawMessage,
        normalizedMessage,
        candidateSnapshot,
        modelName,
        timeoutMs,
      });

      const parserResult = createParserResult({
        parserType: PARSER_TYPES.GEMINI_STRUCTURED,
        parserVersion: PARSER_VERSION,
        meaningFrame: attempt.meaningFrame,
        rawParserOutput: attempt.rawParserOutput,
        warnings: [],
        errors,
        trace: {
          latencyMs: Date.now() - startedAt,
          model: modelName,
          promptVersion: PROMPT_VERSION,
          activeContextPresent: Boolean(activeContext),
          attemptLatencyMs: attempt.trace?.latencyMs ?? null,
        },
      });

      assertParserResultShape(parserResult);
      return parserResult;
    } catch (error) {
      errors.push({
        model: modelName,
        message: error?.message || String(error || ''),
      });

      if (modelName === fallbackModel) break;
    }
  }

  const errorFrame = createErrorMeaningFrame({
    rawMessage,
    normalizedMessage,
    error: errors[errors.length - 1]?.message || 'Unknown parser failure',
  });

  const parserResult = createParserResult({
    parserType: PARSER_TYPES.GEMINI_STRUCTURED,
    parserVersion: PARSER_VERSION,
    meaningFrame: errorFrame,
    rawParserOutput: null,
    warnings: ['Gemini meaning parser failed; returned clarification frame'],
    errors,
    trace: {
      latencyMs: Date.now() - startedAt,
      model: primaryModel,
      fallbackModel,
      promptVersion: PROMPT_VERSION,
      activeContextPresent: Boolean(activeContext),
    },
  });

  assertParserResultShape(parserResult);
  return parserResult;
}

export {
  PARSER_VERSION,
  parseGeminiMeaningFrame,
};

export default parseGeminiMeaningFrame;
