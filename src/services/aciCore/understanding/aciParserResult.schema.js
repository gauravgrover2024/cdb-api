'use strict';

/**
 * ACI Parser Result Contract
 *
 * Parser can be Gemini, deterministic baseline, or future hybrid parser.
 * It must return a meaning frame plus parser trace.
 */

import {
  assertMeaningFrameShape,
} from './aciMeaningFrame.schema.js';

const PARSER_RESULT_SCHEMA_VERSION = 'aci.parserResult.v1';

const PARSER_TYPES = Object.freeze({
  GEMINI_STRUCTURED: 'gemini_structured',
  DETERMINISTIC_BASELINE: 'deterministic_baseline',
  HYBRID: 'hybrid',
  NOOP: 'noop',
});

function createParserResult(overrides = {}) {
  return {
    schemaVersion: PARSER_RESULT_SCHEMA_VERSION,
    parserType: overrides.parserType || PARSER_TYPES.NOOP,
    parserVersion: overrides.parserVersion || '0.0.0',
    meaningFrame: overrides.meaningFrame || null,
    rawParserOutput: overrides.rawParserOutput || null,
    warnings: Array.isArray(overrides.warnings) ? overrides.warnings : [],
    errors: Array.isArray(overrides.errors) ? overrides.errors : [],
    trace: {
      latencyMs: typeof overrides?.trace?.latencyMs === 'number' ? overrides.trace.latencyMs : null,
      model: overrides?.trace?.model || null,
      promptVersion: overrides?.trace?.promptVersion || null,
      createdAt: overrides?.trace?.createdAt || new Date().toISOString(),
      ...(overrides.trace || {}),
    },
  };
}

function assertParserResultShape(result) {
  if (!result || typeof result !== 'object') {
    throw new Error('ACI parser result must be an object');
  }

  if (result.schemaVersion !== PARSER_RESULT_SCHEMA_VERSION) {
    throw new Error(`Unsupported parser result schemaVersion: ${result.schemaVersion}`);
  }

  if (!Object.values(PARSER_TYPES).includes(result.parserType)) {
    throw new Error(`Invalid parserType: ${result.parserType}`);
  }

  if (!result.meaningFrame) {
    throw new Error('Parser result missing meaningFrame');
  }

  assertMeaningFrameShape(result.meaningFrame);

  return true;
}

export {
  PARSER_RESULT_SCHEMA_VERSION,
  PARSER_TYPES,
  createParserResult,
  assertParserResultShape,
};
