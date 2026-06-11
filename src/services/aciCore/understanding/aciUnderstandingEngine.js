'use strict';

/**
 * ACI Understanding Engine
 *
 * Orchestrates:
 * message + active context
 * → candidate snapshot
 * → parser
 * → meaning frame validation
 *
 * This file must stay domain-extensible.
 * It must not contain automotive facts.
 */

import {
  createEmptyCandidateSnapshot,
  assertCandidateSnapshotShape,
} from '../candidates/aciCandidateSnapshot.schema.js';

import {
  createEmptyMeaningFrame,
  assertMeaningFrameShape,
  ACI_MESSAGE_TYPES,
  ACI_TASKS,
} from './aciMeaningFrame.schema.js';

import {
  PARSER_TYPES,
  createParserResult,
  assertParserResultShape,
} from './aciParserResult.schema.js';

function normalizeMessage(message = '') {
  return String(message || '').trim().replace(/\s+/g, ' ');
}

function createFallbackParserResult({ rawMessage = '', normalizedMessage = '', activeContext = null } = {}) {
  const meaningFrame = createEmptyMeaningFrame({
    messageType: ACI_MESSAGE_TYPES.AUTOMOTIVE_QUERY,
    primaryTask: ACI_TASKS.CLARIFICATION,
    rawMessage,
    normalizedMessage,
    anchors: {
      ...createEmptyMeaningFrame().anchors,
    },
    context: {
      ...createEmptyMeaningFrame().context,
      action: 'ask_clarification',
    },
    clarification: {
      needed: true,
      reason: 'parser_not_configured',
      question: 'I can help, but I need the topic first: price, features, on-road price, EMI, colours, offers, service cost, or comparison.',
      options: [],
    },
    confidence: {
      overall: 0,
      entityResolution: 0,
      taskUnderstanding: 0,
      toolReadiness: 0,
    },
    trace: {
      parser: PARSER_TYPES.NOOP,
      parserVersion: '0.0.0',
      createdAt: new Date().toISOString(),
    },
  });

  return createParserResult({
    parserType: PARSER_TYPES.NOOP,
    parserVersion: '0.0.0',
    meaningFrame,
    warnings: ['No parser supplied; returned clarification fallback'],
    trace: {
      latencyMs: 0,
      activeContextPresent: Boolean(activeContext),
    },
  });
}

async function runAciUnderstandingEngine({
  message = '',
  activeContext = null,
  candidateSnapshot = null,
  candidateRetriever = null,
  parser = null,
  trace = {},
} = {}) {
  const startedAt = Date.now();
  const rawMessage = String(message || '');
  const normalizedMessage = normalizeMessage(rawMessage);

  const snapshot = candidateSnapshot || (
    typeof candidateRetriever === 'function'
      ? await candidateRetriever({ rawMessage, normalizedMessage, activeContext })
      : createEmptyCandidateSnapshot({ rawMessage, normalizedMessage, activeContext })
  );

  assertCandidateSnapshotShape(snapshot);

  const parserResult = typeof parser === 'function'
    ? await parser({ rawMessage, normalizedMessage, activeContext, candidateSnapshot: snapshot })
    : createFallbackParserResult({ rawMessage, normalizedMessage, activeContext });

  assertParserResultShape(parserResult);
  assertMeaningFrameShape(parserResult.meaningFrame);

  return {
    ok: true,
    schemaVersion: 'aci.understandingResult.v1',
    rawMessage,
    normalizedMessage,
    candidateSnapshot: snapshot,
    parserResult,
    meaningFrame: parserResult.meaningFrame,
    trace: {
      engine: 'aciUnderstandingEngine',
      engineVersion: '0.1.0',
      latencyMs: Date.now() - startedAt,
      ...trace,
    },
  };
}

export {
  normalizeMessage,
  createFallbackParserResult,
  runAciUnderstandingEngine,
};

export default runAciUnderstandingEngine;
