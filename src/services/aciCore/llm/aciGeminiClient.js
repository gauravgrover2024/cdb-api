'use strict';

/**
 * ACI Gemini Client
 *
 * Isolated Gemini wrapper for ACI Core.
 * Do not import old aiAgent planner logic here.
 */

import { generateObject } from 'ai';
import { createGoogleGenerativeAI } from '@ai-sdk/google';

const DEFAULT_MODEL = 'gemini-2.5-flash-lite';
const DEFAULT_FALLBACK_MODEL = 'gemini-2.5-flash';
const DEFAULT_TIMEOUT_MS = 8000;

const getEnv = (key, fallback = '') => {
  const value = process.env[key];
  return value === undefined || value === null || value === '' ? fallback : value;
};

const getGeminiApiKey = () =>
  getEnv('GEMINI_API_KEY') ||
  getEnv('GOOGLE_GENERATIVE_AI_API_KEY') ||
  getEnv('GOOGLE_API_KEY');

const getAciMeaningParserModelName = () =>
  getEnv('ACI_MEANING_PARSER_MODEL', DEFAULT_MODEL);

const getAciMeaningParserFallbackModelName = () =>
  getEnv('ACI_MEANING_PARSER_FALLBACK_MODEL', DEFAULT_FALLBACK_MODEL);

const getAciMeaningParserTimeoutMs = () => {
  const parsed = Number(process.env.ACI_MEANING_PARSER_TIMEOUT_MS || DEFAULT_TIMEOUT_MS);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_TIMEOUT_MS;
};

const isAciMeaningParserEnabled = () =>
  String(process.env.ACI_MEANING_PARSER_ENABLED || 'false').toLowerCase() === 'true';

const createAciGeminiProvider = () => {
  const apiKey = getGeminiApiKey();

  if (!apiKey) {
    throw new Error(
      'Gemini API key missing. Set GEMINI_API_KEY, GOOGLE_GENERATIVE_AI_API_KEY, or GOOGLE_API_KEY.',
    );
  }

  return createGoogleGenerativeAI({ apiKey });
};

async function generateAciStructuredObject({
  schema,
  system,
  prompt,
  modelName = getAciMeaningParserModelName(),
  timeoutMs = getAciMeaningParserTimeoutMs(),
  temperature = 0,
} = {}) {
  const provider = createAciGeminiProvider();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const startedAt = Date.now();

  try {
    const result = await generateObject({
      model: provider(modelName),
      schema,
      system,
      prompt,
      temperature,
      abortSignal: controller.signal,
    });

    return {
      object: result.object,
      rawResult: result,
      trace: {
        model: modelName,
        latencyMs: Date.now() - startedAt,
      },
    };
  } finally {
    clearTimeout(timer);
  }
}

export {
  getGeminiApiKey,
  getAciMeaningParserModelName,
  getAciMeaningParserFallbackModelName,
  getAciMeaningParserTimeoutMs,
  isAciMeaningParserEnabled,
  createAciGeminiProvider,
  generateAciStructuredObject,
};

export default generateAciStructuredObject;
