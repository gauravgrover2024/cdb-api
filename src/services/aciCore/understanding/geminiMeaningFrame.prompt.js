'use strict';

/**
 * Prompt builder for Gemini meaning-frame parser.
 */

import {
  ACI_MESSAGE_TYPES,
  ACI_DOMAINS,
  ACI_TASKS,
  ACI_CONTEXT_ACTIONS,
  ACI_RESULT_GRANULARITY,
} from './aciMeaningFrame.schema.js';

const PROMPT_VERSION = 'aci.meaningFrame.prompt.v1';

const compactCandidate = (item = {}) => ({
  key: item.canonicalKey || null,
  name: item.displayName || item.rawText || null,
  type: item.type || null,
  confidence: item.confidence ?? null,
  metadata: item.metadata || {},
});

const compactCandidateSnapshotForPrompt = (snapshot = {}) => ({
  rawMessage: snapshot.rawMessage || '',
  normalizedMessage: snapshot.normalizedMessage || '',
  activeContext: snapshot.activeContext || null,
  vehicles: {
    makes: (snapshot.vehicles?.makes || []).slice(0, 8).map(compactCandidate),
    models: (snapshot.vehicles?.models || []).slice(0, 8).map(compactCandidate),
    variants: (snapshot.vehicles?.variants || []).slice(0, 12).map(compactCandidate),
    colors: (snapshot.vehicles?.colors || []).slice(0, 8).map(compactCandidate),
  },
  taxonomy: {
    features: (snapshot.taxonomy?.features || []).slice(0, 12).map(compactCandidate),
    bodyTypes: (snapshot.taxonomy?.bodyTypes || []).slice(0, 8).map(compactCandidate),
    fuelTypes: (snapshot.taxonomy?.fuelTypes || []).slice(0, 8).map(compactCandidate),
    transmissions: (snapshot.taxonomy?.transmissions || []).slice(0, 8).map(compactCandidate),
  },
  commerce: {
    budgets: (snapshot.commerce?.budgets || []).slice(0, 4).map(compactCandidate),
    cities: (snapshot.commerce?.cities || []).slice(0, 6).map(compactCandidate),
    finance: (snapshot.commerce?.finance || []).slice(0, 6).map(compactCandidate),
    ownership: (snapshot.commerce?.ownership || []).slice(0, 6).map(compactCandidate),
  },
  language: {
    tasks: (snapshot.language?.tasks || []).slice(0, 10).map(compactCandidate),
    ambiguity: snapshot.language?.ambiguity || [],
  },
});

const systemPrompt = `
You are the ACI Assist semantic parser.

Your job is to convert a customer message into the ACI meaning-frame JSON contract.

Rules:
- Return only structured JSON matching the schema.
- Do not answer the customer.
- Do not invent car facts, prices, variants, colors, features, availability, ratings, offers, waiting periods, or ownership claims.
- Use the candidate snapshot as the source of grounded entities.
- Prefer candidate canonical keys for filters.features, fuelTypes, transmissions, colors and provider/tool hints.
- If a model/variant/feature is not confidently grounded, mark ambiguity or clarification.
- Understand compressed Indian buyer language, Hinglish, typos, no-comma queries, and multi-intent vehicle questions.
- Treat "Punch and Nexon CNG sunroof ABS ADAS" and "Punch CNG and Nexon CNG sunroof ABS ADAS" as equivalent meaning.
- For broad listing questions like "cars under 20 lakh", "Hyundai cars", "cars with sunroof", set discovery.isBroadDiscovery=true.
- For two or more explicit vehicles, set primaryTask=vehicle_comparison unless the message is clearly only asking for a list/filter.
- For unsupported/future-provider requests, classify the correct domain/task and set safety.unsupportedReason if not currently answerable.
- Never route test-drive requests; test drive is currently not part of ACI Assist scope.
`.trim();

function buildGeminiMeaningFramePrompt({
  rawMessage = '',
  normalizedMessage = '',
  candidateSnapshot = {},
} = {}) {
  const compactSnapshot = compactCandidateSnapshotForPrompt(candidateSnapshot);

  return `
Allowed messageTypes:
${JSON.stringify(Object.values(ACI_MESSAGE_TYPES))}

Allowed domains:
${JSON.stringify(Object.values(ACI_DOMAINS))}

Allowed tasks:
${JSON.stringify(Object.values(ACI_TASKS))}

Allowed context actions:
${JSON.stringify(Object.values(ACI_CONTEXT_ACTIONS))}

Allowed result granularities:
${JSON.stringify(Object.values(ACI_RESULT_GRANULARITY))}

Customer raw message:
${rawMessage}

Customer normalized message:
${normalizedMessage}

Candidate snapshot:
${JSON.stringify(compactSnapshot, null, 2)}

Return the best ACI meaning frame.
`.trim();
}

export {
  PROMPT_VERSION,
  systemPrompt,
  compactCandidateSnapshotForPrompt,
  buildGeminiMeaningFramePrompt,
};

export default buildGeminiMeaningFramePrompt;
