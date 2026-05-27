import {
  ACI_UNDERSTANDING_CORPUS_V1,
} from './corpus/aciUnderstandingCorpus.v1.js';

import {
  runAciUnderstandingEngine,
} from '../../services/aciCore/understanding/aciUnderstandingEngine.js';

const sampleItems = [
  ACI_UNDERSTANDING_CORPUS_V1.find((item) => item.id === 'direct-001'),
  ACI_UNDERSTANDING_CORPUS_V1.find((item) => item.id === 'broad-003'),
  ACI_UNDERSTANDING_CORPUS_V1.find((item) => item.id === 'messy-001'),
  ACI_UNDERSTANDING_CORPUS_V1.find((item) => item.id === 'context-009'),
].filter(Boolean);

const failures = [];
const results = [];

for (const item of sampleItems) {
  try {
    const result = await runAciUnderstandingEngine({
      message: item.message,
      activeContext: item.activeContext,
    });

    results.push({
      id: item.id,
      message: item.message,
      ok: result.ok,
      fallbackTask: result.meaningFrame.primaryTask,
      clarificationNeeded: result.meaningFrame.clarification?.needed,
    });
  } catch (error) {
    failures.push({
      id: item.id,
      message: item.message,
      error: error.message,
    });
  }
}

console.log(JSON.stringify({
  suite: 'ACI Understanding Engine smoke',
  ok: failures.length === 0,
  total: sampleItems.length,
  failures,
  results,
}, null, 2));

if (failures.length > 0) {
  process.exit(1);
}
