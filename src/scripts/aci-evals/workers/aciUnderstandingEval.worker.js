import { parentPort, workerData } from 'worker_threads';

import {
  runAciUnderstandingEngine,
} from '../../../services/aciCore/understanding/aciUnderstandingEngine.js';

const { cases = [], caseTimeoutMs = 12000 } = workerData || {};

function withTimeout(promise, timeoutMs, label) {
  let timer = null;

  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`Timed out after ${timeoutMs}ms: ${label}`));
    }, timeoutMs);
  });

  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

const results = [];

for (const item of cases) {
  const startedAt = Date.now();

  try {
    const output = await withTimeout(
      runAciUnderstandingEngine({
        message: item.message,
        activeContext: item.activeContext,
      }),
      caseTimeoutMs,
      item.id,
    );

    results.push({
      id: item.id,
      bucket: item.bucket,
      message: item.message,
      ok: Boolean(output?.ok),
      durationMs: Date.now() - startedAt,
      expectedPrimaryTask: item.expected?.primaryTask || null,
      actualPrimaryTask: output?.meaningFrame?.primaryTask || null,
      parserType: output?.parserResult?.parserType || null,
      clarificationNeeded: Boolean(output?.meaningFrame?.clarification?.needed),
      error: null,
    });
  } catch (error) {
    results.push({
      id: item.id,
      bucket: item.bucket,
      message: item.message,
      ok: false,
      durationMs: Date.now() - startedAt,
      expectedPrimaryTask: item.expected?.primaryTask || null,
      actualPrimaryTask: null,
      parserType: null,
      clarificationNeeded: null,
      error: error.message,
    });
  }
}

parentPort.postMessage({ results });
