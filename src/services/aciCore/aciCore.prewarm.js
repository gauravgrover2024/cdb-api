'use strict';

/**
 * ACI Core prewarm.
 *
 * Warms DB-backed candidate-retriever caches before the first live query pays
 * the cold-start cost. This module is intentionally small and non-live until
 * wired explicitly into server startup.
 */

import mongoose from 'mongoose';

import {
  prewarmAciDbCandidateRetrieverCaches,
} from './candidates/aciDbCandidateRetriever.js';

import { prewarmBudgetDiscoveryCache } from '../aiAgent/aiAgent.executor.js';

const DEFAULT_PREWARM_TTL_MS = Number(
  process.env.ACI_CORE_PREWARM_TTL_MS || 10 * 60 * 1000,
);

let prewarmState = {
  startedAt: 0,
  completedAt: 0,
  durationMs: 0,
  status: 'idle',
  error: '',
  promise: null,
  results: [],
};

const isMongoReady = () =>
  Boolean(mongoose.connection?.readyState === 1 && mongoose.connection?.db);

const shouldSkipPrewarm = ({ force = false } = {}) => {
  if (force) return false;
  if (prewarmState.status === 'running' && prewarmState.promise) return true;
  if (!prewarmState.completedAt) return false;

  return Date.now() - prewarmState.completedAt < DEFAULT_PREWARM_TTL_MS;
};

const normalizeSettled = (item, label) => ({
  label,
  ok: item.status === 'fulfilled' && item.value?.ok !== false,
  durationMs: item.status === 'fulfilled' ? item.value?.durationMs ?? null : null,
  cache: item.status === 'fulfilled' ? item.value?.cache ?? null : null,
  error: item.status === 'rejected'
    ? item.reason?.message || String(item.reason || '')
    : item.value?.error || '',
});

async function prewarmAciCoreRuntime({ force = false } = {}) {
  if (!isMongoReady()) {
    return {
      ...prewarmState,
      status: 'skipped',
      error: 'mongoose_not_connected',
    };
  }

  if (shouldSkipPrewarm({ force })) {
    return prewarmState.promise || prewarmState;
  }

  const startedAt = Date.now();

  prewarmState = {
    ...prewarmState,
    startedAt,
    status: 'running',
    error: '',
    promise: null,
  };

  prewarmState.promise = (async () => {
    const tasks = [
      [
        'candidate_retriever_catalogs',
        prewarmAciDbCandidateRetrieverCaches({ force }),
      ],
      [
        'budget_discovery_cache',
        prewarmBudgetDiscoveryCache({ force }),
      ],
    ];

    const settled = await Promise.allSettled(tasks.map(([, promise]) => promise));
    const results = settled.map((item, index) => normalizeSettled(item, tasks[index][0]));
    const failed = results.filter((item) => !item.ok);
    const completedAt = Date.now();

    prewarmState = {
      startedAt,
      completedAt,
      durationMs: completedAt - startedAt,
      status: failed.length ? 'partial' : 'ready',
      error: failed.map((item) => `${item.label}: ${item.error}`).join(' | '),
      promise: null,
      results,
    };

    if (process.env.ACI_CORE_PREWARM_LOG !== 'false') {
      const summary = results
        .map((item) => `${item.label}:${item.ok ? 'ok' : 'failed'}`)
        .join(', ');

      console.log(
        `[ACI Core] prewarm ${prewarmState.status} in ${prewarmState.durationMs}ms (${summary})`,
      );

      if (prewarmState.error) {
        console.warn(`[ACI Core] prewarm warnings: ${prewarmState.error}`);
      }
    }

    return prewarmState;
  })();

  return prewarmState.promise;
}

const triggerAciCoreRuntimePrewarm = ({ force = false } = {}) => {
  if (!isMongoReady()) return null;

  if (shouldSkipPrewarm({ force })) {
    return prewarmState.promise || null;
  }

  return prewarmAciCoreRuntime({ force }).catch((error) => {
    prewarmState = {
      ...prewarmState,
      completedAt: Date.now(),
      durationMs: Date.now() - (prewarmState.startedAt || Date.now()),
      status: 'failed',
      error: error?.message || String(error || ''),
      promise: null,
    };

    return prewarmState;
  });
};

const getAciCoreRuntimePrewarmState = () => ({
  ...prewarmState,
  promise: prewarmState.promise ? '[in-flight]' : null,
});

export {
  prewarmAciCoreRuntime,
  triggerAciCoreRuntimePrewarm,
  getAciCoreRuntimePrewarmState,
};

export default prewarmAciCoreRuntime;
