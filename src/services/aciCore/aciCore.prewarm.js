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

import {
  prewarmBudgetDiscoveryCache,
  triggerBudgetDiscoveryCacheWarm,
} from '../aiAgent/aiAgent.executor.js';

const DEFAULT_PREWARM_TTL_MS = Number(
  process.env.ACI_CORE_PREWARM_TTL_MS || 10 * 60 * 1000,
);

const ACI_CORE_PREWARM_WARN_MS = Number(
  process.env.ACI_CORE_PREWARM_WARN_MS || 10000,
);

let prewarmState = {
  startedAt: 0,
  completedAt: 0,
  durationMs: 0,
  status: 'idle',
  mode: 'light',
  backgroundWarmTriggered: false,
  error: '',
  promise: null,
  results: [],
};

const normalizePrewarmMode = (mode = '') => {
  const normalized = String(mode || process.env.ACI_CORE_PREWARM_MODE || 'light').trim().toLowerCase();
  return normalized === 'full' ? 'full' : 'light';
};

const shouldTriggerBackgroundWarm = ({ background = null } = {}) => {
  if (typeof background === 'boolean') return background;
  return process.env.ACI_CORE_PREWARM_BACKGROUND === 'true';
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

const skippedResult = (label, reason, mode) => ({
  label,
  ok: true,
  durationMs: 0,
  cache: {
    skipped: true,
    reason,
    mode,
  },
  error: '',
});

const triggerHeavyBackgroundWarm = ({ force = false } = {}) => {
  prewarmAciDbCandidateRetrieverCaches({ force })
    .catch((error) => {
      console.warn(`[ACI Core] background candidate warm failed: ${error?.message || error}`);
    });

  triggerBudgetDiscoveryCacheWarm({ force });
};

async function prewarmAciCoreRuntime({ force = false, mode = null, background = null } = {}) {
  if (!isMongoReady()) {
    return {
      ...prewarmState,
      status: 'skipped',
      error: 'mongoose_not_connected',
    };
  }

  const prewarmMode = normalizePrewarmMode(mode);
  const backgroundWarm = prewarmMode === 'light' && shouldTriggerBackgroundWarm({ background });

  if (shouldSkipPrewarm({ force }) && prewarmState.mode === prewarmMode) {
    return prewarmState.promise || prewarmState;
  }

  const startedAt = Date.now();

  prewarmState = {
    ...prewarmState,
    startedAt,
    status: 'running',
    mode: prewarmMode,
    backgroundWarmTriggered: false,
    error: '',
    promise: null,
  };

  prewarmState.promise = (async () => {
    let results = [];

    if (prewarmMode === 'full') {
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
      results = settled.map((item, index) => normalizeSettled(item, tasks[index][0]));
    } else {
      results = [
        skippedResult('candidate_retriever_catalogs', 'light_prewarm_skips_heavy_candidate_catalogs', prewarmMode),
        skippedResult('budget_discovery_cache', 'light_prewarm_skips_heavy_budget_cache', prewarmMode),
      ];

      if (backgroundWarm) {
        triggerHeavyBackgroundWarm({ force: false });
      }
    }

    const failed = results.filter((item) => !item.ok);
    const completedAt = Date.now();

    prewarmState = {
      startedAt,
      completedAt,
      durationMs: completedAt - startedAt,
      status: failed.length ? 'partial' : 'ready',
      mode: prewarmMode,
      backgroundWarmTriggered: Boolean(backgroundWarm),
      error: failed.map((item) => `${item.label}: ${item.error}`).join(' | '),
      promise: null,
      results,
    };

    if (process.env.ACI_CORE_PREWARM_LOG !== 'false') {
      const summary = results
        .map((item) => `${item.label}:${item.ok ? 'ok' : 'failed'}`)
        .join(', ');

      console.log(
        `[ACI Core] prewarm ${prewarmState.status} in ${prewarmState.durationMs}ms mode=${prewarmMode} background=${Boolean(backgroundWarm)} (${summary})`,
      );

      results.forEach((item) => {
        if (Number(item.durationMs || 0) > ACI_CORE_PREWARM_WARN_MS) {
          console.warn(
            `[ACI Core] slow prewarm step ${item.label}: ${item.durationMs}ms`,
          );
        }
      });

      if (prewarmState.durationMs > ACI_CORE_PREWARM_WARN_MS) {
        console.warn(
          `[ACI Core] slow total prewarm: ${prewarmState.durationMs}ms. Target is < ${ACI_CORE_PREWARM_WARN_MS}ms.`,
        );
      }

      if (prewarmState.error) {
        console.warn(`[ACI Core] prewarm warnings: ${prewarmState.error}`);
      }
    }

    return prewarmState;
  })();

  return prewarmState.promise;
}

const triggerAciCoreRuntimePrewarm = ({ force = false, mode = null, background = null } = {}) => {
  if (!isMongoReady()) return null;

  const prewarmMode = normalizePrewarmMode(mode);

  if (shouldSkipPrewarm({ force }) && prewarmState.mode === prewarmMode) {
    return prewarmState.promise || null;
  }

  return prewarmAciCoreRuntime({ force, mode: prewarmMode, background }).catch((error) => {
    prewarmState = {
      ...prewarmState,
      completedAt: Date.now(),
      durationMs: Date.now() - (prewarmState.startedAt || Date.now()),
      status: 'failed',
      mode: prewarmMode,
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
  normalizePrewarmMode,
};

export default prewarmAciCoreRuntime;
