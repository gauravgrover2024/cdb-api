import fs from 'fs';
import os from 'os';
import path from 'path';
import { Worker } from 'worker_threads';
import { fileURLToPath } from 'url';

import {
  ACI_UNDERSTANDING_CORPUS_V1,
} from './corpus/aciUnderstandingCorpus.v1.js';

import {
  ACI_UNDERSTANDING_CORPUS_EXTENDED_V1,
} from './corpus/aciUnderstandingCorpus.extendedV1.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ALL_CASES = [
  ...ACI_UNDERSTANDING_CORPUS_V1,
  ...ACI_UNDERSTANDING_CORPUS_EXTENDED_V1,
];

const workerCount = Math.max(1, Math.min(
  Number(process.env.ACI_EVAL_WORKERS || 4),
  os.cpus().length,
  ALL_CASES.length,
));

const caseTimeoutMs = Number(process.env.ACI_EVAL_CASE_TIMEOUT_MS || 12000);
const reportDir = process.env.ACI_EVAL_REPORT_DIR || path.join(__dirname, 'reports');
const shard = process.env.ACI_EVAL_SHARD || null;

function applyShard(cases) {
  if (!shard) return cases;

  const match = String(shard).match(/^(\d+)\/(\d+)$/);
  if (!match) {
    throw new Error(`Invalid ACI_EVAL_SHARD value: ${shard}. Expected format "1/4".`);
  }

  const shardIndex = Number(match[1]);
  const shardTotal = Number(match[2]);

  if (shardIndex < 1 || shardIndex > shardTotal) {
    throw new Error(`Invalid ACI_EVAL_SHARD range: ${shard}`);
  }

  return cases.filter((_, index) => (index % shardTotal) === (shardIndex - 1));
}

function chunkItems(items, chunks) {
  const output = Array.from({ length: chunks }, () => []);
  items.forEach((item, index) => {
    output[index % chunks].push(item);
  });
  return output.filter((chunk) => chunk.length > 0);
}

function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[index];
}

function runWorker({ workerPath, cases }) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(workerPath, {
      workerData: {
        cases,
        caseTimeoutMs,
      },
    });

    worker.once('message', (message) => {
      resolve(message.results || []);
    });

    worker.once('error', reject);

    worker.once('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`ACI eval worker exited with code ${code}`));
      }
    });
  });
}

const startedAt = Date.now();
const selectedCases = applyShard(ALL_CASES);
const chunks = chunkItems(selectedCases, workerCount);
const workerPath = path.join(__dirname, 'workers', 'aciUnderstandingEval.worker.js');

fs.mkdirSync(reportDir, { recursive: true });

const workerResults = await Promise.all(
  chunks.map((cases) => runWorker({ workerPath, cases })),
);

const results = workerResults.flat();
const failed = results.filter((item) => !item.ok);
const durations = results.map((item) => item.durationMs).filter((value) => Number.isFinite(value));

const buckets = results.reduce((acc, item) => {
  acc[item.bucket] = (acc[item.bucket] || 0) + 1;
  return acc;
}, {});

const summary = {
  suite: 'ACI Understanding Engine worker eval',
  ok: failed.length === 0,
  total: results.length,
  failed: failed.length,
  passed: results.length - failed.length,
  sourceTotal: ALL_CASES.length,
  shard,
  workers: chunks.length,
  configuredWorkers: workerCount,
  caseTimeoutMs,
  durationMs: Date.now() - startedAt,
  latency: {
    minMs: durations.length ? Math.min(...durations) : null,
    p50Ms: percentile(durations, 50),
    p95Ms: percentile(durations, 95),
    p99Ms: percentile(durations, 99),
    maxMs: durations.length ? Math.max(...durations) : null,
  },
  buckets,
  failedIds: failed.map((item) => item.id),
};

const report = {
  ...summary,
  results,
};

const reportPath = path.join(
  reportDir,
  `aci_understanding_worker_${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
);

fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);

console.log(JSON.stringify({
  ...summary,
  reportPath,
}, null, 2));

if (!summary.ok) {
  process.exit(1);
}
