#!/usr/bin/env node

const { spawn } = require('child_process');

const DEFAULT_GATE_WORKERS = Object.freeze({
  phase0: 1,
  score: 4,
  scoreFull: 3,
  similar: 3,
  similarFull: 2,
});

function getGateWorkers(gateName) {
  const override = Number(process.env.ACI_DECISION_GATE_WORKERS || 0);
  if (Number.isFinite(override) && override > 0) return Math.max(1, override);
  return Math.max(1, DEFAULT_GATE_WORKERS[gateName] || 4);
}
const SLOW_TASK_WARN_MS = Math.max(1, Number(process.env.ACI_DECISION_GATE_SLOW_TASK_WARN_MS || 30000));

const GATES = {
  score: [
    { id: 'score-user-query-smoke', cmd: 'npm run -s aci:decision:score-user-query:smoke' },
    { id: 'score-tool-language-audit', cmd: 'npm run -s aci:decision:score-tool-language:audit' },
    { id: 'score-service-language-audit-fast', cmd: 'ACI_SCORE_LANGUAGE_SAMPLE_LIMIT=12 npm run -s aci:decision:score-language:audit' },
    { id: 'score-output-fixture', cmd: 'npm run -s aci:decision:score-output-fixture:eval' },
    { id: 'module-policy-eval', cmd: 'npm run -s aci:decision:module-policy:eval' },
    { id: 'market-judgement-audit', cmd: 'npm run -s aci:decision:market-judgement:audit' },
  ],
  scoreFull: [
    { id: 'score-user-query-smoke', cmd: 'npm run -s aci:decision:score-user-query:smoke' },
    { id: 'score-tool-language-audit', cmd: 'npm run -s aci:decision:score-tool-language:audit' },
    { id: 'score-service-language-audit-full', cmd: 'ACI_SCORE_LANGUAGE_SAMPLE_LIMIT=50 npm run -s aci:decision:score-language:audit' },
    { id: 'score-output-fixture', cmd: 'npm run -s aci:decision:score-output-fixture:eval' },
    { id: 'module-policy-eval', cmd: 'npm run -s aci:decision:module-policy:eval' },
    { id: 'market-judgement-audit', cmd: 'npm run -s aci:decision:market-judgement:audit' },
  ],
  similar: [
    { id: 'similar-relation-mode-eval-fast', cmd: 'npm run -s aci:decision:similar-relation-mode:eval:fast' },
    { id: 'similar-filter-audit-fast', cmd: 'npm run -s aci:decision:similar-filter:audit:fast' },
    { id: 'similar-output-fixture-fast', cmd: 'npm run -s aci:decision:similar-output-fixture:eval:fast' },
    { id: 'similar-graph-smoke-fast', cmd: 'npm run -s aci:decision:similar-graph:smoke:fast' },
    { id: 'module-policy-eval', cmd: 'npm run -s aci:decision:module-policy:eval' },
    { id: 'market-judgement-audit', cmd: 'npm run -s aci:decision:market-judgement:audit' },
  ],
  similarFull: [
    { id: 'similar-relation-mode-eval-full', cmd: 'npm run -s aci:decision:similar-relation-mode:eval:full' },
    { id: 'similar-filter-audit-full', cmd: 'npm run -s aci:decision:similar-filter:audit:full' },
    { id: 'similar-output-fixture-full', cmd: 'npm run -s aci:decision:similar-output-fixture:eval:full' },
    { id: 'similar-graph-smoke-full', cmd: 'npm run -s aci:decision:similar-graph:smoke:full' },
    { id: 'module-policy-eval', cmd: 'npm run -s aci:decision:module-policy:eval' },
    { id: 'market-judgement-audit', cmd: 'npm run -s aci:decision:market-judgement:audit' },
  ],
  phase0: [
    { id: 'policy-smoke', cmd: 'npm run -s aci:decision:policy:smoke' },
    { id: 'policy-eval', cmd: 'npm run -s aci:decision:policy:eval' },
    { id: 'provenance-eval', cmd: 'npm run -s aci:decision:provenance:eval' },
    { id: 'degraded-mode-eval', cmd: 'npm run -s aci:decision:degraded-mode:eval' },
    { id: 'evidence-freshness-audit', cmd: 'npm run -s aci:decision:evidence-freshness:audit' },
    { id: 'decision-runtime-envelope-smoke-fast', cmd: 'npm run -s aci:decision:runtime-envelope:smoke:fast' },
    { id: 'decision-final-eligibility-smoke-fast', cmd: 'npm run -s aci:decision:final-eligibility:smoke:fast' },
    { id: 'decision-language-composer-smoke', cmd: 'npm run -s aci:decision:language-composer:smoke' },
    { id: 'decision-recovery-no-data-smoke', cmd: 'npm run -s aci:decision:recovery-no-data:smoke' },
    { id: 'decision-final-blocked-readiness-smoke', cmd: 'npm run -s aci:decision:final-blocked-readiness:smoke' },
    { id: 'decision-phase4-closure-smoke', cmd: 'npm run -s aci:decision:closure:smoke' },
    { id: 'buyer-decision-input-contract-smoke', cmd: 'npm run -s aci:decision:buyer-input:smoke' },
    { id: 'buyer-context-extraction-smoke', cmd: 'npm run -s aci:decision:buyer-context:smoke' },
    { id: 'module-policy-eval', cmd: 'npm run -s aci:decision:module-policy:eval' },
    { id: 'market-judgement-audit', cmd: 'npm run -s aci:decision:market-judgement:audit' },
    { id: 'score-output-fixture', cmd: 'npm run -s aci:decision:score-output-fixture:eval' },
    { id: 'similar-output-fixture-fast', cmd: 'npm run -s aci:decision:similar-output-fixture:eval:fast' },
    { id: 'similar-filter-audit-fast', cmd: 'npm run -s aci:decision:similar-filter:audit:fast' },
    { id: 'similar-relation-mode-eval-fast', cmd: 'npm run -s aci:decision:similar-relation-mode:eval:fast' },
    { id: 'similar-graph-smoke-fast', cmd: 'npm run -s aci:decision:similar-graph:smoke:fast' },
  ],
};

function tail(text = '', maxLines = 80) {
  return String(text).split('\n').slice(-maxLines).join('\n');
}

function runTask(task) {
  const startedAt = Date.now();

  return new Promise((resolve) => {
    const child = spawn(task.cmd, {
      shell: true,
      cwd: process.cwd(),
      env: process.env,
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('close', (code) => {
      resolve({
        id: task.id,
        cmd: task.cmd,
        ok: code === 0,
        code,
        durationMs: Date.now() - startedAt,
        stdout,
        stderr,
      });
    });
  });
}

async function runGate(gateName) {
  const tasks = GATES[gateName];
  if (!tasks) {
    throw new Error(`Unknown gate "${gateName}". Available gates: ${Object.keys(GATES).join(', ')}`);
  }

  const queue = [...tasks];
  const results = [];
  let active = 0;

  const maxWorkers = getGateWorkers(gateName);

  console.log(JSON.stringify({
    suite: 'ACI Decision Parallel Gate v1',
    gate: gateName,
    workers: maxWorkers,
    taskCount: tasks.length,
    taskIds: tasks.map((task) => task.id),
  }, null, 2));

  await new Promise((resolve) => {
    const pump = () => {
      while (active < maxWorkers && queue.length > 0) {
        const task = queue.shift();
        active += 1;
        console.log(`▶ ${task.id}`);

        runTask(task).then((result) => {
          active -= 1;
          results.push(result);
          console.log(`${result.ok ? '✅' : '❌'} ${result.id} ${result.durationMs}ms`);
          pump();
        });
      }

      if (active === 0 && queue.length === 0) resolve();
    };

    pump();
  });

  const failed = results.filter((result) => !result.ok);
  const summary = {
    suite: 'ACI Decision Parallel Gate v1',
    gate: gateName,
    ok: failed.length === 0,
    workers: maxWorkers,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((result) => result.id),
    durationsMs: Object.fromEntries(results.map((result) => [result.id, result.durationMs])),
    slowTasks: results
      .filter((result) => result.durationMs >= SLOW_TASK_WARN_MS)
      .map((result) => ({ id: result.id, durationMs: result.durationMs, warnAtMs: SLOW_TASK_WARN_MS })),
  };

  console.log('\n===== PARALLEL GATE SUMMARY =====');
  console.log(JSON.stringify(summary, null, 2));

  if (summary.slowTasks.length > 0) {
    console.log('\n===== SLOW TASK WARNINGS =====');
    for (const slowTask of summary.slowTasks) {
      console.log(`⚠️ ${slowTask.id} took ${slowTask.durationMs}ms (warnAt ${slowTask.warnAtMs}ms)`);
    }
  }

  if (failed.length > 0) {
    console.log('\n===== FAILURE OUTPUT TAILS =====');
    for (const failure of failed) {
      console.log(`\n--- ${failure.id} :: ${failure.cmd} ---`);
      console.log(tail(failure.stdout));
      if (failure.stderr) {
        console.log('\n[stderr]');
        console.log(tail(failure.stderr));
      }
    }
    process.exit(1);
  }
}

runGate(process.argv[2] || 'score').catch((error) => {
  console.error(error);
  process.exit(1);
});
