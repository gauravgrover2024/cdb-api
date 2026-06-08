#!/usr/bin/env node

const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");

const JSON_START = "__ACI_DEEP_AUDIT_JSON_START__";
const JSON_END = "__ACI_DEEP_AUDIT_JSON_END__";

const timestamp = () => {
  const date = new Date();
  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    pad(date.getMonth() + 1),
    pad(date.getDate()),
    "_",
    pad(date.getHours()),
    pad(date.getMinutes()),
    pad(date.getSeconds()),
  ].join("");
};

const readTail = (filePath = "", maxBytes = 4000) => {
  try {
    const stat = fs.statSync(filePath);
    const start = Math.max(0, stat.size - maxBytes);
    const fd = fs.openSync(filePath, "r");
    const buffer = Buffer.alloc(stat.size - start);
    fs.readSync(fd, buffer, 0, buffer.length, start);
    fs.closeSync(fd);
    return buffer.toString("utf8");
  } catch {
    return "";
  }
};

const run = ({ command, args = [], env = {}, outputFile = "" } = {}) =>
  new Promise((resolve) => {
    let outputFd = null;

    const spawnOptions = {
      shell: false,
      env: { ...process.env, ...env },
    };

    if (outputFile) {
      outputFd = fs.openSync(outputFile, "w");
      spawnOptions.stdio = ["ignore", outputFd, outputFd];
    }

    const child = spawn(command, args, spawnOptions);

    let stdout = "";
    let stderr = "";

    if (!outputFile) {
      child.stdout.on("data", (chunk) => {
        const text = chunk.toString();
        stdout += text;
        process.stdout.write(text);
      });

      child.stderr.on("data", (chunk) => {
        const text = chunk.toString();
        stderr += text;
        process.stderr.write(text);
      });
    }

    const closeOutputFd = () => {
      if (outputFd !== null) {
        try {
          fs.closeSync(outputFd);
        } catch {
          // noop
        }
        outputFd = null;
      }
    };

    child.on("error", (error) => {
      closeOutputFd();
      resolve({
        ok: false,
        status: 1,
        output: outputFile
          ? `${readTail(outputFile)}\n${error?.stack || error?.message || String(error)}`
          : `${stdout}${stderr}\n${error?.stack || error?.message || String(error)}`,
      });
    });

    child.on("close", (code, signal) => {
      closeOutputFd();
      const output = outputFile
        ? readTail(outputFile)
        : `${stdout}${stderr}${signal ? `\nProcess terminated by signal: ${signal}` : ""}`;

      resolve({
        ok: code === 0,
        status: Number.isInteger(code) ? code : 1,
        signal: signal || "",
        output,
      });
    });
  });

const hasCompleteDeepAuditJson = (filePath = "") => {
  const raw = fs.readFileSync(filePath, "utf8");
  const start = raw.lastIndexOf(JSON_START);
  if (start < 0) return false;
  return raw.indexOf(JSON_END, start + JSON_START.length) >= 0;
};

async function main() {
  const logPath =
    process.env.ACI_NO_DATA_BASELINE_SOURCE_LOG ||
    path.join("/tmp", `aci_full_185_no_data_freeze_${timestamp()}.log`);

  console.log(JSON.stringify({
    suite: "ACI No-Data Baseline Freeze Gate v1",
    step: "run_full_buyer_deep_audit",
    logPath,
  }));

  const deepAudit = await run({
    command: "npm",
    args: ["run", "aci:buyer-answer:deep-audit"],
    env: {
      ACI_DEEP_AUDIT_WORKERS: process.env.ACI_DEEP_AUDIT_WORKERS || "10",
    },
    outputFile: logPath,
  });

  if (!deepAudit.ok) {
    console.error(deepAudit.output.slice(-4000));
    console.log(JSON.stringify({
      suite: "ACI No-Data Baseline Freeze Gate v1",
      ok: false,
      failedStep: "deep_audit",
      logPath,
      status: deepAudit.status,
    }, null, 2));
    return false;
  }

  if (!hasCompleteDeepAuditJson(logPath)) {
    console.log(JSON.stringify({
      suite: "ACI No-Data Baseline Freeze Gate v1",
      ok: false,
      failedStep: "deep_audit_json_markers",
      logPath,
    }, null, 2));
    return false;
  }

  console.log(JSON.stringify({
    suite: "ACI No-Data Baseline Freeze Gate v1",
    step: "run_no_data_baseline",
    logPath,
  }));

  const baseline = await run({
    command: "npm",
    args: ["run", "aci:no-data:baseline:audit", "--", logPath],
    env: {
      ACI_NO_DATA_BASELINE_STRICT: "1",
    },
  });

  const ok = Boolean(baseline.ok);

  console.log(JSON.stringify({
    suite: "ACI No-Data Baseline Freeze Gate v1",
    ok,
    failedStep: ok ? "" : "no_data_baseline",
    logPath,
    status: baseline.status,
  }, null, 2));

  return ok;
}

main()
  .then((ok) => process.exit(ok ? 0 : 1))
  .catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
