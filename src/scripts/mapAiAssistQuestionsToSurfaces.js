import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import mongoose from "mongoose";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";

dotenv.config();

const regressionFile =
  process.argv[2] || "src/scripts/testAiPlanner.fullRegression.js";

const outputFile =
  process.argv[3] || "/tmp/aci-assist-186-question-surface-map.md";

const source = fs.readFileSync(regressionFile, "utf8");

const uniq = (arr) => [...new Set(arr.filter(Boolean).map((v) => String(v).trim()))];

function cleanQuestion(value = "") {
  return String(value)
    .replace(/\\n/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractQuestions(text) {
  const candidates = [];

  // Matches objects like { id: "...", message: "..." } or { message: "..." }
  const patterns = [
    /(?:message|query|input|prompt)\s*:\s*["'`]([^"'`]{2,300})["'`]/g,
    /\bq\s*:\s*["'`]([^"'`]{2,300})["'`]/g,
  ];

  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(text))) {
      const question = cleanQuestion(match[1]);

      if (
        question &&
        !question.includes("Expected") &&
        !question.includes("function") &&
        !question.includes("import ") &&
        !question.includes("console.")
      ) {
        candidates.push(question);
      }
    }
  }

  return uniq(candidates);
}

function normalizeResponse(response = {}) {
  const root =
    response.summary ||
    response.response?.summary ||
    response.result?.summary ||
    response.data?.summary ||
    response;

  return {
    intent: root.intent || response.intent || "",
    mode: root.mode || "",
    displayMode: root.displayMode || "",
    canvasType:
      root.canvasType ||
      root.canvas_type ||
      response.canvasType ||
      response.canvas_type ||
      "",
    inlineType:
      root.inlineType ||
      root.inline_type ||
      response.inlineType ||
      response.inline_type ||
      "",
    title: root.title || response.title || "",
    answer: root.answer || response.answer || "",
    actions: root.actions || response.actions || [],
    leadingQuestions: root.leadingQuestions || response.leadingQuestions || [],
    oldSystemUsed: Boolean(root.oldSystemUsed || response.oldSystemUsed),
    plannerFallbackUsed: Boolean(root.plannerFallbackUsed || response.plannerFallbackUsed),
    contractValid:
      root.contractValid === undefined
        ? response.contractValid
        : root.contractValid,
    contractErrors: root.contractErrors || response.contractErrors || [],
  };
}

async function ask(question) {
  const sharedContext = {
    city: "new-delhi",
    selectedVehicle: null,
    source: "aci_surface_map",
  };

  const adminUser = {
    _id: "aci-surface-map-user",
    id: "aci-surface-map-user",
    name: "ACI Surface Map",
    role: "admin",
    email: "gaurav@acillp.com",
  };

  try {
    return await chatWithAgent({
      message: question,
      query: question,
      text: question,
      context: sharedContext,
      user: adminUser,
    });
  } catch (firstError) {
    try {
      return await chatWithAgent(question, sharedContext, adminUser);
    } catch (secondError) {
      throw firstError || secondError;
    }
  }
}

function surfaceName(value, fallback) {
  return value || fallback;
}

function pushGroup(map, key, item) {
  if (!map.has(key)) map.set(key, []);
  map.get(key).push(item);
}

function mdEscape(value = "") {
  return String(value).replace(/\|/g, "\\|").replace(/\n/g, " ");
}

async function main() {
  const questions = extractQuestions(source);

  if (!questions.length) {
    console.error(`No questions found in ${regressionFile}`);
    console.error("Open the regression file and confirm the field name used for questions.");
    process.exit(1);
  }

  console.log(`Found ${questions.length} questions in ${regressionFile}`);

  const mongoUri =
    process.env.MONGO_URI ||
    process.env.MONGODB_URI ||
    process.env.DATABASE_URL;

  if (mongoUri && mongoose.connection.readyState === 0) {
    await mongoose.connect(mongoUri);
    console.log("MongoDB connected");
  }

  const canvasGroups = new Map();
  const inlineGroups = new Map();
  const intentGroups = new Map();
  const failures = [];

  for (let i = 0; i < questions.length; i += 1) {
    const question = questions[i];
    process.stdout.write(`\r${i + 1}/${questions.length} ${question.slice(0, 70)}...`);

    try {
      const response = await ask(question);
      const summary = normalizeResponse(response);

      const item = {
        no: i + 1,
        question,
        ...summary,
      };

      const canvasKey = surfaceName(summary.canvasType, "chat_only_or_inline_only");
      const inlineKey = surfaceName(summary.inlineType, "no_inline_card");
      const intentKey = surfaceName(summary.intent, "unknown_intent");

      pushGroup(canvasGroups, canvasKey, item);
      pushGroup(inlineGroups, inlineKey, item);
      pushGroup(intentGroups, intentKey, item);
    } catch (error) {
      failures.push({
        no: i + 1,
        question,
        error: error?.message || String(error),
      });

      pushGroup(canvasGroups, "execution_failed", {
        no: i + 1,
        question,
        intent: "",
        canvasType: "execution_failed",
        inlineType: "",
        title: "",
        answer: error?.message || String(error),
      });
    }
  }

  console.log("\nDone.");

  const lines = [];

  lines.push("# ACI Assist V2 — 186 Question Surface Map");
  lines.push("");
  lines.push(`Generated from: \`${regressionFile}\``);
  lines.push(`Total extracted questions: **${questions.length}**`);
  lines.push(`Execution failures: **${failures.length}**`);
  lines.push("");

  lines.push("## Summary by Canvas");
  lines.push("");
  lines.push("| Canvas / Surface | Count |");
  lines.push("|---|---:|");
  [...canvasGroups.entries()]
    .sort((a, b) => b[1].length - a[1].length)
    .forEach(([key, items]) => {
      lines.push(`| ${mdEscape(key)} | ${items.length} |`);
    });
  lines.push("");

  lines.push("## Summary by Inline Type");
  lines.push("");
  lines.push("| Inline Type | Count |");
  lines.push("|---|---:|");
  [...inlineGroups.entries()]
    .sort((a, b) => b[1].length - a[1].length)
    .forEach(([key, items]) => {
      lines.push(`| ${mdEscape(key)} | ${items.length} |`);
    });
  lines.push("");

  lines.push("## Questions Grouped by Canvas");
  lines.push("");

  [...canvasGroups.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .forEach(([canvas, items]) => {
      lines.push(`### ${canvas} (${items.length})`);
      lines.push("");
      lines.push("| # | Question | Intent | Inline | Title |");
      lines.push("|---:|---|---|---|---|");

      items.forEach((item) => {
        lines.push(
          `| ${item.no} | ${mdEscape(item.question)} | ${mdEscape(item.intent)} | ${mdEscape(item.inlineType || "-")} | ${mdEscape(item.title || "-")} |`,
        );
      });

      lines.push("");
    });

  lines.push("## Questions Grouped by Inline Type");
  lines.push("");

  [...inlineGroups.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .forEach(([inline, items]) => {
      lines.push(`### ${inline} (${items.length})`);
      lines.push("");
      lines.push("| # | Question | Intent | Canvas | Title |");
      lines.push("|---:|---|---|---|---|");

      items.forEach((item) => {
        lines.push(
          `| ${item.no} | ${mdEscape(item.question)} | ${mdEscape(item.intent)} | ${mdEscape(item.canvasType || "-")} | ${mdEscape(item.title || "-")} |`,
        );
      });

      lines.push("");
    });

  lines.push("## Questions Grouped by Intent");
  lines.push("");

  [...intentGroups.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .forEach(([intent, items]) => {
      lines.push(`### ${intent} (${items.length})`);
      lines.push("");

      items.forEach((item) => {
        lines.push(
          `- **${item.no}.** ${item.question}  \n  Canvas: \`${item.canvasType || "-"}\` · Inline: \`${item.inlineType || "-"}\``,
        );
      });

      lines.push("");
    });

  if (failures.length) {
    lines.push("## Failures");
    lines.push("");

    failures.forEach((failure) => {
      lines.push(`- **${failure.no}.** ${failure.question}`);
      lines.push(`  - Error: ${failure.error}`);
    });

    lines.push("");
  }

  fs.writeFileSync(outputFile, lines.join("\n"));
  console.log(`Wrote ${outputFile}`);

  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
  }
}

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
  }
  process.exit(1);
});
