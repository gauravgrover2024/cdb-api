/**
 * ACI Assist V2 Pipeline Debugger
 * --------------------------------
 * Purpose:
 * For one or more queries, print each backend layer separately:
 *
 * 1. Deterministic router output
 * 2. Intent parser output, if callable export is found
 * 3. Planner output, if callable export is found
 * 4. Service/final response output, if callable export is found
 * 5. Contract health summary
 *
 * This script is read-only. It does not modify DB or code.
 */

import dotenv from "dotenv";
import mongoose from "mongoose";
import util from "node:util";

dotenv.config();

const inspect = (value, depth = 8) =>
  util.inspect(value, {
    depth,
    colors: true,
    maxArrayLength: 40,
    maxStringLength: 5000,
    breakLength: 120,
  });

const line = (title = "") => {
  console.log("\n" + "=".repeat(110));
  if (title) console.log(title);
  console.log("=".repeat(110));
};

const section = (title = "") => {
  console.log("\n" + "-".repeat(90));
  console.log(title);
  console.log("-".repeat(90));
};

const compact = (value) => {
  if (!value || typeof value !== "object") return value;

  return JSON.parse(
    JSON.stringify(value, (_key, val) => {
      if (typeof val === "string" && val.length > 700) {
        return `${val.slice(0, 700)}... [truncated ${val.length} chars]`;
      }
      return val;
    }),
  );
};

const getMongoUri = () =>
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

async function connectMongo() {
  const uri = getMongoUri();

  if (!uri) {
    console.warn("⚠️  No Mongo URI found in env. Continuing without explicit mongoose connect.");
    return;
  }

  if (mongoose.connection.readyState === 1) return;

  await mongoose.connect(uri);
  console.log("✅ MongoDB connected");
}

async function safeImport(label, path) {
  try {
    const mod = await import(path);
    return mod;
  } catch (error) {
    console.log(`⚠️  Could not import ${label}: ${path}`);
    console.log(`   ${error.message}`);
    return null;
  }
}

const pickCallable = (mod, candidates = []) => {
  if (!mod) return null;

  for (const name of candidates) {
    if (typeof mod[name] === "function") {
      return { name, fn: mod[name] };
    }
  }

  if (typeof mod.default === "function") {
    return { name: "default", fn: mod.default };
  }

  return null;
};

const listExports = (label, mod) => {
  if (!mod) {
    console.log(`${label}: import failed`);
    return;
  }

  console.log(`${label} exports:`, Object.keys(mod).sort().join(", ") || "(none)");
};

const safeCall = async (label, fn, callShapes = []) => {
  section(label);

  if (!fn) {
    console.log("SKIPPED: no callable export found.");
    return { ok: false, skipped: true, result: null, error: null };
  }

  for (const shape of callShapes) {
    try {
      console.log(`Trying shape: ${shape.name}`);
      const result = await fn.fn(...shape.args);
      console.log(`✅ Success via ${fn.name} / ${shape.name}`);
      console.log(inspect(compact(result), 8));
      return {
        ok: true,
        skipped: false,
        result,
        error: null,
        callableName: fn.name,
        shapeName: shape.name,
      };
    } catch (error) {
      console.log(`❌ Failed shape: ${shape.name}`);
      console.log(`   ${error.message}`);
    }
  }

  return { ok: false, skipped: false, result: null, error: "All call shapes failed." };
};

const getNested = (obj, paths = []) => {
  for (const path of paths) {
    const parts = path.split(".");
    let current = obj;

    for (const part of parts) {
      current = current?.[part];
    }

    if (current !== undefined && current !== null && current !== "") {
      return current;
    }
  }

  return "";
};

const asArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
};

const summarizeFinalResponse = (response = {}) => {
  const widgets = asArray(response.widgets);
  const widget = response.widget || widgets[0] || null;
  const runtimeMeta = asArray(response.runtimeResultsMeta || response.meta?.runtimeResultsMeta);
  const sourceTransparency =
    response.sourceTransparency ||
    widget?.sourceTransparency ||
    response.meta?.sourceTransparency ||
    null;

  const rows =
    asArray(response.rows).length ||
    asArray(response.items).length ||
    asArray(widget?.rows).length ||
    asArray(widget?.items).length ||
    asArray(widget?.features).length ||
    asArray(response.features).length;

  const contextPatch = response.contextPatch || {};
  const contextSnapshot = response.contextSnapshot || {};

  return {
    intent: response.intent || response.detectedIntent || response.summary?.intent || "",
    mode: response.mode || response.summary?.mode || "",
    displayMode: response.displayMode || response.summary?.displayMode || "",
    canvasType:
      response.canvasType ||
      widget?.canvasType ||
      response.summary?.canvasType ||
      "",
    inlineType: response.inlineType || response.summary?.inlineType || "",
    title: response.title || response.summary?.title || "",
    answer: response.answer || response.message || response.summary?.answer || "",
    hasWidget: Boolean(widget),
    widgetsCount: widgets.length,
    rowsOrFeaturesCount: rows,
    actionsCount: asArray(response.actions || response.summary?.actions).length,
    leadingQuestionsCount: asArray(response.leadingQuestions || response.summary?.leadingQuestions).length,
    conversationSuggestionsCount: asArray(response.conversationSuggestions).length,
    hasContextPatch: Boolean(Object.keys(contextPatch).length),
    hasContextSnapshot: Boolean(Object.keys(contextSnapshot).length),
    contextPatch: {
      anchorMake: contextPatch.anchorMake || "",
      anchorModel: contextPatch.anchorModel || "",
      anchorVariant: contextPatch.anchorVariant || "",
      anchorCity: contextPatch.anchorCity || "",
      selectedVehicle: contextPatch.selectedVehicle
        ? {
            make: contextPatch.selectedVehicle.make || contextPatch.selectedVehicle.brand || "",
            model: contextPatch.selectedVehicle.model || "",
            variant:
              contextPatch.selectedVehicle.variant ||
              contextPatch.selectedVehicle.selectedVariant ||
              "",
            city: contextPatch.selectedVehicle.city || contextPatch.selectedVehicle.citySlug || "",
          }
        : null,
    },
    contextSnapshot: {
      make: contextSnapshot.make || contextSnapshot.anchorMake || "",
      model: contextSnapshot.model || contextSnapshot.anchorModel || "",
      variant: contextSnapshot.variant || contextSnapshot.anchorVariant || "",
      city: contextSnapshot.city || contextSnapshot.anchorCity || "",
    },
    runtimeResultsMeta: runtimeMeta.map((item) => ({
      tool: item.tool || "",
      matched: item.matched ?? item.matchedCount ?? "",
      source: item.source || "",
      modulesChecked: item.modulesChecked || [],
      error: item.error || "",
    })),
    sourceTransparency,
    contractFlags: {
      canvasButNoWidget: Boolean(
        (response.canvasType || widget?.canvasType) && !widget && !widgets.length,
      ),
      canvasButNoContextSnapshot: Boolean(
        (response.canvasType || widget?.canvasType) && !Object.keys(contextSnapshot).length,
      ),
      noRuntimeMeta: runtimeMeta.length === 0,
      noSourceTransparency: !sourceTransparency,
    },
  };
};

const buildContext = () => ({
  city: "new-delhi",
  anchorCity: "new-delhi",
  selectedVehicle: null,
  entities: {},
  modelHints: [
    "verna",
    "elevate",
    "city",
    "creta",
    "venue",
    "venue n line",
    "seltos",
    "thar",
  ],
});

async function debugOneQuery(query, modules) {
  line(`QUERY: ${query}`);

  const context = buildContext();

  section("0) Module exports");
  listExports("intentRouter", modules.intentRouter);
  listExports("intentParser", modules.intentParser);
  listExports("planner", modules.planner);
  listExports("executor", modules.executor);
  listExports("service", modules.service);

  section("1) Deterministic router");
  let routed = null;

  if (modules.intentRouter?.routeAiAgentIntent) {
    routed = modules.intentRouter.routeAiAgentIntent({
      message: query,
      context,
      filters: {},
      selectedEntity: null,
    });

    console.log(inspect(compact(routed), 8));
  } else {
    console.log("SKIPPED: routeAiAgentIntent export not found.");
  }

  const parserFn = pickCallable(modules.intentParser, [
    "parseAiAgentIntent",
    "parseIntent",
    "parseUserIntent",
    "parseMessageIntent",
    "parseAiIntent",
    "default",
  ]);

  const plannerFn = pickCallable(modules.planner, [
    "createPlan",
    "buildPlan",
    "planAiAgentResponse",
    "planAiResponse",
    "planQuery",
    "runPlanner",
    "default",
  ]);

  const executorFn = pickCallable(modules.executor, [
    "executePlan",
    "executeAiPlan",
    "runExecutor",
    "executeAiAgentPlan",
    "default",
  ]);

  const serviceFn = pickCallable(modules.service, [
    "runAiAgent",
    "handleAiAgentMessage",
    "processAiAgentMessage",
    "chatWithAiAgent",
    "askAiAgent",
    "askAciAssist",
    "askAciAssistV2",
    "runAciAssist",
    "default",
  ]);

  const parserResult = await safeCall("2) Intent parser / Gemini wrapper, if available", parserFn, [
    {
      name: "object { message, context }",
      args: [{ message: query, context, filters: {}, selectedEntity: null }],
    },
    {
      name: "string + context",
      args: [query, context],
    },
    {
      name: "string only",
      args: [query],
    },
  ]);

  const parsed = parserResult.result || routed || null;

  const plannerResult = await safeCall("3) Planner, if available", plannerFn, [
    {
      name: "object { message, parsed, intent, entities, context }",
      args: [
        {
          message: query,
          parsed,
          intent: parsed?.intent,
          entities: parsed?.entities || {},
          context,
          filters: {},
        },
      ],
    },
    {
      name: "message + parsed + context",
      args: [query, parsed, context],
    },
    {
      name: "parsed object only",
      args: [parsed],
    },
  ]);

  const plan = plannerResult.result || null;

  await safeCall("4) Executor, if available", executorFn, [
    {
      name: "object { message, plan, parsed, context }",
      args: [
        {
          message: query,
          plan,
          parsed,
          context,
          filters: {},
        },
      ],
    },
    {
      name: "plan + context",
      args: [plan, context],
    },
    {
      name: "plan only",
      args: [plan],
    },
  ]);

  const serviceResult = await safeCall("5) Final service response", serviceFn, [
    {
      name: "object { message, context }",
      args: [
        {
          message: query,
          context,
          filters: {},
          selectedEntity: null,
          user: { _id: "debug-user", name: "Debug User" },
        },
      ],
    },
    {
      name: "object { query, context }",
      args: [
        {
          query,
          context,
          filters: {},
          selectedEntity: null,
          user: { _id: "debug-user", name: "Debug User" },
        },
      ],
    },
    {
      name: "string + context",
      args: [query, context],
    },
    {
      name: "string only",
      args: [query],
    },
  ]);

  section("6) Final response health summary");

  if (!serviceResult.result) {
    console.log("No final service response captured.");
  } else {
    const summary = summarizeFinalResponse(serviceResult.result);
    console.log(inspect(summary, 10));

    const likelyLayer = [];

    const routerIntent = routed?.intent || "";
    const parsedIntent = getNested(parsed, ["intent", "primaryIntent", "detectedIntent"]);
    const finalIntent = summary.intent;

    if (routerIntent && parsedIntent && routerIntent !== parsedIntent) {
      likelyLayer.push(`Intent changed between router (${routerIntent}) and parser/Gemini (${parsedIntent})`);
    }

    if (parsedIntent && finalIntent && parsedIntent !== finalIntent) {
      likelyLayer.push(`Intent changed between parser (${parsedIntent}) and final response (${finalIntent})`);
    }

    if (summary.contractFlags.canvasButNoWidget) {
      likelyLayer.push("Response contract issue: canvas response has no widget/widgets");
    }

    if (summary.contractFlags.canvasButNoContextSnapshot) {
      likelyLayer.push("Response contract issue: canvas response has no contextSnapshot");
    }

    if (summary.contractFlags.noRuntimeMeta) {
      likelyLayer.push("Executor/contract issue: runtimeResultsMeta missing");
    }

    if (summary.contractFlags.noSourceTransparency) {
      likelyLayer.push("Tool/contract issue: sourceTransparency missing");
    }

    if (!summary.rowsOrFeaturesCount && summary.canvasType) {
      likelyLayer.push("Tool/query issue or widget construction issue: canvas has zero rows/features");
    }

    console.log("\nLikely layer signals:");
    if (likelyLayer.length) {
      likelyLayer.forEach((item) => console.log(`- ${item}`));
    } else {
      console.log("- No obvious pipeline break detected by this script.");
    }
  }

  section("7) Compact comparison");
  console.log(
    inspect(
      {
        query,
        routerIntent: routed?.intent || "",
        routerWidgetType: routed?.widgetType || "",
        routerEntities: routed?.entities || {},
        parserCallable: parserFn?.name || "",
        plannerCallable: plannerFn?.name || "",
        executorCallable: executorFn?.name || "",
        serviceCallable: serviceFn?.name || "",
      },
      6,
    ),
  );
}

async function main() {
  const queries = process.argv.slice(2);

  const finalQueries = queries.length
    ? queries
    : [
        "Verna pricelist",
        "Show colors of Verna",
        "Show features of Verna",
        "Does Verna SX have sunroof?",
        "Which Verna variants have sunroof?",
      ];

  await connectMongo();

  const modules = {
    intentRouter: await safeImport(
      "intentRouter",
      "../services/aiAgent/aiAgent.intentRouter.js",
    ),
    intentParser: await safeImport(
      "intentParser",
      "../services/aiAgent/aiAgent.intentParser.js",
    ),
    planner: await safeImport(
      "planner",
      "../services/aiAgent/aiAgent.planner.js",
    ),
    executor: await safeImport(
      "executor",
      "../services/aiAgent/aiAgent.executor.js",
    ),
    service: await safeImport(
      "service",
      "../services/aiAgent/aiAgent.service.js",
    ),
  };

  for (const query of finalQueries) {
    await debugOneQuery(query, modules);
  }

  if (mongoose.connection.readyState === 1) {
    await mongoose.disconnect();
  }
}

main().catch(async (error) => {
  console.error("FATAL DEBUG SCRIPT ERROR:");
  console.error(error);
  if (mongoose.connection.readyState === 1) {
    await mongoose.disconnect();
  }
  process.exit(1);
});
