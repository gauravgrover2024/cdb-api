import {
  renderAciLanguageText,
} from "../../../aciCore/language/aciAnswerLanguageComposer.js";
import mongoose from "mongoose";
import { createNewCarsToolStub } from "./_toolStub.js";
import { NEW_CAR_CANVAS_TYPES } from "./shared/canvasContracts.js";

const TOOL_NAME = "vehicle_similar";
const GRAPH_COLLECTION =
  process.env.ACI_SIMILAR_MODEL_GRAPH_COLLECTION || "aci_vehicle_similar_model_graph_v1";
const GRAPH_VERSION = "similar_model_graph_v1";

const fallbackSimilarTool = createNewCarsToolStub({
  toolName: TOOL_NAME,
  canvasType: NEW_CAR_CANVAS_TYPES.SIMILAR,
});

const normalizeSearchText = (value = "") =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeKey = (value = "") =>
  normalizeSearchText(value).replace(/\s+/g, "-");

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const numberValue = (value) => {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : 0;
};

const midpoint = (min = 0, max = 0) => {
  const low = numberValue(min);
  const high = numberValue(max);
  if (low && high) return (low + high) / 2;
  return low || high || 0;
};

const getDb = (db) => {
  if (db) return db;
  if (mongoose.connection?.readyState === 1 && mongoose.connection?.db) {
    return mongoose.connection.db;
  }
  return null;
};

const getToolInput = (toolPlan = {}) =>
  toolPlan.input || toolPlan.args || toolPlan.params || {};

const includesAny = (text = "", terms = []) =>
  terms.some((term) => String(text || "").includes(term));

const getRequestedMode = (request = {}) => {
  const message = typeof request === "string" ? request : String(request?.message || "");
  const toolPlan = request && typeof request === "object" && !Array.isArray(request)
    ? request.toolPlan || {}
    : {};
  const input = getToolInput(toolPlan);

  const modeText = [
    message,
    input.mode,
    input.intent,
    input.relationType,
    input.fuelType,
    input.fuel,
    toolPlan.mode,
    toolPlan.intent,
    toolPlan.primaryTask,
    toolPlan.filters?.mode,
    toolPlan.filters?.intent,
    toolPlan.filters?.fuelType,
    toolPlan.filters?.fuel,
    toolPlan.entities?.fuelType,
    toolPlan.entities?.fuel,
    JSON.stringify(toolPlan.requestedFacts || {}),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (includesAny(modeText, [" ev", "ev ", "electric", "battery", "powertrain shift", "powertrain_shift"])) {
    return "powertrain_shift";
  }

  if (includesAny(modeText, ["cheaper", "lower price", "less expensive", "budget alternative", "step down", "step-down", "cheaper_step_down"])) {
    return "cheaper";
  }

  if (includesAny(modeText, ["premium", "upgrade", "step up", "step-up", "higher segment", "premium_step_up"])) {
    return "premium";
  }

  return "default";
};

const similarDecisionLanguageText = (templateKey = "", input = {}) =>
  renderAciLanguageText(templateKey, input, {
    seed: ["vehicle_similar", templateKey, input.mode, input.anchorName].filter(Boolean).join("|"),
  });

const createGuardrail = () => ({
  canUseForFinalRecommendation: false,
  reason: similarDecisionLanguageText("decision_similar_graph_guardrail_reason", {
    mode: "guardrail",
  }),
});

const getModelTextCandidates = ({ toolPlan = {}, context = {}, userMessage = "" } = {}) => {
  const input = getToolInput(toolPlan);
  const entities = toolPlan.entities || {};
  const filters = toolPlan.filters || {};
  const selectedVehicle = context.selectedVehicle || context.vehicle || {};

  return [
    input.modelKey,
    input.model,
    toolPlan.modelKey,
    toolPlan.model,
    entities.modelKey,
    entities.model,
    entities.primaryModel,
    filters.modelKey,
    filters.model,
    selectedVehicle.modelKey,
    selectedVehicle.model,
    context.anchorModel,
    userMessage,
  ].filter(Boolean);
};

const resolveAnchorGraph = async ({ col, toolPlan = {}, context = {}, userMessage = "" } = {}) => {
  const candidates = getModelTextCandidates({ toolPlan, context, userMessage });
  const directKeys = candidates
    .map(normalizeKey)
    .filter(Boolean)
    .flatMap((key) => [key, key.replace(/-/g, "_")]);

  for (const key of directKeys) {
    const exact = await col.findOne({
      graphVersion: GRAPH_VERSION,
      $or: [
        { "anchor.modelKey": key },
        { "anchor.displayName": new RegExp(`(^|\\b)${key.replace(/[-_]+/g, " ")}(\\b|$)`, "i") },
      ],
    });
    if (exact) return exact;
  }

  const queryText = normalizeSearchText(candidates.join(" "));
  if (!queryText) return null;

  const graphDocs = await col
    .find(
      { graphVersion: GRAPH_VERSION },
      {
        projection: {
          anchor: 1,
          graphVersion: 1,
          formulaVersion: 1,
          sourceCollection: 1,
          similarModels: { $slice: 1 },
          coverage: 1,
        },
      },
    )
    .limit(1000)
    .toArray();

  const scored = graphDocs
    .map((doc) => {
      const modelKeyText = normalizeSearchText(doc.anchor?.modelKey || "");
      const displayText = normalizeSearchText(doc.anchor?.displayName || "");
      let score = 0;

      if (modelKeyText && new RegExp(`(^|\\b)${modelKeyText.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(\\b|$)`).test(queryText)) {
        score += 80;
      }
      if (displayText && queryText.includes(displayText)) score += 100;
      for (const token of displayText.split(/\s+/).filter((token) => token.length >= 3)) {
        if (queryText.includes(token)) score += 10;
      }

      return { doc, score };
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score);

  if (!scored.length) return null;

  return col.findOne({
    graphVersion: GRAPH_VERSION,
    "anchor.makeKey": scored[0].doc.anchor.makeKey,
    "anchor.modelKey": scored[0].doc.anchor.modelKey,
  });
};

const defaultRelationTypes = new Set([
  "direct_rival",
  "platform_twin",
  "nearby_alternative",
  "adjacent_crossover",
]);

const isPremiumAnchorBudgetStepDown = ({ anchor = {}, model = {} } = {}) => {
  const anchorMid = midpoint(anchor.minExShowroomPrice, anchor.maxExShowroomPrice);
  const candidateMid = midpoint(model.minExShowroomPrice, model.maxExShowroomPrice);
  if (!anchorMid || !candidateMid) return false;
  if (anchorMid >= 4000000) return candidateMid < anchorMid * 0.65;
  if (anchorMid >= 2500000) return candidateMid < anchorMid * 0.55;
  return false;
};

const isCheaperPeer = ({ anchor = {}, model = {} } = {}) => {
  const anchorMid = midpoint(anchor.minExShowroomPrice, anchor.maxExShowroomPrice);
  const candidateMid = midpoint(model.minExShowroomPrice, model.maxExShowroomPrice);
  return Boolean(
    anchorMid &&
      candidateMid &&
      candidateMid < anchorMid &&
      ["direct_rival", "platform_twin", "nearby_alternative", "adjacent_crossover"].includes(model.relationType),
  );
};

const filterSimilarModels = ({ anchor = {}, models = [], mode = "default" } = {}) => {
  const cleanModels = asArray(models);

  if (mode === "premium") {
    const filtered = cleanModels.filter((model) => model.relationType === "premium_step_up");
    return filtered.slice(0, 12);
  }

  if (mode === "powertrain_shift" || mode === "ev") {
    const filtered = cleanModels.filter((model) => model.relationType === "powertrain_shift");
    return filtered.slice(0, 12);
  }

  if (mode === "cheaper") {
    const filtered = cleanModels.filter(
      (model) => model.relationType === "cheaper_step_down" || isCheaperPeer({ anchor, model })
    );
    return filtered.slice(0, 12);
  }

  const tightRows = cleanModels.filter((model) => defaultRelationTypes.has(model.relationType));
  return tightRows.slice(0, 9);
};

const matchLabelFor = (row = {}) => {
  if (row.relationType === "cheaper_step_down") return "Budget step-down";
  if (row.relationType === "premium_step_up") return "Premium step-up";
  if (row.relationType === "powertrain_shift") return "Powertrain-shift option";
  if (row.similarityScore >= 90) return "Very close match";
  if (row.similarityScore >= 78) return "Close alternative";
  return "Nearby alternative";
};

const withMatchLabels = (rows = []) =>
  asArray(rows).map((row) => ({
    ...row,
    matchLabel: row.matchLabel || matchLabelFor(row),
  }));

const buildAnswer = ({ anchor = {}, rows = [], mode = "default" } = {}) => {
  if (!rows.length) {
    if (mode === "premium") {
      return `I understood ${anchor.displayName || "this model"}, but the current graph does not have a clean premium step-up bucket for it. I can still show close rivals, cheaper step-downs, or EV/powertrain alternatives instead.`;
    }
    return `I understood ${anchor.displayName || "this model"}, but the current graph does not have clean close alternatives for the default view. I can still show cheaper step-downs, premium step-ups, or EV/powertrain alternatives instead.`;
  }

  const relationLabel =
    mode === "cheaper"
      ? "cheaper alternatives"
      : mode === "premium"
        ? "premium alternatives"
        : mode === "powertrain_shift" || mode === "ev"
          ? "electric/powertrain-shift alternatives"
          : "similar cars";

  const names = rows
    .slice(0, 5)
    .map((row) => `${row.displayName} (${row.matchLabel})`)
    .join(", ");
  const note = similarDecisionLanguageText("decision_similar_graph_note", {
    mode,
    anchorName: anchor.displayName || "",
  });

  return `I found ${relationLabel} for ${anchor.displayName}: ${names}. ${note}`.trim();
};

export const runVehicleSimilarTool = async ({
  toolPlan = {},
  context = {},
  userMessage = "",
  db = null,
} = {}) => {
  const resolvedDb = getDb(db);
  if (!resolvedDb) {
    return fallbackSimilarTool({ toolPlan, context });
  }

  const col = resolvedDb.collection(GRAPH_COLLECTION);
  const graphDoc = await resolveAnchorGraph({ col, toolPlan, context, userMessage });

  if (!graphDoc?.anchor) {
    return {
      ...(await fallbackSimilarTool({ toolPlan, context })),
      dataSource: "similar_model_graph_missing_anchor",
      modulesChecked: [GRAPH_COLLECTION, "missing_anchor", "stub_fallback"],
    };
  }

  const requestedMode = getRequestedMode({ message: userMessage, toolPlan });
  const rows = withMatchLabels(filterSimilarModels({
    anchor: graphDoc.anchor,
    models: graphDoc.similarModels || [],
    mode: requestedMode,
  }).slice(0, 12));

  return {
    tool: TOOL_NAME,
    intent: "vehicle_similar",
    count: rows.length,
    matched: rows.length,
    rows,
    items: rows,
    similarModels: rows,
    anchor: graphDoc.anchor,
    vehicle: {
      make: graphDoc.anchor.makeKey,
      model: graphDoc.anchor.displayName,
      modelKey: graphDoc.anchor.modelKey,
    },
    canvasType: NEW_CAR_CANVAS_TYPES.SIMILAR,
    inlineType: "similar_cars_summary",
    title: `Similar cars to ${graphDoc.anchor.displayName}`,
    answer: buildAnswer({ anchor: graphDoc.anchor, rows, mode: requestedMode }),
    usageGuardrail: createGuardrail(),
    modulesChecked: [GRAPH_COLLECTION],
    dataSource: GRAPH_COLLECTION,
    sourceTransparency: {
      modulesChecked: [GRAPH_COLLECTION],
      matched: rows.length,
      dataSource: GRAPH_COLLECTION,
      graphVersion: graphDoc.graphVersion,
      formulaVersion: graphDoc.formulaVersion,
    },
    meta: {
      graphVersion: graphDoc.graphVersion,
      formulaVersion: graphDoc.formulaVersion,
      requestedRelation: requestedMode,
      finalRecommendationEnabled: false,
    },
  };
};

export default runVehicleSimilarTool;
