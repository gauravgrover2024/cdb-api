import { buildAccessContext } from "./aiAgent.accessControl.js";
import { parseAgentMessage } from "./aiAgent.intentParser.js";
import { getToolForIntent } from "./aiAgent.toolRegistry.js";
import {
  assembleResponse,
  buildFilters,
  unavailableWidget,
} from "./aiAgent.responseBuilders.js";

const fallbackHandler = async () => {
  return {
    widgets: [
      unavailableWidget(
        "I need a more specific request",
        "I could not identify an exact ACI Assist intent. Ask for a pricelist, colors, features, loan report, latest insurance, Customer 360, Vehicle 360, payout report, or delivery order.",
        ["ACI Assist"],
      ),
    ],
    followUpSuggestions: [
      "How many cars are without registration number?",
      "Cases with payout missing",
      "Verna pricelist",
      "Customer 360 Rahul Diwan",
    ],
  };
};

const intentLabel = (intent) =>
  String(intent || "answer")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());

const countWidgetRows = (widget = {}) => {
  if (!widget) return 0;

  if (widget.summary?.total !== undefined)
    return Number(widget.summary.total) || 0;
  if (widget.data?.total !== undefined) return Number(widget.data.total) || 0;
  if (widget.total !== undefined) return Number(widget.total) || 0;

  if (Array.isArray(widget.rows)) return widget.rows.length;
  if (Array.isArray(widget.records)) return widget.records.length;
  if (Array.isArray(widget.options)) return widget.options.length;
  if (Array.isArray(widget.colors)) return widget.colors.length;
  if (Array.isArray(widget.models)) return widget.models.length;
  if (Array.isArray(widget.variants)) return widget.variants.length;
  if (Array.isArray(widget.modelCards)) return widget.modelCards.length;
  if (Array.isArray(widget.groupedByModel)) return widget.groupedByModel.length;

  if (Array.isArray(widget.data?.rows)) return widget.data.rows.length;
  if (Array.isArray(widget.data?.records)) return widget.data.records.length;
  if (Array.isArray(widget.data?.options)) return widget.data.options.length;
  if (Array.isArray(widget.data?.modelCards))
    return widget.data.modelCards.length;
  if (Array.isArray(widget.data?.groupedByModel))
    return widget.data.groupedByModel.length;

  return 0;
};

const buildAssistantMessage = (parsed, result) => {
  if (result.ambiguity) return result.ambiguity.message;
  if (!result.widgets?.length)
    return "I checked live records but did not find a matching result.";

  const primary = result.widgets[0];

  if (primary.type === "unavailable_notice") {
    return (
      primary.data?.message || primary.message || "That data is unavailable."
    );
  }

  const count = countWidgetRows(primary);

  if (count > 0) {
    if (
      [
        "vehicle_recommendation_results",
        "vehicle_safety_results",
        "vehicle_emi_recommendations",
        "vehicle_variant_recommendation",
      ].includes(primary.type)
    ) {
      return `I found ${count} matching model result${count === 1 ? "" : "s"}.`;
    }

    return `I found ${count} matching record${count === 1 ? "" : "s"}.`;
  }

  return `Here is the ${intentLabel(parsed.intent)} result from live CDrive records.`;
};

export const chatWithAgent = async ({
  message,
  sessionId,
  context = {},
  selectedEntity = null,
  filters = {},
  debug = false,
  user,
} = {}) => {
  const parsed = parseAgentMessage(message, context, selectedEntity, filters);
  parsed.wantsDebug = Boolean(parsed.wantsDebug || debug);

  const access = buildAccessContext(user);
  const trace = [];

  const tool = getToolForIntent(parsed.intent);
  const result = tool
    ? await tool.run(parsed, access, trace)
    : await fallbackHandler(parsed, access, trace);

  const recordsFound = trace.reduce(
    (sum, item) => sum + (Number(item.matched) || 0),
    0,
  );

  const queryPlan =
    (parsed.wantsDebug || context?.debug || filters?.debug || debug) &&
    access.canDebug
      ? {
          sessionId,
          detectedIntent: parsed.intent,
          extractedEntities: parsed.entities,
          selectedTool: tool?.intent || "generic_search",
          collectionsUsed:
            tool?.collectionsUsed ||
            parsed.collections ||
            parsed.route?.collections ||
            [],
          filters,
          modulesScanned: trace.map((item) => item.module),
          toolsUsed: [tool?.intent || "generic_search"],
          recordsFound,
          confidence: parsed.confidence,
          accessRestrictionsApplied: access.restrictions,
          queryPlan: parsed.queryPlan || parsed.route?.queryPlan,
        }
      : undefined;

  return assembleResponse({
    parsed,
    assistantMessage: buildAssistantMessage(parsed, result),
    resultType: result.ambiguity
      ? "ambiguity"
      : result.widgets?.[0]?.type || "answer",
    widgets: result.widgets || [],
    modulesChecked: trace,
    filtersApplied: buildFilters(parsed).map(
      (chip) => `${chip.label}: ${chip.value}`,
    ),
    followUpSuggestions: result.followUpSuggestions || [],
    ambiguity: result.ambiguity,
    access,
    queryPlan,
    filters: buildFilters(
      parsed,
      tool?.collectionsUsed?.[0] || result.moduleName,
    ),
  });
};
