import { buildAccessContext } from "./aiAgent.accessControl.js";
import { routeAiAgentIntent } from "./aiAgent.intentRouter.js";
import { parseAgentMessage } from "./aiAgent.intentParser.js";
import { getToolForIntent } from "./aiAgent.toolRegistry.js";
import { assembleResponse, buildFilters, unavailableWidget } from "./aiAgent.responseBuilders.js";

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

const buildAssistantMessage = (parsed, result) => {
  if (result.ambiguity) return result.ambiguity.message;
  if (!result.widgets?.length) return "I checked live records but did not find a matching result.";
  const primary = result.widgets[0];
  if (primary.type === "unavailable_notice") return primary.data?.message || "That data is unavailable.";
  if (primary.summary?.total !== undefined) return `I found ${primary.summary.total} matching records.`;
  if (primary.data?.total !== undefined) return `I found ${primary.data.total} matching records.`;
  if (Array.isArray(primary.rows)) return `I found ${primary.rows.length} matching records.`;
  return `Here is the ${intentLabel(parsed.intent)} result from live CDrive records.`;
};

const mergeParsedWithRoute = (parsed, routed, debug = false) => ({
  ...parsed,
  intent: routed.intent,
  confidence: routed.confidence,
  route: routed,
  wantsDebug: Boolean(parsed.wantsDebug || debug),
  entities: {
    ...parsed.entities,
    ...routed.entities,
  },
});

export const chatWithAgent = async ({
  message,
  sessionId,
  context = {},
  selectedEntity = null,
  filters = {},
  debug = false,
  user,
} = {}) => {
  const routed = routeAiAgentIntent({ message, context, selectedEntity, filters });
  const parsedBase = parseAgentMessage(message, context, selectedEntity, filters);
  const parsed = mergeParsedWithRoute(parsedBase, routed, debug);
  const access = buildAccessContext(user);
  const trace = [];
  const tool = routed.structured ? getToolForIntent(routed.intent) : null;
  const result = tool ? await tool.run(parsed, access, trace) : await fallbackHandler(parsed, access, trace);
  const recordsFound = trace.reduce((sum, item) => sum + (Number(item.matched) || 0), 0);
  const queryPlan =
    (parsed.wantsDebug || context?.debug || filters?.debug || debug) && access.canDebug
      ? {
          sessionId,
          detectedIntent: parsed.intent,
          extractedEntities: parsed.entities,
          selectedTool: tool?.intent || "generic_search",
          collectionsUsed: tool?.collectionsUsed || routed.collections || [],
          filters,
          modulesScanned: trace.map((item) => item.module),
          toolsUsed: [tool?.intent || "generic_search"],
          recordsFound,
          confidence: parsed.confidence,
          accessRestrictionsApplied: access.restrictions,
        }
      : undefined;

  return assembleResponse({
    parsed,
    assistantMessage: buildAssistantMessage(parsed, result),
    resultType: result.ambiguity ? "ambiguity" : result.widgets?.[0]?.type || "answer",
    widgets: result.widgets || [],
    modulesChecked: trace,
    filtersApplied: buildFilters(parsed).map((chip) => `${chip.label}: ${chip.value}`),
    followUpSuggestions: result.followUpSuggestions || [],
    ambiguity: result.ambiguity,
    access,
    queryPlan,
    filters: buildFilters(parsed, tool?.collectionsUsed?.[0] || result.moduleName),
  });
};
