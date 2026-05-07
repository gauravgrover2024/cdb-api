import { buildAccessContext } from "./aiAgent.accessControl.js";
import { parseAgentMessage } from "./aiAgent.intentParser.js";
import { getToolForIntent } from "./aiAgent.toolRegistry.js";
import {
  assembleResponse,
  buildFilters,
  unavailableWidget,
} from "./aiAgent.responseBuilders.js";
import {
  buildContextSnapshot,
  buildConversationSuggestions,
  buildFollowUpSuggestions,
  buildLeadingQuestions,
} from "./aiAgent.actionBuilder.js";
import {
  getIntentForWidgetType,
  getNewCarQuestionConfig,
  mapIntentAlias,
} from "./aiAgent.newCarQuestionMap.js";
import { logInteraction } from "./aiAgent.learningEngine.js";
import { sanitizeAiAgentResponse } from "./aiAgent.responseSanitizer.js";

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

const getUserId = (user = {}, context = {}) =>
  String(
    context?.userId ||
      context?.profile?.userId ||
      user?._id ||
      user?.id ||
      user?.userId ||
      "",
  );

const buildServiceContext = ({ context = {}, user = {} } = {}) => {
  const userId = getUserId(user, context);

  return {
    ...context,
    userId,
    profile: {
      ...(context?.profile || {}),
      userId,
    },
  };
};

const buildAssistantMessage = (parsed, result, contextSnapshot = null) => {
  if (result.ambiguity) return result.ambiguity.message;
  if (!result.widgets?.length)
    return "I checked live records but did not find a matching result.";

  const primary = result.widgets[0];
  const widgetData = primary?.data || primary;
  const model =
    contextSnapshot?.anchorModel ||
    widgetData?.model ||
    parsed?.entities?.model ||
    parsed?.entities?.models?.[0];
  const variant =
    contextSnapshot?.anchorVariant ||
    widgetData?.variantQuery ||
    parsed?.entities?.variant;
  const city =
    contextSnapshot?.city ||
    contextSnapshot?.requestedCity ||
    widgetData?.city ||
    parsed?.entities?.city ||
    "Delhi / New Delhi";
  const feature = widgetData?.feature || parsed?.entities?.feature;

  if (primary.type === "unavailable_notice") {
    return (
      primary.data?.message || primary.message || "That data is unavailable."
    );
  }

  if (primary.type === "model_ambiguity") {
    return `I found more than one model matching ${model || "your query"}. Which one should I use so I can show exact price, colors, features, and EMI?`;
  }

  if (primary.type === "variant_ambiguity") {
    return `I found multiple matching variants for ${model || "this model"}. Please pick the exact variant so I can show precise price, features, and EMI.`;
  }

  if (primary.type === "vehicle_feature_answer") {
    const answer = String(widgetData?.answer || "").toLowerCase();
    const yesNo =
      answer === "yes"
        ? "Yes"
        : answer === "no"
          ? "No"
          : answer === "mixed"
            ? "It varies by variant"
            : "I could not confirm it in the stored records";
    return `${yesNo}, ${model || "this model"}${variant ? ` ${variant}` : ""}${feature ? ` ${feature}` : ""}. I checked the matched feature records and can now open full features, compare variants, or calculate EMI.`;
  }

  if (primary.type === "vehicle_pricelist") {
    return `Here is the ${model || "selected model"} price list for ${city}. I can also show colors, calculate EMI, compare with relevant rivals, or prepare an ACI quotation.`;
  }

  if (primary.type === "vehicle_colors") {
    return `Here are the available colors for ${model || "this model"}. Color data is model-level in the current dataset, and I can include your preferred color in quotation, then show pricing or EMI.`;
  }

  if (primary.type === "vehicle_model_comparison") {
    const compareModels = (widgetData?.models || [])
      .map((item) => item?.model || item?.name)
      .filter(Boolean);
    const label = compareModels.length
      ? compareModels.join(" vs ")
      : model || "selected models";
    return `I compared ${label}. I can now break down feature differences, run EMI for each model, and prepare a quotation for your best fit.`;
  }

  if (
    [
      "vehicle_recommendation_results",
      "vehicle_safety_results",
      "vehicle_emi_recommendations",
    ].includes(primary.type)
  ) {
    return `I shortlisted matching models based on your request. I can compare the top options, refine filters, or move to EMI and quotation for your preferred model.`;
  }

  if (primary.type === "vehicle_emi_calculator") {
    return `I calculated EMI for ${model || "the selected model"}${variant ? ` ${variant}` : ""}. I can adjust down payment/tenure, compare EMI across variants, or add finance in quotation.`;
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

const actionFromConversationSuggestion = (item = {}) => ({
  id: item.id,
  label: item.title,
  type: item.type || "ask",
  query: item.query,
  canvasType: item.canvasType || "",
  inlineType: item.inlineType || "",
  leadType: item.leadType || item?.contextPatch?.leadType || "",
  route: item.route || "",
  intent: item.intent,
  entities: item.entities || {},
  contextPatch: item.contextPatch || {},
  icon: item.icon,
  tone: item.tone,
});

const logSuggestionImpressions = ({ userId, suggestions = [] } = {}) => {
  if (!suggestions.length) return;

  Promise.allSettled(
    suggestions.map((item) =>
      logInteraction({
        userId,
        intent: item.intent,
        suggestionId: item.id,
        actionTaken: false,
        countImpression: true,
      }),
    ),
  ).catch(() => {});
};

const __chatWithAgentCore = async ({
  message,
  sessionId,
  context = {},
  selectedEntity = null,
  filters = {},
  debug = false,
  user,
} = {}) => {
  const serviceContext = buildServiceContext({ context, user });

  const parsed = parseAgentMessage(
    message,
    serviceContext,
    selectedEntity,
    filters,
  );

  parsed.wantsDebug = Boolean(parsed.wantsDebug || debug);

  const access = buildAccessContext(user);
  const trace = [];

  const tool = getToolForIntent(parsed.intent);
  const result = tool
    ? await tool.run(parsed, access, trace)
    : await fallbackHandler(parsed, access, trace);

  const canonicalIntent = mapIntentAlias(parsed.intent);
  const primaryWidget = result.widgets?.[0] || {};
  const widgetIntent = getIntentForWidgetType(primaryWidget.type || "");

  const isStructuredNewCar = Boolean(
    getNewCarQuestionConfig(canonicalIntent) ||
    getNewCarQuestionConfig(widgetIntent),
  );
  const resolvedConversationIntent = [
    "model_ambiguity",
    "variant_ambiguity",
  ].includes(primaryWidget.type)
    ? widgetIntent || canonicalIntent
    : canonicalIntent;
  const parsedForConversation = {
    ...parsed,
    intent: resolvedConversationIntent,
  };
  const toolCollections = tool?.collectionsUsed || [];
  const conversationContextResult = {
    ...result,
    modulesChecked: trace,
    collectionsUsed: toolCollections,
  };
  let contextSnapshot = null;
  let conversationSuggestions = [];
  let derivedLeadingQuestions = result.leadingQuestions || [];
  let derivedFollowUps = result.followUpSuggestions || [];
  let derivedActions = result.actions || [];

  if (isStructuredNewCar) {
    contextSnapshot = buildContextSnapshot({
      parsed: parsedForConversation,
      result: conversationContextResult,
      primaryWidget,
      selectedEntity,
      filters,
      context: serviceContext,
    });
    conversationSuggestions = await buildConversationSuggestions({
      parsed: parsedForConversation,
      result: conversationContextResult,
      contextSnapshot,
      primaryWidget,
    });

    logSuggestionImpressions({
      userId: contextSnapshot?.userId,
      suggestions: conversationSuggestions,
    });

    derivedLeadingQuestions = buildLeadingQuestions({
      parsed: parsedForConversation,
      result: conversationContextResult,
      contextSnapshot,
    });
    derivedFollowUps = buildFollowUpSuggestions({ conversationSuggestions });
    derivedActions = conversationSuggestions.map(
      actionFromConversationSuggestion,
    );
  }

  // Preserve module transparency for multi-intent chat questions where
  // primary intent is different from supporting feature/color intents.
  const moduleNames = new Set(
    trace.map((item) => String(item.module || "").toLowerCase()),
  );
  const secondaryIntents = parsed.secondaryIntents || [];
  if (
    secondaryIntents.some((intent) =>
      [
        "vehicle_feature_answer",
        "vehicle_spec_lookup",
        "vehicle_feature_discovery",
        "vehicle_model_features_explorer",
      ].includes(intent),
    ) &&
    ![...moduleNames].some((name) => name.includes("feature"))
  ) {
    trace.push({
      module: "Vehicle Features (secondary intent)",
      matched: 0,
      secondaryIntent: true,
    });
  }
  if (
    secondaryIntents.some((intent) =>
      ["vehicle_colors", "vehicle_color_gallery"].includes(intent),
    ) &&
    ![...moduleNames].some((name) => name.includes("color"))
  ) {
    trace.push({
      module: "Vehicle Colors (secondary intent)",
      matched: 0,
      secondaryIntent: true,
    });
  }

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
    assistantMessage: buildAssistantMessage(parsed, result, contextSnapshot),
    resultType: result.ambiguity
      ? "ambiguity"
      : result.widgets?.[0]?.type || "answer",
    widgets: result.widgets || [],
    modulesChecked: trace,
    filtersApplied: buildFilters(parsed).map(
      (chip) => `${chip.label}: ${chip.value}`,
    ),
    actions: derivedActions,
    leadingQuestions: derivedLeadingQuestions,
    followUpSuggestions: derivedFollowUps,
    conversationSuggestions,
    salesNudges: contextSnapshot?.salesNudges || [],
    closingActions: contextSnapshot?.closingActions || [],
    conversationMode: contextSnapshot?.mode || "",
    conversationStage: contextSnapshot?.stage || "",
    userProfile: contextSnapshot?.profile || null,
    contextSnapshot,
    ambiguity: result.ambiguity,
    access,
    queryPlan,
    filters: buildFilters(
      parsed,
      tool?.collectionsUsed?.[0] || result.moduleName,
    ),
  });
};

/* ACI_RESPONSE_SANITIZER_WRAPPER_START */
export const chatWithAgent = async (...args) => {
  const result = await __chatWithAgentCore(...args);

  const firstArg = args[0];
  const secondArg = args[1];

  const message =
    typeof firstArg === "string"
      ? firstArg
      : firstArg?.message || firstArg?.query || "";

  const context =
    firstArg && typeof firstArg === "object" && firstArg.context
      ? firstArg.context
      : secondArg && typeof secondArg === "object"
        ? secondArg
        : {};

  return sanitizeAiAgentResponse(result, {
    message,
    context,
  });
};
/* ACI_RESPONSE_SANITIZER_WRAPPER_END */

