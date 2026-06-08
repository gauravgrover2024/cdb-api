import * as PlannerModule from "./aiAgent.planner.js";
import { normalizeAciFinalResponse } from "./aiAgent.contractNormalizer.js";
import {
  applyAciExplicitMessageModelContextOverride,
  repairAciResponseContextFromActiveContext,
} from "./aiAgent.contextPriority.js";
import {
  hydrateAciExplicitModelEntityFromReadModel,
  resolveAciExplicitMessageModelEntity,
} from "./aiAgent.modelContextResolver.js";
import {
  maybeRunAciEarlyFeatureGate,
  maybeRunAciPreBridgeMultiFeatureAnswer,
} from "./aiAgent.earlyFeatureGate.js";

import {
  makeUnavailablePlan,
  sanitizePlannerPlan,
  validatePlannerPlan,
} from "./aiAgent.planSchema.js";

import {
  executeAciPlannerPlan,
  EXECUTOR_VERSION,
} from "./aiAgent.executor.js";
import {
  runAciCoreLiveBridge,
  shouldUseAciCoreLiveBridge,
} from "../aciCore/integration/aciCoreLiveBridge.service.js";

/**
 * ACI Assist V2 Service
 *
 * This service intentionally does NOT use:
 *
 * Flow:
 * user message
 * -> aiAgent.planner.js
 * -> aiAgent.executor.js
 * -> aiAgent.responseTools.js
 * -> aiAgent.responseSanitizer.js
 * -> stable frontend contract
 */

/* -------------------------------------------------------------------------- */
/*  Constants                                                                 */
/* -------------------------------------------------------------------------- */

export const ACI_ASSIST_SERVICE_VERSION = "aci-assist-v2-service";

export const DEFAULT_CONTEXT = Object.freeze({
  selectedVehicle: {},
  selectedComparisonSet: {},
  userPreferences: {},
  leadContext: {},
});

const PLANNER_FUNCTION_CANDIDATES = [
  "planAciAssistMessage",
  "createAciPlannerPlan",
  "createPlannerPlan",
  "buildAciPlannerPlan",
  "buildPlannerPlan",
  "generateAciPlannerPlan",
  "generatePlannerPlan",
  "runAciPlanner",
  "runPlanner",
  "planMessage",
  "planner",
  "default",
];

/* -------------------------------------------------------------------------- */
/*  Generic Helpers                                                           */
/* -------------------------------------------------------------------------- */

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const isPlainObject = (value) =>
  Boolean(value) &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  !(value instanceof Date);

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const hasPlannerTools = (value) =>
  Boolean(value) &&
  typeof value === "object" &&
  Array.isArray(value.tools) &&
  value.tools.length > 0;

const safeObject = (value) => (isPlainObject(value) ? value : {});

const mergeContext = (...contexts) => {
  const output = {
    ...DEFAULT_CONTEXT,
  };

  for (const context of contexts) {
    if (!isPlainObject(context)) continue;

    Object.assign(output, context);

    if (context.selectedVehicle) {
      output.selectedVehicle = {
        ...(output.selectedVehicle || {}),
        ...context.selectedVehicle,
      };
    }

    if (context.selectedComparisonSet) {
      output.selectedComparisonSet = {
        ...(output.selectedComparisonSet || {}),
        ...context.selectedComparisonSet,
      };
    }

    if (context.userPreferences) {
      output.userPreferences = {
        ...(output.userPreferences || {}),
        ...context.userPreferences,
      };
    }

    if (context.leadContext) {
      output.leadContext = {
        ...(output.leadContext || {}),
        ...context.leadContext,
      };
    }
  }

  return output;
};

export const normalizeChatInput = (...args) => {
  const firstArg = args[0];
  const secondArg = args[1];

  if (isPlainObject(firstArg)) {
    const message = cleanText(
      firstArg.message ||
        firstArg.query ||
        firstArg.userMessage ||
        firstArg.prompt ||
        firstArg.text ||
        "",
    );

    const context = mergeContext(
      firstArg.context,
      firstArg.conversationContext,
      firstArg.state,
      firstArg.session,
      secondArg,
    );

    return {
      message,
      context,
      history: asArray(firstArg.history || firstArg.messages),
      options: safeObject(firstArg.options),
      rawInput: firstArg,
    };
  }

  return {
    message: cleanText(firstArg || ""),
    context: mergeContext(secondArg),
    history: [],
    options: {},
    rawInput: {
      message: cleanText(firstArg || ""),
      context: safeObject(secondArg),
    },
  };
};

const extractPlannerPlan = (plannerResult) => {
  if (hasPlannerTools(plannerResult)) return plannerResult;

  const candidates = [
    plannerResult?.plan,
    plannerResult?.plannerPlan,
    plannerResult?.aiPlan,
    plannerResult?.aciPlan,
    plannerResult?.data?.plan,
    plannerResult?.result?.plan,
    plannerResult?.planner?.plan,
    plannerResult?.validation?.plan,
  ];

  for (const candidate of candidates) {
    if (hasPlannerTools(candidate)) return candidate;
  }

  return null;
};

const getPlannerFunction = () => {
  for (const name of PLANNER_FUNCTION_CANDIDATES) {
    const candidate = PlannerModule[name];

    if (typeof candidate === "function") {
      return {
        name,
        fn: candidate,
      };
    }
  }

  return {
    name: "",
    fn: null,
  };
};

const tryPlannerCall = async ({
  plannerFn,
  callShape,
  message,
  context,
  history,
  options,
}) => {
  if (callShape === "object") {
    return plannerFn({
      message,
      userMessage: message,
      query: message,
      prompt: message,
      context,
      conversationContext: context,
      history,
      messages: history,
      options,
    });
  }

  if (callShape === "positional") {
    return plannerFn(message, context, {
      history,
      options,
    });
  }

  if (callShape === "message-only") {
    return plannerFn(message);
  }

  return null;
};

export const createUnavailableFallbackPlan = ({
  message = "",
  reason = "unsupported_request",
  details = "",
} = {}) =>
  makeUnavailablePlan({
    reason,
    message:
      details ||
      `ACI Assist could not create a valid planner plan for: ${message}`,
    confidence: 0,
  });

export const createPlannerPlanForMessage = async ({
  message = "",
  context = {},
  history = [],
  options = {},
} = {}) => {
  const { name, fn } = getPlannerFunction();

  if (!fn) {
    return {
      plan: createUnavailableFallbackPlan({
        message,
        details:
          "No planner function export was found in aiAgent.planner.js. Expected one of: " +
          PLANNER_FUNCTION_CANDIDATES.join(", "),
      }),
      plannerMeta: {
        plannerExport: "",
        fallbackUsed: true,
        error: "planner_function_not_found",
      },
    };
  }

  const callShapes = ["object", "positional", "message-only"];
  const errors = [];

  for (const callShape of callShapes) {
    try {
      const plannerResult = await tryPlannerCall({
        plannerFn: fn,
        callShape,
        message,
        context,
        history,
        options,
      });

      const extractedPlan = extractPlannerPlan(plannerResult);

      if (extractedPlan) {
        const sanitizedPlan = sanitizePlannerPlan(extractedPlan, {
          message,
        });

        const validation = validatePlannerPlan(sanitizedPlan, {
          message,
        });

        return {
          plan: validation.plan || sanitizedPlan,
          rawPlannerResult: plannerResult,
          plannerMeta: {
            plannerExport: name,
            callShape,
            fallbackUsed: false,
            validation: {
              valid: validation.valid,
              errors: validation.errors || [],
              warnings: validation.warnings || [],
            },
          },
        };
      }

      errors.push({
        callShape,
        error: "planner_returned_no_tools",
      });
    } catch (error) {
      errors.push({
        callShape,
        error: error?.message || String(error),
      });
    }
  }

  return {
    plan: createUnavailableFallbackPlan({
      message,
      details:
        "Planner function was found but did not return a valid plan with tools[].",
    }),
    plannerMeta: {
      plannerExport: name,
      fallbackUsed: true,
      error: "planner_invalid_output",
      attempts: errors,
    },
  };
};

/* -------------------------------------------------------------------------- */
/*  Internal / CDrive Passthrough Guard                                        */
/* -------------------------------------------------------------------------- */

const isInternalCdriveOpsUser = (user = {}) => {
  const role = String(user?.role || user?.type || user?.userType || "").toLowerCase();
  return Boolean(user?.id || user?._id || user?.email) && /admin|internal|staff|ops|manager|owner/.test(role);
};

const isInternalCdriveOpsQuery = (message = "") =>
  /\b(loan closure|closure\s+\d{3,}|loan id|approved but not disbursed|disbursed cases|total business|customer 360|payment pending|receivables?|delivery order|do number|insurance renewal)\b/i.test(
    String(message || ""),
  );

const buildInternalPassthroughContractResponse = ({
  message = "",
  user = null,
  startedAt = Date.now(),
} = {}) => ({
  intent: "internal_passthrough",
  mode: "single_tool",
  displayMode: "inline",
  canvasType: "",
  inlineType: "",
  title: "Internal CDrive request",
  answer: "This looks like an internal CDrive operations request. I will route it through the internal workflow instead of the public new-car assistant.",
  data: {
    requestType: "internal_cdrive_ops",
    originalMessage: String(message || ""),
    routedTo: "internal_passthrough",
  },
  actions: [],
  leadingQuestions: [],
  conversationSuggestions: [],
  contextPatch: {
    selectedVehicle: {
      make: "",
      brand: "",
      model: "",
      variant: "",
      city: "new-delhi",
    },
    anchorMake: "",
    anchorModel: "",
    anchorVariant: "",
    anchorCity: "new-delhi",
    customerStage: "internal",
    conversationMode: "internal_passthrough",
  },
  sourceTransparency: {
    mode: "internal_passthrough",
    collections: [],
    recordCount: 0,
    notes: ["Internal workflow passthrough. No public new-car factual data used."],
  },
  runtimeResultsMeta: [
    {
      tool: "internal_passthrough",
      intent: "internal_passthrough",
      recordCount: 0,
      status: "routed",
      source: "aiAgent.service.internal_guard",
    },
  ],
  plannerTools: ["internal_passthrough"],
  secondaryResponses: [],
  service: {
    version: ACI_ASSIST_SERVICE_VERSION,
    executorVersion: EXECUTOR_VERSION,
    durationMs: Math.max(0, Date.now() - startedAt),
    oldSystemUsed: false,
    channel: "internal_web",
    routedBy: "internal_cdrive_ops_guard",
    userRole: user?.role || "",
  },
  oldSystemUsed: false,
  meta: {
    responseTool: "internal_passthrough",
    domain: "internal",
  },
});

/* -------------------------------------------------------------------------- */
/*  Main V2 Chat Function                                                     */
/* -------------------------------------------------------------------------- */





const chatWithAgentCore = async (...args) => {
  const __aciEarlyAgentArgs = Array.isArray(args) ? (args[0] || {}) : {};
  const startedAt = Date.now();

  const {
    message,
    context,
    history,
    options,
    rawInput,
  } = normalizeChatInput(...args);

  if (isInternalCdriveOpsUser(__aciEarlyAgentArgs?.user) && isInternalCdriveOpsQuery(message)) {
    return buildInternalPassthroughContractResponse({
      message,
      user: __aciEarlyAgentArgs?.user,
      startedAt,
    });
  }

  if (!message) {
    const plan = createUnavailableFallbackPlan({
      message,
      reason: "insufficient_information",
      details: "No user message was provided.",
    });

    const response = await executeAciPlannerPlan({
      plan,
      userMessage: "",
      context,
    });

    return {
      ...response,
      service: {
        version: ACI_ASSIST_SERVICE_VERSION,
        executorVersion: EXECUTOR_VERSION,
        durationMs: Date.now() - startedAt,
        plannerFallbackUsed: true,
      },
    };
  }

  const preBridgeMultiFeatureResponse = await maybeRunAciPreBridgeMultiFeatureAnswer({
    message,
    context,
    selectedEntity: __aciEarlyAgentArgs.selectedEntity,
  });

  if (preBridgeMultiFeatureResponse) {
    return preBridgeMultiFeatureResponse;
  }

  if (shouldUseAciCoreLiveBridge({ message })) {
    try {
      return await runAciCoreLiveBridge({
        message,
        context,
        user: __aciEarlyAgentArgs?.user || null,
        session: __aciEarlyAgentArgs?.session || null,
        meta: {
          ...(__aciEarlyAgentArgs?.meta || {}),
          source: "aiAgent.service",
          fallback: "legacy_ai_agent",
        },
      });
    } catch (error) {
      console.warn(
        "[ACI Core Live Bridge] failed; falling back to legacy ACI Assist path:",
        error?.message || error,
      );
    }
  }

  const explicitMessageModelEntityForContext =
    await resolveAciExplicitMessageModelEntity(message);

  applyAciExplicitMessageModelContextOverride({
    message,
    context,
    dynamicModelEntity: explicitMessageModelEntityForContext,
  });

  const earlyFeatureResponse = await maybeRunAciEarlyFeatureGate({
    message,
    context,
    selectedEntity: __aciEarlyAgentArgs.selectedEntity,
    filters: __aciEarlyAgentArgs.filters,
  });

  if (earlyFeatureResponse) {
    return earlyFeatureResponse;
  }

  const {
    plan,
    plannerMeta,
    rawPlannerResult,
  } = await createPlannerPlanForMessage({
    message,
    context,
    history,
    options,
  });

  const response = await executeAciPlannerPlan({
    plan,
    userMessage: message,
    context,
    runtimeHints: {
      rawPlannerResult,
      rawInput,
    },
  });

  return {
    ...response,
    service: {
      version: ACI_ASSIST_SERVICE_VERSION,
      executorVersion: EXECUTOR_VERSION,
      durationMs: Date.now() - startedAt,
      planner: plannerMeta,
      oldSystemUsed: false,
    },
  };
};

const getNormalizerInputs = (args = []) => {
  const rawInput = args[0] || {};

  const message =
    typeof rawInput === "string"
      ? rawInput
      : rawInput.message || rawInput.query || rawInput.text || rawInput.userMessage || rawInput.prompt || "";

  const context =
    typeof rawInput === "object"
      ? rawInput.context ||
        rawInput.conversationContext ||
        rawInput.sessionContext ||
        rawInput.state ||
        rawInput.session ||
        {}
      : args[1] || {};

  return {
    message,
    context,
  };
};


const pickAciFirstObject = (...values) => {
  for (const value of values) {
    if (isPlainObject(value)) return value;
  }
  return {};
};

const pickAciFirstArray = (...values) => {
  for (const value of values) {
    if (Array.isArray(value)) return value.filter(Boolean);
  }
  return [];
};

const normalizeAciServiceResponseContract = (
  response = {},
  { startedAt = Date.now() } = {},
) => {
  if (!response || typeof response !== "object") return response;

  const widget =
    response.widget ||
    (Array.isArray(response.widgets) ? response.widgets.find(Boolean) : null) ||
    null;

  const canvasType = response.canvasType || widget?.canvasType || "";
  const inlineType = response.inlineType || widget?.inlineType || "";

  const displayMode =
    response.displayMode ||
    (canvasType && inlineType
      ? "both"
      : canvasType
        ? "canvas"
        : inlineType
          ? "inline"
          : "inline");

  const actions = pickAciFirstArray(
    response.actions,
    widget?.actions,
    response.data?.actions,
  );

  const leadingQuestions = pickAciFirstArray(
    response.leadingQuestions,
    response.conversationSuggestions,
    widget?.leadingQuestions,
    widget?.conversationSuggestions,
    response.data?.leadingQuestions,
    response.data?.conversationSuggestions,
  );

  const rows = pickAciFirstArray(
    response.rows,
    response.items,
    response.data?.rows,
    response.data?.items,
    widget?.rows,
    widget?.items,
  );

  const data = {
    ...pickAciFirstObject(response.data),
  };

  if (!Object.keys(data).length) {
    data.title = response.title || widget?.title || "";
    data.answer = response.answer || widget?.answer || "";
    data.intent = response.intent || widget?.intent || "";
    data.canvasType;
    data.inlineType;
    data.displayMode = displayMode;
    data.vehicle =
      response.vehicle ||
      widget?.vehicle ||
      response.contextPatch?.selectedVehicle ||
      {};
    data.rows = rows;
    data.items = rows;
    data.contextPatch = response.contextPatch || {};
  }

  const secondaryResponses = Array.isArray(response.secondaryResponses)
    ? response.secondaryResponses
    : [];

  const runtimeResultsMeta = Array.isArray(response.runtimeResultsMeta)
    ? response.runtimeResultsMeta
    : [];

  return {
    ...response,
    displayMode,
    canvasType,
    inlineType,
    actions,
    leadingQuestions,
    secondaryResponses,
    runtimeResultsMeta,
    data,
    service: {
      ...(response.service || {}),
      version:
        response.service?.version ||
        ACI_ASSIST_SERVICE_VERSION,
      executorVersion:
        response.service?.executorVersion ||
        EXECUTOR_VERSION,
      durationMs:
        response.service?.durationMs ??
        Math.max(0, Date.now() - startedAt),
      oldSystemUsed: false,
    },
  };
};



export const chatWithAgent = async (...args) => {
  const startedAt = Date.now();
  const { message, context } = getNormalizerInputs(args);
  const response = await chatWithAgentCore(...args);

  await repairAciResponseContextFromActiveContext({
    response,
    context,
    hydrateModelEntity: hydrateAciExplicitModelEntityFromReadModel,
  });

  const normalized = await normalizeAciFinalResponse(response, {
    message,
    context,
  });

  await repairAciResponseContextFromActiveContext({
    response: normalized,
    context,
    hydrateModelEntity: hydrateAciExplicitModelEntityFromReadModel,
  });

  return normalizeAciServiceResponseContract(normalized, {
    startedAt,
  });
};

export const chatWithAciAssist = chatWithAgent;
export const runAciAssist = chatWithAgent;
export const askAciAssist = chatWithAgent;


export default {
  chatWithAgent,
  chatWithAciAssist,
  runAciAssist,
  askAciAssist,
  createPlannerPlanForMessage,
};
