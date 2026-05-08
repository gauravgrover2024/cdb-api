import * as PlannerModule from "./aiAgent.planner.js";

import {
  makeUnavailablePlan,
  sanitizePlannerPlan,
  validatePlannerPlan,
} from "./aiAgent.planSchema.js";

import {
  executeAciPlannerPlan,
  EXECUTOR_VERSION,
} from "./aiAgent.executor.js";

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
/*  Main V2 Chat Function                                                     */
/* -------------------------------------------------------------------------- */

export const chatWithAgent = async (...args) => {
  const startedAt = Date.now();

  const {
    message,
    context,
    history,
    options,
    rawInput,
  } = normalizeChatInput(...args);

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
