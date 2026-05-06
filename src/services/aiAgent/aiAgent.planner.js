import { generateObject, generateText } from "ai";
import { createGoogleGenerativeAI } from "@ai-sdk/google";

import {
  AciPlannerSchema,
  DATA_AVAILABILITY,
  makeClarificationPlan,
  makeInternalPassthroughPlan,
  makeUnavailablePlan,
  plannerSchemaForPrompt,
  plannerSystemRules,
  sanitizePlannerPlan,
  validatePlannerPlan,
  looksLikeInternalOpsQuery,
  looksLikeNewCarQuery,
  normalizeSearchKey,
} from "./aiAgent.planSchema.js";

const DEFAULT_PLANNER_MODEL = "gemini-2.5-flash";
const DEFAULT_TIMEOUT_MS = 12000;
const LOW_CONFIDENCE_THRESHOLD = 0.55;

const getEnv = (key, fallback = "") => {
  const value = process.env[key];
  return value === undefined || value === null || value === ""
    ? fallback
    : value;
};

export const isAiPlannerEnabled = () =>
  String(process.env.ACI_USE_AI_PLANNER || "false").toLowerCase() === "true";

export const getPlannerModelName = () =>
  getEnv("ACI_PLANNER_MODEL", DEFAULT_PLANNER_MODEL);

export const getPlannerTimeoutMs = () => {
  const parsed = Number(
    process.env.ACI_PLANNER_TIMEOUT_MS || DEFAULT_TIMEOUT_MS,
  );
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_TIMEOUT_MS;
};

const getGeminiApiKey = () =>
  getEnv("GEMINI_API_KEY") ||
  getEnv("GOOGLE_GENERATIVE_AI_API_KEY") ||
  getEnv("GOOGLE_API_KEY");

const createPlannerProvider = () => {
  const apiKey = getGeminiApiKey();

  if (!apiKey) {
    throw new Error(
      "Gemini planner API key missing. Set GEMINI_API_KEY or GOOGLE_GENERATIVE_AI_API_KEY.",
    );
  }

  return createGoogleGenerativeAI({
    apiKey,
  });
};

const safeJsonStringify = (value, space = 2) => {
  try {
    return JSON.stringify(value, null, space);
  } catch {
    return JSON.stringify(String(value || ""));
  }
};

const truncateText = (value = "", max = 1200) => {
  const text = String(value || "");
  return text.length > max ? `${text.slice(0, max)}…` : text;
};

const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (value === undefined || value === null || value === "") return false;
      if (Array.isArray(value)) return value.length > 0;
      if (typeof value === "object") return Object.keys(value).length > 0;
      return true;
    }),
  );

const sanitizeContextForPlanner = (context = {}) => {
  const allowed = {
    lastIntent: context.lastIntent || context.intent || "",
    stage: context.stage || context.mode || "",
    city: context.city || context.entities?.city || "",
    model:
      context.model ||
      context.anchorModel ||
      context.entities?.model ||
      context.selectedModels?.[0] ||
      "",
    variant:
      context.variant ||
      context.anchorVariant ||
      context.entities?.variant ||
      "",
    selectedModels: Array.isArray(context.selectedModels)
      ? context.selectedModels.slice(0, 5)
      : [],
    selectedVariants: Array.isArray(context.selectedVariants)
      ? context.selectedVariants.slice(0, 5)
      : [],
    budgetMax:
      context.budgetMax ||
      context.entities?.budgetMax ||
      context.filters?.budgetMax ||
      null,
    budgetMin:
      context.budgetMin ||
      context.entities?.budgetMin ||
      context.filters?.budgetMin ||
      null,
    profile: context.profile
      ? {
          preferredBudget: context.profile.preferredBudget || null,
          preferredBodyType: context.profile.preferredBodyType || null,
          preferredFuel: context.profile.preferredFuel || null,
          preferredTransmission: context.profile.preferredTransmission || null,
          buyingPriority: context.profile.buyingPriority || null,
        }
      : {},
    history: context.history || {},
  };

  return compactObject(allowed);
};

const sanitizeSelectedEntityForPlanner = (selectedEntity = null) => {
  if (!selectedEntity || typeof selectedEntity !== "object") return null;

  return compactObject({
    entityType: selectedEntity.entityType || selectedEntity.type || "",
    brand: selectedEntity.brand || selectedEntity.make || "",
    make: selectedEntity.make || selectedEntity.brand || "",
    model: selectedEntity.model || "",
    variant: selectedEntity.variant || "",
    city: selectedEntity.city || "",
    color: selectedEntity.color || selectedEntity.colorName || "",
    fuelType: selectedEntity.fuelType || selectedEntity.fuel || "",
    transmission: selectedEntity.transmission || "",
    price:
      selectedEntity.price ||
      selectedEntity.onRoadPrice ||
      selectedEntity.exShowroom ||
      "",
  });
};

const sanitizeFiltersForPlanner = (filters = {}) => {
  if (!filters || typeof filters !== "object") return {};

  return compactObject({
    city: filters.city || "",
    budgetMin: filters.budgetMin || null,
    budgetMax: filters.budgetMax || null,
    bodyType: filters.bodyType || "",
    fuelType: filters.fuelType || "",
    transmission: filters.transmission || "",
    priceBasis: filters.priceBasis || "",
    activeOnly: filters.activeOnly,
    includeDiscontinued: filters.includeDiscontinued,
    mustHaveFeatures: Array.isArray(filters.mustHaveFeatures)
      ? filters.mustHaveFeatures.slice(0, 12)
      : [],
    compareFeatures: Array.isArray(filters.compareFeatures)
      ? filters.compareFeatures.slice(0, 12)
      : [],
  });
};

export const classifyPlannerDomain = (message = "") => {
  const text = normalizeSearchKey(message);

  const explicitInternal =
    /\b(loan closure|loan id|approved but not disbursed|disbursed cases|total business|customer 360|payment pending|receivable|delivery order|do number|insurance renewal)\b/.test(
      text,
    );

  if (explicitInternal) return "internal";

  if (looksLikeInternalOpsQuery(message) && !looksLikeNewCarQuery(message)) {
    return "internal";
  }

  if (looksLikeNewCarQuery(message)) return "new_car";

  if (
    /\b(what is|explain|meaning|difference between|how does|why is)\b/.test(
      text,
    ) &&
    /\b(car|emi|loan|on road|ex showroom|cvt|dct|ivt|amt|adas|airbag|ncap|sunroof|insurance|rto|tcs)\b/.test(
      text,
    )
  ) {
    return "new_car";
  }

  return "unknown";
};

const buildPlannerExamples = () => [
  {
    user: "Verna pricelist",
    plan: {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "direct_answer",
      customerStage: "exploration",
      tools: [
        {
          tool: "vehicle_pricelist",
          entities: { model: "Verna" },
          filters: {
            city: "new-delhi",
            activeOnly: true,
            priceBasis: "on_road",
          },
          ranking: null,
          output: {
            canvasType: "pricelist_canvas",
            inlineType: null,
            groupBy: "variant",
          },
        },
      ],
      nextSteps: [
        {
          label: "Show colors",
          query: "Show colors of Verna",
          tool: "vehicle_colors",
          priority: 90,
        },
        {
          label: "Calculate EMI",
          query: "Calculate EMI for Verna",
          tool: "vehicle_emi",
          priority: 80,
        },
      ],
      clarification: null,
      confidence: 0.95,
      reasoningSummary: "User wants the current Verna price list.",
    },
  },
  {
    user: "Best automatic SUV under 20 lakh with sunroof and 6 airbags",
    plan: {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "recommendation",
      customerStage: "exploration",
      tools: [
        {
          tool: "vehicle_recommend",
          entities: {},
          filters: {
            bodyType: "suv",
            budgetMax: 2000000,
            priceBasis: "on_road",
            transmission: "automatic",
            activeOnly: true,
            mustHaveFeatures: ["sunroof", "6 airbags"],
            city: "new-delhi",
          },
          ranking: "feature_match",
          output: {
            canvasType: "recommendation_results_canvas",
            inlineType: null,
            groupBy: "model",
          },
        },
      ],
      nextSteps: [
        {
          label: "Show safest options",
          query: "Show safest SUVs under 20 lakh",
          tool: "vehicle_recommend",
          ranking: "safety",
          priority: 85,
        },
        {
          label: "Compare top cars",
          query: "Compare the top recommended cars",
          tool: "vehicle_compare",
          priority: 75,
        },
      ],
      clarification: null,
      confidence: 0.9,
      reasoningSummary:
        "User wants a recommendation filtered by body type, budget, transmission and must-have features.",
    },
  },
  {
    user: "Does Verna SX have sunroof?",
    plan: {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "direct_answer",
      customerStage: "evaluation",
      tools: [
        {
          tool: "vehicle_feature_lookup",
          entities: {
            model: "Verna",
            variant: "SX",
            features: ["sunroof"],
          },
          filters: { city: "new-delhi", activeOnly: true },
          ranking: null,
          output: {
            canvasType: null,
            inlineType: "feature_answer_card",
            groupBy: null,
          },
        },
      ],
      nextSteps: [
        {
          label: "Show all features",
          query: "Show features of Verna SX",
          tool: "vehicle_feature_lookup",
          priority: 85,
        },
      ],
      clarification: null,
      confidence: 0.92,
      reasoningSummary:
        "User is asking for a specific feature on a specific variant.",
    },
  },
  {
    user: "Latest offers on Verna",
    plan: {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "lead_capture",
      customerStage: "closing",
      tools: [
        {
          tool: "aci_lead_capture",
          entities: {
            model: "Verna",
            leadType: "offer_enquiry",
          },
          filters: {
            city: "new-delhi",
            leadType: "offer_enquiry",
            unavailableReason: "offers_not_available",
          },
          ranking: null,
          output: {
            canvasType: "lead_capture_canvas",
            inlineType: null,
            groupBy: null,
          },
        },
      ],
      nextSteps: [
        {
          label: "Get quotation",
          query: "Get quotation for Verna",
          tool: "aci_lead_capture",
          priority: 90,
        },
      ],
      clarification: null,
      confidence: 0.86,
      reasoningSummary:
        "Verified offers are not stored, so this should become an offer enquiry lead instead of inventing discount values.",
      unavailableReason: "offers_not_available",
    },
  },
  {
    user: "What is IVT in Hyundai cars?",
    plan: {
      mode: "single_tool",
      domain: "new_car",
      conversationMode: "education",
      customerStage: "exploration",
      tools: [
        {
          tool: "vehicle_explainer",
          entities: {
            topic: "ivt",
            topics: ["ivt", "automatic_types"],
          },
          filters: {},
          ranking: null,
          output: {
            canvasType: null,
            inlineType: "explainer_card",
            groupBy: null,
          },
        },
      ],
      nextSteps: [
        {
          label: "Show automatic cars",
          query: "Show automatic cars under 20 lakh",
          tool: "vehicle_recommend",
          ranking: "automatic_value",
          priority: 80,
        },
      ],
      clarification: null,
      confidence: 0.9,
      reasoningSummary:
        "User wants a concept explanation, not live vehicle data.",
    },
  },
];

export const buildPlannerPrompt = ({
  message,
  context = {},
  selectedEntity = null,
  filters = {},
} = {}) => {
  const schema = plannerSchemaForPrompt();

  return `
User message:
${truncateText(message, 1200)}

Current safe context:
${safeJsonStringify(sanitizeContextForPlanner(context))}

Selected entity:
${safeJsonStringify(sanitizeSelectedEntityForPlanner(selectedEntity))}

Additional filters:
${safeJsonStringify(sanitizeFiltersForPlanner(filters))}

Planner schema metadata:
${safeJsonStringify(schema)}

Data availability:
${safeJsonStringify(DATA_AVAILABILITY)}

Examples:
${safeJsonStringify(buildPlannerExamples())}

Return only a JSON object matching the planner schema.

Important:
- Do not answer the user.
- Do not invent prices, offers, service centers, service costs, bank schemes, waiting periods, resale values, or dealer stock.
- Use unavailable or aci_lead_capture when the requested data is not stored.
- Use vehicle_explainer for conceptual car-buying questions.
- Use vehicle_recommend for analytical recommendation questions.
- Use nextSteps for safe follow-up questions the user can click next.
- nextSteps must be executable using allowed tools.
- Do not generate "2000000 lakh"; say "20 lakh" in nextStep query text.
`.trim();
};

const runWithTimeout = async (factory, timeoutMs = DEFAULT_TIMEOUT_MS) => {
  const controller = new AbortController();

  let timer = null;

  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      controller.abort();
      reject(new Error(`AI planner timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });

  try {
    return await Promise.race([factory(controller.signal), timeoutPromise]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

const parseJsonFromText = (text = "") => {
  const raw = String(text || "").trim();

  if (!raw) {
    throw new Error("Planner text fallback returned empty output.");
  }

  try {
    return JSON.parse(raw);
  } catch {
    // Try extracting first JSON object from markdown/code-fenced response.
    const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (fenced?.[1]) {
      return JSON.parse(fenced[1].trim());
    }

    const first = raw.indexOf("{");
    const last = raw.lastIndexOf("}");

    if (first >= 0 && last > first) {
      return JSON.parse(raw.slice(first, last + 1));
    }

    throw new Error("Could not parse JSON from planner fallback text.");
  }
};

const callGeminiPlannerObject = async ({
  model,
  system,
  prompt,
  signal,
  debug = false,
}) => {
  const result = await generateObject({
    model,
    schema: AciPlannerSchema,
    system,
    prompt,
    temperature: 0,
    abortSignal: signal,
    providerOptions: {
      google: {
        structuredOutputs: true,
      },
    },
  });

  return {
    rawPlan: result.object,
    usage: result.usage || null,
    finishReason: result.finishReason || null,
    mode: "generateObject",
    debug: debug
      ? {
          warnings: result.warnings || [],
          response: result.response || null,
        }
      : undefined,
  };
};

const callGeminiPlannerTextFallback = async ({
  model,
  system,
  prompt,
  signal,
  debug = false,
}) => {
  const result = await generateText({
    model,
    system,
    prompt: `${prompt}

Return valid JSON only. Do not wrap it in markdown.`,
    temperature: 0,
    abortSignal: signal,
  });

  return {
    rawPlan: parseJsonFromText(result.text),
    usage: result.usage || null,
    finishReason: result.finishReason || null,
    mode: "generateText-json-fallback",
    debug: debug
      ? {
          text: result.text,
          warnings: result.warnings || [],
          response: result.response || null,
        }
      : undefined,
  };
};

export const buildAiPlan = async ({
  message,
  context = {},
  selectedEntity = null,
  filters = {},
  debug = false,
  force = false,
} = {}) => {
  const startedAt = Date.now();
  const cleanMessage = String(message || "").trim();

  if (!cleanMessage) {
    const plan = makeClarificationPlan({
      question: "What would you like to check?",
      domain: "unknown",
      confidence: 0.8,
    });

    return {
      ok: true,
      plan,
      validation: validatePlannerPlan(plan, { message: cleanMessage }),
      provider: "local",
      model: "none",
      plannerMode: "empty-message",
      fallbackRequired: false,
      durationMs: Date.now() - startedAt,
    };
  }

  const domain = classifyPlannerDomain(cleanMessage);

  if (!force && domain === "internal") {
    const plan = makeInternalPassthroughPlan({
      domain: "internal",
      confidence: 0.95,
      summary:
        "Internal CDrive operation should use deterministic backend routing.",
    });

    return {
      ok: true,
      plan,
      validation: validatePlannerPlan(plan, { message: cleanMessage }),
      provider: "local",
      model: "none",
      plannerMode: "internal-passthrough",
      fallbackRequired: true,
      durationMs: Date.now() - startedAt,
    };
  }

  if (!force && domain === "unknown") {
    const plan = makeClarificationPlan({
      question:
        "Are you asking about a new car, pricing, comparison, EMI, or something inside CDrive?",
      domain: "unknown",
      confidence: 0.55,
    });

    return {
      ok: true,
      plan,
      validation: validatePlannerPlan(plan, { message: cleanMessage }),
      provider: "local",
      model: "none",
      plannerMode: "unknown-clarification",
      fallbackRequired: false,
      durationMs: Date.now() - startedAt,
    };
  }

  try {
    const google = createPlannerProvider();
    const modelName = getPlannerModelName();
    const model = google(modelName);
    const system = plannerSystemRules();
    const prompt = buildPlannerPrompt({
      message: cleanMessage,
      context,
      selectedEntity,
      filters,
    });

    const timeoutMs = getPlannerTimeoutMs();

    let plannerResult;
    let objectError = null;

    try {
      plannerResult = await runWithTimeout(
        (signal) =>
          callGeminiPlannerObject({
            model,
            system,
            prompt,
            signal,
            debug,
          }),
        timeoutMs,
      );
    } catch (error) {
      objectError = error;

      plannerResult = await runWithTimeout(
        (signal) =>
          callGeminiPlannerTextFallback({
            model,
            system,
            prompt,
            signal,
            debug,
          }),
        timeoutMs,
      );
    }

    const plan = sanitizePlannerPlan(plannerResult.rawPlan, {
      message: cleanMessage,
    });

    const validation = validatePlannerPlan(plan, {
      message: cleanMessage,
    });

    const confidence = Number(plan?.confidence || 0);
    const fallbackRequired =
      !validation.valid ||
      confidence < LOW_CONFIDENCE_THRESHOLD ||
      plan?.tools?.[0]?.tool === "internal_passthrough";

    return {
      ok: validation.valid,
      plan: validation.plan || plan,
      validation,
      provider: "google",
      model: modelName,
      plannerMode: plannerResult.mode,
      fallbackRequired,
      lowConfidence: confidence < LOW_CONFIDENCE_THRESHOLD,
      usage: plannerResult.usage,
      finishReason: plannerResult.finishReason,
      durationMs: Date.now() - startedAt,
      debug: debug
        ? compactObject({
            domain,
            objectError: objectError?.message || "",
            plannerDebug: plannerResult.debug,
            rawPlan: plannerResult.rawPlan,
          })
        : undefined,
    };
  } catch (error) {
    const plan = makeUnavailablePlan({
      reason: "unsupported_request",
      message: "AI planner failed and deterministic fallback should be used.",
      confidence: 0,
    });

    return {
      ok: false,
      plan,
      validation: validatePlannerPlan(plan, { message: cleanMessage }),
      provider: "google",
      model: getPlannerModelName(),
      plannerMode: "error",
      fallbackRequired: true,
      error: error?.message || "AI planner failed",
      durationMs: Date.now() - startedAt,
      debug: debug
        ? {
            stack: error?.stack,
          }
        : undefined,
    };
  }
};

export const buildAiPlanOrFallback = async (args = {}) => {
  const result = await buildAiPlan(args);

  if (!result.ok || result.fallbackRequired) {
    return {
      ...result,
      shouldUseDeterministicFallback: true,
    };
  }

  return {
    ...result,
    shouldUseDeterministicFallback: false,
  };
};

export default buildAiPlan;
