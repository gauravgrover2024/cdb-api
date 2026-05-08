import mongoose from "mongoose";
import {
  filterChip,
  sourceTransparency,
} from "./aiAgent.renderPayloads.js";
import {
  firstMeaningful,
  formatDateValue,
  getRegistration,
  getVehicleName,
  makeRegex,
  normalizeText,
  registrationConditions,
} from "./aiAgent.normalizers.js";
import {
  getIntentForWidgetType,
  getNewCarQuestionConfig,
  mapIntentAlias,
} from "./aiAgent.newCarQuestionMap.js";

export const LIMIT = 50;
export const QUERY_TIMEOUT_MS = 3000;

export const safeId = (doc) => String(doc?._id || doc?.id || "");

export const objectIdOrNull = (value) =>
  mongoose.Types.ObjectId.isValid(value) ? new mongoose.Types.ObjectId(value) : null;

export const pushModuleTrace = (trace, module, matched = 0, extra = {}) => {
  trace.push({ module, matched, ...extra });
};

export const findLean = (Model, query, options = {}) => {
  let builder = Model.find(query);
  if (options.select) builder = builder.select(options.select);
  if (options.sort) builder = builder.sort(options.sort);
  if (options.limit) builder = builder.limit(options.limit);
  return builder.maxTimeMS(options.maxTimeMS || QUERY_TIMEOUT_MS).lean();
};

export const countDocumentsSafe = async (Model, query, options = {}) => {
  try {
    return {
      count: await Model.countDocuments(query).maxTimeMS(options.maxTimeMS || QUERY_TIMEOUT_MS),
      approximate: false,
    };
  } catch (error) {
    return {
      count: options.fallbackCount || 0,
      approximate: true,
      error: error.message,
    };
  }
};

export const findAndCount = async (Model, query, options = {}) => {
  const [rowsResult, countResult] = await Promise.allSettled([
    findLean(Model, query, options),
    countDocumentsSafe(Model, query, options),
  ]);
  const rows = rowsResult.status === "fulfilled" ? rowsResult.value : [];
  const countPayload =
    countResult.status === "fulfilled"
      ? countResult.value
      : { count: rows.length, approximate: true, error: countResult.reason?.message };
  return {
    rows,
    count: countPayload.count || rows.length,
    approximate: countPayload.approximate,
    error: rowsResult.status === "rejected" ? rowsResult.reason?.message : countPayload.error,
  };
};

export const buildTextClauses = (fields, value) => {
  const regex = makeRegex(value);
  return regex ? fields.map((field) => ({ [field]: regex })) : [];
};

export const buildEntityQuery = ({
  customerFields = [],
  registrationFields = [],
  vehicleFields = [],
  entities = {},
} = {}) => {
  const and = [];
  const customer = buildTextClauses(customerFields, entities.customerName);
  if (customer.length) and.push({ $or: customer });

  const registration = registrationConditions(
    registrationFields,
    entities.registrationNumber,
    entities.last4,
  );
  if (registration.length) and.push({ $or: registration });

  const vehicleNeedles = [entities.make, entities.model, entities.variant].filter(Boolean);
  for (const needle of vehicleNeedles) {
    const clauses = buildTextClauses(vehicleFields, needle);
    if (clauses.length) and.push({ $or: clauses });
  }

  return and.length ? { $and: and } : {};
};

export const canSearchByEntity = (entities = {}) =>
  Boolean(entities.customerName || entities.registrationNumber || entities.last4 || entities.model);

export const getLoanRoute = (loan) => `/loans/edit/${safeId(loan)}`;
export const getInsuranceRoute = (insuranceCase) =>
  `/insurance/edit/${insuranceCase?.caseId || safeId(insuranceCase)}`;
export const getCustomerRoute = (customer) => `/customers/edit/${safeId(customer)}`;
export const getPaymentRoute = (loan) => `/payments/${loan?.loanId || safeId(loan)}`;
export const getUsedCarRoute = (lead) => `/used-cars/leads/${safeId(lead)}`;

export const rowBase = (doc = {}) => ({
  id: safeId(doc),
  customer: firstMeaningful(doc.customerName, doc.companyName, doc?.seller?.name),
  vehicle: getVehicleName(doc),
  registrationNumber: getRegistration(doc),
  status: firstMeaningful(doc.status, doc.loanStatus, doc.currentStage, doc?.workflow?.status),
  createdAt: formatDateValue(doc.createdAt),
  updatedAt: formatDateValue(doc.updatedAt),
});

export const makeAmbiguity = (options) => {
  if (!options?.length) return null;
  return {
    message: "I found multiple possible matches. Which one do you mean?",
    options: options.slice(0, 8).map((option) => ({
      id: option.id,
      entityType: option.entityType,
      displayName: option.displayName,
      customerName: option.customerName,
      vehicle: option.vehicle,
      registrationNumber: option.registrationNumber,
      module: option.module,
      status: option.status,
      lastActivityDate: option.lastActivityDate,
      context: option.context,
    })),
  };
};

export const buildFilters = (parsed, moduleName = "") =>
  [
    filterChip("intent", "Intent", parsed.intent.replace(/_/g, " ")),
    filterChip("customer", "Customer", parsed.entities.customerName),
    filterChip("make", "Make", parsed.entities.make),
    filterChip("model", "Model", parsed.entities.model),
    filterChip("variant", "Variant", parsed.entities.variant),
    filterChip("last4", "Vehicle Last 4", parsed.entities.last4),
    filterChip("module", "Module", moduleName),
    ...parsed.statusTerms.map((status) => filterChip(`status_${status}`, "Status", status)),
  ].filter(Boolean);

export const assembleResponse = ({
  parsed,
  assistantMessage,
  resultType = "answer",
  widgets = [],
  modulesChecked = [],
  filtersApplied = [],
  followUpSuggestions = [],
  actions = [],
  leadingQuestions = [],
  ambiguity,
  access,
  queryPlan,
  filters,
  conversationSuggestions = [],
  contextSnapshot = null,
  salesNudges = [],
  closingActions = [],
  conversationMode = "",
  conversationStage = "",
  userProfile = null,
}) => {
  const primaryWidget = widgets?.[0] || {};
  const canonicalIntent = mapIntentAlias(parsed.intent);
  const widgetIntent = getIntentForWidgetType(primaryWidget.type || "");
  const resolvedIntent =
    primaryWidget.type === "model_ambiguity" ||
    primaryWidget.type === "variant_ambiguity"
      ? widgetIntent || canonicalIntent
      : canonicalIntent || widgetIntent;
  const questionConfig =
    getNewCarQuestionConfig(resolvedIntent) ||
    getNewCarQuestionConfig(canonicalIntent);
  const secondaryConfigs = (parsed.secondaryIntents || [])
    .map((intent) => getNewCarQuestionConfig(intent))
    .filter(Boolean);

  const inferCanvasTypeFromWidget = (type = "") =>
    ({
      vehicle_pricelist: "pricelist_canvas",
      vehicle_price_breakup: "price_breakup_canvas",
      vehicle_colors: "color_studio_canvas",
      vehicle_color_search: "color_studio_canvas",
      vehicle_features: "feature_explorer_canvas",
      vehicle_feature_discovery: "feature_explorer_canvas",
      vehicle_feature_answer: null,
      vehicle_model_comparison: "comparison_canvas",
      vehicle_variant_difference: "variant_upgrade_value_canvas",
      vehicle_variant_recommendation: "variant_finder_canvas",
      vehicle_emi_calculator: "emi_calculator_canvas",
      vehicle_emi_recommendations: "emi_calculator_canvas",
      similar_cars: "similar_cars_canvas",
      vehicle_recommendation_results: "recommendation_results_canvas",
      vehicle_safety_results: "safety_advisor_canvas",
      vehicle_spec_ranking: "performance_spec_ranking_canvas",
      model_ambiguity: null,
      variant_ambiguity: null,
      unavailable_notice: null,
    })[type] || null;

  const inferInlineTypeFromWidget = (type = "") =>
    ({
      vehicle_feature_answer: "feature_answer_card",
      model_ambiguity: "model_ambiguity_card",
      variant_ambiguity: "variant_ambiguity_card",
      unavailable_notice: "fallback_card",
    })[type] || null;

  const slugValue = (value = "") =>
    String(value || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");

  const displayMode =
    questionConfig?.displayMode ||
    (questionConfig?.canvasType
      ? "canvas"
      : questionConfig?.inlineType
        ? "inline"
        : inferInlineTypeFromWidget(primaryWidget.type)
          ? "inline"
          : "canvas");

  const canvasType =
    questionConfig?.canvasType ??
    primaryWidget.canvasType ??
    inferCanvasTypeFromWidget(primaryWidget.type);

  const inlineType =
    questionConfig?.inlineType ??
    primaryWidget.inlineType ??
    inferInlineTypeFromWidget(primaryWidget.type);

  const normalizeAction = (item = {}) => ({
    id: item.id || "",
    label: item.label || item.text || item.title || "",
    type: item.type || item.action || item.kind || "ask",
    query: item.query || item.message || item.followUpQuery || "",
    canvasType: item.canvasType || "",
    leadType: item.leadType || "",
    route: item.route || "",
    intent: item.intent || "",
    entities: item.entities || {},
    contextPatch: item.contextPatch || item.context || {},
    icon: item.icon || "",
    tone: item.tone || "",
  });

  const dedupeActions = (items = []) => {
    const seen = new Set();
    return items
      .map(normalizeAction)
      .filter((item) => item.label || item.query)
      .filter((item) => {
        const key = [
          item.label,
          item.type,
          item.query,
          item.canvasType,
          item.leadType,
          item.route,
        ]
          .filter(Boolean)
          .join("|")
          .toLowerCase();
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .map((item, index) => ({
        ...item,
        id:
          item.id ||
          `act-${slugValue(item.intent || resolvedIntent || "next")}-${slugValue(item.label || item.query || item.type || "action")}-${index + 1}`,
      }));
  };

  const normalizedConversationSuggestions = (conversationSuggestions || [])
    .filter(Boolean)
    .map((item) => ({
      ...item,
      title: item.title || item.label || item.query || "",
      query: item.query || item.message || "",
    }))
    .filter((item) => item.title || item.query);

  const finalActions = dedupeActions(
    normalizedConversationSuggestions.length
      ? normalizedConversationSuggestions.map((item) => ({
          id: item.id,
          label: item.title,
          type: item.type || "ask",
          query: item.query || "",
          canvasType: item.canvasType || "",
          leadType: item.leadType || item.contextPatch?.leadType || "",
          route: item.route || "",
          intent: item.intent,
          entities: item.entities || {},
          contextPatch: item.contextPatch || {},
          icon: item.icon || "",
          tone: item.tone || "",
        }))
      : [
          ...(actions || []),
          ...(primaryWidget.actions || []),
          ...(questionConfig?.defaultActions || []),
          ...secondaryConfigs.flatMap((config) =>
            (config.defaultActions || []).slice(0, 2),
          ),
        ],
  );
  const actionsWithFallback = finalActions.length
    ? finalActions
    : dedupeActions([
        {
          label: "Show price",
          type: "open_canvas",
          canvasType: "pricelist_canvas",
        },
        {
          label: "Calculate EMI",
          type: "open_canvas",
          canvasType: "emi_calculator_canvas",
        },
        {
          label: "Get quotation",
          type: "open_canvas",
          canvasType: "aci_quotation_canvas",
        },
      ]);

  const normalizeLeadingQuestion = (item, index = 0) => {
    if (typeof item === "string") {
      return {
        id: `lead-q-${slugValue(resolvedIntent || "next")}-${slugValue(item || "question")}-${index + 1}`,
        label: item,
        query: item,
        intent: resolvedIntent,
        displayMode:
          questionConfig?.displayMode ||
          (questionConfig?.canvasType ? "canvas" : "inline"),
        canvasType: questionConfig?.canvasType || undefined,
        inlineType: questionConfig?.inlineType || undefined,
        entities: {},
        contextPatch: {},
      };
    }
    const label = item?.label || item?.query || "";
    const query = item?.query || item?.label || "";
    const intent = item?.intent || resolvedIntent;
    return {
      id:
        item?.id ||
        `lead-q-${slugValue(intent || "next")}-${slugValue(label || query || "question")}-${index + 1}`,
      label,
      query,
      intent,
      displayMode:
        item?.displayMode ||
        questionConfig?.displayMode ||
        (questionConfig?.canvasType ? "canvas" : "inline"),
      canvasType: item?.canvasType || questionConfig?.canvasType || undefined,
      inlineType: item?.inlineType || questionConfig?.inlineType || undefined,
      entities: item?.entities || {},
      contextPatch: item?.contextPatch || item?.context || {},
    };
  };

  const finalLeadingQuestions = (
    normalizedConversationSuggestions.length
      ? normalizedConversationSuggestions
          .filter((item) =>
            ["question", "clarification"].includes(item.kind || ""),
          )
          .map((item, index) =>
            normalizeLeadingQuestion({
              id: item.id,
              label: item.title,
              query: item.query,
              intent: item.intent || resolvedIntent,
              displayMode: item.displayMode || undefined,
              canvasType: item.canvasType || undefined,
              inlineType: item.inlineType || undefined,
              entities: item.entities || {},
              contextPatch: item.contextPatch || {},
            }, index),
          )
      : [
          ...(leadingQuestions || []),
          ...(questionConfig?.leadingQuestions || []),
        ].map((item, index) => normalizeLeadingQuestion(item, index))
  )
    .filter((item) => item.label && item.query)
    .slice(0, 10);

  const finalFollowUpSuggestions = normalizedConversationSuggestions.length
    ? normalizedConversationSuggestions.map((item) => ({
        id: item.id,
        label: item.title,
        title: item.title,
        subtitle: item.subtitle || "",
        query: item.query,
        message: item.query,
        intent: item.intent || resolvedIntent,
        kind: item.kind || "",
        type: item.type || "ask",
        icon: item.icon || "",
        tone: item.tone || "",
        entities: item.entities || {},
        contextPatch: item.contextPatch || {},
        canvasType: item.canvasType || null,
        inlineType: item.inlineType || null,
        leadType: item.leadType || item.contextPatch?.leadType || "",
        priority: item.priority,
        adaptiveScore: item.adaptiveScore,
        context: {
          ...(item.contextPatch || {}),
          actionContext: item,
          entities: item.entities || {},
        },
      }))
    : followUpSuggestions;

  const response = {
    assistantMessage,
    intent: resolvedIntent || parsed.intent,
    displayMode,
    canvasType: canvasType || null,
    inlineType: inlineType || null,
    title: primaryWidget.title || "ACI Assist",
    answer: assistantMessage,
    actions: actionsWithFallback,
    leadingQuestions: finalLeadingQuestions,
    entities: parsed.entities,
    context: {
      history: contextSnapshot?.history || {},
      profile: contextSnapshot?.profile || {},
      mode: contextSnapshot?.mode || "",
      stage: contextSnapshot?.stage || "",
      buyingSignals: contextSnapshot?.buyingSignals || [],
      intent: resolvedIntent || parsed.intent,
      lastIntent: resolvedIntent || parsed.intent,
      previousIntent: parsed.intent,
      entities: parsed.entities || {},
      model:
        contextSnapshot?.anchorModel ||
        contextSnapshot?.model ||
        parsed.entities?.model ||
        "",
      variant:
        contextSnapshot?.anchorVariant ||
        contextSnapshot?.variant ||
        parsed.entities?.variant ||
        "",
      city:
        contextSnapshot?.city ||
        contextSnapshot?.requestedCity ||
        parsed.entities?.city ||
        "",
    },
    confidence: parsed.confidence,
    filters: filters || buildFilters(parsed),
    resultType,
    widgets,
    sourceTransparency: sourceTransparency({
      modulesChecked,
      filtersApplied,
      accessRestrictions: access?.restrictions || [],
    }),
    followUpSuggestions: finalFollowUpSuggestions,
    conversationSuggestions: normalizedConversationSuggestions,
    suggestions: normalizedConversationSuggestions.slice(0, 5),
    salesNudges: contextSnapshot?.salesNudges || [],
    closingActions: contextSnapshot?.closingActions || [],
    userProfile: contextSnapshot?.profile || {},
    stage: contextSnapshot?.stage || "",
    mode: contextSnapshot?.mode || "",
    contextSnapshot,
    secondaryIntents: parsed.secondaryIntents || [],
  };
  if (ambiguity) response.ambiguity = ambiguity;
  if (queryPlan) response.queryPlan = queryPlan;
  return response;
};

export const latestActivity = (doc) =>
  formatDateValue(
    firstMeaningful(doc?.updatedAt, doc?.createdAt, doc?.newIssueDate, doc?.newPolicyStartDate),
  );

export const entityOption = (doc, module, entityType, context = {}) => ({
  id: safeId(doc),
  entityType,
  displayName: normalizeText(
    [firstMeaningful(doc.customerName, doc.companyName, doc?.seller?.name), getVehicleName(doc), getRegistration(doc)]
      .filter(Boolean)
      .join(" - "),
  ),
  customerName: firstMeaningful(doc.customerName, doc.companyName, doc?.seller?.name),
  vehicle: getVehicleName(doc),
  registrationNumber: getRegistration(doc),
  module,
  status: firstMeaningful(doc.status, doc.loanStatus, doc.currentStage, doc?.workflow?.status),
  lastActivityDate: latestActivity(doc),
  context: {
    customerName: firstMeaningful(doc.customerName, doc.companyName, doc?.seller?.name),
    registrationNumber: getRegistration(doc),
    last4: getRegistration(doc).replace(/\D/g, "").slice(-4),
    ...context,
  },
});
