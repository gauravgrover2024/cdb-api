/**
 * ACI Assist V2 Official Response Contract
 *
 * Purpose:
 * This is the canonical backend response contract for ACI Assist.
 *
 * Rules:
 * - Frontend should render from this shape.
 * - Mastra must eventually produce this shape.
 * - Testing agent must validate this shape.
 * - Public/private/WhatsApp channels must respect this shape.
 * - AI must not invent factual car data.
 */

export const ACI_V2_RESPONSE_CONTRACT_VERSION = "aci-v2-response-contract-v1";

export const ACI_CHANNELS = Object.freeze({
  PUBLIC_WEB: "public_web",
  INTERNAL_WEB: "internal_web",
  WHATSAPP: "whatsapp",
});

export const ACI_DISPLAY_MODES = Object.freeze({
  INLINE: "inline",
  CANVAS: "canvas",
  BOTH: "both",
});

export const ACI_TOOL_NAMES = Object.freeze({
  VEHICLE_PRICELIST: "vehicle_pricelist",
  VEHICLE_FEATURE_LOOKUP: "vehicle_feature_lookup",
  VEHICLE_COLORS: "vehicle_colors",
  VEHICLE_COMPARE: "vehicle_compare",
  VEHICLE_EMI: "vehicle_emi",
  VEHICLE_RECOMMEND: "vehicle_recommend",
  VEHICLE_SCORE_INSIGHT: "vehicle_score_insight",
  VEHICLE_VARIANT_ADVISOR: "vehicle_variant_advisor",
  VEHICLE_OFFERS: "vehicle_offers",
  VEHICLE_OVERVIEW: "vehicle_overview",
  ACI_LEAD_CAPTURE: "aci_lead_capture",
  INTERNAL_PASSTHROUGH: "internal_passthrough",
  UNAVAILABLE: "unavailable",
});

export const ACI_CANVAS_TYPES = Object.freeze({
  PRICELIST: "pricelist_canvas",
  FEATURE_EXPLORER: "feature_explorer_canvas",
  COLORS: "colors_canvas",
  COMPARISON: "comparison_canvas",
  EMI: "emi_calculator_canvas",
  QUOTATION: "aci_quotation_canvas",
  OFFERS: "offers_canvas",
  RECOMMENDATION: "recommendation_canvas",
  VARIANT_ADVISOR: "variant_advisor_canvas",
  OVERVIEW: "car_overview_canvas",
  SAFETY: "safety_canvas",
});

export const ACI_INLINE_TYPES = Object.freeze({
  FEATURE_ANSWER: "feature_answer_card",
  PRICE_ANSWER: "price_answer_card",
  COLOR_ANSWER: "color_answer_card",
  UNAVAILABLE_NOTICE: "unavailable_notice",
});

export const ACI_PUBLIC_ALLOWED_TOOLS = Object.freeze([
  ACI_TOOL_NAMES.VEHICLE_PRICELIST,
  ACI_TOOL_NAMES.VEHICLE_FEATURE_LOOKUP,
  ACI_TOOL_NAMES.VEHICLE_COLORS,
  ACI_TOOL_NAMES.VEHICLE_COMPARE,
  ACI_TOOL_NAMES.VEHICLE_EMI,
  ACI_TOOL_NAMES.VEHICLE_RECOMMEND,
  ACI_TOOL_NAMES.VEHICLE_SCORE_INSIGHT,
  ACI_TOOL_NAMES.VEHICLE_VARIANT_ADVISOR,
  ACI_TOOL_NAMES.VEHICLE_OFFERS,
  ACI_TOOL_NAMES.VEHICLE_OVERVIEW,
  ACI_TOOL_NAMES.ACI_LEAD_CAPTURE,
  ACI_TOOL_NAMES.UNAVAILABLE,
]);

export const ACI_INTERNAL_ALLOWED_TOOLS = Object.freeze([
  ...ACI_PUBLIC_ALLOWED_TOOLS,
  ACI_TOOL_NAMES.INTERNAL_PASSTHROUGH,
]);

export const ACI_WHATSAPP_ALLOWED_TOOLS = Object.freeze([
  ACI_TOOL_NAMES.VEHICLE_PRICELIST,
  ACI_TOOL_NAMES.VEHICLE_FEATURE_LOOKUP,
  ACI_TOOL_NAMES.VEHICLE_COMPARE,
  ACI_TOOL_NAMES.VEHICLE_EMI,
  ACI_TOOL_NAMES.VEHICLE_RECOMMEND,
  ACI_TOOL_NAMES.VEHICLE_SCORE_INSIGHT,
  ACI_TOOL_NAMES.ACI_LEAD_CAPTURE,
  ACI_TOOL_NAMES.UNAVAILABLE,
]);

export const createEmptyAciContextPatch = () => ({
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
  customerStage: "",
  conversationMode: "",
});

export const isPlainObject = (value) =>
  Boolean(value && typeof value === "object" && !Array.isArray(value));

export const asArray = (value) => (Array.isArray(value) ? value : []);

export const getAllowedToolsForChannel = (channel = ACI_CHANNELS.PUBLIC_WEB) => {
  if (channel === ACI_CHANNELS.INTERNAL_WEB) return ACI_INTERNAL_ALLOWED_TOOLS;
  if (channel === ACI_CHANNELS.WHATSAPP) return ACI_WHATSAPP_ALLOWED_TOOLS;
  return ACI_PUBLIC_ALLOWED_TOOLS;
};

export const collectToolNamesFromResponse = (response = {}) => {
  const names = [];

  // Important:
  // response.intent is a user/product intent, not necessarily an executable tool.
  // Example: vehicle_emi_calculator maps to the vehicle_emi tool.
  // Permission checks must apply to actual executed/planned tools only.
  for (const tool of asArray(response.plannerTools)) {
    if (typeof tool === "string" && tool.trim()) names.push(tool.trim());
  }

  for (const meta of asArray(response.runtimeResultsMeta)) {
    if (meta?.tool) names.push(meta.tool);
  }

  for (const secondary of asArray(response.secondaryResponses)) {
    for (const tool of asArray(secondary?.plannerTools)) {
      if (typeof tool === "string" && tool.trim()) names.push(tool.trim());
    }

    for (const meta of asArray(secondary?.runtimeResultsMeta)) {
      if (meta?.tool) names.push(meta.tool);
    }
  }

  return [...new Set(names.filter(Boolean))];
};

export const validateAciResponseContract = (
  response = {},
  {
    channel = ACI_CHANNELS.PUBLIC_WEB,
    requireData = true,
    requireService = true,
  } = {},
) => {
  const errors = [];
  const warnings = [];

  if (!isPlainObject(response)) {
    return {
      valid: false,
      errors: ["Response is not an object."],
      warnings,
    };
  }

  const displayMode = response.displayMode || "";
  const canvasType = response.canvasType || "";
  const inlineType = response.inlineType || "";

  if (!response.intent || typeof response.intent !== "string") {
    errors.push("Missing string intent.");
  }

  if (!Object.values(ACI_DISPLAY_MODES).includes(displayMode)) {
    errors.push("Missing or invalid displayMode.");
  }

  if (displayMode === ACI_DISPLAY_MODES.CANVAS && !canvasType) {
    errors.push("Canvas displayMode requires canvasType.");
  }

  if (displayMode === ACI_DISPLAY_MODES.INLINE && !inlineType && !response.answer) {
    errors.push("Inline displayMode requires inlineType or answer.");
  }

  if (displayMode === ACI_DISPLAY_MODES.BOTH && !canvasType && !inlineType) {
    errors.push("Both displayMode requires canvasType or inlineType.");
  }

  if (typeof response.title !== "string") {
    errors.push("Missing string title.");
  }

  if (typeof response.answer !== "string") {
    errors.push("Missing string answer.");
  }

  if (!Array.isArray(response.actions)) {
    errors.push("actions must be an array.");
  }

  if (!Array.isArray(response.leadingQuestions)) {
    errors.push("leadingQuestions must be an array.");
  }

  if (!isPlainObject(response.contextPatch)) {
    errors.push("contextPatch must be an object.");
  } else {
    const patch = response.contextPatch;
    if (!("selectedVehicle" in patch)) {
      warnings.push("contextPatch.selectedVehicle missing.");
    }
    if (!("anchorModel" in patch)) {
      warnings.push("contextPatch.anchorModel missing.");
    }
    if (!("anchorVariant" in patch)) {
      warnings.push("contextPatch.anchorVariant missing.");
    }
    if (!("anchorCity" in patch)) {
      warnings.push("contextPatch.anchorCity missing.");
    }
  }

  if (!Array.isArray(response.secondaryResponses)) {
    errors.push("secondaryResponses must be an array.");
  }

  if (!Array.isArray(response.runtimeResultsMeta)) {
    errors.push("runtimeResultsMeta must be an array.");
  }

  if (requireData && !isPlainObject(response.data)) {
    errors.push("data must be an object.");
  }

  if (requireService && !isPlainObject(response.service)) {
    errors.push("service must be an object.");
  }

  if (response.oldSystemUsed === true) {
    errors.push("oldSystemUsed must never be true.");
  }

  if (response.contractValid === false) {
    errors.push("response.contractValid is false.");
  }

  const allowedTools = getAllowedToolsForChannel(channel);
  const toolNames = collectToolNamesFromResponse(response);

  for (const tool of toolNames) {
    if (tool && !allowedTools.includes(tool)) {
      errors.push(`Tool "${tool}" is not allowed for channel "${channel}".`);
    }
  }

  if (
    channel !== ACI_CHANNELS.INTERNAL_WEB &&
    toolNames.includes(ACI_TOOL_NAMES.INTERNAL_PASSTHROUGH)
  ) {
    errors.push("internal_passthrough is forbidden outside internal_web.");
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    toolNames,
  };
};

export const normalizeAciResponseContractShell = (response = {}) => ({
  intent: response.intent || "",
  mode: response.mode || "single_tool",
  displayMode: response.displayMode || ACI_DISPLAY_MODES.INLINE,
  canvasType: response.canvasType || "",
  inlineType: response.inlineType || "",
  title: response.title || "",
  answer: response.answer || "",
  actions: Array.isArray(response.actions) ? response.actions : [],
  leadingQuestions: Array.isArray(response.leadingQuestions)
    ? response.leadingQuestions
    : [],
  contextPatch: isPlainObject(response.contextPatch)
    ? response.contextPatch
    : createEmptyAciContextPatch(),
  secondaryResponses: Array.isArray(response.secondaryResponses)
    ? response.secondaryResponses
    : [],
  runtimeResultsMeta: Array.isArray(response.runtimeResultsMeta)
    ? response.runtimeResultsMeta
    : [],
  data: isPlainObject(response.data) ? response.data : {},
  service: isPlainObject(response.service) ? response.service : {},
  contractVersion:
    response.contractVersion || ACI_V2_RESPONSE_CONTRACT_VERSION,
});
