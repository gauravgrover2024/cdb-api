import {
  applyAciExplicitMessageModelContextOverride,
  buildAciContextModelEntity,
  chooseAciDynamicModelEntity,
} from "./aiAgent.contextPriority.js";
import { resolveAciExplicitMessageModelEntity } from "./aiAgent.modelContextResolver.js";
import { polishAciEarlyFeatureResponseCopy } from "./aiAgent.earlyFeatureGate.responsePolisher.js";
import {
  applyAciFeatureAuthorityContextPatch,
  buildAciFeatureAuthorityContextPatch,
} from "./aiAgent.earlyFeatureGate.contextPatch.js";
import {
  detectAciEarlyDynamicRoutedRequest,
  shouldSkipAciEarlyFeatureGate,
} from "./aiAgent.earlyFeatureGate.detector.js";
import {
  buildAciEarlyGateScopedExecution,
  buildAciEarlyGateToolPlan,
} from "./aiAgent.earlyFeatureGate.planBuilder.js";
import {
  normalizeAciEarlyGateOverviewResponse,
  runAciEarlyGateTool,
} from "./aiAgent.earlyFeatureGate.runner.js";
import { maybeRunAciMultiFeatureAnswer } from "./aiAgent.multiFeatureAnswer.js";
import { maybeRunAciFeatureComparisonAnswer } from "./aiAgent.featureComparisonAnswer.js";


const normalizeAciEarlyGateAliasText = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const compactAciEarlyGateAliasText = (value = "") =>
  normalizeAciEarlyGateAliasText(value).replace(/\s+/g, "");

const escapeAciEarlyGateRegExp = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const sanitizeAciEarlyFeatureCleanUserMessage = ({
  cleanUserMessage = "",
  dynamicModelEntity = {},
} = {}) => {
  const text = normalizeAciEarlyGateAliasText(cleanUserMessage);
  if (!text) return cleanUserMessage;

  const modelAliases = [
    dynamicModelEntity?.fullModel,
    dynamicModelEntity?.displayName,
    dynamicModelEntity?.brand && dynamicModelEntity?.model
      ? `${dynamicModelEntity.brand} ${dynamicModelEntity.model}`
      : "",
    dynamicModelEntity?.make && dynamicModelEntity?.model
      ? `${dynamicModelEntity.make} ${dynamicModelEntity.model}`
      : "",
    dynamicModelEntity?.model,
  ]
    .map(normalizeAciEarlyGateAliasText)
    .filter(Boolean)
    .sort((a, b) => b.length - a.length);

  for (const alias of modelAliases) {
    const aliasCompact = compactAciEarlyGateAliasText(alias);
    if (!aliasCompact) continue;

    const aliasRegex = new RegExp(
      `(^|\\s)${escapeAciEarlyGateRegExp(alias).replace(/\s+/g, "\\s+")}(?=\\s|$)`,
      "i",
    );

    const residual = text
      .replace(aliasRegex, " ")
      .replace(/\s+/g, " ")
      .trim();

    if (!residual) continue;

    const residualCompact = compactAciEarlyGateAliasText(residual);

    // Generic model-alias guard:
    // If the leftover text is just the detected model plus a tiny attached suffix,
    // treat it as a noisy model alias, not a variant.
    // Example: "be 6e" after model "Be 6" -> model-level Be 6, not variant Be 6e.
    if (
      residualCompact.startsWith(aliasCompact) &&
      residualCompact.length > aliasCompact.length &&
      residualCompact.length <= aliasCompact.length + 2
    ) {
      return dynamicModelEntity?.model || cleanUserMessage;
    }
  }

  return cleanUserMessage;
};



export const maybeRunAciPreBridgeMultiFeatureAnswer = async ({
  message = "",
  context = {},
  selectedEntity = null,
} = {}) => {
  if (shouldSkipAciEarlyFeatureGate(message)) {
    return null;
  }

  if (!/\b(and|plus|also|as well as)\b|[,/]/i.test(message || "")) {
    return null;
  }

  const dynamicModelEntityFromText = await resolveAciExplicitMessageModelEntity(message);
  const dynamicModelEntityFromContext = buildAciContextModelEntity({
    context,
    selectedEntity,
  });

  const dynamicModelEntity = chooseAciDynamicModelEntity({
    textEntity: dynamicModelEntityFromText,
    contextEntity: dynamicModelEntityFromContext,
    message,
  });

  const multiFeatureAnswer = await maybeRunAciMultiFeatureAnswer({
    message,
    modelEntity: dynamicModelEntity,
    context,
    allowSingleFeature: false,
  });

  return multiFeatureAnswer?.intent === "vehicle_multi_feature_answer"
    ? multiFeatureAnswer
    : null;
};

export const maybeRunAciEarlyFeatureGate = async ({
  message = "",
  context = {},
  selectedEntity = null,
  filters = {},
} = {}) => {
  const featureComparisonAnswer = await maybeRunAciFeatureComparisonAnswer({
    message,
    context,
  });

  if (featureComparisonAnswer) {
    return featureComparisonAnswer;
  }

  if (shouldSkipAciEarlyFeatureGate(message)) {
    return null;
  }

  const dynamicModelEntityFromText = await resolveAciExplicitMessageModelEntity(message);

  applyAciExplicitMessageModelContextOverride({
    message,
    context,
    dynamicModelEntity: dynamicModelEntityFromText,
  });
  const dynamicModelEntityFromContext = buildAciContextModelEntity({
    context,
    selectedEntity,
  });

  const dynamicModelEntity = chooseAciDynamicModelEntity({
    textEntity: dynamicModelEntityFromText,
    contextEntity: dynamicModelEntityFromContext,
    message,
  });

  const multiFeatureAnswer = await maybeRunAciMultiFeatureAnswer({
    message,
    modelEntity: dynamicModelEntity,
    context,
  });

  if (multiFeatureAnswer) {
    return multiFeatureAnswer;
  }

  const detected =
    detectAciEarlyDynamicRoutedRequest({
      message,
      modelEntity: dynamicModelEntity,
    });

  if (!detected) {
    const catalogSingleFeatureAnswer = await maybeRunAciMultiFeatureAnswer({
      message,
      modelEntity: dynamicModelEntity,
      context,
      allowSingleFeature: true,
    });

    if (catalogSingleFeatureAnswer) {
      return catalogSingleFeatureAnswer;
    }

    return null;
  }

  const detectedHasFeature =
    Boolean(detected.feature) ||
    Boolean(detected.featureKey) ||
    Boolean(detected.category) ||
    Boolean(detected.categoryKey);

  const detectedLooksLikeOverview =
    detected.intent === "vehicle_overview" ||
    detected.canvasType === "car_overview_canvas" ||
    !detectedHasFeature;

  if (detectedLooksLikeOverview && !detectedHasFeature) {
    const catalogSingleFeatureAnswer = await maybeRunAciMultiFeatureAnswer({
      message,
      modelEntity: dynamicModelEntity,
      context,
      allowSingleFeature: true,
    });

    if (catalogSingleFeatureAnswer) {
      return catalogSingleFeatureAnswer;
    }
  }

  const cleanUserMessage = sanitizeAciEarlyFeatureCleanUserMessage({
    cleanUserMessage: detected.cleanUserMessage || message,
    dynamicModelEntity,
  });

  detected.cleanUserMessage = cleanUserMessage;

  const toolPlan = buildAciEarlyGateToolPlan({
    detected,
    dynamicModelEntity,
    filters,
  });

  const preToolAuthorityContextPatch = buildAciFeatureAuthorityContextPatch({
    context,
    detected,
    dynamicModelEntity,
    cleanUserMessage,
    message,
  });

  const {
    scopedSelectedEntity,
    scopedFeatureContext,
    scopedFeatureFilters,
    scopedDetected,
    scopedToolPlan,
  } = buildAciEarlyGateScopedExecution({
    detected,
    toolPlan,
    context,
    filters,
    selectedEntity,
    preToolAuthorityContextPatch,
  });

  let response = await runAciEarlyGateTool({
    detected,
    scopedDetected,
    scopedFeatureFilters,
    scopedFeatureContext,
    scopedToolPlan,
    scopedSelectedEntity,
    cleanUserMessage,
  });

  const overviewResult = normalizeAciEarlyGateOverviewResponse({
    response,
    detected,
    preToolAuthorityContextPatch,
  });

  response = overviewResult.response;
  const overviewAuthorityContextPatch = overviewResult.overviewAuthorityContextPatch;

  polishAciEarlyFeatureResponseCopy(response, { detected, cleanUserMessage, message });

  const authorityContextPatch =
    overviewAuthorityContextPatch || preToolAuthorityContextPatch;

  applyAciFeatureAuthorityContextPatch(response, authorityContextPatch);



  return {
    ...response,
    meta: {
      ...(response?.meta || {}),
      earlyFeatureGate: true,
      detectedModel: detected.model,
      detectedFullModel: detected.fullModel || "",
      detectedFeature: detected.feature,
      detectedCategory: detected.category || "",
      detectedCategoryLabel: detected.categoryLabel || "",
      modelMatchedText: dynamicModelEntity?.matchedText || "",
      modelCorrectionConfidence: dynamicModelEntity?.confidence || null,
    },
  };
};
