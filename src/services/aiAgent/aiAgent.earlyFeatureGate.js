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

  if (!detected) return null;

  const cleanUserMessage = detected.cleanUserMessage || message;

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
