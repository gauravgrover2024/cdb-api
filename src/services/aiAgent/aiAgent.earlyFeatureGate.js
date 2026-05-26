import {
  applyAciExplicitMessageModelContextOverride,
  buildAciContextModelEntity,
  chooseAciDynamicModelEntity,
} from "./aiAgent.contextPriority.js";
import { resolveAciExplicitMessageModelEntity } from "./aiAgent.modelContextResolver.js";
import { runVehicleFeaturesTool } from "./tools/newCars/vehicleFeatures.tool.js";
import { runVehiclePricelistNewCarsTool } from "./tools/newCars/vehiclePricelist.tool.js";
import { polishAciEarlyFeatureResponseCopy } from "./aiAgent.earlyFeatureGate.responsePolisher.js";
import {
  applyAciFeatureAuthorityContextPatch,
  buildAciFeatureAuthorityContextPatch,
} from "./aiAgent.earlyFeatureGate.contextPatch.js";
import {
  detectAciEarlyDynamicRoutedRequest,
  shouldSkipAciEarlyFeatureGate,
} from "./aiAgent.earlyFeatureGate.detector.js";

export const maybeRunAciEarlyFeatureGate = async ({
  message = "",
  context = {},
  selectedEntity = null,
  filters = {},
} = {}) => {
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

  const detected =
    detectAciEarlyDynamicRoutedRequest({
      message,
      modelEntity: dynamicModelEntity,
    });

  if (!detected) return null;

  const toolRunner =
    detected.intent === "vehicle_overview"
      ? runVehiclePricelistNewCarsTool
      : detected.intent === "vehicle_pricelist" ||
    detected.canvasType === "pricelist_canvas"
      ? runVehiclePricelistNewCarsTool
      : runVehicleFeaturesTool;

  const cleanUserMessage = detected.cleanUserMessage || message;

  const toolPlan = {
    tool: detected.intent,
    intent: detected.intent,
    toolIntent: detected.intent,
    canvasType: detected.canvasType,
    entities: {
      make: detected.make || dynamicModelEntity?.brand || "",
      brand: detected.brand || dynamicModelEntity?.brand || "",
      model: detected.model,
      fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      feature: detected.feature || "",
      variants: detected.variants || [],
      variant: detected.variant || "",
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
    input: {
      make: detected.make || dynamicModelEntity?.brand || "",
      brand: detected.brand || dynamicModelEntity?.brand || "",
      model: detected.model,
      fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      feature: detected.feature || "",
      variants: detected.variants || [],
      variant: detected.variant || "",
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
    filters: {
      ...(filters || {}),
      make: detected.make || dynamicModelEntity?.brand || "",
      brand: detected.brand || dynamicModelEntity?.brand || "",
      model: detected.model,
      fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      feature: detected.feature || "",
      variants: detected.variants || [],
      variant: detected.variant || "",
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
  };

  const preToolAuthorityContextPatch = buildAciFeatureAuthorityContextPatch({
    context,
    detected,
    dynamicModelEntity,
    cleanUserMessage,
    message,
  });

  const scopedAnchorVariant = String(
    preToolAuthorityContextPatch.anchorVariant || "",
  );
  const scopedSelectedVehicle = {
    ...(preToolAuthorityContextPatch.selectedVehicle || {}),
    variant: scopedAnchorVariant,
    variantName: scopedAnchorVariant,
  };

  const scopedSelectedEntity =
    selectedEntity && typeof selectedEntity === "object"
      ? {
          ...selectedEntity,
          selectedVehicle: {
            ...(selectedEntity.selectedVehicle || selectedEntity.vehicle || {}),
            ...scopedSelectedVehicle,
          },
          vehicle: {
            ...(selectedEntity.vehicle || selectedEntity.selectedVehicle || {}),
            ...scopedSelectedVehicle,
          },
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
        }
      : {
          selectedVehicle: scopedSelectedVehicle,
          vehicle: scopedSelectedVehicle,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
        };

  const scopedFeatureContext = {
    ...(context || {}),
    ...preToolAuthorityContextPatch,
    selectedEntity: scopedSelectedEntity,
    anchorVariant: scopedAnchorVariant,
    selectedVehicle: scopedSelectedVehicle,
  };

  const scopedFeatureFilters =
    filters && typeof filters === "object"
      ? {
          ...filters,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
        }
      : {
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
        };

  const scopedDetected =
    detected && typeof detected === "object"
      ? {
          ...detected,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
          entities: {
            ...(detected.entities || {}),
            variant: scopedAnchorVariant,
            variantName: scopedAnchorVariant,
            selectedVariant: scopedAnchorVariant,
            selectedVariantKey: scopedAnchorVariant,
            requestedVariant: scopedAnchorVariant,
          },
        }
      : detected;

  const scopedToolPlan =
    toolPlan && typeof toolPlan === "object"
      ? {
          ...toolPlan,
          variant: scopedAnchorVariant,
	          variantName: scopedAnchorVariant,
	          selectedVariant: scopedAnchorVariant,
	          selectedVariantKey: scopedAnchorVariant,
	          requestedVariant: scopedAnchorVariant,
	          entities: {
	            ...(toolPlan.entities || {}),
	            variant: scopedAnchorVariant,
	            variantName: scopedAnchorVariant,
	            selectedVariant: scopedAnchorVariant,
	            selectedVariantKey: scopedAnchorVariant,
	            requestedVariant: scopedAnchorVariant,
	          },
	          input: {
	            ...(toolPlan.input || {}),
	            variant: scopedAnchorVariant,
	            variantName: scopedAnchorVariant,
	            selectedVariant: scopedAnchorVariant,
	            selectedVariantKey: scopedAnchorVariant,
	            requestedVariant: scopedAnchorVariant,
	          },
	          filters: {
	            ...(toolPlan.filters || {}),
	            variant: scopedAnchorVariant,
	            variantName: scopedAnchorVariant,
	            selectedVariant: scopedAnchorVariant,
	            selectedVariantKey: scopedAnchorVariant,
	            requestedVariant: scopedAnchorVariant,
	          },
	        }
	      : toolPlan;

  let response = await toolRunner({
    detected: scopedDetected,
    filters: scopedFeatureFilters,
    context: scopedFeatureContext,
    toolPlan: scopedToolPlan,
    selectedEntity: scopedSelectedEntity,
    userMessage: cleanUserMessage,
  });

  let overviewAuthorityContextPatch = null;

  if (detected.intent === "vehicle_overview") {
    const overviewVehicle =
      response.vehicle ||
      response.widget?.vehicle ||
      response.contextPatch?.selectedVehicle ||
      preToolAuthorityContextPatch.selectedVehicle ||
      {};
    const overviewContextPatch = {
      ...preToolAuthorityContextPatch,
      ...(response.contextPatch || {}),
      selectedVehicle: {
        ...(overviewVehicle || {}),
        variant: detected.variant || "",
        variantName: detected.variant || "",
        selectedVariant: detected.variant || "",
      },
      anchorMake:
        overviewVehicle.make ||
        overviewVehicle.brand ||
        response.contextPatch?.anchorMake ||
        preToolAuthorityContextPatch.anchorMake ||
        "",
      anchorModel:
        overviewVehicle.model ||
        response.contextPatch?.anchorModel ||
        preToolAuthorityContextPatch.anchorModel ||
        detected.model ||
        "",
      anchorFullModel:
        overviewVehicle.fullModel ||
        overviewVehicle.displayName ||
        response.contextPatch?.anchorFullModel ||
        preToolAuthorityContextPatch.anchorFullModel ||
        detected.fullModel ||
        "",
      anchorVariant: detected.variant || "",
    };

    response = {
      ...response,
      tool: "vehicle_overview",
      intent: "vehicle_overview",
      canvasType: "car_overview_canvas",
      answer: `Opened ${overviewVehicle.displayName || detected.model} overview.`,
      vehicle: overviewContextPatch.selectedVehicle,
      contextPatch: overviewContextPatch,
      widget: {
        ...(response.widget || {}),
        type: "vehicle_overview",
        tool: "vehicle_overview",
        intent: "vehicle_overview",
        canvasType: "car_overview_canvas",
        title: `${overviewVehicle.displayName || detected.model} overview`,
        answer: `Opened ${overviewVehicle.displayName || detected.model} overview.`,
        vehicle: overviewContextPatch.selectedVehicle,
        rows: response.rows || response.widget?.rows || [],
        items: response.items || response.widget?.items || response.rows || [],
        contextPatch: overviewContextPatch,
      },
    };

    overviewAuthorityContextPatch = overviewContextPatch;
  }

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
