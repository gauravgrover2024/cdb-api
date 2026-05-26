export const buildAciEarlyGateToolPlan = ({
  detected = {},
  dynamicModelEntity = null,
  filters = {},
} = {}) => {
  const payload = {
    make: detected.make || dynamicModelEntity?.brand || "",
    brand: detected.brand || dynamicModelEntity?.brand || "",
    model: detected.model || "",
    fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
    feature: detected.feature || "",
    variants: detected.variants || [],
    variant: detected.variant || "",
    category: detected.category || "",
    categoryLabel: detected.categoryLabel || "",
  };

  return {
    tool: detected.intent,
    intent: detected.intent,
    toolIntent: detected.intent,
    canvasType: detected.canvasType,
    entities: {
      ...payload,
    },
    input: {
      ...payload,
    },
    filters: {
      ...(filters || {}),
      ...payload,
    },
  };
};

const withScopedVariant = (target = {}, scopedAnchorVariant = "") => ({
  ...(target || {}),
  variant: scopedAnchorVariant,
  variantName: scopedAnchorVariant,
  selectedVariant: scopedAnchorVariant,
  selectedVariantKey: scopedAnchorVariant,
  requestedVariant: scopedAnchorVariant,
});

export const buildAciEarlyGateScopedExecution = ({
  detected = {},
  toolPlan = {},
  context = {},
  filters = {},
  selectedEntity = null,
  preToolAuthorityContextPatch = {},
} = {}) => {
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
          ...withScopedVariant({}, scopedAnchorVariant),
        }
      : {
          selectedVehicle: scopedSelectedVehicle,
          vehicle: scopedSelectedVehicle,
          ...withScopedVariant({}, scopedAnchorVariant),
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
      ? withScopedVariant(filters, scopedAnchorVariant)
      : withScopedVariant({}, scopedAnchorVariant);

  const scopedDetected =
    detected && typeof detected === "object"
      ? {
          ...withScopedVariant(detected, scopedAnchorVariant),
          entities: {
            ...(detected.entities || {}),
            ...withScopedVariant({}, scopedAnchorVariant),
          },
        }
      : detected;

  const scopedToolPlan =
    toolPlan && typeof toolPlan === "object"
      ? {
          ...withScopedVariant(toolPlan, scopedAnchorVariant),
          entities: {
            ...(toolPlan.entities || {}),
            ...withScopedVariant({}, scopedAnchorVariant),
          },
          input: {
            ...(toolPlan.input || {}),
            ...withScopedVariant({}, scopedAnchorVariant),
          },
          filters: {
            ...(toolPlan.filters || {}),
            ...withScopedVariant({}, scopedAnchorVariant),
          },
        }
      : toolPlan;

  return {
    scopedAnchorVariant,
    scopedSelectedVehicle,
    scopedSelectedEntity,
    scopedFeatureContext,
    scopedFeatureFilters,
    scopedDetected,
    scopedToolPlan,
  };
};
