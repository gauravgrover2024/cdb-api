import { normalizeAciContextText } from "./aiAgent.contextPriority.js";
import { formatAciInlineVariantName } from "./aiAgent.earlyFeatureGate.formatters.js";

const pickAciContextValue = (...values) => {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    const text = String(value).trim();
    if (text) return value;
  }
  return "";
};


const extractAciScopedVariantFromCleanMessage = ({
  cleanUserMessage = "",
  model = "",
  fullModel = "",
  make = "",
} = {}) => {
  let text = String(cleanUserMessage || "").trim();

  [
    fullModel,
    model,
    make,
  ]
    .filter(Boolean)
    .sort((a, b) => String(b).length - String(a).length)
    .forEach((candidate) => {
      const safe = String(candidate).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      text = text.replace(new RegExp(`\\b${safe}\\b`, "ig"), " ");
    });

  text = text
    .replace(/\b(abs|ags|anti\s*lock\s*braking|anti-lock\s*braking|sunroof|mileage|arai\s*mileage|features?|feature|price|pricelist|overview|details?|on\s*road|on-road)\b/gi, " ")
    .replace(/\b(does|do|is|are|has|have|having|with|get|gets|got|which|what|best|highest|maximum|max|most|variant|variants|car|cars|it|this|that|current|selected|new|old)\b/gi, " ")
    .replace(/[?.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return text ? formatAciInlineVariantName(text) : "";
};

const shouldCarryAciCurrentVariant = ({ message = "", dynamicModelEntity = null, explicitVariant = "", context = {} } = {}) => {
  if (explicitVariant) return false;
  if (!dynamicModelEntity?.fromContext) return false;
  if (!context?.anchorVariant && !context?.selectedVehicle?.variant && !context?.selectedVehicle?.variantName) return false;

  // Carry variant only for true pronoun/current-variant questions.
  // Do not carry it for model-level questions like "Does Seltos have ABS?"
  return /\b(it|this|that|current one|selected one|this variant|current variant|selected variant)\b/i.test(
    String(message || ""),
  );
};

export const buildAciFeatureAuthorityContextPatch = ({
  context = {},
  detected = {},
  dynamicModelEntity = null,
  cleanUserMessage = "",
  message = "",
} = {}) => {
  const make =
    detected.make ||
    detected.brand ||
    dynamicModelEntity?.brand ||
    context?.anchorMake ||
    context?.selectedVehicle?.make ||
    context?.selectedVehicle?.brand ||
    "";

  const model =
    detected.model ||
    dynamicModelEntity?.model ||
    context?.anchorModel ||
    context?.selectedVehicle?.model ||
    "";

  const fullModel =
    detected.fullModel ||
    dynamicModelEntity?.fullModel ||
    context?.anchorFullModel ||
    context?.selectedVehicle?.fullModel ||
    (make && model ? `${make} ${model}` : model);

  const explicitVariant = extractAciScopedVariantFromCleanMessage({
    cleanUserMessage,
    model,
    fullModel,
    make,
  });

  const carriedVariant = shouldCarryAciCurrentVariant({
    message,
    dynamicModelEntity,
    explicitVariant,
    context,
  })
    ? pickAciContextValue(
        context?.anchorVariant,
        context?.selectedVehicle?.variant,
        context?.selectedVehicle?.variantName,
      )
    : "";

  const isComparisonIntent =
    detected?.intent === "vehicle_feature_comparison" ||
    detected?.canvasType === "comparison_canvas";

  const nextVariant = isComparisonIntent
    ? ""
    : explicitVariant || carriedVariant || "";
  const contextVehicle = context?.selectedVehicle || {};
  const contextVehicleMatchesModel =
    normalizeAciContextText(contextVehicle.model || "") ===
    normalizeAciContextText(model || "");

  return {
    selectedVehicle: {
      ...(contextVehicleMatchesModel ? contextVehicle : {}),
      make,
      brand: make,
      model,
      fullModel,
      variant: nextVariant,
      variantName: nextVariant,
      city: pickAciContextValue(context?.anchorCity, context?.city, context?.selectedVehicle?.city, "new-delhi"),
      citySlug: pickAciContextValue(context?.anchorCity, context?.citySlug, context?.selectedVehicle?.citySlug, "new-delhi"),
    },
    anchorMake: make,
    anchorModel: model,
    anchorFullModel: fullModel,
    anchorVariant: nextVariant,
    anchorCity: pickAciContextValue(context?.anchorCity, context?.city, context?.selectedVehicle?.citySlug, "new-delhi"),
    selectedColor: null,
    ...(isComparisonIntent
      ? {
          selectedComparisonSet: {
            model,
            variants: Array.isArray(detected?.variants) ? detected.variants : [],
          },
        }
      : {}),
  };
};

export const applyAciFeatureAuthorityContextPatch = (response = {}, patch = {}) => {
  if (!response || typeof response !== "object") return response;

  const mergeAuthorityPatch = (existingPatch = {}) => {
    const existingVehicle =
      existingPatch.selectedVehicle ||
      response.vehicle ||
      response.widget?.vehicle ||
      {};
    const patchVehicle = patch.selectedVehicle || {};
    const existingModel = normalizeAciContextText(existingVehicle.model || "");
    const patchModel = normalizeAciContextText(
      patchVehicle.model || patch.anchorModel || "",
    );
    const canPreserveExistingVehicle =
      existingVehicle &&
      (!existingModel || !patchModel || existingModel === patchModel);
    const selectedVehicle = {
      ...(canPreserveExistingVehicle ? existingVehicle : {}),
      ...patchVehicle,
    };

    if (canPreserveExistingVehicle) {
      selectedVehicle.imageUrl =
        patchVehicle.imageUrl || existingVehicle.imageUrl || "";
      selectedVehicle.normalizedImageUrl =
        patchVehicle.normalizedImageUrl ||
        existingVehicle.normalizedImageUrl ||
        existingVehicle.imageUrl ||
        "";
      selectedVehicle.imageFrame =
        patchVehicle.imageFrame || existingVehicle.imageFrame || null;
      selectedVehicle.displayFrameMeta =
        patchVehicle.displayFrameMeta ||
        existingVehicle.displayFrameMeta ||
        selectedVehicle.imageFrame ||
        null;
    }

    return {
      ...existingPatch,
      ...patch,
      selectedVehicle,
    };
  };

  response.contextPatch = {
    ...mergeAuthorityPatch(response.contextPatch || {}),
  };

  response.context = {
    ...mergeAuthorityPatch(response.context || {}),
  };

  if (response.data && typeof response.data === "object") {
    response.data = {
      ...response.data,
      contextPatch: mergeAuthorityPatch(response.data.contextPatch || {}),
    };
  }

  if (response.widget && typeof response.widget === "object") {
    response.widget = {
      ...response.widget,
      contextPatch: mergeAuthorityPatch(response.widget.contextPatch || {}),
    };
  }

  return response;
};
