/**
 * ACI Assist context-priority helpers.
 *
 * Keep this module deterministic and DB-agnostic where possible.
 * DB hydration can be injected by the caller so service orchestration remains thin.
 */

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

export const normalizeAciContextText = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

export const repairAciResponseContextFromActiveContext = async ({
  response = {},
  context = {},
  hydrateModelEntity = null,
} = {}) => {
  if (!response || typeof response !== "object") return response;
  if (!context || typeof context !== "object") return response;

  const bridgeContextIsolation =
    response.aciCoreBridge?.contextIsolation ||
    response.meta?.aciCoreBridge?.contextIsolation ||
    "";

  if (bridgeContextIsolation && bridgeContextIsolation !== "preserve_context") {
    return response;
  }

  const contextVehicle = context.selectedVehicle || {};
  const patch = response.contextPatch || {};
  const patchVehicle = patch.selectedVehicle || {};

  const responseModel = cleanText(
    patch.anchorModel ||
      patchVehicle.model ||
      response.vehicle?.model ||
      response.data?.model ||
      "",
  );

  const activeModel = cleanText(
    context.anchorModel ||
      contextVehicle.model ||
      context.model ||
      responseModel ||
      "",
  );

  let activeMake = cleanText(
    context.anchorMake ||
      contextVehicle.make ||
      contextVehicle.brand ||
      patch.anchorMake ||
      patch.anchorBrand ||
      patchVehicle.make ||
      patchVehicle.brand ||
      "",
  );

  let activeFullModel = cleanText(
    context.anchorFullModel ||
      contextVehicle.fullModel ||
      contextVehicle.displayName ||
      patch.anchorFullModel ||
      patchVehicle.fullModel ||
      patchVehicle.displayName ||
      "",
  );

  if (!activeModel) return response;

  // Empty-context flows like EMI/lead know the model but often miss make/brand.
  // Hydrate from read-model summaries instead of guessing or hardcoding.
  if (
    (!activeMake ||
      !activeFullModel ||
      normalizeAciContextText(activeFullModel) === normalizeAciContextText(activeModel)) &&
    typeof hydrateModelEntity === "function"
  ) {
    try {
      const hydrated = await hydrateModelEntity({
        model: activeModel,
        make: activeMake,
        brand: activeMake,
        fullModel: activeFullModel,
      });

      if (
        hydrated?.model &&
        normalizeAciContextText(hydrated.model) === normalizeAciContextText(activeModel)
      ) {
        activeMake = activeMake || cleanText(hydrated.make || hydrated.brand || "");
        activeFullModel =
          cleanText(hydrated.fullModel || hydrated.displayName || "") ||
          activeFullModel;
      }
    } catch {
      // Keep existing context if read-model hydration is unavailable.
    }
  }

  if (!activeFullModel) {
    activeFullModel = activeMake ? `${activeMake} ${activeModel}` : activeModel;
  }

  const patchModel = cleanText(
    patch.anchorModel ||
      patchVehicle.model ||
      response.vehicle?.model ||
      response.data?.model ||
      "",
  );

  // Only repair when response is for the same active model.
  // Never force active context into a different returned car.
  if (
    patchModel &&
    normalizeAciContextText(patchModel) !== normalizeAciContextText(activeModel)
  ) {
    return response;
  }

  const repairedVehicle = {
    ...patchVehicle,
    make: cleanText(patchVehicle.make || patchVehicle.brand || "") || activeMake,
    brand: cleanText(patchVehicle.brand || patchVehicle.make || "") || activeMake,
    model: patchVehicle.model || activeModel,
    fullModel: patchVehicle.fullModel || activeFullModel,
    displayName: patchVehicle.displayName || activeFullModel,
  };

  response.contextPatch = {
    ...patch,
    selectedVehicle: repairedVehicle,
    anchorMake: cleanText(patch.anchorMake || "") || activeMake,
    anchorModel: patch.anchorModel || activeModel,
    anchorFullModel: patch.anchorFullModel || activeFullModel,
    anchorCity:
      patch.anchorCity ||
      context.anchorCity ||
      contextVehicle.citySlug ||
      contextVehicle.city ||
      "new-delhi",
  };

  if (response.data && typeof response.data === "object") {
    response.data.contextPatch = {
      ...(response.data.contextPatch || {}),
      ...response.contextPatch,
    };
  }

  if (response.widget && typeof response.widget === "object") {
    response.widget.contextPatch = {
      ...(response.widget.contextPatch || {}),
      ...response.contextPatch,
    };
  }

  return response;
};


export const stripAciContextMake = (value = "", make = "") => {
  const raw = String(value || "").trim();
  const brand = String(make || "").trim();

  if (!raw || !brand) return raw;

  const rawNorm = normalizeAciContextText(raw);
  const brandNorm = normalizeAciContextText(brand);

  if (rawNorm.startsWith(`${brandNorm} `)) {
    const brandWordCount = brandNorm.split(" ").filter(Boolean).length;
    return raw.split(/\s+/).slice(brandWordCount).join(" ").trim() || raw;
  }

  return raw;
};

export const titleAciContextName = (value = "") =>
  String(value || "")
    .trim()
    .split(/\s+/)
    .map((word) => {
      if (/^ivt$/i.test(word)) return "iVT";
      if (/^(dct|amt|at|mt|cvt)$/i.test(word)) return word.toUpperCase();
      if (/^sx$/i.test(word)) return "SX";
      if (/^htx$/i.test(word)) return "HTX";
      if (/^abs$/i.test(word)) return "ABS";
      if (/^[A-Z0-9()]+$/.test(word)) return word;
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join(" ");

export const isAciGenericModelEntityMatch = (entity = {}) => {
  const matched = normalizeAciContextText(entity?.matchedText || "");

  if (!matched) return false;

  const genericMatches = new Set([
    "best",
    "better",
    "top",
    "highest",
    "maximum",
    "max",
    "most",
    "mileage",
    "average",
    "fuel",
    "efficiency",
    "kmpl",
    "kpl",
    "variant",
    "variants",
    "feature",
    "features",
    "worth",
    "upgrade",
    "buy",
    "family",
    "rear",
    "seat",
    "night",
    "driving",
  ]);

  return genericMatches.has(matched);
};

export const hasAciComparisonLanguage = (message = "") =>
  /\b(difference|different|compare|comparison|vs|versus|v\/s|extra\s+features?|upgrade)\b/i.test(
    String(message || ""),
  );

export const isAciLikelyVariantTokenModelMatch = ({
  message = "",
  textEntity = null,
  contextEntity = null,
} = {}) => {
  if (!textEntity || !contextEntity) return false;
  if (!hasAciComparisonLanguage(message)) return false;

  const matched = normalizeAciContextText(textEntity.matchedText || "");
  if (!matched) return false;

  const weakVariantTokens = new Set([
    "e",
    "ex",
    "s",
    "sx",
    "vx",
    "zx",
    "v",
    "z",
    "ht",
    "htk",
    "htx",
    "gtx",
    "ax",
    "lx",
    "mx",
  ]);

  return matched.length <= 3 && weakVariantTokens.has(matched);
};

export const chooseAciDynamicModelEntity = ({
  textEntity = null,
  contextEntity = null,
  message = "",
} = {}) => {
  if (!textEntity) return contextEntity;
  if (!contextEntity) return textEntity;

  if (isAciLikelyVariantTokenModelMatch({ message, textEntity, contextEntity })) {
    return contextEntity;
  }

  if (isAciGenericModelEntityMatch(textEntity)) {
    return contextEntity;
  }

  return textEntity;
};

export const buildAciContextModelEntity = ({ context = {}, selectedEntity = null } = {}) => {
  const selectedVehicle =
    context?.selectedVehicle ||
    selectedEntity?.selectedVehicle ||
    selectedEntity?.vehicle ||
    selectedEntity ||
    {};

  const make =
    context?.anchorMake ||
    context?.make ||
    selectedVehicle?.make ||
    selectedVehicle?.brand ||
    "";

  const rawModel =
    context?.anchorModel ||
    context?.model ||
    selectedVehicle?.model ||
    selectedVehicle?.name ||
    "";

  const rawFullModel =
    context?.anchorFullModel ||
    selectedVehicle?.fullModel ||
    selectedVehicle?.fullName ||
    (make && rawModel ? `${make} ${stripAciContextMake(rawModel, make)}` : rawModel);

  const model = titleAciContextName(stripAciContextMake(rawModel, make));
  const fullModel = titleAciContextName(rawFullModel);
  const brand = titleAciContextName(make);

  if (!model) return null;

  return {
    brand,
    model,
    fullModel: fullModel || (brand ? `${brand} ${model}` : model),
    matchedText: "",
    confidence: 1,
    method: "context_anchor",
    fromContext: true,
  };
};

export const isAciComparisonMessage = (message = "") =>
  /\b(compare|comparison|vs|versus)\b/i.test(String(message || ""));

export const applyAciExplicitMessageModelContextOverride = ({
  message = "",
  context = {},
  dynamicModelEntity = null,
} = {}) => {
  if (!context || typeof context !== "object") return context;
  if (!dynamicModelEntity?.model) return context;

  // Do not hijack "Compare with City" type follow-ups.
  // In comparison follow-ups, the mentioned model can be the rival, not the selected car.
  if (isAciComparisonMessage(message)) return context;

  const nextModel = cleanText(dynamicModelEntity.model);
  const nextMake = cleanText(dynamicModelEntity.brand || dynamicModelEntity.make || "");
  const nextFullModel = cleanText(
    dynamicModelEntity.fullModel ||
      (nextMake && nextModel ? `${nextMake} ${nextModel}` : nextModel),
  );

  if (!nextModel) return context;

  const selectedVehicle = context.selectedVehicle || {};
  const currentModel = cleanText(
    context.anchorModel ||
      selectedVehicle.model ||
      context.model ||
      "",
  );

  const sameModel =
    normalizeAciContextText(currentModel) === normalizeAciContextText(nextModel);

  if (sameModel) return context;

  // Latest explicit user message must beat stale frontend/backend context.
  context.selectedVehicle = {
    ...selectedVehicle,

    // Explicit model switch must not carry stale make/brand from previous car.
    // Example: Verna context + "Does Thar have sunroof?" must not become Hyundai Thar.
    make: nextMake || "",
    brand: nextMake || "",
    model: nextModel,
    displayName: nextFullModel || nextModel,
    fullModel: nextFullModel || nextModel,
    variant: "",
    selectedVariant: "",
    variantName: "",
  };

  context.anchorMake = nextMake || "";
  context.anchorModel = nextModel;
  context.anchorFullModel = nextFullModel || nextModel;
  context.anchorVariant = "";
  context.model = nextModel;

  return context;
};
