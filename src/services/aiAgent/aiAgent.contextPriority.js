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

const normalizeAciContextText = (value = "") =>
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
