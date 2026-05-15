import { normalizeVehicle } from "./normalizeVehicle.js";

const cleanText = (value = "") => String(value || "").replace(/\s+/g, " ").trim();

const normalizeKey = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

export const resolveVehicle = ({ toolPlan = {}, context = {} } = {}) => {
  const entities = toolPlan.entities || {};
  const selectedVehicle = context.selectedVehicle || {};
  const explicitModel = cleanText(entities.model || "");
  const selectedModel = cleanText(selectedVehicle.model || context.anchorModel || "");
  const modelChanged =
    explicitModel &&
    selectedModel &&
    normalizeKey(explicitModel) !== normalizeKey(selectedModel);
  const canReuseSelectedVehicle = !modelChanged;

  return normalizeVehicle({
    make:
      entities.make ||
      entities.brand ||
      (canReuseSelectedVehicle
        ? selectedVehicle.make || selectedVehicle.brand || context.anchorMake
        : ""),
    model: entities.model || selectedVehicle.model || context.anchorModel,
    variant:
      entities.variant ||
      (canReuseSelectedVehicle
        ? selectedVehicle.variant || context.anchorVariant
        : ""),
    city: toolPlan.filters?.city || entities.city || selectedVehicle.city || context.anchorCity,
    normalizedImageUrl: canReuseSelectedVehicle ? selectedVehicle.normalizedImageUrl : "",
    imageUrl: canReuseSelectedVehicle ? selectedVehicle.imageUrl : "",
  });
};

export default resolveVehicle;
