import { normalizeVehicle } from "./normalizeVehicle.js";

export const resolveVehicle = ({ toolPlan = {}, context = {} } = {}) => {
  const entities = toolPlan.entities || {};
  const selectedVehicle = context.selectedVehicle || {};

  return normalizeVehicle({
    make: entities.make || entities.brand || selectedVehicle.make || selectedVehicle.brand,
    model: entities.model || selectedVehicle.model || context.anchorModel,
    variant: entities.variant || selectedVehicle.variant || context.anchorVariant,
    city: toolPlan.filters?.city || entities.city || selectedVehicle.city || context.anchorCity,
    normalizedImageUrl: selectedVehicle.normalizedImageUrl,
    imageUrl: selectedVehicle.imageUrl,
  });
};

export default resolveVehicle;
