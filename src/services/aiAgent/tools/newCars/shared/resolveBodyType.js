export const resolveBodyType = (toolPlan = {}) => {
  const entities = toolPlan.entities || {};
  const filters = toolPlan.filters || {};
  return String(filters.bodyType || entities.bodyType || "").trim().toLowerCase();
};

export default resolveBodyType;
