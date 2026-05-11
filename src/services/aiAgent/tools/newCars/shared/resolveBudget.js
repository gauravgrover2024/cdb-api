import { parseAmount } from "./normalizePrice.js";

export const resolveBudget = (toolPlan = {}) => {
  const entities = toolPlan.entities || {};
  const filters = toolPlan.filters || {};

  const min = parseAmount(filters.minBudget || entities.minBudget || 0);
  const max = parseAmount(filters.maxBudget || entities.maxBudget || 0);

  return {
    min,
    max,
    hasBudget: min > 0 || max > 0,
  };
};

export default resolveBudget;
