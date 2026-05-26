import { normalizeSearchKey } from "./aiAgent.planSchema.js";

export const buildAutocompleteEntityMatchesFromIndex = ({
  index = {},
  query = "",
  limit = 8,
} = {}) => {
  const q = normalizeSearchKey(query);
  const safeLimit = Math.min(Math.max(Number(limit) || 8, 1), 12);

  if (!q) return [];

  const modelMatches = [];

  for (const model of index.models || []) {
    const bag = normalizeSearchKey(
      `${model.brand} ${model.model} ${(model.aliases || []).join(" ")}`,
    );

    if (bag.includes(q)) {
      modelMatches.push({
        id: `model-${model.modelKey}`,
        type: "model",
        label: model.displayName || `${model.brand} ${model.model}`.trim(),
        subLabel: [
          model.variantsCount ? `${model.variantsCount} variants` : "",
          model.fuelTypes?.slice(0, 2).join(" / "),
          model.transmissions?.slice(0, 2).join(" / "),
        ]
          .filter(Boolean)
          .join(" • "),
        icon: "car",
        autotypeText: model.displayName || model.model,
        sendOnClick: false,
        priority: model.priority || 50,
        entity: {
          brand: model.brand,
          model: model.model,
        },
      });
    }
  }

  modelMatches.sort((a, b) => b.priority - a.priority);

  const topModel = modelMatches[0];

  const querySuggestions = [];

  if (topModel) {
    const modelLabel = topModel.entity.model;

    querySuggestions.push(
      {
        id: `query-price-${normalizeSearchKey(modelLabel)}`,
        type: "query",
        label: `Show ${modelLabel} price`,
        subLabel: "Variant-wise pricelist",
        icon: "tag",
        autotypeText: `Show ${modelLabel} price`,
        sendOnClick: false,
        priority: 80,
        tool: "vehicle_pricelist",
        entity: topModel.entity,
      },
      {
        id: `query-colors-${normalizeSearchKey(modelLabel)}`,
        type: "query",
        label: `Show ${modelLabel} colors`,
        subLabel: "Exterior color gallery",
        icon: "paintbrush",
        autotypeText: `Show colors of ${modelLabel}`,
        sendOnClick: false,
        priority: 76,
        tool: "vehicle_colors",
        entity: topModel.entity,
      },
      {
        id: `query-emi-${normalizeSearchKey(modelLabel)}`,
        type: "query",
        label: `Calculate EMI for ${modelLabel}`,
        subLabel: "On-road EMI estimate",
        icon: "calculator",
        autotypeText: `Calculate EMI for ${modelLabel}`,
        sendOnClick: false,
        priority: 72,
        tool: "vehicle_emi",
        entity: topModel.entity,
      },
    );
  }

  return [...modelMatches.slice(0, safeLimit), ...querySuggestions]
    .sort((a, b) => (b.priority || 0) - (a.priority || 0))
    .slice(0, safeLimit);
};
