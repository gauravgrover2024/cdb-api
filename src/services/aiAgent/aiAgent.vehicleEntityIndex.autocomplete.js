import { normalizeSearchKey } from "./aiAgent.planSchema.js";

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const uniqueBy = (items = [], keyFor = (item) => item?.id) => {
  const seen = new Set();
  return items.filter((item) => {
    const key = keyFor(item);
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const matchScore = (value = "", query = "") => {
  const text = normalizeSearchKey(value);
  const q = normalizeSearchKey(query);
  if (!text || !q) return 0;
  if (text === q) return 120;
  if (text.startsWith(q)) return 105;
  if (text.split(" ").some((token) => token.startsWith(q))) return 92;
  if (text.includes(q)) return 76;
  return 0;
};

const getSelectedVehicle = (context = {}) =>
  context.selectedVehicle ||
  context.contextState?.selectedVehicle ||
  context.aciContextState?.selectedVehicle ||
  {};

const sameSelectedModel = (variant = {}, selectedVehicle = {}) => {
  const selectedModel = normalizeSearchKey(selectedVehicle.model || "");
  const selectedMake = normalizeSearchKey(
    selectedVehicle.make || selectedVehicle.brand || "",
  );
  if (!selectedModel) return false;

  return (
    normalizeSearchKey(variant.model || "") === selectedModel &&
    (!selectedMake ||
      normalizeSearchKey(variant.brand || variant.make || "") === selectedMake)
  );
};

const buildContextActionSuggestions = ({ query = "", context = {} } = {}) => {
  const vehicle = getSelectedVehicle(context);
  const make = clean(vehicle.make || vehicle.brand);
  const model = clean(vehicle.model);
  if (!model) return [];

  const fullModel = clean(vehicle.fullModel || [make, model].filter(Boolean).join(" "));
  const actions = [
    {
      key: "price",
      terms: "price on road ex showroom cost",
      label: `Show ${model} prices`,
      subLabel: `Continue with ${fullModel}`,
      tool: "vehicle_pricelist",
      icon: "tag",
    },
    {
      key: "features",
      terms: "feature features equipment",
      label: `Show ${model} features`,
      subLabel: `Continue with ${fullModel}`,
      tool: "vehicle_model_features_explorer",
      icon: "list-checks",
    },
    {
      key: "colors",
      terms: "color colors colour colours paint",
      label: `Show ${model} colours`,
      subLabel: `Continue with ${fullModel}`,
      tool: "vehicle_colors",
      icon: "paintbrush",
    },
    {
      key: "variants",
      terms: "variant variants trim trims",
      label: `Show ${model} variants`,
      subLabel: `Continue with ${fullModel}`,
      tool: "vehicle_pricelist",
      icon: "rows-3",
    },
    {
      key: "emi",
      terms: "emi finance loan monthly payment",
      label: `Calculate ${model} EMI`,
      subLabel: `Continue with ${fullModel}`,
      tool: "vehicle_emi",
      icon: "calculator",
    },
  ];

  return actions
    .map((action) => ({
      ...action,
      score: matchScore(action.terms, query),
    }))
    .filter((action) => action.score > 0)
    .map((action) => ({
      id: `context-${action.key}-${normalizeSearchKey(fullModel).replace(/\s+/g, "-")}`,
      type: "context_action",
      label: action.label,
      subLabel: action.subLabel,
      icon: action.icon,
      autotypeText: action.label,
      sendOnClick: false,
      priority: 500 + action.score,
      tool: action.tool,
      entity: { make, brand: make, model, fullModel },
    }));
};

export const buildAutocompleteEntityMatchesFromIndex = ({
  index = {},
  featureCatalog = {},
  query = "",
  context = {},
  limit = 8,
} = {}) => {
  const q = normalizeSearchKey(query);
  const safeLimit = Math.min(Math.max(Number(limit) || 8, 1), 12);
  if (!q) return [];

  const selectedVehicle = getSelectedVehicle(context);
  const brandCounts = new Map();
  for (const model of index.models || []) {
    const brand = clean(model.brand || model.make);
    if (!brand) continue;
    brandCounts.set(brand, (brandCounts.get(brand) || 0) + 1);
  }

  const brands = [...brandCounts.entries()]
    .map(([brand, modelCount]) => ({
      id: `brand-${normalizeSearchKey(brand).replace(/\s+/g, "-")}`,
      type: "brand",
      label: brand,
      subLabel: `${modelCount} model${modelCount === 1 ? "" : "s"}`,
      icon: "badge",
      autotypeText: brand,
      sendOnClick: false,
      priority: 340 + matchScore(brand, q),
      entity: { make: brand, brand },
      score: matchScore(brand, q),
    }))
    .filter((item) => item.score > 0);

  const models = (index.models || [])
    .map((model) => {
      const label = clean(
        model.displayName || `${model.brand || ""} ${model.model || ""}`,
      );
      const bag = `${label} ${(model.aliases || []).join(" ")}`;
      const score = matchScore(bag, q);
      return {
        id: `model-${normalizeSearchKey(model.modelKey || label).replace(/\s+/g, "-")}`,
        type: "model",
        label,
        subLabel: [
          model.variantsCount ? `${model.variantsCount} variants` : "",
          model.fuelTypes?.slice(0, 2).join(" / "),
          model.transmissions?.slice(0, 2).join(" / "),
        ]
          .filter(Boolean)
          .join(" | "),
        icon: "car",
        autotypeText: label,
        sendOnClick: false,
        priority: 320 + score + Number(model.priority || 0) / 100,
        entity: { brand: model.brand, make: model.brand, model: model.model },
        score,
      };
    })
    .filter((item) => item.score > 0);

  const variants = (index.variants || [])
    .map((variant) => {
      const fullModel = clean(
        variant.fullModel || `${variant.brand || ""} ${variant.model || ""}`,
      );
      const label = clean(`${fullModel} ${variant.variant || ""}`);
      const selectedVariantScore = sameSelectedModel(variant, selectedVehicle)
        ? matchScore(variant.variant, q)
        : 0;
      const variantOnlyScore = selectedVariantScore
        ? selectedVariantScore + 80
        : 0;
      const score = Math.max(matchScore(label, q), variantOnlyScore);
      return {
        id: `variant-${normalizeSearchKey(variant.variantKey || label).replace(/\s+/g, "-")}`,
        type: "variant",
        label,
        subLabel: [variant.fuel, variant.transmission].filter(Boolean).join(" | "),
        icon: "car-front",
        autotypeText: label,
        sendOnClick: false,
        priority: 280 + score + Number(variant.priority || 0) / 100,
        entity: {
          brand: variant.brand,
          make: variant.brand,
          model: variant.model,
          variant: variant.variant,
          fuel: variant.fuel,
          transmission: variant.transmission,
        },
        score,
      };
    })
    .filter((item) => item.score > 0);

  const features = (featureCatalog.features || [])
    .map((feature) => {
      const label = clean(feature.displayName || feature.canonicalKey);
      const bag = `${label} ${(feature.aliases || []).join(" ")}`;
      const score = matchScore(bag, q);
      return {
        id: `feature-${feature.canonicalKey}`,
        type: "feature",
        label,
        subLabel: clean(feature.groupLabel || feature.groupKey || "Car feature"),
        icon: "sparkles",
        autotypeText: label,
        sendOnClick: false,
        priority: 310 + score,
        entity: {
          feature: label,
          featureKey: feature.canonicalKey,
        },
        score,
      };
    })
    .filter((item) => item.score > 0);

  const contextActions = buildContextActionSuggestions({ query: q, context });

  return uniqueBy(
    [...contextActions, ...variants, ...models, ...features, ...brands].sort(
      (left, right) =>
        (right.priority || 0) - (left.priority || 0) ||
        left.label.localeCompare(right.label),
    ),
  ).slice(0, safeLimit);
};
