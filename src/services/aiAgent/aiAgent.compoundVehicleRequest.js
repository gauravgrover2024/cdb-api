import { resolveSpecAttributesFromText } from "../aciCore/specs/aciVehicleSpecAttributeResolver.service.js";
import { parseAciFeatureRequestFromMessage } from "./aiAgent.featureRequestParser.js";
import {
  findModelMatches,
  getVehicleEntityIndex,
} from "./aiAgent.vehicleEntityIndex.js";
import { normalizeSearchKey } from "./aiAgent.planSchema.js";

const MAX_COMPOUND_MODELS = 5;

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const modelLabel = (model = {}) =>
  cleanText(
    model.fullModel ||
      model.displayName ||
      [model.brand || model.make, model.model].filter(Boolean).join(" "),
  );

const compactObject = (value = {}) =>
  Object.fromEntries(
    Object.entries(value).filter(([, item]) => {
      if (Array.isArray(item)) return item.length > 0;
      return item !== undefined && item !== null && item !== "";
    }),
  );

const getCity = (message = "", context = {}) => {
  const text = normalizeSearchKey(message);
  if (/\bnoida\b/.test(text)) return "noida";
  if (/\bgurgaon|gurugram\b/.test(text)) return "gurgaon";
  if (/\bnew delhi|\bdelhi\b|\bncr\b/.test(text)) return "new-delhi";

  return cleanText(
    context.anchorCity ||
      context.selectedVehicle?.citySlug ||
      context.selectedVehicle?.city ||
      "new-delhi",
  );
};

const getMentionPosition = (message = "", model = {}) => {
  const text = normalizeSearchKey(message);
  const aliases = [
    model.matchedAlias,
    model.fullModel,
    model.displayName,
    model.brand && model.model ? `${model.brand} ${model.model}` : "",
    model.model,
  ]
    .map(normalizeSearchKey)
    .filter(Boolean)
    .sort((left, right) => right.length - left.length);

  let position = Number.MAX_SAFE_INTEGER;
  for (const alias of aliases) {
    const index = ` ${text} `.indexOf(` ${alias} `);
    if (index >= 0) position = Math.min(position, index);
  }
  return position;
};

const contextModel = (context = {}) => {
  const vehicle = context.selectedVehicle || {};
  const model = cleanText(vehicle.model || context.anchorModel || "");
  if (!model) return null;
  const brand = cleanText(
    vehicle.make || vehicle.brand || context.anchorMake || context.anchorBrand || "",
  );

  return {
    brand,
    make: brand,
    model,
    fullModel: cleanText(
      vehicle.fullModel ||
        vehicle.displayName ||
        context.anchorFullModel ||
        [brand, model].filter(Boolean).join(" "),
    ),
    modelKey: vehicle.modelKey || normalizeSearchKey(model),
    fromContext: true,
  };
};

const resolveMentionedModels = async (message = "", context = {}) => {
  const index = await getVehicleEntityIndex();
  const seen = new Set();
  const matches = findModelMatches(index, message).sort(
    (left, right) =>
      getMentionPosition(message, left) - getMentionPosition(message, right),
  );
  const fallback = matches.length ? [] : [contextModel(context)].filter(Boolean);

  return [...matches, ...fallback]
    .filter((model) => {
      const key = cleanText(
        model.modelKey || model.shortModelKey || modelLabel(model),
      ).toLowerCase();
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .slice(0, MAX_COMPOUND_MODELS);
};

const detectRequestedFacts = (message = "") => {
  const text = normalizeSearchKey(message);
  const priceBreakup = /\b(price breakup|on road breakup|onroad breakup|charges|rto and insurance)\b/.test(text);
  const priceHistory = /\b(price history|old price|previous price|price trend|launch price)\b/.test(text);
  const prices = /\b(prices?|pricelist|price list|on road prices?|onroad prices?|ex showroom prices?)\b/.test(text) &&
    (!priceBreakup || /\b(pricelist|price list|prices)\b/.test(text));

  return {
    comparison: /\b(compare|comparison|vs|versus|difference|different|which is better|better one)\b/.test(text),
    colors: /\b(colors?|colours?|paint|shade options?)\b/.test(text),
    prices,
    priceBreakup,
    priceHistory,
    variants: /\b(variants?|trims?|automatic variants?|petrol variants?|diesel variants?)\b/.test(text),
    emi: /\b(emi|loan instalment|loan installment|monthly payment|down payment)\b/.test(text),
    similar: /\b(similar cars?|alternatives?|rivals?|other options?)\b/.test(text),
    score: /\b(scores?|ratings?|ranks?|rankings?)\b/.test(text),
  };
};

const makeTool = ({ tool, entities = {}, filters = {}, resolution = {} }) => ({
  tool,
  entities: compactObject(entities),
  filters: compactObject(filters),
  ranking: null,
  output: {},
  resolution,
});

const makeResolution = ({ models = [], comparison = false } = {}) => ({
  comparisonLevel: comparison ? "model" : null,
  variantSelectionMode: comparison ? "representative_default" : "not_required",
  selectedModels: models.map((model) => ({ model })),
  selectedVariants: comparison
    ? models.map((model) => ({
        model,
        variantStrategy: "comparable_by_price_transmission",
      }))
    : [],
  changeAllowed: true,
  note: comparison
    ? "Use comparable representative variants until exact trims are selected."
    : "Keep this result scoped to the explicitly requested model.",
});

const modelPairs = (models = []) =>
  models.slice(1).map((model) => [models[0], model]);

const addPerModelTools = ({
  tools,
  modelNames,
  city,
  tool,
  entities = {},
  filters = {},
} = {}) => {
  for (const model of modelNames) {
    tools.push(
      makeTool({
        tool,
        entities: { model, primaryModel: model, ...entities },
        filters: { city, model, activeOnly: true, ...filters },
        resolution: makeResolution({ models: [model] }),
      }),
    );
  }
};

export const buildAciCompoundVehiclePlan = async ({
  message = "",
  context = {},
} = {}) => {
  const facts = detectRequestedFacts(message);
  const models = await resolveMentionedModels(message, context);
  if (!models.length) return null;

  const parsedFeatures = await parseAciFeatureRequestFromMessage({
    message,
    modelEntity: models[0],
  });
  const specAttributes = resolveSpecAttributesFromText({ message });
  const specAttributeKeys = new Set(
    specAttributes.map((attribute) => attribute.key),
  );
  const requestedFeatures = (parsedFeatures.requestedFeatures || []).filter(
    (feature) => !specAttributeKeys.has(feature.canonicalKey),
  );
  const featureKeys = requestedFeatures.map((feature) => feature.canonicalKey);
  const requestedCapabilities = [
    featureKeys.length ? "features" : "",
    specAttributes.length ? "specifications" : "",
    ...Object.entries(facts)
      .filter(([, requested]) => requested)
      .map(([name]) => name),
  ].filter(Boolean);
  const isMultiVehicle = models.length >= 2;
  const hasDistinctSurfaceIntent = [
    facts.colors,
    facts.prices,
    facts.priceBreakup,
    facts.priceHistory,
    facts.variants,
    facts.emi,
    facts.similar,
    facts.score,
  ].some(Boolean);
  const isCompound = isMultiVehicle
    ? requestedCapabilities.length >= 1
    : hasDistinctSurfaceIntent && requestedCapabilities.length >= 2;

  if (!isCompound) return null;

  const city = getCity(message, context);
  const modelNames = models.map(modelLabel).filter(Boolean);
  const pairs = modelPairs(modelNames);
  const tools = [];

  if (featureKeys.length && isMultiVehicle) {
    for (const pair of pairs) {
      const pairVehicles = pair.map((name) => {
        const source = models.find((model) => modelLabel(model) === name) || {};
        return {
          make: source.brand || source.make || "",
          brand: source.brand || source.make || "",
          model: source.model || name,
          fullModel: name,
          modelKey: source.modelKey || source.shortModelKey || "",
        };
      });
      tools.push(
        makeTool({
          tool: "vehicle_feature_comparison",
          entities: {
            models: pair,
            comparisonModels: pair,
            comparisonVehicles: pairVehicles,
            primaryModel: pair[0],
            features: featureKeys,
          },
          filters: { city, compareFeatures: featureKeys, activeOnly: true },
          resolution: makeResolution({ models: pair, comparison: true }),
        }),
      );
    }
  } else if (featureKeys.length) {
    for (const feature of requestedFeatures) {
      addPerModelTools({
        tools,
        modelNames,
        city,
        tool: "vehicle_feature_lookup",
        entities: {
          feature: feature.displayName,
          features: [feature.canonicalKey],
        },
        filters: { mustHaveFeatures: [feature.canonicalKey] },
      });
    }
  }

  const wantsOverallComparison =
    facts.comparison &&
    (!featureKeys.length || /\b(overall|full comparison|which\s+.*\s+better|better overall)\b/i.test(message));

  if (wantsOverallComparison && isMultiVehicle) {
    for (const pair of pairs) {
      tools.push(
        makeTool({
          tool: "vehicle_compare",
          entities: {
            models: pair,
            comparisonModels: pair,
            primaryModel: pair[0],
          },
          filters: { city, priceBasis: "on_road", activeOnly: true },
          resolution: makeResolution({ models: pair, comparison: true }),
        }),
      );
    }
  }

  if (facts.colors) {
    addPerModelTools({ tools, modelNames, city, tool: "vehicle_colors" });
  }

  if (facts.prices || facts.variants) {
    addPerModelTools({
      tools,
      modelNames,
      city,
      tool: "vehicle_pricelist",
      filters: { priceBasis: "on_road" },
    });
  }

  if (facts.priceBreakup) {
    addPerModelTools({
      tools,
      modelNames,
      city,
      tool: "vehicle_price_breakup",
      filters: { priceBasis: "on_road" },
    });
  }

  if (facts.priceHistory) {
    addPerModelTools({ tools, modelNames, city, tool: "vehicle_price_history" });
  }

  if (facts.emi) {
    addPerModelTools({
      tools,
      modelNames,
      city,
      tool: "vehicle_emi",
      filters: { priceBasis: "on_road" },
    });
  }

  for (const attribute of specAttributes) {
    addPerModelTools({
      tools,
      modelNames,
      city,
      tool: "vehicle_spec_attribute_lookup",
      entities: {
        attributeKey: attribute.key,
        attributeLabel: attribute.label,
      },
      filters: {
        attributeKey: attribute.key,
        attributeLabel: attribute.label,
      },
    });
  }

  if (facts.similar) {
    addPerModelTools({ tools, modelNames, city, tool: "vehicle_similar" });
  }

  if (facts.score) {
    addPerModelTools({ tools, modelNames, city, tool: "vehicle_score_insight" });
  }

  if (tools.length < 2) return null;

  const selectedComparisonVehicles = models.map((model) => ({
    make: model.brand || model.make || "",
    brand: model.brand || model.make || "",
    model: model.model || "",
    fullModel: modelLabel(model),
    modelKey: model.modelKey || model.shortModelKey || "",
    city,
  }));
  const contextPatch = isMultiVehicle
    ? {
        anchorCity: city,
        selectedComparisonSet: {
          vehicles: selectedComparisonVehicles,
          models: modelNames,
          variantSelectionMode: "representative_default",
        },
      }
    : {
        anchorMake: models[0].brand || models[0].make || "",
        anchorModel: models[0].model || "",
        anchorFullModel: modelNames[0],
        anchorCity: city,
        selectedVehicle: selectedComparisonVehicles[0],
      };

  return {
    mode: "multi_tool",
    domain: "new_car",
    conversationMode: isMultiVehicle ? "comparison" : "research",
    customerStage: isMultiVehicle ? "evaluation" : "exploration",
    tools,
    nextSteps: [
      {
        label: isMultiVehicle ? "Choose comparable variants" : "Choose the right variant",
        query: isMultiVehicle
          ? `Help me choose comparable variants of ${modelNames.join(" and ")}`
          : `Help me choose the right ${modelNames[0]} variant`,
        tool: isMultiVehicle ? "vehicle_compare" : "vehicle_pricelist",
        priority: 95,
        displayStyle: "pill",
        icon: "compare",
      },
    ],
    ambiguity: isMultiVehicle
      ? {
          level: "soft_default",
          type: "comparison_variant",
          message:
            "I am using model-level data or comparable representative variants until exact trims are selected.",
          options: [],
          selectedDefault: { variantSelectionMode: "representative_default" },
        }
      : { level: "none", type: "none", message: "", options: [] },
    contextPatch: {
      ...contextPatch,
      customerStage: isMultiVehicle ? "evaluation" : "exploration",
      conversationMode: isMultiVehicle ? "comparison" : "research",
      compoundRequest: {
        version: "aci_compound_request_v2",
        modelCount: modelNames.length,
        models: modelNames,
        requestedCapabilities,
        featureKeys,
        specAttributes: specAttributes.map((attribute) => attribute.key),
        toolCount: tools.length,
      },
    },
    clarification: null,
    confidence: 0.99,
    reasoningSummary:
      "Capability-based compound router fanned supported facts across every resolved vehicle.",
    unavailableReason: null,
  };
};

export default buildAciCompoundVehiclePlan;
