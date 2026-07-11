import mongoose from "mongoose";

import {
  getModelFeatureExplorerV2,
  answerModelFeatureV2,
  discoverFeatureVariantsV2,
  compareVariantFeaturesV2,
} from "../../aiAgent.featureResolverV2.js";
import { sampleVehicleColorImages } from "./vehiclePricelist.tool.js";
import {
  buildAciLanguageSeed,
  renderAciTemplate,
} from "../../../aciCore/language/aciAnswerLanguageComposer.js";
import {
  ACI_FEATURE_EXPLAINER_COLLECTION,
  composeAciFeatureExplanation,
  resolveAciFeatureExplainer,
} from "../../../aciCore/features/aciFeatureExplainer.service.js";

const TOOL_NAME = "vehicle_features";
const DEFAULT_CITY = "new-delhi";

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const normalizeText = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const asArray = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) return value.filter(Boolean);
  return [value].filter(Boolean);
};

const valueToText = (value) => {
  if (!value) return "";

  if (Array.isArray(value)) {
    return firstText(...value);
  }

  if (typeof value === "object") {
    return firstText(
      value.variant,
      value.variantName,
      value.trim,
      value.version,
      value.label,
      value.name,
      value.title,
      value.value,
      value.model,
    );
  }

  return cleanText(value);
};

const firstText = (...values) => {
  for (const value of values) {
    const text = valueToText(value);
    if (text) return text;
  }

  return "";
};

const collectTextArray = (...values) => {
  const result = [];

  const visit = (value) => {
    if (!value) return;

    if (Array.isArray(value)) {
      value.forEach(visit);
      return;
    }

    const text = valueToText(value);
    if (text) result.push(text);
  };

  values.forEach(visit);
  return [...new Set(result)];
};

const escapeRegExp = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const getEntities = (toolPlan = {}) => ({
  ...(toolPlan.entities || {}),
  ...(toolPlan.input || {}),
  ...(toolPlan.filters || {}),
});

const getIntent = ({ toolPlan = {} } = {}) =>
  firstText(
    toolPlan.intent,
    toolPlan.toolIntent,
    toolPlan.type,
    toolPlan.name,
    toolPlan.input?.intent,
    toolPlan.entities?.intent,
  );

const getModel = ({ toolPlan = {}, context = {}, userMessage = "" } = {}) => {
  const entities = getEntities(toolPlan);

  return firstText(
    entities.model,
    entities.models,
    entities.carModel,
    entities.vehicleModel,
    toolPlan.model,
    toolPlan.models,
    toolPlan.carModel,
    toolPlan.vehicleModel,
    toolPlan.input?.model,
    toolPlan.input?.models,
    toolPlan.filters?.model,
    context.anchorModel,
    context.selectedVehicle?.model,
    context.selectedVehicle?.modelName,
    context.activeCanvasPayload?.model,
    context.activeCanvasPayload?.selectedVehicle?.model,
    userMessage?.match?.(/\b(creta|verna|elevate|city|seltos|sonet|venue|exter|i20|alcazar|brezza|fronx|swift|dzire|baleno|nexon|harrier|safari|punch|scorpio|xuv700|xuv 700|xuv7xo|xuv 7xo|xuv300|xuv 300|xuv3xo|xuv 3xo|thar|slavia|kushaq|virtus|taigun|glanza|hyryder|fortuner|innova)\b/i)?.[0],
  );
};

const KNOWN_FEATURE_TERMS = [
  "ADAS",
  "ABS",
  "anti-lock braking system",
  "anti lock braking system",
  "sunroof",
  "panoramic sunroof",
  "single pane sunroof",
  "6 airbags",
  "six airbags",
  "airbags",
  "360 camera",
  "360 degree camera",
  "360 view camera",
  "rear camera",
  "reverse camera",
  "ventilated seats",
  "ventilated seat",
  "wireless charger",
  "wireless charging",
  "cruise control",
  "adaptive cruise",
  "connected car",
  "connected features",
  "alloy wheels",
  "alloys",
  "LED headlamps",
  "LED headlights",
  "rear ac vents",
  "rear vents",
  "automatic climate control",
  "climate control",
  "hill hold",
  "hill assist",
  "TPMS",
  "tyre pressure monitor",
  "tire pressure monitor",
  "bose speakers",
  "premium speakers",
  "music system",
  "audio system",
  "sound system",
  "stereo",
  "car stereo",
  "infotainment system",
  "infotainment",
  "touchscreen",
  "android auto",
  "apple carplay",

  "speaker system",
  "speakers",
  "touch screen",
  "apple car play",
  "carplay",
  "range",
  "driving range",
  "battery range",
  "boot space",
  "bootspace",
  "ground clearance",
  "clearance",
  "length",
  "width",
  "height",
  "wheelbase",
];

const isFeatureVariantCollision = ({ variant = "", feature = "" } = {}) => {
  const variantKey = normalizeText(variant);
  const featureKey = normalizeText(feature);
  if (!variantKey || !featureKey) return false;
  if (variantKey === featureKey) return true;

  const featureAcronyms = [
    ...String(feature || "").matchAll(/\(([A-Za-z0-9]{2,8})\)/g),
  ].map((match) => normalizeText(match[1]));

  if (featureAcronyms.includes(variantKey)) return true;
  if (variantKey === "abs" && /\babs\b|anti\s+lock\s+brak/i.test(featureKey)) return true;

  return false;
};


const FEATURE_QUERY_ALIASES = [
  {
    feature: "Integrated 2DIN Audio",
    aliases: [
      "music system",
      "audio system",
      "sound system",
      "stereo",
      "car stereo",
      "speaker system",
    ],
  },
  {
    feature: "Speakers",
    aliases: [
      "speaker",
      "speakers",
      "premium speakers",
      "bose speakers",
    ],
  },
  {
    feature: "Touchscreen",
    aliases: [
      "infotainment",
      "infotainment system",
      "touchscreen",
      "touch screen",
      "music display",
      "display audio",
    ],
  },
  {
    feature: "Android Auto",
    aliases: [
      "android auto",
      "android connect",
      "phone projection",
    ],
  },
  {
    feature: "Apple CarPlay",
    aliases: [
      "apple carplay",
      "apple car play",
      "carplay",
      "iphone carplay",
    ],
  },
];

const getFeature = ({ toolPlan = {}, userMessage = "" } = {}) => {
  const entities = getEntities(toolPlan);

  const explicit = firstText(
    entities.feature,
    entities.features,
    entities.featureName,
    entities.featureKey,
    toolPlan.feature,
    toolPlan.features,
    toolPlan.featureName,
    toolPlan.featureKey,
    toolPlan.input?.feature,
    toolPlan.input?.features,
    toolPlan.input?.featureName,
    toolPlan.filters?.feature,
  );

  if (explicit) return explicit;

  const messageNorm = normalizeText(userMessage);

  const aliasMatch = FEATURE_QUERY_ALIASES.find((entry) =>
    entry.aliases.some((alias) => messageNorm.includes(normalizeText(alias))),
  );

  if (aliasMatch?.feature) return aliasMatch.feature;

  return (
    KNOWN_FEATURE_TERMS.find((term) =>
      messageNorm.includes(normalizeText(term)),
    ) || ""
  );
};

const parseComparisonVariantsFromMessage = ({
  userMessage = "",
  model = "",
} = {}) => {
  const cleanMessage = cleanText(userMessage);
  const cleanModel = cleanText(model);

  if (!cleanMessage || !/\b(compare|comparison|difference|different|extra|vs|versus|v\/s|against)\b/i.test(cleanMessage)) {
    return [];
  }

  let body = cleanMessage;

  if (cleanModel) {
    body = body.replace(new RegExp(`\\b${escapeRegExp(cleanModel)}\\b`, "ig"), " ");
  }

  body = body
    .replace(/\b(compare|comparison|features|feature|difference|different|between|extra|over|of|in|variant|variants|what|do|i|get|show|tell|me)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  const parts = body
    .split(/\s+(?:and|vs|versus|v\/s|against|with|over|to)\s+/i)
    .map((part) =>
      part
        .replace(/\b(features|feature|variant|variants)\b/gi, " ")
        .replace(/\s+/g, " ")
        .trim(),
    )
    .filter(Boolean);

  return parts.length >= 2 ? parts.slice(0, 3) : [];
};


const normalizeFeatureCategoryKey = (value = "") => {
  const normalized = normalizeText(value);

  if (!normalized) return "";

  if (/\badas\b/.test(normalized)) return "adas";
  if (/\bsafety|airbag|abs|esc|hill hold|tpms|camera|parking sensor\b/.test(normalized)) return "safety";
  if (/\bcomfort|convenience|seat|climate|ac|cruise|armrest|keyless\b/.test(normalized)) return "comfort";
  if (/\binfotainment|music|audio|speaker|touchscreen|android auto|apple carplay|carplay|bluetooth|radio\b/.test(normalized)) return "infotainment";
  if (/\bexterior|sunroof|headlamp|tail lamp|drl|alloy|wheel|tyre|roof rail\b/.test(normalized)) return "exterior";
  if (/\binterior|dashboard|upholstery|cabin|cluster|steering|ambient\b/.test(normalized)) return "interior";
  if (/\bengine|power|torque|fuel|transmission|gearbox|mileage|performance\b/.test(normalized)) return "engine";
  if (/\bdimension|capacity|boot space|ground clearance|length|width|height|wheelbase|seating|fuel tank\b/.test(normalized)) return "dimensions";
  if (/\bconnected|bluelink|blue link|connected car|connected feature|connected service\b/.test(normalized)) return "connected";

  return normalized.replace(/\s+/g, "_");
};

const getRequestedFeatureCategory = ({ toolPlan = {}, context = {}, userMessage = "" } = {}) => {
  const fromPlan =
    toolPlan.category ||
    toolPlan.categoryKey ||
    toolPlan.input?.category ||
    toolPlan.input?.categoryKey ||
    toolPlan.entities?.category ||
    toolPlan.entities?.categoryKey ||
    toolPlan.filters?.category ||
    toolPlan.filters?.categoryKey ||
    context.anchorFeatureCategory ||
    "";

  const fromLabel =
    toolPlan.categoryLabel ||
    toolPlan.input?.categoryLabel ||
    toolPlan.entities?.categoryLabel ||
    toolPlan.filters?.categoryLabel ||
    context.anchorFeatureCategoryLabel ||
    "";

  const raw = fromPlan || fromLabel;
  if (raw) {
    return {
      key: normalizeFeatureCategoryKey(raw),
      label: fromLabel || raw,
    };
  }

  const inferred = normalizeFeatureCategoryKey(userMessage);
  if (inferred && inferred !== normalizeText(userMessage).replace(/\s+/g, "_")) {
    return {
      key: inferred,
      label: inferred.replace(/_/g, " "),
    };
  }

  return { key: "", label: "" };
};

const isFeatureExplorerIntent = (toolPlan = {}) => {
  const intent = String(
    toolPlan.intent ||
      toolPlan.toolIntent ||
      toolPlan.tool ||
      toolPlan.input?.intent ||
      "",
  );

  return [
    "vehicle_model_features_explorer",
    "vehicle_features_explorer",
    "features_explorer",
  ].includes(intent);
};

const matchesFeatureCategory = (item = {}, categoryKey = "") => {
  if (!categoryKey) return true;

  const keys = [
    item.groupKey,
    item.category,
    item.categoryKey,
    item.sectionKey,
    item.group,
    item.groupLabel,
    item.section,
  ]
    .filter(Boolean)
    .map((value) => normalizeFeatureCategoryKey(value));

  return keys.includes(categoryKey);
};

const filterFeatureExplorerByCategory = ({ result = {}, categoryKey = "", categoryLabel = "" } = {}) => {
  if (!result || !categoryKey) return result;

  const clone = { ...result };

  const groups =
    clone.groups ||
    clone.featureGroups ||
    clone.data?.groups ||
    clone.widget?.groups ||
    [];

  const filteredGroups = Array.isArray(groups)
    ? groups
        .map((group) => {
          const groupKey = normalizeFeatureCategoryKey(
            group.groupKey ||
              group.key ||
              group.category ||
              group.categoryKey ||
              group.label ||
              group.groupLabel ||
              group.name ||
              "",
          );

          const groupMatches = groupKey === categoryKey;

          const features = Array.isArray(group.features)
            ? group.features.filter((feature) =>
                groupMatches || matchesFeatureCategory(feature, categoryKey),
              )
            : group.features;

          if (!groupMatches && Array.isArray(features) && features.length === 0) {
            return null;
          }

          return {
            ...group,
            features,
          };
        })
        .filter(Boolean)
    : groups;

  const features =
    clone.features ||
    clone.data?.features ||
    clone.widget?.features ||
    [];

  const filteredFeatures = Array.isArray(features)
    ? features.filter((feature) => matchesFeatureCategory(feature, categoryKey))
    : features;

  const categoryFeatureCount =
    filteredFeatures.length ||
    filteredGroups.reduce((sum, group) => {
      if (Array.isArray(group.features)) return sum + group.features.length;
      if (typeof group.count === "number") return sum + group.count;
      return sum;
    }, 0);

  clone.groups = filteredGroups;
  clone.featureGroups = filteredGroups;
  clone.features = filteredFeatures;
  clone.category = categoryKey;
  clone.categoryLabel = categoryLabel;

  if (clone.data && typeof clone.data === "object") {
    clone.data = {
      ...clone.data,
      groups: filteredGroups,
      features: filteredFeatures,
      category: categoryKey,
      categoryLabel,
    };
  }

  if (clone.widget && typeof clone.widget === "object") {
    clone.widget = {
      ...clone.widget,
      groups: filteredGroups,
      features: filteredFeatures,
      category: categoryKey,
      categoryLabel,
    };
  }

  clone.answer = `Here are the ${categoryLabel || categoryKey} features for ${clone.model || clone.selectedModel || clone.displayModel || "this car"} — ${categoryFeatureCount} searchable features, filtered to this category.`;

  return clone;
};


const getVariantCandidates = ({

  toolPlan = {},
  context = {},
  userMessage = "",
  model = "",
} = {}) => {
  if (isFeatureExplorerIntent(toolPlan)) {
    return [];
  }


  const entities = getEntities(toolPlan);
  const cleanModel = cleanText(model);
  const cleanMessage = cleanText(userMessage);

  const explicitVariants = collectTextArray(
    entities.variants,
    toolPlan.variants,
    toolPlan.input?.variants,
    toolPlan.filters?.variants,
  );

  if (explicitVariants.length >= 2) return explicitVariants;

  const comparisonVariants = parseComparisonVariantsFromMessage({
    userMessage,
    model,
  });

  if (comparisonVariants.length >= 2) return comparisonVariants;
  if (explicitVariants.length) return explicitVariants;

  const single = firstText(
    entities.variant,
    entities.trim,
    entities.version,
    toolPlan.variant,
    toolPlan.trim,
    toolPlan.version,
    toolPlan.input?.variant,
    toolPlan.input?.trim,
    toolPlan.filters?.variant,
    context.anchorVariant,
    context.selectedVehicle?.variant,
    context.selectedVehicle?.variantName,
  );

  if (single) return [single];

  if (cleanModel && cleanMessage) {
    const withoutModel = cleanMessage
      .replace(new RegExp(`\\b${escapeRegExp(cleanModel)}\\b`, "ig"), " ")
      .replace(/\b(show|all|features|feature|does|have|with|of|in|available|which|variant|variants|compare|vs|and|price|colors|colours|open|overview|on road|on-road|this|it|its|same|current|selected|tell|me|safety|lose|lost|miss|missing|what|do|i)\b/gi, " ")
      .replace(/\s+/g, " ")
      .trim();

    if (
      withoutModel &&
      withoutModel.length <= 40 &&
      !KNOWN_FEATURE_TERMS.some((term) =>
        normalizeText(withoutModel).includes(normalizeText(term)),
      )
    ) {
      return [withoutModel];
    }
  }

  return [];
};

const getCity = ({ toolPlan = {}, context = {} } = {}) => {
  const entities = getEntities(toolPlan);

  return (
    firstText(
      entities.city,
      toolPlan.city,
      toolPlan.input?.city,
      toolPlan.filters?.city,
      context.anchorCity,
      context.selectedVehicle?.citySlug,
      context.selectedVehicle?.city,
    ) || DEFAULT_CITY
  );
};

const isExplicitModelLevelFeatureExplorerQuery = (message = "") => {
  const raw = String(message || "").trim();

  return (
    /^show\s+(all\s+)?features\s+of\s+(creta|verna|seltos|sonet|venue|exter|alcazar|city|elevate|nexon|harrier|safari|punch|thar|xuv700)$/i.test(raw) ||
    /^(creta|verna|seltos|sonet|venue|exter|alcazar|city|elevate|nexon|harrier|safari|punch|thar|xuv700)\s+features$/i.test(raw) ||
    /^open\s+feature\s+explorer\s+for\s+(creta|verna|seltos|sonet|venue|exter|alcazar|city|elevate|nexon|harrier|safari|punch|thar|xuv700)$/i.test(raw) ||
    /^list\s+(all\s+)?features\s+of\s+(creta|verna|seltos|sonet|venue|exter|alcazar|city|elevate|nexon|harrier|safari|punch|thar|xuv700)$/i.test(raw)
  );
};

const wantsComparison = ({ intent = "", userMessage = "", toolPlan = {} } = {}) => {
  const text = normalizeText(
    [
      intent,
      userMessage,
      toolPlan.intent,
      toolPlan.toolIntent,
      toolPlan.type,
      toolPlan.name,
    ].join(" "),
  );

  return (
    text.includes("compare") ||
    text.includes("comparison") ||
    /\bvs\b/i.test(userMessage || "") ||
    text.includes("difference between") ||
    text.includes("extra features")
  );
};

const wantsDiscovery = ({ intent = "", userMessage = "", toolPlan = {} } = {}) => {
  const text = normalizeText(
    [
      intent,
      userMessage,
      toolPlan.intent,
      toolPlan.toolIntent,
      toolPlan.type,
      toolPlan.name,
    ].join(" "),
  );

  return (
    text.includes("vehicle feature discovery") ||
    text.includes("feature discovery") ||
    text.includes("which variants") ||
    text.includes("which variant") ||
    /\bwhich\b.*\bvariants?\b.*\b(have|has|get|gets|with|available)\b/i.test(userMessage || "") ||
    text.includes("available in which") ||
    text.includes("cheapest") ||
    text.includes("most affordable") ||
    text.includes("do not have") ||
    text.includes("without") ||
    text.includes("miss")
  );
};

const wantsMissing = ({ userMessage = "" } = {}) => {
  const text = normalizeText(userMessage);
  return (
    text.includes("do not have") ||
    text.includes("does not have") ||
    text.includes("dont have") ||
    text.includes("don t have") ||
    text.includes("without") ||
    text.includes("miss") ||
    text.includes("missing")
  );
};

const wantsCheapest = ({ userMessage = "" } = {}) => {
  const text = normalizeText(userMessage);
  return (
    text.includes("cheapest") ||
    text.includes("most affordable") ||
    text.includes("lowest price") ||
    text.includes("least expensive")
  );
};

const normalizeCustomerCopy = (value = "") => {
  const cleaned = cleanText(value)
    .replace(/\bADAS Package\b/g, "ADAS")
    .replace(/\b6 Airbags\b/g, "6 airbags");

  const allHaveMatch = cleaned.match(
    /^(.+?) depends on the exact (.+?) sub-variant — (\d+) have it, 0 skip it\. Choose the fuel\/transmission version to confirm\.$/i,
  );

  if (allHaveMatch) {
    const [, feature, target, count] = allHaveMatch;
    return `All ${count} active ${target} sub-variants get ${feature}. Pick the exact fuel/transmission version to see the full details.`;
  }

  return cleaned;
};

const lowerFirst = (value = "") => {
  const text = cleanText(value);
  if (!text) return "";
  if (/^anti-lock braking system \(abs\)$/i.test(text)) {
    return "anti-lock braking system (ABS)";
  }
  if (/^[A-Z0-9]{2,}$/.test(text)) return text;
  return text.charAt(0).toLowerCase() + text.slice(1);
};


const CUSTOMER_FEATURE_LABELS = {
  integrated_2din_audio: "music system",
  speakers: "speakers",
  touchscreen: "touchscreen infotainment",
  android_auto: "Android Auto",
  apple_carplay: "Apple CarPlay",
  adas: "ADAS",
  sunroof: "sunroof",
  six_airbags: "6 airbags",
};

const getCustomerFeatureName = (featureLabel = "") => {
  if (/led\s*(headlamp|headlight)/i.test(featureLabel)) return "LED headlamps";
  if (/automatic\s*climate\s*control|climate\s*control|auto\s*ac/i.test(featureLabel)) return "automatic climate control";
  if (/hill\s*hold|hill\s*assist|hill\s*start/i.test(featureLabel)) return "hill assist";
  if (/connected\s*car|connected\s*features|connected\s*car\s*features/i.test(featureLabel)) return "connected car features";
  if (/rear\s*camera|reverse\s*camera/i.test(featureLabel)) return "rear camera";
  if (/ventilated\s*seat/i.test(featureLabel)) return "ventilated seats";

  const normalized = normalizeText(featureLabel).replace(/\s+/g, "_");

  if (/integrated.*2din.*audio/i.test(featureLabel)) return "a music system";
  if (/music|audio|sound|stereo/i.test(featureLabel)) return "a music system";
  if (/touchscreen|infotainment/i.test(featureLabel)) return "touchscreen infotainment";
  if (/android auto/i.test(featureLabel)) return "Android Auto";
  if (/apple.*carplay|carplay/i.test(featureLabel)) return "Apple CarPlay";
  if (/adas/i.test(featureLabel)) return "ADAS";
  if (/6 airbags|six airbags/i.test(featureLabel)) return "6 airbags";

  return CUSTOMER_FEATURE_LABELS[normalized] || lowerFirst(featureLabel);
};

const getReadableFeatureValue = ({ featureLabel = "", rows = [] } = {}) => {
  const values = [
    ...new Set(
      asArray(rows)
        .map((row) =>
          cleanText(
            row.displayValue ||
              row.value ||
              row.featureValue ||
              row.matchedValue ||
              "",
          ),
        )
        .filter(Boolean)
        .filter((value) => !/^(yes|true|available|included)$/i.test(value)),
    ),
  ];

  if (values.length !== 1) return getCustomerFeatureName(featureLabel);

  const value = values[0];
  const feature = getCustomerFeatureName(featureLabel);

  if (/sunroof/i.test(featureLabel) && !/sunroof/i.test(value)) {
    return `${value} ${feature}`;
  }

  if (/airbag/i.test(featureLabel) && /airbag/i.test(value)) {
    return lowerFirst(value);
  }

  return feature;
};

const getFeatureDisplayName = (row = {}) =>
  normalizeCustomerCopy(
    firstText(
      row.displayName,
      row.feature,
      row.featureName,
      row.label,
      row.name,
      row.title,
    ),
  );

const summarizeFeatureNames = (features = [], limit = 6) => {
  const names = [
    ...new Set(asArray(features).map(getFeatureDisplayName).filter(Boolean)),
  ];

  if (!names.length) return "";

  const shown = names.slice(0, limit);
  const extra = names.length > shown.length ? ` +${names.length - shown.length} more` : "";
  return `${shown.join(", ")}${extra}`;
};

const getFeatureLabelForAnswer = ({ result = {}, data = {}, userMessage = "" } = {}) =>
  normalizeCustomerCopy(
    firstText(
      data.featureName,
      data.resolvedFeature?.displayName,
      data.resolvedFeature?.name,
      data.feature,
      result.featureName,
      result.feature,
      getFeature({ userMessage }),
    ),
  );

const buildCustomerFeatureAnswer = ({
  result = {},
  data = {},
  model = "",
  variant = "",
  userMessage = "",
} = {}) => {
  const original = normalizeCustomerCopy(result.answer);
  const featureLabel = getFeatureLabelForAnswer({ result, data, userMessage });
  const target = cleanText(`${model}${variant ? ` ${variant}` : ""}`) || "this car";

  const availableRows = asArray(data.availableRows);
  const unavailableRows = asArray(data.unavailableRows);
  const conflictedRows = asArray(data.conflictedRows);
  const rows = asArray(data.rows);
  const featureRows = asArray(data.features);
  const groups = asArray(data.groups || data.featureGroups);
  const totalChecked =
    availableRows.length + unavailableRows.length + conflictedRows.length || rows.length;
  const isFeatureSummaryQuery = /\bfeatures?\b/i.test(userMessage || "") && !/\b(?:sunroof|airbags?|adas|camera|ventilated|rear\s+ac|tpms|isofix|cruise)\b/i.test(userMessage || "");
  const isSafetyFeatureQuery = /\bsafety\s+features?\b/i.test(userMessage || "");
  const isFeatureExplorer =
    result.intent === "vehicle_model_features_explorer" ||
    result.canvasType === "features_explorer_canvas";
  const isModelLevelFeatureSummary = isFeatureExplorer && !variant;

  if (isFeatureExplorer && (isFeatureSummaryQuery || isSafetyFeatureQuery)) {
    const evidenceCount = featureRows.length || groups.reduce((sum, group = {}) => {
      if (Array.isArray(group.features)) return sum + group.features.length;
      if (Number.isFinite(Number(group.count))) return sum + Number(group.count);
      return sum;
    }, 0);
    const sample = summarizeFeatureNames(featureRows);

    if (evidenceCount > 0) {
      const scope = isSafetyFeatureQuery ? "safety features" : "features";
      const exampleScope = isSafetyFeatureQuery ? "safety feature" : "feature";
      const prefix = isModelLevelFeatureSummary
        ? `Here are indexed ${exampleScope} examples across ${target} variants`
        : isSafetyFeatureQuery
          ? `Here are ${target}'s safety features`
          : `Here are ${target}'s available features`;
      const suffix = sample ? `: ${sample}.` : ` — ${evidenceCount} indexed ${scope}.`;
      const limitation = /lose|lost|miss|missing|without/i.test(userMessage || "")
        ? " I can show the confirmed feature differences, but this view does not yet calculate a complete upgrade-loss ladder."
        : isModelLevelFeatureSummary
          ? " Features vary by variant."
          : " Equipment can differ by fuel/transmission sub-variant.";
      return `${prefix}${suffix}${limitation}`;
    }
  }

  if (
    isFeatureSummaryQuery &&
    (!original || /could not safely match\s+[“"]{1,2}\s*[”"]{1,2}\s+to a feature/i.test(original))
  ) {
    const evidenceCount = featureRows.length || rows.length || groups.reduce((sum, group = {}) => {
      if (Array.isArray(group.features)) return sum + group.features.length;
      if (Number.isFinite(Number(group.count))) return sum + Number(group.count);
      return sum;
    }, 0);

    if (evidenceCount > 0) {
      const scope = isSafetyFeatureQuery ? "safety feature" : "feature";
      return `I found ${evidenceCount} ${scope}${evidenceCount === 1 ? "" : "s"} for ${target}. Review the feature card because equipment can differ by fuel/transmission sub-variant.`;
    }

    const scope = isSafetyFeatureQuery ? "safety feature list" : "feature list";
    return `I found ${target}, but I cannot verify the full ${scope} yet. I would rather leave a feature open than guess.`;
  }

  if (result.intent === "vehicle_feature_answer" && featureLabel) {
    const readableFeature = getReadableFeatureValue({
      featureLabel,
      rows: availableRows.length ? availableRows : rows,
    });

    if (availableRows.length > 0 && unavailableRows.length === 0 && conflictedRows.length === 0) {
      const count = totalChecked || availableRows.length;
      if (variant || count === 1) {
        return `Yes — ${target} gets ${readableFeature}.`;
      }
      return `All ${count} current ${target} variants get ${readableFeature}.`;
    }

    if (availableRows.length > 0 && unavailableRows.length > 0) {
      const total = availableRows.length + unavailableRows.length + conflictedRows.length;
      return renderAciTemplate(
        "resolved_feature_available_summary",
        {
          model: target,
          topic: readableFeature,
          availableCount: availableRows.length,
          totalCount: total,
          missingCount: unavailableRows.length,
        },
        {
          seed: buildAciLanguageSeed(
            "resolved_feature_available_summary",
            target,
            readableFeature,
            availableRows.length,
            total,
            unavailableRows.length,
            userMessage,
          ),
        },
      ).text;
    }

    if (/matching .* feature records/i.test(original) && (availableRows.length > 0 || rows.length > 0)) {
      return `Yes — ${target} gets ${readableFeature}.`;
    }
  }

  if (result.intent === "vehicle_feature_discovery") {
    const feature = featureLabel || getFeature({ userMessage });

    if ((/with \.$/i.test(original) || !featureLabel) && feature && rows.length > 0) {
      const sample = rows
        .slice(0, 4)
        .map((row) => cleanText(row.variant || row.variantName || row.label))
        .filter(Boolean)
        .join(", ");

      return `${rows.length} current ${model} variants get ${lowerFirst(feature)}${sample ? ` — ${sample}${rows.length > 4 ? ` +${rows.length - 4} more` : ""}.` : "."}`;
    }

    if ((/with \.$/i.test(original) || !featureLabel) && feature) {
      return `I could not find any current ${model} variant with ${lowerFirst(feature)}.`;
    }
  }

  return original;
};

const makeLeadingQuestion = ({
  id = "",
  label = "",
  query = "",
  intent = "",
  canvasType = "",
  model = "",
  variant = "",
  city = DEFAULT_CITY,
} = {}) => ({
  id,
  label,
  title: label,
  query,
  intent,
  canvasType,
  model,
  variant,
  contextPatch: {
    anchorModel: model,
    anchorVariant: variant,
    anchorCity: city,
    selectedVehicle: {
      model,
      variant,
      city,
    },
  },
});

const buildBridgeLeadingQuestions = ({
  model = "",
  variant = "",
  city = DEFAULT_CITY,
} = {}) => {
  const cleanModel = cleanText(model);
  const cleanVariant = cleanText(variant);
  const target = [cleanModel, cleanVariant].filter(Boolean).join(" ");

  if (!cleanModel) return [];

  if (cleanVariant) {
    return [
      makeLeadingQuestion({
        id: "open-car-overview",
        label: "Open Car Overview",
        query: `Open ${target} overview`,
        intent: "vehicle_overview",
        canvasType: "car_overview_canvas",
        model: cleanModel,
        variant: cleanVariant,
        city,
      }),
      makeLeadingQuestion({
        id: "check-on-road-price",
        label: `Check ${target} on-road price`,
        query: `Check ${target} on-road price`,
        intent: "vehicle_variant_price",
        canvasType: "pricelist_canvas",
        model: cleanModel,
        variant: cleanVariant,
        city,
      }),
      makeLeadingQuestion({
        id: "show-all-features",
        label: `Show all ${target} features`,
        query: `Show all ${target} features`,
        intent: "vehicle_model_features_explorer",
        canvasType: "features_explorer_canvas",
        model: cleanModel,
        variant: cleanVariant,
        city,
      }),
      makeLeadingQuestion({
        id: "show-colors",
        label: `Which colors are available in ${cleanModel}?`,
        query: `Which colors are available in ${cleanModel}?`,
        intent: "vehicle_colors",
        canvasType: "color_gallery_canvas",
        model: cleanModel,
        variant: "",
        city,
      }),
    ];
  }

  return [
    makeLeadingQuestion({
      id: "open-car-overview",
      label: "Open Car Overview",
      query: `Open ${cleanModel} overview`,
      intent: "vehicle_overview",
      canvasType: "car_overview_canvas",
      model: cleanModel,
      variant: "",
      city,
    }),
    makeLeadingQuestion({
      id: "check-on-road-price",
      label: `Check ${cleanModel} on-road price`,
      query: `Check ${cleanModel} on-road price`,
      intent: "vehicle_pricelist",
      canvasType: "pricelist_canvas",
      model: cleanModel,
      variant: "",
      city,
    }),
    makeLeadingQuestion({
      id: "show-all-features",
      label: `Show all ${cleanModel} features`,
      query: `Show all ${cleanModel} features`,
      intent: "vehicle_model_features_explorer",
      canvasType: "features_explorer_canvas",
      model: cleanModel,
      variant: "",
      city,
    }),
    makeLeadingQuestion({
      id: "show-colors",
      label: `Which colors are available in ${cleanModel}?`,
      query: `Which colors are available in ${cleanModel}?`,
      intent: "vehicle_colors",
      canvasType: "color_gallery_canvas",
      model: cleanModel,
      variant: "",
      city,
    }),
  ];
};

const isInactiveVariantResult = (result = {}) => {
  const rows = result.data?.rows || [];
  return (
    result.intent === "vehicle_feature_answer" &&
    rows.length === 0 &&
    /older|current new-car option|current new-car/i.test(result.answer || "")
  );
};

const FEATURE_IMPORTANCE_RANK = Object.freeze({
  critical: 5,
  high: 4,
  medium: 3,
  low: 2,
  not_applicable: 0,
});

const scoreFeatureDecisionImpact = (explainer = {}) => {
  if (explainer.importance?.safetyCritical === true) return 6;
  return Math.max(
    0,
    ...Object.values(explainer.importance || {}).map((value) =>
      FEATURE_IMPORTANCE_RANK[normalizeText(value)] || 0),
  );
};

const enrichFeatureComparisonDecisionContext = async ({ data = {} } = {}) => {
  const differenceRows = asArray(data.differenceRows);
  if (!differenceRows.length) return [];

  const enriched = [];
  for (const row of differenceRows.slice(0, 80)) {
    const explainer = await resolveAciFeatureExplainer({
      canonicalKey: row.featureKey,
      featureName: row.displayName,
    });
    if (!explainer) {
      enriched.push(row);
      continue;
    }

    enriched.push({
      ...row,
      decisionImpact: {
        canonicalKey: explainer.canonicalKey,
        buyerSummary: explainer.buyerSummary,
        whenItMattersSummary: explainer.whenItMattersSummary,
        buyerAdvice: explainer.buyerAdvice,
        featureType: explainer.featureType,
        decisionCategory: explainer.decisionCategory,
        importance: explainer.importance,
        impactScore: scoreFeatureDecisionImpact(explainer),
        sourceCollection: ACI_FEATURE_EXPLAINER_COLLECTION,
      },
    });
  }

  data.differenceRows = enriched;
  data.featureExplanationRecordCount = enriched.filter((row) => row.decisionImpact).length;
  data.decisionRelevantDifferences = enriched
    .filter((row) => row.decisionImpact)
    .sort((left, right) =>
      Number(right.decisionImpact?.impactScore || 0) -
      Number(left.decisionImpact?.impactScore || 0))
    .slice(0, 10);
  return data.decisionRelevantDifferences;
};

const toPublicResponse = async ({
  result = {},
  model = "",
  variant = "",
  city = DEFAULT_CITY,
  userMessage = "",
} = {}) => {
  const data = result.data || {};
  const inactiveVariant = isInactiveVariantResult(result);
  const modelLevelExplorer = isExplicitModelLevelFeatureExplorerQuery(userMessage);
  const modelLevelFeatureSummary =
    result.intent === "vehicle_model_features_explorer" &&
    !cleanText(variant) &&
    /\bfeatures?\b/i.test(userMessage || "");
  const broadFeatureDiscoveryResponse =
    normalizeText(result.intent).includes("vehicle feature discovery") ||
    normalizeText(result.intent).includes("feature discovery") ||
    normalizeText(result.canvasType).includes("feature_discovery");
  const shouldClearResponseVariant =
    inactiveVariant ||
    modelLevelExplorer ||
    modelLevelFeatureSummary ||
    broadFeatureDiscoveryResponse;

  const responseModel = firstText(data.model, result.model, model);
  const responseVariant =
    shouldClearResponseVariant
      ? ""
      : firstText(
          data.variant,
          result.variant,
          data.requestedVariant,
          data.selectedVariant,
          variant,
        );
  const firstVisualRow =
    asArray(data.rows)[0] ||
    asArray(data.availableRows)[0] ||
    asArray(data.unavailableRows)[0] ||
    asArray(result.rows)[0] ||
    {};
  let responseImageUrl = firstText(
    data.imageUrl,
    data.normalizedImageUrl,
    result.imageUrl,
    result.normalizedImageUrl,
    firstVisualRow.imageUrl,
    firstVisualRow.normalizedImageUrl,
  );
  let responseImageFrame =
    data.imageFrame ||
    data.displayFrameMeta ||
    result.imageFrame ||
    result.displayFrameMeta ||
    firstVisualRow.imageFrame ||
    firstVisualRow.displayFrameMeta ||
    null;

  let visualGallery = [];

  if (!responseImageUrl) {
    visualGallery = await sampleVehicleColorImages({
      make: data.brand || data.make || "",
      model: responseModel,
      limit: 4,
    });

    const selectedVisual = visualGallery[0] || null;
    responseImageUrl = firstText(
      selectedVisual?.imageUrl,
      selectedVisual?.normalizedImageUrl,
    );
    responseImageFrame = responseImageFrame || selectedVisual?.imageFrame || null;
  }

  if (shouldClearResponseVariant) {
    data.selectedVariant = "";
    data.selectedVariantKey = "";
    data.requestedVariant = "";
    result.variant = "";
    result.selectedVariant = "";
  }

  const selectedVehicle = {
    make: data.brand || data.make || "",
    brand: data.brand || data.make || "",
    model: responseModel,
    variant: responseVariant,
    variantName: responseVariant,
    selectedVariant: responseVariant,
    variantKey: shouldClearResponseVariant
      ? ""
      : data.selectedVariantKey || data.variantKey || firstVisualRow.variantKey || "",
    city,
    imageUrl: responseImageUrl,
    normalizedImageUrl: responseImageUrl,
    imageFrame: responseImageFrame,
    visualGallery,
  };

  const resolverQuestions =
    result.leadingQuestions || result.conversationSuggestions || [];

  const leadingQuestions =
    inactiveVariant || modelLevelExplorer || broadFeatureDiscoveryResponse
      ? buildBridgeLeadingQuestions({
          model: responseModel,
          variant: "",
          city,
        })
      : resolverQuestions.length
        ? resolverQuestions
        : buildBridgeLeadingQuestions({
            model: responseModel,
            variant: responseVariant,
            city,
          });

  const decisionRelevantDifferences =
    result.intent === "vehicle_feature_comparison"
      ? await enrichFeatureComparisonDecisionContext({ data })
      : [];
  let answer = buildCustomerFeatureAnswer({
    result,
    data,
    model: responseModel,
    variant: responseVariant,
    userMessage,
  });
  if (decisionRelevantDifferences.length) {
    const priorityNames = decisionRelevantDifferences
      .slice(0, 3)
      .map((row) => cleanText(row.displayName))
      .filter(Boolean);
    if (priorityNames.length) {
      answer = cleanText(
        `${answer} Start by checking how ${priorityNames.join(", ")} differ, then weigh the remaining equipment against the price gap and your regular use.`,
      );
    }
  }
  const shouldAttachFeatureExplanation = result.intent === "vehicle_feature_answer";
  const featureExplanation = shouldAttachFeatureExplanation
    ? await resolveAciFeatureExplainer({
        canonicalKey: firstText(
          data.featureKey,
          data.resolvedFeature?.canonicalKey,
          result.featureKey,
        ),
        featureName: firstText(
          data.featureName,
          data.resolvedFeature?.displayName,
          data.feature,
          result.feature,
        ),
      })
    : null;
  const featureExplanationText = composeAciFeatureExplanation(featureExplanation || {});

  if (featureExplanation && featureExplanationText) {
    data.featureExplanation = featureExplanation;
    answer = cleanText(`${answer} ${featureExplanationText}`);
  }
  const title = normalizeCustomerCopy(result.title);
  const baseSourceTransparency = result.sourceTransparency || {
    modulesChecked: [
      "vehicle_feature_catalog_v2",
      "vehicle_variant_feature_matrix_v2",
    ],
    recordCount:
      Number(data.rows?.length || 0) ||
      Number(data.features?.length || 0) ||
      Number(data.variants?.length || 0),
  };
  const sourceTransparency = featureExplanation
    ? {
        ...baseSourceTransparency,
        modulesChecked: [...new Set([
          ...asArray(baseSourceTransparency.modulesChecked),
          ACI_FEATURE_EXPLAINER_COLLECTION,
        ])],
        featureExplanationRecordCount: 1,
      }
    : decisionRelevantDifferences.length
      ? {
          ...baseSourceTransparency,
          modulesChecked: [...new Set([
            ...asArray(baseSourceTransparency.modulesChecked),
            ACI_FEATURE_EXPLAINER_COLLECTION,
          ])],
          featureExplanationRecordCount: Number(data.featureExplanationRecordCount || 0),
        }
      : baseSourceTransparency;

  const widget = {
    type: TOOL_NAME,
    tool: TOOL_NAME,
    intent: result.intent,
    canvasType: result.canvasType,
    title,
    answer,
    vehicle: selectedVehicle,
    model: selectedVehicle.model,
    variant: selectedVehicle.variant,
    selectedVariant:
      shouldClearResponseVariant
        ? null
        : data.selectedVariant || null,
    variants: data.variants || data.variantOptions || [],
    variantOptions: data.variantOptions || data.variants || [],
    features: data.features || [],
    featureList: data.features || [],
    rows: data.rows || [],
    items: data.rows || data.features || [],
    groups: data.groups || data.featureGroups || [],
    stats: data.stats || {},
    leadingQuestions,
    data,
    meta: {
      ...(result.meta || {}),
      resolver: "featureResolverV2",
      ...(featureExplanation
        ? { featureExplainerCollection: ACI_FEATURE_EXPLAINER_COLLECTION }
        : {}),
      ...(decisionRelevantDifferences.length
        ? { featureDecisionImpactCollection: ACI_FEATURE_EXPLAINER_COLLECTION }
        : {}),
    },
  };

  return {
    tool: TOOL_NAME,
    intent: result.intent,
    displayMode: result.displayMode || "canvas",
    canvasType: result.canvasType,
    inlineType: result.inlineType,
    title,
    answer,
    vehicle: selectedVehicle,
    widget,
    widgets: [widget],
    rows: data.rows || [],
    features: data.features || [],
    variants: data.variants || data.variantOptions || [],
    selectedVariant:
      inactiveVariant || modelLevelExplorer || modelLevelFeatureSummary
        ? null
        : data.selectedVariant || null,
    leadingQuestions,
    conversationSuggestions: leadingQuestions,
    data,
    contextPatch: {
      ...(result.contextPatch || {}),
      selectedVehicle,
      anchorMake: selectedVehicle.make || "",
      anchorModel: selectedVehicle.model || "",
      anchorVariant: selectedVehicle.variant || "",
      anchorCity: city,
    },
    ...(featureExplanation ? { featureExplanation } : {}),
    sourceTransparency,
    meta: {
      ...(result.meta || {}),
      resolver: "featureResolverV2",
      inactiveVariant,
      modelLevelExplorer,
      ...(featureExplanation
        ? { featureExplainerCollection: ACI_FEATURE_EXPLAINER_COLLECTION }
        : {}),
      ...(decisionRelevantDifferences.length
        ? { featureDecisionImpactCollection: ACI_FEATURE_EXPLAINER_COLLECTION }
        : {}),
    },
  };
};

const buildUnavailableResponse = ({
  model = "",
  variant = "",
  city = DEFAULT_CITY,
} = {}) => {
  const target = [model, variant].filter(Boolean).join(" ");
  const leadingQuestions = buildBridgeLeadingQuestions({ model, variant: "", city });

  return {
    tool: TOOL_NAME,
    intent: "vehicle_model_features_explorer",
    displayMode: "inline",
    canvasType: "features_explorer_canvas",
    inlineType: "unavailable_notice",
    title: target ? `${target} features` : "Vehicle features",
    answer: target
      ? `I couldn’t identify the feature request for ${target}. Try “Show all ${target} features” or ask about one feature like ADAS or sunroof.`
      : "Please tell me the car model, like “Show features of Creta”.",
    vehicle: {
      model,
      variant: "",
      city,
    },
    widget: null,
    widgets: [],
    rows: [],
    features: [],
    variants: [],
    leadingQuestions,
    conversationSuggestions: leadingQuestions,
    data: {
      model,
      variant: "",
      city,
      rows: [],
      features: [],
      variants: [],
    },
    contextPatch: {
      anchorModel: model,
      anchorVariant: "",
      anchorCity: city,
      selectedVehicle: {
        model,
        variant: "",
        city,
      },
    },
    sourceTransparency: {
      modulesChecked: [
        "vehicle_feature_catalog_v2",
        "vehicle_variant_feature_matrix_v2",
      ],
      recordCount: 0,
    },
    meta: {
      resolver: "featureResolverV2",
      unavailable: true,
    },
  };
};


const MODEL_FEATURE_SUMMARY_COLLECTION = "aci_vehicle_model_feature_summary_v1";

const modelFeatureSummaryAliases = (value = "") => {
  const normalized = normalizeText(value);
  if (!normalized) return [];

  const hyphen = normalized.replace(/\s+/g, "-");
  const underscore = normalized.replace(/\s+/g, "_");
  const compact = normalized.replace(/\s+/g, "");

  return [...new Set([normalized, hyphen, underscore, compact].filter(Boolean))];
};

const labelsFromHighlights = (...groups) => {
  const labels = [];

  for (const group of groups) {
    for (const item of Array.isArray(group) ? group : []) {
      const label = firstText(item?.label, item?.displayName, item?.name);
      if (label && !labels.some((existing) => normalizeText(existing) === normalizeText(label))) {
        labels.push(label);
      }
    }
  }

  return labels;
};

const buildModelFeatureSummaryAnswer = ({
  doc = {},
  model = "",
  safetyOnly = false,
} = {}) => {
  const modelLabel = firstText(doc.fullModel, [doc.make, doc.model].filter(Boolean).join(" "), model);

  if (safetyOnly) {
    const safety = labelsFromHighlights(doc.safetyHighlights, doc.adasHighlights).slice(0, 10);

    return safety.length
      ? `${modelLabel} safety highlights indexed across current variants include ${safety.join(", ")}. Features vary by variant, so the exact trim should be checked before finalizing.`
      : `I found the model feature summary for ${modelLabel}, but no dedicated safety highlights are indexed yet.`;
  }

  const premium = labelsFromHighlights(doc.premiumHighlights).slice(0, 8);
  const safety = labelsFromHighlights(doc.safetyHighlights).slice(0, 4);
  const comfort = labelsFromHighlights(doc.comfortHighlights).slice(0, 4);
  const infotainment = labelsFromHighlights(doc.infotainmentHighlights).slice(0, 4);

  const parts = [];
  if (premium.length) parts.push(`Top highlights include ${premium.join(", ")}`);
  if (safety.length) parts.push(`Safety coverage includes ${safety.join(", ")}`);
  if (comfort.length) parts.push(`Comfort features include ${comfort.join(", ")}`);
  if (infotainment.length) parts.push(`Tech features include ${infotainment.join(", ")}`);

  if (!parts.length) {
    return `I found feature information for ${modelLabel}, but there are not enough clear highlights to present it confidently yet.`;
  }

  return `${modelLabel}: ${parts.join(". ")}. Features vary by variant, so check the exact trim before finalizing.`;
};

const buildModelFeatureSummaryReadModelResponse = async ({
  model = "",
  city = DEFAULT_CITY,
  context = {},
  userMessage = "",
  safetyOnly = false,
} = {}) => {
  const db = mongoose.connection?.db;
  if (!db) return null;

  const aliases = modelFeatureSummaryAliases(model);
  if (!aliases.length) return null;

  const doc = await db.collection(MODEL_FEATURE_SUMMARY_COLLECTION).findOne(
    { modelKey: { $in: aliases } },
    {
      projection: {
        _id: 0,
        make: 1,
        brand: 1,
        model: 1,
        modelKey: 1,
        fullModel: 1,
        activeVariantCount: 1,
        totalIndexedFeatureCount: 1,
        premiumHighlights: 1,
        safetyHighlights: 1,
        adasHighlights: 1,
        comfortHighlights: 1,
        infotainmentHighlights: 1,
        allFeatures: 1,
        sourceCollection: 1,
        sourceBuildIds: 1,
        builtAt: 1,
        updatedAt: 1,
      },
    },
  );

  if (!doc) return null;

  const modelLabel = firstText(doc.model, model);
  const make = firstText(doc.make, doc.brand, context.selectedVehicle?.make, context.selectedVehicle?.brand);
  const fullModel = firstText(doc.fullModel, [make, modelLabel].filter(Boolean).join(" "), modelLabel);
  const answer = buildModelFeatureSummaryAnswer({ doc, model: fullModel, safetyOnly });

  const selectedVehicle = {
    make,
    brand: make,
    model: modelLabel,
    fullModel,
    variant: "",
    variantName: "",
    selectedVariant: "",
    city,
  };

  const leadingQuestions = buildBridgeLeadingQuestions({
    model: modelLabel,
    variant: "",
    city,
  });

  const features = safetyOnly
    ? [...(doc.safetyHighlights || []), ...(doc.adasHighlights || [])]
    : [
        ...(doc.premiumHighlights || []),
        ...(doc.safetyHighlights || []),
        ...(doc.comfortHighlights || []),
        ...(doc.infotainmentHighlights || []),
      ];

  const data = {
    ...doc,
    model: modelLabel,
    fullModel,
    city,
    features,
    rows: [],
    variants: [],
    dataStatus: "available",
  };

  const widget = {
    type: TOOL_NAME,
    tool: TOOL_NAME,
    intent: "vehicle_model_features_explorer",
    canvasType: "features_explorer_canvas",
    title: `${modelLabel} features`,
    answer,
    vehicle: selectedVehicle,
    model: modelLabel,
    variant: "",
    features,
    featureList: features,
    rows: [],
    items: features,
    groups: [],
    stats: {
      activeVariantCount: doc.activeVariantCount || 0,
      totalIndexedFeatureCount: doc.totalIndexedFeatureCount || 0,
    },
    leadingQuestions,
    data,
    meta: {
      resolver: "modelFeatureSummaryReadModel",
      collection: MODEL_FEATURE_SUMMARY_COLLECTION,
      builtAt: doc.builtAt || null,
      updatedAt: doc.updatedAt || null,
    },
  };

  return {
    tool: TOOL_NAME,
    intent: "vehicle_model_features_explorer",
    displayMode: "canvas",
    canvasType: "features_explorer_canvas",
    title: `${modelLabel} features`,
    answer,
    dataStatus: "available",
    vehicle: selectedVehicle,
    widget,
    widgets: [widget],
    rows: [],
    features,
    variants: [],
    selectedVariant: null,
    leadingQuestions,
    conversationSuggestions: leadingQuestions,
    data,
    contextPatch: {
      selectedVehicle,
      anchorMake: make || "",
      anchorModel: modelLabel,
      anchorVariant: "",
      anchorCity: city,
    },
    sourceTransparency: {
      modulesChecked: [
        MODEL_FEATURE_SUMMARY_COLLECTION,
        "vehicle_variant_feature_matrix_v2",
        "vehicle_feature_catalog_v2",
      ],
      recordCount: 1,
    },
    meta: {
      resolver: "modelFeatureSummaryReadModel",
      collection: MODEL_FEATURE_SUMMARY_COLLECTION,
      activeVariantCount: doc.activeVariantCount || 0,
      totalIndexedFeatureCount: doc.totalIndexedFeatureCount || 0,
      builtAt: doc.builtAt || null,
      updatedAt: doc.updatedAt || null,
      userMessage,
    },
  };
};


export const runVehicleFeaturesTool = async (args = {
}) => {

  const { toolPlan = {}, context = {}, userMessage = "" } = args;

  const requestedCategory = getRequestedFeatureCategory({
    toolPlan,
    context,
    userMessage,
  });



  const intent = getIntent({ toolPlan });
  const model = getModel({ toolPlan, context, userMessage });
  const city = getCity({ toolPlan, context });
  const feature = getFeature({ toolPlan, userMessage });
  const normalizedFeature = normalizeText(feature);
  const featureIsSummaryTopic =
    !normalizedFeature ||
    [
      "feature",
      "features",
      "feature summary",
      "features summary",
      "safety",
      "safety features",
    ].includes(normalizedFeature);
  const variants = getVariantCandidates({
    toolPlan,
    context,
    userMessage,
    model,
  }).filter((variant) => !isFeatureVariantCollision({ variant, feature }));
  const contextFeatureSummary =
    /\b(this|it|its|same|current|selected)\b/i.test(userMessage || "") &&
    /\bfeatures?\b/i.test(userMessage || "");
  const safetyFeatureSummary = /\bsafety\s+features?\b/i.test(userMessage || "");
  const genericFeatureSummary =
    /\bfeatures?\b/i.test(userMessage || "") &&
    featureIsSummaryTopic &&
    !wantsComparison({ intent, userMessage, toolPlan });
  const featureLossSummary =
    /\bfeatures?\b/i.test(userMessage || "") &&
    /\b(lose|lost|miss|missing)\b/i.test(userMessage || "") &&
    !wantsComparison({ intent, userMessage, toolPlan });
  const requestedVariant = variants[0] || "";

  if (!model) {
    return buildUnavailableResponse({
      model: "",
      variant: requestedVariant,
      city,
    });
  }

  let result;

  const normalizedIntent = normalizeText(
    [intent, toolPlan.tool, toolPlan.toolIntent, toolPlan.canvasType].join(" "),
  );

  const explicitExplorerMessage =
    /\b(show|list|open)\b.*\bfeatures?\b/i.test(userMessage || "") &&
    !/\b(which|have|has|does|cheapest|compare|vs|versus|with|without|miss)\b/i.test(
      userMessage || "",
    );

  const wantsExplorer =
    normalizedIntent.includes("vehicle model features explorer") ||
    normalizedIntent.includes("vehicle features explorer") ||
    normalizedIntent.includes("features explorer canvas") ||
    explicitExplorerMessage ||
    safetyFeatureSummary ||
    genericFeatureSummary ||
    featureLossSummary;

  if (
    wantsExplorer &&
    !requestedVariant &&
    (!requestedCategory.key || safetyFeatureSummary) &&
    !featureLossSummary &&
    !wantsComparison({ intent, userMessage, toolPlan }) &&
    (contextFeatureSummary || genericFeatureSummary || safetyFeatureSummary || isExplicitModelLevelFeatureExplorerQuery(userMessage))
  ) {
    const summaryResponse = await buildModelFeatureSummaryReadModelResponse({
      model,
      city,
      context,
      userMessage,
      safetyOnly: safetyFeatureSummary,
    });

    if (summaryResponse) return summaryResponse;
  }

  if (wantsExplorer) {
    result = await getModelFeatureExplorerV2({
      model,
      variant: requestedVariant,
      city,
    });
  } else if (wantsComparison({ intent, userMessage, toolPlan })) {
    result = await compareVariantFeaturesV2({
      model,
      variants,
      city,
    });
  } else if (wantsDiscovery({ intent, userMessage, toolPlan }) && feature) {
    result = await discoverFeatureVariantsV2({
      model,
      feature,
      city,
      includeMissing: wantsMissing({ userMessage }),
      cheapestOnly: wantsCheapest({ userMessage }),
    });
  } else if (
    feature ||
    normalizeText(intent).includes("vehicle feature answer") ||
    normalizeText(intent).includes("feature lookup")
  ) {
    result = await answerModelFeatureV2({
      model,
      variant: requestedVariant,
      feature,
      city,
    });
  } else {
    result = await getModelFeatureExplorerV2({
      model,
      variant: requestedVariant,
      city,
    });
  }

  return await toPublicResponse({
    result: result?.intent === "vehicle_model_features_explorer" && requestedCategory.key
      ? filterFeatureExplorerByCategory({
          result,
          categoryKey: requestedCategory.key,
          categoryLabel: requestedCategory.label,
        })
      : result,
    model,
    variant: requestedVariant,
    city,
    userMessage,
  });
};

export default runVehicleFeaturesTool;
