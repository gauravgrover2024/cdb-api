import {
  applyAciExplicitMessageModelContextOverride,
  buildAciContextModelEntity,
  chooseAciDynamicModelEntity,
  normalizeAciContextText,
} from "./aiAgent.contextPriority.js";
import { resolveAciExplicitMessageModelEntity } from "./aiAgent.modelContextResolver.js";
import { runVehicleFeaturesTool } from "./tools/newCars/vehicleFeatures.tool.js";
import { runVehiclePricelistNewCarsTool } from "./tools/newCars/vehiclePricelist.tool.js";

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const ACI_EARLY_FEATURE_ALIASES = [
  {
    feature: "ARAI Mileage",
    pattern: /\b(mileage|fuel\s*efficiency|average|kitna\s*deti|kitna\s*deti\s*hai|kmpl|kpl|arai\s*mileage)\b/i,
  },
  { feature: "Integrated 2DIN Audio", pattern: /\b(music\s*system|audio\s*system|sound\s*system|stereo|car\s*stereo|speaker\s*system)\b/i },
  { feature: "Speakers", pattern: /\b(speakers?|bose\s*speakers?|premium\s*speakers?)\b/i },
  { feature: "Touchscreen", pattern: /\b(infotainment\s*system|infotainment|touch\s*screen|touchscreen|music\s*display|display\s*audio)\b/i },
  { feature: "Android Auto", pattern: /\b(android\s*auto|android\s*connect|phone\s*projection)\b/i },
  { feature: "Apple CarPlay", pattern: /\b(apple\s*car\s*play|apple\s*carplay|carplay|iphone\s*carplay)\b/i },
  { feature: "Sunroof", pattern: /\b(sunroof|panoramic\s*sunroof|single\s*pane\s*sunroof)\b/i },
  { feature: "ADAS", pattern: /\b(adas|advanced\s*driver|driver\s*assist)\b/i },
  { feature: "6 Airbags", pattern: /\b(6\s*airbags?|six\s*airbags?|airbags?)\b/i },
  { feature: "Rear Camera", pattern: /\b(rear\s*camera|reverse\s*camera|parking\s*camera|rear\s*view\s*camera)\b/i },
  { feature: "360 Camera", pattern: /\b(360\s*camera|360\s*degree\s*camera|360\s*view\s*camera)\b/i },
  { feature: "Ventilated Seats", pattern: /\b(ventilated\s*seats?|seat\s*ventilation)\b/i },
  { feature: "Wireless Charging", pattern: /\b(wireless\s*charger|wireless\s*charging|phone\s*charging)\b/i },
  { feature: "Cruise Control", pattern: /\b(cruise\s*control|adaptive\s*cruise)\b/i },
  { feature: "Alloy Wheels", pattern: /\b(alloy\s*wheels?|alloys?)\b/i },
  { feature: "Rear AC Vents", pattern: /\b(rear\s*ac\s*vents?|rear\s*vents?|rear\s*blower)\b/i },
  { feature: "TPMS", pattern: /\b(tpms|tyre\s*pressure|tire\s*pressure)\b/i },

  {
    feature: "LED Headlamps",
    pattern: /\b(led\s*headlamps?|led\s*headlights?|headlamps?|headlights?|projector\s*headlamps?)\b/i,
  },
  {
    feature: "Automatic Climate Control",
    pattern: /\b(automatic\s*climate\s*control|climate\s*control|auto\s*ac|automatic\s*ac)\b/i,
  },
  {
    feature: "Hill Hold",
    pattern: /\b(hill\s*hold|hill\s*assist|hill\s*start\s*assist)\b/i,
  },

  {
    feature: "ABS",
    pattern: /\b(abs|ags|anti\s*lock\s*braking|anti-lock\s*braking|anti\s*lock\s*braking\s*system|anti-lock\s*braking\s*system|braking\s*system)\b/i,
  },
];

const ACI_DYNAMIC_CONNECTED_FEATURE_ALIAS = {
  feature: "Connected Features",
  pattern: /\b(connected\s*car|connected\s*features|connected\s*tech|connected\s*services|bluelink|blue\s*link)\b/i,
};

const getAciDynamicFeatureAlias = (message = "") => {
  const raw = String(message || "");

  const aliases = [
    ...(Array.isArray(ACI_EARLY_FEATURE_ALIASES) ? ACI_EARLY_FEATURE_ALIASES : []),
    ACI_DYNAMIC_CONNECTED_FEATURE_ALIAS,
  ].filter((entry) => entry?.feature && entry?.pattern);

  return aliases.find((entry) => entry.pattern.test(raw)) || null;
};


const buildAciDynamicFeatureCleanUserMessage = ({
  raw = "",
  model = "",
  modelEntity = null,
  feature = "",
  alias = null,
} = {}) => {
  let tail = String(raw || "").trim();
  const modelCandidates = [
    modelEntity?.matchedText,
    modelEntity?.fullModel,
    modelEntity?.model,
    model,
  ]
    .filter(Boolean)
    .sort((a, b) => String(b).length - String(a).length);

  for (const candidate of modelCandidates) {
    const safe = String(candidate).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    tail = tail.replace(new RegExp(`\\b${safe}\\b`, "ig"), " ");
  }

  if (alias?.pattern) {
    tail = tail.replace(alias.pattern, " ");
  }

  tail = tail
    .replace(/\b(does|do|is|are|has|have|having|come|comes|with|get|gets|got|available|check|tell|show|please|which|what|who|where|best|better|top|highest|maximum|max|most|gives|give|for|about|should|would|could|can|it|this|that)\b/gi, " ")
    .replace(/\b(me|mein|mai|hai|kya|in|of|the|a|an|variant|variants|car|cars|feature|features|mileage|petrol)\b/gi, " ")
    .replace(/[?.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  // The feature is already passed explicitly in toolPlan.entities.feature.
  // Do NOT append it to userMessage, otherwise the feature word can be misread as a variant.
  //
  // "Does seltos has abs in htx ivt" -> "Seltos htx ivt"
  // "Does seltos has abs"            -> "Seltos"
  if (tail) {
    return `${model} ${tail}`.replace(/\s+/g, " ").trim();
  }

  return `${model}`.replace(/\s+/g, " ").trim();
};


const extractAciDynamicComparisonVariants = ({ message = "", modelEntity = null } = {}) => {
  const raw = String(message || "").trim();
  if (!raw || !modelEntity?.model) return [];

  let tail = raw;

  const mention = String(modelEntity.matchedText || "").trim();
  if (mention) {
    const idx = raw.toLowerCase().indexOf(mention.toLowerCase());
    if (idx >= 0) {
      tail = raw.slice(idx + mention.length);
    }
  } else {
    const modelWords = [
      modelEntity.fullModel,
      modelEntity.model,
    ].filter(Boolean);

    for (const candidate of modelWords) {
      const idx = raw.toLowerCase().indexOf(String(candidate).toLowerCase());
      if (idx >= 0) {
        tail = raw.slice(idx + String(candidate).length);
        break;
      }
    }
  }

  tail = tail
    .replace(/\b(difference|different|between|in|of|features?|feature|compare|comparison|variant|variants|what|extra|do|i|get|show|tell|me|please)\b/gi, " ")
    .replace(/[?,.]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!tail) return [];

  return tail
    .split(/\s+(?:and|vs|versus|v\/s|against|over|to|with)\s+/i)
    .map((part) =>
      part
        .replace(/\b(features?|variant|variants|difference|compare|comparison)\b/gi, " ")
        .replace(/\s+/g, " ")
        .trim(),
    )
    .filter(Boolean)
    .slice(0, 3);
};


const ACI_DYNAMIC_FEATURE_CATEGORY_MAP = [
  {
    key: "safety",
    label: "Safety",
    pattern: /\b(safety|airbags?|abs|esc|hill\s*hold|tpms|camera|parking\s*sensors?|adas)\b/i,
  },
  {
    key: "comfort",
    label: "Comfort",
    pattern: /\b(comfort|convenience|seat|seats|ventilated|climate|ac|cruise|armrest|keyless|push\s*button)\b/i,
  },
  {
    key: "infotainment",
    label: "Infotainment",
    pattern: /\b(infotainment|music|audio|speakers?|touchscreen|android\s*auto|apple\s*carplay|carplay|bluetooth|radio)\b/i,
  },
  {
    key: "exterior",
    label: "Exterior",
    pattern: /\b(exterior|sunroof|headlamps?|tail\s*lamps?|drl|alloy|wheels?|tyres?|roof\s*rails?)\b/i,
  },
  {
    key: "interior",
    label: "Interior",
    pattern: /\b(interior|dashboard|upholstery|cabin|cluster|steering|ambient)\b/i,
  },
  {
    key: "engine",
    label: "Engine",
    pattern: /\b(engine|power|torque|fuel|transmission|gearbox|mileage|performance)\b/i,
  },
  {
    key: "dimensions",
    label: "Dimensions",
    pattern: /\b(dimensions?|capacity|boot\s*space|ground\s*clearance|length|width|height|wheelbase|seating|fuel\s*tank)\b/i,
  },
  {
    key: "connected",
    label: "Connected Car",
    pattern: /\b(connected|bluelink|blue\s*link|connected\s*car|connected\s*features|connected\s*services)\b/i,
  },
  {
    key: "adas",
    label: "ADAS",
    pattern: /\b(adas|driver\s*assist|lane\s*keep|blind\s*spot|adaptive\s*cruise|collision)\b/i,
  },
];

const detectAciDynamicFeatureCategory = (message = "") => {
  const raw = String(message || "");
  return ACI_DYNAMIC_FEATURE_CATEGORY_MAP.find((entry) =>
    entry.pattern.test(raw),
  ) || null;
};


const detectAciEarlyDynamicRoutedRequest = ({ message = "", modelEntity = null } = {}) => {
  const raw = String(message || "").trim();
  if (!raw || !modelEntity?.model) return null;
  const model = cleanText(modelEntity.model || "");
  const brand = cleanText(modelEntity.brand || modelEntity.make || "");
  const fullModel = cleanText(
    modelEntity.fullModel ||
      modelEntity.displayName ||
      (brand && model ? `${brand} ${model}` : model),
  );

  const categoryMatch = detectAciDynamicFeatureCategory(raw);

  const isCategoryFeatureExplorerRequest =
    /\b(featuers|features|feature\s*list)\b/i.test(raw) &&
    categoryMatch?.key &&
    categoryMatch.key !== "connected";

  if (isCategoryFeatureExplorerRequest) {
    return {
      model,
      make: brand,
      brand,
      fullModel,
      feature: "",
      category: categoryMatch.key,
      categoryLabel: categoryMatch.label || categoryMatch.key,
      cleanUserMessage: `${model} ${categoryMatch.label || categoryMatch.key} features`,
      intent: "vehicle_model_features_explorer",
      canvasType: "features_explorer_canvas",
    };
  }


  if (/\b(price\s*list|pricelist|price|on\s*road|on-road)\b/i.test(raw)) {
    return {
      model,
      make: brand,
      brand,
      fullModel,
      feature: "",
      cleanUserMessage: `${model} price`,
      intent: "vehicle_pricelist",
      canvasType: "pricelist_canvas",
    };
  }

  const hasComparisonLanguage =
    /\b(difference|different|compare|comparison|vs|versus|v\/s|extra\s+features?|upgrade)\b/i.test(raw);

  if (hasComparisonLanguage) {
    const variants = extractAciDynamicComparisonVariants({
      message: raw,
      modelEntity,
    });

    if (variants.length >= 2) {
      return {
        model,
        make: brand,
        brand,
        fullModel,
        variants,
        feature: "",
        cleanUserMessage: `${model} ${variants.join(" vs ")}`,
        intent: "vehicle_feature_comparison",
        canvasType: "comparison_canvas",
      };
    }
  }

  const alias = getAciDynamicFeatureAlias(raw);

  if (alias?.feature) {
    const isConnectedFeaturesPhrase =
      alias.feature === "Connected Features" &&
      /\bconnected\s*features\b/i.test(raw);

    const isDiscovery =
      /\b(which|show|find|list)\b.*\b(variants?|cars?)\b/i.test(raw) ||
      /\bavailable\b.*\b(which|variant|variants)\b/i.test(raw) ||
      /\bvariants?\b/i.test(raw) ||
      isConnectedFeaturesPhrase ||
      /\b(best|highest|maximum|max|most)\b.*\b(mileage|fuel\s*efficiency|average|kmpl|kpl)\b/i.test(raw) ||
      /\b(cheapest|most affordable|lowest price|without|miss|missing|do not have|dont have|don't have)\b/i.test(raw);

    return {
      model,
      make: brand,
      brand,
      fullModel,
      feature: alias.feature,
      cleanUserMessage: isDiscovery
        ? raw
        : buildAciDynamicFeatureCleanUserMessage({
            raw,
            model,
            modelEntity,
            feature: alias.feature,
            alias,
          }),
      intent: isDiscovery ? "vehicle_feature_discovery" : "vehicle_feature_answer",
      canvasType: isDiscovery ? "feature_match_builder_canvas" : "",
    };
  }

  const hasFeatureExplorerLanguage =
    /\b(show|open|list|full|all)\b.*\b(featuers|features|feature\s*list)\b/i.test(raw) ||
    /\b(featuers|features)\b$/i.test(raw) ||
    /\b(safety|comfort|infotainment|entertainment|exterior|engine|dimensions?|capacity)\s+features\b/i.test(raw) ||
    /\b(show|open|list|tell|check)\b.*\b(dimensions?|capacity|boot\s*space|ground\s*clearance|length|width|height|wheelbase)\b/i.test(raw) ||
    /\b(dimensions?|capacity|boot\s*space|ground\s*clearance|length|width|height|wheelbase)\b.*\b(of|for|in)\b/i.test(raw) ||
    /\b(best|most|money|worth|upgrade|buy)\b.*\b(features|safety|comfort)\b/i.test(raw);

  if (hasFeatureExplorerLanguage) {
    const categoryKey = categoryMatch?.key || "";
    const categoryLabel = categoryMatch?.label || "";

    return {
      model,
      make: brand,
      brand,
      fullModel,
      feature: "",
      category: categoryKey,
      categoryLabel,
      cleanUserMessage: categoryLabel
        ? `${model} ${categoryLabel} features`
        : /\b(dimensions?|capacity|boot\s*space|ground\s*clearance|length|width|height|wheelbase)\b/i.test(raw)
          ? `${model} dimensions and capacity`
          : `${model} features`,
      intent: "vehicle_model_features_explorer",
      canvasType: "features_explorer_canvas",
    };
  }

  const explicitModelMention = cleanText(
    modelEntity.matchedText ||
      modelEntity.fullModel ||
      modelEntity.displayName ||
      modelEntity.model ||
      "",
  );
  if (explicitModelMention) {
    let residual = raw;
    [
      modelEntity.matchedText,
      fullModel,
      modelEntity.model,
      model,
    ]
      .filter(Boolean)
      .sort((a, b) => String(b).length - String(a).length)
      .forEach((candidate) => {
        const safe = String(candidate).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        residual = residual.replace(new RegExp(`\\b${safe}\\b`, "ig"), " ");
      });

    residual = residual
      .replace(/\b(show|open|tell|me|about|overview|details?|car|model|variant|new)\b/gi, " ")
      .replace(/[?.,]/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    const hasSpecificIntent =
      /\b(price|pricelist|on\s*road|on-road|emi|compare|comparison|vs|versus|features?|colors?|colours?|sunroof|abs|mileage|airbags?|quotation|offer)\b/i.test(
        raw,
      );

    if (!hasSpecificIntent && residual.split(/\s+/).filter(Boolean).length <= 3) {
      return {
        model,
        make: brand,
        brand,
        fullModel,
        variant: formatAciInlineVariantName(residual),
        feature: "",
        cleanUserMessage: `${model} overview`,
        intent: "vehicle_overview",
        canvasType: "car_overview_canvas",
      };
    }
  }

  return null;
};



const formatAciInlineVariantName = (value = "") =>
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


const toAciInlineArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value].filter(Boolean);
};

const getAciInlineWidget = (response = {}) =>
  response.widget || (Array.isArray(response.widgets) ? response.widgets[0] : null) || {};

const getAciInlineRows = (response = {}) => {
  const widget = getAciInlineWidget(response);

  return toAciInlineArray(
    response.rows ||
      response.items ||
      response.data?.rows ||
      response.data?.items ||
      response.data?.matchedVariants ||
      response.widget?.rows ||
      response.widget?.items ||
      response.widget?.matchedVariants ||
      widget.rows ||
      widget.items ||
      widget.matchedVariants ||
      widget.data?.rows ||
      widget.data?.items,
  );
};

const getAciRowVariantName = (row = {}) =>
  row.variant ||
  row.variantName ||
  row.displayVariant ||
  row.name ||
  row.title ||
  row.label ||
  "";

const getAciRowValue = (row = {}) =>
  row.value ??
  row.displayValue ??
  row.featureValue ??
  row.specValue ??
  row.formattedValue ??
  row.rawValue ??
  row.text ??
  "";

const normalizeAciInlineValue = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .replace(/\baRAI\b/g, "ARAI")
    .trim();

const uniqueAciInlineValues = (items = []) => {
  const seen = new Set();
  const out = [];

  for (const item of items) {
    const key = String(item || "").toLowerCase().trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }

  return out;
};

const isAciMileageFeature = (value = "") =>
  /\b(arai\s*mileage|mileage|fuel\s*efficiency|kmpl|kpl|average)\b/i.test(
    String(value || ""),
  );

const extractAciVariantFromCleanMessage = ({ cleanUserMessage = "", model = "" } = {}) => {
  let text = String(cleanUserMessage || "").trim();
  const modelText = String(model || "").trim();

  if (!text || !modelText) return "";

  const safeModel = modelText.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  text = text.replace(new RegExp(`\\b${safeModel}\\b`, "ig"), " ");

  text = text
    .replace(/\b(arai\s*mileage|mileage|fuel\s*efficiency|kmpl|kpl|average)\b/gi, " ")
    .replace(/\b(features?|variant|variants|show|tell|check|does|do|has|have|is|are|it|which|what|who|where|best|better|top|highest|maximum|max|most|gives|give|for|worth|buy|should|would|could|can)\b/gi, " ")
    .replace(/[?.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return text ? formatAciInlineVariantName(text) : "";
};

const extractAciNumericMileage = (value = "") => {
  const text = String(value || "");
  const match = text.match(/(\d+(?:\.\d+)?)/);
  return match ? Number(match[1]) : null;
};

const buildAciMileageDirectAnswer = (
  response = {},
  { detected = {}, cleanUserMessage = "", message = "" } = {},
) => {
  const featureName =
    detected.feature ||
    response.meta?.detectedFeature ||
    response.detectedFeature ||
    response.feature ||
    response.data?.feature ||
    response.widget?.feature ||
    "";

  if (!isAciMileageFeature(featureName)) return "";

  const rows = getAciInlineRows(response).filter((row) => row?.available !== false);

  if (!rows.length) return "";

  const values = uniqueAciInlineValues(
    rows
      .map((row) => normalizeAciInlineValue(getAciRowValue(row)))
      .filter((value) => value && !/not available|false|no$/i.test(value)),
  );

  if (!values.length) return "";

  const model =
    detected.model ||
    response.meta?.detectedModel ||
    response.model ||
    response.data?.model ||
    response.widget?.model ||
    "this car";

  const askedVariant =
    extractAciVariantFromCleanMessage({
      cleanUserMessage: cleanUserMessage || message,
      model,
    }) ||
    (rows.length === 1 ? formatAciInlineVariantName(getAciRowVariantName(rows[0])) : "");

  const subject = [model, askedVariant].filter(Boolean).join(" ");

  const isBestMileageQuery =
    /\b(best|highest|maximum|max|most)\b.*\b(mileage|fuel\s*efficiency|average|kmpl|kpl)\b/i.test(
      String(message || cleanUserMessage || ""),
    );

  if (isBestMileageQuery) {
    const numericPairs = rows
      .map((row) => ({
        variant: formatAciInlineVariantName(getAciRowVariantName(row)),
        value: normalizeAciInlineValue(getAciRowValue(row)),
        numeric: extractAciNumericMileage(getAciRowValue(row)),
      }))
      .filter((item) => Number.isFinite(item.numeric))
      .sort((a, b) => b.numeric - a.numeric);

    if (numericPairs.length) {
      const best = numericPairs[0];
      const topVariants = uniqueAciInlineValues(
        numericPairs
          .filter((item) => item.numeric === best.numeric)
          .map((item) => item.variant)
          .filter(Boolean),
      ).slice(0, 4);

      const variantCopy = topVariants.length
        ? ` Top variant${topVariants.length > 1 ? "s" : ""}: ${topVariants.join(", ")}.`
        : "";

      return `The best claimed mileage in ${model} is ${best.value}.${variantCopy}`;
    }
  }

  if (rows.length === 1 || values.length === 1) {
    return `${subject} mileage is ${values[0]}.`;
  }

  const numericValues = values
    .map(extractAciNumericMileage)
    .filter((value) => Number.isFinite(value));

  if (numericValues.length >= 2) {
    const min = Math.min(...numericValues);
    const max = Math.max(...numericValues);

    if (min !== max) {
      const unit = values.find((value) => /\bkmpl\b|\bkpl\b/i.test(value))?.match(/\b(kmpl|kpl)\b/i)?.[1] || "kmpl";
      return `${subject} mileage ranges from ${min} to ${max} ${unit}, depending on the exact transmission/variant.`;
    }
  }

  return `${subject} mileage varies by variant — ${values.slice(0, 3).join(", ")}${values.length > 3 ? " and more" : ""}.`;
};


const polishAciEarlyFeatureResponseCopy = (response = {}, options = {}) => {
  if (!response || typeof response !== "object") return response;

  const directMileageAnswer = buildAciMileageDirectAnswer(response, options);
  let answer = directMileageAnswer || String(response.answer || "");

  answer = answer
    .replace(/\baRAI Mileage\b/g, "ARAI mileage")
    .replace(/\barai mileage\b/gi, "ARAI mileage")
    .replace(/anti-lock\s+Braking\s+System\s*\(ABS\)/gi, "Anti-lock Braking System (ABS)")
    .replace(/anti-lock\s+braking\s+system\s*\(ABS\)/gi, "Anti-lock Braking System (ABS)")
    .replace(/\bHTX IVT\b/g, "HTX iVT")
    .replace(/\bSX IVT\b/g, "SX iVT")
    .replace(/\bSingle Pane sunroof\b/g, "single-pane sunroof")
    .replace(/\bPanoramic sunroof\b/g, "panoramic sunroof");

  const singleVariantMatch = answer.match(
    /Good news\s*—\s*all\s+1\s+current\s+(.+?)\s+variants\s+get\s+(.+?)\./i,
  );

  if (singleVariantMatch) {
    const variantName = formatAciInlineVariantName(singleVariantMatch[1]);
    const featureName = singleVariantMatch[2]
      .replace(/\baRAI Mileage\b/g, "ARAI mileage")
      .replace(/\barai mileage\b/gi, "ARAI mileage")
      .replace(/anti-lock\s+Braking\s+System\s*\(ABS\)/gi, "Anti-lock Braking System (ABS)")
      .replace(/anti-lock\s+braking\s+system\s*\(ABS\)/gi, "Anti-lock Braking System (ABS)");

    answer = `Yes — ${variantName} gets ${featureName}.`;
  }

  response.answer = answer;

  if (response.data && typeof response.data === "object") {
    response.data.answer = answer;
  }

  if (response.widget && typeof response.widget === "object") {
    response.widget.answer = answer;
  }

  return response;
};




const pickAciContextValue = (...values) => {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    const text = String(value).trim();
    if (text) return value;
  }
  return "";
};


const extractAciScopedVariantFromCleanMessage = ({
  cleanUserMessage = "",
  model = "",
  fullModel = "",
  make = "",
} = {}) => {
  let text = String(cleanUserMessage || "").trim();

  [
    fullModel,
    model,
    make,
  ]
    .filter(Boolean)
    .sort((a, b) => String(b).length - String(a).length)
    .forEach((candidate) => {
      const safe = String(candidate).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      text = text.replace(new RegExp(`\\b${safe}\\b`, "ig"), " ");
    });

  text = text
    .replace(/\b(abs|ags|anti\s*lock\s*braking|anti-lock\s*braking|sunroof|mileage|arai\s*mileage|features?|feature|price|pricelist|overview|details?|on\s*road|on-road)\b/gi, " ")
    .replace(/\b(does|do|is|are|has|have|having|with|get|gets|got|which|what|best|highest|maximum|max|most|variant|variants|car|cars|it|this|that|current|selected|new|old)\b/gi, " ")
    .replace(/[?.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return text ? formatAciInlineVariantName(text) : "";
};

const shouldCarryAciCurrentVariant = ({ message = "", dynamicModelEntity = null, explicitVariant = "", context = {} } = {}) => {
  if (explicitVariant) return false;
  if (!dynamicModelEntity?.fromContext) return false;
  if (!context?.anchorVariant && !context?.selectedVehicle?.variant && !context?.selectedVehicle?.variantName) return false;

  // Carry variant only for true pronoun/current-variant questions.
  // Do not carry it for model-level questions like "Does Seltos have ABS?"
  return /\b(it|this|that|current one|selected one|this variant|current variant|selected variant)\b/i.test(
    String(message || ""),
  );
};

const buildAciFeatureAuthorityContextPatch = ({
  context = {},
  detected = {},
  dynamicModelEntity = null,
  cleanUserMessage = "",
  message = "",
} = {}) => {
  const make =
    detected.make ||
    detected.brand ||
    dynamicModelEntity?.brand ||
    context?.anchorMake ||
    context?.selectedVehicle?.make ||
    context?.selectedVehicle?.brand ||
    "";

  const model =
    detected.model ||
    dynamicModelEntity?.model ||
    context?.anchorModel ||
    context?.selectedVehicle?.model ||
    "";

  const fullModel =
    detected.fullModel ||
    dynamicModelEntity?.fullModel ||
    context?.anchorFullModel ||
    context?.selectedVehicle?.fullModel ||
    (make && model ? `${make} ${model}` : model);

  const explicitVariant = extractAciScopedVariantFromCleanMessage({
    cleanUserMessage,
    model,
    fullModel,
    make,
  });

  const carriedVariant = shouldCarryAciCurrentVariant({
    message,
    dynamicModelEntity,
    explicitVariant,
    context,
  })
    ? pickAciContextValue(
        context?.anchorVariant,
        context?.selectedVehicle?.variant,
        context?.selectedVehicle?.variantName,
      )
    : "";

  const isComparisonIntent =
    detected?.intent === "vehicle_feature_comparison" ||
    detected?.canvasType === "comparison_canvas";

  const nextVariant = isComparisonIntent
    ? ""
    : explicitVariant || carriedVariant || "";
  const contextVehicle = context?.selectedVehicle || {};
  const contextVehicleMatchesModel =
    normalizeAciContextText(contextVehicle.model || "") ===
    normalizeAciContextText(model || "");

  return {
    selectedVehicle: {
      ...(contextVehicleMatchesModel ? contextVehicle : {}),
      make,
      brand: make,
      model,
      fullModel,
      variant: nextVariant,
      variantName: nextVariant,
      city: pickAciContextValue(context?.anchorCity, context?.city, context?.selectedVehicle?.city, "new-delhi"),
      citySlug: pickAciContextValue(context?.anchorCity, context?.citySlug, context?.selectedVehicle?.citySlug, "new-delhi"),
    },
    anchorMake: make,
    anchorModel: model,
    anchorFullModel: fullModel,
    anchorVariant: nextVariant,
    anchorCity: pickAciContextValue(context?.anchorCity, context?.city, context?.selectedVehicle?.citySlug, "new-delhi"),
    selectedColor: null,
    ...(isComparisonIntent
      ? {
          selectedComparisonSet: {
            model,
            variants: Array.isArray(detected?.variants) ? detected.variants : [],
          },
        }
      : {}),
  };
};

const applyAciFeatureAuthorityContextPatch = (response = {}, patch = {}) => {
  if (!response || typeof response !== "object") return response;

  const mergeAuthorityPatch = (existingPatch = {}) => {
    const existingVehicle =
      existingPatch.selectedVehicle ||
      response.vehicle ||
      response.widget?.vehicle ||
      {};
    const patchVehicle = patch.selectedVehicle || {};
    const existingModel = normalizeAciContextText(existingVehicle.model || "");
    const patchModel = normalizeAciContextText(
      patchVehicle.model || patch.anchorModel || "",
    );
    const canPreserveExistingVehicle =
      existingVehicle &&
      (!existingModel || !patchModel || existingModel === patchModel);
    const selectedVehicle = {
      ...(canPreserveExistingVehicle ? existingVehicle : {}),
      ...patchVehicle,
    };

    if (canPreserveExistingVehicle) {
      selectedVehicle.imageUrl =
        patchVehicle.imageUrl || existingVehicle.imageUrl || "";
      selectedVehicle.normalizedImageUrl =
        patchVehicle.normalizedImageUrl ||
        existingVehicle.normalizedImageUrl ||
        existingVehicle.imageUrl ||
        "";
      selectedVehicle.imageFrame =
        patchVehicle.imageFrame || existingVehicle.imageFrame || null;
      selectedVehicle.displayFrameMeta =
        patchVehicle.displayFrameMeta ||
        existingVehicle.displayFrameMeta ||
        selectedVehicle.imageFrame ||
        null;
    }

    return {
      ...existingPatch,
      ...patch,
      selectedVehicle,
    };
  };

  response.contextPatch = {
    ...mergeAuthorityPatch(response.contextPatch || {}),
  };

  response.context = {
    ...mergeAuthorityPatch(response.context || {}),
  };

  if (response.data && typeof response.data === "object") {
    response.data = {
      ...response.data,
      contextPatch: mergeAuthorityPatch(response.data.contextPatch || {}),
    };
  }

  if (response.widget && typeof response.widget === "object") {
    response.widget = {
      ...response.widget,
      contextPatch: mergeAuthorityPatch(response.widget.contextPatch || {}),
    };
  }

  return response;
};



const ACI_EARLY_GATE_INTERNAL_PATTERN =
  /\b(loan|closure|lan|case\s*id|customer|cust\s*id|policy|payout|rc|challan|cdrive|internal|file\s*no|agreement|collection|overdue|repo|noc|insurance\s*expiry)\b/i;

const ACI_EARLY_GATE_LEAD_PATTERN =
  /\b(best\s*price|quotation|quote|final\s*price|deal|discount|offer|offers|callback|call\s*back|book|booking|finance|exchange|insurance)\b/i;

const countAciEarlyGateIntentFamilies = (message = "") => {
  const raw = String(message || "");
  const checks = [
    /\b(price|pricelist|on\s*road|on-road|ex\s*showroom|ex-showroom)\b/i,
    /\b(compare|comparison|vs|versus|v\/s|with)\b/i,
    /\b(emi|loan|down\s*payment|tenure|interest)\b/i,
    /\b(offer|offers|discount|deal|best\s*price|quotation|quote)\b/i,
    /\b(color|colors|colour|colours|black|white|red|blue|grey|gray)\b/i,
    /\b(feature|features|sunroof|adas|airbags?|abs|mileage|camera|ventilated)\b/i,
  ];

  return checks.reduce((count, pattern) => count + (pattern.test(raw) ? 1 : 0), 0);
};

const shouldSkipAciEarlyFeatureGate = (message = "") => {
  const raw = String(message || "").trim();
  if (!raw) return true;

  // Internal office queries must never be interpreted as new-car model names.
  // Example: "Loan closure 7077" was being misread as Mahindra Logan.
  if (ACI_EARLY_GATE_INTERNAL_PATTERN.test(raw)) return true;

  // Quote/best-price/offer flows need planner + lead tools, not a quick price card.
  if (ACI_EARLY_GATE_LEAD_PATTERN.test(raw)) return true;

  // Multi-intent needs executor secondaryResponses. Early gate can only return one card.
  if (countAciEarlyGateIntentFamilies(raw) >= 2) return true;

  return false;
};



export const maybeRunAciEarlyFeatureGate = async ({
  message = "",
  context = {},
  selectedEntity = null,
  filters = {},
} = {}) => {
  if (shouldSkipAciEarlyFeatureGate(message)) {
    return null;
  }

  const dynamicModelEntityFromText = await resolveAciExplicitMessageModelEntity(message);

  applyAciExplicitMessageModelContextOverride({
    message,
    context,
    dynamicModelEntity: dynamicModelEntityFromText,
  });
  const dynamicModelEntityFromContext = buildAciContextModelEntity({
    context,
    selectedEntity,
  });

  const dynamicModelEntity = chooseAciDynamicModelEntity({
    textEntity: dynamicModelEntityFromText,
    contextEntity: dynamicModelEntityFromContext,
    message,
  });

  const detected =
    detectAciEarlyDynamicRoutedRequest({
      message,
      modelEntity: dynamicModelEntity,
    });

  if (!detected) return null;

  const toolRunner =
    detected.intent === "vehicle_overview"
      ? runVehiclePricelistNewCarsTool
      : detected.intent === "vehicle_pricelist" ||
    detected.canvasType === "pricelist_canvas"
      ? runVehiclePricelistNewCarsTool
      : runVehicleFeaturesTool;

  const cleanUserMessage = detected.cleanUserMessage || message;

  const toolPlan = {
    tool: detected.intent,
    intent: detected.intent,
    toolIntent: detected.intent,
    canvasType: detected.canvasType,
    entities: {
      make: detected.make || dynamicModelEntity?.brand || "",
      brand: detected.brand || dynamicModelEntity?.brand || "",
      model: detected.model,
      fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      feature: detected.feature || "",
      variants: detected.variants || [],
      variant: detected.variant || "",
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
    input: {
      make: detected.make || dynamicModelEntity?.brand || "",
      brand: detected.brand || dynamicModelEntity?.brand || "",
      model: detected.model,
      fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      feature: detected.feature || "",
      variants: detected.variants || [],
      variant: detected.variant || "",
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
    filters: {
      ...(filters || {}),
      make: detected.make || dynamicModelEntity?.brand || "",
      brand: detected.brand || dynamicModelEntity?.brand || "",
      model: detected.model,
      fullModel: detected.fullModel || dynamicModelEntity?.fullModel || "",
      feature: detected.feature || "",
      variants: detected.variants || [],
      variant: detected.variant || "",
      category: detected.category || "",
      categoryLabel: detected.categoryLabel || "",
    },
  };

  const preToolAuthorityContextPatch = buildAciFeatureAuthorityContextPatch({
    context,
    detected,
    dynamicModelEntity,
    cleanUserMessage,
    message,
  });

  const scopedAnchorVariant = String(
    preToolAuthorityContextPatch.anchorVariant || "",
  );
  const scopedSelectedVehicle = {
    ...(preToolAuthorityContextPatch.selectedVehicle || {}),
    variant: scopedAnchorVariant,
    variantName: scopedAnchorVariant,
  };

  const scopedSelectedEntity =
    selectedEntity && typeof selectedEntity === "object"
      ? {
          ...selectedEntity,
          selectedVehicle: {
            ...(selectedEntity.selectedVehicle || selectedEntity.vehicle || {}),
            ...scopedSelectedVehicle,
          },
          vehicle: {
            ...(selectedEntity.vehicle || selectedEntity.selectedVehicle || {}),
            ...scopedSelectedVehicle,
          },
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
        }
      : {
          selectedVehicle: scopedSelectedVehicle,
          vehicle: scopedSelectedVehicle,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
        };

  const scopedFeatureContext = {
    ...(context || {}),
    ...preToolAuthorityContextPatch,
    selectedEntity: scopedSelectedEntity,
    anchorVariant: scopedAnchorVariant,
    selectedVehicle: scopedSelectedVehicle,
  };

  const scopedFeatureFilters =
    filters && typeof filters === "object"
      ? {
          ...filters,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
        }
      : {
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
        };

  const scopedDetected =
    detected && typeof detected === "object"
      ? {
          ...detected,
          variant: scopedAnchorVariant,
          variantName: scopedAnchorVariant,
          selectedVariant: scopedAnchorVariant,
          selectedVariantKey: scopedAnchorVariant,
          requestedVariant: scopedAnchorVariant,
          entities: {
            ...(detected.entities || {}),
            variant: scopedAnchorVariant,
            variantName: scopedAnchorVariant,
            selectedVariant: scopedAnchorVariant,
            selectedVariantKey: scopedAnchorVariant,
            requestedVariant: scopedAnchorVariant,
          },
        }
      : detected;

  const scopedToolPlan =
    toolPlan && typeof toolPlan === "object"
      ? {
          ...toolPlan,
          variant: scopedAnchorVariant,
	          variantName: scopedAnchorVariant,
	          selectedVariant: scopedAnchorVariant,
	          selectedVariantKey: scopedAnchorVariant,
	          requestedVariant: scopedAnchorVariant,
	          entities: {
	            ...(toolPlan.entities || {}),
	            variant: scopedAnchorVariant,
	            variantName: scopedAnchorVariant,
	            selectedVariant: scopedAnchorVariant,
	            selectedVariantKey: scopedAnchorVariant,
	            requestedVariant: scopedAnchorVariant,
	          },
	          input: {
	            ...(toolPlan.input || {}),
	            variant: scopedAnchorVariant,
	            variantName: scopedAnchorVariant,
	            selectedVariant: scopedAnchorVariant,
	            selectedVariantKey: scopedAnchorVariant,
	            requestedVariant: scopedAnchorVariant,
	          },
	          filters: {
	            ...(toolPlan.filters || {}),
	            variant: scopedAnchorVariant,
	            variantName: scopedAnchorVariant,
	            selectedVariant: scopedAnchorVariant,
	            selectedVariantKey: scopedAnchorVariant,
	            requestedVariant: scopedAnchorVariant,
	          },
	        }
	      : toolPlan;

  let response = await toolRunner({
    detected: scopedDetected,
    filters: scopedFeatureFilters,
    context: scopedFeatureContext,
    toolPlan: scopedToolPlan,
    selectedEntity: scopedSelectedEntity,
    userMessage: cleanUserMessage,
  });

  let overviewAuthorityContextPatch = null;

  if (detected.intent === "vehicle_overview") {
    const overviewVehicle =
      response.vehicle ||
      response.widget?.vehicle ||
      response.contextPatch?.selectedVehicle ||
      preToolAuthorityContextPatch.selectedVehicle ||
      {};
    const overviewContextPatch = {
      ...preToolAuthorityContextPatch,
      ...(response.contextPatch || {}),
      selectedVehicle: {
        ...(overviewVehicle || {}),
        variant: detected.variant || "",
        variantName: detected.variant || "",
        selectedVariant: detected.variant || "",
      },
      anchorMake:
        overviewVehicle.make ||
        overviewVehicle.brand ||
        response.contextPatch?.anchorMake ||
        preToolAuthorityContextPatch.anchorMake ||
        "",
      anchorModel:
        overviewVehicle.model ||
        response.contextPatch?.anchorModel ||
        preToolAuthorityContextPatch.anchorModel ||
        detected.model ||
        "",
      anchorFullModel:
        overviewVehicle.fullModel ||
        overviewVehicle.displayName ||
        response.contextPatch?.anchorFullModel ||
        preToolAuthorityContextPatch.anchorFullModel ||
        detected.fullModel ||
        "",
      anchorVariant: detected.variant || "",
    };

    response = {
      ...response,
      tool: "vehicle_overview",
      intent: "vehicle_overview",
      canvasType: "car_overview_canvas",
      answer: `Opened ${overviewVehicle.displayName || detected.model} overview.`,
      vehicle: overviewContextPatch.selectedVehicle,
      contextPatch: overviewContextPatch,
      widget: {
        ...(response.widget || {}),
        type: "vehicle_overview",
        tool: "vehicle_overview",
        intent: "vehicle_overview",
        canvasType: "car_overview_canvas",
        title: `${overviewVehicle.displayName || detected.model} overview`,
        answer: `Opened ${overviewVehicle.displayName || detected.model} overview.`,
        vehicle: overviewContextPatch.selectedVehicle,
        rows: response.rows || response.widget?.rows || [],
        items: response.items || response.widget?.items || response.rows || [],
        contextPatch: overviewContextPatch,
      },
    };

    overviewAuthorityContextPatch = overviewContextPatch;
  }

  polishAciEarlyFeatureResponseCopy(response, { detected, cleanUserMessage, message });

  const authorityContextPatch =
    overviewAuthorityContextPatch || preToolAuthorityContextPatch;

  applyAciFeatureAuthorityContextPatch(response, authorityContextPatch);



  return {
    ...response,
    meta: {
      ...(response?.meta || {}),
      earlyFeatureGate: true,
      detectedModel: detected.model,
      detectedFullModel: detected.fullModel || "",
      detectedFeature: detected.feature,
      detectedCategory: detected.category || "",
      detectedCategoryLabel: detected.categoryLabel || "",
      modelMatchedText: dynamicModelEntity?.matchedText || "",
      modelCorrectionConfidence: dynamicModelEntity?.confidence || null,
    },
  };
};
