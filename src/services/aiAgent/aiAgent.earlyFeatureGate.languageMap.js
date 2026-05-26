/**
 * Early gate language maps.
 *
 * Important:
 * - These are NOT factual vehicle data.
 * - These are only language/query hints used to route user wording to deterministic tools.
 * - Actual feature availability, variant support, specs, prices, colors, and facts must come from DB-backed tools.
 * - Do not add models, variants, prices, colors, offers, or availability facts here.
 * - Long-term direction: move feature synonyms/category metadata into DB/catalog-backed configuration.
 */

export const ACI_EARLY_FEATURE_ALIASES = [
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

export const ACI_DYNAMIC_CONNECTED_FEATURE_ALIAS = {
  feature: "Connected Features",
  pattern: /\b(connected\s*car|connected\s*features|connected\s*tech|connected\s*services|bluelink|blue\s*link)\b/i,
};

export const getAciDynamicFeatureAlias = (message = "") => {
  const raw = String(message || "");

  const aliases = [
    ...(Array.isArray(ACI_EARLY_FEATURE_ALIASES) ? ACI_EARLY_FEATURE_ALIASES : []),
    ACI_DYNAMIC_CONNECTED_FEATURE_ALIAS,
  ].filter((entry) => entry?.feature && entry?.pattern);

  return aliases.find((entry) => entry.pattern.test(raw)) || null;
};


export const buildAciDynamicFeatureCleanUserMessage = ({
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


export const extractAciDynamicComparisonVariants = ({ message = "", modelEntity = null } = {}) => {
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


export const ACI_DYNAMIC_FEATURE_CATEGORY_MAP = [
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
