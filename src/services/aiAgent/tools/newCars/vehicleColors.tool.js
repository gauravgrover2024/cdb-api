import mongoose from "mongoose";

const TOOL_NAME = "vehicle_colors";
const INTENT = "vehicle_colors";
const CANVAS_TYPE = "color_studio_canvas";
const COLLECTION_NAME = "vehicle_colors_v2";
const DEFAULT_CITY = "new-delhi";

const asText = (value = "") => String(value || "").trim();

const titleCaseWords = (value = "") =>
  cleanText(value)
    .split(" ")
    .map((part) => {
      if (!part) return "";
      if (/^[A-Z0-9]+$/.test(part)) return part;
      if (/^\d/.test(part)) return part.toUpperCase();
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join(" ");

const cleanText = (value = "") => asText(value).replace(/\s+/g, " ").trim();

const firstText = (...values) => {
  for (const value of values) {
    const text = cleanText(value);
    if (text) return text;
  }
  return "";
};

const escapeRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const exactRegex = (value = "") =>
  new RegExp(`^\\s*${escapeRegex(cleanText(value))}\\s*$`, "i");

const slugify = (value = "", fallback = "") => {
  const slug = cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  return slug || fallback;
};

const normalizeKey = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeModelKey = (value = "", make = "") => {
  let text = normalizeKey(value);
  const makeKey = normalizeKey(make);

  if (makeKey && text.startsWith(`${makeKey} `)) {
    text = text.slice(makeKey.length).trim();
  }

  if (makeKey === "maruti" && text.startsWith("maruti suzuki ")) {
    text = text.replace(/^maruti suzuki\s+/, "");
  }

  if (makeKey === "mercedes benz" && text.startsWith("mercedes benz ")) {
    text = text.replace(/^mercedes benz\s+/, "");
  }

  return text;
};

const displayCity = (value = DEFAULT_CITY) => {
  const text = cleanText(value || DEFAULT_CITY);
  if (!text) return "New Delhi";

  return text
    .replace(/-/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
};

const normalizeHex = (value = "") => {
  const text = cleanText(value).replace(/^#/, "");
  if (/^[0-9a-f]{3}$/i.test(text) || /^[0-9a-f]{6}$/i.test(text)) {
    return `#${text}`;
  }
  return "#E5E7EB";
};

const normalizePublicImageUrl = (value = "") => {
  const text = cleanText(value);
  if (!text) return "";

  if (/^(https?:)?\/\//i.test(text)) return text;

  if (text.startsWith("/media/")) {
    return `https://pub-8504a10fc1c04f02ac8760cb90462ae3.r2.dev${text}`;
  }

  return "";
};

const normalizeFrameMeta = (frame = {}) => {
  if (!frame || typeof frame !== "object") return frame || null;

  const readNumber = (...values) => {
    for (const value of values) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
    return null;
  };

  const x = readNumber(frame.x, frame.left, frame.minX);
  const y = readNumber(frame.y, frame.top, frame.minY);
  const width = readNumber(frame.width, frame.w);
  const height = readNumber(frame.height, frame.h);
  const canvasWidth = readNumber(frame.canvas_width, frame.canvasWidth, frame.naturalWidth, frame.imageWidth, frame.sourceWidth);
  const canvasHeight = readNumber(frame.canvas_height, frame.canvasHeight, frame.naturalHeight, frame.imageHeight, frame.sourceHeight);

  if (
    !Number.isFinite(x) ||
    !Number.isFinite(y) ||
    !Number.isFinite(width) ||
    !Number.isFinite(height) ||
    !Number.isFinite(canvasWidth) ||
    !Number.isFinite(canvasHeight) ||
    width <= 0 ||
    height <= 0 ||
    canvasWidth <= 0 ||
    canvasHeight <= 0
  ) {
    return frame;
  }

  const centerX = (x + width / 2) / canvasWidth;
  const centerY = (y + height / 2) / canvasHeight;
  const widthRatio = width / canvasWidth;
  const heightRatio = height / canvasHeight;
  const scale = Math.min(
    1.3,
    Math.max(1, Math.max(0.86 / Math.max(widthRatio, 0.01), 0.58 / Math.max(heightRatio, 0.01))),
  );

  return {
    ...frame,
    naturalWidth: canvasWidth,
    naturalHeight: canvasHeight,
    bounds: { x, y, width, height },
    cssVars: {
      ...(frame.cssVars || {}),
      "--car-frame-scale": Number(scale.toFixed(3)),
      "--car-frame-x": `${Number(((0.5 - centerX) * 100).toFixed(2))}%`,
      "--car-frame-y": `${Number(((0.5 - centerY) * 100).toFixed(2))}%`,
      "--car-frame-origin": "center center",
    },
  };
};

const firstMeaningfulFrame = (...frames) =>
  frames.find((frame) => frame && typeof frame === "object" && Object.keys(frame).length) || null;

const getEntities = (toolPlan = {}) => ({
  ...(toolPlan.entities || {}),
  ...(toolPlan.input || {}),
  ...(toolPlan.filters || {}),
});

const stripColorWordsFromMessage = (message = "") =>
  cleanText(message)
    .replace(
      /\b(show|open|display|available|all|exterior|car|vehicle)\b/gi,
      " ",
    )
    .replace(
      /\b(colou?rs?|paint|shade|shades|colour options|color options)\b/gi,
      " ",
    )
    .replace(/\b(of|for|in|please|pls|available)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

const inferModelFromMessage = (message = "") => {
  const cleaned = stripColorWordsFromMessage(message);
  const tokens = cleaned.split(/\s+/).filter(Boolean);

  if (!tokens.length) return "";
  if (tokens.length > 5) return "";

  return cleaned;
};

const getRequestedMake = ({ toolPlan = {}, context = {} } = {}) => {
  const entities = getEntities(toolPlan);
  const selectedVehicle = context.selectedVehicle || {};

  return firstText(
    entities.make,
    entities.brand,
    entities.manufacturer,
    toolPlan.make,
    toolPlan.brand,
    context.anchorMake,
    selectedVehicle.make,
    selectedVehicle.brand,
  );
};

const getExplicitRequestedMake = ({ toolPlan = {} } = {}) => {
  const entities = getEntities(toolPlan);

  return firstText(
    entities.make,
    entities.brand,
    entities.manufacturer,
    toolPlan.make,
    toolPlan.brand,
  );
};

const getRequestedModel = ({
  toolPlan = {},
  context = {},
  userMessage = "",
} = {}) => {
  const entities = getEntities(toolPlan);
  const selectedVehicle = context.selectedVehicle || {};

  return firstText(
    entities.model,
    entities.modelName,
    entities.carModel,
    toolPlan.model,
    toolPlan.modelName,
    context.anchorModel,
    selectedVehicle.model,
    selectedVehicle.modelName,
    inferModelFromMessage(userMessage),
  );
};

const resolveContextSafeMake = ({
  requestedMake = "",
  requestedModel = "",
  toolPlan = {},
  context = {},
} = {}) => {
  const explicitMake = getExplicitRequestedMake({ toolPlan });
  if (explicitMake) return explicitMake;

  const selectedVehicle = context.selectedVehicle || {};
  const contextMake = cleanText(
    requestedMake ||
      context.anchorMake ||
      selectedVehicle.make ||
      selectedVehicle.brand,
  );

  if (!contextMake) return "";

  const contextModel = cleanText(
    context.anchorModel || selectedVehicle.model || "",
  );
  const selectedModelKey = normalizeModelKey(contextModel, contextMake);
  const requestedModelKey = normalizeModelKey(requestedModel, contextMake);

  // If user explicitly asks a new model and it does not match the old context model,
  // do not force old context make. This prevents "Safari" becoming "Kia Safari".
  if (
    requestedModelKey &&
    selectedModelKey &&
    requestedModelKey !== selectedModelKey
  ) {
    return cleanText(requestedMake);
  }

  return contextMake;
};

const buildMakeModelQuery = ({ make = "", model = "" } = {}) => {
  const cleanMake = titleCaseWords(make);
  const cleanModel = titleCaseWords(model);
  const modelWithoutMake = normalizeModelKey(cleanModel, cleanMake);

  const modelRegex = exactRegex(modelWithoutMake || cleanModel);
  const makeRegex = cleanMake ? exactRegex(cleanMake) : null;
  const modelSlug = slugify(modelWithoutMake || cleanModel);
  const makeSlug = cleanMake ? slugify(cleanMake) : "";

  const modelOr = [
    { model: modelRegex },
    { modelName: modelRegex },
    { model_name: modelRegex },
    { model_normalized: modelRegex },
    { modelNormalized: modelRegex },
    { model_slug: modelSlug },
  ];

  const and = [
    { $or: modelOr },
    {
      $or: [
        { scopeStatus: { $exists: false } },
        { scopeStatus: { $ne: "rejected" } },
      ],
    },
    {
      $or: [
        { normalizedImageUrl: /^https?:\/\//i },
        { cleanImageUrl: /^https?:\/\//i },
        { displayNormalizedImageUrl: /^https?:\/\//i },
        { heroImageUrl: /^https?:\/\//i },
        { heroImage: /^https?:\/\//i },
        { defaultNormalizedImageUrl: /^https?:\/\//i },
        { imageUrl: /^https?:\/\//i },
        { image_url: /^https?:\/\//i },
        { "colors.normalizedImageUrl": /^https?:\/\//i },
        { "colors.stagedImageUrl": /^https?:\/\//i },
      ],
    },
  ];

  if (makeRegex) {
    and.push({
      $or: [
        { brand: makeRegex },
        { make: makeRegex },
        { brandName: makeRegex },
        { manufacturer: makeRegex },
        { brand_slug: makeSlug },
      ],
    });
  }

  return { $and: and };
};

const rowModelMatchesRequest = ({
  row = {},
  requestedModel = "",
  requestedMake = "",
} = {}) => {
  const rowModel = firstText(
    row.model,
    row.modelName,
    row.model_name,
    row.model_normalized,
    row.modelNormalized,
  );

  if (!requestedModel || !rowModel) return true;

  const rowKey = normalizeModelKey(
    rowModel,
    requestedMake || row.brand || row.make,
  );
  const requestedKey = normalizeModelKey(
    requestedModel,
    requestedMake || row.brand || row.make,
  );

  return rowKey === requestedKey;
};

const pickImageFrame = (row = {}) =>
  normalizeFrameMeta(
    row.imageFrame ||
      row.frameMeta ||
      row.displayFrameMeta ||
      row.image_frame ||
      row.carImageFrame ||
      row.car_image_frame ||
      row.frame ||
      null,
  );

const flattenColorDocuments = (docs = []) =>
  docs.flatMap((doc = {}) => {
    const make = doc.make || doc.brand || doc.brandName || "";
    const model = doc.model || doc.modelName || doc.model_name || "";
    const topFrame = pickImageFrame(doc);
    const topImage = normalizePublicImageUrl(
      doc.displayNormalizedImageUrl ||
        doc.heroImageUrl ||
        doc.heroImage ||
        doc.defaultNormalizedImageUrl ||
        doc.displayStagedImageUrl ||
        doc.normalizedImageUrl ||
        doc.cleanImageUrl ||
        doc.imageUrl ||
        doc.image_url ||
        "",
    );

    const rows = [];
    (Array.isArray(doc.colors) ? doc.colors : []).forEach((color, index) => {
      const imageUrl = normalizePublicImageUrl(
        color.normalizedImageUrl ||
          color.stagedImageUrl ||
          color.normalizedImagePngUrl ||
          color.cleanImageUrl ||
          color.imageUrl ||
          color.sourceImageUrl ||
          "",
      );
      if (!imageUrl) return;

      rows.push({
        ...color,
        id: color.id || `${doc._id || `${make}-${model}`}-${index}`,
        _id: `${doc._id || `${make}-${model}`}:${index}`,
        make,
        brand: doc.brand || make,
        model,
        color_name: color.name || color.color_name || color.colorName || `Color ${index + 1}`,
        colorName: color.name || color.colorName || color.color_name || `Color ${index + 1}`,
        hex: color.hex || color.color_hex || color.colorHex || "",
        color_hex: color.hex || color.color_hex || color.colorHex || "",
        normalizedImageUrl: imageUrl,
        cleanImageUrl: imageUrl,
        imageUrl,
        stagedImageUrl: imageUrl,
        sourceImageUrl: color.sourceImageUrl || "",
        imageFrame: normalizeFrameMeta(firstMeaningfulFrame(color.imageFrame, color.frameMeta, topFrame)),
        updatedAt: color.updatedAt || doc.updatedAt,
        source: doc.source || COLLECTION_NAME,
      });
    });

    if (!rows.length && topImage) {
      rows.push({
        ...doc,
        make,
        brand: doc.brand || make,
        model,
        color_name: doc.defaultColorName || doc.color_name || doc.colorName || "Display",
        colorName: doc.defaultColorName || doc.colorName || doc.color_name || "Display",
        normalizedImageUrl: topImage,
        cleanImageUrl: topImage,
        imageUrl: topImage,
        sourceImageUrl: doc.displayImageUrl || doc.defaultColorImageUrl || doc.sourceImageUrl || "",
        imageFrame: topFrame,
        source: doc.source || COLLECTION_NAME,
      });
    }

    return rows.length ? rows : [doc];
  });

const normalizeColorRow = (row = {}, index = 0) => {
  const colorName = firstText(
    row.colorName,
    row.color_name,
    row.name,
    row.label,
    `Color ${index + 1}`,
  );

  const normalizedImageUrl = normalizePublicImageUrl(
    row.normalizedImageUrl ||
      row.cleanImageUrl ||
      row.stagedImageUrl ||
      row.normalized_image_url ||
      row.clean_image_url ||
      row.imageUrl ||
      row.image_url,
  );

  const sourceImageUrl = normalizePublicImageUrl(
    row.sourceImageUrl ||
      row.source_image_url ||
      row.displayImageUrl ||
      row.defaultColorImageUrl ||
      row.image_url ||
      "",
  );

  const id =
    cleanText(row.id || row._id) ||
    slugify(`${colorName}-${normalizedImageUrl}`, `color-${index + 1}`);

  return {
    id,
    colorName,
    name: colorName,
    mobileName: firstText(row.mobileName, colorName),
    desktopName: firstText(row.desktopName, colorName),
    hex: normalizeHex(row.hex || row.hexCode || row.colorHex || row.color_hex),
    deep: normalizeHex(
      row.deep ||
        row.deepHex ||
        row.darkHex ||
        row.dark_hex ||
        row.hex ||
        row.hexCode ||
        row.colorHex ||
        row.color_hex,
    ),
    imageUrl: normalizedImageUrl,
    normalizedImageUrl,
    cleanImageUrl: normalizedImageUrl,
    stagedImageUrl: normalizedImageUrl,
    sourceImageUrl,
    imageFrame: pickImageFrame(row),
    scopeStatus: row.scopeStatus || "active",
    scopeVersion: row.scopeVersion || "",
    source: row.source || COLLECTION_NAME,
    hasPopularity:
      row.votes !== undefined ||
      row.popularity !== undefined ||
      row.popularityScore !== undefined,
    votes: Number(row.votes ?? row.popularity ?? row.popularityScore ?? 0) || 0,
    description:
      row.description ||
      row.note ||
      "Color availability may vary by variant and city.",
    rawModel: row.model || row.modelName || row.model_name || "",
    brand: row.brand || row.make || "",
  };
};

const dedupeColors = (colors = []) => {
  const seen = new Set();
  const output = [];

  for (const color of colors) {
    if (!color?.normalizedImageUrl) continue;

    const key = [normalizeKey(color.colorName), color.normalizedImageUrl].join(
      "|",
    );

    if (seen.has(key)) continue;

    seen.add(key);
    output.push(color);
  }

  return output;
};

const sortColorsStable = (colors = []) =>
  [...colors].sort((a, b) => {
    const nameA = normalizeKey(a.colorName);
    const nameB = normalizeKey(b.colorName);
    return nameA.localeCompare(nameB);
  });

const findSelectedColor = ({ colors = [], context = {} } = {}) => {
  const selected =
    context.selectedColor || context.selectedVehicle?.selectedColor || {};
  const selectedName = normalizeKey(
    selected.colorName ||
      selected.name ||
      selected.desktopName ||
      selected.mobileName ||
      "",
  );

  if (selectedName) {
    const match = colors.find(
      (item) => normalizeKey(item.colorName) === selectedName,
    );
    if (match) return match;
  }

  return colors[0] || null;
};

const buildVehicle = ({
  make = "",
  model = "",
  city = DEFAULT_CITY,
  selectedColor,
  colorCount = 0,
} = {}) => {
  const cleanMake = cleanText(make);
  const cleanModel = cleanText(model);
  const displayName =
    [cleanMake, cleanModel].filter(Boolean).join(" ") ||
    cleanModel ||
    "Vehicle";

  return {
    id: slugify(displayName, "vehicle"),
    make: cleanMake,
    brand: cleanMake,
    model: cleanModel,
    displayName,
    city: displayCity(city),
    citySlug: slugify(city || DEFAULT_CITY, DEFAULT_CITY),
    imageUrl: selectedColor?.normalizedImageUrl || "",
    normalizedImageUrl: selectedColor?.normalizedImageUrl || "",
    imageFrame: selectedColor?.imageFrame || null,
    selectedColor: selectedColor || null,
    colorName: selectedColor?.colorName || "",
    colorCount,
  };
};

const buildActions = ({ vehicle } = {}) => [
  {
    id: "open-pricelist",
    label: "Open pricelist",
    title: "Open pricelist",
    intent: "vehicle_pricelist",
    canvasType: "pricelist_canvas",
    query: `Show ${vehicle?.displayName || vehicle?.model || "this car"} pricelist`,
    vehicle,
    contextPatch: {
      selectedVehicle: vehicle,
      anchorMake: vehicle?.make || vehicle?.brand || "",
      anchorModel: vehicle?.model || "",
      anchorCity: vehicle?.citySlug || DEFAULT_CITY,
    },
  },
  {
    id: "calculate-emi",
    label: "Calculate EMI",
    title: "Calculate EMI",
    intent: "vehicle_emi",
    canvasType: "emi_calculator_canvas",
    query: `Calculate EMI for ${vehicle?.displayName || vehicle?.model || "this car"}`,
    vehicle,
    contextPatch: {
      selectedVehicle: vehicle,
      anchorMake: vehicle?.make || vehicle?.brand || "",
      anchorModel: vehicle?.model || "",
      anchorCity: vehicle?.citySlug || DEFAULT_CITY,
    },
  },
  {
    id: "get-quotation",
    label: "Get quotation",
    title: "Get quotation",
    intent: "aci_lead_capture",
    canvasType: "aci_quotation_canvas",
    query: `Get quotation for ${vehicle?.displayName || vehicle?.model || "this car"}`,
    vehicle,
    contextPatch: {
      selectedVehicle: vehicle,
      anchorMake: vehicle?.make || vehicle?.brand || "",
      anchorModel: vehicle?.model || "",
      anchorCity: vehicle?.citySlug || DEFAULT_CITY,
    },
  },
];

const buildLeadingQuestions = ({ vehicle } = {}) => [
  {
    id: "best-color",
    label: "Which color looks best?",
    title: "Which color looks best?",
    icon: "palette",
    query: `Which color looks best for ${vehicle?.displayName || vehicle?.model || "this car"}?`,
    intent: "vehicle_colors",
    canvasType: CANVAS_TYPE,
    vehicle,
  },
  {
    id: "open-pricelist",
    label: "Open pricelist",
    title: "Open pricelist",
    icon: "receipt",
    query: `Show ${vehicle?.displayName || vehicle?.model || "this car"} pricelist`,
    intent: "vehicle_pricelist",
    canvasType: "pricelist_canvas",
    vehicle,
  },
];

const buildUnavailableResponse = ({
  requestedMake,
  requestedModel,
  requestedCity,
  queryUsed,
} = {}) => {
  const displayName =
    [requestedMake, requestedModel].filter(Boolean).join(" ") ||
    requestedModel ||
    "this model";
  const vehicle = buildVehicle({
    make: requestedMake,
    model: requestedModel,
    city: requestedCity,
    selectedColor: null,
    colorCount: 0,
  });

  const widget = {
    type: "vehicle_colors",
    tool: TOOL_NAME,
    intent: INTENT,
    canvasType: CANVAS_TYPE,
    title: `${displayName} colors`,
    vehicle,
    colors: [],
    records: [],
    rows: [],
    selectedColor: null,
    isUnavailable: true,
    emptyReason: "No active color images found for this exact model.",
  };

  return {
    tool: TOOL_NAME,
    intent: INTENT,
    canvasType: CANVAS_TYPE,
    title: `${displayName} colors`,
    answer: `I could not find active color images for ${displayName}.`,
    vehicle,
    colors: [],
    records: [],
    rows: [],
    selectedColor: null,
    widget,
    widgets: [widget],
    actions: [],
    leadingQuestions: [],
    data: {
      vehicle,
      colors: [],
      selectedColor: null,
      queryUsed,
    },
    contextPatch: {
      selectedVehicle: vehicle,
      anchorMake: requestedMake || "",
      anchorModel: requestedModel || "",
      anchorCity: requestedCity || DEFAULT_CITY,
      selectedColor: null,
    },
    sourceTransparency: {
      modulesChecked: [COLLECTION_NAME],
      recordCount: 0,
    },
    meta: {
      collection: COLLECTION_NAME,
      queryUsed,
      exactModelOnly: true,
    },
  };
};

export const runVehicleColorsTool = async (args = {}) => {
  const { toolPlan = {}, context = {}, userMessage = "" } = args;

  const requestedModel = getRequestedModel({ toolPlan, context, userMessage });
  const rawRequestedMake = getRequestedMake({ toolPlan, context });
  const requestedMake = resolveContextSafeMake({
    requestedMake: rawRequestedMake,
    requestedModel,
    toolPlan,
    context,
  });
  const requestedCity =
    cleanText(
      getEntities(toolPlan).city ||
        toolPlan.city ||
        context.anchorCity ||
        context.selectedVehicle?.citySlug ||
        context.selectedVehicle?.city,
    ) || DEFAULT_CITY;

  if (!requestedModel) {
    return buildUnavailableResponse({
      requestedMake,
      requestedModel: "",
      requestedCity,
      queryUsed: null,
    });
  }

  const collection = mongoose.connection.db.collection(COLLECTION_NAME);

  const queries = [
    {
      query: buildMakeModelQuery({
        make: requestedMake,
        model: requestedModel,
      }),
      make: requestedMake,
    },
    requestedMake
      ? {
          query: buildMakeModelQuery({
            make: "",
            model: requestedModel,
          }),
          make: "",
        }
      : null,
  ].filter(Boolean);

  let docs = [];
  let queryUsed = null;
  let queryMakeUsed = "";

  for (const { query, make: queryMake } of queries) {
    docs = await collection
      .find(query)
      .sort({ activeColorCount: -1, updatedAt: -1, color_name: 1, colorName: 1, name: 1 })
      .limit(toolPlan.limit || toolPlan.input?.limit || 80)
      .toArray();

    docs = flattenColorDocuments(docs).filter((row) =>
      rowModelMatchesRequest({
        row,
        requestedModel,
        requestedMake: queryMake,
      }),
    );

    if (docs.length) {
      queryUsed = query;
      queryMakeUsed = queryMake;
      break;
    }
  }

  let colors = sortColorsStable(dedupeColors(docs.map(normalizeColorRow)));

  if (!colors.length) {
    return buildUnavailableResponse({
      requestedMake,
      requestedModel,
      requestedCity,
      queryUsed,
    });
  }

  const selectedColor = findSelectedColor({ colors, context });

  colors = colors.map((color) => ({
    ...color,
    isSelected: selectedColor?.id === color.id,
    selected: selectedColor?.id === color.id,
  }));

  const resolvedMake = cleanText(
    queryMakeUsed || docs[0]?.brand || docs[0]?.make || requestedMake,
  );

  const vehicle = buildVehicle({
    make: resolvedMake,
    model: normalizeModelKey(requestedModel, resolvedMake),
    city: requestedCity,
    selectedColor,
    colorCount: colors.length,
  });

  vehicle.visualGallery = colors.map((color) => ({
    id: color.id,
    model: vehicle.model,
    rawModel: docs[0]?.model || vehicle.model,
    make: vehicle.make,
    brand: vehicle.brand,
    colorName: color.colorName,
    name: color.colorName,
    hex: color.hex,
    imageUrl: color.normalizedImageUrl,
    normalizedImageUrl: color.normalizedImageUrl,
    imageFrame: color.imageFrame,
  }));

  const title = `${vehicle.displayName} colors`;
  const answer = `I found ${colors.length} colors for ${vehicle.displayName}.`;

  const actions = buildActions({ vehicle });
  const leadingQuestions = buildLeadingQuestions({ vehicle });

  const widget = {
    type: "vehicle_colors",
    tool: TOOL_NAME,
    intent: INTENT,
    canvasType: CANVAS_TYPE,
    title,
    subtitle: `${colors.length} exterior colors`,
    answer,
    vehicle,
    colors,
    rows: colors,
    records: colors,
    items: colors,
    selectedColor,
    visualGallery: vehicle.visualGallery,
    actions,
    leadingQuestions,
  };

  return {
    tool: TOOL_NAME,
    intent: INTENT,
    canvasType: CANVAS_TYPE,
    title,
    answer,
    vehicle,
    colors,
    rows: colors,
    records: colors,
    items: colors,
    selectedColor,
    visualGallery: vehicle.visualGallery,
    widget,
    widgets: [widget],
    actions,
    leadingQuestions,
    data: {
      vehicle,
      colors,
      selectedColor,
      visualGallery: vehicle.visualGallery,
      queryUsed,
    },
    contextPatch: {
      selectedVehicle: {
        ...vehicle,
        selectedColor,
        imageUrl: selectedColor?.normalizedImageUrl || vehicle.imageUrl || "",
        normalizedImageUrl:
          selectedColor?.normalizedImageUrl || vehicle.normalizedImageUrl || "",
        imageFrame: selectedColor?.imageFrame || vehicle.imageFrame || null,
      },
      anchorMake: vehicle.make || vehicle.brand || "",
      anchorModel: vehicle.model || "",
      anchorCity: vehicle.citySlug || DEFAULT_CITY,
      selectedColor,
    },
    sourceTransparency: {
      modulesChecked: [COLLECTION_NAME],
      recordCount: colors.length,
    },
    meta: {
      collection: COLLECTION_NAME,
      exactModelOnly: true,
      stableOrder: "color_name_asc",
      randomSampling: false,
      cssTintingRequired: false,
      queryUsed,
    },
  };
};

export default runVehicleColorsTool;
