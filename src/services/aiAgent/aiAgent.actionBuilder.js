import Vehicle from "../../models/Vehicle.js";
import VehicleFeature from "../../models/VehicleFeature.js";
import { normalizeText } from "./aiAgent.normalizers.js";
import {
  getNewCarQuestionConfig,
  mapIntentAlias,
} from "./aiAgent.newCarQuestionMap.js";
import {
  detectBuyingSignals,
  detectUserStage,
  generateSalesNudges,
} from "./aiAgent.salesBrain.js";
import { generateClosingActions } from "./aiAgent.closingEngine.js";
import { updateUserProfile } from "./aiAgent.userProfile.js";
import { getSuggestionScore } from "./aiAgent.learningEngine.js";

const asArray = (value) =>
  Array.isArray(value) ? value : value ? [value] : [];
const TEN_MINUTES_MS = 10 * 60 * 1000;

const firstMeaningful = (...values) =>
  values.find((value) => value !== undefined && value !== null && value !== "");

const toWords = (value = "") =>
  normalizeText(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const parsePrice = (...values) => {
  for (const value of values) {
    if (value === undefined || value === null || value === "") continue;
    const number = Number(
      String(value)
        .replace(/[^0-9.]/g, "")
        .trim(),
    );
    if (Number.isFinite(number) && number > 0) return number;
  }
  return 0;
};

const formatBudgetForQuery = (value, fallbackLakh = 20) => {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num <= 0) return `${fallbackLakh} lakh`;
  if (num > 1000) {
    const lakh = num / 100000;
    return `${Number.isInteger(lakh) ? lakh : lakh.toFixed(1)} lakh`;
  }
  return `${num} lakh`;
};

const unique = (items = []) => [...new Set(items.filter(Boolean))];

const isRecent = (entry) => {
  if (!entry) return false;
  if (typeof entry === "boolean") return entry;
  if (typeof entry !== "object") return false;
  return (
    Boolean(entry.value) && Date.now() - Number(entry.ts || 0) < TEN_MINUTES_MS
  );
};

const withHistoryEntry = (previous, shouldSet, now) => {
  if (shouldSet) return { value: true, ts: now };
  if (!previous) return { value: false, ts: now };
  if (typeof previous === "boolean") return { value: previous, ts: now };
  return {
    value: Boolean(previous.value),
    ts: Number(previous.ts || now),
  };
};

const slug = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const rowsFromWidget = (widget = {}) =>
  asArray(
    widget.rows ||
      widget.records ||
      widget.modelCards ||
      widget.groupedByModel ||
      widget.variants ||
      widget.options ||
      widget.colors ||
      widget.data?.rows ||
      widget.data?.records ||
      widget.data?.modelCards ||
      widget.data?.groupedByModel ||
      widget.data?.variants ||
      widget.data?.options ||
      widget.data?.colors,
  ).filter((item) => item && typeof item === "object");

const captureModelsFromRows = (rows = []) =>
  unique(
    rows.map((row) => displayModel(row)).map((item) => normalizeText(item)),
  );

const displayModel = (row = {}) =>
  firstMeaningful(
    row.model,
    row.modelName,
    row.model_normalized,
    row.title,
    row.name,
  );

const displayVariant = (row = {}) =>
  firstMeaningful(
    row.variant,
    row.variantName,
    row.variant_name,
    row.variant_normalized,
    row.trim,
  );

const displayBrand = (row = {}) => firstMeaningful(row.brand, row.make);

const isStructuredNewCarIntent = (intent = "") =>
  Boolean(getNewCarQuestionConfig(mapIntentAlias(intent)));

const pickAnchorModel = ({
  parsed,
  primaryWidget,
  selectedModels = [],
  selectedEntity,
}) =>
  normalizeText(
    firstMeaningful(
      parsed?.entities?.model,
      parsed?.entities?.models?.[0],
      primaryWidget?.model,
      primaryWidget?.data?.model,
      selectedModels[0],
      selectedEntity?.model,
    ),
  );

const pickAnchorVariant = ({
  parsed,
  primaryWidget,
  selectedVariants = [],
  selectedEntity,
}) =>
  normalizeText(
    firstMeaningful(
      parsed?.entities?.variant,
      primaryWidget?.variantQuery,
      primaryWidget?.variant,
      primaryWidget?.data?.variant,
      selectedVariants[0],
      selectedEntity?.variant,
    ),
  );

export const resolveConversationMode = (context = {}) => {
  const { intent, history = {} } = context;

  if (
    ["vehicle_budget_search", "vehicle_recommendation_discovery"].includes(
      intent,
    )
  ) {
    return "explore";
  }
  if (["vehicle_comparison"].includes(intent)) return "compare";
  if (["vehicle_emi_calculator", "aci_new_car_quotation"].includes(intent))
    return "buy";

  if (isRecent(history.compared)) return "compare";
  if (isRecent(history.viewedPrice)) return "consider";

  return "explore";
};

export const INTENT_CHAINS = {
  vehicle_pricelist: [
    "vehicle_colors",
    "vehicle_model_features_explorer",
    "vehicle_emi_calculator",
    "vehicle_comparison",
    "aci_new_car_quotation",
  ],
  vehicle_comparison: [
    "vehicle_feature_answer",
    "vehicle_emi_calculator",
    "aci_new_car_quotation",
  ],
  vehicle_emi_calculator: [
    "vehicle_price_breakup",
    "new_car_loan_enquiry",
    "aci_new_car_quotation",
  ],
};

export const inferVehicleSegment = (modelResult = {}) => {
  const raw = toWords(
    firstMeaningful(
      modelResult.bodyType,
      modelResult.body_type_bucket,
      modelResult.segment,
      modelResult.category,
    ),
  );

  if (/\bcompact suv\b/.test(raw)) return "compact suv";
  if (/\bsuv\b/.test(raw)) return "suv";
  if (/\bsedan\b/.test(raw)) return "sedan";
  if (/\bhatchback\b/.test(raw)) return "hatchback";
  if (/\bmpv|muv|7 seater\b/.test(raw)) return "mpv";
  return raw || "";
};

export const findRelevantRivals = async ({
  model,
  bodyType,
  priceMin,
  priceMax,
  brand,
  city,
}) => {
  const MAX_QUERY_MS = 3000;
  const modelNeedle = toWords(model);
  if (!modelNeedle) return [];

  const bodyCategory = (value = "") => {
    const text = toWords(value);
    if (!text) return "unknown";
    if (/\bmpv|muv|van\b/.test(text)) return "mpv";
    if (/\bhatchback\b/.test(text)) return "hatchback";
    if (/\bsedan\b/.test(text)) return "sedan";
    if (/\bcompact suv|mid suv|suv|crossover\b/.test(text)) return "suv";
    return "unknown";
  };

  const isRowInactive = (row = {}) =>
    Boolean(
      row.is_discontinued ||
        /discontinued|inactive/i.test(
          `${row.status || ""} ${row.model_status || ""}`,
        ),
    );

  const cityPattern = (inputCity = "new delhi") => {
    const normalized = toWords(inputCity || "new delhi") || "new delhi";
    return new RegExp(`^${normalized.replace(/\s+/g, "[- ]?")}$`, "i");
  };

  const extractEffectivePrice = (row = {}) =>
    parsePrice(
      row.on_road_price,
      row.onRoadPrice,
      row.total_on_road_with_accessories,
      row.on_road_price_cardekho,
      row.orp_without_accessories,
      row.ex_showroom,
      row.exShowroom,
    );

  const anchorCityRegex = cityPattern(city || "new delhi");

  const anchorModelRegex = new RegExp(modelNeedle.replace(/\s+/g, ".*"), "i");
  const anchorModelNormalized = normalizeText(model).toLowerCase().trim();
  const anchorModelSlug = anchorModelNormalized.replace(/\s+/g, "-");

  const anchorRows = await Vehicle.find({
    city: { $regex: anchorCityRegex },
    $or: [
      { model_normalized: anchorModelNormalized },
      { model_slug: anchorModelSlug },
      { model: { $regex: anchorModelRegex } },
    ],
  })
    .select({
      brand: 1,
      make: 1,
      model: 1,
      ex_showroom: 1,
      exShowroom: 1,
      on_road_price: 1,
      onRoadPrice: 1,
      total_on_road_with_accessories: 1,
      on_road_price_cardekho: 1,
      orp_without_accessories: 1,
      status: 1,
      model_status: 1,
      is_discontinued: 1,
      city: 1,
    })
    .limit(220)
    .maxTimeMS(MAX_QUERY_MS)
    .lean();

  const activeAnchorRows = anchorRows.filter((row) => !isRowInactive(row));
  const anchorPrices = activeAnchorRows
    .map(extractEffectivePrice)
    .filter((value) => Number.isFinite(value) && value > 0);

  const anchorMin = Number(priceMin) || (anchorPrices.length ? Math.min(...anchorPrices) : 0);
  const anchorMax = Number(priceMax) || (anchorPrices.length ? Math.max(...anchorPrices) : 0);
  const anchorMid = anchorMin && anchorMax ? (anchorMin + anchorMax) / 2 : anchorMin || anchorMax || 0;

  let resolvedBodyType = toWords(bodyType);
  if (!resolvedBodyType) {
    const anchorBodyDoc = await VehicleFeature.findOne({
      $or: [
        { model_normalized: anchorModelNormalized },
        { model_slug: anchorModelSlug },
        { model: { $regex: anchorModelRegex } },
      ],
    })
      .select({ body_type_bucket: 1, bodyType: 1, segment: 1, category: 1 })
      .maxTimeMS(MAX_QUERY_MS)
      .lean();
    resolvedBodyType = toWords(
      firstMeaningful(
        anchorBodyDoc?.body_type_bucket,
        anchorBodyDoc?.bodyType,
        anchorBodyDoc?.segment,
        anchorBodyDoc?.category,
      ) || "",
    );
  }
  const anchorCategory = bodyCategory(resolvedBodyType);

  const candidateRowsRaw = await Vehicle.find({
    city: { $regex: anchorCityRegex },
  })
    .select({
      brand: 1,
      make: 1,
      model: 1,
      ex_showroom: 1,
      exShowroom: 1,
      on_road_price: 1,
      onRoadPrice: 1,
      total_on_road_with_accessories: 1,
      on_road_price_cardekho: 1,
      orp_without_accessories: 1,
      status: 1,
      model_status: 1,
      is_discontinued: 1,
      city: 1,
    })
    .limit(500)
    .maxTimeMS(MAX_QUERY_MS)
    .lean();

  const deduped = new Map();
  for (const row of candidateRowsRaw) {
    if (isRowInactive(row)) continue;
    const candidateModel = normalizeText(row.model);
    const candidateModelNeedle = toWords(candidateModel);
    if (!candidateModelNeedle || candidateModelNeedle === modelNeedle) continue;
    if (
      candidateModelNeedle.includes(modelNeedle) ||
      modelNeedle.includes(candidateModelNeedle)
    ) {
      continue;
    }

    const candidateBrand = normalizeText(firstMeaningful(row.brand, row.make));
    const key = `${toWords(candidateBrand)}|${candidateModelNeedle}`;
    const price = extractEffectivePrice(row);

    if (!deduped.has(key)) {
      deduped.set(key, { brand: candidateBrand, model: candidateModel, price });
      continue;
    }

    const prev = deduped.get(key);
    if ((!prev.price && price) || (prev.price && price && price < prev.price)) {
      deduped.set(key, { brand: candidateBrand, model: candidateModel, price });
    }
  }

  const candidates = [...deduped.values()];
  if (!candidates.length) return [];

  const candidateNames = candidates.map((item) => item.model).slice(0, 60);
  const candidateNormalizedNames = candidateNames
    .map((name) => toWords(name))
    .filter(Boolean);
  const candidateModelSlugs = candidateNormalizedNames.map((name) =>
    name.replace(/\s+/g, "-"),
  );
  const bodyRows = candidateNames.length
    ? await VehicleFeature.find({
        $or: [
          { model_normalized: { $in: candidateNormalizedNames } },
          { model_slug: { $in: candidateModelSlugs } },
          ...candidateNames.slice(0, 60).map((name) => ({
            model: {
              $regex: new RegExp(toWords(name).replace(/\s+/g, ".*"), "i"),
            },
          })),
        ],
      })
        .select({ model: 1, body_type_bucket: 1, bodyType: 1, segment: 1, category: 1 })
        .limit(300)
        .maxTimeMS(MAX_QUERY_MS)
        .lean()
    : [];

  const bodyByModel = new Map();
  for (const row of bodyRows) {
    const modelKey = toWords(row.model);
    const bodyValue = toWords(
      firstMeaningful(
        row.body_type_bucket,
        row.bodyType,
        row.segment,
        row.category,
      ) || "",
    );
    if (!modelKey || !bodyValue) continue;
    if (!bodyByModel.has(modelKey)) bodyByModel.set(modelKey, bodyValue);
  }

  const sedanBoost = /\bcity|slavia|virtus|ciaz|verna\b/i;
  const suvBoost =
    /\bcreta|seltos|elevate|grand vitara|hyryder|taigun|kushaq|venue|sonet|brezza|nexon|3xo|xuv\b/i;
  const normalizedAnchorBrand = toWords(brand);

  const scored = candidates
    .map((item) => {
      const modelKey = toWords(item.model);
      const directBody = bodyByModel.get(modelKey) || "";
      const fuzzyBody =
        directBody ||
        [...bodyByModel.entries()].find(([key]) =>
          key.includes(modelKey) || modelKey.includes(key),
        )?.[1] ||
        "";
      const candidateBodyCategory = bodyCategory(fuzzyBody);

      let score = 0;
      let compatible = true;

      if (anchorCategory === "sedan") {
        if (candidateBodyCategory === "sedan") score += 50;
        else if (candidateBodyCategory === "unknown") score -= 40;
        else compatible = false;
      } else if (anchorCategory === "suv") {
        if (candidateBodyCategory === "suv") score += 50;
        else if (candidateBodyCategory === "unknown") score -= 40;
        else compatible = false;
      } else if (anchorCategory === "hatchback") {
        if (candidateBodyCategory === "hatchback") score += 50;
        else if (candidateBodyCategory === "unknown") score -= 40;
        else compatible = false;
      } else if (anchorCategory === "mpv") {
        if (candidateBodyCategory === "mpv") score += 50;
        else if (candidateBodyCategory === "unknown") score -= 40;
        else compatible = false;
      }

      // Hard guard: never map MPV/MUV as rivals for sedan/SUV/hatchback anchors.
      if (
        ["sedan", "suv", "hatchback"].includes(anchorCategory) &&
        candidateBodyCategory === "mpv"
      ) {
        compatible = false;
      }

      if (!compatible) return null;

      if (anchorMin && anchorMax && item.price) {
        const minFactor = anchorCategory === "sedan" ? 0.8 : 0.75;
        const maxFactor = anchorCategory === "sedan" ? 1.25 : 1.35;
        if (item.price < anchorMin * minFactor || item.price > anchorMax * maxFactor) {
          return null;
        }
        if (anchorMid) {
          const diffRatio = Math.abs(item.price - anchorMid) / Math.max(anchorMid, 1);
          score += Math.max(0, 30 - diffRatio * 30);
        }
      } else if (!item.price) {
        score -= 25;
      }

      if (!normalizedAnchorBrand || toWords(item.brand) !== normalizedAnchorBrand) {
        score += 3;
      }

      if (anchorCategory === "sedan" && sedanBoost.test(item.model)) score += 8;
      if (anchorCategory === "suv" && suvBoost.test(item.model)) score += 8;

      return {
        ...item,
        score,
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      if (anchorMid && a.price && b.price) {
        return Math.abs(a.price - anchorMid) - Math.abs(b.price - anchorMid);
      }
      return a.model.localeCompare(b.model);
    });

  return unique(scored.map((item) => normalizeText(item.model))).slice(0, 3);
};

export const buildContextSnapshot = ({
  parsed,
  result,
  primaryWidget,
  selectedEntity,
  filters,
  context = {},
}) => {
  const canonicalIntent = mapIntentAlias(parsed?.intent || "");
  const questionConfig = getNewCarQuestionConfig(canonicalIntent);
  const rows = rowsFromWidget(primaryWidget);
  const resultModels = captureModelsFromRows(rows);
  const selectedModels = unique(
    [
      parsed?.entities?.model,
      ...(parsed?.entities?.models || []),
      primaryWidget?.model,
    ].map((item) => normalizeText(item)),
  );

  const selectedVariants = unique(
    [
      parsed?.entities?.variant,
      ...(parsed?.entities?.variants || []),
      primaryWidget?.variant,
    ].map((item) => normalizeText(item)),
  );

  const priceValues = rows
    .map((row) =>
      parsePrice(
        row.exShowroomPrice,
        row.ex_showroom,
        row.exShowroom,
        row.onRoadPrice,
        row.onRoad,
        row.price,
      ),
    )
    .filter(Boolean);

  const anchorModel = pickAnchorModel({
    parsed,
    primaryWidget,
    selectedModels,
    selectedEntity,
  });

  const anchorVariant = pickAnchorVariant({
    parsed,
    primaryWidget,
    selectedVariants,
    selectedEntity,
  });

  const sourceCollections = unique([
    ...(result?.collectionsUsed || []),
    ...(result?.sourceCollections || []),
    ...(asArray(result?.modulesChecked).map((item) => item.module) || []),
  ]);

  const priorHistory = context?.history || {};
  const now = Date.now();
  const history = {
    ...priorHistory,
    viewedPrice: withHistoryEntry(
      priorHistory.viewedPrice,
      [
        "vehicle_pricelist",
        "vehicle_city_price",
        "vehicle_variant_price",
      ].includes(canonicalIntent),
      now,
    ),
    viewedFeatures: withHistoryEntry(
      priorHistory.viewedFeatures,
      [
        "vehicle_model_features_explorer",
        "vehicle_feature_discovery",
        "vehicle_feature_answer",
      ].includes(canonicalIntent),
      now,
    ),
    compared: withHistoryEntry(
      priorHistory.compared,
      [
        "vehicle_comparison",
        "vehicle_model_comparison",
        "vehicle_variant_comparison",
      ].includes(canonicalIntent) || selectedModels.length > 1,
      now,
    ),
    checkedEmi: withHistoryEntry(
      priorHistory.checkedEmi,
      ["vehicle_emi_calculator", "vehicle_emi_options"].includes(
        canonicalIntent,
      ),
      now,
    ),
    viewedOffers: withHistoryEntry(
      priorHistory.viewedOffers,
      ["vehicle_offers", "vehicle_offer_lookup"].includes(canonicalIntent),
      now,
    ),
    requestedQuotation: withHistoryEntry(
      priorHistory.requestedQuotation,
      canonicalIntent === "aci_new_car_quotation",
      now,
    ),
    requestedTestDrive: withHistoryEntry(
      priorHistory.requestedTestDrive,
      canonicalIntent === "vehicle_test_drive_request",
      now,
    ),
  };

  const userProfile = updateUserProfile(context, parsed);

  const snapshot = {
    domain: "new_car",
    intent: canonicalIntent,
    canvasType: primaryWidget?.canvasType || questionConfig?.canvasType || null,
    inlineType: primaryWidget?.inlineType || questionConfig?.inlineType || null,
    make: normalizeText(
      firstMeaningful(
        parsed?.entities?.make,
        primaryWidget?.make,
        primaryWidget?.data?.make,
        displayBrand(rows[0]),
      ),
    ),
    brand: normalizeText(
      firstMeaningful(
        parsed?.entities?.make,
        primaryWidget?.brand,
        primaryWidget?.data?.brand,
        displayBrand(rows[0]),
      ),
    ),
    model: anchorModel,
    variant: anchorVariant,
    city: normalizeText(
      firstMeaningful(
        parsed?.entities?.city,
        primaryWidget?.city,
        primaryWidget?.data?.city,
      ),
    ),
    requestedCity: normalizeText(
      firstMeaningful(
        primaryWidget?.requestedCity,
        primaryWidget?.data?.requestedCity,
        parsed?.entities?.city,
      ),
    ),
    cityFallbackApplied: Boolean(
      firstMeaningful(
        primaryWidget?.cityFallbackUsed,
        primaryWidget?.data?.cityFallbackUsed,
      ),
    ),
    resultModels,
    bodyType: normalizeText(
      firstMeaningful(
        parsed?.entities?.bodyType,
        primaryWidget?.bodyType,
        primaryWidget?.data?.bodyType,
        rows[0]?.bodyType,
      ),
    ),
    fuel: normalizeText(
      firstMeaningful(
        parsed?.entities?.fuelType,
        primaryWidget?.fuel,
        primaryWidget?.data?.fuel,
        rows[0]?.fuelType,
        rows[0]?.fuel,
      ),
    ),
    transmission: normalizeText(
      firstMeaningful(
        parsed?.entities?.transmission,
        primaryWidget?.transmission,
        primaryWidget?.data?.transmission,
        rows[0]?.transmission,
      ),
    ),
    budgetMin: parsed?.entities?.budgetMin || null,
    budgetMax: parsed?.entities?.budgetMax || null,
    priceMin: priceValues.length ? Math.min(...priceValues) : null,
    priceMax: priceValues.length ? Math.max(...priceValues) : null,
    selectedModels,
    selectedVariants,
    anchorModel,
    anchorVariant,
    activeOnly: true,
    resultCount:
      firstMeaningful(
        primaryWidget?.summary?.total,
        primaryWidget?.data?.total,
        primaryWidget?.total,
      ) || rows.length,
    primaryResultIds: unique(
      rows.map((row) => String(firstMeaningful(row.id, row._id) || "")),
    ).slice(0, 12),
    sourceCollections,
    filters: filters || {},
    history,
    profile: userProfile,
    userId: String(
      firstMeaningful(
        context?.userId,
        context?.profile?.userId,
        context?.sessionUserId,
        context?.customerId,
      ) || "",
    ),
  };

  snapshot.mode = resolveConversationMode(snapshot);
  snapshot.stage = detectUserStage(snapshot);
  snapshot.buyingSignals = detectBuyingSignals(snapshot);
  snapshot.salesNudges = generateSalesNudges(snapshot);
  snapshot.closingActions = generateClosingActions(snapshot);
  snapshot.missingContext = {
    variant: !snapshot.variant,
    city: !snapshot.city,
    budget: !snapshot.budgetMax,
  };

  return snapshot;
};

const createSuggestion = (partial, contextSnapshot) => {
  const normalized = normalizeConversationSuggestion(partial, contextSnapshot);
  return normalized;
};

const suggestionForModel = (contextSnapshot, query, partial = {}) => ({
  ...partial,
  query,
  entities: {
    model: contextSnapshot.anchorModel,
    city: contextSnapshot.city || contextSnapshot.requestedCity,
    ...(contextSnapshot.anchorVariant
      ? { variant: contextSnapshot.anchorVariant }
      : {}),
    ...(partial.entities || {}),
  },
});

const addCompareSuggestion = async (suggestions, contextSnapshot) => {
  const anchor = contextSnapshot.anchorModel;
  if (!anchor) return;
  let rivals = [];
  try {
    rivals =
      (await Promise.race([
        findRelevantRivals({
          model: anchor,
          bodyType: contextSnapshot.bodyType,
          priceMin: contextSnapshot.priceMin || contextSnapshot.budgetMin || 0,
          priceMax: contextSnapshot.priceMax || contextSnapshot.budgetMax || 0,
          brand: contextSnapshot.brand,
          city:
            contextSnapshot.city || contextSnapshot.requestedCity || "new delhi",
        }),
        new Promise((resolve) => setTimeout(() => resolve([]), 1800)),
      ])) || [];
  } catch (error) {
    void error;
    rivals = [];
  }

  if (!rivals.length) {
    suggestions.push(
      createSuggestion(
        {
          title: `Which cars should I compare ${anchor} with?`,
          subtitle:
            "Tell me your preferred rivals and I’ll compare them side-by-side.",
          kind: "clarification",
          type: "ask",
          intent: "vehicle_comparison",
          query: `Which cars do you want to compare ${anchor} with?`,
          entities: { model: anchor, city: contextSnapshot.city },
          priority: 84,
          icon: "scale",
          tone: "neutral",
        },
        contextSnapshot,
      ),
    );
    return;
  }

  const compareModels = unique([
    anchor,
    ...(contextSnapshot.selectedModels || []),
    ...rivals,
  ]).slice(0, 3);
  if (compareModels.length < 2) return;
  const compareQuery =
    compareModels.length >= 3
      ? `Compare ${compareModels[0]}, ${compareModels[1]} and ${compareModels[2]}`
      : `Compare ${compareModels[0]} and ${compareModels[1]}`;

  suggestions.push(
    createSuggestion(
      {
        title: `Compare ${anchor} with similar ${contextSnapshot.bodyType?.includes("suv") ? "SUVs" : "rivals"}`,
        subtitle: compareModels.slice(1).join(" • "),
        kind: "action",
        type: "ask",
        intent: "vehicle_comparison",
        query: compareQuery,
        entities: {
          model: anchor,
          models: compareModels,
          city: contextSnapshot.city || contextSnapshot.requestedCity,
        },
        displayMode: "canvas",
        canvasType: "comparison_canvas",
        priority: 80,
        icon: "scale",
        tone: "primary",
      },
      contextSnapshot,
    ),
  );
};

const suggestionKindFromType = (type = "") => {
  if (type === "lead") return "lead";
  if (type === "navigate") return "navigation";
  if (type === "select") return "clarification";
  return "action";
};


const stableSuggestionId = (suggestion = {}, intent = "") => {
  const base = [
    mapIntentAlias(intent || suggestion.intent || ""),
    suggestion.type || "ask",
    suggestion.leadType || suggestion.contextPatch?.leadType || "",
    suggestion.canvasType || "",
    suggestion.inlineType || "",
    suggestion.query ||
      suggestion.message ||
      suggestion.title ||
      suggestion.label ||
      "",
    suggestion.entities?.model || "",
    suggestion.entities?.variant || "",
    asArray(suggestion.entities?.models).join("-"),
  ]
    .map((item) => slug(item))
    .filter(Boolean)
    .join("-");

  return base || `suggestion-${slug(intent || "next")}`;
};

export const normalizeConversationSuggestion = (
  suggestion,
  contextSnapshot,
) => {
  const type = suggestion.type || "ask";
  const intent = mapIntentAlias(
    suggestion.intent || contextSnapshot.intent || "",
  );
  const title = normalizeText(
    firstMeaningful(
      suggestion.title,
      suggestion.label,
      suggestion.query,
      "Next",
    ),
  );
  const query = normalizeText(
    firstMeaningful(suggestion.query, suggestion.message, title),
  );
  const entities = {
    ...(suggestion.entities || {}),
  };
  const contextPatch = {
    ...(suggestion.contextPatch || {}),
    entities: {
      ...(suggestion.contextPatch?.entities || {}),
      ...(suggestion.entities || {}),
    },
  };
  return {
    id: suggestion.id || stableSuggestionId(suggestion, intent),
    title,
    subtitle: normalizeText(suggestion.subtitle || ""),
    kind: suggestion.kind || suggestionKindFromType(type),
    type,
    intent,
    query,
    entities,
    contextPatch,
    displayMode: suggestion.displayMode || "",
    canvasType: suggestion.canvasType || null,
    inlineType: suggestion.inlineType || null,
    priority: Number(suggestion.priority || 50),
    icon: suggestion.icon || "sparkles",
    tone: suggestion.tone || "neutral",
    leadType: suggestion.leadType || suggestion.contextPatch?.leadType || "",
    route: suggestion.route || "",
  };
};

export const validateConversationSuggestions = (
  suggestions,
  contextSnapshot,
) => {
  const anchorModel = toWords(contextSnapshot.anchorModel);
  const anchorVariant = toWords(contextSnapshot.anchorVariant);
  const variantScopedIntents = new Set([
    "vehicle_variant_price",
    "vehicle_feature_answer",
    "aci_new_car_quotation",
  ]);

  return suggestions.filter((item) => {
    const suggestion = normalizeConversationSuggestion(item, contextSnapshot);

    if (suggestion.type === "ask" && !suggestion.query) return false;
    if (
      suggestion.kind === "lead" &&
      !firstMeaningful(suggestion.leadType, suggestion.contextPatch?.leadType)
    ) {
      return false;
    }

    const queryWords = toWords(suggestion.query);
    const entityModel = toWords(suggestion.entities?.model);
    const entityVariant = toWords(suggestion.entities?.variant);

    if (anchorModel) {
      const hasAnchor =
        queryWords.includes(anchorModel) ||
        entityModel.includes(anchorModel) ||
        asArray(suggestion.entities?.models).map(toWords).includes(anchorModel);
      if (!hasAnchor && suggestion.intent !== "vehicle_comparison")
        return false;
    }

    if (suggestion.intent === "vehicle_comparison" && anchorModel) {
      const models = asArray(suggestion.entities?.models).map(toWords);
      const hasAnchor =
        models.includes(anchorModel) ||
        entityModel.includes(anchorModel) ||
        queryWords.includes(anchorModel);
      if (!hasAnchor) return false;
    }

    if (
      anchorVariant &&
      variantScopedIntents.has(suggestion.intent) &&
      /variant|emi|quote|price|feature|zx|sx|vx|htx|gtx/.test(queryWords)
    ) {
      const hasVariant =
        queryWords.includes(anchorVariant) ||
        entityVariant.includes(anchorVariant);
      if (!hasVariant && suggestion.intent !== "vehicle_comparison")
        return false;
    }

    return true;
  });
};

const suggestionKey = (suggestion = {}) =>
  [
    mapIntentAlias(suggestion.intent || ""),
    toWords(suggestion.query || suggestion.title || ""),
    toWords(suggestion.entities?.model || ""),
    toWords(suggestion.entities?.variant || ""),
    toWords(asArray(suggestion.entities?.models).join("|")),
  ].join("::");

const dedupeSuggestions = (suggestions = []) => {
  const seen = new Map();

  for (const suggestion of suggestions) {
    const key = suggestionKey(suggestion);
    const existing = seen.get(key);

    if (
      !existing ||
      Number(suggestion.priority || 0) > Number(existing.priority || 0)
    ) {
      seen.set(key, suggestion);
    }
  }

  return [...seen.values()];
};

export const rankSuggestions = async (suggestions, context = {}) => {
  const ranked = await Promise.all(
    suggestions.map(async (suggestion) => {
      let adaptiveFromLearning = 0;
      try {
        adaptiveFromLearning =
          (await Promise.race([
            getSuggestionScore(
              context.userId || "",
              context.intent || "",
              suggestion.id,
            ),
            new Promise((resolve) => setTimeout(() => resolve(0), 250)),
          ])) || 0;
      } catch (error) {
        adaptiveFromLearning = 0;
      }

      const affinityBoost =
        Number(context?.profile?.intentAffinity?.[suggestion.intent] || 0) * 5;
      const adaptiveScore =
        Number(suggestion.priority || 0) +
        adaptiveFromLearning * 100 +
        affinityBoost;

      return {
        ...suggestion,
        adaptiveScore,
      };
    }),
  );

  return ranked.sort((a, b) => b.adaptiveScore - a.adaptiveScore);
};

const buildChainedSuggestion = (intent, contextSnapshot) => {
  const model = contextSnapshot.anchorModel;
  const variant = contextSnapshot.anchorVariant;
  const city = contextSnapshot.city || contextSnapshot.requestedCity;
  if (!model) return null;

  if (intent === "vehicle_colors") {
    return suggestionForModel(contextSnapshot, `Show colors of ${model}`, {
      title: `Show ${model} colors`,
      subtitle: "View available exterior options",
      kind: "question",
      type: "ask",
      intent,
      canvasType: "color_studio_canvas",
      priority: 78,
      icon: "palette",
    });
  }

  if (intent === "vehicle_model_features_explorer") {
    return suggestionForModel(contextSnapshot, `Show features of ${model}`, {
      title: `Show ${model} features`,
      subtitle: "Explore full feature list",
      kind: "question",
      type: "ask",
      intent,
      canvasType: "feature_explorer_canvas",
      priority: 77,
      icon: "sparkles",
    });
  }

  if (intent === "vehicle_emi_calculator") {
    return suggestionForModel(
      contextSnapshot,
      `Calculate EMI for ${model}${variant ? ` ${variant}` : ""}`,
      {
        title: `Calculate EMI for ${model}${variant ? ` ${variant}` : ""}`,
        subtitle: "Plan your monthly outflow",
        kind: "action",
        type: "ask",
        intent,
        canvasType: "emi_calculator_canvas",
        priority: 80,
        icon: "calculator",
      },
    );
  }

  if (intent === "vehicle_comparison") {
    return {
      title: `Compare ${model} with rivals`,
      subtitle: "Side-by-side view",
      kind: "action",
      type: "ask",
      intent,
      query: `Compare ${model} with similar cars`,
      entities: { model, city },
      canvasType: "comparison_canvas",
      priority: 79,
      icon: "scale",
    };
  }

  if (intent === "vehicle_feature_answer") {
    return suggestionForModel(
      contextSnapshot,
      `Does ${model}${variant ? ` ${variant}` : ""} have ADAS?`,
      {
        title: `Check ${model}${variant ? ` ${variant}` : ""} key features`,
        subtitle: "Quick spec answer",
        kind: "question",
        type: "ask",
        intent,
        inlineType: "feature_answer_card",
        priority: 75,
        icon: "help",
      },
    );
  }

  if (intent === "vehicle_price_breakup") {
    return suggestionForModel(contextSnapshot, `Show ${model} price breakup`, {
      title: `Show ${model} price breakup`,
      subtitle: "On-road charge details",
      kind: "question",
      type: "ask",
      intent,
      canvasType: "price_breakup_canvas",
      priority: 78,
      icon: "receipt",
    });
  }

  if (intent === "new_car_loan_enquiry") {
    return suggestionForModel(contextSnapshot, "Request finance callback", {
      title: "Talk to finance advisor",
      subtitle: "Check eligibility and documents",
      kind: "lead",
      type: "lead",
      leadType: "finance_callback",
      intent,
      canvasType: "finance_guide_canvas",
      priority: 76,
      icon: "phone",
      contextPatch: { leadType: "finance_callback" },
    });
  }

  if (intent === "aci_new_car_quotation") {
    return suggestionForModel(
      contextSnapshot,
      `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
      {
        title: `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
        subtitle: "Move to best deal",
        kind: "lead",
        type: "lead",
        leadType: "quotation",
        intent,
        canvasType: "aci_quotation_canvas",
        priority: 81,
        icon: "file-text",
        contextPatch: { leadType: "quotation" },
      },
    );
  }

  return null;
};

const addContextAwarePrompts = (suggestions, contextSnapshot) => {
  const model = contextSnapshot.anchorModel;
  if (!model) return;

  const missingContext = {
    variant: !contextSnapshot.variant,
    city: !contextSnapshot.city,
    budget: !contextSnapshot.budgetMax,
  };

  if (missingContext.variant) {
    suggestions.push(
      createSuggestion(
        suggestionForModel(contextSnapshot, `Show ${model} variants`, {
          title: `Which ${model} variant do you prefer?`,
          subtitle: "I can narrow to exact variant",
          kind: "clarification",
          type: "ask",
          intent: "vehicle_pricelist",
          canvasType: "pricelist_canvas",
          priority: 68,
          icon: "list",
        }),
        contextSnapshot,
      ),
    );
  }

  if (missingContext.city) {
    suggestions.push(
      createSuggestion(
        suggestionForModel(contextSnapshot, `Show ${model} price in my city`, {
          title: `Set city for ${model} on-road price`,
          subtitle: "On-road cost changes by city",
          kind: "clarification",
          type: "ask",
          intent: "vehicle_city_price",
          canvasType: "pricelist_canvas",
          priority: 67,
          icon: "map-pin",
        }),
        contextSnapshot,
      ),
    );
  }

  if (missingContext.budget) {
    suggestions.push(
      createSuggestion(
        suggestionForModel(
          contextSnapshot,
          `My budget for ${model} is under 20 lakh`,
          {
            title: "Share your budget",
            subtitle: "I will tailor variant and EMI recommendations",
            kind: "question",
            type: "ask",
            intent: "vehicle_budget_search",
            canvasType: "recommendation_results_canvas",
            priority: 66,
            icon: "calculator",
          },
        ),
        contextSnapshot,
      ),
    );
  }
};

const addIntentChainSuggestions = (suggestions, contextSnapshot) => {
  const chain = INTENT_CHAINS[contextSnapshot.intent] || [];
  for (const nextIntent of chain) {
    const nextSuggestion = buildChainedSuggestion(nextIntent, contextSnapshot);
    if (!nextSuggestion) continue;
    suggestions.push(createSuggestion(nextSuggestion, contextSnapshot));
  }
};

export const buildConversationSuggestions = async ({
  parsed,
  result,
  contextSnapshot,
  primaryWidget,
}) => {
  void parsed;
  void result;
  const suggestions = [];
  const intent = contextSnapshot.intent;
  const model = contextSnapshot.anchorModel;
  const variant = contextSnapshot.anchorVariant;
  const city = contextSnapshot.city || contextSnapshot.requestedCity;

  const add = (entry) =>
    suggestions.push(createSuggestion(entry, contextSnapshot));

  if (!isStructuredNewCarIntent(intent)) {
    return [];
  }

  if (intent === "vehicle_model_ambiguity") {
    const options = asArray(
      primaryWidget?.options || primaryWidget?.data?.options,
    ).slice(0, 4);
    for (const option of options) {
      const optionModel = normalizeText(
        firstMeaningful(option.model, option.displayName),
      );
      add({
        title: option.displayName || optionModel,
        subtitle: "Use this model",
        kind: "clarification",
        type: "select",
        intent: "vehicle_pricelist",
        query: option.followUpQuery || `${optionModel} pricelist`,
        entities: {
          model: optionModel,
          city,
        },
        contextPatch: {
          selectedModels: [optionModel],
          selectedEntity: option,
          forceModelSelection: true,
          entities: {
            model: optionModel,
            models: [optionModel],
            ...(option.brand ? { make: normalizeText(option.brand) } : {}),
          },
        },
        priority: 98,
        icon: "car",
      });
    }
    if (options.length >= 2) {
      const a = normalizeText(
        firstMeaningful(options[0]?.model, options[0]?.displayName),
      );
      const b = normalizeText(
        firstMeaningful(options[1]?.model, options[1]?.displayName),
      );
      add({
        title: `Compare ${a} and ${b}`,
        subtitle: "Quick side-by-side",
        kind: "action",
        type: "ask",
        intent: "vehicle_comparison",
        query: `Compare ${a} and ${b}`,
        entities: { model: a, models: [a, b], city },
        displayMode: "canvas",
        canvasType: "comparison_canvas",
        priority: 86,
        icon: "scale",
      });
    }
    const brand = normalizeText(
      firstMeaningful(options[0]?.brand, contextSnapshot.brand),
    );
    if (brand) {
      add({
        title: `Show all ${brand} compact SUVs`,
        subtitle: "Broader shortlist",
        kind: "question",
        type: "ask",
        intent: "vehicle_budget_search",
        query: `Show all ${brand} compact SUVs`,
        entities: { make: brand, bodyType: "compact suv", city },
        displayMode: "canvas",
        canvasType: "recommendation_results_canvas",
        priority: 80,
        icon: "list",
      });
    }
  } else if (intent === "vehicle_variant_ambiguity") {
    const options = asArray(
      primaryWidget?.options || primaryWidget?.data?.options,
    ).slice(0, 5);
    for (const option of options.slice(0, 3)) {
      const pickedVariant = normalizeText(
        firstMeaningful(option.variant, option.label, option.displayName),
      );
      add({
        title: pickedVariant,
        subtitle: "Use this variant",
        kind: "clarification",
        type: "select",
        intent: "vehicle_variant_price",
        query: `${model} ${pickedVariant} price`,
        entities: { model, variant: pickedVariant, city },
        contextPatch: {
          selectedVariants: [pickedVariant],
        },
        priority: 98,
        icon: "check",
      });
    }
    add({
      title: `Compare ${model} variants`,
      subtitle: "Find the right one",
      kind: "action",
      type: "ask",
      intent: "vehicle_variant_upgrade_value",
      query: `Compare ${model} variants`,
      entities: { model, city },
      displayMode: "canvas",
      canvasType: "variant_upgrade_value_canvas",
      priority: 84,
      icon: "scale",
    });
    add({
      title: `Show all ${model} variants`,
      subtitle: "Open full list",
      kind: "question",
      type: "ask",
      intent: "vehicle_pricelist",
      query: `${model} pricelist`,
      entities: { model, city },
      canvasType: "pricelist_canvas",
      priority: 80,
      icon: "list",
    });
  } else if (
    [
      "vehicle_pricelist",
      "vehicle_city_price",
      "vehicle_variant_price",
    ].includes(intent)
  ) {
    add(
      suggestionForModel(contextSnapshot, `Show colors of ${model}`, {
        title: `Show ${model} colors`,
        subtitle: "View exterior options",
        kind: "question",
        type: "ask",
        intent: "vehicle_colors",
        displayMode: "canvas",
        canvasType: "color_studio_canvas",
        priority: 95,
        icon: "palette",
      }),
    );
    add(
      suggestionForModel(contextSnapshot, `Show features of ${model}`, {
        title: `Show ${model} features`,
        subtitle: "Explore key features",
        kind: "question",
        type: "ask",
        intent: "vehicle_model_features_explorer",
        displayMode: "canvas",
        canvasType: "feature_explorer_canvas",
        priority: 93,
        icon: "sparkles",
      }),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Calculate EMI for ${model}${variant ? ` ${variant}` : ""}`,
        {
          title: `Calculate EMI for ${model}${variant ? ` ${variant}` : ""}`,
          subtitle: "Plan monthly budget",
          kind: "action",
          type: "ask",
          intent: "vehicle_emi_calculator",
          displayMode: "canvas",
          canvasType: "emi_calculator_canvas",
          priority: 91,
          icon: "calculator",
        },
      ),
    );
    await addCompareSuggestion(suggestions, contextSnapshot);
    add(
      suggestionForModel(
        contextSnapshot,
        `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
        {
          title: `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
          subtitle: "Prepare ACI quote",
          kind: "lead",
          type: "lead",
          leadType: "quotation",
          intent: "aci_new_car_quotation",
          displayMode: "canvas",
          canvasType: "aci_quotation_canvas",
          priority: 89,
          icon: "file-text",
          contextPatch: { leadType: "quotation" },
        },
      ),
    );
  } else if (["vehicle_colors", "vehicle_color_gallery"].includes(intent)) {
    const chosenColor = normalizeText(
      firstMeaningful(
        primaryWidget?.color,
        primaryWidget?.data?.color,
        primaryWidget?.rows?.[0]?.colorName,
      ),
    );
    add(
      suggestionForModel(contextSnapshot, `Show ${model} pricelist`, {
        title: `Show ${model} pricelist`,
        subtitle: "Back to pricing",
        kind: "question",
        type: "ask",
        intent: "vehicle_pricelist",
        canvasType: "pricelist_canvas",
        priority: 95,
        icon: "tag",
      }),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Confirm ${chosenColor || "preferred"} color for ${model} quotation`,
        {
          title: `Confirm color in quotation`,
          subtitle: "Variant-wise color availability needs confirmation",
          kind: "lead",
          type: "lead",
          leadType: "quotation",
          intent: "aci_new_car_quotation",
          canvasType: "aci_quotation_canvas",
          priority: 93,
          icon: "check-circle",
          contextPatch: {
            leadType: "quotation",
            preferredColor: chosenColor || undefined,
            colorConfirmationRequired: true,
          },
        },
      ),
    );
    add(
      suggestionForModel(contextSnapshot, `Calculate EMI for ${model}`, {
        title: `Calculate EMI for ${model}`,
        subtitle: "Finance planning",
        kind: "action",
        type: "ask",
        intent: "vehicle_emi_calculator",
        canvasType: "emi_calculator_canvas",
        priority: 90,
        icon: "calculator",
      }),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Get quotation for ${model}${chosenColor ? ` in ${chosenColor}` : ""}`,
        {
          title: `Get quotation for ${model}`,
          subtitle: chosenColor
            ? `Include ${chosenColor} color`
            : "Include preferred color",
          kind: "lead",
          type: "lead",
          leadType: "quotation",
          intent: "aci_new_car_quotation",
          canvasType: "aci_quotation_canvas",
          priority: 88,
          icon: "file-text",
          contextPatch: {
            leadType: "quotation",
            preferredColor: chosenColor || undefined,
          },
        },
      ),
    );
    add(
      suggestionForModel(contextSnapshot, `Book test drive for ${model}`, {
        title: `Book test drive for ${model}`,
        subtitle: "Experience the car",
        kind: "lead",
        type: "lead",
        leadType: "test_drive",
        intent: "vehicle_test_drive_request",
        priority: 84,
        icon: "car",
        contextPatch: { leadType: "test_drive" },
      }),
    );
  } else if (
    ["vehicle_feature_answer", "vehicle_spec_lookup"].includes(intent)
  ) {
    add(
      suggestionForModel(contextSnapshot, `Show features of ${model}`, {
        title: `Open ${model} features`,
        subtitle: "Full feature explorer",
        kind: "question",
        type: "ask",
        intent: "vehicle_model_features_explorer",
        canvasType: "feature_explorer_canvas",
        priority: 96,
        icon: "sparkles",
      }),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Show ${model}${variant ? ` ${variant}` : ""} price`,
        {
          title: `Show ${model}${variant ? ` ${variant}` : ""} price`,
          subtitle: "Check latest price",
          kind: "question",
          type: "ask",
          intent: "vehicle_variant_price",
          canvasType: "pricelist_canvas",
          priority: 92,
          icon: "tag",
        },
      ),
    );
    if (variant) {
      add(
        suggestionForModel(
          contextSnapshot,
          `Compare ${model} ${variant} variants`,
          {
            title: `Compare ${model} ${variant} variants`,
            subtitle: "Find better value",
            kind: "action",
            type: "ask",
            intent: "vehicle_variant_upgrade_value",
            canvasType: "variant_upgrade_value_canvas",
            priority: 90,
            icon: "scale",
          },
        ),
      );
    }
    add(
      suggestionForModel(
        contextSnapshot,
        `Calculate EMI for ${model}${variant ? ` ${variant}` : ""}`,
        {
          title: `Calculate EMI for ${model}${variant ? ` ${variant}` : ""}`,
          subtitle: "Monthly estimate",
          kind: "action",
          type: "ask",
          intent: "vehicle_emi_calculator",
          canvasType: "emi_calculator_canvas",
          priority: 88,
          icon: "calculator",
        },
      ),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
        {
          title: `Get quotation for ${model}${variant ? ` ${variant}` : ""}`,
          subtitle: "Proceed with quote",
          kind: "lead",
          type: "lead",
          leadType: "quotation",
          intent: "aci_new_car_quotation",
          canvasType: "aci_quotation_canvas",
          priority: 86,
          icon: "file-text",
          contextPatch: { leadType: "quotation" },
        },
      ),
    );
  } else if (
    [
      "vehicle_recommendation_discovery",
      "vehicle_budget_search",
      "vehicle_brand_search",
      "vehicle_body_type_search",
      "vehicle_use_case_search",
      "vehicle_safety_search",
    ].includes(intent)
  ) {
    const models = unique([
      ...(contextSnapshot.resultModels || []),
      ...(contextSnapshot.selectedModels || []),
    ]).slice(0, 3);
    const anchor = normalizeText(firstMeaningful(models[0], model));
    if (models.length >= 2) {
      add({
        title: "Compare top 3 results",
        subtitle: models.join(" • "),
        kind: "action",
        type: "ask",
        intent: "vehicle_comparison",
        query:
          models.length >= 3
            ? `Compare ${models[0]}, ${models[1]} and ${models[2]}`
            : `Compare ${models[0]} and ${models[1]}`,
        entities: { model: models[0], models, city },
        canvasType: "comparison_canvas",
        priority: 96,
        icon: "scale",
      });
    }
    add({
      title: "Show safest SUVs under this budget",
      subtitle: "Safety-focused shortlist",
      kind: "question",
      type: "ask",
      intent: "vehicle_safety_search",
      query: `Show safest SUVs under ${formatBudgetForQuery(contextSnapshot.budgetMax, 20)}`,
      entities: {
        budgetMax: contextSnapshot.budgetMax || undefined,
        bodyType: "suv",
        city,
      },
      canvasType: "safety_advisor_canvas",
      priority: 92,
      icon: "shield",
    });
    if (anchor) {
      add({
        title: `Calculate EMI for ${anchor}`,
        subtitle: "Check affordability",
        kind: "action",
        type: "ask",
        intent: "vehicle_emi_calculator",
        query: `Calculate EMI for ${anchor}`,
        entities: { model: anchor, city },
        canvasType: "emi_calculator_canvas",
        priority: 89,
        icon: "calculator",
      });
      add({
        title: `Get quotation for ${anchor}`,
        subtitle: "Move ahead with a quote",
        kind: "lead",
        type: "lead",
        leadType: "quotation",
        intent: "aci_new_car_quotation",
        query: `Get quotation for ${anchor}`,
        entities: { model: anchor, city },
        canvasType: "aci_quotation_canvas",
        priority: 84,
        icon: "file-text",
        contextPatch: { leadType: "quotation" },
      });
    }
    add({
      title: "Show automatic SUVs only",
      subtitle: "Refine shortlist",
      kind: "question",
      type: "ask",
      intent: "vehicle_budget_search",
      query: `Show automatic SUVs${contextSnapshot.budgetMax ? ` under ${formatBudgetForQuery(contextSnapshot.budgetMax, 20)}` : ""}`,
      entities: {
        budgetMax: contextSnapshot.budgetMax || undefined,
        bodyType: "suv",
        transmission: "automatic",
        city,
      },
      canvasType: "recommendation_results_canvas",
      priority: 82,
      icon: "filter",
    });
  } else if (
    [
      "vehicle_comparison",
      "vehicle_model_comparison",
      "vehicle_variant_comparison",
      "vehicle_safety_comparison",
    ].includes(intent)
  ) {
    const models = unique(contextSnapshot.selectedModels).slice(0, 3);
    const first = models[0];
    const second = models[1];
    if (first) {
      add({
        title: `Calculate EMI for ${first}`,
        subtitle: "Monthly view",
        kind: "action",
        type: "ask",
        intent: "vehicle_emi_calculator",
        query: `Calculate EMI for ${first}`,
        entities: { model: first, city },
        canvasType: "emi_calculator_canvas",
        priority: 95,
        icon: "calculator",
      });
    }
    if (second) {
      add({
        title: `Calculate EMI for ${second}`,
        subtitle: "Monthly view",
        kind: "action",
        type: "ask",
        intent: "vehicle_emi_calculator",
        query: `Calculate EMI for ${second}`,
        entities: { model: second, city },
        canvasType: "emi_calculator_canvas",
        priority: 93,
        icon: "calculator",
      });
    }
    if (first && second) {
      add({
        title: "Show feature differences",
        subtitle: `${first} vs ${second}`,
        kind: "question",
        type: "ask",
        intent: "vehicle_comparison",
        query: `Compare ${first} and ${second} feature differences`,
        entities: { model: first, models: [first, second], city },
        canvasType: "comparison_canvas",
        priority: 90,
        icon: "list",
      });
    }
    const compareAnchor = normalizeText(firstMeaningful(model, first));
    let rivals = [];
    try {
      rivals =
        (await Promise.race([
          findRelevantRivals({
            model: compareAnchor,
            bodyType: contextSnapshot.bodyType,
            priceMin: contextSnapshot.priceMin || contextSnapshot.budgetMin || 0,
            priceMax: contextSnapshot.priceMax || contextSnapshot.budgetMax || 0,
            brand: contextSnapshot.brand,
            city,
          }),
          new Promise((resolve) => setTimeout(() => resolve([]), 1800)),
        ])) || [];
    } catch (error) {
      void error;
      rivals = [];
    }
    const addModel = rivals.find(
      (item) => !models.map(toWords).includes(toWords(item)),
    );
    if (addModel && models.length >= 2) {
      add({
        title: `Add ${addModel} to comparison`,
        subtitle: "Expand comparison",
        kind: "action",
        type: "ask",
        intent: "vehicle_comparison",
        query: `Compare ${models[0]}, ${models[1]} and ${addModel}`,
        entities: {
          model: models[0],
          models: [models[0], models[1], addModel],
          city,
        },
        canvasType: "comparison_canvas",
        priority: 86,
        icon: "plus",
      });
    }
    if (compareAnchor) {
      add({
        title: `Get quotation for ${compareAnchor}`,
        subtitle: "Proceed with best fit",
        kind: "lead",
        type: "lead",
        leadType: "quotation",
        intent: "aci_new_car_quotation",
        query: `Get quotation for ${compareAnchor}`,
        entities: { model: compareAnchor, city },
        canvasType: "aci_quotation_canvas",
        priority: 84,
        icon: "file-text",
        contextPatch: { leadType: "quotation" },
      });
    }
  } else if (
    [
      "vehicle_emi_calculator",
      "vehicle_emi_options",
      "vehicle_monthly_budget_planner",
    ].includes(intent)
  ) {
    add(
      suggestionForModel(
        contextSnapshot,
        model
          ? `Change down payment and recalculate EMI for ${model}`
          : "Change down payment and recalculate EMI",
        {
          title: "Change down payment",
          subtitle: "Recalculate monthly EMI",
          kind: "question",
          type: "ask",
          intent: "vehicle_emi_calculator",
          canvasType: "emi_calculator_canvas",
          priority: 96,
          icon: "sliders",
        },
      ),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Compare EMI across ${model} variants`,
        {
          title: `Compare EMI across ${model} variants`,
          subtitle: "Find affordable variant",
          kind: "action",
          type: "ask",
          intent: "vehicle_emi_options",
          canvasType: "emi_calculator_canvas",
          priority: 92,
          icon: "scale",
        },
      ),
    );
    add(
      suggestionForModel(contextSnapshot, `Show ${model} price breakup`, {
        title: `Show ${model} price breakup`,
        subtitle: "Understand on-road components",
        kind: "question",
        type: "ask",
        intent: "vehicle_price_breakup",
        canvasType: "price_breakup_canvas",
        priority: 88,
        icon: "receipt",
      }),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Get quotation for ${model} with finance`,
        {
          title: `Get quotation with finance`,
          subtitle: `For ${model}`,
          kind: "lead",
          type: "lead",
          leadType: "quotation",
          intent: "aci_new_car_quotation",
          canvasType: "aci_quotation_canvas",
          priority: 85,
          icon: "file-text",
          contextPatch: { leadType: "quotation", finance: true },
        },
      ),
    );
    add(
      suggestionForModel(contextSnapshot, "Request finance callback", {
        title: "Request finance callback",
        subtitle: "Talk to finance advisor",
        kind: "lead",
        type: "lead",
        leadType: "finance_callback",
        intent: "new_car_loan_enquiry",
        canvasType: "finance_guide_canvas",
        priority: 82,
        icon: "phone",
        contextPatch: { leadType: "finance_callback" },
      }),
    );
  } else if (["vehicle_offers", "vehicle_offer_lookup"].includes(intent)) {
    add(
      suggestionForModel(contextSnapshot, `Get quotation for ${model}`, {
        title: `Get ACI quotation for ${model}`,
        subtitle: "Include latest offers",
        kind: "lead",
        type: "lead",
        leadType: "quotation",
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        priority: 95,
        icon: "file-text",
        contextPatch: { leadType: "quotation" },
      }),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Calculate EMI for ${model} after offers`,
        {
          title: "Calculate EMI after offers",
          subtitle: model,
          kind: "action",
          type: "ask",
          intent: "vehicle_emi_calculator",
          canvasType: "emi_calculator_canvas",
          priority: 91,
          icon: "calculator",
        },
      ),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Show offers on ${model} and exchange benefit`,
        {
          title: "Check exchange benefit",
          subtitle: model,
          kind: "question",
          type: "ask",
          intent: "vehicle_offers",
          canvasType: "offers_canvas",
          priority: 87,
          icon: "refresh",
        },
      ),
    );
    add(
      suggestionForModel(contextSnapshot, `Book test drive for ${model}`, {
        title: `Book test drive for ${model}`,
        subtitle: "Experience before booking",
        kind: "lead",
        type: "lead",
        leadType: "test_drive",
        intent: "vehicle_test_drive_request",
        priority: 84,
        icon: "car",
        contextPatch: { leadType: "test_drive" },
      }),
    );
  } else if (intent === "aci_new_car_quotation") {
    add(
      suggestionForModel(
        contextSnapshot,
        `Continue quotation for ${model}${variant ? ` ${variant}` : ""}`,
        {
          title: "Continue with quote",
          subtitle: `${model}${variant ? ` • ${variant}` : ""}`,
          kind: "lead",
          type: "lead",
          leadType: "quotation",
          intent: "aci_new_car_quotation",
          canvasType: "aci_quotation_canvas",
          priority: 97,
          icon: "file-text",
          contextPatch: { leadType: "quotation" },
        },
      ),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Add exchange car in ${model} quote`,
        {
          title: "Add exchange car",
          subtitle: "Increase quote savings",
          kind: "question",
          type: "ask",
          intent: "aci_new_car_quotation",
          canvasType: "aci_quotation_canvas",
          priority: 90,
          icon: "refresh",
        },
      ),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Include finance in ${model} quotation`,
        {
          title: "Include finance",
          subtitle: "Add EMI plan in quote",
          kind: "question",
          type: "ask",
          intent: "vehicle_emi_calculator",
          canvasType: "emi_calculator_canvas",
          priority: 87,
          icon: "calculator",
        },
      ),
    );
    add(
      suggestionForModel(contextSnapshot, `Book test drive for ${model}`, {
        title: "Book test drive also",
        subtitle: "Evaluate before finalizing",
        kind: "lead",
        type: "lead",
        leadType: "test_drive",
        intent: "vehicle_test_drive_request",
        priority: 84,
        icon: "car",
        contextPatch: { leadType: "test_drive" },
      }),
    );
  } else if (intent === "vehicle_test_drive_request") {
    add(
      suggestionForModel(contextSnapshot, `Book test drive for ${model}`, {
        title: `Confirm test drive for ${model}`,
        subtitle: "Reserve your preferred slot",
        kind: "lead",
        type: "lead",
        leadType: "test_drive",
        intent: "vehicle_test_drive_request",
        priority: 96,
        icon: "car",
        contextPatch: { leadType: "test_drive" },
      }),
    );
    add(
      suggestionForModel(contextSnapshot, `Get quotation for ${model}`, {
        title: `Get quotation for ${model}`,
        subtitle: "Carry latest deal for test drive",
        kind: "lead",
        type: "lead",
        leadType: "quotation",
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        priority: 90,
        icon: "file-text",
        contextPatch: { leadType: "quotation" },
      }),
    );
  } else if (intent === "vehicle_callback_request") {
    add(
      suggestionForModel(contextSnapshot, `Request callback for ${model}`, {
        title: "Request callback",
        subtitle: `Talk to advisor for ${model}`,
        kind: "lead",
        type: "lead",
        leadType: "callback",
        intent: "vehicle_callback_request",
        priority: 96,
        icon: "phone",
        contextPatch: { leadType: "callback" },
      }),
    );
    add(
      suggestionForModel(contextSnapshot, `Get quotation for ${model}`, {
        title: `Get quotation for ${model}`,
        subtitle: "Share details on callback",
        kind: "lead",
        type: "lead",
        leadType: "quotation",
        intent: "aci_new_car_quotation",
        canvasType: "aci_quotation_canvas",
        priority: 90,
        icon: "file-text",
        contextPatch: { leadType: "quotation" },
      }),
    );
  }

  addContextAwarePrompts(suggestions, contextSnapshot);
  addIntentChainSuggestions(suggestions, contextSnapshot);

  if (
    isRecent(contextSnapshot.history?.viewedPrice) &&
    isRecent(contextSnapshot.history?.compared) &&
    isRecent(contextSnapshot.history?.checkedEmi) &&
    model
  ) {
    add(
      suggestionForModel(
        contextSnapshot,
        `Would you like me to prepare the best deal for you for ${model}${variant ? ` ${variant}` : ""}?`,
        {
          title: "Would you like me to prepare the best deal for you?",
          subtitle: `${model}${variant ? ` • ${variant}` : ""}`,
          kind: "lead",
          type: "lead",
          leadType: "quotation",
          intent: "aci_new_car_quotation",
          canvasType: "aci_quotation_canvas",
          priority: 99,
          icon: "file-text",
          tone: "primary",
          contextPatch: { leadType: "quotation" },
        },
      ),
    );
  }

  const hasQuotationLeadAlready = suggestions.some(
    (item) =>
      item.kind === "lead" &&
      (item.leadType === "quotation" ||
        item.contextPatch?.leadType === "quotation" ||
        item.intent === "aci_new_car_quotation"),
  );

  if (!hasQuotationLeadAlready) {
    for (const action of asArray(contextSnapshot.closingActions)) {
      add(action);
    }
  }

  if (!suggestions.length && model) {
    add(
      suggestionForModel(contextSnapshot, `Show ${model} pricelist`, {
        title: `Show ${model} pricelist`,
        kind: "question",
        type: "ask",
        intent: "vehicle_pricelist",
        canvasType: "pricelist_canvas",
        priority: 80,
      }),
    );
    add(
      suggestionForModel(
        contextSnapshot,
        `Compare ${model} with similar cars`,
        {
          title: `Compare ${model}`,
          kind: "action",
          type: "ask",
          intent: "vehicle_comparison",
          canvasType: "comparison_canvas",
          priority: 78,
        },
      ),
    );
    add(
      suggestionForModel(contextSnapshot, `Calculate EMI for ${model}`, {
        title: `Calculate EMI for ${model}`,
        kind: "action",
        type: "ask",
        intent: "vehicle_emi_calculator",
        canvasType: "emi_calculator_canvas",
        priority: 75,
      }),
    );
  }

  const validated = validateConversationSuggestions(
    suggestions,
    contextSnapshot,
  );
  const deduped = dedupeSuggestions(validated);
  const ranked = await rankSuggestions(deduped, contextSnapshot);
  return ranked.slice(0, 5);
};

export const buildLeadingQuestions = ({ contextSnapshot, parsed }) => {
  const leading = [];
  if (contextSnapshot.intent === "vehicle_budget_search") {
    leading.push(
      {
        label: "Do you want on-road or ex-showroom budget?",
        query: "Show on-road price options",
        intent: "vehicle_pricelist",
      },
      {
        label: "Automatic only?",
        query: "Show automatic options only",
        intent: "vehicle_budget_search",
      },
    );
  }
  if (contextSnapshot.intent === "vehicle_recommendation_discovery") {
    leading.push({
      label: "What is your budget range?",
      query: "My budget is under 20 lakh",
      intent: "vehicle_budget_search",
    });
  }
  if (
    [
      "vehicle_pricelist",
      "vehicle_city_price",
      "vehicle_variant_price",
    ].includes(contextSnapshot.intent)
  ) {
    leading.push({
      label: "Would you like EMI for this model?",
      query: `Calculate EMI for ${contextSnapshot.anchorModel}`,
      intent: "vehicle_emi_calculator",
    });
  }
  if (!leading.length && parsed?.secondaryIntents?.length) {
    for (const intent of parsed.secondaryIntents.slice(0, 2)) {
      const config = getNewCarQuestionConfig(intent);
      if (!config) continue;
      leading.push({
        label:
          config.exampleQuestions?.[0] ||
          `Explore ${intent.replace(/_/g, " ")}`,
        query: config.exampleQuestions?.[0] || "",
        intent,
      });
    }
  }
  return leading
    .filter((item) => item.label && item.query)
    .slice(0, 4)
    .map((item, index) => ({
      id: `lead-q-${index + 1}`,
      ...item,
      displayMode: "inline",
      canvasType: getNewCarQuestionConfig(item.intent)?.canvasType,
      inlineType: getNewCarQuestionConfig(item.intent)?.inlineType,
    }));
};

export const buildFollowUpSuggestions = ({ conversationSuggestions = [] }) =>
  conversationSuggestions.map((item) => ({
    id: item.id,
    label: item.title,
    title: item.title,
    subtitle: item.subtitle,
    message: item.query,
    query: item.query,
    intent: item.intent,
    type: item.type,
    kind: item.kind,
    icon: item.icon,
    tone: item.tone,
    entities: item.entities,
    contextPatch: item.contextPatch,
    canvasType: item.canvasType,
    inlineType: item.inlineType,
    priority: item.priority,
    leadType: item.leadType,
    route: item.route,
    adaptiveScore: item.adaptiveScore,
    context: {
      ...(item.contextPatch || {}),
      actionContext: item,
      entities: item.entities,
    },
  }));
