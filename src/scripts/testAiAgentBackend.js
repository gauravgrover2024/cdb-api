import dotenv from "dotenv";
import fs from "fs";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import SuggestionPerformance from "../models/SuggestionPerformance.js";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";
import { parseAgentMessage } from "../services/aiAgent/aiAgent.intentParser.js";
import { getToolForIntent } from "../services/aiAgent/aiAgent.toolRegistry.js";
import {
  NEW_CAR_CANVAS_TYPES,
  NEW_CAR_INLINE_TYPES,
} from "../services/aiAgent/aiAgent.newCarQuestionMap.js";

dotenv.config();

const adminUser = {
  _id: "000000000000000000000000",
  role: "admin",
  name: "ACI Assist Test Harness",
};

const CHAIN_DEPTH = Number(process.env.ACI_TEST_CHAIN_DEPTH || 1);
const CHAIN_LIMIT = Number(process.env.ACI_TEST_CHAIN_LIMIT || 4);
const VERIFY_LEARNING =
  String(process.env.ACI_TEST_VERIFY_LEARNING || "true").toLowerCase() !==
  "false";
const VERBOSE_LOGS =
  String(process.env.ACI_TEST_VERBOSE || "false").toLowerCase() === "true";
const QUERY_WARN_MS = 8000;
const QUERY_TIMEOUT_MS = 15000;

const asArray = (value) =>
  Array.isArray(value) ? value : value ? [value] : [];

const unique = (items = []) => [...new Set(items.filter(Boolean))];
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const RETRYABLE_ERROR_REGEX =
  /(server monitor timeout|buffering timed out|timed out|connection .* interrupted|econnreset|network)/i;

const intentMatches = (actual, expected) => {
  if (Array.isArray(expected)) return expected.includes(actual);
  return actual === expected;
};

const textOf = (item = {}) =>
  String(
    item.title ||
      item.label ||
      item.message ||
      item.query ||
      item.text ||
      item.intent ||
      "",
  ).trim();

const queryOf = (item = {}) =>
  String(item.query || item.message || item.label || item.title || "").trim();

const compactSuggestion = (item = {}) => ({
  id: item.id || "",
  title: item.title || item.label || "",
  query: item.query || item.message || "",
  intent: item.intent || "",
  type: item.type || "",
  kind: item.kind || "",
  leadType: item.leadType || item.contextPatch?.leadType || "",
  canvasType: item.canvasType || null,
  inlineType: item.inlineType || null,
  priority: item.priority,
  adaptiveScore: item.adaptiveScore,
  entities: item.entities || {},
});

const compactLeadingQuestion = (item = {}) => ({
  id: item.id || "",
  label: item.label || item.title || "",
  query: item.query || item.message || "",
  intent: item.intent || "",
  displayMode: item.displayMode || "",
  canvasType: item.canvasType || null,
  inlineType: item.inlineType || null,
});

const toCompactLine = (line = {}) => ({
  query: line.query,
  testId: line.testId,
  expectedIntent: line.expectedIntent,
  detectedIntent: line.detectedIntent,
  pass: line.pass,
  failureReason: line.failureReason,
  displayMode: line.displayMode,
  canvasType: line.canvasType,
  inlineType: line.inlineType,
  matchedCount: line.matchedCount,
  actionsCount: line.actionsCount,
  leadingQuestionsCount: line.leadingQuestionsCount,
  conversationSuggestionsCount: line.conversationSuggestionsCount,
});

const TEST_CASES = [
  // Conversation flow anchor tests (A-J)
  {
    id: "A",
    query: "Elevate pricelist",
    expectIntent: "vehicle_pricelist",
    expectAnchorModel: "elevate",
    expectConversationSuggestions: true,
    assertAnchoredSuggestions: true,
    disallowSuggestionModels: ["city", "verna", "marazzo", "ertiga", "carens"],
    expectCompareIncludesAnchor: true,
  },
  {
    id: "B",
    query: "Verna pricelist",
    expectIntent: "vehicle_pricelist",
    expectAnchorModel: "verna",
    expectConversationSuggestions: true,
    disallowSuggestionModels: ["marazzo", "scorpio", "bolero", "innova"],
    expectCompareIncludesAnchor: true,
  },
  {
    id: "C",
    query: "Show colors of Elevate",
    expectIntent: "vehicle_colors",
    expectAnchorModel: "elevate",
    expectConversationSuggestions: true,
    assertAnchoredSuggestions: true,
    disallowSuggestionModels: ["verna", "marazzo", "ertiga", "carens"],
  },
  {
    id: "D",
    query: "Does Elevate ZX have ADAS?",
    expectIntent: "vehicle_feature_answer",
    expectInline: true,
    expectAnchorModel: "elevate",
    expectAnchorVariant: "zx",
    expectConversationSuggestions: true,
    expectVariantInSuggestions: true,
  },
  {
    id: "E",
    query: "Show Venue price",
    expectIntent: "vehicle_model_ambiguity",
    expectInline: true,
    expectConversationSuggestions: true,
    expectSuggestionTitles: ["venue", "n line"],
  },
  {
    id: "F",
    query: "SUVs under 20L",
    expectIntent: "vehicle_budget_search",
    expectModelGrouped: true,
    expectConversationSuggestions: true,
    disallowSuggestionModels: ["verna", "city", "marazzo", "scorpio", "bolero"],
  },
  {
    id: "G",
    query: "Show Elevate price, colors and EMI",
    expectIntent: "vehicle_pricelist",
    expectAnchorModel: "elevate",
    expectMulti: true,
    expectConversationSuggestions: true,
    disallowSuggestionModels: ["marazzo", "ertiga", "carens"],
    expectSuggestionTitles: ["colors", "emi"],
  },
  {
    id: "H",
    query: "Compare Elevate with Creta",
    expectIntent: "vehicle_comparison",
    expectConversationSuggestions: true,
    expectCompareIncludesAnchor: true,
    disallowSuggestionModels: ["marazzo", "ertiga", "carens"],
    expectSuggestionTitles: ["elevate", "creta"],
  },
  {
    id: "I",
    query: "Offers on Elevate",
    expectIntent: "vehicle_offers",
    expectAnchorModel: "elevate",
    expectConversationSuggestions: true,
    assertAnchoredSuggestions: true,
    disallowSuggestionModels: ["verna", "city", "marazzo", "ertiga", "carens"],
  },
  {
    id: "J",
    query: "Get quotation for Elevate ZX",
    expectIntent: "aci_new_car_quotation",
    expectAnchorModel: "elevate",
    expectAnchorVariant: "zx",
    expectConversationSuggestions: true,
    expectLeadSuggestion: true,
  },
  {
    query: "Book test drive for Verna",
    expectIntent: "vehicle_test_drive_request",
    expectAnchorModel: "verna",
    expectLeadSuggestion: true,
  },
  {
    query: "Call me about Seltos",
    expectIntent: "vehicle_callback_request",
    expectAnchorModel: "seltos",
    expectLeadSuggestion: true,
  },
  {
    query: "I want this Verna",
    expectIntent: "aci_new_car_quotation",
    expectAnchorModel: "verna",
    expectLeadSuggestion: true,
  },
  {
    query: "Does Verna SX get Titan Grey?",
    expectIntent: ["vehicle_colors", "vehicle_feature_answer"],
  },
  {
    query: "Which bank gives best loan for Verna?",
    expectIntent: "new_car_loan_enquiry",
  },
  {
    query: "Show all Hyundai compact SUVs",
    expectIntent: ["vehicle_budget_search", "vehicle_brand_search"],
    expectModelGrouped: true,
  },

  // Core
  { query: "Verna pricelist", expectIntent: "vehicle_pricelist" },
  { query: "Verna price in Mumbai", expectIntent: "vehicle_city_price" },
  { query: "Verna SX price", expectIntent: "vehicle_variant_price" },
  { query: "Show colors of Verna", expectIntent: "vehicle_colors" },
  {
    query: "Show features of Verna",
    expectIntent: "vehicle_model_features_explorer",
  },
  {
    query: "Does Verna SX have sunroof?",
    expectIntent: "vehicle_feature_answer",
    expectInline: true,
  },
  {
    query: "Which Verna variants have sunroof?",
    expectIntent: "vehicle_feature_discovery",
  },
  {
    query: "SUVs under 20L",
    expectIntent: "vehicle_budget_search",
    expectModelGrouped: true,
    expectLeading: true,
  },
  {
    query: "Safest SUVs under 20L",
    expectIntent: "vehicle_safety_search",
    expectModelGrouped: true,
    expectLeading: true,
  },
  {
    query: "Automatic cars under 15 lakh",
    expectIntent: "vehicle_budget_search",
    expectModelGrouped: true,
    expectLeading: true,
  },
  { query: "Compare Verna City Slavia", expectIntent: "vehicle_comparison" },
  { query: "Cars similar to Verna", expectIntent: "vehicle_similar_cars" },
  {
    query: "Which Verna variant should I buy?",
    expectIntent: "vehicle_variant_recommendation",
  },
  {
    query: "Difference between Verna SX and SX(O)",
    expectIntent: "vehicle_variant_upgrade_value",
  },
  {
    query: "EMI for Verna with 90% loan for 5 years at 9 percent",
    expectIntent: "vehicle_emi_calculator",
  },
  {
    query: "EMI for Verna with 2 lakh down payment",
    expectIntent: "vehicle_emi_calculator",
  },
  {
    query: "What documents are required for car loan?",
    expectIntent: "new_car_finance_faq",
    expectInline: true,
  },
  { query: "Latest offers on Verna", expectIntent: "vehicle_offers" },
  {
    query: "Get quotation for Verna SX in Delhi",
    expectIntent: "aci_new_car_quotation",
  },
  {
    query: "Nearest Hyundai service center in Delhi",
    expectIntent: "new_car_service_center_search",
  },
  { query: "Verna service cost", expectIntent: "new_car_service_cost" },
  {
    query: "Is Verna available in Delhi?",
    expectIntent: "vehicle_availability",
  },

  // Analytical
  {
    query: "Which car is cheapest to own for 5 years?",
    expectIntent: "vehicle_tco_analysis",
    expectLeading: true,
  },
  {
    query: "Should I buy petrol or diesel for Creta?",
    expectIntent: "vehicle_fuel_decision_advisor",
    expectLeading: true,
  },
  {
    query: "Which car has best resale value?",
    expectIntent: "vehicle_resale_value_analysis",
    expectLeading: true,
  },
  {
    query: "Best car for my lifestyle",
    expectIntent: "vehicle_lifestyle_fit_score",
    expectLeading: true,
  },
  {
    query: "Best car for parents",
    expectIntent: "vehicle_senior_friendly_recommendation",
    expectLeading: true,
  },
  {
    query: "Most spacious cars under 20 lakh",
    expectIntent: "vehicle_space_practicality_advisor",
    expectLeading: true,
  },
  {
    query: "Best performance car under 20 lakh",
    expectIntent: "vehicle_performance_advisor",
    expectLeading: true,
  },
  {
    query: "Cars with highest ground clearance",
    expectIntent: "vehicle_spec_ranking",
    expectLeading: true,
  },
  {
    query: "Best automatic car under 20 lakh",
    expectIntent: "vehicle_budget_search",
    expectModelGrouped: true,
  },
  {
    query: "Cars similar to Elevate",
    expectIntent: "vehicle_similar_cars",
  },
  {
    query: "I want automatic, sunroof and 6 airbags under 15 lakh",
    expectIntent: "vehicle_must_have_feature_builder",
    expectLeading: true,
  },
  {
    query: "My monthly budget is 30000, which car can I buy?",
    expectIntent: "vehicle_monthly_budget_planner",
    expectLeading: true,
  },
  {
    query: "What extra features do I get by paying 1.5 lakh more?",
    expectIntent: "vehicle_variant_upgrade_value",
  },

  // Ambiguity
  {
    query: "Show Venue price",
    expectIntent: "vehicle_model_ambiguity",
    expectInline: true,
    allowAmbiguityFallback: true,
  },
  {
    query: "Verna SX price",
    expectIntent: "vehicle_variant_price",
    allowVariantAmbiguity: true,
  },

  // Multi-intent
  {
    query: "Show Verna price, colors and EMI",
    expectIntent: "vehicle_pricelist",
    expectMulti: true,
  },
  {
    query:
      "Does Verna SX have sunroof and what is EMI with 2 lakh down payment?",
    expectIntent: "vehicle_emi_calculator",
    expectMulti: true,
  },
  {
    query: "Compare Creta and Seltos and tell me which has better mileage",
    expectIntent: "vehicle_comparison",
    expectMulti: true,
  },
  {
    query: "Show offers and quotation for Safari in Delhi",
    expectIntent: "aci_new_car_quotation",
    expectMulti: true,
  },
  {
    query: "Find Hyundai service center and service cost for Verna",
    expectIntent: "new_car_service_center_search",
    expectMulti: true,
  },

  // Out of scope / internal ops separation
  {
    query: "Loan closure 7077",
    expectIntent: ["loan_closure_pos", "loan_closure"],
  },
  {
    query: "Sell my used car",
    expectIntent: "new_car_unavailable_or_out_of_scope",
    expectInline: true,
  },

  // City fallback transparency
  {
    query: "Verna price in Patna",
    expectIntent: "vehicle_city_price",
    expectFallbackNotice: true,
  },
];

const rowsOfWidget = (widget = {}) => {
  if (!widget) return [];
  if (Array.isArray(widget.rows)) return widget.rows;
  if (Array.isArray(widget.records)) return widget.records;
  if (Array.isArray(widget.options)) return widget.options;
  if (Array.isArray(widget.colors)) return widget.colors;
  if (Array.isArray(widget.models)) return widget.models;
  if (Array.isArray(widget.variants)) return widget.variants;
  if (Array.isArray(widget.data?.rows)) return widget.data.rows;
  if (Array.isArray(widget.data?.records)) return widget.data.records;
  if (Array.isArray(widget.data?.options)) return widget.data.options;
  if (Array.isArray(widget.data?.groupedByModel))
    return widget.data.groupedByModel;
  if (Array.isArray(widget.groupedByModel)) return widget.groupedByModel;
  return [];
};

const matchedCountFor = (response) =>
  (response.widgets || []).reduce((sum, widget) => {
    const directCount =
      widget?.summary?.total ?? widget?.data?.total ?? widget?.total ?? null;

    if (directCount !== null && directCount !== undefined) {
      return sum + (Number(directCount) || 0);
    }

    return sum + rowsOfWidget(widget).length;
  }, response?.ambiguity?.options?.length || 0);

const containsAny = (text = "", patterns = []) =>
  patterns.some((item) =>
    text.toLowerCase().includes(String(item).toLowerCase()),
  );

const hasGroupedModels = (response = {}) => {
  const primary = response.widgets?.[0] || {};

  if (Array.isArray(primary.modelCards) && primary.modelCards.length)
    return true;

  if (Array.isArray(primary.groupedByModel) && primary.groupedByModel.length) {
    return true;
  }

  if (
    Array.isArray(primary.data?.groupedByModel) &&
    primary.data.groupedByModel.length
  ) {
    return true;
  }

  const rows = rowsOfWidget(primary);
  if (!rows.length) return false;

  return rows.every((row) => row.model && !row.variant);
};

const responseRows = (response = {}) => {
  const primary = response.widgets?.[0] || {};
  const rows = [
    ...asArray(primary.rows),
    ...asArray(primary.records),
    ...asArray(primary.modelCards),
    ...asArray(primary.groupedByModel),
    ...asArray(primary.data?.rows),
    ...asArray(primary.data?.records),
    ...asArray(primary.data?.modelCards),
    ...asArray(primary.data?.groupedByModel),
  ];
  const deduped = [];
  const seen = new Set();
  for (const row of rows) {
    const key = JSON.stringify({
      model: row?.model,
      variant: row?.variant,
      id: row?.id,
      price: row?.canonicalOnRoadPrice || row?.onRoadPrice || row?.price,
    });
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(row);
  }
  return deduped;
};

const checkFrontendRegistryCoverage = () => {
  const registryPath =
    process.env.FRONTEND_CANVAS_REGISTRY_PATH ||
    "/Users/gauravgrover/cdb-frontend/src/components/aci-assist/canvasRegistry.js";

  if (!fs.existsSync(registryPath)) {
    return {
      pass: true,
      skipped: true,
      reason: `Frontend registry not found at ${registryPath}`,
      missingCanvas: [],
      missingInline: [],
    };
  }

  const source = fs.readFileSync(registryPath, "utf8");

  const missingCanvas = NEW_CAR_CANVAS_TYPES.filter(
    (type) => !new RegExp(`\\b${type}\\s*:`).test(source),
  );

  const missingInline = NEW_CAR_INLINE_TYPES.filter(
    (type) => !new RegExp(`\\b${type}\\s*:`).test(source),
  );

  return {
    pass: missingCanvas.length === 0 && missingInline.length === 0,
    skipped: false,
    missingCanvas,
    missingInline,
  };
};

const withRetries = async (fn, { attempts = 3, delayMs = 300 } = {}) => {
  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (/^Query timed out after \d+ms/i.test(String(error?.message || ""))) {
        throw error;
      }
      const isRetryable = RETRYABLE_ERROR_REGEX.test(String(error?.message || ""));
      if (!isRetryable || attempt === attempts) throw error;
      await sleep(delayMs * attempt);
    }
  }
  throw lastError;
};

const timedChatWithAgent = async ({
  query,
  sessionId,
  context = {},
  selectedEntity = null,
  filters = {},
  user = adminUser,
  parentQuery = "",
  level = 0,
}) => {
  const startTs = Date.now();
  console.log(
    JSON.stringify(
      {
        marker: "START_QUERY",
        query,
        parentQuery: parentQuery || null,
        level,
        startedAt: new Date(startTs).toISOString(),
      },
      null,
      2,
    ),
  );

  const response = await Promise.race([
    chatWithAgent({
      message: query,
      sessionId,
      context,
      selectedEntity,
      filters,
      user,
    }),
    new Promise((_, reject) =>
      setTimeout(
        () =>
          reject(
            new Error(
              `Query timed out after ${QUERY_TIMEOUT_MS}ms | query="${query}" | parent="${parentQuery || ""}"`,
            ),
          ),
        QUERY_TIMEOUT_MS,
      ),
    ),
  ]);

  const durationMs = Date.now() - startTs;
  console.log(
    JSON.stringify(
      {
        marker: "END_QUERY",
        query,
        parentQuery: parentQuery || null,
        level,
        durationMs,
        detectedIntent: response?.intent || "",
      },
      null,
      2,
    ),
  );

  if (durationMs > QUERY_WARN_MS) {
    console.log(
      JSON.stringify(
        {
          marker: "SLOW_QUERY",
          query,
          parentQuery: parentQuery || null,
          level,
          durationMs,
          detectedIntent: response?.intent || "",
        },
        null,
        2,
      ),
    );
  }

  return response;
};

const validateLine = ({ test, parsed, tool, response }) => {
  void parsed;
  void tool;

  const failures = [];

  const suggestions = Array.isArray(response.conversationSuggestions)
    ? response.conversationSuggestions
    : [];

  const suggestionsDump = suggestions
    .map(
      (item) =>
        `${item.title || ""} ${item.query || ""} ${
          item?.entities?.model || ""
        } ${asArray(item?.entities?.models).join(" ")}`,
    )
    .join(" ")
    .toLowerCase();

  const snapshotModel = String(
    response?.contextSnapshot?.model ||
      response?.contextSnapshot?.anchorModel ||
      response?.context?.model ||
      "",
  ).toLowerCase();

  const snapshotVariant = String(
    response?.contextSnapshot?.variant ||
      response?.contextSnapshot?.anchorVariant ||
      response?.context?.variant ||
      "",
  ).toLowerCase();

  if (test.expectIntent && !intentMatches(response.intent, test.expectIntent)) {
    if (
      !(
        test.allowAmbiguityFallback && response.intent === "vehicle_pricelist"
      ) &&
      !(
        test.allowVariantAmbiguity &&
        response.intent === "vehicle_variant_ambiguity"
      )
    ) {
      failures.push(
        `intent expected ${JSON.stringify(test.expectIntent)}, got ${
          response.intent
        }`,
      );
    }
  }

  if (
    response.intent !== "new_car_unavailable_or_out_of_scope" &&
    !response.canvasType &&
    !response.inlineType
  ) {
    failures.push("structured intent has no canvasType/inlineType");
  }

  if ((response.widgets || []).length === 0) {
    failures.push("no widget returned for structured response");
  }

  const canSkipActions =
    response.intent === "new_car_unavailable_or_out_of_scope" ||
    response.widgets?.some((widget) => widget.type === "unavailable_notice");

  if (!canSkipActions && (response.actions || []).length === 0) {
    failures.push("no actions returned");
  }

  if (
    response.intent !== "new_car_unavailable_or_out_of_scope" &&
    !response.contextSnapshot
  ) {
    failures.push("contextSnapshot missing for structured new-car response");
  }

  if (test.expectConversationSuggestions && suggestions.length === 0) {
    failures.push("conversationSuggestions missing");
  }

  if (test.expectAnchorModel) {
    const expected = String(test.expectAnchorModel).toLowerCase();
    if (!snapshotModel.includes(expected)) {
      failures.push(
        `contextSnapshot.model expected to include ${expected}, got ${
          snapshotModel || "empty"
        }`,
      );
    }
  }

  if (test.expectAnchorVariant) {
    const expected = String(test.expectAnchorVariant).toLowerCase();
    if (!snapshotVariant.includes(expected)) {
      failures.push(
        `contextSnapshot.variant expected to include ${expected}, got ${
          snapshotVariant || "empty"
        }`,
      );
    }
  }

  if (test.assertAnchoredSuggestions && test.expectAnchorModel) {
    const anchor = String(test.expectAnchorModel).toLowerCase();

    const modelScoped = suggestions.filter((item) =>
      [
        "vehicle_pricelist",
        "vehicle_colors",
        "vehicle_feature_answer",
        "vehicle_model_features_explorer",
        "vehicle_emi_calculator",
        "vehicle_offers",
        "aci_new_car_quotation",
      ].includes(item.intent),
    );

    const unanchored = modelScoped.filter((item) => {
      const query = String(item.query || "").toLowerCase();
      const model = String(item?.entities?.model || "").toLowerCase();
      return !query.includes(anchor) && !model.includes(anchor);
    });

    if (unanchored.length) {
      failures.push("found unanchored model-specific suggestions");
    }
  }

  if (Array.isArray(test.disallowSuggestionModels)) {
    for (const banned of test.disallowSuggestionModels) {
      if (new RegExp(`\\b${banned}\\b`, "i").test(suggestionsDump)) {
        failures.push(`suggestions contain unrelated model ${banned}`);
      }

      const bannedRegex = new RegExp(`\\b${banned}\\b`, "i");
      const compareWithBanned = suggestions.filter((item) => {
        if (item.intent !== "vehicle_comparison") return false;
        const query = String(item.query || "").toLowerCase();
        const models = asArray(item?.entities?.models)
          .map((value) => String(value).toLowerCase())
          .join(" ");
        return bannedRegex.test(query) || bannedRegex.test(models);
      });
      if (compareWithBanned.length) {
        failures.push(`comparison suggestion includes banned rival ${banned}`);
      }
    }
  }

  if (test.expectCompareIncludesAnchor && test.expectAnchorModel) {
    const anchor = String(test.expectAnchorModel).toLowerCase();

    const compareSuggestions = suggestions.filter(
      (item) => item.intent === "vehicle_comparison",
    );

    const invalid = compareSuggestions.filter((item) => {
      const query = String(item.query || "").toLowerCase();
      const modelList = asArray(item?.entities?.models).map((model) =>
        String(model).toLowerCase(),
      );
      const models = modelList.join(" ");
      const entityModel = String(item?.entities?.model || "").toLowerCase();
      const dedupedModels = unique(modelList.filter(Boolean));
      const anchorCount = modelList.filter((value) => value === anchor).length;

      if (
        !query.includes(anchor) &&
        !models.includes(anchor) &&
        !entityModel.includes(anchor)
      ) {
        return true;
      }

      if (anchorCount > 1) return true;
      if (modelList.length && dedupedModels.length !== modelList.length) return true;

      return false;
    });

    if (invalid.length) {
      failures.push("comparison suggestion does not include anchor model");
    }
  }

  if (test.expectSuggestionTitles) {
    for (const token of test.expectSuggestionTitles) {
      if (!new RegExp(token, "i").test(suggestionsDump)) {
        failures.push(`expected suggestion token missing: ${token}`);
      }
    }
  }

  if (test.expectVariantInSuggestions && test.expectAnchorVariant) {
    const variant = String(test.expectAnchorVariant).toLowerCase();

    const hasVariant = suggestions.some((item) => {
      const query = String(item.query || "").toLowerCase();
      const v = String(item?.entities?.variant || "").toLowerCase();
      return query.includes(variant) || v.includes(variant);
    });

    if (!hasVariant) {
      failures.push("variant-specific suggestion missing anchor variant");
    }
  }

  if (test.expectLeadSuggestion) {
    const hasLead = suggestions.some(
      (item) =>
        item.type === "lead" && (item.leadType || item?.contextPatch?.leadType),
    );

    if (!hasLead) failures.push("lead suggestion missing");
  }

  if (test.expectLeading && (response.leadingQuestions || []).length === 0) {
    failures.push("leadingQuestions missing for exploratory intent");
  }

  if (test.expectModelGrouped && !hasGroupedModels(response)) {
    failures.push("broad recommendation is not grouped by model");
  }

  if (test.expectFallbackNotice) {
    const notices = (response.widgets || [])
      .flatMap((widget) => [
        ...(Array.isArray(widget.notices) ? widget.notices : []),
        ...(Array.isArray(widget.data?.notices) ? widget.data.notices : []),
        widget?.data?.message,
      ])
      .filter(Boolean)
      .join(" ");

    if (!containsAny(notices, ["showing", "fallback", "instead"])) {
      failures.push("unsupported city fallback notice missing");
    }
  }

  if (test.query.toLowerCase().includes("color")) {
    const modules = (response.sourceTransparency?.modulesChecked || []).map(
      (item) => item.module,
    );

    if (!modules.some((item) => /color/i.test(item))) {
      failures.push("colors query did not use color collection/module trace");
    }
  }

  if (
    test.query.toLowerCase().includes("sunroof") ||
    test.query.toLowerCase().includes("feature")
  ) {
    const modules = (response.sourceTransparency?.modulesChecked || []).map(
      (item) => item.module,
    );

    const hasFeatureModule = modules.some((item) => /feature/i.test(item));

    if (!hasFeatureModule) {
      failures.push(
        "feature query did not use feature collection/module trace",
      );
    }
  }

  if (test.expectInline && response.displayMode === "canvas") {
    failures.push("expected inline response but got canvas mode");
  }

  const actionableSuggestions = suggestions.filter((item) =>
    ["ask", "lead", "select"].includes(item.type),
  );

  const suggestionsWithoutId = actionableSuggestions.filter((item) => !item.id);

  if (suggestionsWithoutId.length) {
    failures.push("conversationSuggestions missing stable id");
  }

  const actionsWithoutId = (response.actions || []).filter((item) => {
    if (item.type === "open_record" || item.type === "edit_record")
      return false;
    return !item.id && item.intent;
  });

  if (actionsWithoutId.length) {
    failures.push("actions missing suggestion id");
  }

  const fullDump = JSON.stringify(response);
  const primaryWidget = response.widgets?.[0] || {};
  const primaryRows = responseRows(response);

  if (
    /(pricelist|price|emi|comparison|variant)/i.test(test.query.toLowerCase()) &&
    primaryRows.length
  ) {
    for (const row of primaryRows.slice(0, 10)) {
      if (
        (row.canonicalOnRoadPrice || row.onRoadPrice || row.price) &&
        !Array.isArray(row.priceBreakupLines)
      ) {
        failures.push("price row missing priceBreakupLines");
        break;
      }
      if ((row.canonicalOnRoadPrice || row.onRoadPrice || row.price) && !row.priceIntegrity) {
        failures.push("price row missing priceIntegrity");
        break;
      }
      if ((row.optionalItems || row.otherItems) && !("optionalItems" in row && "otherItems" in row)) {
        failures.push("price row missing optionalItems/otherItems");
        break;
      }
    }
  }

  if (/show colors of verna/i.test(test.query)) {
    if (primaryWidget.colorAvailabilityLevel !== "model") {
      failures.push("colors response missing model-level availability marker");
    }
    if (primaryWidget.variantWiseAvailabilityAvailable !== false) {
      failures.push("colors response should mark variantWiseAvailabilityAvailable false");
    }
  }

  if (/does verna sx get titan grey/i.test(test.query)) {
    const notices = [
      ...(primaryWidget.notices || []),
      ...(primaryWidget.data?.notices || []),
      primaryWidget.data?.message || "",
    ]
      .join(" ")
      .toLowerCase();
    if (!notices.includes("model-level") || !notices.includes("variant-wise")) {
      failures.push("variant color question should clearly state model-level only");
    }
  }

  if (/latest offers on verna|offers on elevate/i.test(test.query.toLowerCase())) {
    const dump = fullDump.toLowerCase();
    if (!/not stored yet|unavailable/.test(dump)) {
      failures.push("offers response should clearly mark live schemes unavailable");
    }
  }

  if (/nearest hyundai service center/i.test(test.query.toLowerCase())) {
    const dump = fullDump.toLowerCase();
    if (!/not stored yet|service-center.*service-cost data is not stored/.test(dump)) {
      failures.push("service center response should be unavailable and explicit");
    }
  }

  if (/cheapest to own for 5 years/i.test(test.query.toLowerCase())) {
    const missing = asArray(primaryWidget.missingData || primaryWidget.data?.missingData).map(
      (item) => String(item).toLowerCase(),
    );
    for (const key of ["verified_service_cost", "resale_value", "spare_parts_cost"]) {
      if (!missing.includes(key)) failures.push(`tco missingData should include ${key}`);
    }
  }

  if (/which bank gives best loan/i.test(test.query.toLowerCase())) {
    const dump = fullDump.toLowerCase();
    if (!dump.includes("bank-wise finance scheme data is not stored yet")) {
      failures.push("bank-specific finance response should state scheme data unavailable");
    }
  }

  if (
    /get quotation for verna sx|book test drive for verna|call me about seltos/i.test(
      test.query.toLowerCase(),
    )
  ) {
    const leadPayload =
      primaryWidget?.data?.leadPayload ||
      response.actions?.find((item) => item.type === "lead")?.contextPatch?.leadPayload;
    if (!leadPayload) failures.push("leadPayload missing for conversion intent");
  }

  if (/best automatic car under 20 lakh/i.test(test.query.toLowerCase())) {
    const hasManual = primaryRows.some((row) =>
      /\bmanual|mt\b/i.test(String(row.transmission || "")),
    );
    if (hasManual) failures.push("automatic recommendation should avoid manual rows");
  }

  if (/safest suvs under 20l/i.test(test.query.toLowerCase())) {
    const hasReasons = primaryRows.some((row) => asArray(row.reasons).length > 0);
    if (!hasReasons) failures.push("safety results should include reasons");
  }

  if (/cars similar to verna/i.test(test.query.toLowerCase())) {
    const dump = fullDump.toLowerCase();
    if (/(marazzo|scorpio|bolero|innova)/i.test(dump)) {
      failures.push("verna similar cars should not include mpv/suv outliers");
    }
  }

  if (/cars similar to elevate/i.test(test.query.toLowerCase())) {
    const dump = fullDump.toLowerCase();
    if (/(marazzo|ertiga|carens)/i.test(dump)) {
      failures.push("elevate similar cars should not include unrelated mpv rivals");
    }
  }

  if (/automatic, sunroof and 6 airbags under 15 lakh/i.test(test.query.toLowerCase())) {
    const hasFeatureMatch = primaryRows.some(
      (row) =>
        row.scoringSummary?.matchedFeatures ||
        row.matchedFeatures ||
        row.missingFeatures,
    );
    if (!hasFeatureMatch) {
      failures.push("feature-match results should include matched/missing features");
    }
  }

  if (
    containsAny(fullDump, [
      "Himgiri Hyundai",
      "2-4 Weeks",
      "Value Retention",
      "Family Explorer",
    ])
  ) {
    failures.push("dummy data marker detected in response payload");
  }

  return failures;
};

const validateLearningLogged = async (response = {}) => {
  if (!VERIFY_LEARNING) return [];

  const suggestions = Array.isArray(response.conversationSuggestions)
    ? response.conversationSuggestions
    : [];

  if (!suggestions.length) return [];

  const ids = suggestions.map((item) => item.id).filter(Boolean);

  if (!ids.length) return ["conversationSuggestions have no ids for learning"];

  const loggedCount = await SuggestionPerformance.countDocuments({
    suggestionId: { $in: ids },
  });

  return loggedCount ? [] : ["learning impressions were not logged"];
};

const validateChainedResponse = ({ parentQuery, leading, response }) => {
  const failures = [];

  if (!response.intent) {
    failures.push("chain response missing intent");
  }

  if ((response.widgets || []).length === 0) {
    failures.push("chain response returned no widgets");
  }

  const canSkipActions =
    response.intent === "new_car_unavailable_or_out_of_scope" ||
    response.widgets?.some((widget) => widget.type === "unavailable_notice");

  if (!canSkipActions && (response.actions || []).length === 0) {
    failures.push("chain response returned no actions");
  }

  if (
    response.intent !== "new_car_unavailable_or_out_of_scope" &&
    !response.context
  ) {
    failures.push("chain response missing context");
  }

  const query = queryOf(leading);
  if (!query) {
    failures.push("leading question did not have query/message");
  }

  if (!parentQuery) {
    failures.push("parent query missing");
  }

  return failures;
};

const runLeadingChain = async ({
  parentTest,
  parentResponse,
  depth = CHAIN_DEPTH,
  level = 1,
  lineage = [],
}) => {
  if (!depth || level > depth) return [];

  const leadingQuestions = asArray(parentResponse.leadingQuestions).slice(
    0,
    CHAIN_LIMIT,
  );

  const chainLines = [];

  for (const leading of leadingQuestions) {
    const query = queryOf(leading);

    if (!query) {
      chainLines.push({
        parentQuery: parentTest.query,
        level,
        leadingQuestion: compactLeadingQuestion(leading),
        pass: false,
        failureReason: "leading question missing query/message",
      });
      continue;
    }

    const chainContext = {
      ...(parentResponse.context || {}),
      ...(leading.context || {}),
      ...(leading.contextPatch || {}),
      parentIntent: parentResponse.intent,
      chainLineage: [...lineage, parentResponse.intent].filter(Boolean),
    };

    try {
      const response = await withRetries(
        () =>
          timedChatWithAgent({
            query,
            sessionId: `aci-assist-test-chain-${parentTest.id || parentTest.query}`,
            context: chainContext,
            selectedEntity: leading.selectedEntity || null,
            filters: leading.filters || {},
            user: adminUser,
            parentQuery: parentTest.query,
            level,
          }),
        { attempts: 3, delayMs: 350 },
      );

      const failures = validateChainedResponse({
        parentQuery: parentTest.query,
        leading,
        response,
      });

      chainLines.push({
        parentQuery: parentTest.query,
        level,
        leadingLabel: leading.label || leading.title || "",
        leadingQuery: query,
        detectedIntent: response.intent || "",
        displayMode: response.displayMode || "",
        canvasType: response.canvasType || null,
        inlineType: response.inlineType || null,
        matchedCount: matchedCountFor(response),
        actionsCount: (response.actions || []).length,
        conversationSuggestions: asArray(response.conversationSuggestions).map(
          compactSuggestion,
        ),
        leadingQuestions: asArray(response.leadingQuestions).map(
          compactLeadingQuestion,
        ),
        followUpSuggestions: asArray(response.followUpSuggestions).map(textOf),
        pass: failures.length === 0,
        failureReason: failures.join("; "),
      });

      if (level < depth) {
        const childLines = await runLeadingChain({
          parentTest: {
            ...parentTest,
            query,
            id: `${parentTest.id || "case"}-L${level}`,
          },
          parentResponse: response,
          depth,
          level: level + 1,
          lineage: [...lineage, parentResponse.intent].filter(Boolean),
        });

        chainLines.push(...childLines);
      }
    } catch (error) {
      chainLines.push({
        parentQuery: parentTest.query,
        level,
        leadingLabel: leading.label || leading.title || "",
        leadingQuery: query,
        detectedIntent: "error",
        displayMode: "",
        canvasType: null,
        inlineType: null,
        matchedCount: 0,
        actionsCount: 0,
        conversationSuggestions: [],
        leadingQuestions: [],
        followUpSuggestions: [],
        pass: false,
        failureReason: error?.message || "Unhandled chain error",
      });
    }
  }

  return chainLines;
};

const run = async () => {
  await connectDB();

  const registryCoverage = checkFrontendRegistryCoverage();

  if (!registryCoverage.pass) {
    console.log("[FAIL] Frontend canvas registry coverage check");
    console.log(JSON.stringify(registryCoverage, null, 2));
  } else if (registryCoverage.skipped) {
    console.log("[SKIP] Frontend canvas registry coverage check");
    console.log(JSON.stringify(registryCoverage, null, 2));
  } else {
    console.log("[PASS] Frontend canvas registry coverage check");
  }

  const lines = [];
  const chainLines = [];

  for (const test of TEST_CASES) {
    try {
      const parsed = parseAgentMessage(test.query, {}, null, {});
      const tool = getToolForIntent(parsed.intent);

      const response = await withRetries(
        () =>
          timedChatWithAgent({
            query: test.query,
            sessionId: "backend-smoke-test",
            context: {},
            selectedEntity: null,
            filters: {},
            user: adminUser,
            parentQuery: "",
            level: 0,
          }),
        { attempts: 3, delayMs: 350 },
      );

      const failures = validateLine({ test, parsed, tool, response });
      await sleep(150);
      failures.push(...(await validateLearningLogged(response)));

      const line = {
        query: test.query,
        testId: test.id || "",
        expectedIntent: test.expectIntent,
        parsedIntent: parsed.intent,
        detectedIntent: response.intent || parsed.intent,
        displayMode: response.displayMode || "",
        canvasType: response.canvasType || null,
        inlineType: response.inlineType || null,
        selectedTool: tool?.intent || "generic_search",
        collectionsModulesUsed: (
          response.sourceTransparency?.modulesChecked || []
        ).map((item) => item.module),
        matchedCount: matchedCountFor(response),
        conversationSuggestionsCount: asArray(response.conversationSuggestions)
          .length,
        leadingQuestionsCount: asArray(response.leadingQuestions).length,
        followUpSuggestionsCount: asArray(response.followUpSuggestions).length,
        actionsCount: asArray(response.actions).length,
        salesNudgesCount: asArray(response.salesNudges).length,
        context: {
          intent: response.context?.intent,
          lastIntent: response.context?.lastIntent,
          stage: response.context?.stage,
          mode: response.context?.mode,
          model: response.context?.model,
          variant: response.context?.variant,
          city: response.context?.city,
        },
        conversationSuggestions: asArray(response.conversationSuggestions).map(
          compactSuggestion,
        ),
        leadingQuestions: asArray(response.leadingQuestions).map(
          compactLeadingQuestion,
        ),
        followUpSuggestions: asArray(response.followUpSuggestions).map(textOf),
        salesNudges: asArray(response.salesNudges).map((item) => item.title),
        pass: failures.length === 0,
        failureReason: failures.join("; "),
      };

      lines.push(line);
      console.log(JSON.stringify(VERBOSE_LOGS ? line : toCompactLine(line), null, 2));

      const currentChainLines = await runLeadingChain({
        parentTest: test,
        parentResponse: response,
      });

      chainLines.push(...currentChainLines);

      for (const chainLine of currentChainLines) {
        console.log(
          JSON.stringify(
            VERBOSE_LOGS
              ? {
                  chain: true,
                  ...chainLine,
                }
              : {
                  chain: true,
                  parentQuery: chainLine.parentQuery,
                  level: chainLine.level,
                  leadingLabel: chainLine.leadingLabel || "",
                  leadingQuery: chainLine.leadingQuery || "",
                  detectedIntent: chainLine.detectedIntent,
                  pass: chainLine.pass,
                  failureReason: chainLine.failureReason,
                },
            null,
            2,
          ),
        );
      }
    } catch (error) {
      const line = {
        query: test.query,
        testId: test.id || "",
        expectedIntent: test.expectIntent,
        parsedIntent: "error",
        detectedIntent: "error",
        displayMode: "",
        canvasType: null,
        inlineType: null,
        selectedTool: "error",
        collectionsModulesUsed: [],
        matchedCount: 0,
        conversationSuggestionsCount: 0,
        leadingQuestionsCount: 0,
        followUpSuggestionsCount: 0,
        actionsCount: 0,
        salesNudgesCount: 0,
        context: {},
        conversationSuggestions: [],
        leadingQuestions: [],
        followUpSuggestions: [],
        salesNudges: [],
        pass: false,
        failureReason: error?.message || "Unhandled error",
      };

      lines.push(line);
      console.log(JSON.stringify(VERBOSE_LOGS ? line : toCompactLine(line), null, 2));
    }
  }

  const failed = lines.filter((line) => !line.pass).length;
  const passed = lines.length - failed;

  const chainFailed = chainLines.filter((line) => !line.pass).length;
  const chainPassed = chainLines.length - chainFailed;

  const report = {
    total: lines.length,
    passed,
    failed,
    chainTotal: chainLines.length,
    chainPassed,
    chainFailed,
    frontendRegistryCoverage: registryCoverage,
    failedLines: lines.filter((line) => !line.pass),
    failedChainLines: chainLines.filter((line) => !line.pass),
  };

  console.log("\n--- Summary ---");
  console.log(JSON.stringify(report, null, 2));

  await mongoose.connection.close();

  process.exit(failed > 0 || chainFailed > 0 || !registryCoverage.pass ? 1 : 0);
};

run().catch(async (error) => {
  console.error("ACI Assist test harness crashed:", error);
  await mongoose.connection.close().catch(() => {});
  process.exit(1);
});
