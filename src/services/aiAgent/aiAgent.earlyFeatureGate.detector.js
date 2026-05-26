import { formatAciInlineVariantName } from "./aiAgent.earlyFeatureGate.formatters.js";
import {
  ACI_DYNAMIC_FEATURE_CATEGORY_MAP,
  buildAciDynamicFeatureCleanUserMessage,
  extractAciDynamicComparisonVariants,
  getAciDynamicFeatureAlias,
} from "./aiAgent.earlyFeatureGate.languageMap.js";

/**
 * Early feature gate detector.
 *
 * Important:
 * - This module may use language aliases to understand user intent.
 * - It must not contain factual vehicle truth.
 * - Actual model truth comes from DB-backed resolver before this detector is called.
 * - Actual feature availability comes from deterministic feature tools after detection.
 */

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const detectAciDynamicFeatureCategory = (message = "") => {
  const raw = String(message || "");
  return ACI_DYNAMIC_FEATURE_CATEGORY_MAP.find((entry) =>
    entry.pattern.test(raw),
  ) || null;
};


export const detectAciEarlyDynamicRoutedRequest = ({ message = "", modelEntity = null } = {}) => {
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

export const shouldSkipAciEarlyFeatureGate = (message = "") => {
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
