import { mapIntentAlias } from "./aiAgent.newCarQuestionMap.js";

const asArray = (value) => (Array.isArray(value) ? value : value ? [value] : []);
const TEN_MINUTES_MS = 10 * 60 * 1000;

const isRecent = (entry) => {
  if (!entry) return false;
  if (typeof entry === "boolean") return entry;
  if (typeof entry !== "object") return false;
  return Boolean(entry.value) && Date.now() - Number(entry.ts || 0) < TEN_MINUTES_MS;
};

export const detectUserStage = (context = {}) => {
  const intent = mapIntentAlias(context.intent || "");
  const history = context.history || {};
  const viewedPrice = isRecent(history.viewedPrice);
  const viewedFeatures = isRecent(history.viewedFeatures);
  const compared = isRecent(history.compared);
  const checkedEmi = isRecent(history.checkedEmi);
  const viewedOffers = isRecent(history.viewedOffers);
  const requestedQuotation = isRecent(history.requestedQuotation);
  const requestedTestDrive = isRecent(history.requestedTestDrive);

  if (
    [
      "aci_new_car_quotation",
      "vehicle_test_drive_request",
      "new_car_loan_enquiry",
    ].includes(intent)
  ) {
    return "closing";
  }

  if (
    [
      "vehicle_emi_calculator",
      "vehicle_emi_options",
      "vehicle_offers",
      "vehicle_offer_lookup",
    ].includes(intent)
  ) {
    return "consideration";
  }

  if (
    [
      "vehicle_comparison",
      "vehicle_model_comparison",
      "vehicle_variant_comparison",
      "vehicle_feature_discovery",
      "vehicle_variant_upgrade_value",
    ].includes(intent)
  ) {
    return "evaluation";
  }

  if (requestedQuotation || requestedTestDrive) return "closing";
  if (checkedEmi || viewedOffers) return "consideration";
  if (viewedPrice || viewedFeatures || compared) return "evaluation";

  return "exploration";
};

export const detectBuyingSignals = (context = {}) => {
  const intent = mapIntentAlias(context.intent || "");
  const history = context.history || {};
  const viewedPrice = isRecent(history.viewedPrice);
  const viewedFeatures = isRecent(history.viewedFeatures);
  const compared = isRecent(history.compared);
  const checkedEmi = isRecent(history.checkedEmi);
  const requestedQuotation = isRecent(history.requestedQuotation);
  const requestedTestDrive = isRecent(history.requestedTestDrive);
  const signals = [];

  if (viewedPrice) signals.push("price_interest");
  if (viewedFeatures) signals.push("feature_interest");
  if (compared) signals.push("comparison_done");
  if (checkedEmi) signals.push("finance_interest");

  if (
    checkedEmi ||
    ["vehicle_emi_calculator", "vehicle_emi_options", "new_car_loan_enquiry"].includes(intent)
  ) {
    signals.push("finance_interest");
  }

  if (
    compared ||
    [
      "vehicle_comparison",
      "vehicle_model_comparison",
      "vehicle_variant_comparison",
      "vehicle_variant_upgrade_value",
      "vehicle_variant_recommendation",
    ].includes(intent)
  ) {
    signals.push("comparison_done");
  }

  if (
    requestedQuotation ||
    requestedTestDrive ||
    ["aci_new_car_quotation", "vehicle_test_drive_request"].includes(intent)
  ) {
    signals.push("ready_to_buy");
  }

  return [...new Set(signals)];
};

export const generateSalesNudges = (context = {}) => {
  const stage = context.stage || detectUserStage(context);
  const model = context.anchorModel || context.model || "this car";
  const signals = asArray(context.buyingSignals || detectBuyingSignals(context));
  const nudges = [];
  const urgency = signals.includes("comparison_done")
    ? " since you've compared options"
    : signals.includes("finance_interest")
      ? " based on your EMI plan"
      : "";

  if (stage === "exploration") {
    nudges.push({
      id: "nudge-explore-next",
      title: `Want me to shortlist the best ${model} variant for your usage?`,
      kind: "advisor",
      tone: "helpful",
      priority: 58,
    });
  }

  if (stage === "evaluation") {
    nudges.push({
      id: "nudge-eval-compare",
      title: `I can quickly show where ${model} wins on features, safety, and ownership.`,
      kind: "advisor",
      tone: "confident",
      priority: 64,
    });
  }

  if (stage === "consideration") {
    nudges.push({
      id: "nudge-consider-budget",
      title: `If you share your comfortable EMI, I can optimize down payment and tenure for ${model}.`,
      kind: "advisor",
      tone: "helpful",
      priority: 72,
    });
  }

  if (signals.includes("ready_to_buy") || stage === "closing") {
    nudges.push({
      id: "nudge-close-now",
      title: `I can prepare your best deal for ${model}${urgency}.`,
      kind: "advisor",
      tone: "urgent",
      priority: 90,
    });
  }

  return nudges.slice(0, 3);
};
