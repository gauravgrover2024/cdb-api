import { formatAciInlineVariantName } from "./aiAgent.earlyFeatureGate.formatters.js";

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


export const polishAciEarlyFeatureResponseCopy = (response = {}, options = {}) => {
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
