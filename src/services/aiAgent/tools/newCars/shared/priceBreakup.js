import { formatMoney } from "../../shared/pricing.js";

const asArray = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) return value.filter(Boolean);
  if (typeof value === "object") return Object.values(value).filter(Boolean);
  return [];
};

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const slugify = (value = "", fallback = "item") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || fallback;

export const parsePriceAmount = (...values) => {
  for (const value of values) {
    if (value === null || value === undefined || value === "") continue;

    if (typeof value === "number" && Number.isFinite(value)) {
      return Math.round(value);
    }

    const text = String(value || "").replace(/,/g, "").trim();
    const match = text.match(/-?\d+(?:\.\d+)?/);
    if (!match) continue;

    const number = Number(match[0]);
    if (!Number.isFinite(number)) continue;

    if (/\b(cr|crore|crores)\b/i.test(text)) return Math.round(number * 10000000);
    if (/\b(lakh|lac|lacs|lakhs)\b/i.test(text)) return Math.round(number * 100000);

    return Math.round(number);
  }

  return 0;
};

const normalizeChargeItem = (item = {}, index = 0, section = "other") => {
  const label =
    cleanText(item.text || item.label || item.name || item.title || item.key) ||
    (section === "optional" ? `Optional charge ${index + 1}` : `Other charge ${index + 1}`);

  const amount = parsePriceAmount(item.value, item.amount, item.price);

  return {
    key: slugify(`${section}-${item.key || label || index}`, `${section}-${index + 1}`),
    label,
    amount,
    value: amount,
    formatted: amount ? formatMoney(amount) : "",
    displayValue: amount ? formatMoney(amount) : "",
    rawDisplayValue: item.price || "",
    type: section,
    source: section === "optional" ? "optional_list" : "other_list",
    selectedByDefault: false,
    raw: item,
  };
};

export const normalizeChargeItems = (items = [], section = "other") =>
  asArray(items)
    .map((item, index) => normalizeChargeItem(item, index, section))
    .filter((item) => item.label && item.amount > 0);

export const buildV2PriceBreakup = (row = {}) => {
  const raw = row.raw || row;

  const exShowroom = parsePriceAmount(
    row.exShowroomPrice,
    row.ex_showroom,
    row.exShowroom,
    raw.ex_showroom,
    raw.exShowroom,
    raw.ex_showroom_price_cardekho,
  );

  const rto = parsePriceAmount(
    row.rto,
    row.rtoCharges,
    raw.rto,
    raw.rto_amount_cardekho,
  );

  const insurance = parsePriceAmount(
    row.insurance,
    row.insuranceAmount,
    raw.insurance,
    raw.insurance_amount_cardekho,
  );

  const optionalItems = normalizeChargeItems(
    row.optional_list || raw.optional_list,
    "optional",
  );

  const otherItems = normalizeChargeItems(
    row.other_list || raw.other_list,
    "other",
  );

  const optionalTotalFromItems = optionalItems.reduce(
    (sum, item) => sum + Number(item.amount || 0),
    0,
  );

  const otherTotalFromItems = otherItems.reduce(
    (sum, item) => sum + Number(item.amount || 0),
    0,
  );

  const explicitOptionalTotal = parsePriceAmount(
    row.optional_total,
    row.optional_totalAccessories,
    raw.optional_total,
    raw.optional_totalAccessories,
    raw.optional_totalAccessoriesInRs,
  );

  const optionalTotal = explicitOptionalTotal || 0;
  const effectiveOptionalItems = optionalTotal ? optionalItems : [];

  const otherTotal =
    otherTotalFromItems ||
    parsePriceAmount(
      row.otherCharges,
      row.other_totalOtherCharges,
      raw.otherCharges,
      raw.other_totalOtherCharges,
      raw.other_totalOtherChargesInRsFormat,
    );

  const mandatoryChargesTotal = rto + insurance + otherTotal;
  const otherCharges = optionalTotal + otherTotal;

  const computedOnRoadWithoutOptional = exShowroom + mandatoryChargesTotal;
  const computedOnRoadWithOptional = computedOnRoadWithoutOptional + optionalTotal;

  const explicitOnRoadWithOptional = parsePriceAmount(
    row.total_on_road_with_accessories,
    row.on_road_price_cardekho,
    raw.on_road_price_cardekho,
    raw.total_on_road_with_accessories,
  );
  const genericOnRoadPrice = parsePriceAmount(
    row.onRoadPrice,
    row.on_road_price,
    raw.onRoadPrice,
    raw.on_road_price,
  );
  const dbOnRoadWithOptional =
    explicitOnRoadWithOptional ||
    (!optionalTotal ? genericOnRoadPrice : 0);

  const dbOnRoadWithoutOptional = parsePriceAmount(
    row.orp_without_accessories,
    row.ORPWithoutOptionAccessoriesDoubleType,
    raw.orp_without_accessories,
    raw.ORPWithoutOptionAccessoriesDoubleType,
  );

  const hasMandatoryComponents = Boolean(exShowroom || rto || insurance || otherTotal);
  const onRoadPriceWithoutOptional =
    dbOnRoadWithoutOptional ||
    (hasMandatoryComponents ? computedOnRoadWithoutOptional : 0) ||
    (!optionalTotal ? dbOnRoadWithOptional : 0) ||
    (optionalTotal ? genericOnRoadPrice : 0);
  const onRoadPriceWithOptional =
    dbOnRoadWithOptional ||
    (onRoadPriceWithoutOptional ? onRoadPriceWithoutOptional + optionalTotal : 0);
  const onRoadPrice = onRoadPriceWithoutOptional || onRoadPriceWithOptional;
  const difference = computedOnRoadWithOptional - onRoadPriceWithOptional;
  const incomplete = !dbOnRoadWithoutOptional && !hasMandatoryComponents && Boolean(dbOnRoadWithOptional);
  const isValid = onRoadPriceWithOptional > 0
    ? Math.abs(difference) <= 1 || incomplete
    : computedOnRoadWithOptional > 0;

  const detailSections = [
    {
      key: "optional_list",
      label: "Optional charges",
      amount: optionalTotal,
      total: optionalTotal,
      displayValue: optionalTotal ? formatMoney(optionalTotal) : "",
      items: effectiveOptionalItems,
      selectedByDefault: false,
    },
    {
      key: "other_list",
      label: "Other charges",
      amount: otherTotal,
      total: otherTotal,
      displayValue: otherTotal ? formatMoney(otherTotal) : "",
      items: otherItems,
    },
  ].filter((section) => section.items.length || section.amount > 0);

  const allOtherChargeItems = [
    ...effectiveOptionalItems,
    ...otherItems,
  ];

  const visibleLines = [
    {
      key: "ex_showroom",
      label: "Ex-showroom",
      amount: exShowroom,
      value: exShowroom,
      displayValue: exShowroom ? formatMoney(exShowroom) : "",
    },
    {
      key: "rto",
      label: "RTO",
      amount: rto,
      value: rto,
      displayValue: rto ? formatMoney(rto) : "",
    },
    {
      key: "insurance",
      label: "Insurance",
      amount: insurance,
      value: insurance,
      displayValue: insurance ? formatMoney(insurance) : "",
    },
    {
      key: "other_charges",
      label: "Other charges",
      amount: otherTotal,
      value: otherTotal,
      displayValue: otherTotal ? formatMoney(otherTotal) : "",
      hasDetails: allOtherChargeItems.length > 0,
      items: allOtherChargeItems,
      children: allOtherChargeItems,
      detailSections,
    },
  ];

  return {
    exShowroom,
    rto,
    insurance,

    optionalTotal,
    optionalItems: effectiveOptionalItems,

    otherTotal,
    otherItems,

    otherCharges,
    otherChargeItems: allOtherChargeItems,
    mandatoryChargesTotal,

    visibleLines,
    detailSections,

    computedOnRoadPrice: computedOnRoadWithOptional,
    computedOnRoadWithoutOptional,
    computedOnRoadWithOptional,
    canonicalOnRoadPrice: onRoadPrice,
    onRoadPrice,
    onRoadPriceWithoutOptional,
    onRoadPriceWithOptional,
    difference,
    isValid,
    incomplete,

    contract: {
      priceBasis: "ex_showroom_plus_mandatory_charges",
      city: cleanText(row.city || raw.city),
      currency: "INR",
      incomplete,
      exShowroom: {
        key: "ex_showroom",
        label: "Ex-showroom",
        value: exShowroom,
        formatted: exShowroom ? formatMoney(exShowroom) : "",
      },
      mandatoryCharges: {
        total: mandatoryChargesTotal,
        formatted: mandatoryChargesTotal ? formatMoney(mandatoryChargesTotal) : "",
        items: [
          {
            key: "rto",
            label: "RTO",
            value: rto,
            amount: rto,
            formatted: rto ? formatMoney(rto) : "",
          },
          {
            key: "insurance",
            label: "Insurance",
            value: insurance,
            amount: insurance,
            formatted: insurance ? formatMoney(insurance) : "",
          },
          ...otherItems.map((item) => ({
            ...item,
            type: "mandatory",
            selectedByDefault: true,
          })),
        ].filter((item) => Number(item.value || item.amount || 0) > 0),
      },
      optionalCharges: {
        selectedByDefault: false,
        total: optionalTotal,
        formatted: optionalTotal ? formatMoney(optionalTotal) : "",
        items: effectiveOptionalItems.map((item) => ({
          ...item,
          selectedByDefault: false,
        })),
      },
      totals: {
        onRoadWithoutOptional: onRoadPriceWithoutOptional,
        onRoadWithoutOptionalFormatted: onRoadPriceWithoutOptional ? formatMoney(onRoadPriceWithoutOptional) : "",
        onRoadWithOptional: onRoadPriceWithOptional,
        onRoadWithOptionalFormatted: onRoadPriceWithOptional ? formatMoney(onRoadPriceWithOptional) : "",
        optionalDelta: optionalTotal,
        optionalDeltaFormatted: optionalTotal ? formatMoney(optionalTotal) : "",
      },
    },

    priceIntegrity: {
      isValid,
      difference,
      incomplete,
      computedOnRoad: computedOnRoadWithOptional,
      computedOnRoadWithoutOptional,
      computedOnRoadWithOptional,
      canonicalOnRoadPrice: onRoadPrice,
      formula: "ex_showroom + rto + insurance + other_list + optional_list",
      warnings: [
        ...(incomplete ? ["Only aggregate on-road price is available; breakup fields are incomplete."] : []),
        ...(!isValid ? ["Computed on-road price does not match database onRoadPrice."] : []),
      ],
    },

    tooltip: {
      title: "Other charges",
      amount: otherCharges,
      displayValue: otherCharges ? formatMoney(otherCharges) : "",
      sections: detailSections,
      items: allOtherChargeItems,
    },
  };
};

export default buildV2PriceBreakup;
