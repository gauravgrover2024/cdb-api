import { firstMeaningful } from "./aiAgent.normalizers.js";

const DUPLICATE_OPTIONAL_FIELDS = [
  "optional_total",
  "optional_totalAccessories",
  "optional_totalAccessoriesInRs",
  "optional_accessoriesCharges",
  "optional_extendedWarrantyCharges",
  "optional_zeroDepInsuranceCharges",
  "optional_miscellaneouscharges",
];

const DUPLICATE_OTHER_FIELDS = [
  "otherCharges",
  "other_otherCharges",
  "other_totalOtherCharges",
  "other_totalOtherChargesInRsFormat",
  "other_mcdCharges",
  "other_numberPlateCharges",
  "other_smartCardcharges",
  "other_tcsCharges",
];

const asArray = (value) =>
  Array.isArray(value) ? value : value && typeof value === "object" ? Object.values(value) : [];

const priceNumber = (...values) => {
  for (const value of values) {
    if (value === null || value === undefined || value === "") continue;
    if (typeof value === "number" && Number.isFinite(value)) return value;
    const parsed = Number(String(value).replace(/[^0-9.\-]/g, "").trim());
    if (Number.isFinite(parsed)) return parsed;
  }
  return 0;
};

const formatInr = (value = 0) => `₹${Math.round(Number(value) || 0).toLocaleString("en-IN")}`;

const duplicateFieldsHaveValues = (row = {}, keys = []) =>
  keys.some((key) => priceNumber(row[key]) > 0);

const normalizeItems = (items = [], source, fallbackLabel) =>
  asArray(items)
    .map((item, index) => {
      const key = String(firstMeaningful(item?.key, item?._id, `${source}_${index + 1}`));
      const label = String(firstMeaningful(item?.text, item?.key, fallbackLabel)).trim() || fallbackLabel;
      const value = priceNumber(item?.value, item?.amount, item?.price);
      return {
        key,
        label,
        value,
        displayValue: firstMeaningful(item?.price, formatInr(value)),
        source,
      };
    })
    .filter((item) => item.value > 0 || item.label);

export const resolveVehiclePriceBreakup = (row = {}) => {
  const warnings = [];

  const exShowroom = priceNumber(
    row.ex_showroom,
    row.exShowroom,
    row.ex_showroom_price_cardekho,
  );

  const rto = priceNumber(row.rto, row.rto_amount_cardekho);
  const insurance = priceNumber(row.insurance, row.insurance_amount_cardekho);

  const canonicalOnRoadPrice = priceNumber(
    row.onRoadPrice,
    row.total_on_road_with_accessories,
    row.on_road_price_cardekho,
  );

  const optionalItems = normalizeItems(
    row.optional_list,
    "optional_list",
    "Optional charge",
  );
  const otherItems = normalizeItems(row.other_list, "other_list", "Other charge");

  if (!optionalItems.length && duplicateFieldsHaveValues(row, DUPLICATE_OPTIONAL_FIELDS)) {
    warnings.push(
      "Optional duplicate fields exist, but optional_list is missing. Optional charges were not used.",
    );
  }

  if (!otherItems.length && duplicateFieldsHaveValues(row, DUPLICATE_OTHER_FIELDS)) {
    warnings.push(
      "Other duplicate fields exist, but other_list is missing. Other charges were not used.",
    );
  }

  const optionalTotal = optionalItems.reduce((sum, item) => sum + priceNumber(item.value), 0);
  const otherTotal = otherItems.reduce((sum, item) => sum + priceNumber(item.value), 0);

  const computedOnRoad = exShowroom + rto + insurance + optionalTotal + otherTotal;

  const orpWithoutOptional = priceNumber(row.orp_without_accessories) ||
    (canonicalOnRoadPrice ? canonicalOnRoadPrice - optionalTotal : 0);

  const difference = computedOnRoad - canonicalOnRoadPrice;
  const isValid = canonicalOnRoadPrice > 0 ? Math.abs(difference) <= 1 : computedOnRoad > 0;

  if (canonicalOnRoadPrice > 0 && !isValid) {
    warnings.push("Price breakup does not add up to on-road price.");
  }

  const priceBreakupLines = [
    {
      key: "ex_showroom",
      label: "Ex-showroom",
      value: exShowroom,
    },
    {
      key: "rto",
      label: "RTO",
      value: rto,
    },
    {
      key: "insurance",
      label: "Insurance",
      value: insurance,
    },
    {
      key: "optional_charges",
      label: "Optional charges",
      value: optionalTotal,
      children: optionalItems,
    },
    {
      key: "other_charges",
      label: "Other charges",
      value: otherTotal,
      children: otherItems,
    },
    {
      key: "on_road_price",
      label: "On-road price",
      value: canonicalOnRoadPrice,
      computedValue: computedOnRoad,
      isValid,
    },
  ];

  const sourceFields = {
    exShowroom: firstMeaningful(
      row.ex_showroom !== undefined ? "ex_showroom" : "",
      row.exShowroom !== undefined ? "exShowroom" : "",
      row.ex_showroom_price_cardekho !== undefined
        ? "ex_showroom_price_cardekho"
        : "",
    ),
    rto: firstMeaningful(
      row.rto !== undefined ? "rto" : "",
      row.rto_amount_cardekho !== undefined ? "rto_amount_cardekho" : "",
    ),
    insurance: firstMeaningful(
      row.insurance !== undefined ? "insurance" : "",
      row.insurance_amount_cardekho !== undefined
        ? "insurance_amount_cardekho"
        : "",
    ),
    canonicalOnRoadPrice: firstMeaningful(
      row.onRoadPrice !== undefined ? "onRoadPrice" : "",
      row.total_on_road_with_accessories !== undefined
        ? "total_on_road_with_accessories"
        : "",
      row.on_road_price_cardekho !== undefined ? "on_road_price_cardekho" : "",
    ),
    optionalItems: "optional_list",
    otherItems: "other_list",
  };

  const priceIntegrity = {
    isValid,
    difference,
    computedOnRoad,
    canonicalOnRoadPrice,
    warnings,
  };

  return {
    exShowroom,
    rto,
    insurance,
    optionalItems,
    optionalTotal,
    otherItems,
    otherTotal,
    computedOnRoad,
    canonicalOnRoadPrice,
    orpWithoutOptional,
    isValid,
    difference,
    warnings,
    sourceFields,
    priceIntegrity,
    priceBreakupLines,
  };
};

export const resolvePriceBasis = (row = {}, lower = "") => {
  const normalized = String(lower || "").toLowerCase();
  if (/\bex\s*-?\s*showroom\b/.test(normalized)) return "ex_showroom";
  return "on_road";
};
