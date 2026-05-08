/**
 * Shared pricing helpers for ACI Assist V2 tools.
 */

export const formatMoney = (value) => {
  const number = Number(value || 0);

  if (!Number.isFinite(number) || number <= 0) return "";

  if (number >= 10000000) {
    return `₹${(number / 10000000).toFixed(number % 10000000 === 0 ? 0 : 2)}Cr`;
  }

  if (number >= 100000) {
    return `₹${(number / 100000).toFixed(number % 100000 === 0 ? 0 : 2)}L`;
  }

  return `₹${Math.round(number).toLocaleString("en-IN")}`;
};

export const sortPriceRows = (rows = [], ranking = "") => {
  const normalized = [...rows];

  if (ranking === "price_high_to_low") {
    return normalized.sort(
      (a, b) =>
        (b.onRoadPrice || b.exShowroomPrice || 0) -
        (a.onRoadPrice || a.exShowroomPrice || 0),
    );
  }

  return normalized.sort(
    (a, b) =>
      (a.onRoadPrice || a.exShowroomPrice || 0) -
      (b.onRoadPrice || b.exShowroomPrice || 0),
  );
};

export const buildPriceSummary = (rows = []) => {
  const prices = rows
    .map((row) => row.onRoadPrice || row.exShowroomPrice)
    .filter((value) => value > 0);

  if (!prices.length) return {};

  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);

  return {
    minPrice,
    maxPrice,
    minPriceLabel: formatMoney(minPrice),
    maxPriceLabel: formatMoney(maxPrice),
    rowCount: rows.length,
  };
};
