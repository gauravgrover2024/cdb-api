export const parseAmount = (value) => {
  if (typeof value === "number") return Number.isFinite(value) ? Math.round(value) : 0;
  const text = String(value || "").replace(/,/g, "").trim();
  const match = text.match(/-?\d+(?:\.\d+)?/);
  if (!match) return 0;
  const number = Number(match[0]);
  if (!Number.isFinite(number)) return 0;
  if (/\b(cr|crore|crores)\b/i.test(text)) return Math.round(number * 10000000);
  if (/\b(lakh|lac|lacs|lakhs)\b/i.test(text)) return Math.round(number * 100000);
  return Math.round(number);
};

export const normalizePrice = (row = {}) => {
  const exShowroom = parseAmount(
    row.exShowroomPrice || row.ex_showroom_price || row.exShowroom || row.ex_showroom || 0,
  );
  const onRoad = parseAmount(
    row.onRoadPrice || row.on_road_price || row.total_on_road_with_accessories || 0,
  );

  return {
    exShowroomPrice: exShowroom,
    onRoadPrice: onRoad || exShowroom,
  };
};

export default normalizePrice;
