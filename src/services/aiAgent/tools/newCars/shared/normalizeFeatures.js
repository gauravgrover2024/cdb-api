import { cleanText } from "./normalizeVehicle.js";

export const normalizeFeatures = (row = {}) => {
  const list = Array.isArray(row.features)
    ? row.features
    : Array.isArray(row.keyFeatures)
      ? row.keyFeatures
      : [];

  return list
    .map((item) => (typeof item === "string" ? cleanText(item) : cleanText(item?.name || item?.label)))
    .filter(Boolean);
};

export default normalizeFeatures;
