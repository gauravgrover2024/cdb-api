export const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

export const normalizeVehicle = (row = {}) => {
  const make = cleanText(row.make || row.brand || row.manufacturer || "");
  const model = cleanText(row.model || row.modelName || row.vehicleModel || "");
  const displayName =
    cleanText(row.displayName || row.name || `${make} ${model}`) || model || make;

  return {
    ...row,
    make,
    brand: cleanText(row.brand || make),
    model,
    displayName,
    city: cleanText(row.city || row.location || "new-delhi") || "new-delhi",
    imageUrl: cleanText(
      row.normalizedImageUrl ||
        row.cleanImageUrl ||
        row.normalized_image_url ||
        row.imageUrl ||
        row.image_url ||
        row.sourceImageUrl ||
        "",
    ),
    normalizedImageUrl: cleanText(
      row.normalizedImageUrl || row.cleanImageUrl || row.normalized_image_url || "",
    ),
    sourceImageUrl: cleanText(row.imageUrl || row.image_url || row.sourceImageUrl || ""),
  };
};

export default normalizeVehicle;
