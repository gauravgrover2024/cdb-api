import { cleanText } from "./normalizeVehicle.js";

export const normalizeVariant = (row = {}) => ({
  ...row,
  variant: cleanText(
    row.variant || row.variantName || row.variant_short || row.trim || row.name || "",
  ),
  fuelType: cleanText(row.fuelType || row.fuel || row.fuel_type || ""),
  transmission: cleanText(row.transmission || row.transmissionType || row.gearbox || ""),
});

export default normalizeVariant;
