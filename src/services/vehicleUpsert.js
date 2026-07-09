import { createSuggestionTerm } from "./vehicleSuggestionTermService.js";

// A make/model/variant missing from the scraped `vehicles` collection is
// recorded as a manual suggestion term (see VehicleSuggestionTerm), not as a
// new `vehicles` row — that collection is scraper-owned and would silently
// discontinue anything it doesn't recognize from a run.
export const syncVehicleFromInsurancePayload = async (payload = {}) => {
  const make = String(payload.vehicleMake || "").trim();
  const model = String(payload.vehicleModel || "").trim();
  const variant = String(payload.vehicleVariant || "").trim();

  if (!make || !model || !variant) return null;

  try {
    const result = await createSuggestionTerm({
      level: "variant",
      make,
      model,
      variant,
    });
    if (!result.matchedExisting) {
      console.log(`[Insurance] Recorded manual vehicle term: ${make} ${model} ${variant}`);
    }
    return result;
  } catch (err) {
    console.warn("[Insurance] Vehicle sync failed:", err?.message);
    return null;
  }
};
