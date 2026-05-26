import Vehicle from "../models/Vehicle.js";

export const syncVehicleFromInsurancePayload = async (payload = {}) => {
  try {
    const make = String(payload.vehicleMake || "").trim();
    const model = String(payload.vehicleModel || "").trim();
    const variant = String(payload.vehicleVariant || "").trim();

    if (!make || !model || !variant) return null;

    // Check if it exists
    const existing = await Vehicle.findOne({ make, model, variant });
    if (existing) return existing;

    // Upsert
    const vehicle = await Vehicle.create({
      make,
      model,
      variant,
      brand: make, // assuming brand = make as fallback
      fuel: payload.fuelType || "Unknown",
    });
    
    console.log(`[Insurance] Auto-ingested missing vehicle: ${make} ${model} ${variant}`);
    return vehicle;
  } catch (err) {
    console.warn("[Insurance] Vehicle sync failed:", err?.message);
    return null;
  }
};
