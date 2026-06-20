import mongoose from "mongoose";

const usedCarSchema = mongoose.Schema(
  {
    make: { type: String, required: true, index: true },
    model: { type: String, required: true, index: true },
    variant: { type: String, required: true, index: true },
    year: { type: Number, required: true, index: true },
    fuel_type: { type: String, index: true },
    transmission: { type: String, index: true },
    cc: { type: Number },
    mileage: { type: String },
    seating_capacity: { type: String },
    body_type: { type: String },
    is_active: { type: Boolean, default: true },
    is_discontinued: { type: Boolean, default: false },
    start_year: { type: Number },
    end_year: { type: Number },
    model_generation: { type: String },
    carwale_make_slug: { type: String },
    carwale_model_slug: { type: String },
    carwale_version_id: { type: Number },
    ex_showroom_price: { type: Number },
  },
  {
    timestamps: true,
    collection: "used_cars",
  }
);

// Unique index to prevent duplicate variant listings for the same year
usedCarSchema.index(
  { make: 1, model: 1, variant: 1, year: 1 },
  { unique: true }
);

// Index compound lookups commonly used in filtering dropdowns
usedCarSchema.index({ make: 1, model: 1 });
usedCarSchema.index({ make: 1, model: 1, variant: 1 });

const UsedCar = mongoose.model("UsedCar", usedCarSchema);
export default UsedCar;
