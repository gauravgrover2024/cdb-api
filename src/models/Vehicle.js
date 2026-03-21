import mongoose from "mongoose";

const vehicleSchema = mongoose.Schema(
  {
    // Core identifiers
    make: { type: String, required: true }, // "Audi", "Hyundai"
    model: { type: String, required: true }, // "A6", "I20 N Line"
    variant: { type: String, required: true }, // "Audi A6 45 TFSI Premium Plus"
    fuel: { type: String }, // legacy
    city: { type: String },

    // Legacy pricing (keep for compat)
    exShowroom: { type: Number, default: 0 },
    rto: { type: Number, default: 0 },
    insurance: { type: Number, default: 0 },
    otherCharges: { type: Number, default: 0 },
    onRoadPrice: { type: Number, default: 0 },

    // NEW: Scraper pricelist fields
    ex_showroom: { type: Number },
    fuel_type: { type: String },
    on_road_price_cardekho: { type: Number },
    orp_without_accessories: { type: Number },
    total_on_road_with_accessories: { type: Number },
    other_tcsCharges: { type: String },
    optional_totalAccessories: { type: Number },
    optional_totalAccessoriesInRs: { type: String },
    other_totalOtherCharges: { type: Number },
    other_totalOtherChargesInRsFormat: { type: String },
    raw_price_json: { type: mongoose.Schema.Types.Mixed },
    variant_short: { type: String },
    is_discontinued: { type: Boolean, default: false },
    LastPriceChangeDate: { type: String },
    LastSeenDate: { type: String },
    scrape_timestamp: { type: String },

    // NEW: Features from scraper (dynamic key-value)
    features: {
      type: mongoose.Schema.Types.Mixed, // "Comfort & Convenience | Power Steering": "Yes"
      default: {},
    },

    // Other
    status: { type: String, default: "Active" },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  },
  {
    timestamps: true,
    strict: false, // allows extra scraper fields
  },
);

// Indexes
vehicleSchema.index(
  { make: 1, model: 1, variant: 1, fuel: 1, city: 1 },
  { unique: true },
);
vehicleSchema.index({ city: 1 });
vehicleSchema.index({ city: 1, make: 1, model: 1 });
vehicleSchema.index({ city: 1, make: 1, model: 1, variant: 1 });
vehicleSchema.index({ city: 1, brand: 1, model: 1 });
vehicleSchema.index({
  make: "text",
  model: "text",
  variant: "text",
  city: "text",
});
vehicleSchema.index({ model: 1 });
vehicleSchema.index({ brand: 1 });
vehicleSchema.index({ brand: 1, model: 1, city: 1 });
vehicleSchema.index({ make: 1, model: 1, city: 1 });
// Distinct dropdown acceleration indexes (make/model/variant with active-state filtering)
vehicleSchema.index({ make: 1, city: 1, is_discontinued: 1 });
vehicleSchema.index({ make: 1, model: 1, city: 1, is_discontinued: 1, variant: 1 });
vehicleSchema.index({ brand: 1, model: 1, city: 1, is_discontinued: 1, variant: 1 });

const Vehicle = mongoose.model("Vehicle", vehicleSchema);
export default Vehicle;
