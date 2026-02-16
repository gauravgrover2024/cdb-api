import mongoose from 'mongoose';

const vehicleSchema = mongoose.Schema(
  {
    make: { type: String, required: true },
    model: { type: String, required: true },
    variant: { type: String, required: true },
    fuel: { type: String },
    exShowroom: { type: Number, default: 0 },
    rto: { type: Number, default: 0 },
    insurance: { type: Number, default: 0 },
    otherCharges: { type: Number, default: 0 },
    onRoadPrice: { type: Number, default: 0 },
    city: { type: String },
    status: { type: String, default: 'Active' },
    isDiscontinued: { type: Boolean, default: false },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  {
    timestamps: true,
    strict: false,
  }
);

// Index for search and uniqueness
vehicleSchema.index({ make: 1, model: 1, variant: 1, fuel: 1, city: 1 }, { unique: true });
vehicleSchema.index({ make: 'text', model: 'text', variant: 'text', city: 'text' });

const Vehicle = mongoose.model('Vehicle', vehicleSchema);

export default Vehicle;
