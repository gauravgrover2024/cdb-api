import mongoose from "mongoose";

const vehicleRecordSchema = mongoose.Schema(
  {
    loanId: { type: String, index: true, sparse: true },
    customerId: { type: mongoose.Schema.Types.ObjectId, ref: "Customer", index: true },
    customerName: { type: String },
    primaryMobile: { type: String },

    registrationNumber: { type: String },
    registrationNumberNormalized: { type: String, index: true },
    make: { type: String, index: true },
    model: { type: String, index: true },
    variant: { type: String, index: true },
    cubicCapacityCc: { type: Number },
    engineNumber: { type: String },
    chassisNumber: { type: String },
    manufactureMonth: { type: String },
    yearOfManufacture: { type: String },
    registrationDate: { type: Date },
    hypothecation: { type: String },
    registrationCity: { type: String },

    sourceLoanType: { type: String },
    sourceCaseType: { type: String },
    sourceLoanUpdatedAt: { type: Date },
    lastSyncedAt: { type: Date, default: Date.now },
  },
  {
    timestamps: true,
    collection: "vehicle_master_records",
  },
);

vehicleRecordSchema.index({ loanId: 1 }, { unique: true, sparse: true });
vehicleRecordSchema.index({ registrationNumberNormalized: 1 });
vehicleRecordSchema.index({ make: 1, model: 1, variant: 1 });

const VehicleRecord = mongoose.model("VehicleRecord", vehicleRecordSchema);

export default VehicleRecord;
