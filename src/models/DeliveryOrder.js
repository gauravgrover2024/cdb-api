import mongoose from 'mongoose';

const deliveryOrderSchema = mongoose.Schema(
  {
    loanId: { type: String, required: true, unique: true }, // One DO per loan usually
    
    // Dealer Details
    dealerName: { type: String },
    dealerAddress: { type: String },
    dealerCode: { type: String },

    // Vehicle Details (Redundant but snapshot)
    vehicleModel: { type: String },
    vehicleColor: { type: String },
    chassisNumber: { type: String },
    engineNumber: { type: String },
    
    // DO Details
    doNumber: { type: String },
    doDate: { type: Date },
    validUpto: { type: Date },
    
    status: { type: String, default: 'Generated' }, // Generated, Delivered, Cancelled

    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  {
    timestamps: true,
    strict: false, // Allow all fields from frontend
  }
);

deliveryOrderSchema.index({ createdAt: -1, _id: -1 });
deliveryOrderSchema.index({ updatedAt: -1, _id: -1 });
deliveryOrderSchema.index({ status: 1, updatedAt: -1 });
deliveryOrderSchema.index({ dealerName: 1, updatedAt: -1 });
deliveryOrderSchema.index({ do_loanId: 1 });
deliveryOrderSchema.index({ do_loanId: 1, updatedAt: -1 });
deliveryOrderSchema.index({ loanId: 1, updatedAt: -1 });
deliveryOrderSchema.index({ do_refNo: 1, updatedAt: -1 });
deliveryOrderSchema.index({ doNumber: 1, updatedAt: -1 });
deliveryOrderSchema.index({ vehicleModel: 1, updatedAt: -1 });

const DeliveryOrder = mongoose.model('DeliveryOrder', deliveryOrderSchema);

export default DeliveryOrder;
