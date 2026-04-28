import mongoose from "mongoose";

const QuotationSchema = new mongoose.Schema(
  {
    customer: {
      customerId: { type: mongoose.Schema.Types.ObjectId, ref: "Customer" },
      customerName: String,
      primaryMobile: String,
      email: String,
      residenceAddress: String,
      city: String,
      pincode: String,
    },
    cityTyped: String,
    vehicleCity: String,
    vehicle: {
      vehicleId: { type: mongoose.Schema.Types.ObjectId, ref: "Vehicle" },
      make: String,
      model: String,
      variant: String,
      onRoadPriceList: Number,
    },
    pricing: {
      exShowroom: Number,
      rto: Number,
      insurance: Number,
      tcs: Number,
      epc: Number,
      accessories: Number,
      fastag: Number,
      extendedWarranty: Number,
      additionsOthers: [
        {
          label: String,
          amount: Number,
        },
      ],
      dealerDiscount: Number,
      schemeDiscount: Number,
      insuranceCashback: Number,
      exchange: Number,
      exchangeVehiclePrice: Number,
      loyalty: Number,
      corporate: Number,
      discountsOthers: [
        {
          label: String,
          amount: Number,
        },
      ],
      onRoadBeforeDiscount: Number,
      totalDiscount: Number,
      netOnRoad: Number,
      color: { type: String }, // <- here
    },
    scenarios: {
      A: {
        loanAmount: Number,
        interest: Number,
        tenure: Number,
        tenureType: { type: String, enum: ["years", "months"] },
        emi: Number,
        total: Number,
        principal: Number,
        interestTotal: Number,
        months: Number,
      },
      B: {
        loanAmount: Number,
        interest: Number,
        tenure: Number,
        tenureType: { type: String, enum: ["years", "months"] },
        emi: Number,
        total: Number,
        principal: Number,
        interestTotal: Number,
        months: Number,
      },
    },
    status: {
      type: String,
      enum: ["draft", "sent", "accepted", "lost"],
      default: "draft",
    },
  },
  { timestamps: true },
);

QuotationSchema.index({ createdAt: -1, _id: -1 });
QuotationSchema.index({ updatedAt: -1, _id: -1 });
QuotationSchema.index({ status: 1, updatedAt: -1 });
QuotationSchema.index({ "customer.customerName": 1, updatedAt: -1 });
QuotationSchema.index({ "customer.primaryMobile": 1, updatedAt: -1 });
QuotationSchema.index({ "customer.email": 1, updatedAt: -1 });
QuotationSchema.index({ "vehicle.make": 1, "vehicle.model": 1, "vehicle.variant": 1, updatedAt: -1 });
QuotationSchema.index({ "pricing.color": 1, updatedAt: -1 });

export default mongoose.model("Quotation", QuotationSchema);
