// backend/models/Booking.js
import mongoose from "mongoose";

const bookingSchema = new mongoose.Schema(
  {
    bookingId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },

    // Status + linking
    status: {
      type: String,
      enum: ["Open", "Converted", "Cancelled"],
      default: "Open",
      index: true,
    },
    linkedLoanId: {
      type: String,
      default: null,
    },
    linkedPaymentLoanId: {
      type: String,
      default: null,
    },

    // Lead source
    leadSourceType: {
      type: String,
      enum: ["Direct", "Indirect"],
      default: "Direct",
    },
    directSourceName: String,
    dealerName: String,
    dealerAddress: String,
    dealerMobile: String,

    // Vehicle
    vehicleMake: String,
    vehicleModel: String,
    vehicleVariant: String,
    vehicleColor: String,
    mfgYear: Number,
    regCity: String,

    // Customer
    customerName: String,
    sdwOf: String,
    customerPhone: String,

    // Pricing & finance
    exShowroomPrice: Number,
    dealerDiscount: {
      type: Number,
      default: 0,
    },
    manufacturerDiscount: {
      type: Number,
      default: 0,
    },
    otherDiscounts: {
      type: Number,
      default: 0,
    },
    financeRequired: Number,

    // Showroom contact
    showroomName: String,
    showroomContactPerson: String,
    showroomContactNumber: String,
    showroomAddress: String,

    // Booking amount/payment
    bookingAmount: Number,
    bookingDate: Date,
    bookingPaymentMode: String,
    bookingBankName: String,
    bookingTxnRef: String,
    bookingRemarks: String,

    // Exchange
    exchangePresent: {
      type: String,
      enum: ["Yes", "No"],
      default: "No",
    },
    exchangeMake: String,
    exchangeModel: String,
    exchangeYear: Number,

    // Cancellation
    cancelReason: String,
    cancelRemarks: String,
    cancelledAt: Date,
  },
  {
    timestamps: true, // createdAt, updatedAt
  },
);

export default mongoose.model("Booking", bookingSchema);
