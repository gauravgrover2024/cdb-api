import mongoose from 'mongoose';

const paymentSchema = mongoose.Schema(
  {
    loanId: { type: String, required: true, unique: true }, // One payment sheet per loan
    
    // Showroom Section
    showroomRows: { type: Array, default: [] },
    entryTotals: { type: Object, default: {} },
    isVerified: { type: Boolean, default: false },

    // Autocredits Section
    autocreditsRows: { type: Array, default: [] },
    autocreditsTotals: { type: Object, default: {} },
    isAutocreditsVerified: { type: Boolean, default: false },

    // Showroom & Channel References
    showroomId: { type: String }, // Reference to Showroom
    showroomName: { type: String },
    channelId: { type: String }, // Reference to Channel/Dealer
    channelName: { type: String },
    channelType: { type: String }, // Dealer, DSA, Broker
    
    // Commission & Excess Payment Tracking
    totalPaymentToShowroom: { type: Number, default: 0 }, // Total paid to showroom
    expectedPaymentToShowroom: { type: Number, default: 0 }, // Expected payment amount
    excessPaymentToShowroom: { type: Number, default: 0 }, // Excess = Total - Expected
    commissionReceivableFromShowroom: { type: Number, default: 0 }, // Commission owed by showroom
    commissionReceivedFromShowroom: { type: Number, default: 0 }, // Commission received
    outstandingCommissionFromShowroom: { type: Number, default: 0 }, // Pending commission
    
    // Channel/Dealer Commission
    channelCommissionPayable: { type: Number, default: 0 }, // Commission owed to channel
    channelCommissionPaid: { type: Number, default: 0 }, // Commission paid
    outstandingChannelCommission: { type: Number, default: 0 }, // Pending commission
    
    // Excess Payment Details
    excessPayments: {
      type: [
        {
          date: { type: Date },
          amount: { type: Number },
          adjustedAgainstCommission: { type: Number, default: 0 },
          remainingExcess: { type: Number },
          remarks: { type: String },
          createdAt: { type: Date, default: Date.now },
        },
      ],
      default: [],
    },
    
    // Commission Adjustments
    commissionAdjustments: {
      type: [
        {
          date: { type: Date },
          type: { type: String, enum: ['Received', 'Adjusted', 'Waived'] },
          amount: { type: Number },
          excessUsed: { type: Number, default: 0 },
          remarks: { type: String },
          createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
          createdAt: { type: Date, default: Date.now },
        },
      ],
      default: [],
    },

    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  {
    timestamps: true,
    strict: false,
  }
);

paymentSchema.index({ updatedAt: -1, _id: -1 });
paymentSchema.index({ createdAt: -1, _id: -1 });
paymentSchema.index({ showroomName: 1, updatedAt: -1 });
paymentSchema.index({ channelName: 1, updatedAt: -1 });
paymentSchema.index({ customerName: 1, updatedAt: -1 });
paymentSchema.index({ primaryMobile: 1, updatedAt: -1 });
paymentSchema.index({ vehicleMake: 1, updatedAt: -1 });
paymentSchema.index({ vehicleModel: 1, updatedAt: -1 });
paymentSchema.index({ vehicleVariant: 1, updatedAt: -1 });
paymentSchema.index({ do_refNo: 1, updatedAt: -1 });
paymentSchema.index({ doNumber: 1, updatedAt: -1 });
paymentSchema.index({ loanId: 1, updatedAt: -1 });

// Calculate outstanding amounts before saving
paymentSchema.pre('save', function () {
  // Calculate outstanding commission from showroom
  this.outstandingCommissionFromShowroom = 
    this.commissionReceivableFromShowroom - this.commissionReceivedFromShowroom;
  
  // Calculate outstanding commission to channel
  this.outstandingChannelCommission = 
    this.channelCommissionPayable - this.channelCommissionPaid;
  
});

const Payment = mongoose.model('Payment', paymentSchema);

export default Payment;
