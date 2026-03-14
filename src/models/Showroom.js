import mongoose from 'mongoose';

const showroomSchema = mongoose.Schema(
  {
    showroomId: { type: String, required: true }, // Custom ID e.g., "SH-2024-001"

    // Basic Information
    name: { type: String, required: true },
    businessName: { type: String },
    registeredName: { type: String },

    // Contact Information
    contactPerson: { type: String },
    mobile: { type: String, required: true },
    email: { type: String },
    alternatePhone: { type: String },

    // Address
    address: { type: String, required: true },
    city: { type: String, required: true },
    state: { type: String },
    pincode: { type: String },

    // Business Details
    gstNumber: { type: String },
    panNumber: { type: String },
    businessType: { type: String }, // Dealership, Independent, etc.
    brands: { type: [String], default: [] }, // Display brand names
    brandKeys: { type: [String], default: [] }, // Canonical normalized brand keys for fast filtering

    // Banking Details
    bankName: { type: String },
    accountNumber: { type: String },
    ifscCode: { type: String },
    accountHolderName: { type: String },

    // Commission & Payment Tracking
    commissionRate: { type: Number, default: 0 },
    totalCommissionReceivable: { type: Number, default: 0 },
    totalCommissionPaid: { type: Number, default: 0 },
    outstandingCommission: { type: Number, default: 0 },

    // Payment History
    paymentHistory: {
      type: [
        {
          date: { type: Date },
          amount: { type: Number },
          excessAmount: { type: Number },
          adjustedAmount: { type: Number },
          loanId: { type: String },
          paymentMode: { type: String },
          remarks: { type: String },
          createdAt: { type: Date, default: Date.now },
        },
      ],
      default: [],
    },

    // Status & Metrics
    status: { type: String, enum: ['Active', 'Inactive', 'Suspended'], default: 'Active' },
    totalLoansProcessed: { type: Number, default: 0 },
    totalBusinessVolume: { type: Number, default: 0 },

    // Agreement Details
    agreementDate: { type: Date },
    agreementExpiryDate: { type: Date },
    agreementDocument: { type: String },

    // Notes & Remarks
    notes: { type: String },

    // Metadata
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
    lastModifiedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  {
    timestamps: true,
  }
);

// Indexes for faster queries
showroomSchema.index({ name: 'text', mobile: 'text', city: 'text' });
showroomSchema.index({ showroomId: 1 });
showroomSchema.index({ mobile: 1 });
showroomSchema.index({ status: 1 });
// Core autosuggest index: status + brandKeys + name
showroomSchema.index({ status: 1, brandKeys: 1, name: 1, city: 1 });

// Auto-generate showroomId before saving
showroomSchema.pre('save', async function () {
  if (!this.showroomId) {
    const year = new Date().getFullYear();
    const count = await mongoose.model('Showroom').countDocuments();
    this.showroomId = `SH-${year}-${String(count + 1).padStart(4, '0')}`;
  }
});

// Calculate outstanding commission before saving
showroomSchema.pre('save', function () {
  this.outstandingCommission = this.totalCommissionReceivable - this.totalCommissionPaid;
});

const Showroom = mongoose.model('Showroom', showroomSchema);

export default Showroom;
