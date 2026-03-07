import mongoose from 'mongoose';

const channelSchema = mongoose.Schema(
  {
    channelId: { type: String, required: true }, // Custom ID e.g., "CH-2024-001"
    
    // Basic Information
    name: { type: String, required: true },
    businessName: { type: String },
    type: { type: String, enum: ['Dealer', 'DSA', 'Broker', 'Direct Agent'], required: true },
    
    // Contact Information
    contactPerson: { type: String, required: true },
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
    aadhaarNumber: { type: String },
    
    // DSA/Dealer Specific
    dsaCode: { type: String }, // DSA identification code
    dealerCode: { type: String }, // Dealer identification code
    territory: { type: String }, // Area/Territory they operate in
    
    // Banking Details
    bankName: { type: String },
    accountNumber: { type: String },
    ifscCode: { type: String },
    accountHolderName: { type: String },
    
    // Commission & Payout Structure
    commissionType: { type: String, enum: ['Percentage', 'Fixed', 'Tiered'], default: 'Percentage' },
    commissionRate: { type: Number, default: 0 }, // Percentage or fixed amount
    payoutPercentage: { type: Number, default: 0 }, // Payout percentage from loan amount
    
    // Commission Tracking
    totalCommissionEarned: { type: Number, default: 0 },
    totalCommissionPaid: { type: Number, default: 0 },
    outstandingCommission: { type: Number, default: 0 },
    
    // Payout History
    payoutHistory: {
      type: [
        {
          date: { type: Date },
          loanId: { type: String },
          loanAmount: { type: Number },
          commissionAmount: { type: Number },
          payoutAmount: { type: Number },
          paymentMode: { type: String },
          utrNumber: { type: String },
          status: { type: String, enum: ['Pending', 'Paid', 'Cancelled'], default: 'Pending' },
          remarks: { type: String },
          createdAt: { type: Date, default: Date.now },
        },
      ],
      default: [],
    },
    
    // Performance Metrics
    status: { type: String, enum: ['Active', 'Inactive', 'Suspended', 'Blacklisted'], default: 'Active' },
    totalLeadsGenerated: { type: Number, default: 0 },
    totalLoansApproved: { type: Number, default: 0 },
    totalLoansClosed: { type: Number, default: 0 },
    conversionRate: { type: Number, default: 0 }, // Percentage
    totalBusinessVolume: { type: Number, default: 0 },
    
    // Agreement Details
    agreementDate: { type: Date },
    agreementExpiryDate: { type: Date },
    agreementDocument: { type: String }, // URL to agreement document
    
    // KYC Documents
    panCardUrl: { type: String },
    aadhaarCardUrl: { type: String },
    gstCertificateUrl: { type: String },
    cancelledChequeUrl: { type: String },
    
    // Rating & Reviews
    rating: { type: Number, min: 0, max: 5, default: 0 },
    totalReviews: { type: Number, default: 0 },
    
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
channelSchema.index({ name: 'text', mobile: 'text', city: 'text', type: 'text' });
channelSchema.index({ channelId: 1 });
channelSchema.index({ mobile: 1 });
channelSchema.index({ type: 1 });
channelSchema.index({ status: 1 });
channelSchema.index({ dsaCode: 1 });
channelSchema.index({ dealerCode: 1 });

// Auto-generate channelId before saving
channelSchema.pre('save', async function () {
  if (!this.channelId) {
    const year = new Date().getFullYear();
    const count = await mongoose.model('Channel').countDocuments();
    this.channelId = `CH-${year}-${String(count + 1).padStart(4, '0')}`;
  }
});

// Calculate outstanding commission and conversion rate before saving
channelSchema.pre('save', function () {
  this.outstandingCommission = this.totalCommissionEarned - this.totalCommissionPaid;
  
  if (this.totalLeadsGenerated > 0) {
    this.conversionRate = ((this.totalLoansApproved / this.totalLeadsGenerated) * 100).toFixed(2);
  }
});

const Channel = mongoose.model('Channel', channelSchema);

export default Channel;
