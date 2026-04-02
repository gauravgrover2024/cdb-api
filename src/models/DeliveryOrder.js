import mongoose from 'mongoose';

const deliveryOrderSchema = mongoose.Schema(
  {
    loanId: { type: String, required: true, unique: true }, // One DO per loan usually
    do_loanId: { type: String },

    // Customer snapshot
    customerId: { type: String },
    customerName: { type: String },
    do_customerName: { type: String },
    primaryMobile: { type: String },
    do_primaryMobile: { type: String },
    residenceAddress: { type: String },
    do_residenceAddress: { type: String },
    pincode: { type: String },
    do_pincode: { type: String },
    city: { type: String },
    do_city: { type: String },
    recordSource: { type: String },
    do_recordSource: { type: String },
    sourceName: { type: String },
    do_sourceName: { type: String },
    
    // Dealer Details
    dealerName: { type: String },
    do_dealerName: { type: String },
    dealerAddress: { type: String },
    do_dealerAddress: { type: String },
    dealerMobile: { type: String },
    do_dealerMobile: { type: String },
    dealerContactPerson: { type: String },
    do_dealerContactPerson: { type: String },
    dealerCity: { type: String },
    do_dealerCity: { type: String },
    dealerPincode: { type: String },
    do_dealerPincode: { type: String },
    dealerCode: { type: String },

    // Vehicle Details (Redundant but snapshot)
    vehicleMake: { type: String },
    do_vehicleMake: { type: String },
    vehicleModel: { type: String },
    do_vehicleModel: { type: String },
    vehicleVariant: { type: String },
    do_vehicleVariant: { type: String },
    vehicleColor: { type: String },
    do_vehicleColor: { type: String },
    do_colour: { type: String },
    chassisNumber: { type: String },
    engineNumber: { type: String },

    // Showroom account pricing
    do_exShowroomPrice: { type: Number },
    do_tcs: { type: Number },
    do_epc: { type: Number },
    do_insuranceCost: { type: Number },
    do_roadTax: { type: Number },
    do_accessoriesAmount: { type: Number },
    do_fastag: { type: Number },
    do_extendedWarranty: { type: Number },
    do_additions_others: { type: [mongoose.Schema.Types.Mixed], default: undefined },
    do_marginMoneyPaid: { type: Number },
    do_dealerDiscount: { type: Number },
    do_schemeDiscount: { type: Number },
    do_insuranceCashback: { type: Number },
    do_exchange: { type: Number },
    do_exchangeVehiclePrice: { type: Number },
    do_loyalty: { type: Number },
    do_corporate: { type: Number },
    do_discounts_others: { type: [mongoose.Schema.Types.Mixed], default: undefined },
    do_onRoadVehicleCost: { type: Number },
    do_grossDO: { type: Number },
    do_totalDiscount: { type: Number },
    do_netOnRoadVehicleCost: { type: Number },

    // Customer account pricing
    do_showCustomerVehicleSection: { type: Boolean },
    do_customer_vehicleMake: { type: String },
    do_customer_vehicleModel: { type: String },
    do_customer_vehicleVariant: { type: String },
    do_customer_vehicleColor: { type: String },
    do_customer_colour: { type: String },
    do_customer_exShowroomPrice: { type: Number },
    do_customer_tcs: { type: Number },
    do_customer_epc: { type: Number },
    do_customer_insuranceCost: { type: Number },
    do_customer_actualInsurancePremium: { type: Number },
    do_customer_insuranceBy: { type: String },
    do_customer_insuranceCompanyName: { type: String },
    do_customer_insurancePolicyNumber: { type: String },
    do_customer_insurancePolicyStartDate: { type: Date },
    do_customer_insurancePolicyDurationOD: { type: String },
    do_customer_insurancePolicyEndDateOD: { type: Date },
    do_customer_roadTax: { type: Number },
    do_customer_accessoriesAmount: { type: Number },
    do_customer_fastag: { type: Number },
    do_customer_extendedWarranty: { type: Number },
    do_customer_additions_others: { type: [mongoose.Schema.Types.Mixed], default: undefined },
    do_customer_marginMoneyPaid: { type: Number },
    do_customer_dealerDiscount: { type: Number },
    do_customer_schemeDiscount: { type: Number },
    do_customer_insuranceCashback: { type: Number },
    do_customer_exchange: { type: Number },
    do_customer_vehicleValue: { type: Number },
    do_customer_loyalty: { type: Number },
    do_customer_corporate: { type: Number },
    do_customer_discounts_others: { type: [mongoose.Schema.Types.Mixed], default: undefined },
    do_customer_onRoadVehicleCost: { type: Number },
    do_customer_grossDO: { type: Number },
    do_customer_totalDiscount: { type: Number },
    do_customer_netOnRoadVehicleCost: { type: Number },
    
    // DO Details
    doNumber: { type: String },
    do_refNo: { type: String },
    doDate: { type: Date },
    do_date: { type: Date },
    do_bookingDate: { type: Date },
    validUpto: { type: Date },
    do_accountType: { type: String },
    do_netOffDiscount: { type: Boolean },
    do_insuranceBy: { type: String },
    do_exchangePurchasedBy: { type: String },
    do_hypothecation: { type: String },
    do_loanAmount: { type: Number },
    do_processingFees: { type: Number },
    do_redgRequired: { type: String },
    do_redgCity: { type: String },
    do_exchangeMake: { type: String },
    do_exchangeModel: { type: String },
    do_exchangeVariant: { type: String },
    do_exchangeYear: { type: String },
    do_exchangeRcOwnerName: { type: String },
    do_exchangeRegdNumber: { type: String },
    do_exchangePurchaseDate: { type: Date },
    do_selectedVehicleCost: { type: Number },
    do_selectedGrossDO: { type: Number },
    do_selectedTotalDiscount: { type: Number },
    do_selectedEffectiveTotalDiscount: { type: Number },
    do_selectedDiscountExclVehicleValue: { type: Number },
    do_selectedInsuranceCost: { type: Number },
    do_selectedVehicleValue: { type: Number },
    do_selectedMarginMoney: { type: Number },
    do_insuranceDeduction: { type: Number },
    do_vehicleValueDeduction: { type: Number },
    do_financeDeduction: { type: Number },
    do_netDOAmount: { type: Number },
    
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
deliveryOrderSchema.index({ customerName: 1, updatedAt: -1 });
deliveryOrderSchema.index({ do_customerName: 1, updatedAt: -1 });
deliveryOrderSchema.index({ primaryMobile: 1, updatedAt: -1 });
deliveryOrderSchema.index({ do_primaryMobile: 1, updatedAt: -1 });
deliveryOrderSchema.index({ do_dealerName: 1, updatedAt: -1 });
deliveryOrderSchema.index({ vehicleMake: 1, updatedAt: -1 });
deliveryOrderSchema.index({ do_vehicleModel: 1, updatedAt: -1 });
deliveryOrderSchema.index({ do_vehicleMake: 1, updatedAt: -1 });
deliveryOrderSchema.index({ vehicleVariant: 1, updatedAt: -1 });
deliveryOrderSchema.index({ do_vehicleVariant: 1, updatedAt: -1 });

const DeliveryOrder = mongoose.model('DeliveryOrder', deliveryOrderSchema);

export default DeliveryOrder;
