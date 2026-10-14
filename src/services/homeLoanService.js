/**
 * HomeLoan Service
 *
 * Handles the transformation between the frontend's flat form payload
 * and the hierarchical MongoDB document structure.
 *
 * KEY OPTIMIZATION: 20 cheques × 9 fields (180 flat keys) are collapsed
 * into a single `postFile.instruments[]` array.
 */

import HomeLoan from "../models/HomeLoan.js";
import HomeLoanBreakupField from "../models/HomeLoanBreakupField.js";
import mongoose from "mongoose";

// ─── Flat → Document (inbound transform) ─────────────────────────────────────

export function flatToDoc(flat) {
  if (!flat || typeof flat !== "object") return {};
  const f = flat;

  // ── Instruments: collapse cheque_N_* flat fields ────────────────────────
  // Only rebuilt when the payload actually carries cheque fields, so a
  // partial save (notes, KYC docs, delivery patch) never wipes instruments.
  const chequeSlots = Object.keys(f)
    .map((key) => /^cheque_(\d+)_/.exec(key))
    .filter(Boolean)
    .map((match) => Number(match[1]));
  let instruments;
  if (chequeSlots.length) {
    instruments = [];
    const maxSlot = Math.max(...chequeSlots);
    for (let i = 1; i <= maxSlot; i++) {
      const number = f[`cheque_${i}_number`];
      const amount = f[`cheque_${i}_amount`];
      if (number || amount) {
        instruments.push({
          slot: i,
          tag: f[`cheque_${i}_tag`] ?? null,
          number: number ?? null,
          bankName: f[`cheque_${i}_bankName`] ?? null,
          accountNumber: f[`cheque_${i}_accountNumber`] ?? null,
          date: f[`cheque_${i}_date`] ?? null,
          amount: amount ?? null,
          signedBy: f[`cheque_${i}_signedBy`] ?? null,
          favouring: f[`cheque_${i}_favouring`] ?? null,
          imageUrl: f[`cheque_${i}_image`] ?? null,
        });
      }
    }
  }

  // ── Applicant (primary) ─────────────────────────────────────────────────
  const applicant = {
    customerId: f.customerId || undefined,
    customerName: f.customerName,
    motherName: f.motherName,
    fatherName: f.sdwOf,
    dob: f.dob,
    gender: f.gender,
    maritalStatus: f.maritalStatus,
    dependents: f.dependents,
    education: f.education,
    houseType: f.houseType,
    pan: f.panNumber,
    aadhaar: f.aadhaarNumber || f.aadharNumber,
    passport: f.passportNumber,
    dlNumber: f.dlNumber,
    identityProofType: f.identityProofType,
    identityProofNumber: f.identityProofNumber,
    identityProofExpiry: f.identityProofExpiry,
    addressProofType: f.addressProofType,
    addressProofNumber: f.addressProofNumber,
    primaryMobile: f.primaryMobile,
    extraMobiles: f.extraMobiles,
    email: f.email,
    officialEmail: f.officialEmail,
    addressType: f.addressType,
    address: {
      line1: f.residenceAddress,
      pincode: f.pincode,
      city: f.city,
      state: f.state,
      district: f.district,
      area: f.area,
    },
    permanentAddress: {
      line1: f.permanentAddress,
      pincode: f.permanentPincode,
      city: f.permanentCity,
      state: f.permanentState,
      district: f.permanentDistrict,
      area: f.permanentArea,
    },
    sameAsCurrentAddress: f.sameAsCurrentAddress,
    yearsInCurrentCity: f.yearsInCurrentCity,
    yearsInCurrentHouse: f.yearsInCurrentHouse,
    // Occupational
    occupationType: f.occupationType,
    isMSME: f.isMSME,
    professionalType: f.professionalType,
    companyType: f.companyType,
    businessNature: Array.isArray(f.businessNature)
      ? f.businessNature
      : f.businessNature
      ? [f.businessNature]
      : undefined,
    experienceCurrent: f.experienceCurrent,
    totalExperience: f.totalExperience,
    designation: f.designation,
    companyName: f.companyName,
    employmentAddress: f.employmentAddress,
    employmentPincode: f.employmentPincode,
    employmentCity: f.employmentCity,
    employmentPhone: f.employmentPhone,
    // Company-specific
    contactPersonName: f.contactPersonName,
    contactPersonMobile: f.contactPersonMobile,
    companyPartners: f.companyPartners,
    // Banking
    banking: {
      bankName: f.bankName,
      accountNumber: f.accountNumber,
      ifsc: f.ifsc || f.ifscCode,
      branch: f.branch,
      accountType: f.accountType,
      accountSinceYears: f.accountSinceYears,
      openedIn: f.openedIn,
    },
    additionalBankDetails: f.additionalBankDetails,
    hasAdditionalBankDetails: f.hasAdditionalBankDetails,
    // Income
    grossSalary: f.grossSalary,
    netSalary: f.netSalary,
    totalIncome: f.totalIncome,
    totalTurnoverGST: f.totalTurnoverGST,
  };

  // ── Co-Applicant (co_ prefix) ───────────────────────────────────────────
  const coApplicant = f.hasCoApplicant
    ? {
        customerId: f.co_id || undefined,
        customerName: f.co_customerName,
        motherName: f.co_motherName,
        fatherName: f.co_fatherName,
        dob: f.co_dob,
        gender: f.co_gender,
        maritalStatus: f.co_maritalStatus,
        houseType: f.co_houseType,
        pan: f.co_pan,
        aadhaar: f.co_aadhaar,
        primaryMobile: f.co_primaryMobile,
        address: {
          line1: f.co_address,
          pincode: f.co_pincode,
          city: f.co_city,
        },
        applicantCategory: f.co_applicantCategory,
        occupationType: f.co_occupation,
        professionalType: f.co_professionalType,
        companyType: f.co_companyType,
        businessNature: f.co_businessNature,
        designation: f.co_designation,
        experienceCurrent: f.co_currentExperience,
        totalExperience: f.co_totalExperience,
        companyName: f.co_companyName,
        companyAddress: f.co_companyAddress,
        companyPincode: f.co_companyPincode,
        companyCity: f.co_companyCity,
        companyPhone: f.co_companyPhone,
        totalIncome: f.co_totalIncome,
        totalTurnoverGST: f.co_totalTurnoverGST,
        yearsAtCurrentResidence: f.co_yearsAtCurrentResidence,
        relationWithFirm: f.co_relationWithFirm,
      }
    : undefined;

  // ── Additional Co-Applicants (Firm applicants only) ─────────────────────
  const coApplicants = Array.isArray(f.coApplicants) ? f.coApplicants : undefined;

  // ── Guarantor (gu_ prefix) ──────────────────────────────────────────────
  const guarantor = f.hasGuarantor
    ? {
        customerName: f.gu_customerName,
        fatherName: f.gu_fatherName,
        dob: f.gu_dob,
        gender: f.gu_gender,
        pan: f.gu_pan,
        aadhaar: f.gu_aadhaar,
        primaryMobile: f.gu_primaryMobile,
        address: {
          line1: f.gu_address,
          pincode: f.gu_pincode,
          city: f.gu_city,
        },
        occupationType: f.gu_occupation,
        companyName: f.gu_companyName,
        totalIncome: f.gu_totalIncome,
        relationship: f.gu_relationship,
      }
    : undefined;

  // ── Authorised Signatory (signatory_ prefix) ────────────────────────────
  const authorisedSignatory =
    f.hasAuthorisedSignatory || f.signatory_name
      ? {
          customerName: f.signatory_name,
          designation: f.signatory_designation,
          pan: f.signatory_pan,
          dob: f.signatory_dob,
          primaryMobile: f.signatory_mobile,
        }
      : undefined;

  // ── Approval (approval_ prefix) ─────────────────────────────────────────
  const approval = {
    status: f.approval_status,
    bankId: f.approval_bankId,
    bankName: f.approval_bankName,
    roi: f.approval_roi,
    tenureMonths: f.approval_tenureMonths,
    loanAmountApproved: f.approval_loanAmountApproved,
    loanAmountDisbursed: f.approval_loanAmountDisbursed,
    processingFees: f.approval_processingFees,
    approvalDate: f.approval_approvalDate,
    disbursedDate: f.approval_disbursedDate,
    breakup: {
      netLoanApproved: f.approval_breakup_netLoanApproved,
      insuranceFinance: f.approval_breakup_insuranceFinance,
      ewFinance: f.approval_breakup_ewFinance,
      creditAssured: f.approval_breakup_creditAssured,
    },
    banks: Array.isArray(f.approval_banksData) ? f.approval_banksData : undefined,
    statusHistory: Array.isArray(f.approval_statusHistory)
      ? f.approval_statusHistory
      : undefined,
  };

  // ── Post-file (postfile_ / dispatch_ / rc_ / invoice_ prefix) ───────────
  const postFile = {
    locked: f.__postfileLocked,
    instruments,
    aadhaarCardDocUrl: f.aadhaarCardDocUrl,
    aadhaarCardBackDocUrl: f.aadhaarCardBackDocUrl,
    panCardDocUrl: f.panCardDocUrl,
    passportDocUrl: f.passportDocUrl,
    passportBackDocUrl: f.passportBackDocUrl,
    dlDocUrl: f.dlDocUrl,
    addressProofDocUrl: f.addressProofDocUrl,
    gstDocUrl: f.gstDocUrl,
    gstDocUrlPage2: f.gstDocUrlPage2,
    gstDocUrlPage3: f.gstDocUrlPage3,
    incomeDocUrl: f.incomeDocUrl,
    itrDocUrl: f.itrDocUrl,
    bankStatementDocUrl: f.bankStatementDocUrl,
    firstEmiDate: f.postfile_firstEmiDate,
    emiAmount: f.postfile_emiAmount,
    emiStartDate: f.postfile_emiStartDate,
    invoiceDate: f.invoice_date,
    invoiceReceivedDate: f.invoice_received_date,
    invoiceNumber: f.invoiceNumber,
    rcNumber: f.rcNumber,
    rcRegdDate: f.rc_redg_date,
    rcReceivedDate: f.rc_received_date,
    rcInvStorageNumber: f.rcInvStorageNumber,
    dispatchDate: f.dispatch_date,
    dispatchTime: f.dispatch_time,
    dispatchCourier: f.dispatch_courier,
    dispatchTracking: f.dispatch_tracking,
    dispatchRemarks: f.dispatch_remarks,
    principalOutstandingAmount: f.principalOutstandingAmount,
    principalOutstandingDate: f.principalOutstandingDate,
    vehicleVerified: f.vehicleVerified,
    verificationRemarks: f.verificationRemarks,
    approvalDate: f.approval_approvalDate,
    disbursedDate: f.approval_disbursedDate,
    loanAmountApproved: f.approval_loanAmountApproved,
    roi: f.approval_roi,
    tenureMonths: f.approval_tenureMonths,
    processingFees: f.approval_processingFees,
  };

  // ── Vehicle ─────────────────────────────────────────────────────────────
  const vehicle = {
    make: f.vehicleMake,
    model: f.vehicleModel,
    variant: f.vehicleVariant,
    fuelType: f.vehicleFuelType,
    regNo: f.vehicleRegNo,
    boughtInYear: f.boughtInYear,
    usage: f.usage,
    valuation: f.valuation,
    hypothecation: f.hypothecation,
    hypothecationBank: f.hypothecationBank,
    purposeOfLoan: f.purposeOfLoan,
    registrationCity: f.registrationCity,
    registrationAddress: f.registrationAddress,
    registrationPincode: f.registrationPincode,
    registerSameAsAadhaar: f.registerSameAsAadhaar,
    registerSameAsPermanent: f.registerSameAsPermanent,
  };

  // ── Pricing ─────────────────────────────────────────────────────────────
  // onRoadPrice/grossLoan/netLoan are derived from the merged document in
  // recomputePricing(), so a partial payload can't zero them out.
  const pricing = {
    exShowroomPrice: f.exShowroomPrice,
    insuranceCost: f.insuranceCost,
    roadTax: f.roadTax,
    accessoriesAmount: f.accessoriesAmount,
    accessories: f.accessories,
    additionsOthers: f.additionsOthers,
    dealerDiscount: f.dealerDiscount,
    manufacturerDiscount: f.manufacturerDiscount,
    marginMoney: f.marginMoney,
    advanceEmi: f.advanceEmi,
    tradeInValue: f.tradeInValue,
    otherDiscounts: f.otherDiscounts,
  };

  // ── Dealer ──────────────────────────────────────────────────────────────
  const dealer = {
    name: f.showroomDealerName,
    contactPerson: f.showroomDealerContactPerson,
    contactNumber: f.showroomDealerContactNumber,
    address: f.showroomDealerAddress,
  };

  // ── References ────────────────────────────────────────────────────────────
  // The form sends nested reference1/reference2 objects (Personal or Trade);
  // older clients sent flat reference<N>_* keys, which are still accepted.
  const readReference = (n) => {
    const nested = f[`reference${n}`];
    if (nested && typeof nested === "object" && !Array.isArray(nested)) return nested;
    const legacy = {
      name: f[`reference${n}_name`],
      mobile: f[`reference${n}_mobile`],
      address: f[`reference${n}_address`],
      pincode: f[`reference${n}_pincode`],
      city: f[`reference${n}_city`],
      relation: f[`reference${n}_relation`],
    };
    return Object.values(legacy).some((v) => v !== undefined) ? legacy : undefined;
  };
  const reference1 = readReference(1);
  const reference2 = readReference(2);

  // ── Lead / Sourcing ──────────────────────────────────────────────────────
  const lead = {
    sourcingChannel: f.sourcingChannel,
    dsaCode: f.dsaCode,
    leadId: f.leadId,
    dsaId: f.dsaId,
    salesExecutive: f.salesExecutive,
    leadDate: f.leadDate,
    leadTime: f.leadTime,
    source: f.source,
    recordSource: f.recordSource,
    sourceName: f.sourceName,
    sourceDetails: f.sourceDetails,
    dealerName: f.dealerName,
    dealerAddress: f.dealerAddress,
    dealerMobile: f.dealerMobile,
    dealtBy: f.dealtBy,
    payoutApplicable: f.payoutApplicable,
    payoutPercentage: f.prefile_sourcePayoutPercentage,
    referenceDetails: f.referenceDetails,
    docsPreparedBy: f.docsPreparedBy,
  };

  // ── Delivery (delivery_ prefix) ──────────────────────────────────────────
  const delivery = {
    dealerName: f.delivery_dealerName,
    dealerContactPerson: f.delivery_dealerContactPerson,
    dealerContactNumber: f.delivery_dealerContactNumber,
    dealerAddress: f.delivery_dealerAddress,
    deliveryDate: f.delivery_date,
    notes: f.delivery_notes,
    initialized: f.__deliveryInitialized,
  };

  // ── Payout ───────────────────────────────────────────────────────────────
  const payout = {
    billNumber: f.billNumber,
    billDate: f.billDate,
  };

  // ── Disbursement ─────────────────────────────────────────────────────────
  const disbursement = {
    date: f.disbursement_date,
    time: f.disbursement_time,
    amount: f.disbursement_amount,
    mode: f.disbursement_mode,
    referenceNumber: f.disbursement_reference,
    remarks: f.disbursement_remarks,
  };

  return {
    // Application level
    applicantType: f.applicantType,
    caseType: f.caseType,
    customerType: f.customerType,
    customerName: f.customerName,
    typeOfLoan: f.typeOfLoan,
    unsecuredLoanAmount: f.unsecuredLoanAmount,
    unsecuredLoanBreakup: Array.isArray(f.unsecuredLoanBreakup) ? f.unsecuredLoanBreakup : undefined,
    currentStep: f.currentStep ?? f.currentStage,
    completedSteps: f.completedSteps,
    customerId: f.customerId,
    status: f.status,
    approxClosureDate: f.approxClosureDate,
    // Nested sections
    lead,
    applicant,
    coApplicant,
    coApplicants,
    guarantor,
    authorisedSignatory,
    vehicle,
    pricing,
    dealer,
    reference1,
    reference2,
    approval,
    postFile,
    disbursement,
    delivery,
    payout,
  };
}

// ─── Document → Flat (outbound transform) ────────────────────────────────────

export function docToFlat(doc) {
  if (!doc) return null;
  const d = doc.toObject ? doc.toObject({ virtuals: false }) : { ...doc };

  const a = d.applicant || {};
  const co = d.coApplicant || {};
  const gu = d.guarantor || {};
  const sg = d.authorisedSignatory || {};
  const v = d.vehicle || {};
  const pr = d.pricing || {};
  const dl = d.dealer || {};
  const ap = d.approval || {};
  const pf = d.postFile || {};
  const le = d.lead || {};
  const dv = d.delivery || {};
  const pb = d.payout || {};
  const ds = d.disbursement || {};
  const x =
    d.formExtras && typeof d.formExtras === "object" ? d.formExtras : {};

  // Expand instruments → cheque_N_* flat fields
  const chequeFlat = {};
  if (Array.isArray(pf.instruments)) {
    pf.instruments.forEach((inst, idx) => {
      const n = inst.slot || idx + 1;
      chequeFlat[`cheque_${n}_tag`] = inst.tag;
      chequeFlat[`cheque_${n}_number`] = inst.number;
      chequeFlat[`cheque_${n}_bankName`] = inst.bankName;
      chequeFlat[`cheque_${n}_accountNumber`] = inst.accountNumber;
      chequeFlat[`cheque_${n}_date`] = inst.date;
      chequeFlat[`cheque_${n}_amount`] = inst.amount;
      chequeFlat[`cheque_${n}_signedBy`] = inst.signedBy;
      chequeFlat[`cheque_${n}_favouring`] = inst.favouring;
      chequeFlat[`cheque_${n}_image`] = inst.imageUrl;
    });
  }

  const mapped = {
    _id: d._id,
    applicationNumber: d.applicationNumber,
    loanId: d.loanId,
    customerId: d.customerId,
    status: d.status,
    currentStep: d.currentStep,
    completedSteps: d.completedSteps,
    applicantType: d.applicantType,
    caseType: d.caseType,
    customerType: d.customerType,
    typeOfLoan: d.typeOfLoan,
    unsecuredLoanAmount: d.unsecuredLoanAmount,
    unsecuredLoanBreakup: d.unsecuredLoanBreakup,
    approxClosureDate: d.approxClosureDate,
    createdAt: d.createdAt,
    updatedAt: d.updatedAt,
    currentStage: x.currentStage ?? d.currentStep,
    isBulk: d.isBulk,
    isBulkOriginal: d.isBulkOriginal,
    bulkCount: d.bulkCount,
    bulkIndex: d.bulkIndex,
    bulkGroupId: d.bulkGroupId,
    bulkParentId: d.bulkParentId,

    // Lead
    sourcingChannel: le.sourcingChannel,
    dsaCode: le.dsaCode,
    leadId: le.leadId,
    dsaId: le.dsaId,
    salesExecutive: le.salesExecutive,
    leadDate: le.leadDate,
    leadTime: le.leadTime,
    source: le.source,
    recordSource: le.recordSource,
    sourceName: le.sourceName,
    sourceDetails: le.sourceDetails,
    dealerName: le.dealerName,
    dealerAddress: le.dealerAddress,
    dealerMobile: le.dealerMobile,
    dealtBy: le.dealtBy,
    payoutApplicable: le.payoutApplicable,
    prefile_sourcePayoutPercentage: le.payoutPercentage,
    referenceDetails: le.referenceDetails,
    docsPreparedBy: le.docsPreparedBy,

    // Primary applicant
    customerName: a.customerName,
    motherName: a.motherName,
    sdwOf: a.fatherName,
    dob: a.dob,
    gender: a.gender,
    maritalStatus: a.maritalStatus,
    dependents: a.dependents,
    education: a.education,
    houseType: a.houseType,
    addressType: a.addressType,
    panNumber: a.pan,
    aadhaarNumber: a.aadhaar,
    passportNumber: a.passport,
    dlNumber: a.dlNumber,
    identityProofType: a.identityProofType,
    identityProofNumber: a.identityProofNumber,
    identityProofExpiry: a.identityProofExpiry,
    addressProofType: a.addressProofType,
    addressProofNumber: a.addressProofNumber,
    primaryMobile: a.primaryMobile,
    extraMobiles: a.extraMobiles,
    email: a.email,
    officialEmail: a.officialEmail,
    residenceAddress: a.address?.line1,
    pincode: a.address?.pincode,
    city: a.address?.city,
    state: a.address?.state,
    district: a.address?.district,
    area: a.address?.area,
    permanentAddress: a.permanentAddress?.line1,
    permanentPincode: a.permanentAddress?.pincode,
    permanentCity: a.permanentAddress?.city,
    permanentState: a.permanentAddress?.state,
    permanentDistrict: a.permanentAddress?.district,
    permanentArea: a.permanentAddress?.area,
    sameAsCurrentAddress: a.sameAsCurrentAddress,
    yearsInCurrentCity: a.yearsInCurrentCity,
    yearsInCurrentHouse: a.yearsInCurrentHouse,
    contactPersonName: a.contactPersonName,
    contactPersonMobile: a.contactPersonMobile,
    // Occupational
    occupationType: a.occupationType,
    isMSME: a.isMSME,
    professionalType: a.professionalType,
    companyType: a.companyType,
    businessNature: a.businessNature,
    experienceCurrent: a.experienceCurrent,
    totalExperience: a.totalExperience,
    designation: a.designation,
    companyName: a.companyName,
    employmentAddress: a.employmentAddress,
    employmentPincode: a.employmentPincode,
    employmentCity: a.employmentCity,
    employmentPhone: a.employmentPhone,
    companyPartners: a.companyPartners,
    // Banking
    bankName: a.banking?.bankName,
    accountNumber: a.banking?.accountNumber,
    ifsc: a.banking?.ifsc,
    ifscCode: a.banking?.ifsc,
    branch: a.banking?.branch,
    accountType: a.banking?.accountType,
    accountSinceYears: a.banking?.accountSinceYears,
    openedIn: a.banking?.openedIn,
    additionalBankDetails: a.additionalBankDetails,
    hasAdditionalBankDetails: a.hasAdditionalBankDetails,
    // Income
    grossSalary: a.grossSalary,
    netSalary: a.netSalary,
    totalIncome: a.totalIncome,
    totalTurnoverGST: a.totalTurnoverGST,

    // Co-Applicant (co_ prefix)
    hasCoApplicant: x.hasCoApplicant ?? !!co.customerName,
    co_id: co.customerId,
    co_customerName: co.customerName,
    co_motherName: co.motherName,
    co_fatherName: co.fatherName,
    co_dob: co.dob,
    co_gender: co.gender,
    co_maritalStatus: co.maritalStatus,
    co_houseType: co.houseType,
    co_pan: co.pan,
    co_aadhaar: co.aadhaar,
    co_primaryMobile: co.primaryMobile,
    co_address: co.address?.line1,
    co_pincode: co.address?.pincode,
    co_city: co.address?.city,
    co_applicantCategory: co.applicantCategory,
    co_occupation: co.occupationType,
    co_professionalType: co.professionalType,
    co_companyType: co.companyType,
    co_businessNature: co.businessNature,
    co_designation: co.designation,
    co_currentExperience: co.experienceCurrent,
    co_totalExperience: co.totalExperience,
    co_companyName: co.companyName,
    co_companyAddress: co.companyAddress,
    co_companyPincode: co.companyPincode,
    co_companyCity: co.companyCity,
    co_companyPhone: co.companyPhone,
    co_totalIncome: co.totalIncome,
    co_totalTurnoverGST: co.totalTurnoverGST,
    co_yearsAtCurrentResidence: co.yearsAtCurrentResidence,
    co_relationWithFirm: co.relationWithFirm,
    coApplicants: d.coApplicants,

    // Guarantor (gu_ prefix)
    hasGuarantor: x.hasGuarantor ?? !!gu.customerName,
    gu_customerName: gu.customerName,
    gu_fatherName: gu.fatherName,
    gu_dob: gu.dob,
    gu_gender: gu.gender,
    gu_pan: gu.pan,
    gu_aadhaar: gu.aadhaar,
    gu_primaryMobile: gu.primaryMobile,
    gu_address: gu.address?.line1,
    gu_pincode: gu.address?.pincode,
    gu_city: gu.address?.city,
    gu_occupation: gu.occupationType,
    gu_companyName: gu.companyName,
    gu_totalIncome: gu.totalIncome,
    gu_relationship: gu.relationship,

    // Authorised Signatory
    signatory_name: sg.customerName,
    signatory_designation: sg.designation,
    signatory_pan: sg.pan,
    signatory_dob: sg.dob,
    signatory_mobile: sg.primaryMobile,

    // Vehicle
    vehicleMake: v.make,
    vehicleModel: v.model,
    vehicleVariant: v.variant,
    vehicleFuelType: v.fuelType,
    vehicleRegNo: v.regNo,
    boughtInYear: v.boughtInYear,
    usage: v.usage,
    valuation: v.valuation,
    hypothecation: v.hypothecation,
    hypothecationBank: v.hypothecationBank,
    purposeOfLoan: v.purposeOfLoan,
    registrationCity: v.registrationCity,
    registrationAddress: v.registrationAddress,
    registrationPincode: v.registrationPincode,
    registerSameAsAadhaar: v.registerSameAsAadhaar,
    registerSameAsPermanent: v.registerSameAsPermanent,

    // Pricing
    exShowroomPrice: pr.exShowroomPrice,
    insuranceCost: pr.insuranceCost,
    roadTax: pr.roadTax,
    accessoriesAmount: pr.accessoriesAmount,
    accessories: pr.accessories,
    additionsOthers: pr.additionsOthers,
    dealerDiscount: pr.dealerDiscount,
    manufacturerDiscount: pr.manufacturerDiscount,
    marginMoney: pr.marginMoney,
    advanceEmi: pr.advanceEmi,
    tradeInValue: pr.tradeInValue,
    otherDiscounts: pr.otherDiscounts,

    // Dealer
    showroomDealerName: dl.name,
    showroomDealerContactPerson: dl.contactPerson,
    showroomDealerContactNumber: dl.contactNumber,
    showroomDealerAddress: dl.address,

    // References
    reference1: d.reference1 || undefined,
    reference2: d.reference2 || undefined,

    // Approval
    approval_status: ap.status,
    approval_bankId: ap.bankId,
    approval_bankName: ap.bankName,
    approval_roi: ap.roi,
    approval_tenureMonths: ap.tenureMonths,
    approval_loanAmountApproved: ap.loanAmountApproved,
    approval_loanAmountDisbursed: ap.loanAmountDisbursed,
    approval_processingFees: ap.processingFees,
    approval_approvalDate: ap.approvalDate,
    approval_disbursedDate: ap.disbursedDate,
    approval_breakup_netLoanApproved: ap.breakup?.netLoanApproved,
    approval_breakup_insuranceFinance: ap.breakup?.insuranceFinance,
    approval_breakup_ewFinance: ap.breakup?.ewFinance,
    approval_breakup_creditAssured: ap.breakup?.creditAssured,
    approval_banksData: ap.banks,
    approval_statusHistory: ap.statusHistory,

    // Post-file
    __postfileLocked: pf.locked,
    aadhaarCardDocUrl: pf.aadhaarCardDocUrl,
    aadhaarCardBackDocUrl: pf.aadhaarCardBackDocUrl,
    panCardDocUrl: pf.panCardDocUrl,
    passportDocUrl: pf.passportDocUrl,
    passportBackDocUrl: pf.passportBackDocUrl,
    dlDocUrl: pf.dlDocUrl,
    addressProofDocUrl: pf.addressProofDocUrl,
    gstDocUrl: pf.gstDocUrl,
    gstDocUrlPage2: pf.gstDocUrlPage2,
    gstDocUrlPage3: pf.gstDocUrlPage3,
    incomeDocUrl: pf.incomeDocUrl,
    itrDocUrl: pf.itrDocUrl,
    bankStatementDocUrl: pf.bankStatementDocUrl,
    postfile_firstEmiDate: pf.firstEmiDate,
    postfile_emiAmount: pf.emiAmount,
    postfile_emiStartDate: pf.emiStartDate,
    invoice_date: pf.invoiceDate,
    invoice_received_date: pf.invoiceReceivedDate,
    invoiceNumber: pf.invoiceNumber,
    rcNumber: pf.rcNumber,
    rc_redg_date: pf.rcRegdDate,
    rc_received_date: pf.rcReceivedDate,
    rcInvStorageNumber: pf.rcInvStorageNumber,
    dispatch_date: pf.dispatchDate,
    dispatch_time: pf.dispatchTime,
    dispatch_courier: pf.dispatchCourier,
    dispatch_tracking: pf.dispatchTracking,
    dispatch_remarks: pf.dispatchRemarks,
    principalOutstandingAmount: pf.principalOutstandingAmount,
    principalOutstandingDate: pf.principalOutstandingDate,
    vehicleVerified: pf.vehicleVerified,
    verificationRemarks: pf.verificationRemarks,
    ...chequeFlat,

    // Disbursement
    disbursement_date: ds.date,
    disbursement_time: ds.time,
    disbursement_amount: ds.amount,
    disbursement_mode: ds.mode,
    disbursement_reference: ds.referenceNumber,
    disbursement_remarks: ds.remarks,

    // Delivery
    delivery_dealerName: dv.dealerName,
    delivery_dealerContactPerson: dv.dealerContactPerson,
    delivery_dealerContactNumber: dv.dealerContactNumber,
    delivery_dealerAddress: dv.dealerAddress,
    delivery_date: dv.deliveryDate,
    delivery_notes: dv.notes,
    __deliveryInitialized: dv.initialized,

    // Payout
    billNumber: pb.billNumber,
    billDate: pb.billDate,

    // Relations
    pendencies: d.pendencies,
    documents: d.documents,
    notes: d.notes,
    workflowHistory: d.workflowHistory,
  };

  // Unmapped form fields first; structured paths are the source of truth for
  // everything they cover.
  return { ...x, ...mapped };
}

// ─── Unmapped form fields (formExtras) ────────────────────────────────────────

// Flat keys flatToDoc reads, discovered once by running it over a proxy that
// answers every key - so this can never drift from the mapping above.
const MAPPED_FLAT_KEYS = (() => {
  const seen = new Set();
  const probe = new Proxy(
    {},
    {
      get(_target, key) {
        if (typeof key === "string") seen.add(key);
        return "1";
      },
      has: () => true,
      ownKeys: () => [],
    },
  );
  flatToDoc(probe);
  return seen;
})();

// Read by flatToDoc but also kept verbatim, because their stored value can't
// be inferred back from the structured document.
const ALWAYS_KEPT_FLAT_KEYS = new Set([
  "hasCoApplicant",
  "hasGuarantor",
  "hasAuthorisedSignatory",
  "currentStage",
]);

// Server-owned or derived on read - never accepted from a form payload.
const SERVER_OWNED_FLAT_KEYS = new Set([
  "_id",
  "__v",
  "id",
  "applicationNumber",
  "loanId",
  "createdAt",
  "updatedAt",
  "createdBy",
  "updatedBy",
  "deletedAt",
  "deletedBy",
  "isBulk",
  "isBulkOriginal",
  "bulkCount",
  "bulkIndex",
  "bulkGroupId",
  "bulkParentId",
  "pendencies",
  "documents",
  "notes",
  "workflowHistory",
  "auditLog",
  "formExtras",
  "forceBulkCreate",
]);

const isChequeFlatKey = (key) => /^cheque_\d+_/.test(key);

export function extractFormExtras(flat) {
  const extras = {};
  if (!flat || typeof flat !== "object") return extras;
  for (const [key, value] of Object.entries(flat)) {
    if (value === undefined) continue;
    // Mongo field names can't contain "." or start with "$".
    if (!key || key.includes(".") || key.startsWith("$")) continue;
    if (SERVER_OWNED_FLAT_KEYS.has(key)) continue;
    if (
      (MAPPED_FLAT_KEYS.has(key) || isChequeFlatKey(key)) &&
      !ALWAYS_KEPT_FLAT_KEYS.has(key)
    ) {
      continue;
    }
    extras[key] = value;
  }
  return extras;
}

// ─── Schema-aware value coercion ──────────────────────────────────────────────

const SKIP = Symbol("skip");

const STATUS_ALIASES = {
  completed: "closed",
  complete: "closed",
  "under review": "under_review",
  pending: "submitted",
};

// Form values arrive as whatever the UI held ("₹1,20,000", "", "Invalid Date",
// "Completed"). Casting those straight onto typed paths throws and fails the
// whole save, so each value is normalised to its path's type first; values
// that still can't be represented are skipped instead of aborting the save.
function coerceForPath(path, value) {
  const schemaType = HomeLoan.schema.path(path);
  if (!schemaType || value === null) return value;

  switch (schemaType.instance) {
    case "Number": {
      if (typeof value === "number") return Number.isFinite(value) ? value : null;
      if (typeof value === "boolean") return SKIP;
      const text = String(value).replace(/[^0-9.-]/g, "");
      if (text === "" || text === "-" || text === ".") return null;
      const n = Number(text);
      return Number.isFinite(n) ? n : null;
    }
    case "Date": {
      if (value === "") return null;
      const d = new Date(value);
      return Number.isNaN(d.getTime()) ? null : d;
    }
    case "ObjectId": {
      if (value === "") return null;
      return mongoose.Types.ObjectId.isValid(value) ? value : SKIP;
    }
    case "Boolean": {
      if (typeof value === "boolean") return value;
      const text = String(value).trim().toLowerCase();
      if (["true", "yes", "1"].includes(text)) return true;
      if (["false", "no", "0", ""].includes(text)) return false;
      return SKIP;
    }
    case "String": {
      if (typeof value === "object") return SKIP;
      const allowed = schemaType.enumValues;
      if (Array.isArray(allowed) && allowed.length) {
        const text = String(value).trim().toLowerCase();
        const candidate = STATUS_ALIASES[text] || text.replace(/\s+/g, "_");
        return allowed.includes(candidate) ? candidate : SKIP;
      }
      return value;
    }
    default:
      return value;
  }
}

const isPlainObject = (value) =>
  value !== null &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  !(value instanceof Date) &&
  !(value instanceof mongoose.Types.ObjectId);

// Turns flatToDoc output into leaf paths, dropping undefined so a payload
// only ever touches the fields it actually carries. Arrays are leaves.
function collectLeafPaths(obj, prefix = "", out = []) {
  for (const [key, value] of Object.entries(obj || {})) {
    if (value === undefined) continue;
    const path = prefix ? `${prefix}.${key}` : key;
    const schemaType = HomeLoan.schema.path(path);
    const isMixed = schemaType?.instance === "Mixed";
    if (isPlainObject(value) && !isMixed) {
      collectLeafPaths(value, path, out);
    } else {
      out.push([path, value]);
    }
  }
  return out;
}

function recomputePricing(loan) {
  const p = loan.pricing || {};
  const n = (v) => Number(v) || 0;
  const hasAny = [
    "exShowroomPrice",
    "insuranceCost",
    "roadTax",
    "accessoriesAmount",
  ].some((k) => n(p[k]) > 0);
  if (!hasAny) return;
  const onRoadPrice =
    n(p.exShowroomPrice) +
    n(p.insuranceCost) +
    n(p.roadTax) +
    n(p.accessoriesAmount) -
    n(p.dealerDiscount) -
    n(p.manufacturerDiscount);
  const grossLoan = onRoadPrice - n(p.marginMoney) - n(p.advanceEmi) - n(p.tradeInValue);
  loan.set("pricing.onRoadPrice", onRoadPrice);
  loan.set("pricing.grossLoan", grossLoan);
  loan.set("pricing.netLoan", grossLoan - n(p.otherDiscounts));
}

// Merges a flat form payload into a HomeLoan document (new or loaded).
// Only the fields present in the payload change; everything else on the
// document is kept. This replaces the old `$set` of whole sub-documents,
// which wiped applicant/approval/vehicle/etc. on every partial save.
export function applyFlatPayload(loan, flatPayload = {}, userId) {
  const flat = flatPayload || {};
  const docData = flatToDoc(flat);

  // Sections switched off in the form are removed rather than left stale.
  if (flat.hasCoApplicant === false) {
    loan.set("coApplicant", undefined);
    delete docData.coApplicant;
  }
  if (flat.hasGuarantor === false) {
    loan.set("guarantor", undefined);
    delete docData.guarantor;
  }

  for (const [path, rawValue] of collectLeafPaths(docData)) {
    const value = coerceForPath(path, rawValue);
    if (value === SKIP) continue;
    loan.set(path, value);
  }

  const extras = extractFormExtras(flat);
  if (Object.keys(extras).length) {
    const current =
      loan.formExtras && typeof loan.formExtras === "object" ? loan.formExtras : {};
    loan.formExtras = { ...current, ...extras };
    loan.markModified("formExtras");
  }

  recomputePricing(loan);
  if (userId) loan.updatedBy = userId;
  return loan;
}

// Accepts the Mongo _id or the human-readable applicationNumber/loanId -
// several screens only hold the latter.
function buildLoanLookup(id) {
  const safeId = String(id || "").trim();
  if (!safeId) return null;
  const or = [{ loanId: safeId }, { applicationNumber: safeId }];
  if (mongoose.Types.ObjectId.isValid(safeId)) or.push({ _id: safeId });
  return { $or: or, deletedAt: null };
}

async function findLoanDocument(id) {
  const lookup = buildLoanLookup(id);
  return lookup ? HomeLoan.findOne(lookup) : null;
}

// ─── CRUD helpers ─────────────────────────────────────────────────────────────

export async function createHomeLoan(flatPayload, userId) {
  const loan = new HomeLoan();
  applyFlatPayload(loan, flatPayload, userId);
  const applicationNumber = await HomeLoan.generateApplicationNumber();
  loan.applicationNumber = applicationNumber;
  loan.loanId = applicationNumber;
  if (userId) loan.createdBy = userId;
  await loan.save();
  return docToFlat(loan);
}

// Property-specific data: with multiple loans each file is for its own
// property, so these are entered per file instead of being cloned.
const PER_FILE_PROPERTY_EXTRAS = ["propertyType"];

// Stage data that belongs to one specific loan file and must not be copied
// onto a newly split file.
const PER_FILE_STAGE_EXTRA_PATTERNS = [
  /^postfile_/,
  /^disburse_/,
  /^disbursement_/,
  /^delivery_/,
  /^dispatch_/,
  /^payout_/,
  /^rc_/,
  /^invoice_/,
  /^__postfile/,
  /^__delivery/,
  /^si_/,
  /^nach_/,
  /^ecs_/,
];
const PER_FILE_STAGE_EXTRAS = new Set([
  "instrumentType",
  "loan_notes",
  "status",
  "completedDate",
  "currentStage",
  "rcInvStorageNumber",
  "rc_inv_storage_number",
  "loan_number",
]);

const KYC_DOC_KEYS = [
  "aadhaarCardDocUrl",
  "aadhaarCardBackDocUrl",
  "panCardDocUrl",
  "passportDocUrl",
  "passportBackDocUrl",
  "dlDocUrl",
  "addressProofDocUrl",
  "gstDocUrl",
  "gstDocUrlPage2",
  "gstDocUrlPage3",
  "incomeDocUrl",
  "itrDocUrl",
  "bankStatementDocUrl",
];

// Bank cards carry over so the comparison doesn't have to be rebuilt on every
// split file, but a disbursement belongs to one file only.
function cloneBankForSplit(bank) {
  if (!bank || typeof bank !== "object") return bank;
  const next = { ...bank };
  if (String(next.status || "").trim().toLowerCase() === "disbursed") {
    next.status = "Approved";
    next.approval_status = "Approved";
    delete next.disbursedDate;
    delete next.disbursalDate;
    delete next.disbursedAmount;
    delete next.disbursementRemarks;
    next.statusHistory = (Array.isArray(next.statusHistory) ? next.statusHistory : [])
      .filter((h) => String(h?.status || "").toLowerCase() !== "disbursed");
  }
  return next;
}

function buildSplitClone(original, index, count) {
  const src = original.toObject({ depopulate: true });
  const extras = { ...(src.formExtras || {}) };
  for (const key of Object.keys(extras)) {
    if (
      PER_FILE_PROPERTY_EXTRAS.includes(key) ||
      PER_FILE_STAGE_EXTRAS.has(key) ||
      PER_FILE_STAGE_EXTRA_PATTERNS.some((re) => re.test(key))
    ) {
      delete extras[key];
    }
  }
  extras.currentStage = "profile";

  const kycDocs = {};
  for (const key of KYC_DOC_KEYS) {
    if (src.postFile?.[key]) kycDocs[key] = src.postFile[key];
  }

  const banks = Array.isArray(src.approval?.banks)
    ? src.approval.banks.map(cloneBankForSplit)
    : [];

  return {
    // Customer, applicants, lead, references and pre-file data are shared.
    customerId: src.customerId,
    branchId: src.branchId,
    applicantType: src.applicantType,
    caseType: src.caseType,
    customerType: src.customerType,
    customerName: src.customerName,
    lead: src.lead,
    applicant: src.applicant,
    coApplicant: src.coApplicant,
    coApplicants: src.coApplicants,
    guarantor: src.guarantor,
    authorisedSignatory: src.authorisedSignatory,
    reference1: src.reference1,
    reference2: src.reference2,
    formExtras: extras,
    // Customer KYC documents belong to the same customer on every file.
    postFile: { instruments: [], ...kycDocs },
    approval: { banks },
    // Property details (type of loan, property/loan structure) are per file.
    status: "draft",
    currentStep: "profile",
    completedSteps: [],
    isBulk: true,
    bulkCount: count,
    bulkIndex: index,
    bulkGroupId: String(original._id),
    bulkParentId: original._id,
  };
}

// Splits an original entry into `count` loan files for the same customer
// (Bulk Loan Creation). The original stays file #1 - keeping its id, uploads,
// bank comparison and stage progress - and count-1 linked files are created
// from it. Shared customer/applicant/pre-file data and the bank comparison
// list are copied; property details and stage progress start fresh per file.
export async function splitHomeLoan(originalId, count, userId, flatPayload) {
  const total = Number(count);
  if (!Number.isInteger(total) || total < 2 || total > 50) {
    const e = new Error("Enter a number of loan files between 2 and 50.");
    e.status = 400;
    throw e;
  }

  const original = await findLoanDocument(originalId);
  if (!original) return null;

  if (original.isBulk) {
    const e = new Error(
      "This loan file was created by splitting an original entry and cannot be split again.",
    );
    e.status = 400;
    throw e;
  }
  if (original.isBulkOriginal) {
    const e = new Error("This entry has already been split into multiple loan files.");
    e.status = 400;
    throw e;
  }

  // Persist the latest form state first so nothing typed since the last save
  // is lost from the original or its copies.
  if (flatPayload && typeof flatPayload === "object") {
    applyFlatPayload(original, flatPayload, userId);
  }
  original.isBulkOriginal = true;
  original.bulkCount = total;
  original.bulkIndex = 1;
  original.bulkGroupId = String(original._id);
  original.formExtras = {
    ...(original.formExtras || {}),
    isMultipleCars: true,
    numberOfCars: total,
  };
  original.markModified("formExtras");
  if (userId) original.updatedBy = userId;
  await original.save();

  const created = [];
  try {
    for (let index = 2; index <= total; index++) {
      const clone = new HomeLoan(buildSplitClone(original, index, total));
      const applicationNumber = await HomeLoan.generateApplicationNumber();
      clone.applicationNumber = applicationNumber;
      clone.loanId = applicationNumber;
      if (userId) {
        clone.createdBy = userId;
        clone.updatedBy = userId;
      }
      await clone.save();
      created.push(clone);
    }
  } catch (e) {
    // All-or-nothing: roll back partial splits and re-open the original.
    await HomeLoan.deleteMany({ _id: { $in: created.map((c) => c._id) } });
    await HomeLoan.updateOne(
      { _id: original._id },
      { $set: { isBulkOriginal: false }, $unset: { bulkCount: 1, bulkIndex: 1, bulkGroupId: 1 } },
    );
    throw e;
  }

  return {
    original: docToFlat(original),
    loans: [original, ...created].map(docToFlat),
  };
}

export async function updateHomeLoan(id, flatPayload, userId) {
  const loan = await findLoanDocument(id);
  if (!loan) return null;

  const previousStep = loan.currentStep;
  applyFlatPayload(loan, flatPayload, userId);

  if (loan.currentStep !== previousStep) {
    loan.workflowHistory.push({
      stage: loan.currentStep,
      fromStatus: loan.status,
      toStatus: loan.status,
      action: "step_change",
      actionBy: String(userId || "system"),
      actionAt: new Date(),
    });
  }

  await loan.save();
  return docToFlat(loan);
}

const bankMatches = (bank, { bankId, bankName }) => {
  const ids = [bank?.id, bank?._id, bank?.bankId, bank?.applicationId]
    .map((v) => String(v ?? "").trim())
    .filter(Boolean);
  if (bankId && ids.includes(String(bankId).trim())) return true;
  if (!bankId && bankName) {
    return (
      String(bank?.bankName || "").trim().toLowerCase() ===
      String(bankName).trim().toLowerCase()
    );
  }
  return false;
};

// Records a disbursement against one bank card without touching any other
// part of the file.
export async function disburseHomeLoan(id, body = {}, userId) {
  const loan = await findLoanDocument(id);
  if (!loan) return null;

  const disbursedAt = body.disbursedDate ? new Date(body.disbursedDate) : new Date();
  const when = Number.isNaN(disbursedAt.getTime()) ? new Date() : disbursedAt;
  const amount = coerceForPath("approval.loanAmountDisbursed", body.disburseAmount);
  const remarks = body.remarks || "";

  const banks = Array.isArray(loan.approval?.banks) ? [...loan.approval.banks] : [];
  const target = { bankId: body.bankId, bankName: body.disbursedBankName };
  let matched = false;
  const nextBanks = banks.map((bank) => {
    if (matched || !bankMatches(bank, target)) return bank;
    matched = true;
    const history = Array.isArray(bank.statusHistory) ? bank.statusHistory : [];
    const alreadyDisbursed = history.some(
      (h) => String(h?.status || "").toLowerCase() === "disbursed",
    );
    return {
      ...bank,
      status: "Disbursed",
      approval_status: "Disbursed",
      disbursedDate: when.toISOString(),
      disbursementRemarks: remarks,
      statusHistory: alreadyDisbursed
        ? history
        : [
            ...history,
            { status: "Disbursed", changedAt: when.toISOString(), note: remarks || "Loan disbursed" },
          ],
    };
  });

  loan.set("approval.banks", nextBanks);
  loan.set("approval.status", "Disbursed");
  if (body.disbursedBankName) loan.set("approval.bankName", body.disbursedBankName);
  if (amount !== SKIP && amount !== null) {
    loan.set("approval.loanAmountDisbursed", amount);
    loan.set("disbursement.amount", amount);
  }
  loan.set("approval.disbursedDate", when);
  loan.set("disbursement.date", when);
  if (remarks) loan.set("disbursement.remarks", remarks);
  loan.status = "disbursed";
  loan.approval.statusHistory.push({
    status: "Disbursed",
    updatedAt: when,
    updatedBy: String(userId || "system"),
    remarks,
  });
  if (userId) loan.updatedBy = userId;

  await loan.save();
  return docToFlat(loan);
}

export async function getHomeLoanById(id) {
  // The view modal tries several candidate identifiers (loanId, loan_number,
  // Mongo _id) in sequence, so this resolves by whichever one matches
  // instead of only _id.
  const loan = await findLoanDocument(id);
  return loan ? docToFlat(loan) : null;
}

export async function listHomeLoans(query = {}) {
  const {
    page = 1,
    limit = 20,
    status,
    currentStep,
    typeOfLoan,
    search,
    customerId,
    sortBy = "createdAt",
    sortOrder = "desc",
  } = query;

  const filter = { deletedAt: null };
  if (status) filter.status = status;
  if (currentStep) filter.currentStep = currentStep;
  if (typeOfLoan) filter.typeOfLoan = typeOfLoan;
  if (customerId) filter.customerId = customerId;
  // Skip the $or regex scan for 1-character queries — they match almost every
  // document and were the main cause of the dashboard search hanging on "loading".
  if (search && String(search).trim().length >= 2) {
    filter.$or = [
      { customerName: { $regex: search, $options: "i" } },
      { applicationNumber: { $regex: search, $options: "i" } },
      { "applicant.pan": { $regex: search, $options: "i" } },
      { "vehicle.regNo": { $regex: search, $options: "i" } },
    ];
  }

  const skip = (Number(page) - 1) * Number(limit);
  const sort = { [sortBy]: sortOrder === "asc" ? 1 : -1 };

  const [loans, total] = await Promise.all([
    HomeLoan.find(filter)
      .sort(sort)
      .skip(skip)
      .limit(Number(limit))
      .select("-auditLog -workflowHistory")
      .lean(),
    HomeLoan.countDocuments(filter),
  ]);

  return {
    loans: loans.map(docToFlat),
    total,
    page: Number(page),
    limit: Number(limit),
    pages: Math.ceil(total / Number(limit)),
  };
}

export async function softDeleteHomeLoan(id, userId) {
  const loan = await HomeLoan.findByIdAndUpdate(
    id,
    { deletedAt: new Date(), deletedBy: String(userId || "system") },
    { new: true },
  );
  return !!loan;
}

export async function getDashboardStats() {
  const base = { deletedAt: null };
  const isDisbursedExpr = {
    $or: [
      { $eq: ["$status", "disbursed"] },
      { $ifNull: ["$approval.disbursedDate", false] },
      { $ifNull: ["$disbursement.date", false] },
      { $gt: [{ $ifNull: ["$approval.loanAmountDisbursed", 0] }, 0] },
    ],
  };
  const isApprovedExpr = {
    $or: [
      { $eq: ["$status", "approved"] },
      { $gt: [{ $ifNull: ["$approval.loanAmountApproved", 0] }, 0] },
      { $ifNull: ["$approval.approvalDate", false] },
    ],
  };

  const [total, byStatus, byType, agg] = await Promise.all([
    HomeLoan.countDocuments(base),
    HomeLoan.aggregate([
      { $match: base },
      { $group: { _id: "$status", count: { $sum: 1 } } },
    ]),
    HomeLoan.aggregate([
      { $match: base },
      { $group: { _id: "$typeOfLoan", count: { $sum: 1 } } },
    ]),
    HomeLoan.aggregate([
      { $match: base },
      {
        $addFields: {
          __isDisbursed: isDisbursedExpr,
          __isApproved: isApprovedExpr,
        },
      },
      {
        $group: {
          _id: null,
          pending: {
            $sum: {
              $cond: [
                {
                  $and: [
                    { $eq: ["$currentStep", "approval"] },
                    { $eq: ["$__isApproved", false] },
                    { $eq: ["$__isDisbursed", false] },
                  ],
                },
                1,
                0,
              ],
            },
          },
          pendingDisbursal: {
            $sum: {
              $cond: [
                {
                  $and: [
                    { $eq: ["$__isApproved", true] },
                    { $eq: ["$__isDisbursed", false] },
                  ],
                },
                1,
                0,
              ],
            },
          },
          disbursed: {
            $sum: { $cond: ["$__isDisbursed", 1, 0] },
          },
          totalBookValue: {
            $sum: {
              $cond: [
                "$__isDisbursed",
                {
                  $ifNull: [
                    "$approval.loanAmountDisbursed",
                    { $ifNull: ["$disbursement.amount", 0] },
                  ],
                },
                0,
              ],
            },
          },
          emiCapturedCount: {
            $sum: {
              $cond: [{ $gt: [{ $ifNull: ["$postFile.emiAmount", 0] }, 0] }, 1, 0],
            },
          },
          regNoCapturedCount: {
            $sum: {
              $cond: [
                { $gt: [{ $strLenCP: { $ifNull: ["$vehicle.regNo", ""] } }, 0] },
                1,
                0,
              ],
            },
          },
        },
      },
    ]),
  ]);

  const summary = agg[0] || {};
  return {
    total,
    pending: summary.pending || 0,
    pendingDisbursal: summary.pendingDisbursal || 0,
    disbursed: summary.disbursed || 0,
    totalBookValue: summary.totalBookValue || 0,
    emiCapturedCount: summary.emiCapturedCount || 0,
    regNoCapturedCount: summary.regNoCapturedCount || 0,
    byStatus,
    byType,
  };
}

export async function saveBanksData(loanId, banksData) {
  const lookup = buildLoanLookup(loanId);
  if (!lookup) return null;
  const banks = Array.isArray(banksData) ? banksData : [];
  const loan = await HomeLoan.findOneAndUpdate(
    lookup,
    { $set: { "approval.banks": banks } },
    { new: true },
  );
  if (!loan) return null;
  return loan.approval?.banks || [];
}

export async function getBanksData(loanId) {
  const lookup = buildLoanLookup(loanId);
  if (!lookup) return [];
  const loan = await HomeLoan.findOne(lookup, { "approval.banks": 1 });
  return loan?.approval?.banks || [];
}

export async function getNextRcInvStorageNumber() {
  const prefix = `RC-${new Date().getFullYear()}-`;
  const last = await HomeLoan.findOne(
    { "postFile.rcInvStorageNumber": { $regex: `^${prefix}` } },
    { "postFile.rcInvStorageNumber": 1 },
    { sort: { "postFile.rcInvStorageNumber": -1 } },
  );
  let seq = 1;
  if (last?.postFile?.rcInvStorageNumber) {
    const parts = last.postFile.rcInvStorageNumber.split("-");
    seq = (parseInt(parts[parts.length - 1], 10) || 0) + 1;
  }
  return `${prefix}${String(seq).padStart(4, "0")}`;
}

// ─── Loan amount breakup fields ───────────────────────────────────────────────

// Must match DEFAULT_LOAN_BREAKUP_FIELDS in the frontend's loanBreakupFields.js.
const DEFAULT_BREAKUP_FIELDS = [
  { key: "netLoanApproved", label: "Net Loan Amount Approved", order: 1 },
  { key: "creditAssuredFinance", label: "Credit Assured Finance", order: 2 },
  { key: "insuranceFinance", label: "Insurance Finance", order: 3 },
  { key: "extendedWarrantyFinance", label: "Extended Warranty Finance", order: 4 },
];
const DEFAULT_BREAKUP_KEYS = new Set(DEFAULT_BREAKUP_FIELDS.map((f) => f.key.toLowerCase()));

// Same key normalisation as the frontend (sanitizeKey): "Stamp Duty" -> "stampDuty".
export function normalizeBreakupKey(value = "") {
  const cleaned = String(value || "")
    .trim()
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/[^a-zA-Z0-9]+/g, " ")
    .trim()
    .toLowerCase();
  if (!cleaned) return "";
  return cleaned
    .split(/\s+/)
    .map((part, i) => (i === 0 ? part : part.charAt(0).toUpperCase() + part.slice(1)))
    .join("");
}

const escapeRegex = (value = "") => String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// key (lowercased) -> number of home loans holding a non-zero value for it,
// across bank cards and the approval/post-file breakup snapshots.
async function getBreakupUsageMap() {
  const rows = await HomeLoan.aggregate([
    { $match: { deletedAt: null } },
    {
      $project: {
        rows: {
          $concatArrays: [
            { $ifNull: ["$formExtras.approval_breakup_custom", []] },
            { $ifNull: ["$formExtras.postfile_disbursed_custom", []] },
            {
              $reduce: {
                input: { $ifNull: ["$approval.banks", []] },
                initialValue: [],
                in: {
                  $concatArrays: [
                    "$$value",
                    {
                      $cond: [
                        { $isArray: "$$this.breakupCustomFields" },
                        "$$this.breakupCustomFields",
                        [],
                      ],
                    },
                  ],
                },
              },
            },
          ],
        },
      },
    },
    { $unwind: "$rows" },
    {
      $project: {
        key: { $toLower: { $trim: { input: { $toString: { $ifNull: ["$rows.key", ""] } } } } },
        value: {
          $convert: { input: "$rows.value", to: "double", onError: 0, onNull: 0 },
        },
      },
    },
    { $match: { key: { $ne: "" }, value: { $gt: 0 } } },
    { $group: { _id: { key: "$key", loan: "$_id" } } },
    { $group: { _id: "$_id.key", count: { $sum: 1 } } },
  ]);
  return new Map(rows.map((r) => [String(r._id), Number(r.count || 0)]));
}

export async function listBreakupFields() {
  const [custom, usage] = await Promise.all([
    HomeLoanBreakupField.find({ active: { $ne: false } })
      .sort({ order: 1, createdAt: 1, label: 1 })
      .lean(),
    getBreakupUsageMap(),
  ]);

  const merged = DEFAULT_BREAKUP_FIELDS.map((f) => ({
    ...f,
    isDefault: true,
    canDelete: false,
    inUseCount: 0,
  }));
  const seen = new Set(DEFAULT_BREAKUP_KEYS);
  for (const field of custom) {
    const lower = String(field.key || "").toLowerCase();
    if (!lower || seen.has(lower)) continue;
    seen.add(lower);
    const inUseCount = usage.get(lower) || 0;
    merged.push({
      key: field.key,
      label: field.label,
      order: field.order,
      isDefault: false,
      canDelete: inUseCount === 0,
      inUseCount,
    });
  }
  return merged.sort(
    (a, b) =>
      Number(a.order || 0) - Number(b.order || 0) ||
      String(a.label || "").localeCompare(String(b.label || "")),
  );
}

export async function createBreakupField({ label, key } = {}, userId) {
  const cleanLabel = String(label || "").replace(/\s+/g, " ").trim();
  if (!cleanLabel) {
    const e = new Error("Field label is required");
    e.status = 400;
    throw e;
  }
  const baseKey = normalizeBreakupKey(key || cleanLabel);
  if (!baseKey) {
    const e = new Error("Invalid field label");
    e.status = 400;
    throw e;
  }
  if (DEFAULT_BREAKUP_KEYS.has(baseKey.toLowerCase())) {
    const e = new Error("This field already exists as a default breakup field");
    e.status = 400;
    throw e;
  }

  const existing = await HomeLoanBreakupField.findOne({
    $or: [
      { key: baseKey },
      { label: { $regex: `^${escapeRegex(cleanLabel)}$`, $options: "i" } },
    ],
  }).lean();
  if (existing) {
    if (existing.active === false) {
      await HomeLoanBreakupField.updateOne({ _id: existing._id }, { $set: { active: true } });
    }
    return {
      created: false,
      field: { key: existing.key, label: existing.label, order: existing.order, isDefault: false },
    };
  }

  const last = await HomeLoanBreakupField.findOne({}).sort({ order: -1 }).select("order").lean();
  const order = Math.max(Number(last?.order || 1000) + 1, 1005);
  const doc = await HomeLoanBreakupField.create({
    key: baseKey,
    label: cleanLabel,
    order,
    createdBy: String(userId || "user"),
  });
  return {
    created: true,
    field: {
      key: doc.key,
      label: doc.label,
      order: doc.order,
      isDefault: false,
      canDelete: true,
      inUseCount: 0,
    },
  };
}

export async function deleteBreakupField(rawKey) {
  const key = normalizeBreakupKey(rawKey);
  if (!key) {
    const e = new Error("Field key is required");
    e.status = 400;
    throw e;
  }
  if (DEFAULT_BREAKUP_KEYS.has(key.toLowerCase())) {
    const e = new Error("Default breakup fields cannot be deleted");
    e.status = 400;
    throw e;
  }
  const field = await HomeLoanBreakupField.findOne({ key });
  if (!field) {
    const e = new Error("Breakup field not found");
    e.status = 404;
    throw e;
  }
  const usage = await getBreakupUsageMap();
  if ((usage.get(key.toLowerCase()) || 0) > 0) {
    const e = new Error(
      "This field has data in existing home loan cases and cannot be deleted.",
    );
    e.status = 409;
    throw e;
  }
  await HomeLoanBreakupField.deleteOne({ _id: field._id });
  return { deleted: true, key };
}
