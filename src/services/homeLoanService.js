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
import mongoose from "mongoose";

// ─── Flat → Document (inbound transform) ─────────────────────────────────────

export function flatToDoc(flat) {
  if (!flat || typeof flat !== "object") return {};
  const f = flat;

  // ── Instruments: collapse cheque_N_* flat fields ────────────────────────
  const instruments = [];
  for (let i = 1; i <= 20; i++) {
    const number = f[`cheque_${i}_number`];
    const amount = f[`cheque_${i}_amount`];
    if (number || amount) {
      instruments.push({
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
        dependents: f.co_dependents,
        education: f.co_education,
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
    banks: f.approval_banksData,
    statusHistory: f.approval_statusHistory,
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
  const exShowroom = Number(f.exShowroomPrice) || 0;
  const insurance = Number(f.insuranceCost) || 0;
  const roadTax = Number(f.roadTax) || 0;
  const accessories = Number(f.accessoriesAmount) || 0;
  const dealerDiscount = Number(f.dealerDiscount) || 0;
  const manufacturerDiscount = Number(f.manufacturerDiscount) || 0;
  const marginMoney = Number(f.marginMoney) || 0;
  const advanceEmi = Number(f.advanceEmi) || 0;
  const tradeInValue = Number(f.tradeInValue) || 0;
  const otherDiscounts = Number(f.otherDiscounts) || 0;
  const onRoadPrice = exShowroom + insurance + roadTax + accessories - dealerDiscount - manufacturerDiscount;
  const grossLoan = onRoadPrice - marginMoney - advanceEmi - tradeInValue;
  const netLoan = grossLoan - otherDiscounts;

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
    onRoadPrice: onRoadPrice || undefined,
    grossLoan: grossLoan || undefined,
    netLoan: netLoan || undefined,
  };

  // ── Dealer ──────────────────────────────────────────────────────────────
  const dealer = {
    name: f.showroomDealerName,
    contactPerson: f.showroomDealerContactPerson,
    contactNumber: f.showroomDealerContactNumber,
    address: f.showroomDealerAddress,
  };

  // ── References ────────────────────────────────────────────────────────────
  const reference1 =
    f.reference1 && typeof f.reference1 === "object" ? f.reference1 : undefined;
  const reference2 =
    f.reference2 && typeof f.reference2 === "object" ? f.reference2 : undefined;

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
    currentStep: f.currentStep,
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

  // Expand instruments → cheque_N_* flat fields
  const chequeFlat = {};
  if (Array.isArray(pf.instruments)) {
    pf.instruments.forEach((inst, idx) => {
      const n = idx + 1;
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

  return {
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
    isBulk: d.isBulk,
    bulkCount: d.bulkCount,

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
    hasCoApplicant: !!co.customerName,
    co_id: co.customerId,
    co_customerName: co.customerName,
    co_motherName: co.motherName,
    co_fatherName: co.fatherName,
    co_dob: co.dob,
    co_gender: co.gender,
    co_maritalStatus: co.maritalStatus,
    co_dependents: co.dependents,
    co_education: co.education,
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
    hasGuarantor: !!gu.customerName,
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
}

// ─── CRUD helpers ─────────────────────────────────────────────────────────────

export async function createHomeLoan(flatPayload, userId) {
  const applicationNumber = await HomeLoan.generateApplicationNumber();
  const docData = flatToDoc(flatPayload);
  docData.applicationNumber = applicationNumber;
  docData.loanId = applicationNumber;
  if (userId) {
    docData.createdBy = userId;
    docData.updatedBy = userId;
  }
  const loan = await HomeLoan.create(docData);
  return docToFlat(loan);
}

// Splits one submitted form into `count` independent loan files for the same
// customer (Bulk Loan Creation). Each split file gets its own application
// number and its own document uploads, but the bank comparison list
// (approval.banks) is carried over from the source payload so users don't
// have to re-add every bank for comparison on each split file. All other
// stage-progression data (post-file, disbursement, delivery, payout,
// pendencies, workflow history) is reset so every split starts fresh at the
// Profile step.
export async function createHomeLoanBulk(flatPayload, userId, count) {
  const isSameVehicle = flatPayload.isSameVehicle !== false;
  const baseDoc = flatToDoc(flatPayload);
  const preservedBanks = Array.isArray(baseDoc.approval?.banks)
    ? baseDoc.approval.banks
    : [];

  const createdLoans = [];
  for (let i = 0; i < count; i++) {
    try {
      const applicationNumber = await HomeLoan.generateApplicationNumber();
      const docData = JSON.parse(JSON.stringify(baseDoc));

      docData.applicationNumber = applicationNumber;
      docData.loanId = applicationNumber;

      // Each split file needs its own fresh document uploads.
      docData.documents = [];

      // Vehicle/pricing are only cloned when every split shares the same
      // vehicle; otherwise each split needs to be filled in separately.
      if (!isSameVehicle) {
        docData.vehicle = {};
        docData.pricing = {};
      }

      // Reset stage-progression state so every split starts fresh, while
      // preserving the bank comparison list.
      docData.approval = { banks: preservedBanks };
      docData.postFile = { instruments: [] };
      docData.disbursement = {};
      docData.delivery = {};
      docData.payout = {};
      docData.pendencies = [];
      docData.workflowHistory = [];
      docData.auditLog = [];
      docData.status = "draft";
      docData.currentStep = "profile";
      docData.completedSteps = [];
      docData.isBulk = true;
      docData.bulkCount = count;

      if (userId) {
        docData.createdBy = userId;
        docData.updatedBy = userId;
      }

      const loan = await HomeLoan.create(docData);
      createdLoans.push(docToFlat(loan));
    } catch (e) {
      console.error("Failed to create bulk home loan item", e);
    }
  }

  return createdLoans;
}

export async function updateHomeLoan(id, flatPayload, userId) {
  const docData = flatToDoc(flatPayload);
  if (userId) docData.updatedBy = userId;

  // Record workflow step change
  const existing = await HomeLoan.findById(id, { currentStep: 1, status: 1 });
  const workflowEntry =
    docData.currentStep && existing?.currentStep !== docData.currentStep
      ? {
          stage: docData.currentStep,
          fromStatus: existing?.status,
          toStatus: docData.status || existing?.status,
          action: "step_change",
          actionBy: String(userId || "system"),
          actionAt: new Date(),
        }
      : null;

  const update = { $set: docData };
  if (workflowEntry) {
    update.$push = { workflowHistory: workflowEntry };
  }

  const loan = await HomeLoan.findByIdAndUpdate(id, update, { new: true });
  if (!loan) return null;
  return docToFlat(loan);
}

export async function getHomeLoanById(id) {
  const safeId = String(id || "").trim();
  if (!safeId) return null;

  // The view modal tries several candidate identifiers (loanId, loan_number,
  // Mongo _id) in sequence, so this must resolve by whichever one matches
  // instead of only _id — otherwise every non-ObjectId candidate throws a
  // CastError and the modal has to fail its way through to the right one.
  const orConditions = [{ loanId: safeId }, { applicationNumber: safeId }];
  if (mongoose.Types.ObjectId.isValid(safeId)) {
    orConditions.push({ _id: safeId });
  }

  const loan = await HomeLoan.findOne({
    $or: orConditions,
    deletedAt: null,
  });
  if (!loan) return null;
  return docToFlat(loan);
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
  const loan = await HomeLoan.findByIdAndUpdate(
    loanId,
    { $set: { "approval.banks": banksData } },
    { new: true },
  );
  if (!loan) return null;
  return loan.approval?.banks || [];
}

export async function getBanksData(loanId) {
  const loan = await HomeLoan.findById(loanId, { "approval.banks": 1 });
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
