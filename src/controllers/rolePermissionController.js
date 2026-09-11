import asyncHandler from "express-async-handler";
import RolePermission from "../models/RolePermission.js";

export const SYSTEM_ROLES = [
  "superadmin",
  "admin",
  "staff",
];

export const PERMISSION_CATALOG = [
  {
    key: "customers",
    label: "Customers",
    sections: [
      { key: "customer", label: "Customer", fields: [
        ["customerType", "Customer Type"], ["customerName", "Customer Name"], ["mobile", "Mobile Number"],
        ["alternateMobile", "Alternate Mobile Number"], ["email", "Email Address"], ["panNumber", "PAN Number"],
        ["aadhaarNumber", "Aadhaar Number"], ["passportFront", "Passport Front"], ["passportBack", "Passport Back"],
        ["dateOfBirth", "Date of Birth"], ["gender", "Gender"], ["address", "Address"], ["pincode", "Pincode"],
        ["city", "City"], ["state", "State"], ["employmentType", "Employment Type"],
        ["companyName", "Company Name"], ["designation", "Designation"], ["monthlyIncome", "Monthly Income"],
        ["bankDetails", "Bank Details"], ["incomeDetails", "Income Details"], ["documents", "Documents"],
      ] },
    ],
  },
  {
    key: "insurance",
    label: "Insurance",
    sections: [
      { key: "customer", label: "Customer", fields: [["buyerType", "Buyer Type"], ["customerName", "Customer Name"], ["employeeName", "Employee Name"], ["mobile", "Mobile Number"], ["alternateMobile", "Alternate Mobile Number"], ["email", "Email Address"], ["panNumber", "PAN Number"], ["aadhaarNumber", "Aadhaar Number"], ["insuredAddress", "Insured Address"], ["pincode", "Pincode"], ["city", "City"], ["referenceName", "Reference Name"], ["referenceMobile", "Reference Mobile Number"], ["sourceOrigin", "Source Origin"]] },
      { key: "vehicle", label: "Vehicle", fields: [["caseType", "Case Type"], ["registrationNumber", "Registration Number"], ["registrationAuthority", "Registration Authority"], ["make", "Vehicle Make"], ["model", "Model"], ["variant", "Variant"], ["engineNumber", "Engine Number"], ["chassisNumber", "Chassis Number"], ["manufactureDate", "Manufacture Date"], ["registrationDate", "Date of Registration"], ["fuelType", "Fuel Type"], ["batteryNumber", "Battery Number"], ["chargerNumber", "Charger Number"], ["hypothecation", "Hypothecation"]] },
      { key: "policy", label: "Policy", fields: [["insuranceCompany", "Insurance Company"], ["policyType", "Policy Type"], ["policyNumber", "Policy Number"], ["issueDate", "Issue Date"], ["createdDate", "Created Date"], ["issuedDate", "Issued Date"], ["policyPurchaseDate", "Policy Purchase Date"], ["policyDuration", "Policy Duration"], ["policyStartDate", "Policy Start Date"], ["policyEndDate", "Policy End Date"], ["odExpiryDate", "OD Expiry Date"], ["tpExpiryDate", "TP Expiry Date"], ["exShowroomPrice", "Ex-Showroom Price"], ["odometerReading", "Odometer Reading"], ["kmsCoverage", "Kms Coverage"], ["ncbDiscount", "NCB Discount (%)"], ["vehicleIdv", "Vehicle IDV", true], ["cngIdv", "CNG IDV", true], ["accessoriesIdv", "Accessories IDV", true], ["totalIdv", "Total IDV", true], ["premium", "Premium", true], ["remarks", "Remarks"]] },
      { key: "quotes", label: "Quotes & Add-ons", fields: [["quoteCompany", "Quote Insurance Company"], ["coverageType", "Coverage Type"], ["ownDamage", "Own Damage", true], ["thirdParty", "Third Party", true], ["basicOwnDamage", "Basic Own Damage", true], ["basicThirdParty", "Basic Third Party", true], ["addons", "Add-ons"], ["addonAmounts", "Add-on Amounts", true], ["gst", "GST", true], ["quoteTotal", "Quote Total", true], ["acceptedQuote", "Accepted Quote"]] },
      { key: "payment", label: "Payment", fields: [["entryType", "Entry Type"], ["paidBy", "Paid By"], ["amount", "Amount"], ["paymentDate", "Payment Date"], ["paymentMode", "Payment Mode"], ["referenceUtr", "Reference / UTR"], ["paymentRemarks", "Payment Remarks"], ["customerSettled", "Customer Settled", true], ["insurerSettled", "Insurer Settled", true], ["subventionRefund", "Subvention Refund", true], ["balanceDue", "Balance Due", true]] },
      { key: "documents", label: "Documents", fields: [["policyCopy", "Policy Copy"], ["rcCopy", "RC Copy"], ["invoice", "Invoice"], ["panDocument", "PAN Document"], ["aadhaarDocument", "Aadhaar Document"], ["gstDocument", "GST Document"], ["kycDocuments", "KYC Documents"], ["documentTag", "Document Tag"]] },
      { key: "renewal", label: "Renewal", fields: [["previousPolicy", "Previous Policy"], ["renewalStatus", "Renewal Status"], ["nextRenewalDate", "Next Renewal Date"], ["renewalPremium", "Renewal Premium", true], ["renewalPayment", "Renewal Payment", true]] },
    ],
  },
  {
    key: "loans",
    label: "Loans",
    sections: [
      { key: "customer", label: "Customer & KYC", fields: [["personalDetails", "Personal Details"], ["applicantName", "Applicant Name"], ["motherName", "Mother's Name"], ["fatherName", "Father / Husband Name"], ["dob", "Date of Birth"], ["gender", "Gender"], ["maritalStatus", "Marital Status"], ["dependents", "No of Dependents"], ["education", "Education Details"], ["customerName", "Customer Name"], ["mobile", "Mobile Number"], ["email", "Email Address"], ["address", "Address"], ["pincode", "Pincode"], ["city", "City"], ["house", "House"], ["panNumber", "PAN Number"], ["aadhaarNumber", "Aadhaar Number"], ["passportFront", "Passport Front"], ["passportBack", "Passport Back"], ["financialDetails", "Financial Details"], ["occupation", "Occupation"], ["professionalType", "Professional Type"], ["companyType", "Type of Company"], ["businessNature", "Nature of Business"], ["employerDetail", "Employer / Business Detail"], ["designation", "Designation"], ["monthlyIncome", "Monthly Income"], ["occupationDetails", "Occupation Details"], ["kycDetails", "KYC Details"], ["bankDetails", "Bank Details"], ["coApplicantDetails", "Co-Applicant Details"], ["guarantorDetails", "Guarantor Details"]] },
      { key: "vehicle", label: "Vehicle & Pricing", fields: [["vehicleDetails", "Vehicle Details"], ["registrationNumber", "Registration Number"], ["make", "Vehicle Make"], ["model", "Model"], ["variant", "Variant"], ["exShowroomPrice", "Ex-Showroom Price"], ["onRoadPrice", "On-Road Price", true], ["additions", "Price Additions"], ["discounts", "Price Discounts"], ["totalVehiclePrice", "Total Vehicle Price", true]] },
      { key: "loan", label: "Car Loan", fields: [["loanDetails", "Loan Details"], ["loanAmount", "Loan Amount"], ["tenure", "Tenure"], ["interestRate", "Interest Rate"], ["emi", "EMI", true], ["approvalDetails", "Approval Details"], ["approvalStatus", "Approval Status"], ["disbursalDetails", "Disbursal Details"], ["disbursalDate", "Disbursal Date"], ["loanNumber", "Loan Number"], ["rcDetails", "RC Details"], ["invoiceDetails", "Invoice Details"]] },
      { key: "postFile", label: "Post-File", fields: [["livePos", "Live POS", true], ["instrumentDetails", "Instrument Details"], ["repaymentSchedule", "Repayment Schedule", true], ["principalOutstanding", "Principal Outstanding", true], ["vehicleVerification", "Vehicle Verification"], ["pendency", "Pendency"]] },
      { key: "payout", label: "Payout & DO", fields: [["payoutDetails", "Payout Details"], ["yearWisePayout", "Year-wise Payout", true], ["doDetails", "DO Details"], ["receivables", "DO Receivables", true], ["payables", "DO Payables", true], ["payoutBalance", "Payout Balance", true]] },
      { key: "documents", label: "Documents", fields: [["documents", "Documents"], ["kycDocuments", "KYC Documents"], ["loanAgreement", "Loan Agreement"], ["rcDocument", "RC Document"], ["invoiceDocument", "Invoice Document"]] },
      { key: "payments", label: "Payments", fields: [["paymentDetails", "Payment Details"], ["paymentEntries", "Payment Entries"], ["paymentMode", "Payment Mode"], ["paymentReference", "Payment Reference"], ["amountPaid", "Amount Paid", true], ["balanceDue", "Balance Due", true]] },
    ],
  },
  {
    key: "homeLoans",
    label: "Home Loans",
    sections: [
      { key: "customer", label: "Customer & KYC", fields: [["personalDetails", "Personal Details"], ["applicantName", "Applicant Name"], ["motherName", "Mother's Name"], ["fatherName", "Father / Husband Name"], ["dob", "Date of Birth"], ["gender", "Gender"], ["maritalStatus", "Marital Status"], ["dependents", "No of Dependents"], ["education", "Education Details"], ["customerName", "Customer Name"], ["mobile", "Mobile Number"], ["email", "Email Address"], ["address", "Address"], ["pincode", "Pincode"], ["city", "City"], ["house", "House"], ["panNumber", "PAN Number"], ["aadhaarNumber", "Aadhaar Number"], ["passportFront", "Passport Front"], ["passportBack", "Passport Back"], ["incomeDetails", "Income Details"], ["occupation", "Occupation"], ["professionalType", "Professional Type"], ["companyType", "Type of Company"], ["businessNature", "Nature of Business"], ["employerDetail", "Employer / Business Detail"], ["designation", "Designation"], ["monthlyIncome", "Monthly Income"], ["occupationDetails", "Occupation Details"], ["kycDetails", "KYC Details"], ["bankDetails", "Bank Details"], ["coApplicantDetails", "Co-Applicant Details"], ["guarantorDetails", "Guarantor Details"]] },
      { key: "property", label: "Property", fields: [["propertyDetails", "Property Details"], ["propertyAddress", "Property Address"], ["propertyType", "Property Type"], ["builderDetails", "Builder Details"], ["propertyValue", "Property Value"], ["registrationDetails", "Registration Details"]] },
      { key: "loan", label: "Home Loan", fields: [["loanDetails", "Loan Details"], ["loanAmount", "Loan Amount"], ["tenure", "Tenure"], ["interestRate", "Interest Rate"], ["emi", "EMI", true], ["approvalDetails", "Approval Details"], ["approvalStatus", "Approval Status"], ["disbursalDetails", "Disbursal Details"]] },
      { key: "payments", label: "Payments", fields: [["paymentDetails", "Payment Details"], ["paymentEntries", "Payment Entries"], ["amountPaid", "Amount Paid", true], ["balanceDue", "Balance Due", true]] },
      { key: "documents", label: "Documents", fields: [["documents", "Documents"], ["kycDocuments", "KYC Documents"], ["propertyDocuments", "Property Documents"], ["loanAgreement", "Loan Agreement"]] },
    ],
  },
];

export const getRolePermissions = asyncHandler(async (req, res) => {
  const saved = await RolePermission.find({}).select("role permissions updatedAt").lean();
  const byRole = new Map(saved.map((item) => [item.role, item]));
  res.json({
    success: true,
    data: SYSTEM_ROLES.map((role) => ({ role, permissions: byRole.get(role)?.permissions || {} })),
    catalog: PERMISSION_CATALOG,
  });
});

export const updateRolePermissions = asyncHandler(async (req, res) => {
  const role = String(req.params.role || "").trim().toLowerCase();
  if (!SYSTEM_ROLES.includes(role)) {
    res.status(400);
    throw new Error("Invalid system role");
  }
  if (role === "superadmin") {
    res.status(400);
    throw new Error("Superadmin permissions cannot be restricted");
  }
  const permissions = req.body?.permissions;
  if (!permissions || typeof permissions !== "object" || Array.isArray(permissions)) {
    res.status(400);
    throw new Error("Permissions must be an object");
  }
  const saved = await RolePermission.findOneAndUpdate(
    { role },
    { $set: { permissions, updatedBy: req.user._id } },
    { new: true, upsert: true, setDefaultsOnInsert: true, runValidators: true },
  ).lean();
  res.json({ success: true, data: saved });
});
