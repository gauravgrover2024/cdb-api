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
        ["customerName", "Customer Name"], ["mobile", "Mobile"], ["email", "Email"],
        ["address", "Address"], ["panNumber", "PAN Number"], ["aadhaarNumber", "Aadhaar Number"],
        ["bankDetails", "Bank Details"], ["incomeDetails", "Income Details"], ["documents", "Documents"],
      ] },
    ],
  },
  {
    key: "insurance",
    label: "Insurance",
    sections: [
      { key: "customer", label: "Customer", fields: [["customerName", "Customer Name"], ["mobile", "Mobile"], ["panNumber", "PAN Number"], ["aadhaarNumber", "Aadhaar Number"]] },
      { key: "vehicle", label: "Vehicle", fields: [["registrationNumber", "Registration Number"], ["make", "Make"], ["model", "Model"], ["variant", "Variant"], ["engineNumber", "Engine Number"], ["chassisNumber", "Chassis Number"]] },
      { key: "policy", label: "Policy", fields: [["insuranceCompany", "Insurance Company"], ["policyNumber", "Policy Number"], ["premium", "Premium"], ["policyDates", "Policy Dates"]] },
      { key: "payment", label: "Payment", fields: [["paymentDetails", "Payment Details"]] },
      { key: "documents", label: "Documents", fields: [["policyCopy", "Policy Copy"], ["kycDocuments", "KYC Documents"]] },
    ],
  },
  {
    key: "loans",
    label: "Loans",
    sections: [
      { key: "customer", label: "Customer", fields: [["personalDetails", "Personal Details"], ["financialDetails", "Financial Details"], ["kycDetails", "KYC Details"], ["bankDetails", "Bank Details"]] },
      { key: "loan", label: "Loan", fields: [["loanDetails", "Loan Details"], ["approvalDetails", "Approval Details"], ["disbursalDetails", "Disbursal Details"]] },
      { key: "documents", label: "Documents", fields: [["documents", "Documents"]] },
      { key: "payments", label: "Payments", fields: [["paymentDetails", "Payment Details"]] },
    ],
  },
  {
    key: "homeLoans",
    label: "Home Loans",
    sections: [
      { key: "customer", label: "Customer", fields: [["personalDetails", "Personal Details"], ["incomeDetails", "Income Details"], ["kycDetails", "KYC Details"]] },
      { key: "loan", label: "Loan", fields: [["propertyDetails", "Property Details"], ["loanDetails", "Loan Details"], ["approvalDetails", "Approval Details"]] },
      { key: "documents", label: "Documents", fields: [["documents", "Documents"]] },
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
