import { getDb } from "./_db.js";
import withCors from "./_cors.js";

/**
 * This function:
 * - keeps ALL loan data
 * - but ensures dashboard-critical fields always exist
 */
const sanitizeLoanPayload = (payload = {}) => {
  return {
    // 🔹 Dashboard critical fields
    loanId: payload.loanId,
    customerName: payload.customerName || "",
    primaryMobile: payload.primaryMobile || "",

    vehicleMake: payload.vehicleMake || "",
    vehicleModel: payload.vehicleModel || "",
    vehicleVariant: payload.vehicleVariant || "",

    typeOfLoan: payload.typeOfLoan || payload.loanType || "",
    isFinanced: payload.isFinanced === "No" ? "No" : "Yes",

    currentStage: payload.currentStage || "profile",
    status: payload.status || payload.approval_status || "Pending",

    approval_status: payload.approval_status || "",
    approval_bankName: payload.approval_bankName || "",
    approval_loanAmountApproved:
      Number(payload.approval_loanAmountApproved) || 0,

    // 🔹 KEEP EVERYTHING ELSE (important)
    ...payload,
  };
};

async function handler(req, res) {
  try {
    const db = await getDb();
    const loansCol = db.collection("loans");

    /**
     * =========================
     * GET /api/loans
     * Dashboard list + filters
     * =========================
     */
    if (req.method === "GET") {
      const { q, stage, status, loanType, minAmount, maxAmount } = req.query;

      const filter = {};

      // 🔍 Search
      if (q) {
        filter.$or = [
          { loanId: { $regex: q, $options: "i" } },
          { customerName: { $regex: q, $options: "i" } },
          { primaryMobile: { $regex: q, $options: "i" } },
          { vehicleMake: { $regex: q, $options: "i" } },
          { vehicleModel: { $regex: q, $options: "i" } },
        ];
      }

      if (stage) filter.currentStage = stage;
      if (status) filter.status = status;
      if (loanType) filter.typeOfLoan = loanType;

      if (minAmount || maxAmount) {
        filter.approval_loanAmountApproved = {};
        if (minAmount)
          filter.approval_loanAmountApproved.$gte = Number(minAmount);
        if (maxAmount)
          filter.approval_loanAmountApproved.$lte = Number(maxAmount);
      }

      const loans = await loansCol
        .find(filter)
        .sort({ updatedAt: -1 })
        .toArray();

      return res.status(200).json({
        success: true,
        data: loans,
      });
    }

    /**
     * =========================
     * POST /api/loans
     * Create new loan
     * =========================
     */
    if (req.method === "POST") {
      const cleanPayload = sanitizeLoanPayload(req.body);

      const doc = {
        ...cleanPayload,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };

      const result = await loansCol.insertOne(doc);

      return res.status(201).json({
        success: true,
        loanId: result.insertedId.toString(),
      });
    }

    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    console.error("Loans API error:", err);
    return res.status(500).json({ success: false, error: err.message });
  }
}

export default withCors(handler);
