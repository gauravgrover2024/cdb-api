import { getDb } from "../_db.js";
import withCors from "../_cors.js";
import { ObjectId } from "mongodb";

/**
 * Same sanitizer as loans.js
 * Keeps everything, but guarantees dashboard fields
 */
const sanitizeLoanPayload = (payload = {}) => {
  return {
    customerName: payload.customerName || "",
    primaryMobile: payload.primaryMobile || "",

    vehicleMake: payload.vehicleMake || "",
    vehicleModel: payload.vehicleModel || "",

    typeOfLoan: payload.typeOfLoan || payload.loanType || "",
    isFinanced: payload.isFinanced === "No" ? "No" : "Yes",

    currentStage: payload.currentStage || "profile",
    status: payload.status || payload.approval_status || "Pending",

    approval_status: payload.approval_status || "",
    approval_bankName: payload.approval_bankName || "",
    approval_loanAmountApproved:
      Number(payload.approval_loanAmountApproved) || 0,

    ...payload,
  };
};

async function handler(req, res) {
  try {
    const { id } = req.query;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: "Invalid loan id",
      });
    }

    const db = await getDb();
    const loansCol = db.collection("loans");
    const _id = new ObjectId(id);

    /**
     * GET /api/loans/:id
     */
    if (req.method === "GET") {
      const loan = await loansCol.findOne({ _id });

      if (!loan) {
        return res
          .status(404)
          .json({ success: false, error: "Loan not found" });
      }

      return res.status(200).json(loan);
    }

    /**
     * PUT /api/loans/:id
     */
    if (req.method === "PUT") {
      const cleanPayload = sanitizeLoanPayload(req.body);

      await loansCol.updateOne(
        { _id },
        {
          $set: {
            ...cleanPayload,
            updatedAt: new Date().toISOString(),
          },
        },
      );

      return res.status(200).json({ success: true });
    }

    /**
     * DELETE /api/loans/:id
     */
    if (req.method === "DELETE") {
      await loansCol.deleteOne({ _id });
      return res.status(200).json({ success: true });
    }

    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    console.error("Loan ID API error:", err);
    return res.status(500).json({ success: false, error: err.message });
  }
}

export default withCors(handler);
