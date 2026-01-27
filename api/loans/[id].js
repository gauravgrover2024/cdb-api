import withCors from "../_cors.js";
import { getDb } from "../_db.js";
import { ObjectId } from "mongodb";

/**
 * /api/loans/:id
 * GET    -> fetch loan for edit
 * PUT    -> update loan
 * DELETE -> delete loan
 */
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

    // ------------------------
    // GET /api/loans/:id
    // ------------------------
    if (req.method === "GET") {
      const loan = await loansCol.findOne({ _id });

      if (!loan) {
        return res.status(404).json({
          success: false,
          error: "Loan not found",
        });
      }

      return res.status(200).json({
        success: true,
        data: loan,
      });
    }

    // ------------------------
    // PUT /api/loans/:id
    // ------------------------
    if (req.method === "PUT") {
      const payload = { ...req.body };

      // 🚫 NEVER allow _id to be updated
      delete payload._id;
      delete payload.loanId; // optional but safe

      await loansCol.updateOne(
        { _id },
        {
          $set: {
            ...payload,
            updatedAt: new Date().toISOString(),
          },
        },
      );

      return res.status(200).json({
        success: true,
        data: { id },
      });
    }

    // ------------------------
    // DELETE /api/loans/:id
    // ------------------------
    if (req.method === "DELETE") {
      await loansCol.deleteOne({ _id });

      return res.status(200).json({
        success: true,
        data: { id },
      });
    }

    return res.status(405).json({
      success: false,
      error: "Method not allowed",
    });
  } catch (err) {
    console.error("Loan ID API error:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}

export default withCors(handler);
