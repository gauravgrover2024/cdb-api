// api/loans/index.js
import { getDb } from "../_db.js";
import withCors from "../_cors.js";

async function handler(req, res) {
  try {
    const db = await getDb();
    const loansCol = db.collection("loans");

    // ✅ GET /api/loans
    if (req.method === "GET") {
      const loans = await loansCol.find({}).sort({ createdAt: -1 }).toArray();

      return res.status(200).json({
        success: true,
        data: loans,
      });
    }

    // ✅ POST /api/loans
    if (req.method === "POST") {
      const payload = req.body || {};

      const doc = {
        ...payload,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        status: payload.status || "Pending",
      };

      const result = await loansCol.insertOne(doc);

      return res.status(201).json({
        success: true,
        loanId: result.insertedId.toString(),
        createdAt: doc.createdAt,
      });
    }

    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    console.error("Loans API error:", err);
    return res.status(500).json({
      success: false,
      error: err.message || "Internal server error",
    });
  }
}

export default withCors(handler);
