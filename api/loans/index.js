import { getDb } from "../_db.js";
import withCors from "../_cors.js";

async function handler(req, res) {
  try {
    const db = await getDb();
    const loansCol = db.collection("loans");

    // ---------- GET /api/loans ----------
    if (req.method === "GET") {
      const loans = await loansCol.find({}).sort({ createdAt: -1 }).toArray();

      return res.status(200).json({
        success: true,
        data: loans,
      });
    }

    // ---------- POST /api/loans ----------
    // ---------- POST /api/loans ----------
    if (req.method === "POST") {
      const payload = req.body || {};
      const now = new Date().toISOString();

      const result = await loansCol.insertOne({
        ...payload,
        loanId: null, // temp placeholder
        createdAt: now,
        updatedAt: now,
      });

      const loanId = result.insertedId.toString();

      // 🔥 persist loanId inside the document
      await loansCol.updateOne(
        { _id: result.insertedId },
        { $set: { loanId } },
      );

      return res.status(201).json({
        success: true,
        loanId,
        createdAt: now,
      });
    }

    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    console.error("Loans API error:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}

export default withCors(handler);
