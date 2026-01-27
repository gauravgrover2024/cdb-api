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
    if (req.method === "POST") {
      const payload = req.body || {};
      const now = new Date().toISOString();

      // 🔒 never trust frontend loanId
      delete payload.loanId;

      const doc = {
        ...payload,
        createdAt: now,
        updatedAt: now,
      };

      // 1️⃣ insert
      const result = await loansCol.insertOne(doc);

      // 2️⃣ generate business loanId
      const loanId = result.insertedId.toString();

      // 3️⃣ write it back into the same document
      await loansCol.updateOne(
        { _id: result.insertedId },
        { $set: { loanId } },
      );

      // 4️⃣ respond
      return res.status(201).json({
        success: true,
        data: {
          loanId,
          _id: loanId,
          createdAt: now,
        },
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
