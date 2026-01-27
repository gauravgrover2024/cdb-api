import { getDb } from "../_db.js";
import { applyCors } from "../_cors.js";

export default async function handler(req, res) {
  if (applyCors(req, res)) return;

  try {
    const db = await getDb();
    const loansCol = db.collection("loans");

    if (req.method === "GET") {
      const loans = await loansCol.find({}).sort({ createdAt: -1 }).toArray();

      return res.status(200).json({
        success: true,
        data: loans,
      });
    }

    if (req.method === "POST") {
      const payload = req.body;

      const result = await loansCol.insertOne({
        ...payload,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      });

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
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}
