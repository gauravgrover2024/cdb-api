import withCors from "../_cors.js";
import { getDb } from "../_db.js";

async function handler(req, res) {
  try {
    const { q = "" } = req.query;

    if (!q || q.trim().length < 1) {
      return res.status(200).json({
        success: true,
        data: [],
      });
    }

    const db = await getDb();
    const customersCol = db.collection("customers");

    // 🔍 case-insensitive search
    const regex = new RegExp(q, "i");

    const customers = await customersCol
      .find({
        $or: [
          { customerName: regex },
          { primaryMobile: regex },
          { customerNumber: regex },
        ],
      })
      .limit(10)
      .toArray();

    return res.status(200).json({
      success: true,
      data: customers,
    });
  } catch (err) {
    console.error("Customer search API error:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}

export default withCors(handler);
