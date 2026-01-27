import { getDb } from "../_db.js";
import withCors from "../_cors.js";

async function handler(req, res) {
  try {
    const { q } = req.query;
    if (!q) {
      return res.status(200).json({ success: true, data: [] });
    }

    const db = await getDb();
    const customersCol = db.collection("customers");

    const regex = new RegExp(q, "i");

    const customers = await customersCol
      .find({
        $or: [
          { customerName: regex },
          { primaryMobile: regex },
          { customerNumber: regex },
          { email: regex },
        ],
      })
      .limit(20)
      .toArray();

    return res.status(200).json({
      success: true,
      data: customers,
    });
  } catch (err) {
    console.error("Customer search error:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}

export default withCors(handler);
