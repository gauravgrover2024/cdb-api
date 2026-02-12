import { getDb } from "../_db.js";
import withCors from "../_cors.js";

async function handler(req, res) {
  try {
    const db = await getDb();
    const customersCol = db.collection("customers");

    if (req.method === "GET") {
      // Pagination params
      const page = parseInt(req.query.page || "1", 10);
      const limit = parseInt(req.query.limit || "20", 10);

      const safeLimit = Math.min(limit, 100); // safety cap
      const skip = (page - 1) * safeLimit;

      // Total count
      const total = await customersCol.countDocuments();

      // Fetch paginated data
      const customers = await customersCol
        .find({})
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(safeLimit)
        .toArray();

      return res.status(200).json({
        success: true,
        total,
        page,
        limit: safeLimit,
        data: customers,
      });
    }

    return res.status(405).json({
      success: false,
      error: "Method not allowed",
    });
  } catch (err) {
    console.error("Customers list error:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}

export default withCors(handler);
