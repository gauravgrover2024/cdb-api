import { getDb } from "../_db.js";
import { applyCors } from "../_cors.js";

export default async function handler(req, res) {
  if (applyCors(req, res)) return;

  if (req.method !== "GET") {
    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  }

  try {
    const q = (req.query.q || "").trim();
    if (!q) {
      return res.status(200).json({ success: true, data: [] });
    }

    const db = await getDb();
    const customers = db.collection("customers");

    const regex = new RegExp(q, "i");

    const list = await customers
      .find({
        $or: [
          { customerName: regex },
          { primaryMobile: regex },
          { customerNumber: regex },
          { panNumber: regex },
        ],
      })
      .project({
        customerName: 1,
        customerNumber: 1,
        primaryMobile: 1,
        email: 1,
        city: 1,
        // add more only if needed
      })
      .limit(15)
      .toArray();

    return res.status(200).json({
      success: true,
      data: list,
    });
  } catch (err) {
    console.error("Customer search error:", err);
    return res.status(500).json({
      success: false,
      error: "Search failed",
    });
  }
}
