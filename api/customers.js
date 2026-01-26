import { ObjectId } from "mongodb";
import { getDb } from "./_db.js";

export default async function handler(req, res) {
  try {
    const db = await getDb();
    const customersCol = db.collection("customers");

    // -----------------------------
    // GET /api/customers  (list)
    // -----------------------------
    if (req.method === "GET") {
      const limit = Math.min(parseInt(req.query.limit || "50", 10), 200);
      const skip = Math.max(parseInt(req.query.skip || "0", 10), 0);

      const customers = await customersCol
        .find({})
        .sort({ _id: -1 })
        .skip(skip)
        .limit(limit)
        .toArray();

      return res.status(200).json({ success: true, data: customers });
    }

    // -----------------------------
    // POST /api/customers  (create)
    // -----------------------------
    if (req.method === "POST") {
      const body = req.body || {};

      const now = new Date();

      const doc = {
        ...body,
        createdAt: now.toISOString(),
        updatedAt: now.toISOString(),
      };

      const result = await customersCol.insertOne(doc);

      return res.status(201).json({
        success: true,
        data: { _id: result.insertedId, ...doc },
      });
    }

    // -----------------------------
    // PUT /api/customers?id=XXXX  (update)
    // NOTE: CRA frontend will call /api/customers/:id
    // But Vercel serverless does not support params in same file,
    // so we will handle /api/customers/[id].js separately.
    // -----------------------------
    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
}
