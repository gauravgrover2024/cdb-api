import { getDb } from "./_db.js";

export default async function handler(req, res) {
  try {
    if (req.method !== "GET") {
      return res
        .status(405)
        .json({ success: false, error: "Method not allowed" });
    }

    const db = await getDb();
    const customersCol = db.collection("customers");

    const limit = Math.min(parseInt(req.query.limit || "50", 10), 200);
    const skip = Math.max(parseInt(req.query.skip || "0", 10), 0);

    const customers = await customersCol
      .find({})
      .sort({ _id: -1 })
      .skip(skip)
      .limit(limit)
      .toArray();

    return res.status(200).json({ success: true, data: customers });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
}
