import { getDb } from "./_db.js";

export default async function handler(req, res) {
  try {
    const db = await getDb();
    const loansCol = db.collection("loans"); // we will create this collection

    if (req.method === "GET") {
      const limit = Math.min(parseInt(req.query.limit || "50", 10), 200);
      const skip = Math.max(parseInt(req.query.skip || "0", 10), 0);

      const loans = await loansCol
        .find({})
        .sort({ _id: -1 })
        .skip(skip)
        .limit(limit)
        .toArray();

      return res.status(200).json({ success: true, data: loans });
    }

    if (req.method === "POST") {
      const body = req.body || {};

      const doc = {
        ...body,
        createdAt: new Date(),
        updatedAt: new Date(),
      };

      const result = await loansCol.insertOne(doc);

      return res.status(201).json({
        success: true,
        data: { _id: result.insertedId, ...doc },
      });
    }

    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
}
