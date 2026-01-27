import { ObjectId } from "mongodb";
import { getDb } from "../_db.js";
import withCors from "../_cors.js";

async function handler(req, res) {
  try {
    const db = await getDb();
    const customersCol = db.collection("customers");

    const { id } = req.query;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ success: false, error: "Invalid id" });
    }

    const _id = new ObjectId(id);

    if (req.method === "GET") {
      const customer = await customersCol.findOne({ _id });
      if (!customer) {
        return res
          .status(404)
          .json({ success: false, error: "Customer not found" });
      }

      return res.status(200).json({ success: true, data: customer });
    }

    if (req.method === "PUT") {
      const body = req.body || {};
      const result = await customersCol.findOneAndUpdate(
        { _id },
        { $set: { ...body, updatedAt: new Date().toISOString() } },
        { returnDocument: "after" },
      );

      return res.status(200).json({ success: true, data: result.value });
    }

    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    console.error("Customer API error:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}

export default withCors(handler);
