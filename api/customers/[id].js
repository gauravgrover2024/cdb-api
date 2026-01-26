import { ObjectId } from "mongodb";
import { getDb } from "../_db.js";
import { applyCors } from "../_cors.js";

export default async function handler(req, res) {
  if (applyCors(req, res)) return;

  try {
    const db = await getDb();
    const customersCol = db.collection("customers");

    const { id } = req.query;

    if (!id) {
      return res.status(400).json({ success: false, error: "Missing id" });
    }

    let _id;
    try {
      _id = new ObjectId(id);
    } catch {
      return res.status(400).json({ success: false, error: "Invalid id" });
    }

    // -----------------------------
    // GET /api/customers/:id
    // -----------------------------
    if (req.method === "GET") {
      const customer = await customersCol.findOne({ _id });

      if (!customer) {
        return res
          .status(404)
          .json({ success: false, error: "Customer not found" });
      }

      return res.status(200).json({ success: true, data: customer });
    }

    // -----------------------------
    // PUT /api/customers/:id
    // -----------------------------
    if (req.method === "PUT") {
      const body = req.body || {};
      const now = new Date();

      const updateDoc = {
        ...body,
        updatedAt: now.toISOString(),
      };

      // never allow changing _id
      delete updateDoc._id;

      const result = await customersCol.findOneAndUpdate(
        { _id },
        { $set: updateDoc },
        { returnDocument: "after" },
      );

      if (!result?.value) {
        return res
          .status(404)
          .json({ success: false, error: "Customer not found" });
      }

      return res.status(200).json({ success: true, data: result.value });
    }

    // -----------------------------
    // DELETE /api/customers/:id
    // -----------------------------
    if (req.method === "DELETE") {
      const result = await customersCol.deleteOne({ _id });

      if (result.deletedCount === 0) {
        return res
          .status(404)
          .json({ success: false, error: "Customer not found" });
      }

      return res.status(200).json({ success: true, data: { deleted: true } });
    }

    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
}
