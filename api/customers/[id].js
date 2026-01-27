import { ObjectId } from "mongodb";
import { getDb } from "../_db.js";
import withCors from "../_cors.js";

async function handler(req, res) {
  try {
    const db = await getDb();
    const customersCol = db.collection("customers");

    const { id } = req.query;
    if (!id || !ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: "Invalid customer id",
      });
    }

    const _id = new ObjectId(id);

    // ---------- GET /api/customers/:id ----------
    if (req.method === "GET") {
      const customer = await customersCol.findOne({ _id });

      if (!customer) {
        return res.status(404).json({
          success: false,
          error: "Customer not found",
        });
      }

      return res.status(200).json({
        success: true,
        data: customer,
      });
    }

    // ---------- PUT /api/customers/:id ----------
    if (req.method === "PUT") {
      const body = req.body || {};

      delete body._id; // safety

      const result = await customersCol.findOneAndUpdate(
        { _id },
        {
          $set: {
            ...body,
            updatedAt: new Date().toISOString(),
          },
        },
        { returnDocument: "after" },
      );

      if (!result?.value) {
        return res.status(404).json({
          success: false,
          error: "Customer not found",
        });
      }

      return res.status(200).json({
        success: true,
        data: result.value,
      });
    }

    // ---------- DELETE /api/customers/:id ----------
    if (req.method === "DELETE") {
      const result = await customersCol.deleteOne({ _id });

      if (!result.deletedCount) {
        return res.status(404).json({
          success: false,
          error: "Customer not found",
        });
      }

      return res.status(200).json({
        success: true,
        deleted: true,
      });
    }

    return res.status(405).json({
      success: false,
      error: "Method not allowed",
    });
  } catch (err) {
    console.error("Customer ID API error:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}

export default withCors(handler);
