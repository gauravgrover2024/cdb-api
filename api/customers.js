// api/customers.js
import { getDb } from "./_db.js";
import withCors from "./_cors.js";

// Escape regex to prevent injection / catastrophic backtracking
const escapeRegex = (text = "") => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

async function handler(req, res) {
  try {
    const db = await getDb();
    const customersCol = db.collection("customers");

    /* =========================================================
       GET /api/customers  (Paginated + Search + Filter + Sort)
    ========================================================== */
    if (req.method === "GET") {
      const page = Math.max(parseInt(req.query.page || "1", 10), 1);
      const limit = Math.min(parseInt(req.query.limit || "10", 10), 200);
      const skip = (page - 1) * limit;

      const search = (req.query.search || "").trim();
      const kycStatus = req.query.kycStatus || "";
      const customerType = req.query.customerType || "";

      const sortBy = req.query.sortBy || "createdAt";
      const order = req.query.order === "asc" ? 1 : -1;

      let query = {};

      /* ---------- Search ---------- */
      if (search) {
        const safeSearch = escapeRegex(search);

        query.$or = [
          { customerName: { $regex: safeSearch, $options: "i" } },
          { primaryMobile: { $regex: safeSearch, $options: "i" } },
          { panNumber: { $regex: safeSearch, $options: "i" } },
          { city: { $regex: safeSearch, $options: "i" } },
          { companyName: { $regex: safeSearch, $options: "i" } },
        ];
      }

      /* ---------- KYC Filter (case-insensitive exact) ---------- */
      if (kycStatus) {
        query.kycStatus = {
          $regex: `^${escapeRegex(kycStatus)}$`,
          $options: "i",
        };
      }

      /* ---------- Customer Type Filter ---------- */
      if (customerType) {
        query.customerType = {
          $regex: `^${escapeRegex(customerType)}$`,
          $options: "i",
        };
      }

      const total = await customersCol.countDocuments(query);

      const customers = await customersCol
        .find(query)
        .sort({ [sortBy]: order })
        .skip(skip)
        .limit(limit)
        .toArray();

      return res.status(200).json({
        success: true,
        data: customers,
        total,
        page,
        totalPages: Math.ceil(total / limit),
        limit,
      });
    }

    /* =========================================================
       POST /api/customers (Create with Duplicate Protection)
    ========================================================== */
    if (req.method === "POST") {
      const body = req.body || {};
      const now = new Date().toISOString();

      const doc = {
        ...body,
        createdAt: now,
        updatedAt: now,
      };

      /* ---------- Smart Duplicate Detection ---------- */
      let duplicateQuery = [];

      if (doc.panNumber) {
        duplicateQuery.push({ panNumber: doc.panNumber });
      }

      if (doc.primaryMobile) {
        duplicateQuery.push({ primaryMobile: doc.primaryMobile });
      }

      let existing = null;

      if (duplicateQuery.length > 0) {
        existing = await customersCol.findOne({
          $or: duplicateQuery,
        });
      }

      if (existing) {
        return res.status(200).json({
          success: true,
          duplicate: true,
          data: existing,
        });
      }

      const result = await customersCol.insertOne(doc);

      return res.status(201).json({
        success: true,
        data: {
          _id: result.insertedId,
          ...doc,
        },
      });
    }

    /* =========================================================
       Method Not Allowed
    ========================================================== */
    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    console.error("❌ /api/customers error:", err);
    return res.status(500).json({
      success: false,
      error: err.message || "Internal server error",
    });
  }
}

export default withCors(handler);
