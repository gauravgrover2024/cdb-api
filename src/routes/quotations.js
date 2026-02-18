import express from "express";
import Quotation from "../models/Quotation.js";

const router = express.Router();

// Debug: log pricing paths once
console.log(
  "Quotation pricing paths:",
  Object.keys(Quotation.schema.paths).filter((p) => p.startsWith("pricing.")),
);

// POST /api/quotations
router.post("/", async (req, res) => {
  try {
    const payload = req.body;

    console.log(
      "POST /api/quotations payload._id:",
      payload._id,
      "customer:",
      payload.customer?.customerName,
      payload.customer?.primaryMobile,
    );

    // 1) If _id is present, update that document directly and RETURN
    if (payload._id) {
      const existingById = await Quotation.findById(payload._id);
      if (existingById) {
        existingById.set(payload);
        await existingById.save();
        return res.status(200).json(existingById);
      }
      // If no doc found with that _id, drop it so we can safely create new
      delete payload._id;
    }

    const cust = payload.customer;
    const veh = payload.vehicle;

    if (!cust || !veh) {
      return res.status(400).json({ message: "Missing customer or vehicle" });
    }

    // 2) Optional upsert by customer + vehicle (only if we have enough info)
    const customerMatch = {};
    if (cust.customerId) {
      customerMatch["customer.customerId"] = cust.customerId;
    } else if (cust.customerName && cust.primaryMobile) {
      customerMatch["customer.customerName"] = cust.customerName;
      customerMatch["customer.primaryMobile"] = cust.primaryMobile;
    }

    const vehicleMatch = {
      "vehicle.make": veh.make,
      "vehicle.model": veh.model,
      "vehicle.variant": veh.variant,
    };

    const matchFilter =
      Object.keys(customerMatch).length > 0
        ? { ...customerMatch, ...vehicleMatch }
        : null;

    if (matchFilter) {
      const existing = await Quotation.findOne(matchFilter).sort({
        createdAt: -1,
      });
      if (existing) {
        existing.cityTyped = payload.cityTyped;
        existing.vehicleCity = payload.vehicleCity;
        existing.vehicle = payload.vehicle;
        existing.pricing = payload.pricing;
        existing.scenarios = payload.scenarios;
        if (payload.status) existing.status = payload.status;
        await existing.save();
        return res.status(200).json(existing);
      }
    }

    // 3) No id match and no customer+vehicle match → create new
    const quotation = new Quotation(payload);
    await quotation.save();
    res.status(201).json(quotation);
  } catch (err) {
    console.error("Create quotation error:", err);
    console.error("Validation errors:", err?.errors);
    res
      .status(400)
      .json({ message: err.message || "Failed to create quotation" });
  }
});

// GET /api/quotations/:id/pdf (stub)
router.get("/:id/pdf", async (req, res) => {
  try {
    const q = await Quotation.findById(req.params.id);
    if (!q) return res.status(404).json({ message: "Not found" });

    return res.json({
      message: "PDF generation not yet implemented",
      quotationId: q._id,
    });
  } catch (err) {
    console.error("PDF error:", err);
    res.status(500).json({ message: "Failed to generate PDF" });
  }
});

// GET /api/quotations/:id
router.get("/:id", async (req, res) => {
  try {
    const q = await Quotation.findById(req.params.id);
    if (!q) return res.status(404).json({ message: "Not found" });
    res.json(q);
  } catch (err) {
    console.error("Get quotation error:", err);
    res.status(400).json({ message: "Invalid id" });
  }
});

// GET /api/quotations (list with filters)
router.get("/", async (req, res) => {
  try {
    const { page = 1, limit = 20, q, status, from, to } = req.query;

    const filter = {};

    if (status) {
      filter.status = status;
    }

    if (q) {
      const regex = new RegExp(q, "i");
      filter.$or = [
        { "customer.customerName": regex },
        { "customer.primaryMobile": regex },
        { "vehicle.make": regex },
        { "vehicle.model": regex },
        { "vehicle.variant": regex },
        { cityTyped: regex },
      ];
    }

    if (from && to) {
      filter.createdAt = {
        $gte: new Date(from),
        $lte: new Date(to),
      };
    }

    const pageNum = Number(page) || 1;
    const limitNum = Number(limit) || 20;
    const skip = (pageNum - 1) * limitNum;

    const [items, total] = await Promise.all([
      Quotation.find(filter).sort({ createdAt: -1 }).skip(skip).limit(limitNum),
      Quotation.countDocuments(filter),
    ]);

    res.json({ items, total, page: pageNum, limit: limitNum });
  } catch (err) {
    console.error("List quotations error:", err);
    res.status(500).json({ message: "Failed to fetch quotations" });
  }
});

// DELETE /api/quotations/:id
router.delete("/:id", async (req, res) => {
  try {
    await Quotation.findByIdAndDelete(req.params.id);
    res.status(204).end();
  } catch (err) {
    console.error("Delete quotation error:", err);
    res.status(400).json({ message: "Failed to delete quotation" });
  }
});

export default router;
