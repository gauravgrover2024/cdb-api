import express from "express";
import Quotation from "../models/Quotation.js";

const router = express.Router();

// POST /api/quotations
router.post("/", async (req, res) => {
  try {
    const quotation = new Quotation(req.body);
    await quotation.save();
    res.status(201).json(quotation);
  } catch (err) {
    console.error("Create quotation error:", err);
    res.status(400).json({ message: "Failed to create quotation" });
  }
});

// GET /api/quotations/:id
router.get("/:id", async (req, res) => {
  try {
    const q = await Quotation.findById(req.params.id);
    if (!q) return res.status(404).json({ message: "Not found" });
    res.json(q);
  } catch {
    res.status(400).json({ message: "Invalid id" });
  }
});

// GET /api/quotations
router.get("/", async (req, res) => {
  const { page = 1, limit = 20 } = req.query;
  const skip = (Number(page) - 1) * Number(limit);
  const [items, total] = await Promise.all([
    Quotation.find().sort({ createdAt: -1 }).skip(skip).limit(Number(limit)),
    Quotation.countDocuments(),
  ]);
  res.json({ items, total, page: Number(page), limit: Number(limit) });
});

// DELETE /api/quotations/:id
router.delete("/:id", async (req, res) => {
  await Quotation.findByIdAndDelete(req.params.id);
  res.status(204).end();
});

export default router;
