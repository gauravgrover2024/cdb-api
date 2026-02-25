// backend/routes/bookings.js
import express from "express";
import Booking from "../models/Booking.js";

const router = express.Router();

// Generate bookingId like BKG-0001 using a counter in DB
async function generateBookingId() {
  // Count existing bookings and pad – simple but fine for now
  const count = await Booking.countDocuments({});
  const next = count + 1;
  return `BKG-${String(next).padStart(4, "0")}`;
}

// GET /api/bookings  -> list bookings with optional status filter
router.get("/", async (req, res, next) => {
  try {
    const { status, limit = 200, skip = 0 } = req.query;

    const query = {};
    if (status) {
      // keep exact status (e.g. "Open", "Cancelled", "Converted")
      query.status = status;
    }

    const bookings = await Booking.find(query)
      .sort({ createdAt: -1 })
      .skip(Number(skip) || 0)
      .limit(Number(limit) || 200);

    res.json(bookings);
  } catch (err) {
    next(err);
  }
});

// POST /api/bookings  -> create booking
router.post("/", async (req, res, next) => {
  try {
    const bookingId = await generateBookingId();

    const booking = await Booking.create({
      bookingId,
      status: "Open",
      ...req.body,
    });

    res.json(booking);
  } catch (err) {
    next(err);
  }
});

// GET /api/bookings/:id  -> fetch a booking by bookingId
router.get("/:id", async (req, res, next) => {
  try {
    const booking = await Booking.findOne({ bookingId: req.params.id });
    if (!booking) {
      res.status(404);
      return res.json({ message: "Booking not found" });
    }
    res.json(booking);
  } catch (err) {
    next(err);
  }
});

// POST /api/bookings/:id/cancel  -> cancel booking
router.post("/:id/cancel", async (req, res, next) => {
  try {
    const booking = await Booking.findOne({ bookingId: req.params.id });
    if (!booking) {
      res.status(404);
      return res.json({ message: "Booking not found" });
    }

    const { reason, remarks, cancelledAt } = req.body || {};
    booking.status = "Cancelled";
    booking.cancelReason = reason || booking.cancelReason;
    booking.cancelRemarks = remarks || booking.cancelRemarks;
    booking.cancelledAt = cancelledAt ? new Date(cancelledAt) : new Date();

    await booking.save();
    res.json(booking);
  } catch (err) {
    next(err);
  }
});

// POST /api/bookings/:id/merge-into-payment  -> stub merge
router.post("/:id/merge-into-payment", async (req, res, next) => {
  try {
    const booking = await Booking.findOne({ bookingId: req.params.id });
    if (!booking) {
      res.status(404);
      return res.json({ message: "Booking not found" });
    }

    booking.mergedIntoPaymentAt = new Date();
    booking.linkedPaymentLoanId = booking.linkedLoanId || null;

    await booking.save();
    res.json(booking);
  } catch (err) {
    next(err);
  }
});

// POST /api/bookings/:id/create-loan  -> stub loan creation
router.post("/:id/create-loan", async (req, res, next) => {
  try {
    const booking = await Booking.findOne({ bookingId: req.params.id });
    if (!booking) {
      res.status(404);
      return res.json({ message: "Booking not found" });
    }

    const loanId = `LN-${booking.bookingId}`;

    booking.linkedLoanId = loanId;
    booking.status = "Converted";
    await booking.save();

    res.json({ loanId });
  } catch (err) {
    next(err);
  }
});

export default router;
