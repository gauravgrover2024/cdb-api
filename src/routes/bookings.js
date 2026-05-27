// backend/routes/bookings.js
import express from "express";
import Booking from "../models/Booking.js";
import Counter from "../models/Counter.js";

const router = express.Router();

// Generate bookingId like BKG-0001 using a counter in DB
async function generateBookingId() {
  const key = "booking_id_sequence";
  const bumped = await Counter.findOneAndUpdate(
    { key },
    { $inc: { value: 1 } },
    { upsert: true, returnDocument: 'after', setDefaultsOnInsert: true },
  ).lean();
  const next = Number(bumped?.value || 1);
  return `BKG-${String(next).padStart(4, "0")}`;
}

const escapeRegex = (value = "") =>
  String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// GET /api/bookings  -> list bookings with optional status filter
router.get("/", async (req, res, next) => {
  try {
    const {
      status,
      search = "",
      limit = 200,
      skip = 0,
      page = 1,
      sortBy = "createdAt",
      sortDir = "desc",
      noCount = "",
    } = req.query;

    const query = {};
    if (status) {
      // keep exact status (e.g. "Open", "Cancelled", "Converted")
      query.status = status;
    }
    const safeSearch = String(search || "").trim();
    if (safeSearch) {
      const re = new RegExp(escapeRegex(safeSearch), "i");
      query.$or = [
        { bookingId: re },
        { customerName: re },
        { customerPhone: re },
        { vehicleMake: re },
        { vehicleModel: re },
        { showroomName: re },
      ];
    }

    const safeLimit = Math.min(Math.max(Number(limit) || 200, 1), 1000);
    const safePage = Math.max(Number(page) || 1, 1);
    const safeSkip =
      Number.isFinite(Number(skip)) && Number(skip) >= 0
        ? Number(skip)
        : (safePage - 1) * safeLimit;
    const safeSortBy = new Set(["createdAt", "updatedAt", "bookingId"]).has(
      String(sortBy || "").trim(),
    )
      ? String(sortBy).trim()
      : "createdAt";
    const safeSortDir =
      String(sortDir || "").trim().toLowerCase() === "asc" ? 1 : -1;
    const skipCount = new Set(["1", "true", "yes"]).has(
      String(noCount || "").trim().toLowerCase(),
    );

    const dataPromise = Booking.find(query)
      .sort({ [safeSortBy]: safeSortDir, _id: -1 })
      .skip(safeSkip)
      .limit(safeLimit)
      .lean();
    const totalPromise = skipCount
      ? Promise.resolve(null)
      : Booking.countDocuments(query);
    const [bookings, countedTotal] = await Promise.all([dataPromise, totalPromise]);
    const total = skipCount
      ? safeSkip + bookings.length + (bookings.length === safeLimit ? 1 : 0)
      : Number(countedTotal || 0);

    res.json({
      success: true,
      data: bookings,
      total,
      page: Math.floor(safeSkip / safeLimit) + 1,
      limit: safeLimit,
      skip: safeSkip,
      hasMore: skipCount
        ? bookings.length === safeLimit
        : safeSkip + bookings.length < total,
    });
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
