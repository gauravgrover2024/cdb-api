// backend/routes/bookings.js
import express from "express";

const router = express.Router();

// TEMP: in-memory store — replace with Mongo later
const bookings = new Map();
let counter = 1;

const nextBookingId = () => `BKG-${String(counter++).padStart(4, "0")}`;

// POST /api/bookings  -> create booking
router.post("/", (req, res) => {
  const bookingId = nextBookingId();
  const now = new Date().toISOString();

  const booking = {
    bookingId,
    status: "Open",
    createdAt: now,
    updatedAt: now,
    ...req.body,
  };

  bookings.set(bookingId, booking);
  res.json(booking);
});

// GET /api/bookings/:id  -> fetch a booking
router.get("/:id", (req, res) => {
  const booking = bookings.get(req.params.id);
  if (!booking) {
    return res.status(404).json({ message: "Booking not found" });
  }
  res.json(booking);
});

// POST /api/bookings/:id/cancel  -> cancel booking
router.post("/:id/cancel", (req, res) => {
  const booking = bookings.get(req.params.id);
  if (!booking) {
    return res.status(404).json({ message: "Booking not found" });
  }

  const { reason, remarks, cancelledAt } = req.body || {};
  const now = new Date().toISOString();

  const updated = {
    ...booking,
    status: "Cancelled",
    cancelReason: reason || booking.cancelReason,
    cancelRemarks: remarks || booking.cancelRemarks,
    cancelledAt: cancelledAt || now,
    updatedAt: now,
  };

  bookings.set(req.params.id, updated);
  res.json(updated);
});

// POST /api/bookings/:id/merge-into-payment  -> stub
router.post("/:id/merge-into-payment", (req, res) => {
  const booking = bookings.get(req.params.id);
  if (!booking) {
    return res.status(404).json({ message: "Booking not found" });
  }

  const now = new Date().toISOString();

  const updated = {
    ...booking,
    mergedIntoPaymentAt: now,
    linkedPaymentLoanId: booking.linkedLoanId || null,
    updatedAt: now,
  };

  bookings.set(req.params.id, updated);
  res.json(updated);
});

// POST /api/bookings/:id/create-loan  -> stub
router.post("/:id/create-loan", (req, res) => {
  const booking = bookings.get(req.params.id);
  if (!booking) {
    return res.status(404).json({ message: "Booking not found" });
  }

  const loanId = `LN-${booking.bookingId}`;
  const now = new Date().toISOString();

  const updated = {
    ...booking,
    linkedLoanId: loanId,
    status: "Converted",
    updatedAt: now,
  };

  bookings.set(req.params.id, updated);

  res.json({ loanId });
});

export default router;
