// routes/bookings.js
import express from "express";

const router = express.Router();

// In-memory for now; replace with DB
const bookings = new Map();
let counter = 1;

// POST /api/bookings
router.post("/", (req, res) => {
  const bookingId = `BKG-${String(counter).padStart(4, "0")}`;
  counter += 1;

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

// GET /api/bookings/:id
router.get("/:id", (req, res) => {
  const booking = bookings.get(req.params.id);
  if (!booking) {
    return res.status(404).json({ message: "Booking not found" });
  }
  res.json(booking);
});

// POST /api/bookings/:id/cancel
router.post("/:id/cancel", (req, res) => {
  const booking = bookings.get(req.params.id);
  if (!booking) {
    return res.status(404).json({ message: "Booking not found" });
  }

  const { reason, remarks, cancelledAt } = req.body || {};
  const updated = {
    ...booking,
    status: "Cancelled",
    cancelReason: reason || booking.cancelReason,
    cancelRemarks: remarks || booking.cancelRemarks,
    cancelledAt: cancelledAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  bookings.set(req.params.id, updated);
  res.json(updated);
});

// POST /api/bookings/:id/merge-into-payment
router.post("/:id/merge-into-payment", (req, res) => {
  const booking = bookings.get(req.params.id);
  if (!booking) {
    return res.status(404).json({ message: "Booking not found" });
  }

  // TODO: actually create a showroom payment row in your payments collection
  const updated = {
    ...booking,
    linkedPaymentLoanId: booking.linkedLoanId || null,
    mergedIntoPaymentAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  bookings.set(req.params.id, updated);
  res.json(updated);
});

// POST /api/bookings/:id/create-loan
router.post("/:id/create-loan", (req, res) => {
  const booking = bookings.get(req.params.id);
  if (!booking) {
    return res.status(404).json({ message: "Booking not found" });
  }

  // TODO: actually create loan; for now fake one
  const loanId = `LN-${booking.bookingId}`;

  const updated = {
    ...booking,
    linkedLoanId: loanId,
    status: "Converted",
    updatedAt: new Date().toISOString(),
  };
  bookings.set(req.params.id, updated);

  res.json({ loanId });
});

export default router;
