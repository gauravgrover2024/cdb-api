import mongoose from "mongoose";

/**
 * A case ID is handed out the moment a user starts a new insurance case, not
 * when the case first saves. Without this the number was stamped inside
 * createInsuranceCase — which only runs once name + mobile are filled in — so
 * whoever finished typing first won the lower number and the IDs came out in
 * completion order instead of start order.
 *
 * status:
 *   reserved — held by an open form. Reclaimable once `expiresAt` passes, so a
 *              crashed tab or a closed laptop can never burn a number forever.
 *   free     — the form was abandoned with nothing entered; the next new case
 *              takes this number back before the counter is touched again.
 *   consumed — a real InsuranceCase now owns this number. Terminal state.
 */
const insuranceCaseIdReservationSchema = new mongoose.Schema(
  {
    caseId: { type: String, required: true, unique: true, index: true },
    year: { type: Number, required: true, index: true },
    sequence: { type: Number, required: true },
    status: {
      type: String,
      enum: ["reserved", "free", "consumed"],
      default: "reserved",
      index: true,
    },
    reservedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
    reservedAt: { type: Date, default: Date.now },
    expiresAt: { type: Date, index: true },
    consumedAt: { type: Date },
    consumedByCase: { type: mongoose.Schema.Types.ObjectId, ref: "InsuranceCase" },
  },
  { timestamps: true },
);

// Reclaim order: lowest free/stale number in the year gets reused first, so the
// gap left by an abandoned case is filled before the counter advances.
insuranceCaseIdReservationSchema.index({ year: 1, status: 1, sequence: 1 });

const InsuranceCaseIdReservation = mongoose.model(
  "InsuranceCaseIdReservation",
  insuranceCaseIdReservationSchema,
);

export default InsuranceCaseIdReservation;
