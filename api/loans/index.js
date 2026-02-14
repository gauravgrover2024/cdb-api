import { getDb } from "../_db.js";
import withCors from "../_cors.js";

// Map Mongo loan doc -> dashboard shape
function mapLoanForList(loan) {
  if (!loan) return loan;

  const applicant = loan.applicant || {};
  const vehicle = loan.vehicle || {};
  const finance = loan.finance || {};
  const lead = loan.lead || {};
  const delivery = loan.delivery || {};

  return {
    ...loan,

    // Customer profile (what LoansDataGrid reads)
    customerName: loan.customerName || applicant.name || "",
    primaryMobile: loan.primaryMobile || applicant.primaryMobile || "",
    email: loan.email || applicant.email || "",
    residenceAddress: loan.residenceAddress || applicant.residenceAddress || "",
    permanentAddress: loan.permanentAddress || applicant.permanentAddress || "",
    city: loan.city || applicant.city || applicant.permanentCity || "",
    pincode:
      loan.pincode || applicant.pincode || applicant.permanentPincode || "",

    // Vehicle
    vehicleMake: loan.vehicleMake || vehicle.make || "",
    vehicleModel: loan.vehicleModel || vehicle.model || "",
    vehicleVariant: loan.vehicleVariant || vehicle.variant || "",

    // Finance
    typeOfLoan: loan.typeOfLoan || finance.typeOfLoan || "",

    // Source / dealer
    source: loan.source || lead.source || lead.leadSource || "",
    dealerName: loan.dealerName || lead.dealerName || delivery.dealerName || "",
    dealerContactPerson:
      loan.dealerContactPerson || delivery.dealerContactPerson || "",
  };
}

/**
 * /api/loans
 * GET  -> list loans (dashboard)
 * POST -> create loan
 */
async function handler(req, res) {
  try {
    const db = await getDb();
    const loansCol = db.collection("loans");

    // ------------------------
    // GET /api/loans
    // ------------------------
    if (req.method === "GET") {
      const loans = await loansCol.find({}).sort({ updatedAt: -1 }).toArray();

      const mapped = loans.map(mapLoanForList);

      return res.status(200).json({
        success: true,
        data: mapped,
      });
    }

    // ------------------------
    // POST /api/loans
    // ------------------------
    if (req.method === "POST") {
      const payload = req.body || {};
      const now = new Date().toISOString();

      // 🔒 never trust frontend loanId
      delete payload.loanId;

      const doc = {
        ...payload,
        createdAt: now,
        updatedAt: now,
      };

      // 1️⃣ Insert
      const result = await loansCol.insertOne(doc);

      // 2️⃣ Use Mongo _id as loanId
      const loanId = result.insertedId.toString();

      // 3️⃣ Write loanId back into document
      await loansCol.updateOne(
        { _id: result.insertedId },
        { $set: { loanId } },
      );

      // 4️⃣ IMPORTANT: return loanId at TOP LEVEL
      return res.status(201).json({
        success: true,
        loanId,
        createdAt: now,
      });
    }

    return res
      .status(405)
      .json({ success: false, error: "Method not allowed" });
  } catch (err) {
    console.error("Loans API error:", err);
    return res.status(500).json({
      success: false,
      error: err.message,
    });
  }
}

export default withCors(handler);
