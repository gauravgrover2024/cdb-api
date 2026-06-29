import * as svc from "../services/homeLoanService.js";

const ok = (res, data, status = 200) => res.status(status).json({ success: true, ...data });
const err = (res, message, status = 400) => res.status(status).json({ success: false, message });

// ─── Application CRUD ─────────────────────────────────────────────────────────

export async function listLoans(req, res) {
  try {
    const result = await svc.listHomeLoans(req.query);
    ok(res, result);
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function getLoan(req, res) {
  try {
    const loan = await svc.getHomeLoanById(req.params.id);
    if (!loan) return err(res, "Home loan not found", 404);
    ok(res, { data: loan });
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function createLoan(req, res) {
  try {
    const userId = req.user?._id;
    const loan = await svc.createHomeLoan(req.body, userId);
    ok(res, { data: loan }, 201);
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function updateLoan(req, res) {
  try {
    const userId = req.user?._id;
    const loan = await svc.updateHomeLoan(req.params.id, req.body, userId);
    if (!loan) return err(res, "Home loan not found", 404);
    ok(res, { data: loan });
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function deleteLoan(req, res) {
  try {
    const deleted = await svc.softDeleteHomeLoan(req.params.id, req.user?._id);
    if (!deleted) return err(res, "Home loan not found", 404);
    ok(res, { message: "Deleted successfully" });
  } catch (e) {
    err(res, e.message, 500);
  }
}

// ─── Disbursement ─────────────────────────────────────────────────────────────

export async function disburseLoan(req, res) {
  try {
    const userId = req.user?._id;
    const patch = {
      ...req.body,
      status: "disbursed",
      "approval.status": "Disbursed",
    };
    const loan = await svc.updateHomeLoan(req.params.id, patch, userId);
    if (!loan) return err(res, "Home loan not found", 404);
    ok(res, { data: loan });
  } catch (e) {
    err(res, e.message, 500);
  }
}

// ─── Dashboard ────────────────────────────────────────────────────────────────

export async function getDashboardStats(req, res) {
  try {
    const stats = await svc.getDashboardStats();
    ok(res, { data: stats });
  } catch (e) {
    err(res, e.message, 500);
  }
}

// ─── Banks ────────────────────────────────────────────────────────────────────

export async function getBanks(req, res) {
  try {
    const banks = await svc.getBanksData(req.params.id);
    ok(res, { banks });
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function saveBanks(req, res) {
  try {
    const banks = await svc.saveBanksData(req.params.id, req.body.banks);
    if (banks === null) return err(res, "Home loan not found", 404);
    ok(res, { banks });
  } catch (e) {
    err(res, e.message, 500);
  }
}

// ─── Collections / Receivables ────────────────────────────────────────────────

export async function getCollectionsReceivables(req, res) {
  try {
    const { HomeLoan } = await import("../models/HomeLoan.js");
    const { page = 1, limit = 20, search, status } = req.query;
    const filter = { deletedAt: null };
    if (status) filter["payout.receivables.status"] = status;
    if (search) {
      filter.$or = [
        { customerName: { $regex: search, $options: "i" } },
        { applicationNumber: { $regex: search, $options: "i" } },
      ];
    }
    const skip = (Number(page) - 1) * Number(limit);
    const [loans, total] = await Promise.all([
      HomeLoan.find(filter, {
        applicationNumber: 1,
        customerName: 1,
        "approval.bankName": 1,
        "approval.loanAmountDisbursed": 1,
        "payout.receivables": 1,
        status: 1,
      })
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(Number(limit)),
      HomeLoan.countDocuments(filter),
    ]);
    ok(res, { data: loans, total, page: Number(page), limit: Number(limit) });
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function upsertCollectionReceivable(req, res) {
  try {
    const { HomeLoan } = await import("../models/HomeLoan.js");
    const { loanId, receivable } = req.body;
    if (!loanId) return err(res, "loanId required");
    const loan = await HomeLoan.findByIdAndUpdate(
      loanId,
      {
        $push: {
          "payout.receivables": {
            ...receivable,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
        },
      },
      { new: true },
    );
    if (!loan) return err(res, "Home loan not found", 404);
    ok(res, { data: loan.payout?.receivables });
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function updateCollectionReceivable(req, res) {
  try {
    const { HomeLoan } = await import("../models/HomeLoan.js");
    const { id } = req.params;
    const { loanId, ...fields } = req.body;
    if (!loanId) return err(res, "loanId required in body");
    const setFields = {};
    for (const [k, v] of Object.entries(fields)) {
      setFields[`payout.receivables.$.${k}`] = v;
    }
    setFields["payout.receivables.$.updatedAt"] = new Date();
    const loan = await HomeLoan.findOneAndUpdate(
      { _id: loanId, "payout.receivables._id": id },
      { $set: setFields },
      { new: true },
    );
    if (!loan) return err(res, "Receivable not found", 404);
    ok(res, { data: loan.payout?.receivables });
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function deleteCollectionReceivable(req, res) {
  try {
    const { HomeLoan } = await import("../models/HomeLoan.js");
    const { id } = req.params;
    const { loanId } = req.query;
    if (!loanId) return err(res, "loanId query param required");
    const loan = await HomeLoan.findByIdAndUpdate(
      loanId,
      { $pull: { "payout.receivables": { _id: id } } },
      { new: true },
    );
    if (!loan) return err(res, "Home loan not found", 404);
    ok(res, { message: "Deleted" });
  } catch (e) {
    err(res, e.message, 500);
  }
}

// ─── Analytics ────────────────────────────────────────────────────────────────

export async function getAnalyticsOverview(req, res) {
  try {
    const { HomeLoan } = await import("../models/HomeLoan.js");
    const { fromDate, toDate } = req.query;
    const match = { deletedAt: null };
    if (fromDate || toDate) {
      match.createdAt = {};
      if (fromDate) match.createdAt.$gte = new Date(fromDate);
      if (toDate) match.createdAt.$lte = new Date(toDate);
    }
    const [byStatus, byLoanType, totalDisbursed] = await Promise.all([
      HomeLoan.aggregate([
        { $match: match },
        { $group: { _id: "$status", count: { $sum: 1 } } },
      ]),
      HomeLoan.aggregate([
        { $match: match },
        { $group: { _id: "$typeOfLoan", count: { $sum: 1 } } },
      ]),
      HomeLoan.aggregate([
        { $match: { ...match, status: "disbursed" } },
        {
          $group: {
            _id: null,
            total: { $sum: "$approval.loanAmountDisbursed" },
            count: { $sum: 1 },
          },
        },
      ]),
    ]);
    ok(res, {
      data: {
        byStatus,
        byLoanType,
        totalDisbursed: totalDisbursed[0] || { total: 0, count: 0 },
      },
    });
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function getAnalyticsDrilldown(req, res) {
  try {
    ok(res, { data: [] });
  } catch (e) {
    err(res, e.message, 500);
  }
}

export async function createCustomWidget(req, res) {
  ok(res, { data: { message: "Widget created" } }, 201);
}

export async function createCustomReport(req, res) {
  ok(res, { data: { message: "Report created" } }, 201);
}

// ─── Counters ─────────────────────────────────────────────────────────────────

export async function getNextRcInvNumber(req, res) {
  try {
    const next = await svc.getNextRcInvStorageNumber();
    ok(res, { next });
  } catch (e) {
    err(res, e.message, 500);
  }
}
