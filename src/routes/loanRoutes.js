import express from 'express';
import {
  getLoans,
  getLoanDashboardStats,
  getLoanAnalyticsOverview,
  getLoanAnalyticsDrilldown,
  createLoanCustomWidget,
  createLoanCustomReport,
  getLoanById,
  createLoan,
  updateLoan,
  deleteLoan,
  disburseLoan,
  getBanksData,
  saveBanksData,
} from '../controllers/loanController.js';

const router = express.Router();

router.route('/')
  .get(getLoans)
  .post(createLoan);

router.get('/dashboard/stats', getLoanDashboardStats);
router.get('/analytics/overview', getLoanAnalyticsOverview);
router.get('/analytics/drilldown', getLoanAnalyticsDrilldown);
router.post('/analytics/custom-widget', createLoanCustomWidget);
router.post('/analytics/custom-report', createLoanCustomReport);

router.route('/:id')
  .get(getLoanById)
  .put(updateLoan)
  .delete(deleteLoan);

// Disbursement endpoint - separate from regular update
router.post('/:id/disburse', disburseLoan);

// Banks data endpoints
router.get('/:id/banks', getBanksData);
router.put('/:id/banks', saveBanksData);

export default router;
