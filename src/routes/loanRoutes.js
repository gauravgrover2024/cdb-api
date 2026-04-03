import express from 'express';
import {
  getLoans,
  getCollectionsReceivablesSnapshot,
  upsertCollectionReceivable,
  updateCollectionReceivable,
  deleteCollectionReceivable,
  getLoanDashboardStats,
  getLoanAnalyticsOverview,
  getLoanAnalyticsDrilldown,
  createLoanCustomWidget,
  createLoanCustomReport,
  getLoanById,
  getLoanBreakupFields,
  createLoanBreakupField,
  deleteLoanBreakupField,
  createLoan,
  updateLoan,
  deleteLoan,
  disburseLoan,
  getBanksData,
  saveBanksData,
  getNextRcInvStorageNumber,
} from '../controllers/loanController.js';

const router = express.Router();

router.route('/')
  .get(getLoans)
  .post(createLoan);

router.get('/collections/receivables', getCollectionsReceivablesSnapshot);
router.post('/collections/receivables/upsert', upsertCollectionReceivable);
router.patch('/collections/receivables/:payoutId', updateCollectionReceivable);
router.delete('/collections/receivables/:payoutId', deleteCollectionReceivable);
router.get('/dashboard/stats', getLoanDashboardStats);
router.get('/analytics/overview', getLoanAnalyticsOverview);
router.get('/analytics/drilldown', getLoanAnalyticsDrilldown);
router.post('/analytics/custom-widget', createLoanCustomWidget);
router.post('/analytics/custom-report', createLoanCustomReport);
router.get('/counters/rc-inv/next', getNextRcInvStorageNumber);
router.get('/breakup-fields', getLoanBreakupFields);
router.post('/breakup-fields', createLoanBreakupField);
router.delete('/breakup-fields/:key', deleteLoanBreakupField);

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
