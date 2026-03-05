import express from 'express';
import {
  getLoans,
  getLoanDashboardStats,
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
