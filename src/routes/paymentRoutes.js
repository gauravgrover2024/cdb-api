import express from 'express';
import {
  createDirectPayment,
  getPayments,
  getPaymentsDashboardSnapshot,
  getPaymentsByLoanId,
  savePayment,
} from '../controllers/paymentController.js';
import { protect } from '../middleware/authMiddleware.js';
import { requirePermission } from '../middleware/permissionMiddleware.js';

const router = express.Router();

router.get('/dashboard/snapshot', getPaymentsDashboardSnapshot);

router.route('/')
  .get(getPayments)
  .post(protect, requirePermission('payments', 'payment', 'add'), createDirectPayment);

router.route('/:loanId')
  .get(getPaymentsByLoanId)
  .post(protect, requirePermission('payments', 'payment', 'add'), savePayment)
  .put(protect, requirePermission('payments', 'payment', 'edit'), savePayment);

export default router;
