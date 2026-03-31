import express from 'express';
import { createDirectPayment, getPayments, getPaymentsByLoanId, savePayment } from '../controllers/paymentController.js';

const router = express.Router();

router.route('/')
  .get(getPayments)
  .post(createDirectPayment);

router.route('/:loanId')
  .get(getPaymentsByLoanId)
  .post(savePayment)
  .put(savePayment);

export default router;
