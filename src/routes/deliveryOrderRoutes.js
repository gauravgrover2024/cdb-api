import express from 'express';
import { getDeliveryOrders, getDeliveryOrderByLoanId, saveDeliveryOrder } from '../controllers/deliveryOrderController.js';

const router = express.Router();

router.route('/')
  .get(getDeliveryOrders);

router.route('/:loanId')
  .get(getDeliveryOrderByLoanId)
  .post(saveDeliveryOrder)
  .put(saveDeliveryOrder);

export default router;
