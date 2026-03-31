import express from 'express';
import {
  createDirectDO,
  deleteDeliveryOrder,
  getDeliveryOrders,
  getDeliveryOrderByLoanId,
  saveDeliveryOrder,
} from '../controllers/deliveryOrderController.js';

const router = express.Router();

router.route('/')
  .get(getDeliveryOrders)
  .post(createDirectDO);

router.route('/:loanId')
  .get(getDeliveryOrderByLoanId)
  .post(saveDeliveryOrder)
  .put(saveDeliveryOrder)
  .delete(deleteDeliveryOrder);

export default router;
