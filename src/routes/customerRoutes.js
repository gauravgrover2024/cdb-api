import express from 'express';
import {
  getCustomers,
  getCustomerById,
  getCustomerDashboard,
  createCustomer,
  updateCustomer,
  deleteCustomer,
  searchCustomers,
  reassignLoans,
} from '../controllers/customerController.js';
import { protect } from '../middleware/authMiddleware.js';

const router = express.Router();

// Routes
// Note: Frontend currently does not send tokens, so 'protect' is commented out 
// but ready to be enabled.

// IMPORTANT: Search and dashboard routes must come BEFORE :id route
router.get('/search', searchCustomers);
router.get('/:id/dashboard', getCustomerDashboard);
router.post('/:id/reassign-loans', reassignLoans);

router.route('/')
  .get(getCustomers)
  .post(createCustomer);

router.route('/:id')
  .get(getCustomerById)
  .put(updateCustomer)
  .delete(deleteCustomer);

export default router;
