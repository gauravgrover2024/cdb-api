import express from 'express';
const router = express.Router();
import {
  getShowrooms,
  getShowroomById,
  searchShowrooms,
  createShowroom,
  updateShowroom,
  deleteShowroom,
  addShowroomPayment,
  getShowroomStats,
} from '../controllers/showroomController.js';

// Search route (must come before /:id)
router.route('/search')
  .get(searchShowrooms);

router.route('/')
  .get(getShowrooms)
  .post(createShowroom);

router.route('/:id')
  .get(getShowroomById)
  .put(updateShowroom)
  .delete(deleteShowroom);

router.route('/:id/payments')
  .post(addShowroomPayment);

router.route('/:id/stats')
  .get(getShowroomStats);

export default router;
