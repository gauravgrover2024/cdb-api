import express from 'express';
const router = express.Router();
import {
  getChannels,
  getChannelById,
  searchChannels,
  createChannel,
  updateChannel,
  deleteChannel,
  addChannelPayout,
  getChannelStats,
} from '../controllers/channelController.js';

// Search route (must come before /:id)
router.route('/search')
  .get(searchChannels);

router.route('/')
  .get(getChannels)
  .post(createChannel);

router.route('/:id')
  .get(getChannelById)
  .put(updateChannel)
  .delete(deleteChannel);

router.route('/:id/payouts')
  .post(addChannelPayout);

router.route('/:id/stats')
  .get(getChannelStats);

export default router;
