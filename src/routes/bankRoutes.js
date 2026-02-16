import express from 'express';
import { getAllBanks, createBank } from '../controllers/loanController.js';

const router = express.Router();

// GET /api/banks
router.get('/', getAllBanks);

// POST /api/banks (Create new bank)
router.post('/', createBank);

export default router;
