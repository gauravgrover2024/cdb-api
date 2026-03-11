import express from 'express';
import { getAllBanks, createBank, resolveBankLookup } from '../controllers/loanController.js';

const router = express.Router();

// GET /api/banks
router.get('/', getAllBanks);
router.get('/lookup', resolveBankLookup);

// POST /api/banks (Create new bank)
router.post('/', createBank);

export default router;
