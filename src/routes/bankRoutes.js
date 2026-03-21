import express from 'express';
import { getAllBanks, createBank, resolveBankLookup, updateBank, deleteBank } from '../controllers/loanController.js';

const router = express.Router();

router.get('/lookup', resolveBankLookup);
router.get('/', getAllBanks);
router.post('/', createBank);
router.put('/:id', updateBank);
router.delete('/:id', deleteBank);

export default router;
