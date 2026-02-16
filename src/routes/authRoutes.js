import express from 'express';
import { authUser, registerUser, updateUserRole, getAllUsers } from '../controllers/authController.js';
import { protect, superadmin } from '../middleware/authMiddleware.js';

const router = express.Router();
// Superadmin: get all users
router.get('/users', protect, superadmin, getAllUsers);


router.post('/login', authUser);
router.post('/register', registerUser);

// Superadmin: update user role
router.put('/user/:id/role', protect, superadmin, updateUserRole);

export default router;
