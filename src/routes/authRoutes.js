import express from 'express';
import {
  authUser,
  registerUser,
  googleLogin,
  updateUserRole,
  getAllUsers,
  getUserById,
  approveUser,
  deactivateUser,
  deleteUser,
  getMe,
  changePassword,
} from '../controllers/authController.js';
import { protect, superadmin } from '../middleware/authMiddleware.js';

const router = express.Router();

// Public routes
router.post('/login', authUser);
router.post('/register', registerUser);
router.post('/google-login', googleLogin);

// Current user (any authenticated user)
router.get('/me', protect, getMe);
router.put('/change-password', protect, changePassword);

// Superadmin: user management
router.get('/users', protect, superadmin, getAllUsers);
router.get('/user/:id', protect, superadmin, getUserById);
router.put('/user/:id/role', protect, superadmin, updateUserRole);
router.put('/user/:id/approve', protect, superadmin, approveUser);
router.put('/user/:id/deactivate', protect, superadmin, deactivateUser);
router.delete('/user/:id', protect, superadmin, deleteUser);

export default router;
