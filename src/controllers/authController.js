import asyncHandler from 'express-async-handler';
import User from '../models/User.js';
import generateToken from '../utils/generateToken.js';
import { verifyFirebaseToken } from '../config/firebase.js';

const getAvatarUrlFromDecoded = (decoded = {}) =>
  String(decoded?.picture || decoded?.photoURL || '').trim();

// @desc    Get all users (superadmin only)
// @route   GET /api/auth/users
// @access  Private/Superadmin
const getAllUsers = asyncHandler(async (req, res) => {
  const users = await User.find({}, '-password -firebaseUid');
  res.json({ success: true, data: users });
});

// @desc    Get assignable users for ops workflows
// @route   GET /api/auth/assignable-users
// @access  Private/Staff+
const getAssignableUsers = asyncHandler(async (req, res) => {
  const users = await User.find(
    { status: "active", role: { $in: ["staff", "admin", "superadmin", "team_lead", "insurance_team_lead"] } },
    "_id name email role status",
  )
    .sort({ name: 1 })
    .lean();
  res.json({ success: true, data: users });
});

// @desc    Get single user by ID (superadmin only)
// @route   GET /api/auth/user/:id
// @access  Private/Superadmin
const getUserById = asyncHandler(async (req, res) => {
  const user = await User.findById(req.params.id).select('-password');
  if (!user) {
    res.status(404);
    throw new Error('User not found');
  }
  res.json({ success: true, data: user });
});

// @desc    Login with email/password OR Firebase token
// @route   POST /api/auth/login
// @access  Public
const authUser = asyncHandler(async (req, res) => {
  const { email, password, firebaseIdToken } = req.body;

  let user;

  if (firebaseIdToken) {
    // Firebase-based login: verify the Firebase token
    const decoded = await verifyFirebaseToken(firebaseIdToken);

    user = await User.findOne({ email: decoded.email });

    if (!user) {
      res.status(401);
      throw new Error('No account found. Please register first.');
    }

    // Sync firebaseUid if not set
    if (!user.firebaseUid) {
      user.firebaseUid = decoded.uid;
      await user.save();
    }
  } else {
    // Classic email/password login
    if (!email || !password) {
      res.status(400);
      throw new Error('Email and password are required');
    }

    user = await User.findOne({ email });

    if (!user || !(await user.matchPassword(password))) {
      res.status(401);
      throw new Error('Invalid email or password');
    }
  }

  // Block deactivated/rejected/pending users
  if (user.status === 'deactivated') {
    res.status(403);
    throw new Error('Your account has been deactivated. Contact your administrator.');
  }
  if (user.status === 'rejected') {
    res.status(403);
    throw new Error('Your account has been rejected. Contact your administrator.');
  }
  if (user.status === 'pending') {
    res.status(403);
    throw new Error('Your account is pending approval. The administrator will review your account soon. Please check back later.');
  }

  res.json({
    success: true,
    data: {
      _id: user._id,
      name: user.name,
      email: user.email,
      role: user.role,
      status: user.status,
      avatarUrl: user.avatarUrl || "",
      token: generateToken(user._id),
    },
  });
});

// @desc    Register a new user (with optional Firebase token)
// @route   POST /api/auth/register
// @access  Public
const registerUser = asyncHandler(async (req, res) => {
  const { name, email, password, role, firebaseIdToken } = req.body;

  const userExists = await User.findOne({ email });
  if (userExists) {
    res.status(400);
    throw new Error('User already exists');
  }

  let firebaseUid;
  let firebaseDecoded = null;

  if (firebaseIdToken) {
    // Verify Firebase token and extract UID
    const decoded = await verifyFirebaseToken(firebaseIdToken);
    firebaseDecoded = decoded;
    firebaseUid = decoded.uid;

    if (decoded.email !== email) {
      res.status(400);
      throw new Error('Email does not match Firebase token');
    }
  } else if (!password) {
    res.status(400);
    throw new Error('Password is required for non-Firebase registration');
  }

  const user = await User.create({
    name,
    email,
    password: password || undefined,
    firebaseUid,
    avatarUrl: getAvatarUrlFromDecoded(firebaseDecoded),
    role: role || 'staff',
  });

  if (user) {
    // If pending, do not log them in
    if (user.status === 'pending') {
      res.status(403);
      throw new Error('Your account is pending approval. The administrator will review your account soon. Please check back later.');
    }

    res.status(201).json({
      success: true,
      data: {
        _id: user._id,
        name: user.name,
        email: user.email,
        role: user.role,
        avatarUrl: user.avatarUrl || "",
        token: generateToken(user._id),
      },
    });
  } else {
    res.status(400);
    throw new Error('Invalid user data');
  }
});

// @desc    Google login — create user if not exists, or sign in
// @route   POST /api/auth/google-login
// @access  Public
const googleLogin = asyncHandler(async (req, res) => {
  const { firebaseIdToken, email, name } = req.body;

  if (!firebaseIdToken) {
    res.status(400);
    throw new Error('Firebase ID token is required');
  }

  // Verify Firebase token
  const decoded = await verifyFirebaseToken(firebaseIdToken);

  const resolvedEmail = decoded.email || email;
  const resolvedName = decoded.name || name || resolvedEmail;
  const resolvedAvatarUrl = getAvatarUrlFromDecoded(decoded);

  if (!resolvedEmail) {
    res.status(400);
    throw new Error('Could not retrieve email from Google account');
  }

  // Find existing user or create new one
  let user = await User.findOne({ email: resolvedEmail });

  if (!user) {
    // Auto-create new user with default staff role
    user = await User.create({
      name: resolvedName,
      email: resolvedEmail,
      firebaseUid: decoded.uid,
      avatarUrl: resolvedAvatarUrl,
      role: 'staff',
    });
  } else {
    let shouldSave = false;
    if (!user.firebaseUid) {
      user.firebaseUid = decoded.uid;
      shouldSave = true;
    }
    if (resolvedAvatarUrl && user.avatarUrl !== resolvedAvatarUrl) {
      user.avatarUrl = resolvedAvatarUrl;
      shouldSave = true;
    }
    if (shouldSave) await user.save();
  }

  // Block deactivated/rejected/pending users
  if (user.status === 'deactivated') {
    res.status(403);
    throw new Error('Your account has been deactivated. Contact your administrator.');
  }
  if (user.status === 'rejected') {
    res.status(403);
    throw new Error('Your account has been rejected. Contact your administrator.');
  }
  if (user.status === 'pending') {
    res.status(403);
    throw new Error('Your account is pending approval. The administrator will review your account soon. Please check back later.');
  }

  res.json({
    success: true,
    data: {
      _id: user._id,
      name: user.name,
      email: user.email,
      role: user.role,
      avatarUrl: user.avatarUrl || "",
      token: generateToken(user._id),
    },
  });
});

// @desc    Update user role (superadmin only)
// @route   PUT /api/auth/user/:id/role
// @access  Private/Superadmin
const updateUserRole = asyncHandler(async (req, res) => {
  const { role } = req.body;
  const { id } = req.params;

  // Prevent superadmin from demoting themselves
  if (req.user._id.toString() === id) {
    res.status(400);
    throw new Error('You cannot change your own role');
  }

  const user = await User.findById(id);
  if (!user) {
    res.status(404);
    throw new Error('User not found');
  }

  user.role = role;
  await user.save();

  res.json({
    success: true,
    data: {
      _id: user._id,
      name: user.name,
      email: user.email,
      role: user.role,
    },
    message: 'User role updated successfully',
  });
});

// @desc    Approve or reject a user (superadmin only)
// @route   PUT /api/auth/user/:id/approve
// @access  Private/Superadmin
const approveUser = asyncHandler(async (req, res) => {
  const { status } = req.body; // 'active' or 'rejected'
  const { id } = req.params;

  const allowed = ['active', 'rejected', 'pending'];
  if (!allowed.includes(status)) {
    res.status(400);
    throw new Error(`Invalid status. Must be one of: ${allowed.join(', ')}`);
  }

  const user = await User.findById(id);
  if (!user) {
    res.status(404);
    throw new Error('User not found');
  }

  user.status = status;
  await user.save();

  res.json({
    success: true,
    data: { _id: user._id, name: user.name, email: user.email, role: user.role, status: user.status },
    message: `User ${status === 'active' ? 'approved' : 'rejected'} successfully`,
  });
});

// @desc    Deactivate a user account (superadmin only)
// @route   PUT /api/auth/user/:id/deactivate
// @access  Private/Superadmin
const deactivateUser = asyncHandler(async (req, res) => {
  const { id } = req.params;

  if (req.user._id.toString() === id) {
    res.status(400);
    throw new Error('You cannot deactivate your own account');
  }

  const user = await User.findById(id);
  if (!user) {
    res.status(404);
    throw new Error('User not found');
  }

  user.status = 'deactivated';
  await user.save();

  res.json({
    success: true,
    data: { _id: user._id, name: user.name, email: user.email, role: user.role, status: user.status },
    message: 'User deactivated successfully',
  });
});

// @desc    Delete a user (superadmin only)
// @route   DELETE /api/auth/user/:id
// @access  Private/Superadmin
const deleteUser = asyncHandler(async (req, res) => {
  const { id } = req.params;

  if (req.user._id.toString() === id) {
    res.status(400);
    throw new Error('You cannot delete your own account');
  }

  const user = await User.findByIdAndDelete(id);
  if (!user) {
    res.status(404);
    throw new Error('User not found');
  }

  res.json({ success: true, message: 'User deleted successfully' });
});

// @desc    Get currently logged-in user's fresh data from DB
// @route   GET /api/auth/me
// @access  Private
const getMe = asyncHandler(async (req, res) => {
  const user = await User.findById(req.user._id).select('-password -firebaseUid');
  if (!user) {
    res.status(404);
    throw new Error('User not found');
  }
  res.json({
    success: true,
    data: {
      _id: user._id,
      name: user.name,
      email: user.email,
      role: user.role,
      status: user.status,
      avatarUrl: user.avatarUrl || "",
      createdAt: user.createdAt,
      updatedAt: user.updatedAt,
    },
  });
});

// @desc    Change password for logged-in user
// @route   PUT /api/auth/change-password
// @access  Private
const changePassword = asyncHandler(async (req, res) => {
  const { currentPassword, newPassword } = req.body;

  if (!currentPassword || !newPassword) {
    res.status(400);
    throw new Error('Both currentPassword and newPassword are required');
  }
  if (newPassword.length < 6) {
    res.status(400);
    throw new Error('New password must be at least 6 characters');
  }

  const user = await User.findById(req.user._id);
  if (!user || !user.password) {
    res.status(400);
    throw new Error('Password change is not available for this account type');
  }

  const bcrypt = await import('bcryptjs');
  const isMatch = await bcrypt.default.compare(currentPassword, user.password);
  if (!isMatch) {
    res.status(401);
    throw new Error('Current password is incorrect');
  }

  const salt = await bcrypt.default.genSalt(10);
  user.password = await bcrypt.default.hash(newPassword, salt);
  await user.save();

  res.json({ success: true, message: 'Password changed successfully' });
});

export {
  authUser,
  registerUser,
  googleLogin,
  updateUserRole,
  getAllUsers,
  getAssignableUsers,
  getUserById,
  approveUser,
  deactivateUser,
  deleteUser,
  getMe,
  changePassword,
};
