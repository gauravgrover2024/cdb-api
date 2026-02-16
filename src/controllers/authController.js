import asyncHandler from 'express-async-handler';
import User from '../models/User.js';
import generateToken from '../utils/generateToken.js';

// @desc    Get all users (superadmin only)
// @route   GET /api/users
// @access  Private/Superadmin
const getAllUsers = asyncHandler(async (req, res) => {
  if (!req.user || req.user.role !== 'superadmin') {
    res.status(403);
    throw new Error('Not authorized as superadmin');
  }
  const users = await User.find({}, '-password');
  res.json({ success: true, data: users });
});

// @desc    Auth user & get token
// @route   POST /api/auth/login
// @access  Public
const authUser = asyncHandler(async (req, res) => {
  const { email, password } = req.body;

  const user = await User.findOne({ email });

  if (user && (await user.matchPassword(password))) {
    res.json({
      success: true,
      data: {
        _id: user._id,
        name: user.name,
        email: user.email,
        role: user.role,
        token: generateToken(user._id),
      },
    });
  } else {
    res.status(401);
    throw new Error('Invalid email or password');
  }
});

// @desc    Register a new user
// @route   POST /api/auth/register
// @access  Public
const registerUser = asyncHandler(async (req, res) => {
  const { name, email, password, role } = req.body;

  const userExists = await User.findOne({ email });

  if (userExists) {
    res.status(400);
    throw new Error('User already exists');
  }

  const user = await User.create({
    name,
    email,
    password,
    role,
  });

  if (user) {
    res.status(201).json({
      success: true,
      data: {
        _id: user._id,
        name: user.name,
        email: user.email,
        role: user.role,
        token: generateToken(user._id),
      },
    });
  } else {
    res.status(400);
    throw new Error('Invalid user data');
  }
});

// @desc    Update user role (superadmin only)
// @route   PUT /api/auth/user/:id/role
// @access  Private/Superadmin
const updateUserRole = asyncHandler(async (req, res) => {
  const { role } = req.body;
  const { id } = req.params;

  // Only allow superadmin to update roles
  if (!req.user || req.user.role !== 'superadmin') {
    res.status(403);
    throw new Error('Not authorized as superadmin');
  }

  // Prevent superadmin from demoting themselves
  if (req.user._id.toString() === id) {
    res.status(400);
    throw new Error('Superadmin cannot change their own role');
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

export { authUser, registerUser, updateUserRole, getAllUsers };
