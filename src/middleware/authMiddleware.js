import jwt from 'jsonwebtoken';
import asyncHandler from 'express-async-handler';
import User from '../models/User.js';

const protect = asyncHandler(async (req, res, next) => {
  let token;

  const authHeader = req.headers.authorization || req.headers.Authorization;

  if (authHeader && authHeader.toLowerCase().startsWith('bearer')) {
    try {
      token = authHeader.split(' ')[1];

      const decoded = jwt.verify(token, process.env.JWT_SECRET || 'secret123');

      req.user = await User.findById(decoded.id).select('-password');

      if (!req.user) {
        res.status(401);
        throw new Error('Not authorized, user not found');
      }

      next();
    } catch (error) {
      console.error('JWT Verification Error:', error.message);
      res.status(401);
      throw new Error('Not authorized, token failed');
    }

    // Block deactivated or rejected accounts — checked AFTER try/catch so
    // the status code is not overwritten by the catch block
    if (req.user) {
      if (req.user.status === 'deactivated') {
        res.status(403);
        throw new Error('Your account has been deactivated. Contact your administrator.');
      }
      if (req.user.status === 'rejected') {
        res.status(403);
        throw new Error('Your account has been rejected. Contact your administrator.');
      }
    }
  }

  if (!token) {
    res.status(401);
    throw new Error('Not authorized, no token provided in headers');
  }
});

const admin = (req, res, next) => {
  if (req.user && (req.user.role === 'admin' || req.user.role === 'superadmin')) {
    next();
  } else {
    res.status(403);
    throw new Error('Not authorized as an admin/superadmin');
  }
};

const staff = (req, res, next) => {
  if (req.user && ['staff', 'admin', 'superadmin'].includes(req.user.role)) {
    next();
  } else {
    res.status(403);
    throw new Error('Not authorized');
  }
};

const superadmin = (req, res, next) => {
  if (req.user && req.user.role === 'superadmin') {
    next();
  } else {
    res.status(401);
    throw new Error('Not authorized! Highly restricted to Superadmins only.');
  }
};

export { protect, admin, superadmin, staff };
