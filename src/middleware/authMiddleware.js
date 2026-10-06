import jwt from "jsonwebtoken";
import asyncHandler from "express-async-handler";
import User from "../models/User.js";

const protect = asyncHandler(async (req, res, next) => {
  let token;

  const authHeader = req.headers.authorization || req.headers.Authorization;

  if (authHeader && authHeader.toLowerCase().startsWith("bearer")) {
    token = authHeader.split(" ")[1];

    // Only a bad/expired token is a 401. Anything after this point (e.g. the
    // DB being slow on a serverless cold start) must not look like an auth
    // failure, otherwise the frontend clears the token and logs the user out.
    let decoded;
    try {
      decoded = jwt.verify(token, process.env.JWT_SECRET);
    } catch (error) {
      console.error("JWT Verification Error:", error.message);
      res.status(401);
      throw new Error("Not authorized, token failed");
    }

    try {
      req.user = await User.findById(decoded.id).select("-password");
    } catch (error) {
      console.error("Auth user lookup failed:", error.message);
      res.status(503);
      throw new Error("Service temporarily unavailable, please retry");
    }

    if (!req.user) {
      res.status(401);
      throw new Error("Not authorized, user not found");
    }

    // Block deactivated, rejected, or pending accounts BEFORE calling next()
    if (req.user.status === "deactivated") {
      res.status(403);
      throw new Error(
        "Your account has been deactivated. Contact your administrator.",
      );
    }
    if (req.user.status === "rejected") {
      res.status(403);
      throw new Error(
        "Your account has been rejected. Contact your administrator.",
      );
    }
    if (req.user.status === "pending") {
      res.status(403);
      throw new Error(
        "Your account is pending approval. The administrator will review your account soon. Please check back later.",
      );
    }

    return next();
  }

  if (!token) {
    res.status(401);
    throw new Error("Not authorized, no token provided in headers");
  }
});

const admin = (req, res, next) => {
  if (
    req.user &&
    (req.user.role === "admin" || req.user.role === "superadmin")
  ) {
    next();
  } else {
    res.status(403);
    throw new Error("Not authorized as an admin/superadmin");
  }
};

const staff = (req, res, next) => {
  if (
    req.user &&
    [
      "staff",
      "admin",
      "superadmin",
      "team_lead",
      "insurance_team_lead",
    ].includes(req.user.role)
  ) {
    next();
  } else {
    res.status(403);
    throw new Error("Not authorized");
  }
};

const superadmin = (req, res, next) => {
  if (req.user && req.user.role === "superadmin") {
    next();
  } else {
    res.status(401);
    throw new Error("Not authorized! Highly restricted to Superadmins only.");
  }
};

export { protect, admin, superadmin, staff };
