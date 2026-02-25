import express from "express";
import dotenv from "dotenv";
import helmet from "helmet";
import morgan from "morgan";
import connectDB from "./config/db.js";

import customerRoutes from "./routes/customerRoutes.js";
import loanRoutes from "./routes/loanRoutes.js";
import authRoutes from "./routes/authRoutes.js";
import paymentRoutes from "./routes/paymentRoutes.js";
import deliveryOrderRoutes from "./routes/deliveryOrderRoutes.js";
import uploadRoutes from "./routes/uploadRoutes.js";
import vehicleRoutes from "./routes/vehicleRoutes.js";
import showroomRoutes from "./routes/showroomRoutes.js";
import channelRoutes from "./routes/channelRoutes.js";
import bankRoutes from "./routes/bankRoutes.js";
import quotationsRouter from "./routes/quotations.js";
import featuresRoutes from "./routes/featuresRoutes.js";
import bookingsRouter from "./routes/bookings.js";

dotenv.config();
connectDB();

const app = express();

// Parse JSON
app.use(express.json());

/**
 * ✅ CORS for localhost + Vercel frontend
 */
const allowedOrigins = [
  "http://localhost:3000",
  "http://localhost:5173",
  "https://cdb-frontend-six.vercel.app",
];

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (!origin || allowedOrigins.includes(origin)) {
    res.header("Access-Control-Allow-Origin", origin || "*");
  }

  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }

  next();
});

// Security + logging
app.use(
  helmet({
    crossOriginResourcePolicy: false,
    crossOriginEmbedderPolicy: false,
  }),
);

app.use(morgan("dev"));

// Routes
app.use("/api/banks", bankRoutes);
app.use("/api/customers", customerRoutes);
app.use("/api/loans", loanRoutes);
app.use("/api/auth", authRoutes);
app.use("/api/payments", paymentRoutes);
app.use("/api/do", deliveryOrderRoutes);
app.use("/api/upload", uploadRoutes);
app.use("/api/vehicles", vehicleRoutes);
app.use("/api/showrooms", showroomRoutes);
app.use("/api/channels", channelRoutes);
app.use("/api/quotations", quotationsRouter);
app.use("/api/features", featuresRoutes);
app.use("/api/bookings", bookingsRouter);

// Health check
app.get("/", (req, res) => {
  res.send("API is running...");
});

// Not found
app.use((req, res, next) => {
  const error = new Error(`Not Found - ${req.originalUrl}`);
  res.status(404);
  next(error);
});

// Error handler
app.use((err, req, res, next) => {
  const statusCode = res.statusCode === 200 ? 500 : res.statusCode;

  console.error(`[Error] ${req.method} ${req.url}:`, err.message);

  res.status(statusCode).json({
    success: false,
    message: err.message,
    stack: process.env.NODE_ENV === "production" ? null : err.stack,
  });
});

// ❌ Remove this for Vercel:
// const PORT = process.env.PORT || 5050;
// app.listen(PORT, () => {
//   console.log(`Server running on port ${PORT}`);
// });

// ✅ Export Express app for Vercel serverless
export default app;
