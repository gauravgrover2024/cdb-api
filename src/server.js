import express from "express";
import dotenv from "dotenv";
import helmet from "helmet";
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
import insuranceRoutes from "./routes/insuranceRoutes.js";
import quotationsRouter from "./routes/quotations.js";
import featuresRoutes from "./routes/featuresRoutes.js";
import bookingsRouter from "./routes/bookings.js";
import usedCarRoutes from "./routes/usedCarRoutes.js";
import searchRoutes from "./routes/searchRoutes.js";
import aiAgentRoutes from "./routes/aiAgent.routes.js";

dotenv.config();

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
  "https://cdb.acillp.com"
];

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (!origin || allowedOrigins.includes(origin)) {
    res.header("Access-Control-Allow-Origin", origin || "*");
  }

  res.header(
    "Access-Control-Allow-Methods",
    "GET, POST, PUT, PATCH, DELETE, OPTIONS",
  );
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

// Ensure Mongo connection is ready for every serverless request on Vercel.
app.use(async (req, res, next) => {
  try {
    await connectDB();
    next();
  } catch (error) {
    next(error);
  }
});

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
app.use("/api/insurance", insuranceRoutes);
app.use("/api/used-cars", usedCarRoutes);
app.use("/api/search", searchRoutes);
app.use("/api/ai-agent", aiAgentRoutes);

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

// Local runtime: start server only outside Vercel serverless
const PORT = process.env.PORT || 5050;
if (!process.env.VERCEL) {
  startServer();
}

// Export Express app for Vercel serverless
export default app;

async function startServer() {
  try {
    await connectDB();

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start server:", error.message);
    process.exit(1);
  }
}
