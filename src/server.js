import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import helmet from "helmet";
import morgan from "morgan";
import connectDB from "./config/db.js";

dotenv.config();
connectDB();

dotenv.config();

connectDB();

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

const app = express();

app.use(express.json());

app.use(
  cors({
    origin: [
      "http://localhost:3000",
      "http://127.0.0.1:3000",
      "https://your-frontend-domain.vercel.app",
    ],
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
    credentials: true,
  }),
);

// Handle preflight requests explicitly
app.options("*", cors());

app.use(helmet());
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

// Health Check
app.get("/", (req, res) => {
  res.send("API is running...");
});

// Not Found Middleware
app.use((req, res, next) => {
  const error = new Error(`Not Found - ${req.originalUrl}`);
  res.status(404);
  next(error);
});

// Error Handling Middleware
app.use((err, req, res, next) => {
  const statusCode = res.statusCode === 200 ? 500 : res.statusCode;

  if (process.env.NODE_ENV !== "production") {
    console.error(`[Error] ${req.method} ${req.url}:`, err.message);
  }

  res.status(statusCode).json({
    success: false,
    message: err.message,
    stack: process.env.NODE_ENV === "production" ? null : err.stack,
  });
});

const PORT = process.env.PORT || 5000;

app.listen(PORT, () => {
  console.log(`Server running in ${process.env.NODE_ENV} mode on port ${PORT}`);
});
