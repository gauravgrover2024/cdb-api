import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Payment from "../models/Payment.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import Booking from "../models/Booking.js";

dotenv.config();

const run = async () => {
  try {
    await connectDB();

    const [paymentResult, doResult, bookingResult] = await Promise.all([
      Payment.createIndexes(),
      DeliveryOrder.createIndexes(),
      Booking.createIndexes(),
    ]);

    const [paymentIndexes, doIndexes, bookingIndexes] = await Promise.all([
      Payment.collection.indexes(),
      DeliveryOrder.collection.indexes(),
      Booking.collection.indexes(),
    ]);

    console.log("Operational indexes ensured.");
    console.log("payment createIndexes result:", paymentResult);
    console.log(
      "payment active indexes:",
      paymentIndexes.map((idx) => idx.name),
    );
    console.log("deliveryOrder createIndexes result:", doResult);
    console.log(
      "deliveryOrder active indexes:",
      doIndexes.map((idx) => idx.name),
    );
    console.log("booking createIndexes result:", bookingResult);
    console.log(
      "booking active indexes:",
      bookingIndexes.map((idx) => idx.name),
    );
  } catch (error) {
    console.error("Failed to ensure operational indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
