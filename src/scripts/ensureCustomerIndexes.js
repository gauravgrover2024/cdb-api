import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Customer from "../models/Customer.js";

dotenv.config();

const run = async () => {
  try {
    await connectDB();
    const created = [];
    created.push(
      await Customer.collection.createIndex({ "bankDetails.ifscCode": 1 }),
    );
    created.push(
      await Customer.collection.createIndex({ "bankDetails.accountNumber": 1 }),
    );
    const indexes = await Customer.collection.indexes();

    console.log("Customer indexes ensured.");
    console.log("created/verified indexes:", created);
    console.log(
      "active indexes:",
      indexes.map((idx) => idx.name),
    );
  } catch (error) {
    console.error("Failed to ensure Customer indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
