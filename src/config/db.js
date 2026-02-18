import mongoose from "mongoose";
import dotenv from "dotenv";

dotenv.config();

const connectDB = async () => {
  try {
    const conn = await mongoose.connect(process.env.MONGODB_URI, {
      // These options are no longer necessary in Mongoose 6+, but keeping for safety if older version
      // useNewUrlParser: true,
      // useUnifiedTopology: true,
    });

    console.log(
      `MongoDB Connected: ${conn.connection.host}, db: ${conn.connection.name}`,
    );
  } catch (error) {
    console.error(`Error: ${error.message}`);
    process.exit(1);
  }
};

export default connectDB;
