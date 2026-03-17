import dns from "node:dns";
import mongoose from "mongoose";

let cached = global.mongoose;

const DEFAULT_DNS_FALLBACK_SERVERS = ["8.8.8.8", "1.1.1.1"];

const getDnsFallbackServers = () => {
  const configuredServers = process.env.MONGO_DNS_SERVERS
    ?.split(",")
    .map((server) => server.trim())
    .filter(Boolean);

  return configuredServers?.length
    ? configuredServers
    : DEFAULT_DNS_FALLBACK_SERVERS;
};

const isSrvDnsResolutionError = (error, mongoUri) =>
  typeof mongoUri === "string" &&
  mongoUri.startsWith("mongodb+srv://") &&
  error?.code === "ECONNREFUSED" &&
  error?.syscall === "querySrv";

const connectWithDnsFallback = async (mongoUri) => {
  try {
    return await mongoose.connect(mongoUri, {
      bufferCommands: true,
    });
  } catch (error) {
    if (!isSrvDnsResolutionError(error, mongoUri)) {
      throw error;
    }

    const fallbackServers = getDnsFallbackServers();
    dns.setServers(fallbackServers);

    console.warn(
      `MongoDB SRV lookup failed with the system DNS resolver. Retrying with fallback DNS servers: ${fallbackServers.join(", ")}`,
    );

    return mongoose.connect(mongoUri, {
      bufferCommands: true,
    });
  }
};

if (!cached) {
  cached = global.mongoose = {
    conn: null,
    promise: null,
  };
}

const connectDB = async () => {
  const mongoUri = process.env.MONGO_URI;

  if (!mongoUri) {
    throw new Error("MONGO_URI is not configured");
  }

  if (cached.conn) {
    return cached.conn;
  }

  if (!cached.promise) {
    cached.promise = connectWithDnsFallback(mongoUri);
  }

  try {
    cached.conn = await cached.promise;
  } catch (error) {
    cached.promise = null;
    throw error;
  }

  console.log("MongoDB connected");

  return cached.conn;
};

export default connectDB;
