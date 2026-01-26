import { MongoClient } from "mongodb";

let cachedClient = null;

export async function getDb() {
  const uri = process.env.MONGODB_URI;

  if (!uri) {
    throw new Error("MONGODB_URI missing in environment variables");
  }

  if (!cachedClient) {
    cachedClient = new MongoClient(uri);
    await cachedClient.connect();
  }

  return cachedClient.db("cdrive"); // ✅ Your DB name from screenshot
}
