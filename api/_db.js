import { MongoClient } from "mongodb";

let cachedClient = null;

export async function getDb() {
  const uri = process.env.MONGODB_URI;

  if (!uri) {
    throw new Error("MONGODB_URI missing in environment variables");
  }

  if (!cachedClient) {
    cachedClient = new MongoClient(uri, {
      dbName: "cdrive", // FORCE database explicitly
    });
    await cachedClient.connect();
  }

  const db = cachedClient.db("cdrive");

  console.log("Connected to DB:", db.databaseName);

  return db;
}
