import { MongoClient } from "mongodb";

const MONGODB_URI =
  "mongodb+srv://Vercel-Admin-cdb:m7UR55exPG0pkXpe@cdb.h7adfv5.mongodb.net/?retryWrites=true&w=majority";

async function run() {
  try {
    const client = new MongoClient(MONGODB_URI);
    await client.connect();

    const db = client.db("cdrive");
    console.log("Connected to DB:", db.databaseName);

    const result = await db.collection("customers").deleteMany({});
    console.log("Deleted count:", result.deletedCount);

    await client.close();
    process.exit();
  } catch (err) {
    console.error("Error deleting customers:", err);
    process.exit(1);
  }
}

run();
