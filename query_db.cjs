const { MongoClient } = require('mongodb');

async function main() {
  const uri = "mongodb+srv://Vercel-Admin-cdb:m7UR55exPG0pkXpe@cdb.h7adfv5.mongodb.net/cdrive?retryWrites=true&w=majority";
  const client = new MongoClient(uri);

  try {
    await client.connect();
    const database = client.db('cdrive');
    const insurances = database.collection('insurancecases'); // Try insurancecases instead of insurances
    const doc = await insurances.findOne({ caseId: 'INS-2026-0047' });
    if (doc) {
      console.log(JSON.stringify(doc, null, 2));
    } else {
      console.log("Not found in insurancecases. Trying insurances...");
      const doc2 = await database.collection('insurances').findOne({ caseId: 'INS-2026-0047' });
      console.log(JSON.stringify(doc2, null, 2));
    }
  } finally {
    await client.close();
  }
}

main().catch(console.dir);
