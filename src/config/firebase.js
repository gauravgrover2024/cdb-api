import admin from 'firebase-admin';
import { readFileSync, existsSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const initializeFirebaseAdmin = () => {
  if (admin.apps.length > 0) {
    return admin.apps[0];
  }

  // Option 1: Use environment variable (JSON string) — preferred for production/Vercel
  if (process.env.FIREBASE_SERVICE_ACCOUNT_KEY) {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_KEY);
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    console.log('✅ Firebase Admin SDK initialized via env variable');
    return admin.apps[0];
  }

  // Option 2: Use serviceAccountKey.json file in project root
  const serviceAccountPath = join(__dirname, '../../serviceAccountKey.json');
  if (existsSync(serviceAccountPath)) {
    const serviceAccount = JSON.parse(readFileSync(serviceAccountPath, 'utf8'));
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    console.log('✅ Firebase Admin SDK initialized via serviceAccountKey.json');
    return admin.apps[0];
  }

  // No credentials found — warn but don't crash the server
  console.warn(
    '⚠️  Firebase Admin SDK not initialized. Set FIREBASE_SERVICE_ACCOUNT_KEY env variable or place serviceAccountKey.json in project root.'
  );
  return null;
};

/**
 * Verify a Firebase ID token sent from the frontend after Firebase sign-in.
 * Initialization is lazy so dotenv has loaded env vars before this runs.
 * @param {string} idToken
 * @returns {Promise<import('firebase-admin').auth.DecodedIdToken>}
 */
export const verifyFirebaseToken = async (idToken) => {
  // Lazy init — runs after dotenv.config() has populated process.env
  if (!admin.apps.length) {
    initializeFirebaseAdmin();
  }
  if (!admin.apps.length) {
    throw new Error('Firebase Admin SDK is not configured. Contact the administrator.');
  }
  return await admin.auth().verifyIdToken(idToken);
};

export default admin;
