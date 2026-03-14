import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const REQUIRED_KEYS = [
  "R2_ACCOUNT_ID",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
  "R2_BUCKET",
];

const getEnv = () => {
  const env = {
    accountId: process.env.R2_ACCOUNT_ID,
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    bucket: process.env.R2_BUCKET,
    endpoint: process.env.R2_ENDPOINT,
    publicBaseUrl: process.env.R2_PUBLIC_BASE_URL,
  };

  env.missing = REQUIRED_KEYS.filter((k) => !process.env[k]);
  if (!env.endpoint && env.accountId) {
    env.endpoint = `https://${env.accountId}.r2.cloudflarestorage.com`;
  }

  return env;
};

let clientCache = { signature: "", client: null };

const getR2Client = () => {
  const env = getEnv();
  if (env.missing.length) {
    return { client: null, env };
  }

  const signature = [env.endpoint, env.accessKeyId, env.secretAccessKey].join("|");
  if (clientCache.client && clientCache.signature === signature) {
    return { client: clientCache.client, env };
  }

  const client = new S3Client({
    region: "auto",
    endpoint: env.endpoint,
    credentials: {
      accessKeyId: env.accessKeyId,
      secretAccessKey: env.secretAccessKey,
    },
  });

  clientCache = { signature, client };
  return { client, env };
};

const encodeKeyForUrl = (key = "") =>
  String(key)
    .split("/")
    .map((part) => encodeURIComponent(part))
    .join("/");

export const getR2ConfigError = () => {
  const { env } = getR2Client();
  if (!env.missing.length) return null;
  return `Missing R2 environment variables: ${env.missing.join(", ")}`;
};

export const buildR2PublicUrl = (key) => {
  const { env } = getR2Client();
  const safeKey = encodeKeyForUrl(key);

  if (env.publicBaseUrl) {
    return `${env.publicBaseUrl.replace(/\/$/, "")}/${safeKey}`;
  }

  return `${String(env.endpoint || "").replace(/\/$/, "")}/${env.bucket}/${safeKey}`;
};

export const uploadBufferToR2 = async ({ key, body, contentType }) => {
  const { client, env } = getR2Client();

  if (!client) {
    throw new Error(`Missing R2 environment variables: ${env.missing.join(", ")}`);
  }

  await client.send(
    new PutObjectCommand({
      Bucket: env.bucket,
      Key: key,
      Body: body,
      ContentType: contentType || "application/octet-stream",
    }),
  );

  return {
    key,
    url: buildR2PublicUrl(key),
  };
};
