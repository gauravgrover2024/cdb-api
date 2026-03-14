import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const required = [
  "R2_ACCOUNT_ID",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
  "R2_BUCKET",
];

const missing = required.filter((key) => !process.env[key]);

const endpoint =
  process.env.R2_ENDPOINT ||
  `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;

const r2Client =
  missing.length === 0
    ? new S3Client({
        region: "auto",
        endpoint,
        credentials: {
          accessKeyId: process.env.R2_ACCESS_KEY_ID,
          secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
        },
      })
    : null;

const encodeKeyForUrl = (key = "") =>
  String(key)
    .split("/")
    .map((part) => encodeURIComponent(part))
    .join("/");

export const getR2ConfigError = () => {
  if (missing.length === 0) return null;
  return `Missing R2 environment variables: ${missing.join(", ")}`;
};

export const buildR2PublicUrl = (key) => {
  const safeKey = encodeKeyForUrl(key);
  const publicBase = process.env.R2_PUBLIC_BASE_URL || "";
  if (publicBase) {
    return `${publicBase.replace(/\/$/, "")}/${safeKey}`;
  }
  return `${endpoint.replace(/\/$/, "")}/${process.env.R2_BUCKET}/${safeKey}`;
};

export const uploadBufferToR2 = async ({ key, body, contentType }) => {
  const configError = getR2ConfigError();
  if (configError) throw new Error(configError);

  await r2Client.send(
    new PutObjectCommand({
      Bucket: process.env.R2_BUCKET,
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
