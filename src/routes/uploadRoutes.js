import express from "express";
import multer from "multer";
import path from "path";
import { getObjectFromR2, uploadBufferToR2 } from "../config/r2.js";

const router = express.Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    files: 10,
    fileSize: 20 * 1024 * 1024,
  },
});

const sanitizeBaseName = (name = "") =>
  String(name)
    .replace(/\.[^.]+$/, "")
    .replace(/[^a-zA-Z0-9-_]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .toLowerCase() || "file";

const extFromNameOrMime = (name = "", mime = "") => {
  const fromName = path.extname(name || "").toLowerCase();
  if (fromName) return fromName;

  const byMime = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
  };

  return byMime[mime] || "";
};

const safeDecode = (value = "") => {
  try {
    return decodeURIComponent(String(value || ""));
  } catch {
    return String(value || "");
  }
};

const inferR2KeyFromInputs = ({ key, url, bucket }) => {
  const rawKey = safeDecode(String(key || "")).trim();
  if (rawKey) return rawKey.replace(/^\/+/, "");

  const rawUrl = safeDecode(String(url || "")).trim();
  if (!rawUrl) return "";

  try {
    const parsed = new URL(rawUrl);
    let pathname = safeDecode(parsed.pathname || "").replace(/^\/+/, "");
    if (!pathname) return "";

    if (bucket && pathname.startsWith(`${bucket}/`)) {
      pathname = pathname.slice(bucket.length + 1);
    }

    const uploadsMarker = pathname.indexOf("uploads/");
    if (uploadsMarker >= 0) {
      return pathname.slice(uploadsMarker);
    }

    return pathname;
  } catch {
    const fallback = rawUrl.replace(/^\/+/, "");
    const uploadsMarker = fallback.indexOf("uploads/");
    if (uploadsMarker >= 0) return fallback.slice(uploadsMarker);
    return fallback;
  }
};

// @desc    Upload multiple files to Cloudflare R2
// @route   POST /api/upload
// @access  Public (or Protected if middleware added)
router.post("/", upload.array("files", 10), async (req, res) => {
  try {
    if (!req.files || req.files.length === 0) {
      return res
        .status(400)
        .json({ success: false, message: "No files uploaded" });
    }

    const uploadedFiles = [];

    for (const file of req.files) {
      const safeName = sanitizeBaseName(file.originalname);
      const ext = extFromNameOrMime(file.originalname, file.mimetype);
      const key = `uploads/${Date.now()}-${Math.random().toString(36).slice(2, 8)}-${safeName}${ext}`;

      const stored = await uploadBufferToR2({
        key,
        body: file.buffer,
        contentType: file.mimetype,
      });

      uploadedFiles.push({
        url: stored.url,
        public_id: stored.key,
        original_name: file.originalname,
        format: file.mimetype,
        size: file.size,
      });
    }

    return res.status(200).json({
      success: true,
      data: uploadedFiles,
    });
  } catch (error) {
    console.error("[Upload Error]", error);
    const message = error?.message || error?.name || "Upload failed";
    return res.status(500).json({ success: false, message });
  }
});

// @desc    Fetch uploaded file from Cloudflare R2 via backend stream
// @route   GET /api/upload/file?key=uploads/... OR ?url=https://.../uploads/...
// @access  Public (same as upload)
router.get("/file", async (req, res) => {
  try {
    const bucket = String(process.env.R2_BUCKET || "").trim();
    const key = inferR2KeyFromInputs({
      key: req.query?.key,
      url: req.query?.url,
      bucket,
    });

    if (!key) {
      return res.status(400).json({
        success: false,
        message: "Missing file key or url query parameter",
      });
    }

    const objectData = await getObjectFromR2({ key });

    res.setHeader("Content-Type", objectData.contentType || "application/octet-stream");
    res.setHeader("Cache-Control", "public, max-age=300");
    if (objectData.contentLength) {
      res.setHeader("Content-Length", String(objectData.contentLength));
    }
    if (objectData.etag) {
      res.setHeader("ETag", objectData.etag);
    }
    if (objectData.lastModified) {
      res.setHeader("Last-Modified", new Date(objectData.lastModified).toUTCString());
    }
    const filename = path.basename(key);
    if (filename) {
      res.setHeader(
        "Content-Disposition",
        `inline; filename*=UTF-8''${encodeURIComponent(filename)}`,
      );
    }

    if (objectData.body && typeof objectData.body.pipe === "function") {
      objectData.body.on("error", (streamErr) => {
        if (!res.headersSent) {
          res.status(500).json({
            success: false,
            message: streamErr?.message || "Failed to stream file",
          });
        } else {
          res.destroy(streamErr);
        }
      });
      objectData.body.pipe(res);
      return;
    }

    if (objectData.body?.transformToByteArray) {
      const arr = await objectData.body.transformToByteArray();
      return res.send(Buffer.from(arr));
    }

    return res.status(500).json({
      success: false,
      message: "Unsupported response body from storage provider",
    });
  } catch (error) {
    const status =
      error?.$metadata?.httpStatusCode === 404 ||
      String(error?.name || "").toLowerCase().includes("nosuchkey")
        ? 404
        : 500;
    return res.status(status).json({
      success: false,
      message: error?.message || "Failed to fetch file",
    });
  }
});

export default router;
