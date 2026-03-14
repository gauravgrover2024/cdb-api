import express from "express";
import multer from "multer";
import path from "path";
import { uploadBufferToR2 } from "../config/r2.js";

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

export default router;
