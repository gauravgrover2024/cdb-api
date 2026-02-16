
import express from 'express';
import { upload } from '../config/cloudinary.js';

const router = express.Router();

// @desc    Upload multiple files
// @route   POST /api/upload
// @access  Public (or Protected if middleware added)
router.post('/', upload.array('files', 10), (req, res) => {
  try {
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({ success: false, message: 'No files uploaded' });
    }

    const uploadedFiles = req.files.map((file) => ({
      url: file.path,
      public_id: file.filename,
      original_name: file.originalname,
      format: file.mimetype,
      size: file.size,
    }));

    res.status(200).json({
      success: true,
      data: uploadedFiles,
    });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

export default router;
