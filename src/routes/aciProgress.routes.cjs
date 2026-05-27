const express = require("express");
const { getAciProgressSnapshot } = require("../services/aciProgress/aciProgress.service.cjs");

const router = express.Router();

router.get("/", (req, res) => {
  try {
    res.set("Cache-Control", "no-store");
    res.json(getAciProgressSnapshot());
  } catch (error) {
    console.error("[ACI Progress] Failed to build progress snapshot:", error);
    res.status(500).json({
      error: "ACI_PROGRESS_SNAPSHOT_FAILED",
      message: error.message
    });
  }
});

module.exports = router;
