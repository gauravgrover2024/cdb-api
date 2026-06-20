import asyncHandler from "express-async-handler";
import UsedCar from "../models/UsedCar.js";

// @desc    Get all used cars with filters and pagination
// @route   GET /api/used-cars-db
// @access  Public
const getUsedCars = asyncHandler(async (req, res) => {
  const { make, model, variant, year, transmission, fuel_type, q } = req.query;
  const limit = Math.min(Math.max(Number(req.query.limit) || 20, 1), 100);
  const skip = Math.max(Number(req.query.skip) || 0, 0);

  const query = {};

  if (make) query.make = { $regex: new RegExp(make.trim(), "i") };
  if (model) query.model = { $regex: new RegExp(model.trim(), "i") };
  if (variant) query.variant = { $regex: new RegExp(variant.trim(), "i") };
  if (year) query.year = Number(year);
  if (transmission) query.transmission = { $regex: new RegExp(transmission.trim(), "i") };
  if (fuel_type) query.fuel_type = { $regex: new RegExp(fuel_type.trim(), "i") };

  // General search query matching make, model or variant
  if (q) {
    const searchRegex = new RegExp(q.trim(), "i");
    query.$or = [
      { make: searchRegex },
      { model: searchRegex },
      { variant: searchRegex },
      { model_generation: searchRegex }
    ];
  }

  const count = await UsedCar.countDocuments(query);
  const cars = await UsedCar.find(query)
    .sort({ make: 1, model: 1, year: -1 })
    .limit(limit)
    .skip(skip)
    .lean();

  res.json({
    success: true,
    count,
    data: cars,
  });
});

// @desc    Get used car by ID
// @route   GET /api/used-cars-db/:id
// @access  Public
const getUsedCarById = asyncHandler(async (req, res) => {
  const car = await UsedCar.findById(req.params.id);

  if (car) {
    res.json({ success: true, data: car });
  } else {
    res.status(404);
    throw new Error("Used car record not found");
  }
});

// @desc    Create new used car record
// @route   POST /api/used-cars-db
// @access  Private
const createUsedCar = asyncHandler(async (req, res) => {
  const { make, model, variant, year } = req.body;

  if (!make || !model || !variant || !year) {
    res.status(400);
    throw new Error("Please provide make, model, variant, and year");
  }

  const existingCar = await UsedCar.findOne({
    make: make.trim(),
    model: model.trim(),
    variant: variant.trim(),
    year: Number(year),
  });

  if (existingCar) {
    res.status(400);
    throw new Error("A used car with this exact make, model, variant, and year already exists");
  }

  const car = await UsedCar.create(req.body);

  res.status(201).json({
    success: true,
    data: car,
  });
});

// @desc    Update used car record by ID
// @route   PUT /api/used-cars-db/:id
// @access  Private
const updateUsedCar = asyncHandler(async (req, res) => {
  const car = await UsedCar.findById(req.params.id);

  if (!car) {
    res.status(404);
    throw new Error("Used car record not found");
  }

  // Update fields
  Object.keys(req.body).forEach((key) => {
    car[key] = req.body[key];
  });

  const updatedCar = await car.save();

  res.json({
    success: true,
    data: updatedCar,
  });
});

// @desc    Delete used car record by ID
// @route   DELETE /api/used-cars-db/:id
// @access  Private
const deleteUsedCar = asyncHandler(async (req, res) => {
  const car = await UsedCar.findById(req.params.id);

  if (!car) {
    res.status(404);
    throw new Error("Used car record not found");
  }

  await car.deleteOne();

  res.json({
    success: true,
    message: "Used car record deleted successfully",
  });
});

const getUniqueMakes = asyncHandler(async (req, res) => {
  const includeDiscontinued = req.query.includeDiscontinued === "true";
  const query = {};
  if (!includeDiscontinued) {
    query.$or = [
      { is_discontinued: { $exists: false } },
      { is_discontinued: false },
      { is_discontinued: null }
    ];
  }
  const makes = await UsedCar.distinct("make", query);
  makes.sort((a, b) => a.localeCompare(b));
  res.json({ success: true, data: makes });
});

const getUniqueModels = asyncHandler(async (req, res) => {
  const { make } = req.query;
  const includeDiscontinued = req.query.includeDiscontinued === "true";
  if (!make) {
    res.status(400);
    throw new Error("Make parameter is required");
  }
  const query = {
    make: { $regex: new RegExp(`^${make.trim()}$`, "i") }
  };
  if (!includeDiscontinued) {
    query.$or = [
      { is_discontinued: { $exists: false } },
      { is_discontinued: false },
      { is_discontinued: null }
    ];
  }
  const models = await UsedCar.distinct("model", query);
  models.sort((a, b) => a.localeCompare(b));
  res.json({ success: true, data: models });
});

const getUniqueVariants = asyncHandler(async (req, res) => {
  const { make, model } = req.query;
  const includeDiscontinued = req.query.includeDiscontinued === "true";
  if (!make || !model) {
    res.status(400);
    throw new Error("Make and Model parameters are required");
  }
  const query = {
    make: { $regex: new RegExp(`^${make.trim()}$`, "i") },
    model: { $regex: new RegExp(`^${model.trim()}$`, "i") }
  };
  if (!includeDiscontinued) {
    query.$or = [
      { is_discontinued: { $exists: false } },
      { is_discontinued: false },
      { is_discontinued: null }
    ];
  }
  const variants = await UsedCar.distinct("variant", query);
  variants.sort((a, b) => a.localeCompare(b));
  res.json({ success: true, data: variants });
});

const getUsedCarByDetails = asyncHandler(async (req, res) => {
  const { make, model, variant } = req.query;
  if (!make || !model || !variant) {
    res.status(400);
    throw new Error("Make, model, and variant are required");
  }
  const query = {
    make: { $regex: new RegExp(`^${make.trim()}$`, "i") },
    model: { $regex: new RegExp(`^${model.trim()}$`, "i") },
    variant: { $regex: new RegExp(`^${variant.trim()}$`, "i") }
  };
  const car = await UsedCar.findOne(query).sort({ year: -1 });
  if (car) {
    res.json({ success: true, data: car });
  } else {
    res.status(404);
    throw new Error("Used car variant not found");
  }
});

export {
  getUsedCars,
  getUsedCarById,
  createUsedCar,
  updateUsedCar,
  deleteUsedCar,
  getUniqueMakes,
  getUniqueModels,
  getUniqueVariants,
  getUsedCarByDetails,
};

