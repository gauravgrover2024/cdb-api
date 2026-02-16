import asyncHandler from 'express-async-handler';
import Vehicle from '../models/Vehicle.js';

// @desc    Get all vehicles with search and filtering
// @route   GET /api/vehicles
// @access  Public
const getVehicles = asyncHandler(async (req, res) => {
  const { q, make, model, city, fuel } = req.query;
  const pageSize = req.query.limit ? Number(req.query.limit) : null; // null = unlimited
  const skip = Number(req.query.skip) || 0;

  let query = {};

  if (q) {
    query.$or = [
      { make: new RegExp(q, 'i') },
      { model: new RegExp(q, 'i') },
      { variant: new RegExp(q, 'i') },
    ];
  }

  if (make) query.make = make;
  if (model) query.model = model;
  if (city) query.city = city;
  if (fuel) query.fuel = fuel;

  const count = await Vehicle.countDocuments(query);
  let vehicleQuery = Vehicle.find(query)
    .sort({ make: 1, model: 1, variant: 1 });
  
  // Apply limit only if specified
  if (pageSize) {
    vehicleQuery = vehicleQuery.limit(pageSize);
  }
  
  const vehicles = await vehicleQuery.skip(skip);

  res.json({
    success: true,
    count,
    data: vehicles,
  });
});

// @desc    Get vehicle by ID
// @route   GET /api/vehicles/:id
// @access  Public
const getVehicleById = asyncHandler(async (req, res) => {
  // Skip if the param is not a valid ObjectId (e.g., "by-details")
  if (!req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
    res.status(400);
    throw new Error('Invalid vehicle ID format');
  }
  
  const vehicle = await Vehicle.findById(req.params.id);
  if (vehicle) {
    res.json({ success: true, data: vehicle });
  } else {
    res.status(404);
    throw new Error('Vehicle not found');
  }
});

// @desc    Create a vehicle
// @route   POST /api/vehicles
// @access  Public
const createVehicle = asyncHandler(async (req, res) => {
  const { make, model, variant, fuel, city } = req.body;

  if (!make || !model || !variant) {
    res.status(400);
    throw new Error('Please include Make, Model, and Variant');
  }

  const vehicleExists = await Vehicle.findOne({ make, model, variant, fuel, city });
  if (vehicleExists) {
    res.status(400);
    throw new Error('Vehicle variant already exists for this city/fuel combination');
  }

  const vehicle = await Vehicle.create(req.body);

  if (vehicle) {
    res.status(201).json({ success: true, data: vehicle });
  } else {
    res.status(400);
    throw new Error('Invalid vehicle data');
  }
});

// @desc    Update a vehicle
// @route   PUT /api/vehicles/:id
// @access  Public
const updateVehicle = asyncHandler(async (req, res) => {
  const vehicle = await Vehicle.findById(req.params.id);

  if (vehicle) {
    Object.assign(vehicle, req.body);
    const updatedVehicle = await vehicle.save();
    res.json({ success: true, data: updatedVehicle });
  } else {
    res.status(404);
    throw new Error('Vehicle not found');
  }
});

// @desc    Delete a vehicle
// @route   DELETE /api/vehicles/:id
// @access  Public
const deleteVehicle = asyncHandler(async (req, res) => {
  const vehicle = await Vehicle.findById(req.params.id);
  if (vehicle) {
    await vehicle.deleteOne();
    res.json({ success: true, message: 'Vehicle removed' });
  } else {
    res.status(404);
    throw new Error('Vehicle not found');
  }
});

// @desc    Bulk create/update vehicles
// @route   POST /api/vehicles/bulk
// @access  Public
const bulkUploadVehicles = asyncHandler(async (req, res) => {
  const vehiclesData = req.body; // Expecting an array

  if (!Array.isArray(vehiclesData)) {
    res.status(400);
    throw new Error('Expected an array of vehicle objects');
  }

  const results = {
    inserted: 0,
    updated: 0,
    errors: [],
  };

  for (const item of vehiclesData) {
    try {
      const { make, model, variant, fuel, city } = item;
      if (!make || !model || !variant) continue;

      const filter = { make, model, variant, fuel, city };
      const update = { ...item };
      
      const res = await Vehicle.findOneAndUpdate(filter, update, {
        upsert: true,
        new: true,
        rawResult: true,
      });

      if (res.lastErrorObject.updatedExisting) {
        results.updated++;
      } else {
        results.inserted++;
      }
    } catch (error) {
      results.errors.push({ item, error: error.message });
    }
  }

  res.json({ success: true, data: results });
});

// @desc    Get unique makes from vehicle database
// @route   GET /api/vehicles/distinct/makes
// @access  Public
const getUniqueMakes = asyncHandler(async (req, res) => {
  const makes = await Vehicle.distinct('make');
  const sortedMakes = makes.filter(Boolean).sort();
  res.json({
    success: true,
    data: sortedMakes,
  });
});

// @desc    Get unique models for a specific make
// @route   GET /api/vehicles/distinct/models?make=Toyota
// @access  Public
const getUniqueModels = asyncHandler(async (req, res) => {
  const { make } = req.query;
  
  if (!make) {
    res.status(400);
    throw new Error('Make parameter is required');
  }

  const models = await Vehicle.distinct('model', { make });
  const sortedModels = models.filter(Boolean).sort();
  res.json({
    success: true,
    data: sortedModels,
  });
});

// @desc    Get unique variants for a specific make and model
// @route   GET /api/vehicles/distinct/variants?make=Toyota&model=Corolla
// @access  Public
const getUniqueVariants = asyncHandler(async (req, res) => {
  const { make, model } = req.query;
  
  if (!make || !model) {
    res.status(400);
    throw new Error('Make and Model parameters are required');
  }

  const variants = await Vehicle.distinct('variant', { make, model });
  const sortedVariants = variants.filter(Boolean).sort();
  res.json({
    success: true,
    data: sortedVariants,
  });
});

// @desc    Get vehicle details by make, model, variant
// @route   GET /api/vehicles/by-details?make=Toyota&model=Corolla&variant=1.8E
// @access  Public
const getVehicleByDetails = asyncHandler(async (req, res) => {
  const { make, model, variant, fuel, city } = req.query;
  
  if (!make || !model || !variant) {
    res.status(400);
    throw new Error('Make, Model, and Variant parameters are required');
  }

  const query = { make, model, variant };
  if (fuel) query.fuel = fuel;
  if (city) query.city = city;

  const vehicle = await Vehicle.findOne(query);
  
  if (vehicle) {
    res.json({ success: true, data: vehicle });
  } else {
    res.json({ success: false, data: null, message: 'Vehicle not found' });
  }
});

export {
  getVehicles,
  getVehicleById,
  createVehicle,
  updateVehicle,
  deleteVehicle,
  bulkUploadVehicles,
  getUniqueMakes,
  getUniqueModels,
  getUniqueVariants,
  getVehicleByDetails,
};
