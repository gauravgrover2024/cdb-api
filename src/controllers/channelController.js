import asyncHandler from 'express-async-handler';
import Channel from '../models/Channel.js';

// @desc    Get all channels with search and filtering
// @route   GET /api/channels
// @access  Public
const getChannels = asyncHandler(async (req, res) => {
  const { q, type, city, status } = req.query;
  const pageSize = req.query.limit ? Number(req.query.limit) : 50;
  const skip = Number(req.query.skip) || 0;

  let query = {};

  // Text search
  if (q) {
    query.$or = [
      { name: new RegExp(q, 'i') },
      { businessName: new RegExp(q, 'i') },
      { mobile: new RegExp(q, 'i') },
      { contactPerson: new RegExp(q, 'i') },
      { channelId: new RegExp(q, 'i') },
      { dsaCode: new RegExp(q, 'i') },
      { dealerCode: new RegExp(q, 'i') },
    ];
  }

  if (type) query.type = type;
  if (city) query.city = new RegExp(city, 'i');
  if (status) query.status = status;

  const count = await Channel.countDocuments(query);
  const channels = await Channel.find(query)
    .sort({ createdAt: -1 })
    .limit(pageSize)
    .skip(skip);

  res.json({
    success: true,
    count,
    data: channels,
  });
});

// @desc    Get channel by ID
// @route   GET /api/channels/:id
// @access  Public
const getChannelById = asyncHandler(async (req, res) => {
  const channel = await Channel.findById(req.params.id);
  
  if (channel) {
    res.json({ success: true, data: channel });
  } else {
    res.status(404);
    throw new Error('Channel not found');
  }
});

// @desc    Search channels by name or mobile (for auto-complete)
// @route   GET /api/channels/search
// @access  Public
const searchChannels = asyncHandler(async (req, res) => {
  const { term, type } = req.query;
  const limit = Math.min(Math.max(Number(req.query.limit) || 20, 1), 200);

  const query = { status: 'Active' };
  if (term && String(term).trim()) {
    query.$or = [
      { name: new RegExp(term, 'i') },
      { mobile: new RegExp(term, 'i') },
      { channelId: new RegExp(term, 'i') },
      { dsaCode: new RegExp(term, 'i') },
      { address: new RegExp(term, 'i') },
    ];
  }

  if (type) query.type = type;

  const channels = await Channel.find(query)
    .limit(limit)
    .select('channelId name type mobile contactPerson city address commissionRate payoutPercentage outstandingCommission');

  res.json({
    success: true,
    data: channels,
  });
});

// @desc    Create new channel
// @route   POST /api/channels
// @access  Private
const createChannel = asyncHandler(async (req, res) => {
  const { name, type, contactPerson, mobile, address, city } = req.body;

  if (!name || !type || !contactPerson || !mobile || !address || !city) {
    res.status(400);
    throw new Error('Please provide all required fields');
  }

  // Check if channel with same mobile already exists
  const channelExists = await Channel.findOne({ mobile });
  if (channelExists) {
    res.status(400);
    throw new Error('Channel with this mobile number already exists');
  }

  const channel = await Channel.create({
    ...req.body,
    createdBy: req.user?._id,
  });

  if (channel) {
    res.status(201).json({ success: true, data: channel });
  } else {
    res.status(400);
    throw new Error('Invalid channel data');
  }
});

// @desc    Update channel
// @route   PUT /api/channels/:id
// @access  Private
const updateChannel = asyncHandler(async (req, res) => {
  const channel = await Channel.findById(req.params.id);

  if (channel) {
    Object.keys(req.body).forEach((key) => {
      channel[key] = req.body[key];
    });
    
    channel.lastModifiedBy = req.user?._id;

    const updatedChannel = await channel.save();
    res.json({ success: true, data: updatedChannel });
  } else {
    res.status(404);
    throw new Error('Channel not found');
  }
});

// @desc    Delete channel
// @route   DELETE /api/channels/:id
// @access  Private
const deleteChannel = asyncHandler(async (req, res) => {
  const channel = await Channel.findById(req.params.id);

  if (channel) {
    // Soft delete by setting status to Inactive
    channel.status = 'Inactive';
    await channel.save();
    
    res.json({ success: true, message: 'Channel deactivated successfully' });
  } else {
    res.status(404);
    throw new Error('Channel not found');
  }
});

// @desc    Add payout entry to channel
// @route   POST /api/channels/:id/payouts
// @access  Private
const addChannelPayout = asyncHandler(async (req, res) => {
  const channel = await Channel.findById(req.params.id);

  if (!channel) {
    res.status(404);
    throw new Error('Channel not found');
  }

  const {
    loanId,
    loanAmount,
    commissionAmount,
    payoutAmount,
    paymentMode,
    utrNumber,
    status,
    remarks,
  } = req.body;

  const payoutEntry = {
    date: new Date(),
    loanId,
    loanAmount: loanAmount || 0,
    commissionAmount: commissionAmount || 0,
    payoutAmount: payoutAmount || 0,
    paymentMode,
    utrNumber,
    status: status || 'Pending',
    remarks,
  };

  channel.payoutHistory.push(payoutEntry);
  
  // Update commission totals
  if (commissionAmount > 0) {
    channel.totalCommissionEarned += commissionAmount;
  }
  if (status === 'Paid' && payoutAmount > 0) {
    channel.totalCommissionPaid += payoutAmount;
  }
  
  // Update business metrics
  channel.totalBusinessVolume += loanAmount || 0;
  channel.totalLoansClosed += 1;

  const updatedChannel = await channel.save();
  res.json({ success: true, data: updatedChannel });
});

// @desc    Get channel statistics
// @route   GET /api/channels/:id/stats
// @access  Public
const getChannelStats = asyncHandler(async (req, res) => {
  const channel = await Channel.findById(req.params.id);

  if (!channel) {
    res.status(404);
    throw new Error('Channel not found');
  }

  const stats = {
    totalLeadsGenerated: channel.totalLeadsGenerated,
    totalLoansApproved: channel.totalLoansApproved,
    totalLoansClosed: channel.totalLoansClosed,
    conversionRate: channel.conversionRate,
    totalBusinessVolume: channel.totalBusinessVolume,
    totalCommissionEarned: channel.totalCommissionEarned,
    totalCommissionPaid: channel.totalCommissionPaid,
    outstandingCommission: channel.outstandingCommission,
    payoutHistoryCount: channel.payoutHistory.length,
    rating: channel.rating,
    status: channel.status,
  };

  res.json({ success: true, data: stats });
});

export {
  getChannels,
  getChannelById,
  searchChannels,
  createChannel,
  updateChannel,
  deleteChannel,
  addChannelPayout,
  getChannelStats,
};
