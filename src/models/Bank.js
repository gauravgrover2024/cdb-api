import mongoose from 'mongoose';

const bankSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    trim: true,
  },
  ifsc: {
    type: String,
    required: true,
    trim: true,
    uppercase: true,
    unique: true,
  },
  address: {
    type: String,
    required: false,
    trim: true,
  },
  createdAt: {
    type: Date,
    default: Date.now,
  },
});

const Bank = mongoose.model('Bank', bankSchema);
export default Bank;
