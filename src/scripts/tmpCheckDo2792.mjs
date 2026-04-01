import 'dotenv/config';
import mongoose from 'mongoose';
import DeliveryOrder from '../models/DeliveryOrder.js';
await mongoose.connect(process.env.MONGO_URI);
const doc = await DeliveryOrder.findOne({ loanId: 'LN-2026-2792' }).lean();
console.log(JSON.stringify({
  loanId: doc?.loanId,
  do_exShowroomPrice: doc?.do_exShowroomPrice,
  do_insuranceCost: doc?.do_insuranceCost,
  do_roadTax: doc?.do_roadTax,
  do_processingFees: doc?.do_processingFees,
  updatedAt: doc?.updatedAt,
}, null, 2));
await mongoose.disconnect();
