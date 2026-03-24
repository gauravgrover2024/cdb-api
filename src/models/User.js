import mongoose from 'mongoose';
import bcrypt from 'bcryptjs';

const userSchema = mongoose.Schema(
  {
    name: { type: String, required: true },
    email: { type: String, required: true, unique: true },
    password: { type: String }, // Optional — not set for Google/Firebase-only users
    firebaseUid: { type: String, unique: true, sparse: true },
    role: {
      type: String,
      enum: ['superadmin', 'admin', 'staff', 'user', 'demo'],
      default: 'staff',
    },
    status: {
      type: String,
      enum: ['pending', 'active', 'rejected', 'deactivated'],
      default: 'pending',
    },
  },
  {
    timestamps: true,
  }
);

// Method to check password (only for email/password users)
userSchema.methods.matchPassword = async function (enteredPassword) {
  if (!this.password) return false;
  return await bcrypt.compare(enteredPassword, this.password);
};

// Hash password before save if it was modified
userSchema.pre('save', async function () {
  if (!this.isModified('password') || !this.password) {
    return;
  }
  const salt = await bcrypt.genSalt(10);
  this.password = await bcrypt.hash(this.password, salt);
});

const User = mongoose.model('User', userSchema);

export default User;
