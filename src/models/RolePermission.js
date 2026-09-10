import mongoose from "mongoose";

const rolePermissionSchema = new mongoose.Schema(
  {
    role: { type: String, required: true, unique: true, index: true },
    permissions: { type: mongoose.Schema.Types.Mixed, default: {} },
    updatedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User", default: null },
  },
  { timestamps: true, strict: true },
);

export default mongoose.model("RolePermission", rolePermissionSchema);
