import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import InsuranceCase from "../models/InsuranceCase.js";
import Vehicle from "../models/Vehicle.js";
import VehicleFeature from "../models/VehicleFeature.js";
import VehicleRecord from "../models/VehicleRecord.js";
import UsedCarLead from "../models/UsedCarLead.js";
import Receivable from "../models/Receivable.js";
import Payment from "../models/Payment.js";
import Customer from "../models/Customer.js";

dotenv.config();

const ensure = async (collection, specs) => {
  const results = [];
  for (const [keys, options] of specs) {
    try {
      const name = await collection.createIndex(keys, {
        background: true,
        ...options,
      });
      results.push(name);
    } catch (error) {
      if (error?.code === 85 || /Index already exists/i.test(error?.message || "")) {
        results.push(`${options?.name || JSON.stringify(keys)} (already covered)`);
        continue;
      }
      throw error;
    }
  }
  return results;
};

const run = async () => {
  try {
    await connectDB();

    const plan = [
      [
        "loans",
        Loan.collection,
        [
          [{ customerName: 1, updatedAt: -1 }, { name: "ai_customer_updated" }],
          [{ vehicleModel: 1, vehicleVariant: 1, updatedAt: -1 }, { name: "ai_vehicle_model_variant_updated" }],
          [{ vehicleRegNo: 1, updatedAt: -1 }, { name: "ai_vehicle_reg_updated" }],
          [{ registrationNumber: 1, vehicleRegNo: 1, rc_redg_no: 1 }, { name: "ai_registration_fields" }],
          [{ payoutApplicable: 1, payout_percentage: 1, prefile_sourcePayoutPercentage: 1, updatedAt: -1 }, { name: "ai_loan_payout_missing" }],
          [{ loanStatus: 1, status: 1, currentStage: 1, updatedAt: -1 }, { name: "ai_loan_status_updated" }],
        ],
      ],
      [
        "insurance",
        InsuranceCase.collection,
        [
          [{ customerName: 1, updatedAt: -1 }, { name: "ai_ins_customer_updated" }],
          [{ registrationNumber: 1, updatedAt: -1 }, { name: "ai_ins_registration_updated" }],
          [{ vehicleMake: 1, vehicleModel: 1, vehicleVariant: 1, updatedAt: -1 }, { name: "ai_ins_vehicle_updated" }],
          [{ payoutApplicable: 1, payoutPercent: 1, updatedAt: -1 }, { name: "ai_ins_payout_missing" }],
          [{ newOdExpiryDate: 1, newTpExpiryDate: 1, status: 1, updatedAt: -1 }, { name: "ai_ins_expiry_status" }],
        ],
      ],
      [
        "vehicles",
        Vehicle.collection,
        [
          [{ model: 1, variant: 1, city: 1 }, { name: "ai_vehicle_model_variant_city" }],
          [{ brand: 1, model: 1, variant: 1, city: 1 }, { name: "ai_vehicle_brand_model_variant_city" }],
          [{ model: 1, on_road_price_cardekho: 1, ex_showroom: 1 }, { name: "ai_vehicle_model_prices" }],
        ],
      ],
      [
        "vehicle_features",
        VehicleFeature.collection,
        [
          [{ model: 1, variant: 1 }, { name: "ai_feature_model_variant" }],
          [{ model: 1, updatedAt: -1 }, { name: "ai_feature_model_updated" }],
        ],
      ],
      [
        "vehicle_master_records",
        VehicleRecord.collection,
        [
          [{ customerName: 1, updatedAt: -1 }, { name: "ai_vehicle_record_customer_updated" }],
          [{ registrationNumber: 1, updatedAt: -1 }, { name: "ai_vehicle_record_registration_updated" }],
        ],
      ],
      [
        "used_car_leads",
        UsedCarLead.collection,
        [
          [{ "seller.name": 1, updatedAt: -1 }, { name: "ai_used_seller_updated" }],
          [{ "vehicle.regNo": 1, updatedAt: -1 }, { name: "ai_used_reg_updated" }],
          [{ "backgroundCheck.status": 1, "vehicle.regNo": 1, updatedAt: -1 }, { name: "ai_used_background_reg_updated" }],
        ],
      ],
      [
        "receivables",
        Receivable.collection,
        [
          [{ payout_status: 1, payout_amount: 1, updatedAt: -1 }, { name: "ai_receivable_payout_status_amount" }],
          [{ customerName: 1, updatedAt: -1 }, { name: "ai_receivable_customer_updated" }],
        ],
      ],
      [
        "payments",
        Payment.collection,
        [
          [{ loanId: 1, updatedAt: -1 }, { name: "ai_payment_loan_updated" }],
          [{ customerName: 1, updatedAt: -1 }, { name: "ai_payment_customer_updated" }],
        ],
      ],
      [
        "customers",
        Customer.collection,
        [
          [{ customerName: 1, updatedAt: -1 }, { name: "ai_customer_name_updated" }],
          [{ primaryMobile: 1, updatedAt: -1 }, { name: "ai_customer_mobile_updated" }],
        ],
      ],
    ];

    for (const [label, collection, specs] of plan) {
      const created = await ensure(collection, specs);
      console.log(`${label}: ${created.join(", ")}`);
    }
  } catch (error) {
    console.error("Failed to ensure AI Agent indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
