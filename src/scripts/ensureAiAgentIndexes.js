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
import AciLead from "../models/AciLead.js";

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

const optionalCollection = async (name) => {
  const exists = await mongoose.connection.db
    .listCollections({ name }, { nameOnly: true })
    .toArray();
  return exists.length ? mongoose.connection.db.collection(name) : null;
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
          [{ model_normalized: 1 }, { name: "ai_vehicle_model_normalized" }],
          [{ model_normalized: 1, city: 1 }, { name: "ai_vehicle_model_normalized_city" }],
          [{ model: 1, variant: 1, city: 1 }, { name: "ai_vehicle_model_variant_city" }],
          [{ brand: 1, model: 1, variant: 1, city: 1 }, { name: "ai_vehicle_brand_model_variant_city" }],
          [{ model: 1, on_road_price_cardekho: 1, ex_showroom: 1 }, { name: "ai_vehicle_model_prices" }],
          [{ brand_normalized: 1, model_normalized: 1, variant_normalized: 1, city: 1 }, { name: "ai_vehicle_normalized_catalogue_city" }],
          [{ brand_normalized: 1, model_normalized: 1, city: 1, is_discontinued: 1, ex_showroom: 1 }, { name: "vehicle_popular_price_city_exact" }],
          [{ brand_normalized: 1, model_normalized: 1, is_discontinued: 1, ex_showroom: 1 }, { name: "vehicle_popular_price_exact" }],
          [{ city: 1, is_discontinued: 1, LastSeenDate: -1 }, { name: "ai_vehicle_city_active_seen" }],
          [{ bodyType: 1, fuel: 1, transmission: 1, ex_showroom: 1 }, { name: "ai_vehicle_body_fuel_trans_price" }],
        ],
      ],
      [
        "vehicle_features",
        VehicleFeature.collection,
        [
          [{ model: 1, variant: 1 }, { name: "ai_feature_model_variant" }],
          [{ model: 1, updatedAt: -1 }, { name: "ai_feature_model_updated" }],
          [{ brand: 1, model: 1, variant: 1, updatedAt: -1 }, { name: "ai_feature_brand_model_variant_updated" }],
          [{ model: 1, variant: 1, body_type_bucket: 1 }, { name: "ai_feature_model_variant_body_type" }],
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
      [
        "aci_leads",
        AciLead.collection,
        [
          [{ leadId: 1 }, { name: "ai_lead_id_unique", unique: true }],
          [{ leadType: 1, status: 1, priority: 1, createdAt: -1 }, { name: "ai_lead_type_status_priority" }],
          [{ "customer.mobile": 1, createdAt: -1 }, { name: "ai_lead_customer_mobile" }],
          [{ "vehicle.model": 1, createdAt: -1 }, { name: "ai_lead_vehicle_model" }],
        ],
      ],
    ];

    for (const [label, collection, specs] of plan) {
      const created = await ensure(collection, specs);
      console.log(`${label}: ${created.join(", ")}`);
    }

    const vehicleColorsCollection = await optionalCollection("vehicle_colors_v2");
    if (vehicleColorsCollection) {
      const created = await ensure(vehicleColorsCollection, [
        [{ brand_slug: 1, model_slug: 1 }, { name: "uniq_brand_model_color_media", unique: true }],
        [{ brand: 1, model: 1, variant: 1 }, { name: "vehicle_colors_v2_brand_model_variant" }],
        [{ brand: 1, model: 1 }, { name: "vehicle_colors_v2_brand_model" }],
        [{ brand: 1, model: 1, color_name: 1 }, { name: "ai_vehicle_colors_brand_model_color" }],
        [{ "colors.name": 1, brand: 1, model: 1 }, { name: "vehicle_colors_v2_nested_color_name" }],
        [{ model: 1, updatedAt: -1 }, { name: "ai_vehicle_colors_model_updated" }],
        [{ brand: 1, model: 1, scopeStatus: 1, color_name: 1, updatedAt: -1 }, { name: "vehicle_colors_brand_model_scope_color_updated" }],
        [{ make: 1, model: 1, scopeStatus: 1, color_name: 1, updatedAt: -1 }, { name: "vehicle_colors_make_model_scope_color_updated" }],
        [{ model: 1, scopeStatus: 1, color_name: 1, updatedAt: -1 }, { name: "vehicle_colors_model_scope_color_updated" }],
        [{ brand: 1, model: 1, activeColorCount: -1, updatedAt: -1 }, { name: "vehicle_colors_v2_brand_model_active_updated" }],
        [{ brand_slug: 1, model_slug: 1, activeColorCount: -1, updatedAt: -1 }, { name: "vehicle_colors_v2_slug_active_updated" }],
      ]);
      console.log(`vehicle_colors_v2: ${created.join(", ")}`);
    } else {
      console.log("vehicle_colors_v2: collection not present, skipped optional indexes");
    }

    const monthlySalesCollection = await optionalCollection("monthly_car_sales");
    if (monthlySalesCollection) {
      const created = await ensure(monthlySalesCollection, [
        [{ source: 1, month: -1, rank: 1 }, { name: "monthly_sales_source_month_rank" }],
      ]);
      console.log(`monthly_car_sales: ${created.join(", ")}`);
    } else {
      console.log("monthly_car_sales: collection not present, skipped optional indexes");
    }

    const offersCollection = await optionalCollection("offers");
    if (offersCollection) {
      const created = await ensure(offersCollection, [
        [{ brand: 1, model: 1, city: 1, updatedAt: -1 }, { name: "ai_offers_brand_model_city_updated" }],
      ]);
      console.log(`offers: ${created.join(", ")}`);
    } else {
      console.log("offers: collection not present, skipped optional indexes");
    }

    const serviceCentersCollection = await optionalCollection("service_centers");
    if (serviceCentersCollection) {
      const created = await ensure(serviceCentersCollection, [
        [{ brand: 1, city: 1, location: 1, updatedAt: -1 }, { name: "ai_service_centers_brand_city_location_updated" }],
      ]);
      console.log(`service_centers: ${created.join(", ")}`);
    } else {
      console.log("service_centers: collection not present, skipped optional indexes");
    }

    const priceHistoryCollection = await optionalCollection("price_history");
    if (priceHistoryCollection) {
      const created = await ensure(priceHistoryCollection, [
        [{ brand: 1, model: 1, variant: 1, city: 1, date: 1 }, { name: "ai_price_history_lookup" }],
      ]);
      console.log(`price_history: ${created.join(", ")}`);
    } else {
      console.log("price_history: collection not present, skipped optional indexes");
    }

    const bankDirectoriesCollection = await optionalCollection("bankdirectories");
    if (bankDirectoriesCollection) {
      const created = await ensure(bankDirectoriesCollection, [
        [{ ifsc: 1 }, { name: "ai_bank_ifsc_lookup" }],
        [{ bankName: 1, branch: 1, active: 1 }, { name: "ai_bank_name_branch_active" }],
      ]);
      console.log(`bankdirectories: ${created.join(", ")}`);
    } else {
      console.log("bankdirectories: collection not present, skipped optional indexes");
    }
  } catch (error) {
    console.error("Failed to ensure AI Agent indexes:", error);
    process.exitCode = 1;
  } finally {
    await mongoose.connection.close();
  }
};

run();
