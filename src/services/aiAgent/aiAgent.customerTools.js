import Customer from "../../models/Customer.js";
import Loan from "../../models/Loan.js";
import InsuranceCase from "../../models/InsuranceCase.js";
import Payment from "../../models/Payment.js";
import VehicleRecord from "../../models/VehicleRecord.js";
import UsedCarLead from "../../models/UsedCarLead.js";
import DeliveryOrder from "../../models/DeliveryOrder.js";
import {
  action,
  unavailableWidget,
  widget,
} from "./aiAgent.renderPayloads.js";
import {
  firstMeaningful,
  getRegistration,
  getVehicleName,
  makeRegex,
  normalizeText,
  registrationConditions,
} from "./aiAgent.normalizers.js";
import {
  buildEntityQuery,
  buildTextClauses,
  entityOption,
  getCustomerRoute,
  getInsuranceRoute,
  getLoanRoute,
  getPaymentRoute,
  getUsedCarRoute,
  LIMIT,
  makeAmbiguity,
  pushModuleTrace,
  safeId,
} from "./aiAgent.tools.js";
import { noteRestriction } from "./aiAgent.accessControl.js";

const customerQuery = (name) => {
  const clauses = buildTextClauses(["customerName", "name", "primaryMobile", "mobile"], name);
  return clauses.length ? { $or: clauses } : {};
};

const customerDetails = (customer) => ({
  id: safeId(customer),
  customerId: customer.customerId,
  name: firstMeaningful(customer.customerName, customer.name),
  mobile: firstMeaningful(customer.primaryMobile, customer.mobile),
  email: customer.email,
  city: customer.city,
  status: customer.status,
});

export const customer360 = async (parsed, access, trace) => {
  if (!access.canAccess("customers")) {
    noteRestriction(access, "Customers", "No customer access");
    return { widgets: [unavailableWidget("Customer data unavailable", "You do not have customer access.", ["Customers"])] };
  }
  const name = parsed.entities.customerName || parsed.selectedEntity?.customerName;
  if (!name) {
    return { widgets: [unavailableWidget("Need a customer", "Share a customer name to build Customer 360.", ["Customers"])] };
  }
  const customers = await Customer.find(customerQuery(name)).sort({ updatedAt: -1 }).limit(10).lean();
  pushModuleTrace(trace, "Customers", customers.length);
  if (customers.length > 1 && !parsed.selectedEntity) {
    return {
      ambiguity: makeAmbiguity(
        customers.map((item) => ({
          id: safeId(item),
          entityType: "customer",
          displayName: firstMeaningful(item.customerName, item.name),
          customerName: firstMeaningful(item.customerName, item.name),
          module: "Customers",
          status: item.status,
          lastActivityDate: item.updatedAt,
          context: { customerName: firstMeaningful(item.customerName, item.name) },
        })),
      ),
      widgets: [],
      followUpSuggestions: [],
    };
  }
  const customer = customers[0] || { customerName: name };
  const customerName = firstMeaningful(customer.customerName, customer.name, name);
  const nameRegex = makeRegex(customerName);
  const loans = access.canAccess("loans")
    ? await Loan.find({ customerName: nameRegex }).sort({ updatedAt: -1 }).limit(LIMIT).lean()
    : [];
  const insurance = access.canAccess("insurance")
    ? await InsuranceCase.find({ customerName: nameRegex }).sort({ updatedAt: -1 }).limit(LIMIT).lean()
    : [];
  const payments = access.canAccess("payments") && access.canViewFinance
    ? await Payment.find({ customerName: nameRegex }).sort({ updatedAt: -1 }).limit(20).lean()
    : [];
  const vehicles = access.canAccess("vehicles")
    ? await VehicleRecord.find({ customerName: nameRegex }).sort({ updatedAt: -1 }).limit(LIMIT).lean()
    : [];
  const usedCarLeads = access.canAccess("usedCars")
    ? await UsedCarLead.find({ "seller.name": nameRegex }).sort({ updatedAt: -1 }).limit(20).lean()
    : [];
  pushModuleTrace(trace, "Loans", loans.length);
  pushModuleTrace(trace, "Insurance", insurance.length);
  pushModuleTrace(trace, "Payments", payments.length);
  pushModuleTrace(trace, "Vehicle Records", vehicles.length);
  pushModuleTrace(trace, "Used Cars", usedCarLeads.length);

  return {
    widgets: [
      widget("customer_360", `Customer 360: ${customerName}`, {
        data: {
          customer: customerDetails(customer),
          linkedVehicles: [
            ...vehicles.map((item) => ({
              id: safeId(item),
              vehicle: [item.make, item.model, item.variant].filter(Boolean).join(" "),
              registrationNumber: item.registrationNumber,
            })),
            ...loans.map((item) => ({ vehicle: getVehicleName(item), registrationNumber: getRegistration(item) })),
            ...insurance.map((item) => ({ vehicle: getVehicleName(item), registrationNumber: getRegistration(item) })),
          ],
          linkedLoans: loans.map((item) => ({
            id: safeId(item),
            loanId: item.loanId,
            vehicle: getVehicleName(item),
            status: firstMeaningful(item.loanStatus, item.status, item.currentStage),
            route: getLoanRoute(item),
          })),
          linkedInsurance: insurance.map((item) => ({
            id: safeId(item),
            caseId: item.caseId,
            vehicle: getVehicleName(item),
            status: item.status,
            route: getInsuranceRoute(item),
          })),
          linkedPayments: payments,
          linkedUsedCarLeads: usedCarLeads.map((item) => ({
            id: safeId(item),
            vehicle: [item?.vehicle?.make, item?.vehicle?.model].filter(Boolean).join(" "),
            status: firstMeaningful(item?.workflow?.status, item.status),
            route: getUsedCarRoute(item),
          })),
          latestActivity: [customer.updatedAt, ...loans.map((item) => item.updatedAt), ...insurance.map((item) => item.updatedAt)].filter(Boolean).sort().at(-1),
        },
        actions: [
          customer._id && action("open_record", "Open customer", { route: getCustomerRoute(customer) }),
          customer._id && access.canEdit && action("edit_record", "Edit customer", { route: getCustomerRoute(customer) }),
        ].filter(Boolean),
      }),
    ],
    followUpSuggestions: ["Show vehicle 360", "Latest insurance", "Check loan status", "Approx loan closure"],
  };
};

const vehicleQueries = (parsed) => {
  const { entities } = parsed;
  return {
    loan: buildEntityQuery({
      entities,
      customerFields: ["customerName"],
      registrationFields: ["registrationNumber", "vehicleRegNo", "rc_redg_no"],
      vehicleFields: ["vehicleMake", "vehicleModel", "vehicleVariant"],
    }),
    insurance: buildEntityQuery({
      entities,
      customerFields: ["customerName"],
      registrationFields: ["registrationNumber"],
      vehicleFields: ["vehicleMake", "vehicleModel", "vehicleVariant"],
    }),
    vehicleRecord: {
      $and: [
        ...(registrationConditions(["registrationNumber"], entities.registrationNumber, entities.last4).length
          ? [{ $or: registrationConditions(["registrationNumber"], entities.registrationNumber, entities.last4) }]
          : []),
        ...(entities.model ? [{ model: { $regex: entities.model, $options: "i" } }] : []),
      ],
    },
    usedCar: {
      $or: registrationConditions(["vehicle.regNo"], entities.registrationNumber, entities.last4),
    },
  };
};

export const vehicle360 = async (parsed, access, trace) => {
  const queries = vehicleQueries(parsed);
  const loans = access.canAccess("loans") && Object.keys(queries.loan).length
    ? await Loan.find(queries.loan).sort({ updatedAt: -1 }).limit(LIMIT).lean()
    : [];
  const insurance = access.canAccess("insurance") && Object.keys(queries.insurance).length
    ? await InsuranceCase.find(queries.insurance).sort({ updatedAt: -1 }).limit(LIMIT).lean()
    : [];
  const vehicles = access.canAccess("vehicles") && queries.vehicleRecord.$and.length
    ? await VehicleRecord.find(queries.vehicleRecord).sort({ updatedAt: -1 }).limit(LIMIT).lean()
    : [];
  const usedCarLeads = access.canAccess("usedCars") && queries.usedCar.$or.length
    ? await UsedCarLead.find(queries.usedCar).sort({ updatedAt: -1 }).limit(20).lean()
    : [];
  pushModuleTrace(trace, "Loans", loans.length);
  pushModuleTrace(trace, "Insurance", insurance.length);
  pushModuleTrace(trace, "Vehicle Records", vehicles.length);
  pushModuleTrace(trace, "Used Cars", usedCarLeads.length);

  const all = [...loans, ...insurance, ...vehicles, ...usedCarLeads];
  if (!all.length) {
    return { widgets: [unavailableWidget("No vehicle found", "No matching vehicle history was found.", ["Loans", "Insurance", "Vehicle Records", "Used Cars"])] };
  }
  const registrations = new Set(all.map(getRegistration).filter(Boolean));
  if (!parsed.selectedEntity && parsed.entities.last4 && registrations.size > 1) {
    return { ambiguity: makeAmbiguity(all.map((item) => entityOption(item, "Vehicle", "vehicle"))), widgets: [], followUpSuggestions: [] };
  }
  const primary = all[0];
  const reg = getRegistration(primary);
  const payments = access.canAccess("payments") && access.canViewFinance
    ? await Payment.find({ loanId: { $in: loans.map((loan) => loan.loanId).filter(Boolean) } }).limit(20).lean()
    : [];
  const deliveryOrders = access.canAccess("deliveryOrders")
    ? await DeliveryOrder.find({ loanId: { $in: loans.map((loan) => loan.loanId).filter(Boolean) } }).limit(20).lean()
    : [];
  pushModuleTrace(trace, "Payments", payments.length);
  pushModuleTrace(trace, "Delivery Orders", deliveryOrders.length);
  return {
    widgets: [
      widget("vehicle_360", `Vehicle 360${reg ? `: ${reg}` : ""}`, {
        data: {
          vehicle: { registrationNumber: reg, name: getVehicleName(primary) },
          linkedCustomers: [...new Set(all.map((item) => firstMeaningful(item.customerName, item.companyName, item?.seller?.name)).filter(Boolean))],
          loanRecords: loans.map((item) => ({ id: safeId(item), loanId: item.loanId, status: firstMeaningful(item.loanStatus, item.status), route: getLoanRoute(item) })),
          insuranceRecords: insurance.map((item) => ({ id: safeId(item), caseId: item.caseId, status: item.status, route: getInsuranceRoute(item) })),
          paymentRecords: payments.map((item) => ({ id: safeId(item), loanId: item.loanId, route: getPaymentRoute(item) })),
          deliveryRecords: deliveryOrders,
          usedCarRecords: usedCarLeads.map((item) => ({ id: safeId(item), status: firstMeaningful(item?.workflow?.status, item.status), route: getUsedCarRoute(item) })),
          dataMismatches: registrations.size > 1 ? [`Multiple registrations detected: ${[...registrations].join(", ")}`] : [],
        },
      }),
    ],
    followUpSuggestions: ["Latest insurance", "Check loan status", "Show customer 360", "Approx loan closure"],
  };
};
