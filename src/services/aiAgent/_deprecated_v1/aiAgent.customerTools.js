import Customer from "../../models/Customer.js";
import Loan from "../../models/Loan.js";
import InsuranceCase from "../../models/InsuranceCase.js";
import Payment from "../../models/Payment.js";
import VehicleRecord from "../../models/VehicleRecord.js";
import UsedCarLead from "../../models/UsedCarLead.js";
import DeliveryOrder from "../../models/DeliveryOrder.js";
import { action, unavailableWidget, widget } from "./aiAgent.renderPayloads.js";
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
  findLean,
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
  const clauses = buildTextClauses(
    ["customerName", "name", "primaryMobile", "mobile"],
    name,
  );
  return clauses.length ? { $or: clauses } : {};
};

const maskValue = (value, visible = 4) => {
  const text = normalizeText(value);
  if (!text) return "";
  return `${"*".repeat(Math.max(0, text.length - visible))}${text.slice(-visible)}`;
};

const customerDetails = (customer) => ({
  id: safeId(customer),
  customerId: customer.customerId,
  name: firstMeaningful(customer.customerName, customer.name),
  mobile: firstMeaningful(customer.primaryMobile, customer.mobile),
  email: customer.email,
  city: customer.city,
  status: customer.status,
  applicantType: customer.applicantType,
  customerType: customer.customerType,
  kycStatus: customer.kycStatus,
  panNumber: maskValue(customer.panNumber),
  aadhaarNumber: maskValue(
    firstMeaningful(customer.aadhaarNumber, customer.aadharNumber),
  ),
  typeOfLoan: customer.typeOfLoan,
});

const customerLookupQuery = (entities = {}) => {
  if (entities.customerId) return { customerId: entities.customerId };
  if (entities.mobile)
    return {
      $or: [
        { primaryMobile: entities.mobile },
        { extraMobiles: entities.mobile },
        { mobile: entities.mobile },
      ],
    };
  return customerQuery(entities.customerName);
};

const customerCardRow = (customer) => ({
  id: safeId(customer),
  customerId: customer.customerId,
  customerName: firstMeaningful(customer.customerName, customer.name),
  primaryMobile: firstMeaningful(customer.primaryMobile, customer.mobile),
  email: firstMeaningful(customer.email, customer.emailAddress),
  city: customer.city,
  applicantType: customer.applicantType,
  customerType: customer.customerType,
  kycStatus: customer.kycStatus,
  typeOfLoan: customer.typeOfLoan,
  route: getCustomerRoute(customer),
});

export const customerLookup = async (parsed, access, trace) => {
  if (!access.canAccess("customers")) {
    noteRestriction(access, "Customers", "No customer access");
    return {
      widgets: [
        unavailableWidget(
          "Customer data unavailable",
          "You do not have customer access.",
          ["Customers"],
        ),
      ],
    };
  }
  if (
    parsed.selectedEntity?.entityType === "customer" &&
    parsed.selectedEntity?.id
  ) {
    const customer = await Customer.findById(parsed.selectedEntity.id)
      .maxTimeMS(2500)
      .lean();

    pushModuleTrace(trace, "Customers", customer ? 1 : 0, {
      selectedEntity: true,
    });

    if (customer) {
      return {
        widgets: [
          widget(
            "customer_card",
            `Customer: ${firstMeaningful(customer.customerName, customer.name)}`,
            {
              data: customerDetails(customer),
              rows: [customerCardRow(customer)],
              actions: [
                action("open_record", "Open customer", {
                  route: getCustomerRoute(customer),
                }),
              ],
            },
          ),
        ],
        followUpSuggestions: [
          "Customer 360",
          "Loan status",
          "Latest insurance",
        ],
      };
    }
  }
  if (
    !parsed.entities.customerName &&
    !parsed.entities.customerId &&
    !parsed.entities.mobile
  ) {
    return {
      widgets: [
        unavailableWidget(
          "Need customer detail",
          "Ask with a customer name, mobile number, or customer ID.",
          ["Customers"],
        ),
      ],
    };
  }
  const customers = await findLean(
    Customer,
    customerLookupQuery(parsed.entities),
    { sort: { updatedAt: -1 }, limit: LIMIT },
  );
  pushModuleTrace(trace, "Customers", customers.length);
  if (!customers.length) {
    return {
      widgets: [
        unavailableWidget(
          "No customer found",
          "No matching customer record was found.",
          ["Customers"],
        ),
      ],
    };
  }
  if (customers.length === 1) {
    return {
      widgets: [
        widget(
          "customer_card",
          `Customer: ${firstMeaningful(customers[0].customerName, customers[0].name)}`,
          {
            data: customerDetails(customers[0]),
            rows: [customerCardRow(customers[0])],
            actions: [
              action("open_record", "Open customer", {
                route: getCustomerRoute(customers[0]),
              }),
            ],
          },
        ),
      ],
      followUpSuggestions: ["Customer 360", "Loan status", "Latest insurance"],
    };
  }
  return {
    widgets: [
      widget("records_table", "Matching customers", {
        summary: { total: customers.length },
        rows: customers.map(customerCardRow),
      }),
    ],
    followUpSuggestions: [
      "Customer 360 with exact name",
      "Search by mobile number",
    ],
  };
};

const customerIssuePredicate = (lower) => {
  if (/kyc pending/.test(lower))
    return {
      label: "KYC pending",
      query: { kycStatus: /pending|incomplete|not/i },
    };
  if (/missing pan/.test(lower))
    return {
      label: "Missing PAN",
      query: buildMissingValueQueryForCustomers(["panNumber"]),
    };
  if (/missing aadhaar|missing aadhar/.test(lower))
    return {
      label: "Missing Aadhaar",
      query: buildMissingValueQueryForCustomers([
        "aadhaarNumber",
        "aadharNumber",
      ]),
    };
  if (/missing email/.test(lower))
    return {
      label: "Missing email",
      query: buildMissingValueQueryForCustomers(["email", "emailAddress"]),
    };
  if (/missing mobile/.test(lower))
    return {
      label: "Missing mobile",
      query: buildMissingValueQueryForCustomers(["primaryMobile"]),
    };
  if (/missing address/.test(lower))
    return {
      label: "Missing address",
      query: buildMissingValueQueryForCustomers([
        "residenceAddress",
        "customerAddress",
      ]),
    };
  return {
    label: "Customer data quality",
    query: {
      $or: [
        buildMissingValueQueryForCustomers(["panNumber"]),
        buildMissingValueQueryForCustomers(["primaryMobile"]),
        buildMissingValueQueryForCustomers(["email", "emailAddress"]),
      ],
    },
  };
};

const buildMissingValueQueryForCustomers = (fields) => ({
  $or: fields.flatMap((field) => [
    { [field]: { $exists: false } },
    { [field]: null },
    { [field]: "" },
    {
      [field]: {
        $regex: /^(na|n\/a|not available|not captured|pending|unknown|-|--)$/i,
      },
    },
  ]),
});

export const customerDataQualityReport = async (parsed, access, trace) => {
  if (!access.canAccess("customers")) {
    noteRestriction(access, "Customers", "No customer access");
    return {
      widgets: [
        unavailableWidget(
          "Customer data unavailable",
          "You do not have customer access.",
          ["Customers"],
        ),
      ],
    };
  }
  if (
    /duplicate mobile|duplicate pan|duplicate customers?/.test(parsed.lower)
  ) {
    const field = /pan/.test(parsed.lower) ? "panNumber" : "primaryMobile";
    const grouped = await Customer.aggregate([
      { $match: { [field]: { $nin: [null, ""] } } },
      {
        $group: {
          _id: `$${field}`,
          count: { $sum: 1 },
          customers: {
            $push: {
              id: "$_id",
              customerId: "$customerId",
              customerName: "$customerName",
              primaryMobile: "$primaryMobile",
              city: "$city",
            },
          },
        },
      },
      { $match: { count: { $gt: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 50 },
    ]).option({ maxTimeMS: 3500 });
    const rows = grouped.flatMap((group) =>
      group.customers.map((customer) => ({
        issue: `Duplicate ${field}`,
        duplicateKey: field === "panNumber" ? maskValue(group._id) : group._id,
        ...customer,
        id: String(customer.id),
      })),
    );
    pushModuleTrace(trace, "Customers", rows.length, {
      issue: `duplicate ${field}`,
    });
    return {
      widgets: [
        widget("customer_data_quality_report", "Customer duplicate report", {
          summary: {
            total: rows.length,
            groups: grouped.length,
            issueType: `Duplicate ${field}`,
          },
          rows,
        }),
      ],
      followUpSuggestions: [
        "Customers missing PAN",
        "Customers with KYC pending",
      ],
    };
  }
  const issue = customerIssuePredicate(parsed.lower);
  const rows = await findLean(Customer, issue.query, {
    sort: { updatedAt: -1 },
    limit: LIMIT,
  });
  pushModuleTrace(trace, "Customers", rows.length, { issue: issue.label });
  return {
    widgets: [
      widget("customer_data_quality_report", issue.label, {
        summary: { total: rows.length, issueType: issue.label },
        rows: rows.map((customer) => ({
          issue: issue.label,
          ...customerCardRow(customer),
        })),
      }),
    ],
    followUpSuggestions: [
      "Duplicate mobile report",
      "Customers missing PAN",
      "Customers missing email",
    ],
  };
};

const buildCustomer360Query = (parsed = {}) => {
  const selected = parsed.selectedEntity || {};

  if (selected.entityType === "customer" && selected.id) {
    return {
      query: { _id: selected.id },
      label: selected.customerName || selected.displayName || selected.id,
      source: "selected_customer",
    };
  }

  if (parsed.entities.customerId) {
    return {
      query: { customerId: parsed.entities.customerId },
      label: parsed.entities.customerId,
      source: "customer_id",
    };
  }

  if (parsed.entities.mobile) {
    return {
      query: {
        $or: [
          { primaryMobile: parsed.entities.mobile },
          { extraMobiles: parsed.entities.mobile },
          { mobile: parsed.entities.mobile },
        ],
      },
      label: parsed.entities.mobile,
      source: "mobile",
    };
  }

  const name =
    parsed.entities.customerName ||
    selected.customerName ||
    selected.displayName ||
    "";

  if (name) {
    return {
      query: customerQuery(name),
      label: name,
      source: "customer_name",
    };
  }

  return {
    query: {},
    label: "",
    source: "",
  };
};

export const customer360 = async (parsed, access, trace) => {
  if (!access.canAccess("customers")) {
    noteRestriction(access, "Customers", "No customer access");
    return {
      widgets: [
        unavailableWidget(
          "Customer data unavailable",
          "You do not have customer access.",
          ["Customers"],
        ),
      ],
    };
  }
  const lookup = buildCustomer360Query(parsed);

  if (!Object.keys(lookup.query).length) {
    return {
      widgets: [
        unavailableWidget(
          "Need a customer",
          "Share a customer name, mobile number, or customer ID to build Customer 360.",
          ["Customers"],
        ),
      ],
    };
  }

  const customers = await findLean(Customer, lookup.query, {
    sort: { updatedAt: -1 },
    limit:
      lookup.source === "selected_customer" || lookup.source === "customer_id"
        ? 1
        : 10,
  });

  pushModuleTrace(trace, "Customers", customers.length, {
    lookupSource: lookup.source,
  });

  if (!customers.length) {
    return {
      widgets: [
        unavailableWidget(
          "No customer found",
          `No customer matched ${lookup.label}.`,
          ["Customers"],
        ),
      ],
      followUpSuggestions: [
        "Find customer by mobile",
        "Search customer by exact name",
        "Try customer ID",
      ],
    };
  }
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
          context: {
            customerName: firstMeaningful(item.customerName, item.name),
            customerId: item.customerId,
            mobile: firstMeaningful(item.primaryMobile, item.mobile),
          },
        })),
      ),
      widgets: [],
      followUpSuggestions: [],
    };
  }
  const selectedId =
    parsed.selectedEntity?.entityType === "customer"
      ? String(parsed.selectedEntity?.id || "")
      : "";

  const customer = selectedId
    ? customers.find((item) => safeId(item) === selectedId) || customers[0]
    : customers[0];

  const customerName = firstMeaningful(
    customer.customerName,
    customer.name,
    parsed.entities.customerName,
    parsed.selectedEntity?.customerName,
    parsed.selectedEntity?.displayName,
    lookup.label,
  );
  const nameRegex = makeRegex(customerName);
  const [loans, insurance, payments, vehicles, usedCarLeads] =
    await Promise.all([
      access.canAccess("loans")
        ? findLean(
            Loan,
            { customerName: nameRegex },
            { sort: { updatedAt: -1 }, limit: LIMIT },
          )
        : [],
      access.canAccess("insurance")
        ? findLean(
            InsuranceCase,
            { customerName: nameRegex },
            { sort: { updatedAt: -1 }, limit: LIMIT },
          )
        : [],
      access.canAccess("payments") && access.canViewFinance
        ? findLean(
            Payment,
            { customerName: nameRegex },
            { sort: { updatedAt: -1 }, limit: 20 },
          )
        : [],
      access.canAccess("vehicles")
        ? findLean(
            VehicleRecord,
            { customerName: nameRegex },
            { sort: { updatedAt: -1 }, limit: LIMIT },
          )
        : [],
      access.canAccess("usedCars")
        ? findLean(
            UsedCarLead,
            { "seller.name": nameRegex },
            { sort: { updatedAt: -1 }, limit: 20 },
          )
        : [],
    ]);
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
              vehicle: [item.make, item.model, item.variant]
                .filter(Boolean)
                .join(" "),
              registrationNumber: item.registrationNumber,
            })),
            ...loans.map((item) => ({
              vehicle: getVehicleName(item),
              registrationNumber: getRegistration(item),
            })),
            ...insurance.map((item) => ({
              vehicle: getVehicleName(item),
              registrationNumber: getRegistration(item),
            })),
          ],
          linkedLoans: loans.map((item) => ({
            id: safeId(item),
            loanId: item.loanId,
            vehicle: getVehicleName(item),
            status: firstMeaningful(
              item.loanStatus,
              item.status,
              item.currentStage,
            ),
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
            vehicle: [item?.vehicle?.make, item?.vehicle?.model]
              .filter(Boolean)
              .join(" "),
            status: firstMeaningful(item?.workflow?.status, item.status),
            route: getUsedCarRoute(item),
          })),
          latestActivity: [
            customer.updatedAt,
            ...loans.map((item) => item.updatedAt),
            ...insurance.map((item) => item.updatedAt),
          ]
            .filter(Boolean)
            .sort()
            .at(-1),
        },
        actions: [
          customer._id &&
            action("open_record", "Open customer", {
              route: getCustomerRoute(customer),
            }),
          customer._id &&
            access.canEdit &&
            action("edit_record", "Edit customer", {
              route: getCustomerRoute(customer),
            }),
        ].filter(Boolean),
      }),
    ],
    followUpSuggestions: [
      "Show vehicle 360",
      "Latest insurance",
      "Check loan status",
      "Approx loan closure",
    ],
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
        ...(registrationConditions(
          ["registrationNumber"],
          entities.registrationNumber,
          entities.last4,
        ).length
          ? [
              {
                $or: registrationConditions(
                  ["registrationNumber"],
                  entities.registrationNumber,
                  entities.last4,
                ),
              },
            ]
          : []),
        ...(entities.model
          ? [{ model: { $regex: entities.model, $options: "i" } }]
          : []),
      ],
    },
    usedCar: {
      $or: registrationConditions(
        ["vehicle.regNo"],
        entities.registrationNumber,
        entities.last4,
      ),
    },
  };
};

const chooseSelectedVehicleRecord = (records = [], parsed = {}) => {
  const selectedId = String(parsed.selectedEntity?.id || "");
  if (selectedId) {
    const exact = records.find((record) => safeId(record) === selectedId);
    if (exact) return exact;
  }

  const selectedRegistration =
    parsed.selectedEntity?.registrationNumber ||
    parsed.selectedEntity?.context?.registrationNumber;

  if (selectedRegistration) {
    const normalized = String(selectedRegistration)
      .replace(/[^A-Z0-9]/gi, "")
      .toUpperCase();

    const exact = records.find(
      (record) =>
        String(getRegistration(record))
          .replace(/[^A-Z0-9]/gi, "")
          .toUpperCase() === normalized,
    );

    if (exact) return exact;
  }

  return records[0];
};

export const vehicle360 = async (parsed, access, trace) => {
  const queries = vehicleQueries(parsed);
  const [loans, insurance, vehicles, usedCarLeads] = await Promise.all([
    access.canAccess("loans") && Object.keys(queries.loan).length
      ? findLean(Loan, queries.loan, { sort: { updatedAt: -1 }, limit: LIMIT })
      : [],
    access.canAccess("insurance") && Object.keys(queries.insurance).length
      ? findLean(InsuranceCase, queries.insurance, {
          sort: { updatedAt: -1 },
          limit: LIMIT,
        })
      : [],
    access.canAccess("vehicles") && queries.vehicleRecord.$and.length
      ? findLean(VehicleRecord, queries.vehicleRecord, {
          sort: { updatedAt: -1 },
          limit: LIMIT,
        })
      : [],
    access.canAccess("usedCars") && queries.usedCar.$or.length
      ? findLean(UsedCarLead, queries.usedCar, {
          sort: { updatedAt: -1 },
          limit: 20,
        })
      : [],
  ]);
  pushModuleTrace(trace, "Loans", loans.length);
  pushModuleTrace(trace, "Insurance", insurance.length);
  pushModuleTrace(trace, "Vehicle Records", vehicles.length);
  pushModuleTrace(trace, "Used Cars", usedCarLeads.length);

  const all = [...loans, ...insurance, ...vehicles, ...usedCarLeads];
  if (!all.length) {
    return {
      widgets: [
        unavailableWidget(
          "No vehicle found",
          "No matching vehicle history was found.",
          ["Loans", "Insurance", "Vehicle Records", "Used Cars"],
        ),
      ],
    };
  }
  const registrations = new Set(all.map(getRegistration).filter(Boolean));
  if (
    !parsed.selectedEntity &&
    parsed.entities.last4 &&
    registrations.size > 1
  ) {
    return {
      ambiguity: makeAmbiguity(
        all.map((item) => entityOption(item, "Vehicle", "vehicle")),
      ),
      widgets: [],
      followUpSuggestions: [],
    };
  }
  const primary = chooseSelectedVehicleRecord(all, parsed);
  const reg = getRegistration(primary);
  const loanIds = loans.map((loan) => loan.loanId).filter(Boolean);
  const [payments, deliveryOrders] = await Promise.all([
    access.canAccess("payments") && access.canViewFinance && loanIds.length
      ? findLean(Payment, { loanId: { $in: loanIds } }, { limit: 20 })
      : [],
    access.canAccess("deliveryOrders") && loanIds.length
      ? findLean(DeliveryOrder, { loanId: { $in: loanIds } }, { limit: 20 })
      : [],
  ]);
  pushModuleTrace(trace, "Payments", payments.length);
  pushModuleTrace(trace, "Delivery Orders", deliveryOrders.length);
  return {
    widgets: [
      widget("vehicle_360", `Vehicle 360${reg ? `: ${reg}` : ""}`, {
        data: {
          vehicle: { registrationNumber: reg, name: getVehicleName(primary) },
          linkedCustomers: [
            ...new Set(
              all
                .map((item) =>
                  firstMeaningful(
                    item.customerName,
                    item.companyName,
                    item?.seller?.name,
                  ),
                )
                .filter(Boolean),
            ),
          ],
          loanRecords: loans.map((item) => ({
            id: safeId(item),
            loanId: item.loanId,
            status: firstMeaningful(item.loanStatus, item.status),
            route: getLoanRoute(item),
          })),
          insuranceRecords: insurance.map((item) => ({
            id: safeId(item),
            caseId: item.caseId,
            status: item.status,
            route: getInsuranceRoute(item),
          })),
          paymentRecords: payments.map((item) => ({
            id: safeId(item),
            loanId: item.loanId,
            route: getPaymentRoute(item),
          })),
          deliveryRecords: deliveryOrders,
          usedCarRecords: usedCarLeads.map((item) => ({
            id: safeId(item),
            status: firstMeaningful(item?.workflow?.status, item.status),
            route: getUsedCarRoute(item),
          })),
          dataMismatches:
            registrations.size > 1
              ? [
                  `Multiple registrations detected: ${[...registrations].join(", ")}`,
                ]
              : [],
        },
      }),
    ],
    followUpSuggestions: [
      "Latest insurance",
      "Check loan status",
      "Show customer 360",
      "Approx loan closure",
    ],
  };
};
