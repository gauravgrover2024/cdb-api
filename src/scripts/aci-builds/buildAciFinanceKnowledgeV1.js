import "dotenv/config";
import mongoose from "mongoose";

const COLLECTION_NAME =
  process.env.ACI_FINANCE_KNOWLEDGE_COLLECTION || "aci_finance_knowledge_v1";
const WRITE = process.argv.includes("--write");
const VERSION = "aci_finance_knowledge_v1.0.0";
const REVIEWED_AT = "2026-07-13";

const RBI_KYC = {
  label: "RBI Master Direction - Know Your Customer",
  url: "https://www.rbi.org.in/Scripts/BS_ViewMasDirections.aspx?id=11566",
  publisher: "Reserve Bank of India",
};
const ICICI_DOCUMENTS = {
  label: "Car loan documentation",
  url: "https://www.icicibank.com/personal-banking/loans/car-loan/documentation",
  publisher: "ICICI Bank",
};
const ICICI_CAR_LOAN = {
  label: "Car loan eligibility and application",
  url: "https://www.icicibank.com/personal-banking/loans/car-loan",
  publisher: "ICICI Bank",
};
const SBI_CAR_LOAN = {
  label: "SBI New Car Loan Scheme",
  url: "https://sbi.co.in/web/personal-banking/loans/auto-loans/sbi-new-car-loan-scheme",
  publisher: "State Bank of India",
};
const CIBIL_GUIDE = {
  label: "CIBIL score and report guide",
  url: "https://www.cibil.com/content/dam/cibil/consumer/cibil-score-and-report-brochure-15-10-25.pdf",
  publisher: "TransUnion CIBIL",
};

const records = [
  {
    key: "overview_general",
    topic: "overview",
    applicantType: "general",
    priority: 10,
    title: "How car finance usually works",
    summary:
      "A lender normally checks your identity, income, existing monthly obligations, repayment history and the car quotation before deciding the loan amount, rate and tenure. Approval is always the lender’s decision, but we can get your paperwork and EMI expectations in order before you apply.",
    checklist: [
      "Identity and current address proof",
      "Recent income proof",
      "Recent bank statements",
      "Car quotation or pro-forma invoice",
      "A realistic down-payment and EMI range",
    ],
    caveats: [
      "Approval, rate, tenure and funding percentage vary by lender and applicant profile.",
      "Do not treat an indicative EMI or eligibility check as a loan sanction.",
    ],
    sourceLinks: [RBI_KYC, ICICI_CAR_LOAN, SBI_CAR_LOAN],
  },
  {
    key: "documents_general",
    topic: "documents",
    applicantType: "general",
    priority: 20,
    title: "Documents to keep ready",
    summary:
      "Start with KYC, photographs, income proof, recent bank statements and the dealer quotation. The exact list changes with the lender and applicant type, so this is a preparation checklist rather than a promise that no other document will be asked for.",
    checklist: [
      "PAN and lender-accepted identity proof",
      "Current address proof",
      "Recent passport-size photographs",
      "Recent bank statements",
      "Income proof for your applicant type",
      "Dealer quotation or pro-forma invoice",
    ],
    caveats: [
      "The lender may ask for additional KYC, signature, age or residence proof.",
      "Use only current, valid documents and make sure names and addresses are consistent.",
    ],
    sourceLinks: [RBI_KYC, ICICI_DOCUMENTS, SBI_CAR_LOAN],
  },
  {
    key: "documents_salaried",
    topic: "documents",
    applicantType: "salaried",
    priority: 21,
    title: "Salaried applicant checklist",
    summary:
      "For a salaried application, lenders commonly add recent salary slips and Form 16 or another accepted income record to the standard KYC and bank-statement set. Employment continuity can also form part of the assessment.",
    checklist: [
      "Recent salary slips",
      "Form 16 or lender-accepted income record",
      "Recent salary-account bank statements",
      "Employment or office details if requested",
    ],
    caveats: ["The period and format of income records vary by lender."],
    sourceLinks: [ICICI_DOCUMENTS, SBI_CAR_LOAN],
  },
  {
    key: "documents_self_employed",
    topic: "documents",
    applicantType: "self_employed",
    priority: 22,
    title: "Self-employed applicant checklist",
    summary:
      "For a self-employed application, lenders commonly look for recent income-tax returns, financial statements and proof that the business has been operating consistently, along with KYC and bank statements.",
    checklist: [
      "Recent income-tax returns",
      "Audited or lender-accepted financial statements",
      "Business registration, constitution or continuity proof",
      "Recent personal and business bank statements if requested",
    ],
    caveats: ["Document requirements differ for proprietorships, partnerships and companies."],
    sourceLinks: [ICICI_DOCUMENTS, SBI_CAR_LOAN],
  },
  {
    key: "eligibility_general",
    topic: "eligibility",
    applicantType: "general",
    priority: 30,
    title: "What shapes loan eligibility",
    summary:
      "Eligibility is not decided by one number. Lenders usually consider income, existing EMIs and obligations, work or business stability, credit history, age, requested tenure and their own policy. A stronger overall profile may improve the available amount or terms, but only the lender can approve the loan.",
    checklist: [
      "Monthly take-home or assessable income",
      "Existing EMIs and recurring debt obligations",
      "Employment or business continuity",
      "Credit history and recent repayment behaviour",
      "Requested loan amount, tenure and down payment",
    ],
    caveats: [
      "There is no universal approval threshold across all lenders.",
      "A pre-qualified or indicative result is not a sanction letter.",
    ],
    sourceLinks: [ICICI_CAR_LOAN, SBI_CAR_LOAN, CIBIL_GUIDE],
  },
  {
    key: "credit_score_general",
    topic: "credit_score",
    applicantType: "general",
    priority: 40,
    title: "How your credit score fits in",
    summary:
      "Your CIBIL score is an important signal, but it does not approve or reject the application on its own. The lender reads it with your repayment history, income, current obligations and its internal policy, so even a specific score should be treated as context rather than a guarantee.",
    checklist: [
      "Check your credit report for errors before applying",
      "Avoid missing current EMI or card payments",
      "Keep recent credit applications measured",
      "Compare the EMI with your existing obligations",
    ],
    caveats: ["ACI Assist cannot guarantee approval from a CIBIL score."],
    sourceLinks: [CIBIL_GUIDE, ICICI_CAR_LOAN],
  },
  {
    key: "down_payment_general",
    topic: "down_payment",
    applicantType: "general",
    priority: 50,
    title: "Down payment and funding",
    summary:
      "The financed share can differ by lender, car, price basis and applicant profile. A larger down payment usually reduces the borrowed amount and EMI, but it should not leave you without a sensible cash buffer for insurance, registration and ownership expenses.",
    checklist: [
      "Confirm whether funding is based on ex-showroom or on-road price",
      "Keep registration, insurance and optional charges in view",
      "Compare a comfortable EMI with the cash you want to retain",
    ],
    caveats: ["Do not assume that a quoted maximum funding percentage will be approved."],
    sourceLinks: [ICICI_CAR_LOAN, SBI_CAR_LOAN],
  },
  {
    key: "interest_tenure_general",
    topic: "interest_tenure",
    applicantType: "general",
    priority: 60,
    title: "Rate and tenure trade-off",
    summary:
      "A longer tenure can make the monthly EMI easier, but it generally keeps the loan running longer and can increase total interest paid. Compare the effective rate, total repayment and prepayment terms, not only the smallest EMI shown.",
    checklist: [
      "Interest rate and whether it is fixed or floating",
      "Monthly EMI and total repayment",
      "Loan tenure",
      "Processing and documentation charges",
      "Part-payment and foreclosure rules",
    ],
    caveats: ["Rates and charges change; confirm the lender’s current written schedule before applying."],
    sourceLinks: [ICICI_CAR_LOAN, SBI_CAR_LOAN],
  },
  {
    key: "fees_preclosure_general",
    topic: "fees_preclosure",
    applicantType: "general",
    priority: 70,
    title: "Fees and early repayment",
    summary:
      "Before choosing a loan, ask for the complete charge sheet: processing, documentation, late-payment, part-payment and foreclosure terms. These can change the real cost even when two headline rates look similar.",
    checklist: [
      "Processing and documentation fees",
      "Late-payment or bounce charges",
      "Part-payment eligibility and fee",
      "Foreclosure timing and fee",
    ],
    caveats: ["Use the lender’s current sanction letter and schedule of charges as the final reference."],
    sourceLinks: [ICICI_CAR_LOAN, SBI_CAR_LOAN],
  },
  {
    key: "application_process_general",
    topic: "application_process",
    applicantType: "general",
    priority: 80,
    title: "A clean application flow",
    summary:
      "Shortlist the car and variant, decide a comfortable down payment, collect KYC and income documents, compare written lender terms, then submit the application. Read the sanction letter before accepting and match the disbursal details to the dealer quotation.",
    checklist: [
      "Finalize car, variant and city quotation",
      "Set a comfortable down payment and tenure",
      "Prepare KYC, income and bank documents",
      "Compare written rate, fees and repayment terms",
      "Review the sanction letter before disbursal",
    ],
    caveats: ["Never share OTPs or banking credentials with an unverified person."],
    sourceLinks: [RBI_KYC, ICICI_CAR_LOAN, SBI_CAR_LOAN],
  },
].map((record) => ({
  ...record,
  status: "published",
  active: true,
  version: VERSION,
  reviewedAt: REVIEWED_AT,
  updatedAt: new Date(),
}));

const main = async () => {
  if (!process.env.MONGO_URI) throw new Error("MONGO_URI is not configured");
  await mongoose.connect(process.env.MONGO_URI, { serverSelectionTimeoutMS: 60_000 });
  const collection = mongoose.connection.db.collection(COLLECTION_NAME);

  if (WRITE) {
    await collection.bulkWrite(
      records.map((record) => ({
        updateOne: {
          filter: { key: record.key },
          update: { $set: record, $setOnInsert: { createdAt: new Date() } },
          upsert: true,
        },
      })),
      { ordered: false },
    );
    await Promise.all([
      collection.createIndex({ key: 1 }, { unique: true }),
      collection.createIndex({ status: 1, active: 1, topic: 1, applicantType: 1, priority: 1 }),
      collection.createIndex({ reviewedAt: -1 }),
    ]);
  }

  const published = await collection.countDocuments({ status: "published", active: true });
  const coverage = await collection
    .aggregate([
      { $match: { status: "published", active: true } },
      { $group: { _id: "$topic", count: { $sum: 1 } } },
      { $sort: { _id: 1 } },
    ])
    .toArray();

  console.log(JSON.stringify({ collection: COLLECTION_NAME, write: WRITE, published, coverage }, null, 2));
  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  await mongoose.disconnect().catch(() => {});
  process.exitCode = 1;
});
