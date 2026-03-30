/**
 * Payout Service
 * Handles payout and receivable calculations exclusively after loan disbursement
 * 
 * Key Principles:
 * - Payouts are ONLY calculated after disbursement is confirmed
 * - NOT at approval stage
 * - Receiver/Payable records are created with audit trail
 */

/**
 * Calculate TDS (Tax Deducted at Source) amount
 * @param {number} payoutAmount - Base payout amount
 * @param {string} tdsApplicable - "Yes" or "No"
 * @param {number} tdsPercentage - TDS percentage
 * @returns {number} TDS amount
 */
export const calculateTdsAmount = (payoutAmount, tdsApplicable = 'Yes', tdsPercentage = 5) => {
  if (tdsApplicable !== 'Yes') return 0;
  const base = parseFloat(payoutAmount) || 0;
  const perc = parseFloat(tdsPercentage) || 0;
  return (base * perc) / 100;
};

/**
 * Calculate payout amount based on percentage
 * @param {number} baseAmount - Base amount (net loan approved)
 * @param {number} percentage - Payout percentage
 * @returns {number} Calculated payout amount
 */
export const calculatePayoutAmount = (baseAmount, percentage) => {
  const base = parseFloat(baseAmount) || 0;
  const perc = parseFloat(percentage) || 0;
  return (base * perc) / 100;
};

/**
 * Generate unique payout ID
 * @param {string} type - "receivable" or "payable"
 * @returns {string} Generated payout ID
 */
export const generatePayoutId = (type = 'receivable') => {
  const year = new Date().getFullYear();
  const random = Math.floor(Math.random() * 999999)
    .toString()
    .padStart(6, '0');
  const prefix = type === 'receivable' ? 'PR' : 'PP';
  return `${prefix}-${year}-${random}`;
};

/**
 * Generate Bank Receivables (money we will receive from bank)
 * Called when loan is disbursed by a bank with a payout percentage
 * 
 * @param {object} params
 * @param {string} params.disbursedBankName - Name of bank that disbursed
 * @param {number} params.netLoanApprovedAmount - Net loan amount approved
 * @param {number} params.payoutPercentage - Payout % for bank
 * @returns {array} Receivable records
 */
export const generateBankReceivables = ({
  disbursedBankName,
  netLoanApprovedAmount,
  payoutPercentage,
  disbursementDate = null,
  disbursementRemarks = '',
}) => {
  if (!disbursedBankName || !payoutPercentage) {
    return [];
  }

  const payoutAmount = calculatePayoutAmount(netLoanApprovedAmount, payoutPercentage);
  const tdsPercentage = 5; // Default TDS percentage
  const tdsAmount = calculateTdsAmount(payoutAmount, 'Yes', tdsPercentage);
  const netPayoutAmount = payoutAmount - tdsAmount;

  const receivable = {
    id: Date.now().toString(),
    payoutId: generatePayoutId('receivable'),
    payout_createdAt: new Date().toISOString(),

    // Party details
    payout_type: 'Bank',
    payout_party_name: disbursedBankName,
    payout_partyId: null, // Can be extended for party master

    // Payout calculation
    payout_applicable: 'Yes',
    payout_percentage: parseFloat(payoutPercentage),
    payout_baseAmount: parseFloat(netLoanApprovedAmount),
    payout_amount: parseFloat(payoutAmount.toFixed(2)),

    // TDS calculation
    tds_applicable: 'Yes',
    tds_percentage: tdsPercentage,
    tds_amount: parseFloat(tdsAmount.toFixed(2)),
    net_payout_amount: parseFloat(netPayoutAmount.toFixed(2)),

    // Status tracking
    payout_status: 'Expected', // Expected, Received, Hold
    payout_expected_date: new Date(new Date().getTime() + 7*24*60*60*1000).toISOString(), // Default: 7 days from now
    payout_received_date: null,

    // Disbursement details ✅ 
    payout_disbursement_date: disbursementDate ? new Date(disbursementDate).toISOString() : new Date().toISOString(),
    payout_remarks: disbursementRemarks || `Auto-generated from bank disbursement (${disbursedBankName})`,
    payout_direction: 'Receivable',
    payout_currency: 'INR',

    // Audit trail
    createdAt: new Date().toISOString(),
    createdBy: null, // Will be set by controller
    modifiedAt: null,
    modifiedBy: null,
  };

  console.log(`📦 Bank receivable created for ${disbursedBankName}: ₹${receivable.payout_amount.toFixed(2)}`);

  return [receivable];
};

/**
 * Generate Dealer Payables (money we need to pay to dealer/channel)
 * Called when loan is disbursed and pre-file payout is configured
 * 
 * @param {object} params
 * @param {string} params.recordSource - "Direct" or "Indirect"
 * @param {string} params.sourceName - Dealer/Channel name
 * @param {string} params.dealerAddress - Dealer address
 * @param {string} params.dealerMobile - Dealer contact
 * @param {number} params.netLoanApprovedAmount - Net loan amount
 * @param {number} params.sourcePayoutPercentage - Payout % for dealer
 * @returns {array} Payable records
 */
export const generateDealerPayables = ({
  recordSource,
  sourceName,
  dealerAddress,
  dealerMobile,
  netLoanApprovedAmount,
  sourcePayoutPercentage,
}) => {
  // Only generate if source is Indirect and payout is configured
  if (recordSource !== 'Indirect' || !sourcePayoutPercentage || !sourceName) {
    return [];
  }

  const payoutAmount = calculatePayoutAmount(netLoanApprovedAmount, sourcePayoutPercentage);
  const tdsPercentage = 0; // Dealers typically don't have TDS (verify per business rules)
  const tdsAmount = calculateTdsAmount(payoutAmount, 'No', tdsPercentage);
  const netPayoutAmount = payoutAmount - tdsAmount;

  return [
    {
      id: Date.now().toString(),
      payoutId: generatePayoutId('payable'),
      payout_createdAt: new Date().toISOString(),

      // Party details
      payout_type: 'Dealer',
      payout_party_name: sourceName,
      payout_partyAddress: dealerAddress || null,
      payout_partyMobile: dealerMobile || null,
      payout_partyId: null, // Can be extended for party master

      // Payout calculation
      payout_applicable: 'Yes',
      payout_percentage: parseFloat(sourcePayoutPercentage),
      payout_baseAmount: parseFloat(netLoanApprovedAmount),
      payout_amount: parseFloat(payoutAmount.toFixed(2)),

      // TDS calculation (usually not applicable for dealers)
      tds_applicable: 'No',
      tds_percentage: 0,
      tds_amount: 0,
      net_payout_amount: parseFloat(netPayoutAmount.toFixed(2)),

      // Status tracking
      payout_status: 'Expected', // Expected, Received, Hold
      payout_expected_date: new Date(new Date().getTime() + 7*24*60*60*1000).toISOString(), // Default: 7 days from now
      payout_received_date: null,

      // Additional info
      payout_remarks: `Auto-generated for indirect channel (${sourceName})`,
      payout_direction: 'Payable',
      payout_currency: 'INR',

      // Audit trail
      createdAt: new Date().toISOString(),
      createdBy: null, // Will be set by controller
      modifiedAt: null,
      modifiedBy: null,
    },
  ];
};

/**
 * Main function to calculate all payouts on disbursement
 * This is the entry point called from the disbursement endpoint
 * 
 * @param {object} loan - Loan document from database
 * @param {object} disbursementData - Data submitted for disbursement
 * @param {string} disbursementData.disburseAmount - Amount being disbursed
 * @param {string} disbursementData.disbursedBankName - Bank disbursing
 * @param {number} disbursementData.payoutPercentage - Bank payout %
 * @param {string} disbursementData.disbursedDate - Disbursement date
 * @returns {object} { receivables, payables }
 */
export const calculatePayoutsOnDisbursement = async (loan, disbursementData) => {
  const {
    disburseAmount,
    disbursedBankName,
    payoutPercentage,
    disbursedDate,
    remarks,
  } = disbursementData;

  // ==========================================
  // 1. VALIDATION
  // ==========================================
  
  // Approval must exist before disbursement
  const hasApprovedBankInData = Array.isArray(loan.approval_banksData) &&
    loan.approval_banksData.some((b) => {
      const s = String(b?.status || "").toLowerCase();
      return s === "approved" || s === "disbursed";
    });
  if (loan.approval_status !== 'Approved' && !hasApprovedBankInData) {
    throw new Error(
      `Loan must be in "Approved" status before disbursement. Current status: ${loan.approval_status}`
    );
  }

  // Amount validation
  if (!disburseAmount || parseFloat(disburseAmount) <= 0) {
    throw new Error('Disbursement amount must be greater than 0');
  }

  // Bank name required
  if (!disbursedBankName || disbursedBankName.trim() === '') {
    throw new Error('Disbursed bank name is required');
  }

  // Payout percentage should be provided (can be 0 for no payout)
  if (payoutPercentage === null || payoutPercentage === undefined) {
    throw new Error('Payout percentage is required (can be 0 for no payout)');
  }

  // ==========================================
  // 2. CALCULATE RECEIVABLES (Bank payouts)
  // ==========================================
  const receivables = generateBankReceivables({
    disbursedBankName,
    netLoanApprovedAmount: loan.approval_loanAmountApproved || loan.approval_breakup_netLoanApproved || disburseAmount,
    payoutPercentage,
    disbursementDate: disbursedDate,
    disbursementRemarks: remarks,
  });

  // ==========================================
  // 3. CALCULATE PAYABLES (Dealer payouts)
  // ==========================================
  const payables = generateDealerPayables({
    recordSource: loan.recordSource || loan.source,
    sourceName: loan.sourceName,
    dealerAddress: loan.dealerAddress,
    dealerMobile: loan.dealerMobile,
    netLoanApprovedAmount: loan.approval_loanAmountApproved || loan.approval_breakup_netLoanApproved || disburseAmount,
    sourcePayoutPercentage: loan.prefile_sourcePayoutPercentage,
  });

  console.log(`✅ Payouts calculated: ${receivables.length} receivables, ${payables.length} payables`);

  return {
    receivables,
    payables,
    summary: {
      totalReceivable: receivables.reduce((sum, r) => sum + (r.payout_amount || 0), 0),
      totalPayable: payables.reduce((sum, p) => sum + (p.payout_amount || 0), 0),
      netPayout: (receivables.reduce((sum, r) => sum + (r.net_payout_amount || 0), 0)) -
                 (payables.reduce((sum, p) => sum + (p.net_payout_amount || 0), 0)),
    },
  };
};

/**
 * Validate disbursement data
 * Returns validation result with any errors
 * 
 * @param {object} disbursementData
 * @returns {object} { isValid, errors }
 */
export const validateDisbursementData = (disbursementData) => {
  const errors = [];

  if (!disbursementData.disburseAmount || parseFloat(disbursementData.disburseAmount) <= 0) {
    errors.push('Disbursement amount must be greater than 0');
  }

  if (!disbursementData.disbursedBankName || disbursementData.disbursedBankName.trim() === '') {
    errors.push('Disbursed bank name is required');
  }

  if (disbursementData.payoutPercentage === null || disbursementData.payoutPercentage === undefined) {
    errors.push('Payout percentage is required');
  }

  if (disbursementData.payoutPercentage < 0 || disbursementData.payoutPercentage > 100) {
    errors.push('Payout percentage must be between 0 and 100');
  }

  return {
    isValid: errors.length === 0,
    errors,
  };
};
