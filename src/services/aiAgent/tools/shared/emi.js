/**
 * EMI helpers for ACI Assist V2 tools.
 */

export const DEFAULT_EMI = {
  roi: 9.5,
  tenureMonths: 60,
  loanPercent: 80,
};

export const calculateEmi = ({
  price = 0,
  downPayment,
  loanAmount,
  loanPercent,
  tenureMonths,
  roi,
} = {}) => {
  const effectiveLoanPercent = Number(loanPercent || DEFAULT_EMI.loanPercent);

  const principal =
    loanAmount ||
    Math.max(
      0,
      price -
        (downPayment !== undefined
          ? Number(downPayment)
          : price * (1 - effectiveLoanPercent / 100)),
    );

  const months = Number(tenureMonths || DEFAULT_EMI.tenureMonths);
  const annualRoi = Number(roi || DEFAULT_EMI.roi);
  const monthlyRate = annualRoi / 12 / 100;

  if (!principal || !months || !monthlyRate) {
    return {
      price,
      principal,
      tenureMonths: months,
      roi: annualRoi,
      emi: 0,
      totalPayable: 0,
      totalInterest: 0,
    };
  }

  const emi =
    (principal * monthlyRate * Math.pow(1 + monthlyRate, months)) /
    (Math.pow(1 + monthlyRate, months) - 1);

  const roundedEmi = Math.round(emi);
  const totalPayable = roundedEmi * months;

  return {
    price,
    principal: Math.round(principal),
    tenureMonths: months,
    roi: annualRoi,
    emi: roundedEmi,
    totalPayable,
    totalInterest: Math.max(0, totalPayable - Math.round(principal)),
  };
};
