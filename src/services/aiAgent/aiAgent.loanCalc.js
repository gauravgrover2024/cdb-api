import { firstNumber, parseDate, toNumber } from "./aiAgent.normalizers.js";

const toLocalDate = (value) => {
  const date = parseDate(value);
  if (!date) return null;
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
};

const getDaysInMonth = (year, monthIndex) => new Date(year, monthIndex + 1, 0).getDate();

export const calculateEMI = (principal, annualRate, tenureMonths, type = "Reducing") => {
  const P = toNumber(principal);
  const N = toNumber(tenureMonths);
  if (!P || !N) return 0;
  if (type === "Flat") {
    const yearlyRate = toNumber(annualRate) / 100;
    const totalInterest = P * yearlyRate * (N / 12);
    return Math.round((P + totalInterest) / N);
  }
  const R = toNumber(annualRate) / 12 / 100;
  if (!R) return Math.round(P / N);
  const pow = Math.pow(1 + R, N);
  return Math.round((P * R * pow) / (pow - 1));
};

export const generateRepaymentSchedule = (principal, annualRate, tenureMonths, firstEmiDate = null) => {
  const P = toNumber(principal);
  const N = toNumber(tenureMonths);
  if (!P || !N) return [];
  const emi = calculateEMI(P, annualRate, N);
  const monthlyRate = toNumber(annualRate) / 12 / 100;
  let currentDate = firstEmiDate ? new Date(firstEmiDate) : new Date();
  let balance = P;
  return Array.from({ length: N }, (_, index) => {
    const interestPayment = Math.round(balance * monthlyRate);
    const principalPayment = emi - interestPayment;
    balance = Math.max(0, balance - principalPayment);
    const row = {
      month: index + 1,
      date: new Date(currentDate),
      emi,
      principalPayment: Math.round(principalPayment),
      interestPayment,
      outstandingBalance: Math.round(balance),
    };
    currentDate = new Date(currentDate.setMonth(currentDate.getMonth() + 1));
    return row;
  });
};

export const calculateLivePrincipalOutstanding = (
  principal,
  annualRate,
  tenureMonths,
  firstEmiDate,
) => {
  const P = toNumber(principal);
  const N = toNumber(tenureMonths);
  if (!firstEmiDate || !P || !N) {
    return { outstanding: P, monthsElapsed: 0, monthsRemaining: N, progressPercentage: 0 };
  }
  const today = toLocalDate(new Date());
  const emiDate = toLocalDate(firstEmiDate);
  if (!today || !emiDate) {
    return {
      outstanding: P,
      monthsElapsed: 0,
      monthsRemaining: N,
      progressPercentage: 0,
      emi: calculateEMI(P, annualRate, N),
      totalPaid: 0,
    };
  }
  let monthsElapsed = 0;
  if (today >= emiDate) {
    monthsElapsed =
      (today.getFullYear() - emiDate.getFullYear()) * 12 +
      (today.getMonth() - emiDate.getMonth());
    const dueDayThisMonth = Math.min(
      emiDate.getDate(),
      getDaysInMonth(today.getFullYear(), today.getMonth()),
    );
    if (today.getDate() >= dueDayThisMonth) monthsElapsed += 1;
  }
  const monthsPaid = Math.min(monthsElapsed, N);
  const schedule = generateRepaymentSchedule(P, annualRate, N, firstEmiDate);
  const lastPaidMonth = monthsPaid > 0 ? schedule[monthsPaid - 1] : null;
  const outstanding = monthsPaid >= N ? 0 : monthsPaid === 0 ? P : lastPaidMonth?.outstandingBalance ?? P;
  return {
    outstanding,
    monthsElapsed: monthsPaid,
    monthsRemaining: Math.max(0, N - monthsPaid),
    progressPercentage: Math.round((monthsPaid / N) * 100),
    emi: calculateEMI(P, annualRate, N),
    totalPaid: monthsPaid * calculateEMI(P, annualRate, N),
    schedule,
  };
};

export const getStoredPrincipalOutstanding = (loan = {}) =>
  firstNumber(
    loan.postfile_currentOutstanding,
    loan.postfile_current_outstanding,
    loan.currentOutstanding,
    loan.livePrincipalOutstanding,
    loan.live_principal_outstanding,
    loan.principalOutstanding,
    loan.principal_outstanding,
    loan.outstandingPrincipal,
    loan.outstandingBalance,
    loan.postfile_principalOutstanding,
    loan.postfile_livePrincipalOutstanding,
    loan?.postFile?.currentOutstanding,
  );
