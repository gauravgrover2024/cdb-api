
import mongoose from 'mongoose';
import dotenv from 'dotenv';

import User from './models/User.js';
import Customer from './models/Customer.js';
import Loan from './models/Loan.js';
import DeliveryOrder from './models/DeliveryOrder.js';
import Payment from './models/Payment.js';
import connectDB from './config/db.js';

dotenv.config();

connectDB();

const demoCustomers = [
  {
    id: 1,
    customerNumber: 1001,
    name: "Raj Kumar Singh",
    customerName: "Raj Kumar Singh",
    gender: "Male",
    dateOfBirth: "1985-03-15",
    dob: "1985-03-15",
    maritalStatus: "Married",
    phone: "9999900001",
    primaryMobile: "9999900001",
    email: "raj.kumar@email.com",
    address: "205, Marina Heights, Linking Road, Bandra West",
    residenceAddress: "205, Marina Heights, Linking Road, Bandra West",
    city: "Mumbai",
    state: "Maharashtra",
    pincode: "400050",
    houseType: "owned",
    yearsInCurrentHouse: 12,
    sdwOf: "Ram Kumar Singh",
    motherName: "Sunita Singh",
    education: "graduate",
    dependents: 2,
    nomineeName: "Priya Singh",
    nomineeDob: "1988-06-20",
    nomineeRelation: "Spouse",

    occupationType: "Salaried",
    companyName: "Tata Consultancy Services",
    companyType: "MNC",
    designation: "Senior Software Engineer",
    businessNature: ["IT", "Banking"],
    natureOfBusiness: "IT",
    employmentAddress: "TCS IT Park, Sector 30, Gurugram",
    officeAddress: "TCS IT Park, Sector 30, Gurugram",
    employmentCity: "Gurugram",
    officeCity: "Gurugram",
    employmentPincode: "122001",
    officePincode: "122001",
    employmentPhone: "0124-4871000",
    officePhone: "0124-4871000",
    yearsInBusiness: 12,
    incorporationYear: 2012,

    monthlyIncome: 150000,
    annualIncome: 1800000,
    grossMonthlyIncome: 150000,
    netMonthlyIncome: 120000,
    salaryMonthly: 150000,
    itrYears: 1500000,
    otherIncomeSource: "",
    otherIncomeAmount: 0,

    bankName: "HDFC Bank",
    accountNumber: "1234567890123456",
    ifscCode: "HDFC0000123",
    ifsc: "HDFC0000123",
    accountType: "Savings",
    branch: "Bandra",
    branchName: "Bandra",
    accountHolderName: "Raj Kumar Singh",
    accountSinceYears: 15,

    reference1Name: "Amit Patel",
    reference1Phone: "9999900010",
    reference1Address: "45, Maple Gardens, Bandra East",
    reference1Relationship: "Friend",
    reference1: {
      name: "Amit Patel",
      mobile: "9999900010",
      address: "45, Maple Gardens, Bandra East",
      pincode: "400051",
      city: "Mumbai",
    },

    reference2Name: "Neha Gupta",
    reference2Phone: "9999900011",
    reference2Address: "12, Park Avenue, Andheri West",
    reference2Relationship: "Colleague",
    reference2: {
      name: "Neha Gupta",
      mobile: "9999900011",
      address: "12, Park Avenue, Andheri West",
      pincode: "400053",
      city: "Mumbai",
    },

    aadhaarNumber: "123456789012",
    panNumber: "AAAPA1234A",
    passportNumber: "A12345678",
    gstNumber: "27AABCU1234B1Z5",
    dlNumber: "MH1520200001234",
    drivingLicense: "MH1520200001234",

    mobile: "9999900001",
    customerType: "New",
    kycStatus: "Completed",
    createdOn: "03 Jan 2026",

    personalInfo: {
      name: "Raj Kumar Singh",
      phone: "9999900001",
      fatherSpouseName: "Ram Kumar Singh",
      dateOfBirth: "1985-03-15",
      motherName: "Sunita Singh",
      residenceAddress: "205, Marina Heights, Linking Road, Bandra West",
      pincode: "400050",
      stayingSince: 12,
      houseType: "owned",
    },
    residentialInfo: {
      city: "Mumbai",
      pincode: "400050",
      panNumber: "AAAPA1234A",
      houseType: "owned",
      stayingSince: 12,
      residenceAddress: "205, Marina Heights, Linking Road, Bandra West",
    },
    employmentInfo: {
      employmentType: "Salaried",
      companyName: "Tata Consultancy Services",
      designation: "Senior Software Engineer",
      monthlySalary: 150000,
    },
    incomeInfo: {},
    bankInfo: {
      bankName: "HDFC Bank",
      accountType: "Savings",
      accountNumber: "1234567890123456",
      ifscCode: "HDFC0000123",
    },
    kycInfo: {
      panNumber: "AAAPA1234A",
      aadharNumber: "123456789012",
      drivingLicense: "MH1520200001234",
    },
    references: [],
  },

  {
    id: 2,
    customerNumber: 1002,
    name: "Amit Verma",
    customerName: "Amit Verma",
    gender: "Male",
    dateOfBirth: "1989-07-22",
    dob: "1989-07-22",
    maritalStatus: "Married",
    phone: "9999900002",
    primaryMobile: "9999900002",
    email: "amit.verma@email.com",
    address: "Flat 12B, Green Residency, Indirapuram",
    residenceAddress: "Flat 12B, Green Residency, Indirapuram",
    city: "Ghaziabad",
    state: "Uttar Pradesh",
    pincode: "201014",
    houseType: "owned",
    yearsInCurrentHouse: 8,
    sdwOf: "Ramesh Verma",
    motherName: "Kavita Verma",
    education: "postgraduate",
    dependents: 1,
    nomineeName: "Pooja Verma",
    nomineeDob: "1991-02-14",
    nomineeRelation: "Spouse",

    occupationType: "Salaried",
    companyName: "Infosys",
    companyType: "MNC",
    designation: "Technology Lead",
    businessNature: ["IT"],
    natureOfBusiness: "IT",
    employmentAddress: "Infosys Campus, Electronic City",
    officeAddress: "Infosys Campus, Electronic City",
    employmentCity: "Bengaluru",
    officeCity: "Bengaluru",
    employmentPincode: "560100",
    officePincode: "560100",
    employmentPhone: "080-39820000",
    officePhone: "080-39820000",
    yearsInBusiness: 10,
    incorporationYear: 2014,

    monthlyIncome: 180000,
    annualIncome: 2160000,
    grossMonthlyIncome: 180000,
    netMonthlyIncome: 145000,
    salaryMonthly: 180000,
    itrYears: 1800000,
    otherIncomeSource: "",
    otherIncomeAmount: 0,

    bankName: "ICICI Bank",
    accountNumber: "2345678901234567",
    ifscCode: "ICIC0000456",
    ifsc: "ICIC0000456",
    accountType: "Savings",
    branch: "Indirapuram",
    branchName: "Indirapuram",
    accountHolderName: "Amit Verma",
    accountSinceYears: 12,

    reference1Name: "Rohit Sharma",
    reference1Phone: "9999900020",
    reference1Address: "Sector 62, Noida",
    reference1Relationship: "Friend",
    reference1: {
      name: "Rohit Sharma",
      mobile: "9999900020",
      address: "Sector 62, Noida",
      pincode: "201309",
      city: "Noida",
    },

    reference2Name: "Sneha Jain",
    reference2Phone: "9999900021",
    reference2Address: "Vaishali, Ghaziabad",
    reference2Relationship: "Colleague",
    reference2: {
      name: "Sneha Jain",
      mobile: "9999900021",
      address: "Vaishali, Ghaziabad",
      pincode: "201010",
      city: "Ghaziabad",
    },

    aadhaarNumber: "234567890123",
    panNumber: "BBBPV2345B",
    passportNumber: "B23456789",
    gstNumber: "09AACCV2345C1Z7",
    dlNumber: "UP1620190005678",
    drivingLicense: "UP1620190005678",

    mobile: "9999900002",
    customerType: "New",
    kycStatus: "Completed",
    createdOn: "03 Jan 2026",

    personalInfo: {
      name: "Amit Verma",
      phone: "9999900002",
      fatherSpouseName: "Ramesh Verma",
      dateOfBirth: "1989-07-22",
      motherName: "Kavita Verma",
      residenceAddress: "Flat 12B, Green Residency, Indirapuram",
      pincode: "201014",
      stayingSince: 8,
      houseType: "owned",
    },
    residentialInfo: {
      city: "Ghaziabad",
      pincode: "201014",
      panNumber: "BBBPV2345B",
      houseType: "owned",
      stayingSince: 8,
      residenceAddress: "Flat 12B, Green Residency, Indirapuram",
    },
    employmentInfo: {
      employmentType: "Salaried",
      companyName: "Infosys",
      designation: "Technology Lead",
      monthlySalary: 180000,
    },
    incomeInfo: {},
    bankInfo: {
      bankName: "ICICI Bank",
      accountType: "Savings",
      accountNumber: "2345678901234567",
      ifscCode: "ICIC0000456",
    },
    kycInfo: {
      panNumber: "BBBPV2345B",
      aadharNumber: "234567890123",
      drivingLicense: "UP1620190005678",
    },
    references: [],
  },

  {
    id: 3,
    customerNumber: 1003,
    name: "Neha Malhotra",
    customerName: "Neha Malhotra",
    gender: "Female",
    dateOfBirth: "1992-11-05",
    dob: "1992-11-05",
    maritalStatus: "Single",
    phone: "9999900003",
    primaryMobile: "9999900003",
    email: "neha.m@email.com",
    address: "D-402, Rosewood Apartments, Vasant Kunj",
    residenceAddress: "D-402, Rosewood Apartments, Vasant Kunj",
    city: "Delhi",
    state: "Delhi",
    pincode: "110070",
    houseType: "rented",
    yearsInCurrentHouse: 4,
    sdwOf: "Anil Malhotra",
    motherName: "Rekha Malhotra",
    education: "postgraduate",
    dependents: 0,
    nomineeName: "Rekha Malhotra",
    nomineeDob: "1968-04-10",
    nomineeRelation: "Mother",

    occupationType: "Salaried",
    companyName: "HUL",
    companyType: "MNC",
    designation: "Brand Manager",
    businessNature: ["FMCG"],
    natureOfBusiness: "FMCG",
    employmentAddress: "HUL Corporate Office, Andheri East",
    officeAddress: "HUL Corporate Office, Andheri East",
    employmentCity: "Mumbai",
    officeCity: "Mumbai",
    employmentPincode: "400069",
    officePincode: "400069",
    employmentPhone: "022-39830000",
    officePhone: "022-39830000",
    yearsInBusiness: 7,
    incorporationYear: 2017,

    monthlyIncome: 130000,
    annualIncome: 1560000,
    grossMonthlyIncome: 130000,
    netMonthlyIncome: 105000,
    salaryMonthly: 130000,
    itrYears: 1400000,
    otherIncomeSource: "",
    otherIncomeAmount: 0,

    bankName: "Axis Bank",
    accountNumber: "3456789012345678",
    ifscCode: "UTIB0000789",
    ifsc: "UTIB0000789",
    accountType: "Savings",
    branch: "Vasant Kunj",
    branchName: "Vasant Kunj",
    accountHolderName: "Neha Malhotra",
    accountSinceYears: 6,

    reference1Name: "Kunal Mehra",
    reference1Phone: "9999900030",
    reference1Address: "Saket, Delhi",
    reference1Relationship: "Friend",
    reference1: {
      name: "Kunal Mehra",
      mobile: "9999900030",
      address: "Saket, Delhi",
      pincode: "110017",
      city: "Delhi",
    },

    reference2Name: "Ritika Shah",
    reference2Phone: "9999900031",
    reference2Address: "Malviya Nagar, Delhi",
    reference2Relationship: "Colleague",
    reference2: {
      name: "Ritika Shah",
      mobile: "9999900031",
      address: "Malviya Nagar, Delhi",
      pincode: "110017",
      city: "Delhi",
    },

    aadhaarNumber: "345678901234",
    panNumber: "CCCPM6789C",
    passportNumber: "C34567890",
    gstNumber: "",
    dlNumber: "DL0320180012345",
    drivingLicense: "DL0320180012345",

    mobile: "9999900003",
    customerType: "New",
    kycStatus: "Completed",
    createdOn: "03 Jan 2026",

    personalInfo: {
      name: "Neha Malhotra",
      phone: "9999900003",
      fatherSpouseName: "Anil Malhotra",
      dateOfBirth: "1992-11-05",
      motherName: "Rekha Malhotra",
      residenceAddress: "D-402, Rosewood Apartments, Vasant Kunj",
      pincode: "110070",
      stayingSince: 4,
      houseType: "rented",
    },
    residentialInfo: {
      city: "Delhi",
      pincode: "110070",
      panNumber: "CCCPM6789C",
      houseType: "rented",
      stayingSince: 4,
      residenceAddress: "D-402, Rosewood Apartments, Vasant Kunj",
    },
    employmentInfo: {
      employmentType: "Salaried",
      companyName: "HUL",
      designation: "Brand Manager",
      monthlySalary: 130000,
    },
    incomeInfo: {},
    bankInfo: {
      bankName: "Axis Bank",
      accountType: "Savings",
      accountNumber: "3456789012345678",
      ifscCode: "UTIB0000789",
    },
    kycInfo: {
      panNumber: "CCCPM6789C",
      aadharNumber: "345678901234",
      drivingLicense: "DL0320180012345",
    },
    references: [],
  },

  {
    id: 4,
    customerNumber: 1004,
    name: "Suresh Iyer",
    customerName: "Suresh Iyer",
    gender: "Male",
    dateOfBirth: "1978-01-18",
    dob: "1978-01-18",
    maritalStatus: "Married",
    phone: "9999900004",
    primaryMobile: "9999900004",
    email: "suresh.iyer@email.com",
    address: "No 18, Temple Street, Adyar",
    residenceAddress: "No 18, Temple Street, Adyar",
    city: "Chennai",
    state: "Tamil Nadu",
    pincode: "600020",
    houseType: "owned",
    yearsInCurrentHouse: 18,
    sdwOf: "Krishna Iyer",
    motherName: "Lakshmi Iyer",
    education: "graduate",
    dependents: 2,
    nomineeName: "Meera Iyer",
    nomineeDob: "1982-08-09",
    nomineeRelation: "Spouse",

    occupationType: "Self Employed",
    companyName: "Iyer & Co",
    companyType: "Proprietorship",
    designation: "Owner",
    businessNature: ["Trading"],
    natureOfBusiness: "Trading",
    employmentAddress: "Parrys Corner, Chennai",
    officeAddress: "Parrys Corner, Chennai",
    employmentCity: "Chennai",
    officeCity: "Chennai",
    employmentPincode: "600001",
    officePincode: "600001",
    employmentPhone: "044-25670000",
    officePhone: "044-25670000",
    yearsInBusiness: 20,
    incorporationYear: 2005,

    monthlyIncome: 200000,
    annualIncome: 2400000,
    grossMonthlyIncome: 200000,
    netMonthlyIncome: 165000,
    salaryMonthly: 200000,
    itrYears: 2200000,
    otherIncomeSource: "Rental",
    otherIncomeAmount: 25000,

    bankName: "SBI",
    accountNumber: "4567890123456789",
    ifscCode: "SBIN0000456",
    ifsc: "SBIN0000456",
    accountType: "Current",
    branch: "Adyar",
    branchName: "Adyar",
    accountHolderName: "Suresh Iyer",
    accountSinceYears: 20,

    reference1Name: "Ravi Menon",
    reference1Phone: "9999900040",
    reference1Address: "Mylapore, Chennai",
    reference1Relationship: "Friend",
    reference1: {
      name: "Ravi Menon",
      mobile: "9999900040",
      address: "Mylapore, Chennai",
      pincode: "600004",
      city: "Chennai",
    },

    reference2Name: "Arun Rao",
    reference2Phone: "9999900041",
    reference2Address: "T Nagar, Chennai",
    reference2Relationship: "Business Partner",
    reference2: {
      name: "Arun Rao",
      mobile: "9999900041",
      address: "T Nagar, Chennai",
      pincode: "600017",
      city: "Chennai",
    },

    aadhaarNumber: "456789012345",
    panNumber: "DDDPI4567D",
    passportNumber: "D45678901",
    gstNumber: "33AABPI4567D1Z3",
    dlNumber: "TN0120150007890",
    drivingLicense: "TN0120150007890",

    mobile: "9999900004",
    customerType: "Existing",
    kycStatus: "Completed",
    createdOn: "03 Jan 2026",

    personalInfo: {
      name: "Suresh Iyer",
      phone: "9999900004",
      fatherSpouseName: "Krishna Iyer",
      dateOfBirth: "1978-01-18",
      motherName: "Lakshmi Iyer",
      residenceAddress: "No 18, Temple Street, Adyar",
      pincode: "600020",
      stayingSince: 18,
      houseType: "owned",
    },
    residentialInfo: {
      city: "Chennai",
      pincode: "600020",
      panNumber: "DDDPI4567D",
      houseType: "owned",
      stayingSince: 18,
      residenceAddress: "No 18, Temple Street, Adyar",
    },
    employmentInfo: {
      employmentType: "Self Employed",
      companyName: "Iyer & Co",
      designation: "Owner",
      monthlySalary: 200000,
    },
    incomeInfo: {},
    bankInfo: {
      bankName: "SBI",
      accountType: "Current",
      accountNumber: "4567890123456789",
      ifscCode: "SBIN0000456",
    },
    kycInfo: {
      panNumber: "DDDPI4567D",
      aadharNumber: "456789012345",
      drivingLicense: "TN0120150007890",
    },
    references: [],
  },

  {
    id: 5,
    customerNumber: 1005,
    name: "Pooja Desai",
    customerName: "Pooja Desai",
    gender: "Female",
    dateOfBirth: "1990-05-27",
    dob: "1990-05-27",
    maritalStatus: "Married",
    phone: "9999900005",
    primaryMobile: "9999900005",
    email: "pooja.desai@email.com",
    address: "A-901, Silver Heights, Satellite",
    residenceAddress: "A-901, Silver Heights, Satellite",
    city: "Ahmedabad",
    state: "Gujarat",
    pincode: "380015",
    houseType: "owned",
    yearsInCurrentHouse: 6,
    sdwOf: "Mahesh Desai",
    motherName: "Nita Desai",
    education: "postgraduate",
    dependents: 1,
    nomineeName: "Rohan Desai",
    nomineeDob: "1987-12-02",
    nomineeRelation: "Spouse",

    occupationType: "Salaried",
    companyName: "Adani Group",
    companyType: "Corporate",
    designation: "Finance Manager",
    businessNature: ["Finance"],
    natureOfBusiness: "Finance",
    employmentAddress: "Adani Corporate House, SG Highway",
    officeAddress: "Adani Corporate House, SG Highway",
    employmentCity: "Ahmedabad",
    officeCity: "Ahmedabad",
    employmentPincode: "380051",
    officePincode: "380051",
    employmentPhone: "079-25550000",
    officePhone: "079-25550000",
    yearsInBusiness: 9,
    incorporationYear: 2016,

    monthlyIncome: 160000,
    annualIncome: 1920000,
    grossMonthlyIncome: 160000,
    netMonthlyIncome: 130000,
    salaryMonthly: 160000,
    itrYears: 1700000,
    otherIncomeSource: "",
    otherIncomeAmount: 0,

    bankName: "Kotak Mahindra Bank",
    accountNumber: "5678901234567890",
    ifscCode: "KKBK0000678",
    ifsc: "KKBK0000678",
    accountType: "Savings",
    branch: "Satellite",
    branchName: "Satellite",
    accountHolderName: "Pooja Desai",
    accountSinceYears: 9,

    reference1Name: "Mehul Shah",
    reference1Phone: "9999900050",
    reference1Address: "Bodakdev, Ahmedabad",
    reference1Relationship: "Friend",
    reference1: {
      name: "Mehul Shah",
      mobile: "9999900050",
      address: "Bodakdev, Ahmedabad",
      pincode: "380054",
      city: "Ahmedabad",
    },

    reference2Name: "Kiran Patel",
    reference2Phone: "9999900051",
    reference2Address: "Prahlad Nagar, Ahmedabad",
    reference2Relationship: "Colleague",
    reference2: {
      name: "Kiran Patel",
      mobile: "9999900051",
      address: "Prahlad Nagar, Ahmedabad",
      pincode: "380015",
      city: "Ahmedabad",
    },

    aadhaarNumber: "567890123456",
    panNumber: "EEEPT7890E",
    passportNumber: "E56789012",
    gstNumber: "",
    dlNumber: "GJ0120160004567",
    drivingLicense: "GJ0120160004567",

    mobile: "9999900005",
    customerType: "New",
    kycStatus: "Completed",
    createdOn: "03 Jan 2026",

    personalInfo: {
      name: "Pooja Desai",
      phone: "9999900005",
      fatherSpouseName: "Mahesh Desai",
      dateOfBirth: "1990-05-27",
      motherName: "Nita Desai",
      residenceAddress: "A-901, Silver Heights, Satellite",
      pincode: "380015",
      stayingSince: 6,
      houseType: "owned",
    },
    residentialInfo: {
      city: "Ahmedabad",
      pincode: "380015",
      panNumber: "EEEPT7890E",
      houseType: "owned",
      stayingSince: 6,
      residenceAddress: "A-901, Silver Heights, Satellite",
    },
    employmentInfo: {
      employmentType: "Salaried",
      companyName: "Adani Group",
      designation: "Finance Manager",
      monthlySalary: 160000,
    },
    incomeInfo: {},
    bankInfo: {
      bankName: "Kotak Mahindra Bank",
      accountType: "Savings",
      accountNumber: "5678901234567890",
      ifscCode: "KKBK0000678",
    },
    kycInfo: {
      panNumber: "EEEPT7890E",
      aadharNumber: "567890123456",
      drivingLicense: "GJ0120160004567",
    },
    references: [],
  },
];

const importData = async () => {
  try {
    // Clear existing data
    await User.deleteMany();
    await Customer.deleteMany();
    await Loan.deleteMany();
    await DeliveryOrder.deleteMany();
    await Payment.deleteMany();

    console.log('Data Destroyed...');

    // 1. Create Admin User
    const createdUser = await User.create({
      name: 'Admin User',
      email: 'admin@example.com',
      password: 'password123',
      role: 'superadmin',
    });

    const adminId = createdUser._id;

    // 2. Create Customers from Demo Data
    const customersToInsert = demoCustomers.map(c => ({
      ...c, // Spread all fields from demo data
      
      // Ensure specific required fields map if names differ (though demo data seems to have duplications like phone/primaryMobile)
      customerName: c.customerName || c.name,
      primaryMobile: c.primaryMobile || c.phone,
      
      createdBy: adminId,
    }));

    const createdCustomers = await Customer.insertMany(customersToInsert);
    console.log(`Created ${createdCustomers.length} Customers`);

    const rajKumar = createdCustomers[0]; // Raj Kumar
    const amitVerma = createdCustomers[1]; // Amit Verma

    // 3. Create Loans
    // Loan 1: Raj Kumar - Nexon
    const loan1 = await Loan.create({
      loanId: 'LN-2024-0001',
      customerId: rajKumar._id,
      customerName: rajKumar.customerName,
      
      // Basic Info
      isFinanced: 'Yes',
      loanType: 'New Car',
      currentStage: 'approval', 
      status: 'Approved',

      // Vehicle
      vehicleMake: 'Tata',
      vehicleModel: 'Nexon',
      vehicleVariant: 'XZ+',
      exShowroomPrice: 1200000,
      
      // Approval Data
      approval_bankName: 'HDFC Bank',
      approval_status: 'Approved',
      approval_loanAmountApproved: 950000,
      approval_roi: 8.5,
      approval_tenureMonths: 60,
      approval_processingFees: 5000,
      approval_approvalDate: new Date(),
      
      approval_banksData: [
        {
          id: 'bank-1',
          bankName: 'HDFC Bank',
          status: 'Approved',
          loanAmount: 950000,
          interestRate: 8.5,
          tenure: 60,
          notes: 'Standard approval',
          processingFee: 5000,
        }
      ],
      
      createdBy: adminId,
    });

    // Loan 2: Amit Verma - Swift (Cash)
    await Loan.create({
      loanId: 'LN-2024-0002',
      customerId: amitVerma._id,
      customerName: amitVerma.customerName,
      
      isFinanced: 'No',
      loanType: 'Used Car',
      currentStage: 'delivery',
      status: 'In Progress',

      vehicleMake: 'Maruti',
      vehicleModel: 'Swift',
      vehicleVariant: 'VXI',
      
      createdBy: adminId,
    });

    // 4. Create Delivery Orders
    await DeliveryOrder.create({
      loanId: loan1.loanId,
      do_loanId: loan1.loanId,
      
      do_date: new Date(),
      do_refNo: 'DO-001',
      
      // Vehicle Details Showroom
      do_vehicleMake: 'Tata',
      do_vehicleModel: 'Nexon',
      do_vehicleVariant: 'XZ+',
      do_exShowroomPrice: 1200000,
      do_insuranceCost: 45000,
      do_roadTax: 100000,
      do_tcs: 12000,
      do_fastag: 500,
      
      // Calculated
      do_onRoadVehicleCost: 1357500,
      do_marginMoneyPaid: 50000,
      do_grossDO: 1307500,
      
      createdBy: adminId,
    });

    // 5. Create Payments (Payment Sheet)
    await Payment.create({
      loanId: loan1.loanId,
      
      // Showroom Rows
      showroomRows: [
        {
          id: 'pay-1',
          paymentDate: new Date().toISOString(),
          amount: 50000,
          mode: 'Cash',
          type: 'Booking Amount',
          narration: 'Initial booking by Raj',
        }
      ],
      
      entryTotals: {
        paymentAmountCustomer: 50000,
      },
      
      createdBy: adminId,
    });

    console.log('Data Imported Successfully!');
    process.exit();
  } catch (error) {
    console.error(`Error: ${error.message}`);
    process.exit(1);
  }
};

const destroyData = async () => {
  try {
    await User.deleteMany();
    await Customer.deleteMany();
    await Loan.deleteMany();
    await DeliveryOrder.deleteMany();
    await Payment.deleteMany();

    console.log('Data Destroyed!');
    process.exit();
  } catch (error) {
    console.error(`Error: ${error.message}`);
    process.exit(1);
  }
};

if (process.argv[2] === '-d') {
  destroyData();
} else {
  importData();
}
