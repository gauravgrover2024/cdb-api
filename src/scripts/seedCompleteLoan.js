import mongoose from 'mongoose';
import dotenv from 'dotenv';
import Customer from '../models/Customer.js';
import Loan from '../models/Loan.js';
import connectDB from '../config/db.js';

dotenv.config();

const seedCompleteLoan = async () => {
  try {
    await connectDB();
    
    console.log('🌱 Starting Complete Loan Seeding...\n');

    // Create a comprehensive customer first
    const customerData = {
      customerId: 'ACILLP-2024-0001',
      customerName: 'Rajesh Kumar Sharma',
      primaryMobile: '9876543210',
      email: 'rajesh.sharma@example.com',
      
      // Personal Details
      dob: new Date('1985-06-15'),
      gender: 'Male',
      maritalStatus: 'Married',
      dependents: 2,
      education: 'Graduate',
      sdwOf: 'Late Mohan Lal Sharma',
      motherName: 'Sunita Sharma',
      fatherName: 'Mohan Lal Sharma',
      
      // Address Details
      residenceAddress: '123, Green Park Society, MG Road',
      pincode: '560001',
      city: 'Bangalore',
      state: 'Karnataka',
      houseType: 'Owned',
      yearsInCurrentHouse: 5,
      yearsInCurrentCity: 8,
      permanentAddress: '456, Old Street, Sector 12',
      permanentPincode: '560002',
      permanentCity: 'Bangalore',
      
      // Employment Details
      occupationType: 'Salaried',
      professionalType: 'Software Engineer',
      employmentType: 'Permanent',
      companyName: 'Tech Solutions Pvt Ltd',
      companyType: 'Private Limited',
      designation: 'Senior Software Engineer',
      currentExp: 8,
      totalExp: 10,
      employmentAddress: 'Electronic City Phase 1, Bangalore',
      employmentPincode: '560100',
      employmentCity: 'Bangalore',
      officialEmail: 'rajesh.sharma@techsolutions.com',
      
      // Income Details
      monthlyIncome: 85000,
      salaryMonthly: 85000,
      annualIncome: 1020000,
      
      // Banking Details
      bankName: 'HDFC Bank',
      accountNumber: '50100123456789',
      ifscCode: 'HDFC0001234',
      ifsc: 'HDFC0001234',
      branch: 'MG Road Branch',
      accountType: 'Savings',
      accountSinceYears: 6,
      
      // KYC Details
      panNumber: 'ABCDE1234F',
      aadhaarNumber: '123456789012',
      aadharNumber: '123456789012',
      dlNumber: 'KA0120190012345',
      voterId: 'ABC1234567',
      
      // Nominee
      nomineeName: 'Priya Sharma',
      nomineeDob: new Date('1987-08-20'),
      nomineeRelation: 'Wife',
      
      // Additional
      extraMobiles: ['9876543211', '9876543212'],
      whatsappNumber: '9876543210',
      
      kycStatus: 'Completed',
      createdFrom: 'SEEDER'
    };

    console.log('Creating customer...');
    let customer = await Customer.findOne({ primaryMobile: customerData.primaryMobile });
    
    if (!customer) {
      customer = await Customer.create(customerData);
      console.log('✅ Customer created:', customer.customerId);
    } else {
      console.log('✅ Customer already exists:', customer.customerId);
    }

    // Create comprehensive loan with ALL fields
    const loanData = {
      loanId: 'LN-2026-TEST-001',
      customerId: customer._id,
      
      // Denormalized customer info
      customerName: customerData.customerName,
      primaryMobile: customerData.primaryMobile,
      
      // Personal Details (from customer + additional)
      dob: new Date('1985-06-15'),
      gender: 'Male',
      maritalStatus: 'Married',
      dependents: 2,
      education: 'Graduate',
      sdwOf: 'Late Mohan Lal Sharma',
      motherName: 'Sunita Sharma',
      fatherName: 'Mohan Lal Sharma',
      email: 'rajesh.sharma@example.com',
      extraMobiles: ['9876543211', '9876543212'],
      whatsappNumber: '9876543210',
      
      // Address Details
      residenceAddress: '123, Green Park Society, MG Road',
      pincode: '560001',
      city: 'Bangalore',
      state: 'Karnataka',
      houseType: 'Owned',
      addressType: 'Permanent',
      yearsInCurrentHouse: 5,
      yearsInCurrentCity: 8,
      permanentAddress: '456, Old Street, Sector 12',
      permanentPincode: '560002',
      permanentCity: 'Bangalore',
      
      // Sourcing & Lead
      sourcingChannel: 'Direct Walk-in',
      leadId: 'LEAD-2026-001',
      salesExecutive: 'Vikram Mehta',
      leadDate: new Date('2026-01-15'),
      source: 'Direct',
      recordSource: 'Direct',
      sourceDetails: 'Customer visited showroom directly',
      dealtBy: 'EMP-001',
      
      // Applicant Type
      applicantType: 'Individual',
      isMSME: 'No',
      
      // Employment & Income
      occupationType: 'Salaried',
      professionalType: 'Software Engineer',
      employmentType: 'Permanent',
      companyName: 'Tech Solutions Pvt Ltd',
      companyType: 'Private Limited',
      companyAddress: 'Electronic City Phase 1, Bangalore',
      companyPincode: '560100',
      companyCity: 'Bangalore',
      companyPhone: '080-12345678',
      designation: 'Senior Software Engineer',
      currentExp: 8,
      totalExp: 10,
      employmentAddress: 'Electronic City Phase 1, Bangalore',
      employmentPincode: '560100',
      employmentCity: 'Bangalore',
      officialEmail: 'rajesh.sharma@techsolutions.com',
      
      monthlyIncome: 85000,
      salaryMonthly: 85000,
      monthlySalary: 85000,
      annualIncome: 1020000,
      otherIncome: 5000,
      otherIncomeSource: 'Freelance Consulting',
      
      // KYC & Identity
      identityProofType: 'Aadhaar',
      identityProofNumber: '123456789012',
      addressProofType: 'Voter ID',
      panNumber: 'ABCDE1234F',
      aadhaarNumber: '123456789012',
      aadharNumber: '123456789012',
      dlNumber: 'KA0120190012345',
      voterId: 'ABC1234567',
      kycStatus: 'Completed',
      
      // Banking
      bankName: 'HDFC Bank',
      accountNumber: '50100123456789',
      ifscCode: 'HDFC0001234',
      ifsc: 'HDFC0001234',
      branch: 'MG Road Branch',
      accountType: 'Savings',
      accountSinceYears: 6,
      
      // Nominee
      nomineeName: 'Priya Sharma',
      nomineeDob: new Date('1987-08-20'),
      nomineeRelation: 'Wife',
      
      // Vehicle Details
      vehicleMake: 'Maruti Suzuki',
      vehicleModel: 'Swift VXi',
      vehicleVariant: 'VXi 1.2L Petrol',
      vehicleType: 'Hatchback',
      vehicleFuel: 'Petrol',
      vehicleTransmission: 'Manual',
      vehicleColor: 'Pearl Arctic White',
      usage: 'Personal',
      manufacturingYear: '2026',
      chassisNumber: 'MA3ERLF3S00123456',
      engineNumber: 'K12MN9876543',
      
      // Pricing & Loan
      exShowroomPrice: 750000,
      insuranceCost: 35000,
      roadTax: 45000,
      accessoriesAmount: 20000,
      onRoadPrice: 850000,
      dealerDiscount: 15000,
      manufacturerDiscount: 10000,
      
      isFinanced: 'Yes',
      typeOfLoan: 'New Vehicle',
      loanType: 'Auto Loan',
      loanAmount: 600000,
      requiredLoanAmount: 600000,
      tenure: 60,
      loanTenureMonths: 60,
      interestRate: 9.5,
      financeExpectation: 600000,
      marginMoney: 250000,
      advanceEmi: 12000,
      
      // Dealer Details
      dealerName: 'Prime Auto Motors',
      dealerContactPerson: 'Suresh Iyer',
      dealerContactNumber: '9876000001',
      dealerAddress: 'Plot 45, Industrial Area, Whitefield',
      
      // Registration
      hypothecation: 'Yes',
      hypothecationBank: 'HDFC Bank',
      registerSameAsAadhaar: 'Yes',
      registrationAddress: '123, Green Park Society, MG Road, Bangalore',
      registrationNumber: 'KA01AB1234',
      
      // Insurance
      policyType: 'Comprehensive',
      insuranceExpiry: new Date('2027-02-05'),
      
      // References (FLAT STRUCTURE)
      reference1_name: 'Amit Kumar Patel',
      reference1_mobile: '9876000010',
      reference1_address: '789, Lake View Apartments, Koramangala',
      reference1_pincode: '560034',
      reference1_city: 'Bangalore',
      reference1_relation: 'Friend',
      
      reference2_name: 'Sandeep Reddy',
      reference2_mobile: '9876000020',
      reference2_address: '321, Highland Towers, Indiranagar',
      reference2_pincode: '560038',
      reference2_city: 'Bangalore',
      reference2_relation: 'Colleague',
      
      // Co-Applicant Details (COMPLETE)
      hasCoApplicant: true,
      co_name: 'Priya Sharma',
      co_motherName: 'Kamala Devi',
      co_fatherName: 'Rajendra Kumar',
      co_dob: new Date('1987-08-20'),
      co_gender: 'Female',
      co_maritalStatus: 'Married',
      co_dependents: 2,
      co_education: 'Post Graduate',
      co_house: 'Owned',
      co_mobile: '9876000030',
      co_address: '123, Green Park Society, MG Road, Bangalore',
      co_pincode: '560001',
      co_city: 'Bangalore',
      co_pan: 'XYZAB5678C',
      co_aadhaar: '987654321098',
      co_aadhar: '987654321098',
      co_occupation: 'Salaried',
      co_occupationType: 'Salaried',
      co_professionalType: 'Teacher',
      co_designation: 'Senior Teacher',
      co_currentExp: 5,
      co_totalExp: 7,
      co_companyName: 'Delhi Public School',
      co_companyType: 'Educational Institution',
      co_companyAddress: 'DPS Campus, Bangalore',
      co_companyPincode: '560050',
      co_companyCity: 'Bangalore',
      co_companyPhone: '080-98765432',
      co_salaryMonthly: 45000,
      co_monthlySalary: 45000,
      
      // Guarantor Details (COMPLETE)
      hasGuarantor: true,
      gu_name: 'Ramesh Sharma',
      gu_motherName: 'Sunita Sharma',
      gu_fatherName: 'Mohan Lal Sharma',
      gu_dob: new Date('1960-03-10'),
      gu_gender: 'Male',
      gu_maritalStatus: 'Married',
      gu_dependents: 1,
      gu_education: 'Graduate',
      gu_house: 'Owned',
      gu_mobile: '9876000040',
      gu_address: '999, Old Town, Malleshwaram, Bangalore',
      gu_pincode: '560003',
      gu_city: 'Bangalore',
      gu_pan: 'PQRST9876G',
      gu_aadhaar: '456789123456',
      gu_aadhar: '456789123456',
      gu_occupation: 'Self Employed',
      gu_occupationType: 'Self Employed',
      gu_professionalType: 'Business',
      gu_businessNature: ['Retail', 'Wholesale'],
      gu_designation: 'Proprietor',
      gu_currentExp: 25,
      gu_totalExp: 30,
      gu_companyName: 'Sharma Traders',
      gu_companyType: 'Proprietorship',
      gu_companyAddress: 'Shop 12, Commercial Street',
      gu_companyPincode: '560001',
      gu_companyCity: 'Bangalore',
      gu_companyPhone: '080-22334455',
      gu_salaryMonthly: 75000,
      gu_monthlySalary: 75000,
      
      // Loan Approval Stage
      currentStage: 'approval',
      status: 'Approved',
      
      approval_bankId: 'BANK-001',
      approval_bankName: 'HDFC Bank Auto Loan',
      approval_status: 'Approved',
      approval_loanAmountApproved: 550000,
      approval_roi: 9.25,
      approval_tenureMonths: 60,
      approval_processingFees: 15000,
      approval_approvalDate: new Date('2026-02-01'),
      approval_remarks: 'Approved with standard terms',
      
      // Approval Breakup
      approval_breakup_netLoanApproved: 550000,
      approval_breakup_creditAssured: 10000,
      approval_breakup_insuranceFinance: 35000,
      approval_breakup_ewFinance: 5000,
      
      // Multi-Bank Data
      approval_banksData: [
        {
          id: 'BANK-001',
          bankName: 'HDFC Bank Auto Loan',
          loanAmount: 550000,
          interestRate: 9.25,
          tenure: 60,
          processingFee: 15000,
          status: 'Approved',
          approvalDate: new Date('2026-02-01'),
          remarks: 'Approved with standard terms',
          statusHistory: [
            { status: 'Pending', changedAt: new Date('2026-01-20'), changedBy: 'System' },
            { status: 'Under Review', changedAt: new Date('2026-01-25'), changedBy: 'Bank Officer' },
            { status: 'Approved', changedAt: new Date('2026-02-01'), changedBy: 'Credit Manager' }
          ]
        }
      ],
      
      // Disbursement
      disburse_status: 'Pending',
      approval_loanAmountDisbursed: 0,
      
      // Payout (for indirect sources)
      payoutApplicable: 'No',
      
      // Lead Details
      leadType: 'Hot',
      leadSource: 'Walk-in',
      
      // Document URLs (examples)
      aadhaarDocUrl: 'https://cloudinary.com/sample/aadhaar.pdf',
      panDocUrl: 'https://cloudinary.com/sample/pan.pdf',
      salarySlipDocUrl: 'https://cloudinary.com/sample/salary.pdf',
      bankStatementDocUrl: 'https://cloudinary.com/sample/statement.pdf',
      
      // System fields - remove createdBy as it expects ObjectId
      isBulk: false
    };

    console.log('\nCreating comprehensive loan with ALL fields...');
    
    // Check if loan already exists
    const existingLoan = await Loan.findOne({ loanId: loanData.loanId });
    
    if (existingLoan) {
      console.log('⚠️  Loan already exists. Updating...');
      Object.assign(existingLoan, loanData);
      await existingLoan.save();
      console.log('✅ Loan updated:', existingLoan.loanId);
    } else {
      const loan = await Loan.create(loanData);
      console.log('✅ Loan created:', loan.loanId);
    }

    // Fetch and display created loan
    const createdLoan = await Loan.findOne({ loanId: loanData.loanId })
      .populate('customerId')
      .lean();

    console.log('\n' + '═'.repeat(60));
    console.log('📊 COMPLETE LOAN DATA SUMMARY');
    console.log('═'.repeat(60));
    console.log('\n🔑 LOAN DETAILS:');
    console.log('   Loan ID:', createdLoan.loanId);
    console.log('   Customer:', createdLoan.customerName);
    console.log('   Mobile:', createdLoan.primaryMobile);
    console.log('   Total Fields in DB:', Object.keys(createdLoan).length);
    
    console.log('\n👤 PERSONAL INFO:');
    console.log('   DOB:', createdLoan.dob);
    console.log('   Gender:', createdLoan.gender);
    console.log('   Marital Status:', createdLoan.maritalStatus);
    console.log('   Education:', createdLoan.education);
    
    console.log('\n🏠 ADDRESS:');
    console.log('   Address:', createdLoan.residenceAddress);
    console.log('   City:', createdLoan.city);
    console.log('   Pincode:', createdLoan.pincode);
    console.log('   House Type:', createdLoan.houseType);
    
    console.log('\n💼 EMPLOYMENT:');
    console.log('   Occupation:', createdLoan.occupationType);
    console.log('   Company:', createdLoan.companyName);
    console.log('   Designation:', createdLoan.designation);
    console.log('   Experience:', createdLoan.currentExp, 'years');
    
    console.log('\n💰 INCOME:');
    console.log('   Monthly Income:', '₹', createdLoan.monthlyIncome?.toLocaleString());
    console.log('   Annual Income:', '₹', createdLoan.annualIncome?.toLocaleString());
    
    console.log('\n🚗 VEHICLE:');
    console.log('   Make:', createdLoan.vehicleMake);
    console.log('   Model:', createdLoan.vehicleModel);
    console.log('   Variant:', createdLoan.vehicleVariant);
    console.log('   Color:', createdLoan.vehicleColor);
    
    console.log('\n💳 LOAN:');
    console.log('   Loan Amount:', '₹', createdLoan.loanAmount?.toLocaleString());
    console.log('   Tenure:', createdLoan.tenure, 'months');
    console.log('   Interest Rate:', createdLoan.interestRate, '%');
    console.log('   Ex-Showroom Price:', '₹', createdLoan.exShowroomPrice?.toLocaleString());
    console.log('   On-Road Price:', '₹', createdLoan.onRoadPrice?.toLocaleString());
    
    console.log('\n🏦 BANKING:');
    console.log('   Bank:', createdLoan.bankName);
    console.log('   Account:', createdLoan.accountNumber);
    console.log('   IFSC:', createdLoan.ifscCode);
    
    console.log('\n📄 KYC:');
    console.log('   PAN:', createdLoan.panNumber);
    console.log('   Aadhaar:', createdLoan.aadhaarNumber);
    console.log('   DL:', createdLoan.dlNumber);
    
    console.log('\n📞 REFERENCES:');
    console.log('   Reference 1:', createdLoan.reference1_name, '-', createdLoan.reference1_mobile);
    console.log('   Reference 2:', createdLoan.reference2_name, '-', createdLoan.reference2_mobile);
    
    console.log('\n👥 CO-APPLICANT:');
    console.log('   Has Co-Applicant:', createdLoan.hasCoApplicant);
    if (createdLoan.hasCoApplicant) {
      console.log('   Name:', createdLoan.co_name);
      console.log('   Mobile:', createdLoan.co_mobile);
      console.log('   Occupation:', createdLoan.co_occupation);
      console.log('   Income:', '₹', createdLoan.co_salaryMonthly?.toLocaleString());
    }
    
    console.log('\n🤝 GUARANTOR:');
    console.log('   Has Guarantor:', createdLoan.hasGuarantor);
    if (createdLoan.hasGuarantor) {
      console.log('   Name:', createdLoan.gu_name);
      console.log('   Mobile:', createdLoan.gu_mobile);
      console.log('   Occupation:', createdLoan.gu_occupation);
      console.log('   Income:', '₹', createdLoan.gu_salaryMonthly?.toLocaleString());
    }
    
    console.log('\n✅ APPROVAL:');
    console.log('   Status:', createdLoan.approval_status);
    console.log('   Bank:', createdLoan.approval_bankName);
    console.log('   Approved Amount:', '₹', createdLoan.approval_loanAmountApproved?.toLocaleString());
    console.log('   ROI:', createdLoan.approval_roi, '%');
    console.log('   Approval Date:', createdLoan.approval_approvalDate);
    console.log('   Processing Fee:', '₹', createdLoan.approval_processingFees?.toLocaleString());
    
    console.log('\n' + '═'.repeat(60));
    console.log('✅ Seeding Completed Successfully!');
    console.log('═'.repeat(60));
    console.log('\n📝 You can now test:');
    console.log('   1. View Loan Details: GET /api/loans/' + createdLoan.loanId);
    console.log('   2. Edit Loan: PUT /api/loans/' + createdLoan.loanId);
    console.log('   3. Frontend: Open loan in UI to see all fields populated');
    console.log('\n');

    process.exit(0);

  } catch (error) {
    console.error('\n❌ Seeding Error:', error.message);
    console.error(error);
    process.exit(1);
  }
};

seedCompleteLoan();
