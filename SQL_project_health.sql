

-- Creating Database
CREATE DATABASE HospitalManagement;
GO
-- Using Database
USE HospitalManagement;
GO

-- creating tables
CREATE TABLE Patients (
    patients_id INT PRIMARY KEY IDENTITY(1,1),
    name VARCHAR(100) NOT NULL,
    dob DATE NOT NULL,
    gender CHAR(1) CHECK (gender IN ('M', 'F', 'O')),
    contact_info VARCHAR(100)
	age INT
);
GO

CREATE TABLE Doctors (
    doctors_id INT PRIMARY KEY IDENTITY(1,1),
    doctors_name VARCHAR(100) NOT NULL,
    specialty VARCHAR(100) NOT NULL,
    contact_info VARCHAR(100)
);
GO

CREATE TABLE Appointments (
    appointment_id INT PRIMARY KEY IDENTITY(1,1),
    patients_id INT NOT NULL,
    doctors_id INT NOT NULL,
    appointment_date DATETIME NOT NULL,
    reason VARCHAR(200),
    status VARCHAR(50) CHECK (status IN ('Scheduled', 'Completed', 'Cancelled')),

    FOREIGN KEY (patient_id) REFERENCES Patients(patient_id),
    FOREIGN KEY (doctor_id) REFERENCES Doctors(doctor_id)
);
GO

CREATE TABLE Prescriptions (
   prescription_id INT PRIMARY KEY IDENTITY(1,1),
   appointment_id INT,
   medication_name VARCHAR(100) NOT NULL,
   dosage VARCHAR(50),
   frequency VARCHAR(50),
   duration VARCHAR(50),
   notes TEXT,
   FOREIGN KEY (appointment_id) REFERENCES Appointments(appointment_id)
);

CREATE TABLE MedicalRecords (
   record_id INT PRIMARY KEY IDENTITY(1,1),
   patients_id INT NOT NULL,
   doctors_id INT NOT NULL,
   visit_date DATETIME NOT NULL,
   diagnosis TEXT,
   treatment TEXT,
   allergies TEXT,
   notes TEXT,

   FOREIGN KEY (patients_id) REFERENCES Patients(patients_id),
   FOREIGN KEY (doctors_id) REFERENCES Doctors(doctors_id)
);

CREATE TABLE Billing (
   billing_id INT PRIMARY KEY IDENTITY(1,1),
   patients_id INT NOT NULL,
   amount DECIMAL(10,2) NOT NULL,
   billing_date DATE NOT NULL,
   payment_status VARCHAR(50) CHECK (payment_status IN ('Paid', 'Unpaid', 'Pending')),
   FOREIGN KEY (patients_id) REFERENCES Patients(patients_id)
);

CREATE TABLE LabTests (
   labtest_id INT PRIMARY KEY IDENTITY(1,1),
   patients_id INT NOT NULL,
   doctors_id INT NOT NULL,
   test_name VARCHAR(100) NOT NULL,
   test_date DATE DEFAULT GETDATE(),
   result TEXT,
   status VARCHAR(50) CHECK (status IN ('Pending', 'Completed')),

   FOREIGN KEY (patients_id) REFERENCES Patients(patients_id),
   FOREIGN KEY (doctors_id) REFERENCES Doctors(doctors_id)
);

CREATE TABLE MedicalHistory (
   history_id INT PRIMARY KEY IDENTITY(1,1),
   patients_id INT NOT NULL,
   condition_name VARCHAR(100) NOT NULL,
   diagnosis_date DATE,
   notes TEXT,

   FOREIGN KEY (patients_id) REFERENCES Patients(patients_id)
);

CREATE TABLE LabResults (
   lab_result_id INT PRIMARY KEY IDENTITY(1,1),
   patients_id INT NOT NULL,
   test_name VARCHAR(100) NOT NULL,
   result_value VARCHAR(100),
   unit VARCHAR(50),
   normal_range VARCHAR(100),
   test_date DATE DEFAULT GETDATE(),

   FOREIGN KEY (patients_id) REFERENCES Patients(patients_id)
);


-- inserting data into tables and viewing them
INSERT INTO Patients (name, dob, gender, contact_info, age) 
VALUES
('Temilola Adewale', '1992-04-12', 'F', '07010000001', '25'),
('Yetunde Ogunleye', '1988-08-23', 'F', '07010000002', '40'),
('Folashade Ajayi', '1995-12-05', 'F', '07010000003', '40'),
('Olamide Adebayo', '1990-03-18', 'F', '07010000004', '52'),
('Bukola Olawale', '1997-07-30', 'F', '07010000005', '29'),
('Abimbola Balogun', '1985-11-11', 'F', '07010000006', '61'),
('Adewunmi Ojo', '1999-01-27', 'F', '07010000007', '45'),
('Akinyele Lawal', '1987-09-14', 'M', '07010000008', '31'),
('Oluwaseun Fashola', '1993-05-22', 'F', '07010000009', '38'),
('Kehinde Akinola', '1989-06-10', 'M', '07010000010', '27');
GO

select * from Patients

INSERT INTO Doctors (doctors_name, speciality, contact_info)
VALUES
('Dr. Adeola Akinwale', 'Oncology', '08012345678'),
('Dr. Tayo Olufemi', 'Pathology', '08023456789'),
('Dr. Funmi Ogunleye', 'Histopathology', '08034567890'),
('Dr. Kunle Adebayo', 'Internal Medicine', '08045678901');

select * from Doctors

INSERT INTO Appointments (patients_id, doctors_id, appointment_date, reason, status)
VALUES
(1, 2, '2025-06-12 09:00:00', 'Routine Checkup', 'Scheduled'),
(2, 1, '2025-06-13 11:30:00', 'Headache and Nausea', 'Completed'),
(3, 3, '2025-06-14 14:00:00', 'Follow-up Visit', 'Scheduled'),
(4, 4, '2025-06-15 10:00:00', 'Stomach Ache', 'Cancelled'),
(5, 1, '2025-06-16 15:30:00', 'Cough and Cold', 'Scheduled'),
(6, 2, '2025-06-17 08:45:00', 'Lab Test Result Discussion', 'Completed'),
(7, 3, '2025-06-18 13:15:00', 'Skin Rash', 'Scheduled'),
(8, 4, '2025-06-19 12:00:00', 'Blood Pressure Issues', 'Scheduled'),
(9, 1, '2025-06-20 16:30:00', 'Back Pain', 'Scheduled'),
(10, 2, '2025-06-21 10:45:00', 'General Weakness', 'Scheduled');

select * from Appointments

INSERT INTO Prescriptions (appointment_id, medication_name, dosage, frequency, duration, notes)
VALUES
(1, 'Paracetamol', '500mg', '3 times a day', '5 days', 'Take after meals'),
(2, 'Amoxicillin', '250mg', '2 times a day', '7 days', 'Complete full dose'),
(3, 'Ibuprofen', '400mg', '3 times a day', '5 days', 'Avoid on empty stomach'),
(4, 'Loratadine', '10mg', 'Once daily', '10 days', 'Take in the morning'),
(5, 'Metformin', '500mg', 'Twice daily', '30 days', 'Monitor blood sugar'),
(6, 'Omeprazole', '20mg', 'Once daily', '14 days', 'Take before food'),
(7, 'Hydrocortisone Cream', 'Apply thin layer', 'Twice daily', '7 days', 'Do not apply to broken skin'),
(8, 'Amlodipine', '5mg', 'Once daily', '30 days', 'Monitor blood pressure'),
(9, 'Diclofenac', '50mg', 'Twice daily', '5 days', 'Take with meals'),
(10, 'Vitamin B complex', '1 tablet', 'Once daily', '30 days', 'Take in the morning');

select * from Prescriptions

INSERT INTO Medical_Records (patients_id, doctors_id, visit_date, diagnosis, treatment, allegies, note)
VALUES
(1, 1, '2025-06-01 09:00:00', 'Malaria', 'Artemether-Lumefantrine', 'None', 'Patient responded well to treatment'),
(2, 2, '2025-06-02 10:15:00', 'Tension Headache', 'Paracetamol and rest', 'None', 'Stress-related'),
(3, 3, '2025-06-03 11:30:00', 'Respiratory Infection', 'Amoxicillin and fluids', 'Penicillin', 'Advised to avoid cold drinks'),
(4, 4, '2025-06-04 14:45:00', 'Viral Infection', 'Rest and hydration', 'None', 'Symptoms improving'),
(5, 1, '2025-06-05 09:30:00', 'Type 2 Diabetes', 'Metformin', 'Sulfa drugs', 'Requires lifestyle modification'),
(6, 2, '2025-06-06 13:00:00', 'Gastritis', 'Omeprazole', 'NSAIDs', 'Advised to avoid spicy food'),
(7, 3, '2025-06-07 10:00:00', 'Contact Dermatitis', 'Hydrocortisone Cream', 'None', 'Skin improving'),
(8, 4, '2025-06-08 15:30:00', 'Hypertension', 'Amlodipine', 'None', 'Monitor blood pressure regularly'),
(9, 1, '2025-06-09 08:45:00', 'Back Pain', 'Diclofenac', 'None', 'Mild improvement after 3 days'),
(10, 2, '2025-06-10 12:20:00', 'Vitamin Deficiency', 'Vitamin B complex', 'None', 'Supplements advised for 1 month');
 
 select * from MedicalRecords

INSERT INTO Billing (patient_id, amount, billing_date, payment_status)
VALUES
(1, 15000.00, '2025-06-10', 'Paid'),
(2, 12000.50, '2025-06-11', 'Unpaid'),
(3, 20000.75, '2025-06-12', 'Paid'),
(4, 18000.00, '2025-06-13', 'Unpaid'),
(5, 25000.25, '2025-06-14', 'Paid'),
(6, 14000.00, '2025-06-15', 'Paid'),
(7, 16000.80, '2025-06-16', 'Unpaid'),
(8, 17000.00, '2025-06-17', 'Paid'),
(9, 19000.00, '2025-06-18', 'Paid'),
(10, 21000.40, '2025-06-19', 'Unpaid');

select * from Billing

INSERT INTO LabResults (patients_id, test_name, result_value, unit, normal_range, test_date)
VALUES
(1, 'Hemoglobin', '13.5', 'g/dL', '12-16', '2025-06-01'),
(2, 'ALT (Alanine transaminase)', '55', 'U/L', '7-56', '2025-06-02'),
(3, 'Chest X-Ray', 'Clear', NULL, 'N/A', '2025-06-03'),
(4, 'Fasting Blood Glucose', '105', 'mg/dL', '70-100', '2025-06-05'),
(5, 'Urine Protein', 'Negative', NULL, 'Negative', '2025-06-06'),
(6, 'Serum Sodium', '130', 'mmol/L', '135-145', '2025-06-07'),
(7, 'TSH', '2.5', 'mIU/L', '0.4-4.0', '2025-06-08'),
(8, 'MRI Brain Report', 'Normal', NULL, 'N/A', '2025-06-09'),
(9, 'Vitamin D', '15', 'ng/mL', '20-50', '2025-06-10'),
(10, 'COVID-19 PCR Result', 'Negative', NULL, 'Negative', '2025-06-11');
 
 select * from LabResults

INSERT INTO MedicalHistory (patients_id, condition_name, diagnosis_date, notes)
VALUES
(1, 'Hypertension', '2023-01-10', 'Controlled with medication'),
(2, 'Asthma', '2019-05-20', 'Uses inhaler during attacks'),
(3, 'Diabetes Type 2', '2021-07-15', 'Diet and medication managed'),
(4, 'Allergic Rhinitis', '2020-03-05', 'Seasonal allergies'),
(5, 'Migraine', '2018-11-22', 'Occasional severe headaches'),
(6, 'Gastritis', '2022-09-18', 'Improved with medication'),
(7, 'Hypothyroidism', '2020-12-30', 'On levothyroxine treatment'),
(8, 'Anemia', '2024-04-10', 'Iron supplements prescribed'),
(9, 'Osteoarthritis', '2023-08-05', 'Joint pain management ongoing'),
(10, 'Chronic Kidney Disease', '2022-06-12', 'Under specialist care');

select * from MedicalHistory

INSERT INTO LabTests (patients_id, doctors_id, test_name, test_date, result, status)
VALUES
(1, 1, 'Complete Blood Count', '2025-06-01', 'Normal', 'Completed'),
(2, 2, 'Liver Function Test', '2025-06-02', 'Slightly Elevated ALT', 'Completed'),
(3, 3, 'Chest X-Ray', '2025-06-03', 'No Abnormalities', 'Completed'),
(4, 1, 'Blood Glucose', '2025-06-05', 'Fasting glucose 105 mg/dL', 'Completed'),
(5, 4, 'Urinalysis', '2025-06-06', 'Normal', 'Completed'),
(6, 2, 'Electrolytes Panel', '2025-06-07', 'Sodium low at 130 mmol/L', 'Completed'),
(7, 3, 'Thyroid Panel', '2025-06-08', 'TSH normal', 'Completed'),
(8, 4, 'MRI Brain', '2025-06-09', 'No lesions detected', 'Completed'),
(9, 1, 'Vitamin D Test', '2025-06-10', 'Deficient', 'Completed'),
(10, 2, 'COVID-19 PCR', '2025-06-11', 'Negative', 'Completed');

select * from LabTests


-- =====
-- SECTION 1: BASIC SQL QUERIES
-- Description: Foundational SQL commands to explore the healthcare database
-- ======

-- 1.1. View all patients
SELECT * FROM Patients;

-- 1.2. View only the names and contact details of all doctors
SELECT doctors_name, contact_info FROM Doctors;

-- 1.3. List all appointments scheduled
SELECT * FROM Appointments;

-- 1.4. Show all female patients
SELECT * FROM Patients WHERE gender = 'F';

-- 1.5. Show all completed lab tests
SELECT * FROM LabTests WHERE status = 'Completed';

-- 1.6. View appointments ordered by date
SELECT * FROM Appointments
ORDER BY appointment_date ASC;

-- 1.7. View all doctors who specialize in 'Oncology'
SELECT * FROM Doctors WHERE speciality = 'Oncology';

-- 1.8. List the names of all patients with their IDs
SELECT patients_id, name FROM Patients;

-- 1.9. View the lab results of a patient with ID = 3
SELECT * FROM LabResults WHERE patients_id = 3;

-- 1.10. Find all appointments scheduled after June 15, 2025
SELECT * FROM Appointments WHERE appointment_date > '2025-06-15';

-- 1.11. To view the top 5 rows
SELECT TOP 5 * FROM Patients;

-- 1.12. View patient names with alias(temporary_name_displayed)
SELECT name AS 'Patient Name', dob AS 'Date of Birth' FROM Patients;



-- =====
-- SECTION 2: INTERMEDIATE SQL QUERIES
-- Description: Aggregation, Grouping, Joins
-- ======

-- Secction 2.1: Filtering with Multiple Conditions (Advanced WHERE Clause)
-- 2.1.1. Patients older than 30 AND male
SELECT * 
FROM Patients
WHERE age > 30 AND gender = 'M';

-- 2.1.2. Patients who are either female OR 38 OR younger
SELECT * 
FROM Patients
WHERE gender = 'Female' OR age <= 38;

-- 2.1.3. Patients aged between 30 and 50 (inclusive)
SELECT * 
FROM Patients
WHERE age BETWEEN 30 AND 50;

-- 2.1.4. Patients with gender either 'Male'
SELECT * 
FROM Patients
WHERE gender IN ('M');

-- 2.1.5. Patients whose gender is NOT 'Female'
SELECT * 
FROM Patients
WHERE gender NOT IN ('M');

-- 2.1.6. Patients whose name starts with the letter 'A'
SELECT * 
FROM Patients
WHERE name LIKE 'A%';

-- 2.1.7. Patients whose name ends with 'ola'
SELECT * 
FROM Patients
WHERE name LIKE '%ola';

-- 2.1.8. Patients whose name contains 'ol'
SELECT * 
FROM Patients
WHERE name LIKE '%ol%';

-- 2.1.9. Patients whose contact information is missing (NULL)
SELECT * 
FROM Patients
WHERE contact_info IS NULL;

-- 2.2.1. Count how many male and female patients exist
SELECT gender, COUNT(*) AS gender_count
FROM Patients
GROUP BY gender;

-- 2.2.2. Average patient age
SELECT AVG(age) AS average_age
FROM Patients;

-- 2.2.3. Oldest and youngest patients
SELECT MAX(age) AS oldest_age, MIN(age) AS youngest_age
FROM Patients;

-- 2.2.4. Total number of completed lab tests
SELECT COUNT(*) AS completed_tests
FROM LabTests
WHERE status = 'Completed';

-- 2.2.5. Number of patients per age
SELECT age, COUNT(*) AS patients_per_age
FROM Patients
GROUP BY age
ORDER BY age ASC;

-- 2.2.6. Number of appointments per doctor
SELECT doctors_id, COUNT(*) AS appointment_count
FROM Appointments
GROUP BY doctors_id;

-- 2.2.7. Total billing amount per patient
SELECT patients_id, SUM(amount) AS totat_billing
FROM Billing
GROUP BY patients_id;


--  SECTION 2.3: JOINS

--  2.3.1. INNER JOIN: Patients with Their Appointments
SELECT 
    Patients.patients_id,
    Patients.name,
    Appointments.appointment_id,
    Appointments.appointment_date
FROM Patients
INNER JOIN Appointments 
    ON Patients.patients_id = Appointments.patients_id;

-- 2.3.2. LEFT JOIN to include all patients for finding patients without billing
SELECT 
    Patients.patients_id,
    Patients.name,
    Appointments.appointment_date
FROM Patients
LEFT JOIN Appointments 
    ON Patients.patients_id = Appointments.patients_id;

-- 2.3.3. RIGHT JOIN to include all appointments
SELECT 
    Patients.name,
    Appointments.appointment_date
FROM Patients
RIGHT JOIN Appointments 
    ON Patients.patients_id = Appointments.patients_id;


-- 2.4.4. FULL JOIN to get unmatched data on both sides
SELECT 
    Patients.name,
    Appointments.appointment_date
FROM Patients
FULL JOIN Appointments 
    ON Patients.patients_id = Appointments.patients_id;

-- 2.4.5. CROSS JOIN to pair all patients with all doctors
SELECT 
    Patients.name AS patient_name,
    Doctors.doctors_name AS doctor_name
FROM Patients
CROSS JOIN Doctors;


-- SECTION 2.5: SUBQUERIES (NESTED QUERIES)
-- 2.5.1. Patients with at least one appointment
SELECT * 
FROM Patients
WHERE patients_id IN (
    SELECT patients_id FROM Appointments
);

-- 2.5.2. Each patient's appointment count
SELECT 
    name,
    (SELECT COUNT(*) 
     FROM Appointments 
     WHERE Appointments.patients_id = Patients.patients_id) AS appointment_count
FROM Patients;

-- 2.5.3. Average age of patients with appointments
SELECT AVG(age) AS avg_age_with_appointments
FROM (
    SELECT DISTINCT Patients.age
    FROM Patients
    INNER JOIN Appointments 
        ON Patients.patients_id = Appointments.patients_id
) AS Sub;

-- 2.5.4. Patients who have had at least one lab test
SELECT name  
FROM Patients  
WHERE patients_id IN (
    SELECT DISTINCT patients_id  
    FROM LabTests
);

-- 2.5.5. Doctors with more than 2 appointments
SELECT doctors_name  
FROM Doctors  
WHERE doctors_id IN (
    SELECT doctors_id  
    FROM Appointments  
    GROUP BY doctors_id  
    HAVING COUNT(*) > 2
);

-- 2.5.6. Patients older than the average age
SELECT name, age  
FROM Patients  
WHERE age > (
    SELECT AVG(age)  
    FROM Patients
);


-- SECTION 2.6: CASE STATEMENTS
-- 2.6.1. Categorize patients by age group
SELECT 
    name,
    age,
    CASE 
        WHEN age < 30 THEN 'young_adult'
        WHEN age BETWEEN 30 AND 55 THEN 'adult'
        ELSE 'senior'
    END AS age_group
FROM Patients;

-- 2.6.2. Flag appointment status as Past or Upcoming
SELECT 
    appointment_id,
    appointment_date,
    CASE 
        WHEN appointment_date < GETDATE() THEN 'Past'
        ELSE 'Upcoming'
    END AS appointment_status
FROM Appointments;

-- 2.6.3. Assign billing category based on amount
SELECT 
    billing_id,
    amount,
    CASE 
        WHEN amount < 15000 THEN 'Low'
        WHEN amount BETWEEN 15000 AND 20000 THEN 'Medium'
        ELSE 'High'
    END AS billing_category
FROM Billing;

-- =====
-- SECTION 3: ADVANCED SQL CONCEPTS
-- Description: Views, Stored Procedures, Triggers
-- =====
-- 3.1: Creating and Using Views
-- 3.1.1: Create a view for patient appointment summary
CREATE VIEW PatientLatestAppointment AS 
    p.patients_id,
    p.name,
    COUNT(a.appointment_id) AS total_appointments
FROM Patients p
LEFT JOIN Appointments a ON p.patients_id = a.patients_id
GROUP BY p.patients_id, p.name;

-- Section 3.1.2 – View: Patients with  highBilling
CREATE VIEW HighBillingPatients AS
SELECT 
    Patients.patients_id,
    Patients.name,
    Billing.amount
FROM Patients
JOIN Billing 
    ON Patients.patients_id = Billing.patients_id
WHERE Billing.amount > 17000;
-- to view patients with high billing
select * from HighBillingPatients

--  Section 3.1.3 – View: Appointments with Doctor and Patient Names
CREATE VIEW AppointmentSummary AS
SELECT 
    Appointments.appointment_id,
    Appointments.appointment_date,
    Patients.name AS patient_name,
    Doctors.doctors_name AS doctor_name
FROM Appointments
JOIN Patients 
    ON Appointments.patients_id = Patients.patients_id
JOIN Doctors 
    ON Appointments.doctors_id = Doctors.doctors_id;
-- to view patients appointment summary
select * from AppointmentSummary

-- Section 3.1.4 – View: Total Billing per Patient
CREATE VIEW PatientBillingSummary AS
SELECT 
    patients_id,
    SUM(amount) AS total_billing
FROM Billing
GROUP BY patients_id;
-- to view the patients billing summary
SELECT * FROM PatientBillingSummary;


-- SECTION 3.2: INDEXES
-- 3.2.1. Creating a Simple Index
CREATE INDEX idx_patient_name  
ON Patients(name);

SELECT * FROM Patients WHERE name = 'Bukola Olawale';

-- 3.2.2. Creating a Composite Index (Multi-column)
CREATE INDEX idx_gender_age  
ON Patients(gender, age);

SELECT * FROM Patients WHERE gender = 'M' AND age = 31;


-- 3.2.3. Viewing Existing Indexes (SQL Server)
SELECT *  
FROM sys.indexes  
WHERE object_id = OBJECT_ID('Patients');

-- SECTION 3.3: Stored Procedures
-- 3.3.1. Creating a Simple Stored Procedure
CREATE PROCEDURE GetAllPatients
AS
BEGIN
    SELECT * FROM Patients;
END;

-- 3.3.2. Stored Procedure with Parameter
CREATE PROCEDURE GetPatientByID @ID INT
AS
BEGIN
    SELECT * FROM Patients WHERE patients_id = @ID;
END;


-- 3.4: Triggers
-- 3.4.1. Trigger to Log Whenever a New Patient is Added
-- Step 1: Create a logging table
CREATE TABLE PatientLog (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    patients_id INT,
    name VARCHAR(100),
    created_at DATETIME DEFAULT GETDATE()
);

-- Step 2: Create the trigger
CREATE TRIGGER trg_InsertPatient
ON Patients
AFTER INSERT
AS
BEGIN
    INSERT INTO PatientLog (patients_id, name)
    SELECT patients_id, name
    FROM inserted;
END;


-- 3.4.2. Trigger to Track Billing Updates
-- Create a table to log changes
CREATE TABLE BillingAudit (
    audit_id INT IDENTITY(1,1),
    billing_id INT,
    old_amount DECIMAL(10,2),
    new_amount DECIMAL(10,2),
    updated_at DATETIME DEFAULT GETDATE()
);

-- Create the trigger
CREATE TRIGGER trg_UpdateBilling
ON Billing
AFTER UPDATE
AS
BEGIN
    INSERT INTO BillingAudit (billing_id, old_amount, new_amount)
    SELECT d.billing_id, d.amount, i.amount
    FROM deleted d
    JOIN inserted i ON d.billing_id = i.billing_id
    WHERE d.amount <> i.amount;
END;
















