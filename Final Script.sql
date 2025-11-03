-- ===================================================================================
-- Healthcare Data Warehouse Schema & ETL Procedure
-- Version: 4.0 (Production-Ready, Incremental)
-- Description: This script creates the star schema and a stored procedure
--              for an atomic, incremental load from the staging table.
--              THIS SCRIPT IS RUN ONCE.
-- ===================================================================================

-- Step 1: Create the database
DROP DATABASE IF EXISTS HealthcareADT_DW;
CREATE DATABASE HealthcareADT_DW;
USE HealthcareADT_DW;

-- -----------------------------------------------------------------------------------
-- Step 2: Create the Staging Table
-- Python loads the clean batch data here. So, no need for doing any cleaning in SQL.
-- -----------------------------------------------------------------------------------
DROP TABLE IF EXISTS Staging_Admissions;
CREATE TABLE Staging_Admissions (
    SourceAdmissionID VARCHAR(64) NOT NULL, -- SHA-256 hash from Python
    Name VARCHAR(255),
    Age INT,
    Gender VARCHAR(50),
    Blood_Type VARCHAR(10),
    Medical_Condition VARCHAR(255),
    Date_of_Admission DATE,
    Doctor VARCHAR(255),
    Hospital VARCHAR(255),
    Insurance_Provider VARCHAR(255),
    Billing_Amount DECIMAL(12,2),
    Room_Number VARCHAR(50),
    Admission_Type VARCHAR(255),
    Discharge_Date DATE,
    Medication VARCHAR(255),
    Test_Results VARCHAR(255),
    PRIMARY KEY (SourceAdmissionID) -- Good practice for staging lookups to ensure no duplicates exist
);

-- -----------------------------------------------------------------------------------
-- Step 3: Create Dimension Tables
-- -----------------------------------------------------------------------------------

DROP TABLE IF EXISTS DimPatients;
CREATE TABLE DimPatients (
  PatientID         INT PRIMARY KEY AUTO_INCREMENT,
  PatientSourceID   VARCHAR(255) NOT NULL, -- Name_Gender_BloodType
  PatientName       VARCHAR(255) NULL,
  Gender            VARCHAR(20) NOT NULL,
  BloodType         VARCHAR(10) NOT NULL,
  CONSTRAINT uq_DimPatients_PatientSourceID UNIQUE (PatientSourceID)
);

DROP TABLE IF EXISTS DimDoctors;
CREATE TABLE DimDoctors (
    DoctorID INT PRIMARY KEY AUTO_INCREMENT,
    DoctorName VARCHAR(255) UNIQUE
);

DROP TABLE IF EXISTS DimHospitals;
CREATE TABLE DimHospitals (
    HospitalID INT PRIMARY KEY AUTO_INCREMENT,
    HospitalName VARCHAR(255) UNIQUE
);

DROP TABLE IF EXISTS DimInsurance;
CREATE TABLE DimInsurance (
    InsuranceID INT PRIMARY KEY AUTO_INCREMENT,
    InsuranceProvider VARCHAR(255) UNIQUE
);

-- -----------------------------------------------------------------------------------
-- Step 4: Insert "Unknown" Members for Data Quality
-- -----------------------------------------------------------------------------------
INSERT INTO DimPatients (PatientID, PatientSourceID, PatientName, Gender, BloodType)
VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown');

INSERT INTO DimDoctors (DoctorID, DoctorName)
VALUES (-1, 'Unknown');

INSERT INTO DimHospitals (HospitalID, HospitalName)
VALUES (-1, 'Unknown');

INSERT INTO DimInsurance (InsuranceID, InsuranceProvider)
VALUES (-1, 'Unknown');

-- -----------------------------------------------------------------------------------
-- Step 5: Create the Fact Table (Incremental-Ready NOT full-refresh)
-- -----------------------------------------------------------------------------------
DROP TABLE IF EXISTS FactAdmissions;
CREATE TABLE FactAdmissions (
  AdmissionID        INT PRIMARY KEY AUTO_INCREMENT,
  SourceAdmissionID  VARCHAR(64) NOT NULL, -- The business key from Python
  
  -- Foreign Keys
  PatientID          INT NOT NULL DEFAULT -1,
  DoctorID           INT NOT NULL DEFAULT -1,
  HospitalID         INT NOT NULL DEFAULT -1,
  InsuranceID        INT NOT NULL DEFAULT -1,

  -- Core measures/attributes
  AdmissionDate      DATE NOT NULL,
  DischargeDate      DATE NULL,
  AdmissionType      VARCHAR(100),
  Medication         VARCHAR(100),
  TestResults        VARCHAR(100),
  RoomNumber         VARCHAR(50),
  BillingAmount      DECIMAL(12,2),
  MedicalCondition   VARCHAR(100),

  -- Generated analytical columns
  LengthOfStay       INT
    GENERATED ALWAYS AS (GREATEST(DATEDIFF(COALESCE(DischargeDate, AdmissionDate), AdmissionDate), 0)) STORED,
  AdmissionYear      INT     GENERATED ALWAYS AS (YEAR(AdmissionDate)) STORED,
  AdmissionQuarter   TINYINT GENERATED ALWAYS AS (QUARTER(AdmissionDate)) STORED,
  AdmissionMonth     TINYINT GENERATED ALWAYS AS (MONTH(AdmissionDate)) STORED,

  /* ---------- Constraints ---------- */
  -- This UNIQUE constraint is the core of the incremental strategy
  CONSTRAINT uq_SourceAdmissionID UNIQUE (SourceAdmissionID),
  
  CONSTRAINT fk_fact_patient   FOREIGN KEY (PatientID)  REFERENCES DimPatients (PatientID),
  CONSTRAINT fk_fact_doctor    FOREIGN KEY (DoctorID)   REFERENCES DimDoctors  (DoctorID),
  CONSTRAINT fk_fact_hospital  FOREIGN KEY (HospitalID) REFERENCES DimHospitals(HospitalID),
  CONSTRAINT fk_fact_insurance FOREIGN KEY (InsuranceID)REFERENCES DimInsurance(InsuranceID)
);

-- -----------------------------------------------------------------------------------
-- Step 6: Create the Atomic, Incremental ETL Stored Procedure
-- This procedure is called by Python.
-- It does NOT truncate the DWH tables.
-- -----------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_LoadDataWarehouse_Incremental;

DELIMITER $$

CREATE PROCEDURE sp_LoadDataWarehouse_Incremental()
BEGIN
    -- Declare an error handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL; -- Re-throws the error to the calling Python script
    END;

    -- START THE ATOMIC TRANSACTION
    START TRANSACTION;

    -- STEP 1: POPULATE DIMENSIONS
    INSERT IGNORE INTO DimDoctors (DoctorName)
    SELECT DISTINCT Doctor
    FROM Staging_Admissions
    WHERE Doctor IS NOT NULL AND Doctor != '';

    INSERT IGNORE INTO DimHospitals (HospitalName)
    SELECT DISTINCT Hospital
    FROM Staging_Admissions
    WHERE Hospital IS NOT NULL AND Hospital != '';

    INSERT IGNORE INTO DimInsurance (InsuranceProvider)
    SELECT DISTINCT Insurance_Provider
    FROM Staging_Admissions
    WHERE Insurance_Provider IS NOT NULL AND Insurance_Provider != '';

    -- STEP 2: POPULATE/UPDATE DimPatients (Slow Changing Dimension (SCD) Type 1)
    INSERT INTO DimPatients (PatientSourceID, PatientName, Gender, BloodType) 
    WITH RankedPatients AS (
        SELECT
            Name,
            Gender,
            Blood_Type,        -- <--- FIX 1: Use underscore (from Staging_Admissions)
            Date_of_Admission,
            ROW_NUMBER() OVER(
                PARTITION BY Name, Gender, Blood_Type -- <--- FIX 2: Use underscore
                ORDER BY Date_of_Admission DESC
            ) as rn
        FROM Staging_Admissions
        WHERE Name IS NOT NULL AND Gender IS NOT NULL AND Blood_Type IS NOT NULL
    )
    SELECT
        CONCAT_WS('_', Name, Gender, Blood_Type) AS PatientSourceID, -- <--- FIX 3: Use underscore
        Name AS PatientName,
        Gender,
        Blood_Type AS BloodType -- <--- FIX 4: Alias Blood_Type (source) to BloodType (target)
    FROM RankedPatients
    WHERE
        rn = 1
    ON DUPLICATE KEY UPDATE
        -- If PatientSourceID exists, update the attributes
        PatientName = VALUES(PatientName),
        Gender = VALUES(Gender),
        BloodType = VALUES(BloodType);


    -- STEP 3: POPULATE THE FACT TABLE (INCREMENTAL)
    INSERT IGNORE INTO FactAdmissions (
        SourceAdmissionID,
        PatientID, DoctorID, HospitalID, InsuranceID,
        AdmissionDate, DischargeDate, AdmissionType, Medication, TestResults,
        RoomNumber, BillingAmount, MedicalCondition
    )
    SELECT
        s.SourceAdmissionID,
        COALESCE(dp.PatientID, -1)   AS PatientID,
        COALESCE(dd.DoctorID, -1)    AS DoctorID,
        COALESCE(dh.HospitalID, -1)  AS HospitalID,
        COALESCE(di.InsuranceID, -1) AS InsuranceID,
        
        s.Date_of_Admission,
        s.Discharge_Date,
        s.Admission_Type,
        s.Medication,
        s.Test_Results,
        s.Room_Number,
        s.Billing_Amount,
        s.Medical_Condition
    FROM
        Staging_Admissions s
    -- Join to Dims to get the Surrogate Keys
    LEFT JOIN DimPatients dp ON dp.PatientSourceID = CONCAT_WS('_', s.Name, s.Gender, s.Blood_Type) -- <--- FIX 5: Use underscore
    LEFT JOIN DimDoctors dd ON dd.DoctorName = s.Doctor
    LEFT JOIN DimHospitals dh ON dh.HospitalName = s.Hospital
    LEFT JOIN DimInsurance di ON di.InsuranceProvider = s.Insurance_Provider
    WHERE
        s.Date_of_Admission IS NOT NULL;

    -- If all commands succeeded, commit the transaction
    COMMIT;

END$$

DELIMITER ;