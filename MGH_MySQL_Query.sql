/*PROJECT DISCLAIMER:
The following SQL represents the finalized transformation pipeline used to construct the
analytics-ready flat table. All intermediate or redundant commands executed during early 
data exploration, debugging, schema fixes, and transformation stages have been excluded 
to maintain readability and ensure this script reflects the final production logic only.*/

-- Create database.

CREATE DATABASE hospital_analytics;

-- Activate database.

USE hospital_analytics;

-- Create tables inside database to import external CSVs.

CREATE TABLE fact_procedures (
    start_dt TEXT,
    stop_dt TEXT,
    patient VARCHAR(64),
    encounter VARCHAR(64),
    code VARCHAR(32),
    description TEXT,
    base_cost TEXT,
    reason_code VARCHAR(32),
    reason_description TEXT
);

-- Change MySQL settings to import external CSVs.

SET GLOBAL local_infile = 1;

SHOW VARIABLES LIKE 'local_infile';

-- Import data into created table.

LOAD DATA LOCAL INFILE
'C:/Users/HP/Desktop/Hospital_Analytics_Project/procedures.csv'
INTO TABLE fact_procedures
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Checking if all rows are correctly imported.

SELECT COUNT(*) FROM fact_procedures;

-- Now repeating above steps for other 4 tables.

CREATE TABLE fact_encounters (
    encounter_id VARCHAR(64),
    start_dt TEXT,
    stop_dt TEXT,
    patient_id VARCHAR(64),
    organization_id VARCHAR(64),
    payer_id VARCHAR(64),
    provider_id VARCHAR(64),
    encounter_class VARCHAR(50),
    code VARCHAR(32),
    description TEXT,
    base_encounter_cost TEXT,
    total_claim_cost TEXT,
    payer_coverage TEXT,
    reason_code VARCHAR(32),
    reason_description TEXT
);

LOAD DATA LOCAL INFILE
'C:/Users/HP/Desktop/Hospital_Analytics_Project/encounters.csv'
INTO TABLE fact_encounters
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM fact_encounters;

CREATE TABLE dim_patients (
    patient_id VARCHAR(64) PRIMARY KEY,
    birthdate DATE,
    deathdate DATE,
    prefix VARCHAR(10),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    suffix VARCHAR(20),
    maiden_name VARCHAR(100),
    marital_status VARCHAR(20),
    race VARCHAR(50),
    ethnicity VARCHAR(50),
    gender VARCHAR(20),
    birthplace VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    county VARCHAR(100),
    zip VARCHAR(20),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6)
);

LOAD DATA LOCAL INFILE 'C:/Users/HP/Desktop/Hospital_Analytics_Project/patients.csv'
INTO TABLE dim_patients
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM dim_patients;

CREATE TABLE dim_payers (
    payer_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state_headquartered VARCHAR(10),
    zip VARCHAR(20),
    phone VARCHAR(50)
);

LOAD DATA LOCAL INFILE 'C:/Users/HP/Desktop/Hospital_Analytics_Project/payers.csv'
INTO TABLE dim_payers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM dim_payers;

CREATE TABLE dim_organizations (
    organization_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(200),
    address VARCHAR(200),
    city VARCHAR(100),
    state_headquartered VARCHAR(10),
    zip VARCHAR(20),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6)
);

LOAD DATA LOCAL INFILE 'C:/Users/HP/Desktop/Hospital_Analytics_Project/organizations.csv'
INTO TABLE dim_organizations
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM dim_organizations;

-- In a few tables above we use TEXT instead of VARCHAR to make it easy to load data into columns.

-- Checking datatypes before altering them.

DESCRIBE fact_procedures;

UPDATE fact_procedures
SET 
    start_dt = REPLACE(REPLACE(start_dt, 'T', ' '), 'Z', ''),
    stop_dt  = REPLACE(REPLACE(stop_dt,  'T', ' '), 'Z', '');

-- Altering datatypes.

ALTER TABLE fact_procedures
    MODIFY start_dt DATETIME,
    MODIFY stop_dt DATETIME,
    MODIFY patient VARCHAR(50),
    MODIFY encounter VARCHAR(50),
    MODIFY code VARCHAR(20),
    MODIFY description VARCHAR(255),
    MODIFY base_cost INT,
    MODIFY reason_code VARCHAR(20),
    MODIFY reason_description VARCHAR(255);
    
UPDATE fact_procedures
SET 
    description = TRIM(REPLACE(REPLACE(description, CHAR(13), ''), CHAR(10), '')),
    reason_description = TRIM(REPLACE(REPLACE(reason_description, CHAR(13), ''), CHAR(10), ''));
    
ALTER TABLE fact_procedures
	CHANGE patient patient_id VARCHAR(50),
	CHANGE encounter encounter_id VARCHAR(50);

DESCRIBE fact_procedures;

-- Rechecking dataypes above.

-- Now we repeat steps for other 4 tables

DESCRIBE fact_encounters;

UPDATE fact_encounters
SET 
    start_dt = REPLACE(REPLACE(start_dt, 'T', ' '), 'Z', ''),
    stop_dt  = REPLACE(REPLACE(stop_dt,  'T', ' '), 'Z', '');
    
-- As the column names are misaligned we need to align them to the left by one using below query. This happens in certain cases when importing CSVs.

ALTER TABLE fact_encounters
MODIFY encounter_id VARCHAR(50),
MODIFY start_dt DATETIME,
MODIFY stop_dt DATETIME,
MODIFY patient_id VARCHAR(50),
MODIFY organization_id VARCHAR(50),
MODIFY payer_id VARCHAR(50),
CHANGE provider_id A VARCHAR(50),
CHANGE encounter_class B VARCHAR(20),
CHANGE code C VARCHAR(255),
CHANGE description D DECIMAL(10,2),
CHANGE base_encounter_cost E  DECIMAL(10,2),
CHANGE total_claim_cost F DECIMAL(10,2),
CHANGE payer_coverage G VARCHAR(20),
CHANGE reason_code H VARCHAR(255),
DROP COLUMN reason_description ;

ALTER TABLE fact_encounters
CHANGE A encounter_class VARCHAR(50),
CHANGE B code VARCHAR(20),
CHANGE C description VARCHAR(255),
CHANGE D base_encounter_cost DECIMAL(10,2),
CHANGE E total_claim_cost DECIMAL(10,2),
CHANGE F payer_coverage DECIMAL(10,2),
CHANGE G reason_code VARCHAR(20),
CHANGE H reason_description VARCHAR(255);

UPDATE fact_encounters
SET 
    description = TRIM(REPLACE(REPLACE(description, CHAR(13), ''), CHAR(10), '')),
    reason_description = TRIM(REPLACE(REPLACE(reason_description, CHAR(13), ''), CHAR(10), ''));

DESCRIBE fact_encounters;

DESCRIBE dim_patients;

-- No altering of datatypes required. We only need to change certain dates which are "0000-00-00' to nulls.

SET sql_mode = '';
 
UPDATE dim_patients
SET birthdate = NULL
WHERE birthdate = '0000-00-00';

UPDATE dim_patients
SET deathdate = NULL
WHERE deathdate = '0000-00-00';

UPDATE dim_patients
SET 
    prefix = TRIM(prefix),
    first_name = TRIM(first_name),
    last_name = TRIM(last_name),
    maiden_name = TRIM(maiden_name),
    marital_status = TRIM(marital_status),
    race = TRIM(race),
    ethnicity = TRIM(ethnicity),
    gender = TRIM(gender),
    birthplace = TRIM(REPLACE(REPLACE(birthplace, CHAR(13), ''), CHAR(10), '')),
    address = TRIM(REPLACE(REPLACE(address, CHAR(13), ''), CHAR(10), '')),
    city = TRIM(city),
    state = TRIM(state),
    county = TRIM(county),
    zip = TRIM(zip);

DESCRIBE dim_payers;

-- Below we remove invisible characters from numbers such as Unix and Windows style line breaks \n, \r.

UPDATE dim_payers
SET phone = TRIM(REPLACE(REPLACE(phone, CHAR(13), ''), CHAR(10), ''));

DESCRIBE dim_organizations;

-- Now we check for nulls and join consistency one table at a time.

SELECT 
    SUM(patient_id IS NULL) AS null_patient,
    SUM(encounter_id IS NULL) AS null_encounter,
    SUM(base_cost IS NULL) AS missing_costs
FROM fact_procedures;

-- OR

/* SELECT
    COUNT(*) - COUNT(patient_id) AS null_patient,
    COUNT(*) - COUNT(encounter_id) AS null_encounter,
    COUNT(*) - COUNT(base_cost) AS missing_costs
FROM fact_procedures; */


-- Above code should return 0s

SELECT COUNT(*) AS missing_encounters
FROM fact_procedures p
LEFT JOIN fact_encounters e
  ON p.encounter_id = e.encounter_id
WHERE e.encounter_id IS NULL;

-- Above code should return 0s

SELECT COUNT(*) AS missing_patients
FROM fact_procedures p
LEFT JOIN dim_patients ps
  ON p.patient_id = ps.patient_id
WHERE ps.patient_id IS NULL;

-- Above code should return 0s

SELECT 
    SUM(encounter_id IS NULL) AS null_id,
    SUM(patient_id IS NULL) AS null_patients,
    SUM(organization_id IS NULL) AS null_orgs,
    SUM(payer_id IS NULL) AS null_payers,
    SUM(start_dt IS NULL) AS null_start,
    SUM(stop_dt IS NULL) AS null_stop
FROM fact_encounters;

-- Above code should return 0s

SELECT COUNT(*) AS missing_patients
FROM fact_encounters e
LEFT JOIN dim_patients p 
ON e.patient_id = p.patient_id
WHERE p.patient_id IS NULL;

-- Above code should return 0s

SELECT COUNT(*) AS missing_orgs
FROM fact_encounters e
LEFT JOIN dim_organizations o 
ON e.organization_id = o.organization_id
WHERE o.organization_id IS NULL;

-- Above code should return 0s

SELECT COUNT(*) AS missing_payers
FROM fact_encounters e
LEFT JOIN dim_payers py 
ON e.payer_id = py.payer_id
WHERE py.payer_id IS NULL;

-- Above code should return 0s

SELECT 
    COUNT(*) AS total_rows,
    SUM(base_encounter_cost IS NULL) AS null_base_costs,
    SUM(total_claim_cost IS NULL) AS null_total_costs
FROM fact_encounters;

-- Above code should return 0s

SELECT 
    SUM(birthdate IS NULL) AS null_births,
    SUM(deathdate IS NULL) AS null_deaths,
    SUM(city IS NULL OR city='') AS null_city,
    SUM(state IS NULL OR state='') AS null_state,
    SUM(zip IS NULL OR zip='') AS null_zip
FROM dim_patients;

-- Above code should return 0s in all except deathdate & zip
    
UPDATE dim_payers
SET 
    name = NULLIF(TRIM(name), ''),
    address = NULLIF(TRIM(address), ''),
    city = NULLIF(TRIM(city), ''),
    state_headquartered = NULLIF(TRIM(state_headquartered), ''),
    zip = NULLIF(TRIM(zip), ''),
    phone = NULLIF(TRIM(phone), '');
    
-- Now we define PK and FK relationships before building the ERD (entity relationship diagram)

ALTER TABLE fact_encounters
ADD PRIMARY KEY (encounter_id);

ALTER TABLE fact_procedures
ADD CONSTRAINT fk_proc_pat
FOREIGN KEY (patient_id) REFERENCES dim_patients(patient_id),
ADD CONSTRAINT fk_proc_enc
FOREIGN KEY(encounter_id) REFERENCES fact_encounters(encounter_id);

ALTER TABLE fact_encounters
ADD CONSTRAINT fk_enc_pat
FOREIGN KEY (patient_id) REFERENCES dim_patients(patient_id),
ADD CONSTRAINT fk_enc_pay
FOREIGN KEY (payer_id) REFERENCES dim_payers(payer_id),
ADD CONSTRAINT fk_enc_org
FOREIGN KEY (organization_id) REFERENCES dim_organizations(organization_id);

-- ERD was created and downloaded successfully. Now we make our final flat table for analyzing in BI tool.

CREATE TABLE hospital_data AS
SELECT
-- FACT_PROCEDURES (keep all)
    pr.start_dt AS procedure_start_dt,
    pr.stop_dt AS procedure_stop_dt,
    pr.patient_id,
    pr.encounter_id,
    pr.code AS procedure_code,
    pr.description AS procedure_description,
    pr.base_cost AS procedure_base_cost,
    pr.reason_code AS procedure_reason_code,
    pr.reason_description AS procedure_reason_description,

-- FACT_ENCOUNTERS (drop duplicate patient_id & encounter_id only)
    en.start_dt AS encounter_start_dt,
    en.stop_dt AS encounter_stop_dt,
    en.organization_id,
    en.payer_id,
    en.encounter_class,
    en.code AS encounter_code,
    en.description AS encounter_description,
    en.base_encounter_cost,
    en.total_claim_cost,
    en.payer_coverage,
    en.reason_code AS encounter_reason_code,
    en.reason_description AS encounter_reason_description,

-- DIM_PATIENTS (drop patient_id, keep rest)
    pa.birthdate,
    pa.deathdate,
    pa.prefix,
    pa.first_name,
    pa.last_name,
    pa.suffix,
    pa.maiden_name,
    pa.marital_status,
    pa.race,
    pa.ethnicity,
    pa.gender,
    pa.birthplace,
    pa.address AS patient_address,
    pa.city AS patient_city,
    pa.state AS patient_state,
    pa.county AS patient_county,
    pa.zip AS patient_zip,
    pa.latitude AS patient_latitude,
    pa.longitude AS patient_longitude,

-- DIM_PAYERS (drop payer_id, keep rest) 
    py.name AS payer_name,
    py.address AS payer_address,
    py.city AS payer_city,
    py.state_headquartered AS payer_state_headquartered,
    py.zip AS payer_zip,
    py.phone AS payer_phone,

-- DIM_ORGANIZATIONS (drop organization_id, keep rest)
    org.name AS organization_name,
    org.address AS organization_address,
    org.city AS organization_city,
    org.state_headquartered AS organization_state_headquartered,
    org.zip AS organization_zip,
    org.latitude AS organization_latitude,
    org.longitude AS organization_longitude

FROM fact_procedures pr
LEFT JOIN fact_encounters en
    ON pr.encounter_id = en.encounter_id
LEFT JOIN dim_patients pa
    ON pr.patient_id = pa.patient_id
LEFT JOIN dim_payers py
    ON en.payer_id = py.payer_id
LEFT JOIN dim_organizations org
    ON en.organization_id = org.organization_id;
    
-- Now we validate the flat table.

DESCRIBE hospital_data;

-- Row count check. Rows should be equal to rows of table with max granularity. 

SELECT COUNT(*) FROM hospital_data;
SELECT COUNT(*) FROM fact_procedures;

-- Checking for nulls.

SELECT 
    SUM(CASE WHEN encounter_start_dt IS NULL THEN 1 ELSE 0 END) AS missing_encounters,
    SUM(CASE WHEN payer_name IS NULL THEN 1 ELSE 0 END) AS missing_payers,
    SUM(CASE WHEN organization_name IS NULL THEN 1 ELSE 0 END) AS missing_orgs,
    SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS missing_patients
FROM hospital_data;

-- Checking 20 random rows.

SELECT * 
FROM hospital_data
ORDER BY RAND() 
LIMIT 20;

-- Saving a backup

CREATE TABLE backup_hospital_data AS
SELECT*FROM hospital_data;

-- Export the flat using export feature in the result grid.

SELECT*FROM hospital_data;


















