--------------------------------------------------------------
-- Crime Data Mart Star Schema Creation Script (Corrected)
--------------------------------------------------------------

-- ----------------------
-- Drop Existing Tables
-- ----------------------
DROP TABLE fact_crime_resolution CASCADE CONSTRAINTS;
DROP TABLE dim_crimetype CASCADE CONSTRAINTS;
DROP TABLE dim_location CASCADE CONSTRAINTS;
DROP TABLE dim_officer CASCADE CONSTRAINTS;
DROP TABLE dim_date CASCADE CONSTRAINTS;

--------------------------------------------------------------
-- Dimension Tables
--------------------------------------------------------------

-- Dim_Date
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    data_source VARCHAR(50)
);

-- Dim_Officer
CREATE TABLE dim_officer (
    officer_id INT PRIMARY KEY,
    officer_name VARCHAR2(100) NOT NULL,
    officer_rank VARCHAR2(50),
    department VARCHAR2(50),
    gender VARCHAR(20),
    data_source VARCHAR2(50)
);

-- Dim_Location
CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    station_name VARCHAR(100),
    city VARCHAR(50),
    region_id INT,
    data_source VARCHAR(50)
);

-- Dim_CrimeType
CREATE TABLE dim_crimetype (
    crime_type_id INT PRIMARY KEY,
    crime_type VARCHAR(100),
    severity_level VARCHAR(50),
    status VARCHAR(20),
    data_source VARCHAR(50)
);

--------------------------------------------------------------
-- Fact Table
--------------------------------------------------------------
CREATE TABLE fact_crime_resolution( 
    crime_resolution_key INT PRIMARY KEY,
    reported_crime_id INT NOT NULL,
    fk1_officer_id INT NOT NULL,
    fk2_crime_type_id INT NOT NULL,
    fk3_date_id INT NOT NULL,
    fk4_location_id INT NOT NULL,
    days_to_close INT
);

--------------------------------------------------------------
-- Foreign Key Constraints
--------------------------------------------------------------

ALTER TABLE fact_crime_resolution
ADD CONSTRAINT fk1_fact_to_dim_officer 
FOREIGN KEY (fk1_officer_id)
REFERENCES dim_officer(officer_id);

ALTER TABLE fact_crime_resolution
ADD CONSTRAINT fk2_fact_to_dim_crimetype 
FOREIGN KEY (fk2_crime_type_id)
REFERENCES dim_crimetype(crime_type_id);

ALTER TABLE fact_crime_resolution
ADD CONSTRAINT fk3_fact_to_dim_date 
FOREIGN KEY (fk3_date_id)
REFERENCES dim_date(date_id);

ALTER TABLE fact_crime_resolution
ADD CONSTRAINT fk4_fact_to_dim_location 
FOREIGN KEY (fk4_location_id)
REFERENCES dim_location(location_id);

--------------------------------------------------------------
-- Sequences
--------------------------------------------------------------
DROP SEQUENCE seq_dim_date;
DROP SEQUENCE seq_dim_officer;
DROP SEQUENCE seq_dim_location;
DROP SEQUENCE seq_dim_crimetype;
DROP SEQUENCE seq_fact_crime_resolution;

CREATE SEQUENCE seq_dim_date START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_dim_officer START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_dim_location START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_dim_crimetype START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_fact_crime_resolution START WITH 1 INCREMENT BY 1;

--------------------------------------------------------------
-- Triggers for Auto PK Insert
--------------------------------------------------------------

CREATE OR REPLACE TRIGGER trg_date_pk
BEFORE INSERT ON dim_date
FOR EACH ROW
BEGIN
    IF :NEW.date_id IS NULL THEN
        :NEW.date_id := seq_dim_date.NEXTVAL;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_officer_pk
BEFORE INSERT ON dim_officer
FOR EACH ROW
BEGIN
    IF :NEW.officer_id IS NULL THEN
        :NEW.officer_id := seq_dim_officer.NEXTVAL;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_location_pk
BEFORE INSERT ON dim_location
FOR EACH ROW
BEGIN
    IF :NEW.location_id IS NULL THEN
        :NEW.location_id := seq_dim_location.NEXTVAL;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_crimetype_pk
BEFORE INSERT ON dim_crimetype
FOR EACH ROW
BEGIN
    IF :NEW.crime_type_id IS NULL THEN
        :NEW.crime_type_id := seq_dim_crimetype.NEXTVAL;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_fact_pk
BEFORE INSERT ON fact_crime_resolution
FOR EACH ROW
BEGIN
    IF :NEW.crime_resolution_key IS NULL THEN
        :NEW.crime_resolution_key := seq_fact_crime_resolution.NEXTVAL;
    END IF;
END;
/



CREATE SEQUENCE seq_fact_crime_resolution
START WITH 1
INCREMENT BY 1
NOCACHE
NOCYCLE;