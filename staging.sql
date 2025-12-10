-- Dropping staging tables

DROP TABLE STG_CRIME_TYPE CASCADE CONSTRAINTS;
DROP TABLE STG_LOCATION CASCADE CONSTRAINTS;
DROP TABLE STG_OFFICER CASCADE CONSTRAINTS;
DROP TABLE STG_CRIME_REGISTER CASCADE CONSTRAINTS;



-- Creating statging tables

CREATE TABLE stg_crime_register (
    crime_id            NUMBER,
    reported_date       DATE,
    location_id         NUMBER,
    officer_id          NUMBER,
    crime_type_id       NUMBER,
    is_closed           NUMBER,
    days_to_close       NUMBER,
    data_source         VARCHAR2(50)
);

CREATE TABLE stg_officer (
    officer_id          NUMBER,
    officer_name        VARCHAR2(100),
    officer_rank        VARCHAR2(50),
    department          VARCHAR2(50),
    gender              VARCHAR2(10),
    data_source         VARCHAR2(50)

);

CREATE TABLE stg_location (
    location_id         NUMBER,
    station_name        VARCHAR2(100),
    region_id           NUMBER,
    city                VARCHAR2(50),
    data_source         VARCHAR2(50)
);

CREATE TABLE stg_crime_type (
    crime_type_id       NUMBER,
    crime_type          VARCHAR2(100),
    severity_level      VARCHAR2(50),
    data_source         VARCHAR2(50)
    
);


------------------------------------------------------------
-- 1. Drop Existing Sequences (if exist)
------------------------------------------------------------
DROP SEQUENCE seq_stg_crime_register;
DROP SEQUENCE seq_stg_officer;
DROP SEQUENCE seq_stg_location;
DROP SEQUENCE seq_stg_crime_type;


------------------------------------------------------------
-- 2. Create New Sequences
------------------------------------------------------------
CREATE SEQUENCE seq_stg_crime_register START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_stg_officer START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_stg_location START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE seq_stg_crime_type START WITH 1 INCREMENT BY 1;

------------------------------------------------------------
-- 3. Ensure Staging Tables Have Surrogate Key Columns
------------------------------------------------------------
ALTER TABLE stg_crime_register ADD (stg_crime_register_id NUMBER);
ALTER TABLE stg_officer ADD (stg_officer_id NUMBER);
ALTER TABLE stg_location ADD (stg_location_id NUMBER);
ALTER TABLE stg_crime_type ADD (stg_crime_type_id NUMBER);

------------------------------------------------------------
-- 4. Add Primary Keys 
------------------------------------------------------------
ALTER TABLE stg_crime_register ADD CONSTRAINT pk_stg_crime_register PRIMARY KEY (stg_crime_register_id);
ALTER TABLE stg_officer ADD CONSTRAINT pk_stg_officer PRIMARY KEY (stg_officer_id);
ALTER TABLE stg_location ADD CONSTRAINT pk_stg_location PRIMARY KEY (stg_location_id);
ALTER TABLE stg_crime_type ADD CONSTRAINT pk_stg_crime_type PRIMARY KEY (stg_crime_type_id);

------------------------------------------------------------
-- 5. Create Triggers for Auto PK Generation
------------------------------------------------------------

------------------------------------------------------------
-- Trigger for STG_CRIME_REGISTER
------------------------------------------------------------
CREATE OR REPLACE TRIGGER trg_stg_crime_register_pk
BEFORE INSERT ON stg_crime_register
FOR EACH ROW
BEGIN
    IF :NEW.stg_crime_register_id IS NULL THEN
        :NEW.stg_crime_register_id := seq_stg_crime_register.NEXTVAL;
    END IF;
END;
/
------------------------------------------------------------

------------------------------------------------------------
-- Trigger for STG_OFFICER
------------------------------------------------------------
CREATE OR REPLACE TRIGGER trg_stg_officer_pk
BEFORE INSERT ON stg_officer
FOR EACH ROW
BEGIN
    IF :NEW.stg_officer_id IS NULL THEN
        :NEW.stg_officer_id := seq_stg_officer.NEXTVAL;
    END IF;
END;
/
------------------------------------------------------------

------------------------------------------------------------
-- Trigger for STG_LOCATION
------------------------------------------------------------
CREATE OR REPLACE TRIGGER trg_stg_location_pk
BEFORE INSERT ON stg_location
FOR EACH ROW
BEGIN
    IF :NEW.stg_location_id IS NULL THEN
        :NEW.stg_location_id := seq_stg_location.NEXTVAL;
    END IF;
END;
/
------------------------------------------------------------

------------------------------------------------------------
-- Trigger for STG_CRIME_TYPE
------------------------------------------------------------
CREATE OR REPLACE TRIGGER trg_stg_crime_type_pk
BEFORE INSERT ON stg_crime_type
FOR EACH ROW
BEGIN
    IF :NEW.stg_crime_type_id IS NULL THEN
        :NEW.stg_crime_type_id := seq_stg_crime_type.NEXTVAL;
    END IF;
END;
/

