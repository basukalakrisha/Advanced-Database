------------------------------------------------------------
-- 1. Drop old tables/sequences if they exist
------------------------------------------------------------
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE trf_crime_register CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE etl_log_cleaning CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE seq_log_cleaning';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -2289 THEN RAISE; END IF; END;
/

------------------------------------------------------------
-- 2. Create Transformation Table
------------------------------------------------------------
CREATE TABLE trf_crime_register (
    trf_crime_register_id NUMBER PRIMARY KEY,
    reported_date         DATE,
    officer_id            NUMBER,
    location_id           NUMBER,
    crime_type_id         NUMBER,
    crime_status          VARCHAR2(20),
    days_to_close         NUMBER,
    data_source           VARCHAR2(50),
    error_reason          VARCHAR2(400)
);

------------------------------------------------------------
-- 3. Create ETL Log Table and Sequence
------------------------------------------------------------
CREATE TABLE etl_log_cleaning (
    log_id        NUMBER PRIMARY KEY,
    trf_id        NUMBER,
    source_table  VARCHAR2(50),
    error_reason  VARCHAR2(400),
    processed_at  DATE DEFAULT SYSDATE
);

CREATE SEQUENCE seq_log_cleaning
START WITH 1
INCREMENT BY 1
NOCACHE
NOCYCLE;

------------------------------------------------------------
-- 4. Transform / Clean Bad Data
------------------------------------------------------------
INSERT INTO trf_crime_register (
    trf_crime_register_id,
    reported_date,
    officer_id,
    location_id,
    crime_type_id,
    crime_status,
    days_to_close,
    data_source,
    error_reason
)
SELECT
    sr.stg_crime_register_id,
    NVL(sr.reported_date, DATE '1900-01-01') AS reported_date,
    sr.officer_id,
    sr.location_id,
    sr.crime_type_id,
    CASE sr.is_closed
         WHEN 1 THEN 'CLOSED'
         ELSE 'OPEN'
    END AS crime_status,
    NVL(sr.days_to_close, 0) AS days_to_close,
    sr.data_source,
    RTRIM(
        CASE WHEN sr.reported_date IS NULL THEN 'Missing reported_date; ' ELSE NULL END ||
        CASE WHEN sr.officer_id IS NULL THEN 'Missing officer_id; ' ELSE NULL END ||
        CASE WHEN sr.location_id IS NULL THEN 'Missing location_id; ' ELSE NULL END ||
        CASE WHEN sr.crime_type_id IS NULL THEN 'Missing crime_type_id; ' ELSE NULL END
    ) AS error_reason
FROM stg_crime_register_bad sr;

COMMIT;

------------------------------------------------------------
-- 5. Log Remaining Errors
------------------------------------------------------------
INSERT INTO etl_log_cleaning (log_id, trf_id, source_table, error_reason)
SELECT seq_log_cleaning.NEXTVAL, trf_crime_register_id, 'STG_CRIME_REGISTER', error_reason
FROM trf_crime_register
WHERE error_reason IS NOT NULL;

COMMIT;

------------------------------------------------------------
-- 6. Quick Check
------------------------------------------------------------
SELECT 'Transformed Crime Register', COUNT(*) FROM trf_crime_register
UNION ALL
SELECT 'ETL Log Cleaning', COUNT(*) FROM etl_log_cleaning;
