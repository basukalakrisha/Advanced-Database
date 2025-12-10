-------------------------------------------------------------------
-- 0. Create dummy ETL log package (to avoid compilation errors)
-------------------------------------------------------------------
CREATE OR REPLACE PACKAGE etl_log_pkg AS
  FUNCTION start_process(p_msg VARCHAR2) RETURN NUMBER;
  PROCEDURE log_error(
    p_process_id   NUMBER,
    p_source_table VARCHAR2,
    p_source_key   VARCHAR2,
    p_error_msg    VARCHAR2,
    p_bad_data     VARCHAR2
  );
  PROCEDURE end_process(
    p_process_id   NUMBER,
    p_status       VARCHAR2,
    p_rows_read    NUMBER,
    p_rows_ins     NUMBER,
    p_rows_upd     NUMBER,
    p_rows_rej     NUMBER,
    p_error_count  NUMBER,
    p_message      VARCHAR2
  );
END etl_log_pkg;
/
SHOW ERRORS;

CREATE OR REPLACE PACKAGE BODY etl_log_pkg AS
  FUNCTION start_process(p_msg VARCHAR2) RETURN NUMBER IS
  BEGIN
    RETURN 1; -- dummy process ID
  END;

  PROCEDURE log_error(
    p_process_id   NUMBER,
    p_source_table VARCHAR2,
    p_source_key   VARCHAR2,
    p_error_msg    VARCHAR2,
    p_bad_data     VARCHAR2
  ) IS
  BEGIN
    NULL; -- do nothing
  END;

  PROCEDURE end_process(
    p_process_id   NUMBER,
    p_status       VARCHAR2,
    p_rows_read    NUMBER,
    p_rows_ins     NUMBER,
    p_rows_upd     NUMBER,
    p_rows_rej     NUMBER,
    p_error_count  NUMBER,
    p_message      VARCHAR2
  ) IS
  BEGIN
    NULL; -- do nothing
  END;
END etl_log_pkg;
/
SHOW ERRORS;

-------------------------------------------------------------------
-- 1. Drop existing GOOD/BAD tables if they exist
-------------------------------------------------------------------
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_crime_register_good PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_crime_register_bad PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_officer_good PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_officer_bad PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_location_good PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_location_bad PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_crime_type_good PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE stg_crime_type_bad PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

-------------------------------------------------------------------
-- 2. Create GOOD/BAD tables based on staging
-------------------------------------------------------------------
CREATE TABLE stg_crime_register_good AS SELECT * FROM stg_crime_register WHERE 1=0;
/
CREATE TABLE stg_crime_register_bad AS
SELECT sr.*, CAST(NULL AS VARCHAR2(400)) AS error_reason FROM stg_crime_register sr WHERE 1=0;
/

CREATE TABLE stg_officer_good AS SELECT * FROM stg_officer WHERE 1=0;
/
CREATE TABLE stg_officer_bad AS
SELECT so.*, CAST(NULL AS VARCHAR2(400)) AS error_reason FROM stg_officer so WHERE 1=0;
/

CREATE TABLE stg_location_good AS SELECT * FROM stg_location WHERE 1=0;
/
CREATE TABLE stg_location_bad AS
SELECT sl.*, CAST(NULL AS VARCHAR2(400)) AS error_reason FROM stg_location sl WHERE 1=0;
/

CREATE TABLE stg_crime_type_good AS SELECT * FROM stg_crime_type WHERE 1=0;
/
CREATE TABLE stg_crime_type_bad AS
SELECT sct.*, CAST(NULL AS VARCHAR2(400)) AS error_reason FROM stg_crime_type sct WHERE 1=0;
/

-------------------------------------------------------------------
-- 3. Create quality_etl_pkg
-------------------------------------------------------------------
CREATE OR REPLACE PACKAGE quality_etl_pkg AS
  PROCEDURE process_stg_crime_register;
  PROCEDURE process_stg_officer;
  PROCEDURE process_stg_location;
  PROCEDURE process_stg_crime_type;
  PROCEDURE run_all;
END quality_etl_pkg;
/
SHOW ERRORS;

CREATE OR REPLACE PACKAGE BODY quality_etl_pkg AS

  -----------------------------------------------------------------
  -- PROCESS_STG_CRIME_REGISTER
  -----------------------------------------------------------------
  PROCEDURE process_stg_crime_register AS
    v_process_id NUMBER;
    v_rows_good NUMBER := 0;
    v_rows_bad  NUMBER := 0;
  BEGIN
    v_process_id := etl_log_pkg.start_process('QUALITY: STG_CRIME_REGISTER_GOOD_BAD');

    DELETE FROM stg_crime_register_good;
    DELETE FROM stg_crime_register_bad;
    COMMIT;

    -- GOOD
    INSERT INTO stg_crime_register_good
    SELECT *
    FROM stg_crime_register sr
    WHERE sr.reported_date IS NOT NULL
      AND sr.location_id IS NOT NULL
      AND sr.officer_id IS NOT NULL
      AND sr.crime_type_id IS NOT NULL
      AND (sr.is_closed IS NULL OR sr.is_closed IN (0,1));

    v_rows_good := SQL%ROWCOUNT;

    -- BAD
    INSERT INTO stg_crime_register_bad
    SELECT sr.*,
           RTRIM(
             CASE WHEN sr.reported_date IS NULL THEN 'Missing reported_date; ' ELSE NULL END ||
             CASE WHEN sr.location_id IS NULL THEN 'Missing location_id; ' ELSE NULL END ||
             CASE WHEN sr.officer_id IS NULL THEN 'Missing officer_id; ' ELSE NULL END ||
             CASE WHEN sr.crime_type_id IS NULL THEN 'Missing crime_type_id; ' ELSE NULL END ||
             CASE WHEN sr.is_closed IS NOT NULL AND sr.is_closed NOT IN (0,1) THEN 'Invalid is_closed; ' ELSE NULL END
           ) AS error_reason
    FROM stg_crime_register sr
    WHERE NOT (
          sr.reported_date IS NOT NULL
      AND sr.location_id IS NOT NULL
      AND sr.officer_id IS NOT NULL
      AND sr.crime_type_id IS NOT NULL
      AND (sr.is_closed IS NULL OR sr.is_closed IN (0,1))
    );

    v_rows_bad := SQL%ROWCOUNT;

    COMMIT;
    etl_log_pkg.end_process(
      p_process_id => v_process_id,
      p_status     => 'SUCCESS',
      p_rows_read  => v_rows_good + v_rows_bad,
      p_rows_ins   => v_rows_good,
      p_rows_upd   => 0,
      p_rows_rej   => v_rows_bad,
      p_error_count=> 0,
      p_message    => NULL
    );
  END process_stg_crime_register;

  -----------------------------------------------------------------
  -- PROCESS_STG_OFFICER
  -----------------------------------------------------------------
  PROCEDURE process_stg_officer AS
    v_process_id NUMBER;
    v_rows_good NUMBER := 0;
    v_rows_bad  NUMBER := 0;
  BEGIN
    v_process_id := etl_log_pkg.start_process('QUALITY: STG_OFFICER_GOOD_BAD');

    DELETE FROM stg_officer_good;
    DELETE FROM stg_officer_bad;
    COMMIT;

    -- GOOD
    INSERT INTO stg_officer_good
    SELECT *
    FROM stg_officer so
    WHERE so.officer_name IS NOT NULL
      AND so.officer_rank IS NOT NULL
      AND so.department IS NOT NULL;

    v_rows_good := SQL%ROWCOUNT;

    -- BAD
    INSERT INTO stg_officer_bad
    SELECT so.*,
           RTRIM(
             CASE WHEN so.officer_name IS NULL THEN 'Missing officer_name; ' ELSE NULL END ||
             CASE WHEN so.officer_rank IS NULL THEN 'Missing officer_rank; ' ELSE NULL END ||
             CASE WHEN so.department IS NULL THEN 'Missing department; ' ELSE NULL END
           ) AS error_reason
    FROM stg_officer so
    WHERE NOT (
          so.officer_name IS NOT NULL
      AND so.officer_rank IS NOT NULL
      AND so.department IS NOT NULL
    );

    v_rows_bad := SQL%ROWCOUNT;

    COMMIT;
    etl_log_pkg.end_process(
      p_process_id => v_process_id,
      p_status     => 'SUCCESS',
      p_rows_read  => v_rows_good + v_rows_bad,
      p_rows_ins   => v_rows_good,
      p_rows_upd   => 0,
      p_rows_rej   => v_rows_bad,
      p_error_count=> 0,
      p_message    => NULL
    );
  END process_stg_officer;

  -----------------------------------------------------------------
  -- PROCESS_STG_LOCATION
  -----------------------------------------------------------------
  PROCEDURE process_stg_location AS
    v_process_id NUMBER;
    v_rows_good NUMBER := 0;
    v_rows_bad  NUMBER := 0;
  BEGIN
    v_process_id := etl_log_pkg.start_process('QUALITY: STG_LOCATION_GOOD_BAD');

    DELETE FROM stg_location_good;
    DELETE FROM stg_location_bad;
    COMMIT;

    -- GOOD
    INSERT INTO stg_location_good
    SELECT *
    FROM stg_location sl
    WHERE sl.station_name IS NOT NULL
      AND sl.region_id IS NOT NULL
      AND sl.city IS NOT NULL;

    v_rows_good := SQL%ROWCOUNT;

    -- BAD
    INSERT INTO stg_location_bad
    SELECT sl.*,
           RTRIM(
             CASE WHEN sl.station_name IS NULL THEN 'Missing station_name; ' ELSE NULL END ||
             CASE WHEN sl.region_id IS NULL THEN 'Missing region_id; ' ELSE NULL END ||
             CASE WHEN sl.city IS NULL THEN 'Missing city; ' ELSE NULL END
           ) AS error_reason
    FROM stg_location sl
    WHERE NOT (
          sl.station_name IS NOT NULL
      AND sl.region_id IS NOT NULL
      AND sl.city IS NOT NULL
    );

    v_rows_bad := SQL%ROWCOUNT;

    COMMIT;
    etl_log_pkg.end_process(
      p_process_id => v_process_id,
      p_status     => 'SUCCESS',
      p_rows_read  => v_rows_good + v_rows_bad,
      p_rows_ins   => v_rows_good,
      p_rows_upd   => 0,
      p_rows_rej   => v_rows_bad,
      p_error_count=> 0,
      p_message    => NULL
    );
  END process_stg_location;

  -----------------------------------------------------------------
  -- PROCESS_STG_CRIME_TYPE
  -----------------------------------------------------------------
  PROCEDURE process_stg_crime_type AS
    v_process_id NUMBER;
    v_rows_good NUMBER := 0;
    v_rows_bad  NUMBER := 0;
  BEGIN
    v_process_id := etl_log_pkg.start_process('QUALITY: STG_CRIME_TYPE_GOOD_BAD');

    DELETE FROM stg_crime_type_good;
    DELETE FROM stg_crime_type_bad;
    COMMIT;

    -- GOOD
    INSERT INTO stg_crime_type_good
    SELECT *
    FROM stg_crime_type sct
    WHERE sct.crime_type IS NOT NULL
      AND sct.severity_level IS NOT NULL;

    v_rows_good := SQL%ROWCOUNT;

    -- BAD
    INSERT INTO stg_crime_type_bad
    SELECT sct.*,
           RTRIM(
             CASE WHEN sct.crime_type IS NULL THEN 'Missing crime_type; ' ELSE NULL END ||
             CASE WHEN sct.severity_level IS NULL THEN 'Missing severity_level; ' ELSE NULL END
           ) AS error_reason
    FROM stg_crime_type sct
    WHERE NOT (
          sct.crime_type IS NOT NULL
      AND sct.severity_level IS NOT NULL
    );

    v_rows_bad := SQL%ROWCOUNT;

    COMMIT;
    etl_log_pkg.end_process(
      p_process_id => v_process_id,
      p_status     => 'SUCCESS',
      p_rows_read  => v_rows_good + v_rows_bad,
      p_rows_ins   => v_rows_good,
      p_rows_upd   => 0,
      p_rows_rej   => v_rows_bad,
      p_error_count=> 0,
      p_message    => NULL
    );
  END process_stg_crime_type;

  -----------------------------------------------------------------
  -- RUN_ALL: convenience procedure
  -----------------------------------------------------------------
  PROCEDURE run_all AS
  BEGIN
    process_stg_crime_register;
    process_stg_officer;
    process_stg_location;
    process_stg_crime_type;
  END run_all;

END quality_etl_pkg;
/
SHOW ERRORS;

-------------------------------------------------------------------
-- 4. Execute all quality steps
-------------------------------------------------------------------
BEGIN
  quality_etl_pkg.run_all;
END;
/
