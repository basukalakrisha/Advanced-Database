------------------------------------------------------------
-- Procedure to populate STG_OFFICER
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE load_stg_officer AS
BEGIN
    INSERT INTO stg_officer(officer_id, officer_name, officer_rank, department, gender, data_source)
    SELECT 
        officer_id,
        first_name || ' ' || NVL(middle_name,'') || ' ' || last_name AS officer_name,
        RANK AS officer_rank,
        DEPARTMENT,
        GENDER,
        'PS_WALES' AS data_source
    FROM OFFICER;

    COMMIT;
END;
/
------------------------------------------------------------
-- Procedure to populate STG_LOCATION
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE load_stg_location AS
BEGIN
    INSERT INTO stg_location(location_id, station_name, region_id, city, data_source)
    SELECT 
        location_id,
        street_name AS station_name,
        region_id,
        city_name AS city,
        'PS_WALES' AS data_source
    FROM LOCATION;

    COMMIT;
END;
/
------------------------------------------------------------
-- Procedure to populate STG_CRIME_TYPE
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE load_stg_crime_type AS
BEGIN
    INSERT INTO stg_crime_type(crime_type_id, crime_type, severity_level, data_source)
    SELECT 
        offence_id AS crime_type_id,
        offence_type AS crime_type,
        'Medium' AS severity_level,  -- you can adjust severity mapping later
        'PS_WALES' AS data_source
    FROM OFFENCE;

    COMMIT;
END;
/
------------------------------------------------------------
-- Procedure to populate STG_CRIME_REGISTER
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE load_stg_crime_register AS
BEGIN
    INSERT INTO stg_crime_register(crime_id, reported_date, location_id, officer_id, crime_type_id, is_closed, days_to_close, data_source)
    SELECT 
        cr.crime_id,
        cr.reported_date,
        cr.location_id,
        cr.police_id AS officer_id,
        o.offence_id AS crime_type_id, 
        CASE WHEN cr.crime_status IN ('CLOSED','closed') THEN 1 ELSE 0 END AS is_closed,
        CASE 
            WHEN cr.closed_date IS NOT NULL THEN cr.closed_date - cr.reported_date
            ELSE NULL
        END AS days_to_close,
        'PS_WALES' AS data_source
    FROM CRIME_REGISTER cr
    LEFT JOIN OFFENCE o ON cr.crime_id = o.crime_id;

    COMMIT;
END;
/
------------------------------------------------------------
-- Optional: procedure to populate all staging tables
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE load_all_staging AS
BEGIN
    load_stg_officer;
    load_stg_location;
    load_stg_crime_type;
    load_stg_crime_register;
END;
/

BEGIN
    load_all_staging;
END;
/