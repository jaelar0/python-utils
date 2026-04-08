-- ============================================================
-- SNOWFLAKE NATIVE SCHEDULER TEMPLATE
-- dbt Pipeline Orchestration via Tasks + Stored Procedures
-- ============================================================
-- ASSUMPTIONS:
--   Team database       : TEAM_DB
--   Team schema         : ANALYTICS
--   Source database     : SOURCE_DB  (separate, read-only)
--   dbt models live in  : TEAM_DB.DBT_SCHEMA
--   Warehouse           : TEAM_WH
--   Role running tasks  : TEAM_ROLE
--
-- NOTE: Replace all placeholders (ALL_CAPS) with your actual names.
-- ============================================================


-- ============================================================
-- STEP 1: PIPELINE_LOG TABLE
-- Stores every scheduled run's outcome.
-- Create this ONCE. Do not drop/recreate — it accumulates history.
-- ============================================================

CREATE TABLE IF NOT EXISTS TEAM_DB.ANALYTICS.PIPELINE_LOG (
    LOG_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    STATUS        VARCHAR(50),   -- 'SUCCESS' | 'SKIPPED_SOURCES' | 'SKIPPED_ALREADY_RAN' | 'ERROR'
    MESSAGE       VARCHAR(500),
    SOURCES_READY BOOLEAN,
    ALREADY_RAN   BOOLEAN,
    ERROR_DETAIL  VARCHAR(2000)  -- populated only on ERROR status
);


-- ============================================================
-- STEP 2: DATA_SOURCE_PROD_DATES VIEW
-- Shows all source tables and their latest PROD_DATE.
--
-- *** THIS IS THE MOST FRAGILE PART OF THE PIPELINE ***
-- See "What Went Wrong" section at the bottom for details.
--
-- Best practices applied here:
--   1. Use fully qualified names (DATABASE.SCHEMA.TABLE) always
--   2. Wrap each source in its own CTE for easy debugging
--   3. Use UNION ALL, not a dynamic approach — static is safer
--   4. Test this view manually before relying on the task
-- ============================================================

CREATE OR REPLACE VIEW TEAM_DB.ANALYTICS.DATA_SOURCE_PROD_DATES AS

WITH source_inventory AS (

    -- Add one row per source table. Use the actual MAX(PROD_DATE) or
    -- MAX(AS_OF_DATE) or whatever date column signals data currency.
    -- Fully qualify every table reference.

    SELECT
        'SOURCE_DB.SCHEMA_A.TABLE_ONE'   AS source_name,
        MAX(PROD_DATE)                   AS latest_prod_date
    FROM SOURCE_DB.SCHEMA_A.TABLE_ONE

    UNION ALL

    SELECT
        'SOURCE_DB.SCHEMA_A.TABLE_TWO'   AS source_name,
        MAX(PROD_DATE)                   AS latest_prod_date
    FROM SOURCE_DB.SCHEMA_A.TABLE_TWO

    UNION ALL

    SELECT
        'SOURCE_DB.SCHEMA_B.TABLE_THREE' AS source_name,
        MAX(PROD_DATE)                   AS latest_prod_date
    FROM SOURCE_DB.SCHEMA_B.TABLE_THREE

    -- ... add all sources here ...
)

SELECT
    source_name,
    latest_prod_date,
    CURRENT_DATE()                                        AS check_date,
    (latest_prod_date = CURRENT_DATE())                   AS is_current
FROM source_inventory;


-- ============================================================
-- STEP 3: STORED PROCEDURE
-- Called by the Snowflake Task on the hourly schedule.
-- Written in native Snowflake Scripting (SQL) — no JavaScript needed.
--
-- Logic:
--   1. Has the pipeline already run successfully today? → SKIP
--   2. Are all sources current?                        → SKIP if no
--   3. All clear → run dbt, log result
--
-- EXECUTE AS CALLER: runs under the role that calls/owns the task,
-- not the procedure creator. This is usually what you want so the
-- proc uses the same role as your task, which already has the grants.
-- ============================================================

CREATE OR REPLACE PROCEDURE TEAM_DB.ANALYTICS.SP_RUN_DBT_PIPELINE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    already_ran_count   INTEGER DEFAULT 0;
    total_sources       INTEGER DEFAULT 0;
    ready_sources       INTEGER DEFAULT 0;
    sources_ready       BOOLEAN DEFAULT FALSE;
BEGIN

    -- ----------------------------------------------------------
    -- GUARD 1: Has the pipeline already succeeded today?
    -- ----------------------------------------------------------
    SELECT COUNT(*) INTO :already_ran_count
    FROM TEAM_DB.ANALYTICS.PIPELINE_LOG
    WHERE STATUS = 'SUCCESS'
      AND DATE(RUN_TIMESTAMP) = CURRENT_DATE();

    IF (already_ran_count > 0) THEN
        INSERT INTO TEAM_DB.ANALYTICS.PIPELINE_LOG (STATUS, MESSAGE, SOURCES_READY, ALREADY_RAN)
        VALUES ('SKIPPED_ALREADY_RAN', 'Pipeline refresh skipped. Project already ran.', NULL, TRUE);
        RETURN 'SKIPPED_ALREADY_RAN';
    END IF;

    -- ----------------------------------------------------------
    -- GUARD 2: Are all underlying sources current?
    --
    -- Wrapped in its own BEGIN/EXCEPTION block so a broken view
    -- fails gracefully instead of crashing the whole procedure.
    -- ----------------------------------------------------------
    BEGIN
        SELECT
            COUNT(*)                                     INTO :total_sources,
            SUM(CASE WHEN IS_CURRENT THEN 1 ELSE 0 END) INTO :ready_sources
        FROM TEAM_DB.ANALYTICS.DATA_SOURCE_PROD_DATES;

    EXCEPTION
        WHEN OTHER THEN
            INSERT INTO TEAM_DB.ANALYTICS.PIPELINE_LOG (STATUS, MESSAGE, SOURCES_READY, ALREADY_RAN, ERROR_DETAIL)
            VALUES ('ERROR', 'Pipeline refresh skipped. Could not validate source freshness.',
                    NULL, FALSE, SQLERRM);
            RETURN 'ERROR: source view query failed - ' || SQLERRM;
    END;

    LET sources_ready BOOLEAN := (total_sources > 0 AND total_sources = ready_sources);

    IF (NOT sources_ready) THEN
        INSERT INTO TEAM_DB.ANALYTICS.PIPELINE_LOG (STATUS, MESSAGE, SOURCES_READY, ALREADY_RAN)
        VALUES ('SKIPPED_SOURCES', 'Pipeline refresh skipped. Underlying sources not ready.', FALSE, FALSE);
        RETURN 'SKIPPED_SOURCES';
    END IF;

    -- ----------------------------------------------------------
    -- MAIN: Run the dbt project
    --
    -- Option A (most common): Call a second stored procedure that
    --   executes each compiled dbt model in dependency order.
    --
    -- Option B: If using dbt Cloud, trigger via REST API using
    --   a Python stored procedure. See notes at bottom of file.
    -- ----------------------------------------------------------
    BEGIN
        CALL TEAM_DB.ANALYTICS.SP_EXECUTE_DBT_MODELS();

        INSERT INTO TEAM_DB.ANALYTICS.PIPELINE_LOG (STATUS, MESSAGE, SOURCES_READY, ALREADY_RAN)
        VALUES ('SUCCESS', 'Pipeline refreshed successfully.', TRUE, FALSE);
        RETURN 'SUCCESS';

    EXCEPTION
        WHEN OTHER THEN
            INSERT INTO TEAM_DB.ANALYTICS.PIPELINE_LOG (STATUS, MESSAGE, SOURCES_READY, ALREADY_RAN, ERROR_DETAIL)
            VALUES ('ERROR', 'Pipeline refresh failed during dbt execution.', TRUE, FALSE, SQLERRM);
            RETURN 'ERROR: ' || SQLERRM;
    END;

END;
$$;


-- ============================================================
-- STEP 3b (OPTION A): Stored Procedure to Execute dbt Models
-- If you are NOT using dbt Cloud and instead run compiled SQL
-- directly in Snowflake, this proc runs each model in order.
--
-- Fill in each model in dependency order (staging → intermediate → marts).
-- Each EXECUTE IMMEDIATE runs the compiled SQL for that model.
-- ============================================================

CREATE OR REPLACE PROCEDURE TEAM_DB.ANALYTICS.SP_EXECUTE_DBT_MODELS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

    -- STAGING LAYER (views, recreated)
    CREATE OR REPLACE VIEW TEAM_DB.DBT_SCHEMA.STG_LOANS AS
        /* paste compiled SQL from target/compiled/...stg_loans.sql */
        SELECT * FROM SOURCE_DB.SCHEMA_A.LOANS_TABLE;

    CREATE OR REPLACE VIEW TEAM_DB.DBT_SCHEMA.STG_COLLATERAL AS
        /* paste compiled SQL from target/compiled/...stg_collateral.sql */
        SELECT * FROM SOURCE_DB.SCHEMA_A.COLLATERAL_TABLE;

    -- INTERMEDIATE LAYER (views)
    CREATE OR REPLACE VIEW TEAM_DB.DBT_SCHEMA.INT_LOANS_WITH_COLLATERAL AS
        /* paste compiled intermediate SQL */
        SELECT l.*, c.COLLATERAL_VALUE
        FROM TEAM_DB.DBT_SCHEMA.STG_LOANS l
        LEFT JOIN TEAM_DB.DBT_SCHEMA.STG_COLLATERAL c
            ON l.LOAN_ID = c.LOAN_ID;

    -- MART LAYER (tables, full refresh)
    CREATE OR REPLACE TABLE TEAM_DB.DBT_SCHEMA.MART_CRE_NOO_INPUT AS
        /* paste compiled mart SQL */
        SELECT * FROM TEAM_DB.DBT_SCHEMA.INT_LOANS_WITH_COLLATERAL
        WHERE LOAN_TYPE = 'CRE_NOO';

    RETURN 'dbt models executed successfully';

END;
$$;


-- ============================================================
-- STEP 4: SNOWFLAKE TASK
-- Runs the pipeline check every hour, 8am–6pm EST (13:00–23:00 UTC).
-- Cron format: minute hour day-of-month month day-of-week
--
-- IMPORTANT NOTES ON TASKS:
--   1. Tasks auto-SUSPEND after 8 consecutive failures. You must
--      manually RESUME them with: ALTER TASK ... RESUME;
--   2. The warehouse must be set to AUTO_RESUME = TRUE (see Step 5).
--   3. The task owner role must have EXECUTE TASK privilege.
--   4. EXECUTE AS OWNER means the task runs under the task creator's
--      role — make sure that role has access to all referenced objects.
-- ============================================================

CREATE OR REPLACE TASK TEAM_DB.ANALYTICS.TASK_DBT_PIPELINE_SCHEDULER
    WAREHOUSE   = TEAM_WH
    SCHEDULE    = 'USING CRON 0 13-23 * * MON-FRI America/New_York'
    -- Runs at :00 of every hour from 8am to 6pm ET, Mon-Fri
    -- 13 UTC = 8am ET (EST) / 9am ET (EDT) — adjust if needed for DST
    -- If you want exactly 8am–6pm ET year-round accounting for DST,
    -- use America/New_York and Snowflake handles the offset automatically.
AS
    CALL TEAM_DB.ANALYTICS.SP_RUN_DBT_PIPELINE();


-- Enable the task (tasks start SUSPENDED by default)
ALTER TASK TEAM_DB.ANALYTICS.TASK_DBT_PIPELINE_SCHEDULER RESUME;


-- ============================================================
-- STEP 5: WAREHOUSE SETTINGS (run once, verify once)
-- The task will silently fail if the warehouse can't start.
-- ============================================================

ALTER WAREHOUSE TEAM_WH SET
    AUTO_RESUME    = TRUE    -- warehouse wakes up when a task/query arrives
    AUTO_SUSPEND   = 120;    -- suspends after 2 min of inactivity (cost control)


-- ============================================================
-- STEP 6: REQUIRED GRANTS (run as ACCOUNTADMIN or SYSADMIN)
-- The most common silent failure cause is missing privileges.
-- ============================================================

-- Allow the role to execute tasks
GRANT EXECUTE TASK ON ACCOUNT TO ROLE TEAM_ROLE;

-- Allow the role to read source tables (cross-database)
GRANT USAGE  ON DATABASE   SOURCE_DB            TO ROLE TEAM_ROLE;
GRANT USAGE  ON SCHEMA     SOURCE_DB.SCHEMA_A   TO ROLE TEAM_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SOURCE_DB.SCHEMA_A TO ROLE TEAM_ROLE;
-- Repeat for each source schema

-- Allow the role to write to team objects
GRANT USAGE  ON DATABASE   TEAM_DB              TO ROLE TEAM_ROLE;
GRANT USAGE  ON SCHEMA     TEAM_DB.ANALYTICS    TO ROLE TEAM_ROLE;
GRANT INSERT ON TABLE      TEAM_DB.ANALYTICS.PIPELINE_LOG TO ROLE TEAM_ROLE;

-- Allow the role to use the warehouse
GRANT USAGE ON WAREHOUSE TEAM_WH TO ROLE TEAM_ROLE;


-- ============================================================
-- MONITORING QUERIES (run ad-hoc to check status)
-- ============================================================

-- Check last 20 runs
SELECT *
FROM TEAM_DB.ANALYTICS.PIPELINE_LOG
ORDER BY RUN_TIMESTAMP DESC
LIMIT 20;

-- Check if task is alive
SHOW TASKS LIKE 'TASK_DBT_PIPELINE_SCHEDULER' IN SCHEMA TEAM_DB.ANALYTICS;

-- Check task run history (last 7 days)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
    TASK_NAME => 'TASK_DBT_PIPELINE_SCHEDULER'
))
ORDER BY SCHEDULED_TIME DESC;

-- Validate the source view is healthy right now
SELECT * FROM TEAM_DB.ANALYTICS.DATA_SOURCE_PROD_DATES;

-- Check if task auto-suspended (STATE = 'suspended' means it failed too many times)
SELECT name, state, schedule, last_committed_on
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE name = 'TASK_DBT_PIPELINE_SCHEDULER'
LIMIT 1;


-- ============================================================
-- OPTION B: Trigger dbt Cloud via HTTP (alternative to Option A)
-- Requires: Snowflake External Network Access integration to dbt Cloud API
-- ============================================================
/*
CREATE OR REPLACE PROCEDURE TEAM_DB.ANALYTICS.SP_TRIGGER_DBT_CLOUD()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'run'
AS
$$
import requests
import json

def run(session):
    DBT_CLOUD_URL     = 'https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/'
    DBT_SERVICE_TOKEN = '<your_dbt_cloud_service_token>'  # store in Snowflake secret

    headers = {
        'Authorization': f'Token {DBT_SERVICE_TOKEN}',
        'Content-Type': 'application/json'
    }
    payload = {'cause': 'Triggered by Snowflake Task'}

    response = requests.post(DBT_CLOUD_URL, headers=headers, json=payload)
    return response.json()
$$;
*/
