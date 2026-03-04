CREATE OR REPLACE PROCEDURE refresh_procedure()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_ready BOOLEAN;
  v_has_run BOOLEAN;
BEGIN
  SELECT COALESCE(MIN(up_to_date_ind), FALSE) INTO :v_ready
  FROM src_date_view;

  SELECT has_run INTO :v_has_run
  FROM control_db.control_schema.task_control
  WHERE task_name = 'MY_HOURLY_TASK';

  IF (NOT v_ready) OR v_has_run THEN
    RETURN 'Not ready or already ran; skipping.';
  END IF;

  UPDATE control_db.control_schema.task_control
  SET has_run = TRUE, ran_at = CURRENT_TIMESTAMP()
  WHERE task_name = 'MY_HOURLY_TASK' AND has_run = FALSE;

  EXECUTE DBT PROJECT my_dbt_project
    ARGS = 'run --select +final_model';

  -- optional if you truly want "only once ever"
  -- ALTER TASK my_hourly_task SUSPEND;

  RETURN 'Ran dbt.';
END;
$$;