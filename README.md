# signal-analytics-snowflake

Daily Snowflake pipeline to download INRIX XD travel time data (via the RITIS API) and compute analytics for traffic signals. It registers Snowflake stored procedures and a scheduled task that runs each day.

## What this repo contains

- `ritis_setup.py` — Registers `RITIS_SPROC(start_date, end_date)` which downloads INRIX XD travel time for the XD segments in `DIM_SIGNALS_XD` and writes to `XD_TRAVEL_TIME`. Uses an external access integration and a stored secret for the API key.
- `analytics_setup.py` — Registers `ANALYTICS_SPROC(date, is_last_date)` which computes decomposition, anomaly detection, and changepoints, writing results to `TRAVEL_TIME_ANALYTICS`, `CHANGEPOINTS`, and `FREEFLOW`.
- `pipeline.py` — Registers `PIPELINE_SPROC()` and creates/enables a daily Snowflake Task `PIPELINE_TASK` (cron 3:00 AM America/Los_Angeles). The pipeline uses a `run_stats` table to orchestrate jobs and call `RITIS_SPROC` and `ANALYTICS_SPROC` in order.
- `table_creation.sql` — SQL definitions for required objects (tables like `XD_TRAVEL_TIME`, `TRAVEL_TIME_ANALYTICS`, `CHANGEPOINTS`, `FREEFLOW`, `DIM_SIGNALS_XD`, `run_stats`, stages, etc.).

## One‑time setup

1) Run the SQL manually in Snowflake

- Open a Snowflake Notebook (or Worksheet) and run the contents of `table_creation.sql` to create required tables and objects.
	- Note: This step is manual and must be executed inside Snowflake.

2) Set Snowflake connection settings (Windows PowerShell)

The scripts read connection info from the `SNOWFLAKE_CONNECTION` environment variable (JSON). Set it once using `setx`, then restart your terminal/VS Code.

```powershell
# Example: user/password auth (compact JSON; escape quotes with backticks in PowerShell)
setx SNOWFLAKE_CONNECTION "{`"ACCOUNT`":`"<account_identifier>`",`"USER`":`"<user>`",`"PASSWORD`":`"<password>`",`"ROLE`":`"<role>`",`"WAREHOUSE`":`"<warehouse>`",`"DATABASE`":`"<database>`",`"SCHEMA`":`"<schema>`"}"

# After running setx, close and reopen your terminal for the value to take effect.
```

- Required keys typically include: `ACCOUNT`, `USER`, `PASSWORD` (or `AUTHENTICATOR`), `ROLE`, `WAREHOUSE`, `DATABASE`, `SCHEMA`.

3) Register the stored procedures and task (run each script once)

From this folder, run the scripts to register the procedures and create/enable the scheduled task:

```powershell
python .\ritis_setup.py       # Registers RITIS_SPROC
python .\analytics_setup.py   # Registers ANALYTICS_SPROC
python .\pipeline.py          # Registers PIPELINE_SPROC and creates/enables PIPELINE_TASK
```

## Running the pipeline

- Scheduled: The `PIPELINE_TASK` runs daily at 3:00 AM PT and calls `PIPELINE_SPROC()`.
- Manual run (Snowflake SQL):

```sql
-- End-to-end
CALL PIPELINE_SPROC();

-- Individually
CALL RITIS_SPROC('2025-09-01', '2025-09-02');  -- end_date is exclusive
CALL ANALYTICS_SPROC('2025-09-01', 'true');
```

## Notes

- `RITIS_SPROC` expects an external access integration (e.g., `pda_api_access_integration`) and a secret (e.g., `SHAWNS_RITIS_KEY`) to be present in Snowflake. Obviously, use your own secret and integration.
- The pipeline expects a `run_stats` table to track work and retries; it will populate missing dates for each job type automatically.
