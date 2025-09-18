# signal-analytics-snowflake

Daily Snowflake pipeline to download INRIX XD travel time data (via the RITIS API) and compute analytics for traffic signals. It registers Snowflake stored procedures and a scheduled task that runs each day.

## What this repo contains

- `ritis_setup.py` — Registers `RITIS_SPROC(start_date, end_date)` which downloads INRIX XD travel time for the XD segments in `DIM_SIGNALS_XD` and writes to `XD_TRAVEL_TIME`. Uses an external access integration and a stored secret for the API key.
- `analytics_setup.py` — Registers `ANALYTICS_SPROC(date, is_last_date)` which computes decomposition, anomaly detection, and changepoints, writing results to `TRAVEL_TIME_ANALYTICS`, `CHANGEPOINTS`, and `FREEFLOW`.
- `pipeline.py` — Registers `PIPELINE_SPROC()` and creates/enables a daily Snowflake Task `PIPELINE_TASK` (cron 3:00 AM America/Los_Angeles). The pipeline uses a `run_stats` table to orchestrate jobs and call `RITIS_SPROC` and `ANALYTICS_SPROC` in order.
- `table_creation.sql` — Complete SQL definitions for all required database objects including:
  - **Tables**: `XD_TRAVEL_TIME`, `TRAVEL_TIME_ANALYTICS`, `CHANGEPOINTS`, `FREEFLOW`, `DIM_SIGNALS_XD`, `DIM_CONNECTIVITY`, `run_stats`
  - **File Formats**: `RITISPARQUETFORMAT` for Parquet data processing
  - **Stages**: `CODE_STAGE` for stored procedure code storage  
  - **Secrets**: `SHAWNS_RITIS_KEY` for RITIS API authentication

## One‑time setup

1) **Create database objects in Snowflake**

Run the SQL DDL statements manually in Snowflake to create all required tables, stages, file formats, and secrets:

- Open a Snowflake Notebook or Worksheet
- Copy and paste the contents of `table_creation.sql`
- Execute the statements to create all database objects
- **Important**: Update the `SHAWNS_RITIS_KEY` secret with your actual RITIS API key (replace 'x' with your key)
- Ensure you have the correct database (`TPAU_DB`) and schema (`TPAU_RITIS_SCHEMA`) context

Note: This step is manual and must be executed inside Snowflake with appropriate permissions.

2) **Populate dimension tables** (if not already done)

The pipeline expects these tables to contain data:
- `DIM_SIGNALS_XD` - XD segment definitions with geographic metadata
- `DIM_CONNECTIVITY` - relationships between XD segments

3) **Set Snowflake connection settings** (Windows PowerShell)

The scripts read connection info from the `SNOWFLAKE_CONNECTION` environment variable (JSON). Set it once using `setx`, then restart your terminal/VS Code.

```powershell
# Example: user/password auth (compact JSON; escape quotes with backticks in PowerShell)
setx SNOWFLAKE_CONNECTION "{`"ACCOUNT`":`"<account_identifier>`",`"USER`":`"<user>`",`"PASSWORD`":`"<password>`",`"ROLE`":`"<role>`",`"WAREHOUSE`":`"<warehouse>`",`"DATABASE`":`"<database>`",`"SCHEMA`":`"<schema>`"}"

# After running setx, close and reopen your terminal for the value to take effect.
```

- Required keys typically include: `ACCOUNT`, `USER`, `PASSWORD` (or `AUTHENTICATOR`), `ROLE`, `WAREHOUSE`, `DATABASE`, `SCHEMA`.

4) **Register the stored procedures and task** (run each script once)

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

- All database objects are created in the `TPAU_DB.TPAU_RITIS_SCHEMA` schema structure
- `RITIS_SPROC` expects an external access integration (e.g., `pda_api_access_integration`) and the `SHAWNS_RITIS_KEY` secret to be properly configured in Snowflake
- The pipeline uses the `run_stats` table to track work and retries; it will automatically populate missing dates for each job type
- Dimension tables (`DIM_SIGNALS_XD`, `DIM_CONNECTIVITY`) must be populated with your signal/XD data before running the pipeline
- The `table_creation.sql` includes verification queries and cleanup commands (commented out) for testing and maintenance
