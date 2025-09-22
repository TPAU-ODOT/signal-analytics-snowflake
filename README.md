# Signal Analytics Snowflake Pipeline

A daily Snowflake pipeline that downloads INRIX XD travel time data via the RITIS API and computes performance analytics (anomaly and changepoint detection) for traffic signals. The setup scripts register Snowflake stored procedures and a scheduled task to orchestrate the entire workflow.

## Core Components

- `ritis_setup.py`: Deploys the `RITIS_SPROC` stored procedure, which downloads INRIX data for specified XD segments and saves it to the `XD_TRAVEL_TIME` table.
- `analytics_setup.py`: Deploys the `ANALYTICS_SPROC` stored procedure, which computes decomposition, anomaly detection, and changepoints, writing results to `TRAVEL_TIME_ANALYTICS`, `CHANGEPOINTS`, and `FREEFLOW`.
- `pipeline.py`: Deploys the `PIPELINE_SPROC` stored procedure and creates/enables `PIPELINE_TASK`, a daily scheduled Snowflake Task (default: 3:00 AM PT) that orchestrates the data download and analytics processing.
- `table_creation.sql`: Contains all the SQL DDL statements to create the necessary database objects (tables, stages, secrets) in Snowflake.

## Dependencies

This project relies on the following external Python libraries, which are bundled with the stored procedures:

- **[ritis-inrix-api](https://github.com/ShawnStrasser/RITIS_INRIX_API)**: Used within `RITIS_SPROC` to interface with the RITIS API for downloading INRIX XD data.
- **[traffic-anomaly](https://github.com/ShawnStrasser/traffic-anomaly)**: Powers the analytics in `ANALYTICS_SPROC`. For this pipeline, you must install the library with Snowflake-compatible dependencies as described in its documentation.



```bash
pip install snowflake
pip install traffic-anomaly  # Note: This requires additional steps per the docs
pip install ritis-inrix-api
```

## End-to-End Setup Instructions

Follow these steps to deploy the pipeline. You must first adjust code/scripts as needed for your specific deployment.

### 1. Create Snowflake Database Objects

Snowflake objects are defined in `table_creation.sql`.

1.  Open a Snowflake Notebook or Worksheet.
2.  Copy the entire contents of `table_creation.sql` and paste into a Snowflake worksheet. Carefully review table/variable names before executing. This will create the following:
    - **Tables**: `XD_TRAVEL_TIME`, `TRAVEL_TIME_ANALYTICS`, `CHANGEPOINTS`, `FREEFLOW`, `DIM_SIGNALS_XD`, `DIM_CONNECTIVITY`, and `run_stats`.
    - **Stage**: `CODE_STAGE` for storing Python code for the stored procedures.
    - **Secret**: This example uses `SHAWNS_RITIS_KEY`, use your own name. This is to securely store the RITIS API key.


### 2. Populate Dimension Tables

The pipeline relies on two dimension tables that you must populate with your own data:
- `DIM_SIGNALS_XD`: Contains XD segment definitions and their geographic metadata.
- `DIM_CONNECTIVITY`: Defines the relationships between different XD segments.

### 3. Configure Snowflake Connection

The Python scripts require a `SNOWFLAKE_CONNECTION` environment variable containing your connection details as a JSON string.

In a Windows PowerShell terminal, run `setx` to set the variable permanently. **You will need to restart your terminal or VS Code for the change to take effect.**

```powershell
# Example using user/password authentication. Escape inner quotes with backticks (`).
setx SNOWFLAKE_CONNECTION "{`"ACCOUNT`":`"<account_identifier>`",`"USER`":`"<user>`",`"PASSWORD`":`"<password>`",`"ROLE`":`"<role>`",`"WAREHOUSE`":`"<warehouse>`",`"DATABASE`":`"TPAU_DB`",`"SCHEMA`":`"TPAU_RITIS_SCHEMA`"}"
```

### 4. Deploy Stored Procedures and Task

Run the following Python scripts from your terminal to deploy the business logic to Snowflake. Each script uploads and registers a stored procedure.

```powershell
python ritis_setup.py       # Deploys RITIS_SPROC
python analytics_setup.py   # Deploys ANALYTICS_SPROC
python pipeline.py          # Deploys PIPELINE_SPROC and creates/enables the daily PIPELINE_TASK
```

## Running the Pipeline

- **Scheduled**: The `PIPELINE_TASK` automatically runs the entire pipeline daily at 3:00 AM PT.
- **Manual**: You can trigger the procedures manually in Snowflake for testing or backfilling data.

```sql
-- Run the entire end-to-end pipeline
CALL PIPELINE_SPROC();

-- The pipeline will handle orchestration and calling other procedures, like this:
CALL RITIS_SPROC('2025-09-01', '2025-09-02'); --download data for Sept 1, 2025
CALL ANALYTICS_SPROC('2025-09-01', 'true'); --process analytics for Sept 1, 2025
```
The `PIPELINE_SPROC` procedure updates the `RUN_STATS` to track run history, execution time, and any errors encountered during processing.
