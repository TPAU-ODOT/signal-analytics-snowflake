'''
This script sets up a Snowflake stored procedure to download and store INRIX XD travel time data from the RITIS API.
It requires tables already set up in Snowflake, see setup.sql.
'''

from snowflake.snowpark.session import Session
from snowflake.snowpark.types import StringType
import os
import json
import shutil
import tempfile


# Load connection params from env variable
connection_parameters = json.loads(os.environ.get("SNOWFLAKE_CONNECTION"))

# Create the Snowpark session
session = Session.builder.configs(connection_parameters).create()


# Now register the function as a stored procedure in Snowflake

# Method to automate attaching and uploading custom packages
def add_packages(d, session):
    import parsy
    import pyarrow_hotfix
    import rich
    import sqlglot
    import ibis
    import traffic_anomaly
    for module in (ibis, parsy, pyarrow_hotfix, rich, sqlglot, traffic_anomaly):
        pkgname = module.__name__
        pkgpath = os.path.join(d, pkgname)
        ignore = shutil.ignore_patterns("*.so", "*.pyd", "*.dll", "*.egg-info", "__pycache__")
        shutil.copytree(os.path.dirname(module.__file__), pkgpath, ignore=ignore)
        session.add_import(pkgname, import_path=pkgname)

import sys
d = tempfile.TemporaryDirectory()
os.chdir(d.name)
sys.path.insert(0, d.name)
add_packages(d.name, session)

#Adding custom packages to Snowflake stored procedure, adapted from:
# https://ibis-project.org/posts/run-on-snowflake/

def analytics_sproc(session: Session, current_date_str, is_last_date_str) -> str:
    """
    Process analytics for a single date.
    
    Args:
        current_date_str: Date string in 'YYYY-MM-DD' format
        is_last_date_str: 'true' or 'false' string indicating if this is the last date
    """
    import ibis
    from traffic_anomaly import decompose, anomaly, changepoint
    from ibis import _
    import ibis.backends.snowflake
    from datetime import datetime, timedelta
    import duckdb
    import re

    # Parse inputs
    current_date = datetime.strptime(current_date_str, '%Y-%m-%d').date()
    is_last_date = is_last_date_str.lower() == 'true'
    # Calculate start date (6 weeks before current date)
    start_date = current_date - timedelta(days=42)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = (current_date + timedelta(days=1)).strftime('%Y-%m-%d')

    con = ibis.backends.snowflake.Backend.from_snowpark(session, create_object_udfs=False)
    # Ibis sets session timezone to UTC by default, change to Pacific
    con.raw_sql("ALTER SESSION SET TIMEZONE = 'America/Los_Angeles'")

    # Load raw data for the date range
    raw_data = con.table("XD_TRAVEL_TIME").select(
        XD='XD_ID',
        TIMESTAMP='MEASUREMENT_TSTAMP',
        TRAVEL_TIME_SECONDS='TRAVEL_TIME_SECONDS'
    ).filter(
        (ibis._['TIMESTAMP'] >= start_date_str) & 
        (ibis._['TIMESTAMP'] < end_date_str)
    )
    signals = con.table("DIM_SIGNALS_XD")
    connectivity = con.table("DIM_CONNECTIVITY").rename(next_XD='NEXT_XD')

    decomp_expr = decompose(
        data=raw_data,
        datetime_column='TIMESTAMP',
        value_column='TRAVEL_TIME_SECONDS',
        entity_grouping_columns=['XD'],
        freq_minutes=15,
        rolling_window_days=7,
        drop_days=7,
        min_rolling_window_samples=56*4,
        min_time_of_day_samples=7,
        drop_extras=False,
        rolling_window_enable=False
    )
    # MATERIALIZE decomp into a temporary table since it is used twice
    con.create_table("TEMP_DECOMP", decomp_expr, temp=True, overwrite=True)
    decomp = con.table("TEMP_DECOMP")

    # Freeflow calculation (only used if last date)
    freeflow = (
        decomp
        .group_by('XD')
        .aggregate(
            TRAVEL_TIME_SECONDS=decomp['TRAVEL_TIME_SECONDS'].quantile(0.05)  # Ibis quantile at 5%
        )
    )

    # JOIN SIGNAL GROUPS (COUNTY) TO decomp
    # 1. Get the distinct signals and their counties
    signals_distinct = signals.select('XD', 'COUNTY').distinct()
    # 2. Join the decomp expression with the signals table expression
    decomp_grouped = decomp.join(
        signals_distinct,
        decomp.XD == signals_distinct.XD
    ).select(
        decomp.XD,
        decomp.TIMESTAMP,
        decomp.TRAVEL_TIME_SECONDS,
        decomp.prediction,
        resid=decomp.resid,
        # Rename the county column to 'GROUP'
        GROUP=signals_distinct.COUNTY
    )

    df_anomaly = anomaly(
        decomposed_data=decomp_grouped,
        datetime_column='TIMESTAMP',
        value_column='TRAVEL_TIME_SECONDS',
        entity_grouping_columns=['XD'],
        entity_threshold=4.0,
        group_threshold=4.0,
        group_grouping_columns=['GROUP'],
        connectivity_table=connectivity
    ).filter(ibis._['TIMESTAMP'] >= current_date_str)
    # Shouting case for Snowflake
    df_anomaly = df_anomaly.rename(dict(zip([x.upper() for x in df_anomaly.columns], df_anomaly.columns)))
    df_anomaly = df_anomaly.select(
        'XD', 'TIMESTAMP', 'TRAVEL_TIME_SECONDS', 
        'PREDICTION', 'ANOMALY', 'ORIGINATED_ANOMALY'
    )

    # Now handle changepoints
    # Apply change point detection using 15-day rolling window
    changepoint_start_date = current_date - timedelta(days=16)
    changepoint_start_str = changepoint_start_date.strftime('%Y-%m-%d')
    changepoint_data = decomp.filter(ibis._['TIMESTAMP'] >= changepoint_start_str
                                     ).select(
        'XD', 'TIMESTAMP',
        season_adjusted=(decomp.TRAVEL_TIME_SECONDS - decomp.season_day - decomp.season_week),
    )
    changepoint_data_df = changepoint_data.execute() # Convert to pandas for changepoint detection
    # snowflake doesn't support percentile in interval window functions, which are used in robust=True mode
    # So we will provide a pandas df and let DuckDB handle the changepoint detection
    
    changepoints_sql = changepoint(
        data=changepoint_data_df,
        value_column='season_adjusted',
        entity_grouping_column='XD',
        datetime_column='TIMESTAMP',
        rolling_window_days=14,
        score_threshold=5,
        min_separation_days=3,
        robust=True,
        min_samples=54*3,
        upper_bound=0.96,
        lower_bound=0.03,
        return_sql=True,
        dialect='duckdb',
        recent_days_for_validation=3
    )

    
    # Replace the ibis pandas memtable name with changepoint_data_df
    changepoints_sql = re.sub(r'ibis_pandas_memtable_[a-z0-9]+', 'changepoint_data_df', changepoints_sql)
    changepoints_df = duckdb.query(changepoints_sql).to_df()

    # Use Snowpark to create a TEMP table (avoids PUT)
    sp_df = session.create_dataframe(changepoints_df)
    sp_df.write.mode("overwrite").save_as_table("TEMP_CHANGEPOINTS", table_type="temporary")

    # Now, create an Ibis table expression that points to the new temporary table.
    changepoints = con.table("TEMP_CHANGEPOINTS")

    # Save changepoints for this date - filter to only include the middle day (8 days before current date)
    middle_date = current_date - timedelta(days=8)
    middle_date_str = middle_date.strftime('%Y-%m-%d')
    next_day_str = (middle_date + timedelta(days=1)).strftime('%Y-%m-%d')
    df_changepoint = changepoints.filter(
        (ibis._['TIMESTAMP'] >= middle_date_str) & 
        (ibis._['TIMESTAMP'] < next_day_str)
    )
    # Shouting case for Snowflake
    df_changepoint = df_changepoint.rename(dict(zip([x.upper() for x in df_changepoint.columns], df_changepoint.columns)))
    # Write both inserts atomically
    try:
        session.sql("BEGIN").collect()
        con.insert("TRAVEL_TIME_ANALYTICS", df_anomaly)
        con.insert("CHANGEPOINTS", df_changepoint)
        if is_last_date:
            #con.create_table("FREEFLOW", freeflow, overwrite=True) # OLD WAY, overwriting table breaks reader account access
            session.sql("TRUNCATE TABLE FREEFLOW").collect()
            con.insert("FREEFLOW", freeflow)
        session.sql("COMMIT").collect()
        return 'success'
    except Exception as e:
        session.sql("ROLLBACK").collect()
        return f'error inserting results; rolled back: {e}'
    

# Register with secrets mapping and integration
session.sproc.register(
    func=analytics_sproc,
    name="ANALYTICS_SPROC",
    imports=["ibis", "traffic_anomaly", "parsy", "pyarrow_hotfix", "sqlglot", "rich"],
    packages=[
        "snowflake-snowpark-python",
        "toolz",
        "atpublic",
        "pyarrow",
        "pandas",
        "numpy",
        "duckdb"
    ],
    input_types=[StringType(), StringType()], 
    return_type=StringType(),
    is_permanent=True,
    stage_location="@CODE_STAGE",  # Where the code is stored
    replace=True,
    execute_as="CALLER"
)

print("Stored procedure created successfully!")
os.chdir(os.path.dirname(__file__))



# Run the stored procedure
#session.call("ANALYTICS_SPROC", '2025-09-01', 'true')