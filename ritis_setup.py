'''
This script sets up a Snowflake stored procedure to download and store INRIX XD travel time data from the RITIS API.
It requires tables already set up in Snowflake, see setup.sql.

The created stored procedure is named RITIS_SPROC and takes two parameters: start_date and end_date in 'YYYY-MM-DD' format.
end_date IS EXCLUSIVE!
Running it will result in downloading that date range of data for all XD segments in DIM_SIGNALS_XD and storing it in XD_TRAVEL_TIME.

Checks for data completeness are performed, and if the last hour of data does not contain at least 70% of the expected segments,
the procedure will return 'Incomplete Data' and not store any data.
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

# Method to automate attaching and uploading custom packages
def add_packages(d, session):
    import ritis_inrix_api
    for module in (ritis_inrix_api,):
        pkgname = module.__name__
        pkgpath = os.path.join(d, pkgname)
        ignore = shutil.ignore_patterns("*.so", "*.pyd", "*.dll", "*.egg-info", "*.dist-info", "__pycache__")
        shutil.copytree(os.path.dirname(module.__file__), pkgpath, ignore=ignore)
        session.add_import(pkgname, import_path=pkgname)


d = tempfile.TemporaryDirectory()
os.chdir(d.name)
add_packages(d.name, session)

# Define the stored procedure
def ritis_sproc(session: Session, start_date, end_date) -> str:
    # start_date/end_date format: 'YYYY-MM-DD'
    import _snowflake
    import duckdb
    from ritis_inrix_api import RITIS_Downloader
    START_TIME = '06:00:00'
    END_TIME = '20:59:00'

    api_key = _snowflake.get_generic_secret_string('ritis_key')  # 'ritis_key' is the alias defined below

    # Query distinct XD values and convert to list
    segments = session.sql("SELECT DISTINCT XD FROM DIM_SIGNALS_XD").to_pandas()['XD'].tolist()
    
    updater = RITIS_Downloader(
        api_key=api_key,
        segments=segments,
        start_time=START_TIME,
        end_time=END_TIME,
        bin_size=15,
        units='seconds',
        columns=['travel_time'],
        confidence_score=[30]
    ) 

    df = updater.single_download(start_date, end_date, 'snowflake')
    
    # Data completeness check using DuckDB
    # Count total distinct XD_IDs
    total_distinct_xd = duckdb.sql("SELECT COUNT(DISTINCT xd_id) as count FROM df").fetchone()[0]
    
    # Count distinct XD_IDs in the last hour of data (using end_date - 1 day + END_TIME as the reference)
    last_hour_distinct_xd = duckdb.sql(f"""
        SELECT COUNT(DISTINCT xd_id) as count 
        FROM df 
        WHERE measurement_tstamp >= ('{end_date} {END_TIME}'::TIMESTAMP - INTERVAL '1 day' - INTERVAL '1 hour')
    """).fetchone()[0]
    
    # Check if last hour has > 70% of total distinct XD_IDs
    if last_hour_distinct_xd / total_distinct_xd <= 0.7:
        return 'Incomplete Data'
    
    # Snowflake requires upper case
    df.columns = [c.upper() for c in df.columns]
    session.write_pandas(df, 'XD_TRAVEL_TIME',  use_logical_type=True)
    
    return 'success'


# Register with secrets mapping and integration
session.sproc.register(
    func=ritis_sproc,
    name="RITIS_SPROC",
    imports=["ritis_inrix_api"], # Custom packages
    packages=["requests", "duckdb", "pandas"], # Custom packages dependencies
    input_types=[StringType(), StringType()],
    return_type=StringType(),
    is_permanent=True,
    stage_location="@CODE_STAGE",  # Where the code is stored
    replace=True,
    external_access_integrations=["pda_api_access_integration"],  # Must match integration name
    secrets={"ritis_key": "SHAWNS_RITIS_KEY"}  # Alias: secret name
)

print("Stored procedure created successfully!")

# Change directory back before cleanup to avoid Windows file handle issues
os.chdir(os.path.dirname(__file__))
