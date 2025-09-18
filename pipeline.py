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

def pipeline_sproc(session: Session) -> str:
    from datetime import datetime, timedelta

    def _escape_sql_literal(s: str) -> str:
        # Double single quotes for SQL string literal safety and cap length
        return (s or "").replace("'", "''")[:2000]
    
    # Step 1: Get max DATE for each TYPE and insert missing dates
    max_dates = session.sql("""
        SELECT TYPE, MAX(DATE) as MAX_DATE 
        FROM run_stats 
        GROUP BY TYPE
    """).to_pandas()
    
    yesterday = datetime.now().date() - timedelta(days=1)
    
    insert_values = []
    for _, row in max_dates.iterrows():
        type_name = row['TYPE']
        max_date = row['MAX_DATE']
        
        current_date = max_date + timedelta(days=1)
        while current_date <= yesterday:
            insert_values.append(f"('{current_date}', '{type_name}')")
            current_date += timedelta(days=1)
    
    if insert_values:
        insert_sql = f"INSERT INTO run_stats (DATE, TYPE) VALUES {', '.join(insert_values)}"
        session.sql(insert_sql).collect()
    
    # Step 2: Process all jobs in order
    jobs = session.sql("""
        SELECT DATE, TYPE FROM run_stats 
        WHERE COMPLETED = FALSE AND ATTEMPTS < 3
        ORDER BY DATE ASC, TYPE DESC
    """).to_pandas()
    
    # Count total analytics jobs for is_last_date logic
    total_analytics_jobs = len(jobs[jobs['TYPE'] == 'analytics'])
    analytics_job_count = 0
    
    for _, job in jobs.iterrows():
        job_date = job['DATE']
        job_type = job['TYPE']
        current_date_str = job_date.strftime('%Y-%m-%d')
        end_date_str = (job_date + timedelta(days=1)).strftime('%Y-%m-%d')

        # Update STARTTIME and increment ATTEMPTS
        session.sql(f"""
            UPDATE run_stats 
            SET STARTTIME = CURRENT_TIMESTAMP(), ATTEMPTS = ATTEMPTS + 1
            WHERE DATE = '{current_date_str}' AND TYPE = '{job_type}'
        """).collect()
        
        try:
            start_time = datetime.now()
              
            if job_type == 'download':
                result = session.call("RITIS_SPROC", current_date_str, end_date_str)
            else:  # analytics
                analytics_job_count += 1
                is_last_date_str = 'true' if analytics_job_count == total_analytics_jobs else 'false'
                result = session.call("ANALYTICS_SPROC", current_date_str, is_last_date_str)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            if result == 'success':
                session.sql(f"""
                    UPDATE run_stats 
                    SET DURATION = {duration}, COMPLETED = TRUE
                    WHERE DATE = '{current_date_str}' AND TYPE = '{job_type}'
                """).collect()
            else:
                session.sql(f"""
                    UPDATE run_stats 
                    SET DURATION = {duration}, ERROR_MSG = '{_escape_sql_literal(result)}'
                    WHERE DATE = '{current_date_str}' AND TYPE = '{job_type}'
                """).collect()
                return f"{job_type.capitalize()} failed: {result}"
                
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            session.sql(f"""
                UPDATE run_stats 
                SET DURATION = {duration}, ERROR_MSG = '{_escape_sql_literal(str(e))}'
                WHERE DATE = '{current_date_str}' AND TYPE = '{job_type}'
            """).collect()
            return str(e)
    
    return "success"


session.sproc.register(
    func=pipeline_sproc,
    name="PIPELINE_SPROC",
    return_type=StringType(),
    is_permanent=True,
    stage_location="@CODE_STAGE",
    replace=True,
    execute_as="CALLER"
)

print("PIPELINE_SPROC created successfully!")


# Create the Task
sql = """
CREATE OR REPLACE TASK PIPELINE_TASK
  WAREHOUSE = 'TPAU_WH'
  SCHEDULE = 'USING CRON 0 3 * * * America/Los_Angeles'
AS
  CALL PIPELINE_SPROC();
"""
session.sql(sql).collect()

# Enable the task
session.sql("ALTER TASK PIPELINE_TASK RESUME;").collect()

