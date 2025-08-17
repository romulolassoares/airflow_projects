from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd

# [START default_args]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'sample_data_etl',
    default_args=default_args,
    description='Sample Data ETL',
    catchup=False
)

# Your original SQL task
t1 = SQLExecuteQueryOperator(
    task_id='sample_data_precheck',
    conn_id='mssql_conn',
    sql="""use rlass; 
           select type_of_failure, 
                  (sum(time_repair)/count(*)) as mean_time_repair, 
                  sum(cost)/count(*) as mean_cost 
           from report
           group by type_of_failure 
           order by type_of_failure;
        """,
    dag=dag
)

# Add this task to display the results
def show_results(**kwargs):
    """Display the query results in logs"""
    print("=== DISPLAYING QUERY RESULTS ===")
    
    hook = MsSqlHook(mssql_conn_id='mssql_conn', schema='rlass')
    
    # Re-run the same query to get results
    query = """
    SELECT type_of_failure, 
           (SUM(time_repair)/COUNT(*)) AS mean_time_repair, 
           SUM(cost)/COUNT(*) AS mean_cost 
    FROM report
    GROUP BY type_of_failure 
    ORDER BY type_of_failure
    """
    
    # Get results as DataFrame for nice formatting
    df = hook.get_pandas_df(sql=query)
    
    print(f"\n📊 FAILURE ANALYSIS RESULTS ({len(df)} records)")
    print("=" * 65)
    
    # Display formatted table
    print(df.to_string(index=False, float_format='{:.2f}'.format))
    
    print("\n" + "=" * 65)
    print(f"✅ Analysis complete - processed {len(df)} failure types")

t2 = PythonOperator(
    task_id='display_results',
    python_callable=show_results,
    dag=dag
)

# Set task dependency
t1 >> t2