from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from airflow.exceptions import AirflowException

def check_report_table_exists(**kwargs):
    """Check if the REPORT table exists, fail if it doesn't"""
    print("=== CHECKING REPORT TABLE EXISTENCE ===")
    
    hook = MsSqlHook(mssql_conn_id='mssql_conn', schema='rlass')
    
    # Query to check if table exists
    check_query = """
    SELECT COUNT(*) as table_count
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'DBO' 
    AND TABLE_NAME = 'REPORT'
    """
    
    try:
        result = hook.get_first(sql=check_query)
        table_count = result[0] if result else 0
        
        if table_count == 0:
            error_msg = """
            ❌ CRITICAL ERROR: Source table RLASS.DBO.REPORT does not exist!
            
            The DAG cannot proceed without the source REPORT table.
            Please ensure the REPORT table is created and populated before running this DAG.
            
            Expected table structure should include columns:
            - TYPE_OF_FAILURE (INT)
            - TIME_REPAIR (FLOAT)
            - COST (FLOAT)
            """
            print(error_msg)
            raise AirflowException(error_msg)
        else:
            # Also check if table has data
            count_query = "SELECT COUNT(*) FROM RLASS.DBO.REPORT"
            row_count = hook.get_first(sql=count_query)[0]
            
            print(f"✅ REPORT table exists with {row_count} records")
            
            if row_count == 0:
                warning_msg = "⚠️ WARNING: REPORT table exists but is empty!"
                print(warning_msg)
            
            return {"table_exists": True, "row_count": row_count}
            
    except Exception as e:
        error_msg = f"❌ ERROR checking REPORT table: {str(e)}"
        print(error_msg)
        raise AirflowException(error_msg)

def show_results(**kwargs):
    """Display the query results in logs"""
    print("=== DISPLAYING QUERY RESULTS ===")
    
    hook = MsSqlHook(mssql_conn_id='mssql_conn', schema='rlass')
    
    # Re-run the same query to get results
    query = "SELECT * FROM RLASS.DBO.REPORT_ANALYSIS"
    
    # Get results as DataFrame for nice formatting
    df = hook.get_pandas_df(sql=query)
    
    print(f"\n📊 FAILURE ANALYSIS RESULTS ({len(df)} records)")
    print("=" * 65)
    
    # Display formatted table
    print(df.to_string(index=False, float_format='{:.2f}'.format))
    
    print("\n" + "=" * 65)
    print(f"✅ Analysis complete - processed {len(df)} failure types")

dag = DAG(
    'simple_mssql_process',
    description='Simple MSSQL process',
    catchup=False
)

task_check_report_table = PythonOperator(
    task_id='check_report_table_exists',
    python_callable=check_report_table_exists,
    dag=dag
)

task_drop_clipping = SQLExecuteQueryOperator(
    task_id='drop_clipping_table',
    conn_id='mssql_conn',
    sql="DROP TABLE IF EXISTS RLASS.DBO.REPORT_CLIPPING;",
    dag=dag
)

task_drop_analysis = SQLExecuteQueryOperator(
    task_id='drop_analysis_table',
    conn_id='mssql_conn',
    sql="DROP TABLE IF EXISTS RLASS.DBO.REPORT_ANALYSIS;",
    dag=dag
)


task_create_clipping = SQLExecuteQueryOperator(
    task_id='create_clipping_table',
    conn_id='mssql_conn',
    sql="CREATE TABLE RLASS.DBO.REPORT_CLIPPING (TYPE_OF_FAILURE INT, TIME_REPAIR FLOAT, COST FLOAT);",
    dag=dag
)

task_create_analysis = SQLExecuteQueryOperator(
    task_id='create_analysis_table',
    conn_id='mssql_conn',
    sql="CREATE TABLE RLASS.DBO.REPORT_ANALYSIS (TYPE_OF_FAILURE INT, MEAN_TIME_REPAIR FLOAT, MEAN_COST FLOAT);",
    dag=dag
)

task_report_clipping = SQLExecuteQueryOperator(
    task_id='generate_the_report_clipping',
    conn_id='mssql_conn',
    sql="""
    INSERT INTO RLASS.DBO.REPORT_CLIPPING (TYPE_OF_FAILURE, TIME_REPAIR, COST)
    SELECT DISTINCT TYPE_OF_FAILURE, TIME_REPAIR, COST
    FROM RLASS.DBO.REPORT
    WHERE COST >= 0;
    """,
    dag=dag
)

task_analysis = SQLExecuteQueryOperator(
    task_id='generate_the_analysis_data',
    conn_id='mssql_conn',
    sql="""
        INSERT INTO RLASS.DBO.REPORT_ANALYSIS (TYPE_OF_FAILURE, MEAN_TIME_REPAIR, MEAN_COST)
        SELECT TYPE_OF_FAILURE, (SUM(TIME_REPAIR )/COUNT(*)) MEAN_TIME_REPAIR, SUM(COST)/COUNT(*) MEAN_COST
        FROM RLASS.DBO.REPORT_CLIPPING
        GROUP BY TYPE_OF_FAILURE 
        ORDER BY TYPE_OF_FAILURE;
        """,
    dag=dag
)

task_display_analysis = PythonOperator(
    task_id='display_results',
    python_callable=show_results,
    dag=dag
)

task_check_report_table >> [task_drop_clipping, task_drop_analysis]
task_drop_clipping >> task_create_clipping
task_drop_analysis >> task_create_analysis
[task_create_clipping, task_create_analysis] >> task_report_clipping >> task_analysis >> task_display_analysis