from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

#DAG FOR SIMULATING ETL SCHEDULED ON 2013-02-08
with DAG('run_etl_20130208', start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), schedule_interval=None,
         catchup=False, default_args=default_args,
) as dag:
    run_etl_20130208 = DatabricksRunNowOperator(task_id='run_databricks_job',
                                                databricks_conn_id='databricks_conn', 
                                                job_id='151019715636669', 
                                                json={'notebook_params': {'rundate': '20130208_000000'}})
