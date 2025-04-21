from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pipeline.elt_pipeline import run_pipeline


default_args = {
    'owner': 'abdirahman',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 4, 20)
}

with DAG(
    dag_id='run_pipeline_daily',
    default_args=default_args,
    description='Run daily DAG to run the ETL pipeline',
    schedule='@daily',
    catchup=False,
    tags=['pipeline']
) as dag:

    run_pipeline_task = PythonOperator(
        task_id='run_pipeline_task',
        python_callable=run_pipeline,
        dag=dag
    )