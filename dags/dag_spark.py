from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_local_job',
    default_args=default_args,
    description='Run a Spark job locally using Airflow',
    schedule_interval=None,  # Set this to the desired schedule interval
)

spark_job_task = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit --master local[*] ./dags/utils/spark_test.py',
    dag=dag,
)

# Set task dependencies
spark_job_task

if __name__ == "__main__":
    dag.cli()
