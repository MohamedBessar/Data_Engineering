from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'Bessar',
    'depends_on_past': False,
}

dag = DAG(
    dag_id='ETL_dag',
    default_args=default_args,
    description='ETL pipeline ending to store star schema into iceberg',
    schedule_interval=None,
    catchup=False,
)
bash_task = BashOperator(
    task_id='Starting',
    bash_command='echo "Starting the ETL... "',
    dag=dag,
)

extract_transform_task = SparkSubmitOperator(
    task_id='extract_transform_data',
    application='/home/iceberg/jobs/transform.py',
    conn_id='spark-iceberg',
    verbose=True,
    dag=dag
)
build_wh_task = SparkSubmitOperator(
    task_id='build_wh',
    application='/home/iceberg/jobs/build_warehouse.py',
    conn_id='spark-iceberg',
    verbose=True,
    dag=dag
)

load_data_task = SparkSubmitOperator(
    task_id='load_to_wh',
    application='/home/iceberg/jobs/load.py',
    conn_id='spark-iceberg',
    verbose=True,
    dag=dag
)
bash_task >> build_wh_task >> extract_transform_task >> load_data_task