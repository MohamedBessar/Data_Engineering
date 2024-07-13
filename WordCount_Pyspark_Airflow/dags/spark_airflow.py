from airflow import DAG
import airflow
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='sparking_flow',
    default_args={
        'owner':'Mohamed Bessar',
        'start_date':airflow.utils.dates.days_ago(1)
    },
    schedule='@daily'
)


start = PythonOperator(
    task_id = 'start',
    python_callable= lambda: print('Jobs started'),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id = 'word_count',
    conn_id = 'spark_conn',
    application='/media/cattoocule/e7/study/Data-Engineer/Projects/WordCount_Pyspark_Airflow/jobs/python/word_count.py',
    dag=dag
)

end = PythonOperator(
    task_id = 'end',
    python_callable= lambda: print('Jobs are ended'),
    dag=dag
)

start >> python_job >> end 