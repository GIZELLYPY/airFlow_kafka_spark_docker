import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models.xcom import XCom
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import BranchPythonOperator
import logging


CLIENT = "kafka:9092"
TOPICS = "transactions"


checkpoint_trans_path = "/tmp/checkpoint/transaction_checkpoint"

args = {
    "owner": "gizelly",
    "description": "spark consumer via bash Operator",
    "start_date": airflow.utils.dates.days_ago(1),
    "provide_context": True,
}

def pull(**kwargs):
    test_result = kwargs['ti'].xcom_pull(task_ids='test_data')
    print(test_result)
    if test_result is False:
        return "falha_nos_dados"
    else:
        return "select_data_and_save"
    

dag = DAG(
    dag_id="2_consumer",
    default_args=args,
    schedule_interval="@once",
    catchup=False,
    params={
        "checkpoint_trans_path": checkpoint_trans_path,
        "CLIENT": CLIENT,
        "TOPICS": TOPICS,
    },
)


task1 = BashOperator(
    task_id="create_dir_transaction_checkpoint",
    bash_command="mkdir -p {{ params.checkpoint_trans_path }}",
    dag=dag,
)

task2 = BashOperator(
    task_id="pyspark_consumer",
    bash_command="/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit "
    "--master local[*] "
    '--conf "spark.driver.extraClassPath=$SPARK_HOME/jars/kafka-clients-1.1.0.jar" '
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 "
    "/usr/local/airflow/dags/src/spark_consume_data/pyspark_consumer.py "
    "{{ params.checkpoint_trans_path }} "
    "{{ params.CLIENT }} "
    "{{ params.TOPICS }}",
    dag=dag,
)

task3 = BashOperator(
    task_id="test_data",
    bash_command="/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit "
    "--master local[*] "
    "/usr/local/airflow/dags/src/test_data/test_data.py",
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

task4 = BranchPythonOperator(
    task_id="check_test", 
    python_callable=pull, 
    provide_context=True,
    do_xcom_pull=True,
    dag=dag,
)


task5 = DummyOperator(task_id="falha_nos_dados", dag=dag)
task6 = DummyOperator(task_id="select_data_and_save", dag=dag)

task7 = BashOperator(
    task_id="queries_based_on_places",
    bash_command="/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit "
    "--master local[*] "
    "/usr/local/airflow/dags/src/spark_consume_data/local_queries.py",
    dag=dag,
)

task8 = BashOperator(
    task_id="queries_based_on_ride_amount",
    bash_command="/opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit "
    "--master local[*] "
    "/usr/local/airflow/dags/src/spark_consume_data/ride_amount_queries.py",
    dag=dag,
)

task1 >> task2 >> task3 >> task4 >> [task5, task6]
task6 >> [task7, task8]
