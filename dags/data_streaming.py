import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from src.data_stream.kafka_producer import generate_stream
from airflow.models import Variable


PATH_STREAM = Variable.get("PATH_STREAM")

Topic1 = "transactions"

args = {
    "owner": "gizelly",
    "start_date": airflow.utils.dates.days_ago(1),
    "provide_context": True,
}

dag = DAG(
    dag_id="1_streaming",
    default_args=args,
    schedule_interval="@once",
    catchup=False,
)


task1 = PythonOperator(
    task_id="kafka_producer",
    python_callable=generate_stream,
    op_kwargs={"path_stream": PATH_STREAM, "Topic": Topic1},
    dag=dag,
)


task1
