# импорты операторов Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base_hook import BaseHook

# получаем объект Connection
conn = BaseHook.get_connection('postgres_conn')
# получаем параметры подключения
jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
user = conn.login
password = conn.password
driver = "org.postgresql.Driver"
# готовим к передаче в spark
application_args = [jdbc_url, user, password, driver]


from datetime import datetime

# переменные по умолчанию (наследуются всеми таксами в даге)
DEFAULT_ARGS = {
    'owner':'ELarin',
    'depends_on_past':False, # запустится ли скрипт, если прошлое выполнение было неудачным
    'start_date': datetime(2023, 6, 2), # даг начнёт выполняться начиная с ПРЕДЫДУЩЕЙ даты,
    'catchup': True, # если False - все предыдущие запуски от start_date до сегодня будут пропущены
    #'end_date': datetime(2023, 9, 3), # не будет выполняться после этой даты
}

dag =  DAG("test_dag", # название дага
         schedule_interval = '@daily', # интервал выполнения
         default_args=DEFAULT_ARGS,
         max_active_runs = 1, # сколько процессов дага будет выполняться одновременно
         )

t2 = SparkSubmitOperator(
    task_id="spark_job",
    conn_id="spark_conn",
    application="/opt/airflow/spark_jobs/test_spark_job.py",
    application_args=application_args,
    dag=dag
)


t2