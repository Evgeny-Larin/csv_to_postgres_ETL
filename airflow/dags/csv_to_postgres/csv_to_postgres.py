# импорты Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.common.sql.sensors.sql import SqlSensor

# прочие импорты
import pandas as pd
from datetime import datetime
import os


#-----------------#
# значения jinja templates
yesterday_ds = '{{ yesterday_ds }}'

# путь до файлов csv
path_data = r'/opt/airflow/data/csv_to_postgres/raw'
# список файлов csv
files = os.listdir(path_data)

# путь до обработанных csv
done_data = r'/opt/airflow/data/csv_to_postgres/done'

# подключение к postgres
pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

# схема для данных
schema = 'ds'

# схема для логов
logs_schema = 'logs'
logs_table = 'csv_to_postgres_dag'


#-----------------#


# функция для записи состояния выполненной таски в логи
def logs_callback(context):
    task_instance = context['task_instance'].task_id
    status = context['task_instance'].state
    ts = context['task_instance'].execution_date.timestamp()
    ts = datetime.fromtimestamp(ts).isoformat(sep='T')

    query = f"""
            INSERT INTO {logs_schema}.{logs_table} (execution_datetime, event_datetime, event_name, event_status)
            VALUES ('{ts}', '{datetime.now().isoformat(sep='T')}', '{task_instance}', '{status}');
             """

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    conn.close()
    

# функция запросов к БД через курсор
def sql_query(sql, pg_hook):
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return rows

# функция чтения csv и обработки файла 
def read_data(path_data, file, done_data, schema, pg_hook, **context):
    df = pd.read_csv(fr'{path_data}/{file}.csv', sep=';', encoding='cp866', index_col=0, dtype=str)

    # получаем список столбцов из таблицы БД
    sql = f"""
        select 
            column_name,
            data_type
        from information_schema.columns
        where 
            table_schema = '{schema}' AND  
            table_name = '{file}'
        order by ordinal_position 
        """
    table_columns = sql_query(sql, pg_hook)

    # в исходнике оставляем только те строки, которые мы ожидаем в таблице БД
    column_names = [row[0] for row in table_columns]
    df.columns = df.columns.str.lower()
    df = df[column_names]

    # передаём column_names в xcom
    context['task_instance'].xcom_push(key="column_names", value=column_names)

    # находим столбцы с типом данных дата - приводим их к тому же типу в dataframe
    for i in table_columns:
        if i[1] == 'date':
            column = i[0]
            df[column] = pd.to_datetime(df[column], dayfirst=True, errors='ignore')

    # получаем список первичных ключей из таблицы БД, удаляем дубликаты по первичным ключам
    sql = f"""
        select 
            a.attname as primary_keys
        from pg_index i
        join pg_attribute a 
        on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
        where i.indrelid = '{schema}.{file}'::regclass
        and i.indisprimary;
        """
    primary_keys = sql_query(sql, pg_hook)
    primary_keys = [row[0] for row in primary_keys]
    df.drop_duplicates(primary_keys, inplace=True)

    # передаём primary_keys в xcom
    context['task_instance'].xcom_push(key="primary_keys", value=primary_keys)

    df.to_csv(f'{done_data}/{file}.csv', sep=';', encoding='utf-8', index=False, header=False)


# функция для загрузки в postgres готовых csv с хуком к 'postgres_conn', при конфликтах перезаписывается 
def export_data(done_data, file, schema, pg_hook, **context):
    primary_keys = context['task_instance'].xcom_pull(key="primary_keys", task_ids=f"extr_{file}") # получаем список primary_keys
    column_names = context['task_instance'].xcom_pull(key="column_names", task_ids=f"extr_{file}") # получаем список column_names
    update_columns = [c for c in column_names if c not in primary_keys]                            # определяем столбцы, которые будем update
    update_set = ", ".join([f"{v}=EXCLUDED.{v}" for v in update_columns])                          # подготовка шаблона для запроса
    primary_keys = ', '.join(primary_keys)

    sql = f"""
            begin;
            create temporary table tmp_table 
            (like {schema}.{file} including defaults)
            on commit drop;
                
            copy tmp_table from stdin delimiter ';';
                
            insert into {schema}.{file}
            select *
            from tmp_table
            on conflict ({primary_keys}) do update
            set {update_set};
            commit;
          """
    pg_hook.copy_expert(sql, f'{done_data}/{file}.csv')

# функция сохранения таблицы БД в csv
# был бы пользователь суперюзер Postgres - COPY (SELECT * FROM table) TO 'path\to\file.csv' WITH CSV HEADER
def sql_to_csv(path, schema, table, pg_hook, yesterday_ds):
    sql = f"""
            select *
            from {schema}.{table}
            where dt = '{yesterday_ds}'
          """
    df_f101 = pd.read_sql(sql, con=pg_hook.get_conn())
    # меняем значения в столбце regn
    df_f101['regn'] = 2000
    df_f101.to_csv(f'{path}/{yesterday_ds}_{table}.csv', sep=',', index=False)


# функция сохранения csv в таблицу БД 
def csv_v2_to_sql(pg_hook, path, schema, table, yesterday_ds):
    pg_hook.copy_expert(f"copy {schema}.{table}_v2 from stdin delimiter ',' csv header", f'{path}/{yesterday_ds}_{table}.csv')


#-----------------#
    

# даг который скачивает и объединяет данные
DEFAULT_ARGS = {'owner':'ELarin'}

dag = DAG(dag_id="csv_to_postgres",
          start_date=datetime(2018,1,10),
          schedule_interval = '@daily',
          max_active_runs=1,
          default_args=DEFAULT_ARGS)



#-----------------#

# сенсор, проверяющий подключение к БД
check_conn = SqlSensor(
    task_id='check_conn',
    conn_id='postgres_conn',
    sql='select 1',
    mode='poke',
    dag=dag
)


# таска, создающая схему ds и таблицы, если их нет
create_tables_ds = PostgresOperator(
    task_id='create_tables_ds',
    postgres_conn_id='postgres_conn',
    sql=r'sql/ds_create_tables_query.sql',
    autocommit=True,
    dag=dag) 

# таска, создающая схему logs и таблицы, если их нет
create_tables_logs = PostgresOperator(
    task_id='create_tables_logs',
    postgres_conn_id='postgres_conn',
    sql=r'sql/logs_create_tables_query.sql',
    autocommit=True,
    dag=dag) 

# таска, создающая схему dm и таблицы, если их нет
create_tables_dm = PostgresOperator(
    task_id='create_tables_dm',
    postgres_conn_id='postgres_conn',
    sql=r'sql/dm_create_tables_query.sql',
    autocommit=True,
    dag=dag) 

# лог о начале etl процесса
logs_etl_started = DummyOperator(
    task_id='etl_started',
    dag=dag,
    on_success_callback=logs_callback) 


# генератор тасков
extract_transform_tasks = []
load_tasks = []
for file in files:
    file = file.replace('.csv', '')

    # таски для чтения и обработки csv файлов
    extract_transform_tasks.append(PythonOperator(
                            task_id=f"extr_{file}",
                            dag=dag,
                            provide_context=True,
                            python_callable=read_data,
                            op_kwargs={
                                'path_data':path_data,
                                'file':file,
                                'done_data':done_data,
                                'schema':schema,
                                'pg_hook':pg_hook},
                            on_failure_callback=logs_callback,
                            on_success_callback=logs_callback))
    
    # таски для записи csv в postgres
    load_tasks.append(PythonOperator(
                            task_id=f"load_{file}",
                            dag=dag,
                            provide_context=True,
                            python_callable=export_data,
                            op_kwargs={
                                'file':file,
                                'done_data':done_data,
                                'schema':schema,
                                'pg_hook':pg_hook},
                            on_failure_callback=logs_callback,
                            on_success_callback=logs_callback))
    
    extract_transform_tasks[-1] >> load_tasks[-1]

# таска, вычисляющая и записывающая витрину dm_account_turnover_f
dm_account_turnover = PostgresOperator(
    task_id='dm_account_turnover',
    postgres_conn_id='postgres_conn',
    sql=r'sql/dm_insert_into_dm_account_turnover_f.sql',
    autocommit=True,
    on_success_callback=logs_callback,
    dag=dag) 

# таска, вычисляющая и записывающая витрину dm_f101_round_f
dm_f101_round_f = PostgresOperator(
    task_id='dm_f101_round_f',
    postgres_conn_id='postgres_conn',
    sql=r'sql/dm_insert_into_dm_f101_round_f.sql',
    autocommit=True,
    on_success_callback=logs_callback,
    dag=dag) 

# таска, сохраняющая отчёт 101 за прошлый опер день в csv
to_csv_dm_f101_round_f = PythonOperator(
    task_id='to_csv_dm_f101_round_f',
    dag=dag,
    provide_context=True,
    python_callable=sql_to_csv,
    op_kwargs={
    'path':done_data,
    'schema':'dm',
    'table':'dm_f101_round_f',
    'pg_hook':pg_hook,
    'yesterday_ds':yesterday_ds},
    on_success_callback=logs_callback
)

# таска, записывающая отчёт 101 v2 за прошлый опер день в БД
to_sql_dm_f101_round_f_v2 = PythonOperator(
    task_id='to_sql_dm_f101_round_f_v2',
    dag=dag,
    provide_context=True,
    python_callable=csv_v2_to_sql,
    op_kwargs={
    'pg_hook':pg_hook,
    'path':done_data,
    'schema':'dm',
    'table':'dm_f101_round_f',
    'yesterday_ds':yesterday_ds},
    on_success_callback=logs_callback
)

# лог об окончании etl процесса
logs_etl_ended = DummyOperator(
    task_id='etl_ended',
    dag=dag,
    on_success_callback=logs_callback,
    trigger_rule='all_done') 


check_conn >> [create_tables_ds, create_tables_logs, create_tables_dm] >> logs_etl_started >> extract_transform_tasks

load_tasks >> dm_account_turnover >> dm_f101_round_f >> to_csv_dm_f101_round_f >> to_sql_dm_f101_round_f_v2 >> logs_etl_ended