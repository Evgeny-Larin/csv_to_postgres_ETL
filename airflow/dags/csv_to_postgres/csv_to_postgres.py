# импорты Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

# прочие импорты
import pandas as pd
from datetime import datetime
import os

#-----------------#


# путь до файлов csv
path_data = r'/opt/airflow/data/csv_to_postgres/raw'
# список файлов csv
files = os.listdir(path_data)

# путь до обработанных csv
done_data = r'/opt/airflow/data/csv_to_postgres/done'

# подключение к postgres
pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
cursor = pg_hook.get_conn().cursor()

# схема для загрузки
schema = 'ds'

# схема для логов
logs_schema = 'logs'
logs_table = 'csv_to_postgres_dag'

#-----------------#


# функция запросов к БД через курсор
def sql_query(sql, cursor):
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows

# функция чтения csv и обработки файла 
def read_data(path_data, file, done_data, schema, cursor, **context):
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
    table_columns = sql_query(sql, cursor)

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
            print(column) #
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
    primary_keys = sql_query(sql, cursor)
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
            BEGIN;
            CREATE TEMPORARY TABLE tmp_table 
            (LIKE {schema}.{file} INCLUDING DEFAULTS)
            ON COMMIT DROP;
                
            COPY tmp_table FROM STDIN DELIMITER ';';
                
            INSERT INTO {schema}.{file}
            SELECT *
            FROM tmp_table
            ON CONFLICT ({primary_keys}) DO UPDATE
            SET {update_set};
            COMMIT;
          """
    pg_hook.copy_expert(sql, f'{done_data}/{file}.csv')


#-----------------#
    

# даг который скачивает и объединяет данные
DEFAULT_ARGS = {'owner':'ELarin'}

dag = DAG(dag_id="csv_to_postgres",
          start_date=datetime(2024,1,1),
          schedule_interval = '@once',
          max_active_runs=1,
          default_args=DEFAULT_ARGS)



# таска, создающая схему ds и таблицы, если их нет
create_tables_ds = PostgresOperator(
    task_id='create_tables_ds',
    postgres_conn_id='postgres_conn',
    sql=r'sql/ds_create_tables_query.sql',
    autocommit=True,
    dag=dag
) 

# таска, создающая схему logs и таблицы, если их нет
create_tables_logs = PostgresOperator(
    task_id='create_tables_logs',
    postgres_conn_id='postgres_conn',
    sql=r'sql/logs_create_tables_query.sql',
    autocommit=True,
    dag=dag
) 

# записываем в логи старт дага
logs_dag_started = PostgresOperator(
    task_id='logs_dag_started',
    postgres_conn_id='postgres_conn',
    sql=r'sql/logs_record.sql',
    params={'logs_schema':logs_schema,
            'logs_table':logs_table,
            'event_datetime':datetime.now().isoformat(sep='T'),
            'event_name':'dag_started',
            'event_status':'complite'},
    autocommit=True,
    dag=dag
) 


# генератор тасков
extract_transform_tasks = []
logs_ex_tr_tasks = []
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
                                'cursor':cursor}))
    
    logs_ex_tr_tasks.append(PostgresOperator(
                            task_id=f'logs_extr_{file}',
                            postgres_conn_id='postgres_conn',
                            sql=r'sql/logs_record.sql',
                            params={'logs_schema':logs_schema,
                                    'logs_table':logs_table,
                                    'event_datetime':datetime.now().isoformat(sep='T'),
                                    'event_name':f'extr_{file}',
                                    'event_status':'complite'},
                            autocommit=True,
                            dag=dag))
    
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
                                'pg_hook':pg_hook}))
    
    extract_transform_tasks[-1] >> logs_ex_tr_tasks[-1] >> load_tasks[-1]

# записываем в логи конец дага
logs_dag_ended = PostgresOperator(
    task_id='logs_dag_ended',
    postgres_conn_id='postgres_conn',
    sql=r'sql/logs_record.sql',
    params={'logs_schema':logs_schema,
            'logs_table':logs_table,
            'event_datetime':datetime.now().isoformat(sep='T'),
            'event_name':'dag_ended',
            'event_status':'complite'},
    autocommit=True,
    dag=dag
) 

[create_tables_ds, create_tables_logs] >> logs_dag_started >> extract_transform_tasks

load_tasks >> logs_dag_ended