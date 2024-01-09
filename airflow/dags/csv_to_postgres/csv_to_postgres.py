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
# схема для загрузки
schema = 'ds'
pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
cursor = pg_hook.get_conn().cursor()

#-----------------#


# функция запросов к БД через курсор
def sql_query(sql, cursor):
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows

# функция чтения csv и обработки файла 
def read_data(path_data, file, done_data, schema, cursor):
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

    df.to_csv(f'{done_data}/{file}.csv', sep=';', encoding='utf-8', index=False, header=False)

# функция для загрузки в postgres готовых csv с хуком к 'postgres_conn' 
def export_data(done_data, file, schema, pg_hook):
    pg_hook.copy_expert( f"COPY {schema}.{file} FROM STDIN DELIMITER ';'" , f'{done_data}/{file}.csv')


#-----------------#
    

# даг который скачивает и объединяет данные
DEFAULT_ARGS = {'owner':'ELarin'}

dag = DAG(dag_id="csv_to_postgres",
          start_date=datetime(2024,1,1),
          schedule_interval = '@once',
          max_active_runs=1,
          default_args=DEFAULT_ARGS)

# таска, создающая схемы и таблицы, если их нет
create_tables = PostgresOperator(
    task_id='create_chema_tables',
    postgres_conn_id='postgres_conn',
    sql=r'sql/create_tables_query.sql',
    dag=dag
) 



# генератор тасков
truncate_tables_tasks = []
extract_transform_tasks = []
load_tasks = []
for file in files:
    file = file.replace('.csv', '')

    # таски, очищающие таблицы перед записью
    truncate_tables_tasks.append(PostgresOperator(
                            task_id = f'truncate_{file}',
                            dag = dag,
                            postgres_conn_id='postgres_conn',
                            sql = f'TRUNCATE TABLE {schema}.{file}'))

    # таски для чтения и обработки csv файлов
    extract_transform_tasks.append(PythonOperator(
                            task_id=f"extr_{file}",
                            dag=dag,
                            python_callable=read_data,
                            op_kwargs={
                                'path_data':path_data,
                                'file':file,
                                'done_data':done_data,
                                'schema':schema,
                                'cursor':cursor}))
    
    # таски для записи csv в postgres
    load_tasks.append(PythonOperator(
                            task_id=f"load_{file}",
                            dag=dag,
                            python_callable=export_data,
                            op_kwargs={
                                'file':file,
                                'done_data':done_data,
                                'schema':schema,
                                'pg_hook':pg_hook}))
    
    truncate_tables_tasks[-1] >> extract_transform_tasks[-1] >> load_tasks[-1]


create_tables >> truncate_tables_tasks
