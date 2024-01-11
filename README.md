# Проектное задание Full Cycle ETL Engineer

## Порядок установки и запуска
1. (При работе на Windwos) Устанавить WSL и Ubuntu 22.04
2. Переключиться на среду Ubuntu
3. Клонировать репозиторий
4. **Отредактировать права на чтение и запись для папки data: `chmod -R 777 data`**
5. Запустить Airflow в папке с docker-compose.yaml командой: `docker compose up`
6. Войти в веб-интерфейс Airflow по адресу `localhost:8080`. Логин `airflow`, пароль `airflow`
7. Добавить Connection к БД Postgres с именем `postgres_conn`
8. Запустить DAG `csv_to_postgres`   

За основу взят оригинальный docker compose файл: [ссылка](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)  
