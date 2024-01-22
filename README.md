# Проектное задание Full Cycle ETL Engineer

### Порядок установки и запуска
1. (При работе на Windwos) Устанавить WSL и Ubuntu 22.04
2. Переключиться на среду Ubuntu
3. Клонировать репозиторий
4. **Отредактировать права на чтение и запись для некоторых папок: `chmod -R 777 airflow data`**
5. Запустить Airflow в папке с docker-compose.yaml командой: `docker compose up`
6. Войти в веб-интерфейс Airflow по адресу `localhost:8080`. Логин `airflow`, пароль `airflow`
7. Добавить Connection к БД Postgres с именем `postgres_conn`  

## Задание 1.1 [требования](https://github.com/Evgeny-Larin/csv_to_postgres_ETL/blob/main/project_objectives/objective_1.1.pdf)
### Решение  
1. **[create_tables_ds, create_tables_logs]** В БД создаются схемы ds и logs, а также таблицы, если их нет    
2. **[etl_started]** В таблицу логов записывается строка о начале ETL процесса  
3. Для каждого файла в папке data/raw:  
3.1 **[extr_file]** Файл читается в кодировке cp866, из БД получаются имена ожидаемых столбцов и первичных ключей, в соответсвии с ними файл преобразуется и сохраняется  
3.2 В таблицу логов записывается строка о результате извлечения и преобразования файла  
3.3 **[load_file]** В соответсвующую таблицу БД записывается содержимое преобразованного файла, при изменении существующих строк - они перезаписываются  
3.4 В таблицу логов записывается строка о результате записи данных в таблицу БД  
4. **[etl_started]** В таблицу логов записывается строка об окончании ETL процесса  

**Видеодемонстрация процесса:** [ссылка](https://drive.google.com/file/d/10DpndEC5icDB0mITDhGlHNfAgIwYZkeE/view?usp=sharing) 

## Задание 1.2 [требования](https://github.com/Evgeny-Larin/csv_to_postgres_ETL/blob/main/project_objectives/objective_1.2.pdf)
### Решение
1. **[create_tables_dm]** Создается схема dm и таблицы-витрины dm_account_turnover_f и dm_f101_round_f
2. **[dm_account_turnover]** Рассчитывается витрина dm_account_turnover
3. **[dm_f101_round_f]** Рассчитывается витрина dm_f101_round_f

**Видеодемонстрация процесса:** [ссылка](https://drive.google.com/file/d/1wqge5w1zh3Lph4Y_4QBEpsBzuJIm9WO3/view?usp=sharing) 

## Примечания
+ Остановить и удалить контейнеры, volumes и загруженные образы: `docker compose down --volumes --remove-orphans --rmi all`  
+ За основу взят оригинальный docker compose файл: [ссылка](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)  
+ В файле .env можно задавать дополнительные переменные окружения  
