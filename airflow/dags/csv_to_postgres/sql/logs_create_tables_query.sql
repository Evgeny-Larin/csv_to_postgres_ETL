-- создаём схему
create schema if not exists "logs";

-- создаём таблицу логов 
create table if not exists 
	logs.csv_to_postgres_dag (
		execution_datetime TIMESTAMP not null,
		event_datetime TIMESTAMP not null,
		event_name VARCHAR(30) not null,
		event_status VARCHAR(30)
);