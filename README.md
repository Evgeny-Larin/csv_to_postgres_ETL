# Образ airflow
Запускается в папке с docker-compose командой:  
`docker compose up`  

Добавлен Nginx для проксирования запросов к вебсерверу Airflow  
Nginx вещает с localhost:8080  

Чтобы дать консольную команду airflow, который запущен через Docker нужно ввести:
`docker compose run airflow-worker airflow команда`  

В файле .env можно задавать дополнительные переменные окружения  

Планы:
+ Добавить certbot  
+ Добавить генерацию fetch_key  
+ Добавить min.io  
+ Разобраться как правильно создать админа

Оригинальный compose файл: [ссылка](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)  

Гайд по добавлению дашбордов: [ссылка](https://www.youtube.com/watch?v=xyeR_uFhnD4&list=PLzKRcZrsJN_xcKKyKn18K7sWu5TTtdywh&index=7) и [ссылка](https://www.youtube.com/watch?v=CZS4fAfWcR4&list=PLzKRcZrsJN_xcKKyKn18K7sWu5TTtdywh&index=7) 
Гайд по добавлению CI/CD: [ссылка](https://startdatajourney.com/ru/course/apache-airflow-2/modules/22/74/10)
