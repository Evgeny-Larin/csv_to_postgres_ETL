from pyspark.sql import SparkSession
import sys

# Создаем SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Connection") \
    .getOrCreate()

# получаем аргументы [jdbc_url, user, password, driver]
jdbc_url = sys.argv[1]
properties = {
    "user": sys.argv[2],
    "password": sys.argv[3],
    "driver": sys.argv[4]
}

# Чтение данных из PostgreSQL
schema = 'ds'
table = 'ft_balance_f'
df = spark.read.jdbc(url=jdbc_url, 
                     table=f'{schema}.{table}', 
                     properties=properties)

df.createOrReplaceTempView(f"{table}")

query = spark.sql(f"""select *  from {table}""")
query.show()

# query.write \
#     .format("jdbc") \
#     .option("url", URL) \
#     .option("dbtable", "new_table") \
#     .option("user", USER) \
#     .option("password", PW) \
#     .option("driver", DRIVER).save(mode='append')

spark.stop()