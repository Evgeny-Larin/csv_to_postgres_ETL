from pyspark.sql import SparkSession

# Параметры подключения к PostgreSQL
jdbc_url = "jdbc:postgresql://host:port/db"
properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Создаем SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Connection") \
    .getOrCreate()

# Чтение данных из PostgreSQL
df = spark.read.jdbc(url=jdbc_url, table="ds.ft_balance_f", properties=properties)

df.createOrReplaceTempView("ft_balance_f")

query = spark.sql("""select *  from ft_balance_f""")
query.show()

# query.write \
#     .format("jdbc") \
#     .option("url", URL) \
#     .option("dbtable", "new_table") \
#     .option("user", USER) \
#     .option("password", PW) \
#     .option("driver", DRIVER).save(mode='append')

spark.stop()