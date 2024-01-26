from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("my_test_spark").getOrCreate()

spark.sql('select 1').show()

# Получение конфигурации SparkSession
conf = spark.sparkContext.getConf()

# Вывод всех настроек
all_settings = conf.getAll()

print('start')
# Вывод определенной настройки
for setting in all_settings:
    print(setting)

spark.stop()