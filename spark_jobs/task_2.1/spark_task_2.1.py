from pyspark.sql import SparkSession 

# создаём новую спарк сессию
spark = SparkSession.builder \
         .appName('task_2.2') \
         .getOrCreate()

"""
Проверочные задачи:

1. Сгенерировать DataFrame из трёх колонок (row_id, discipline, season) - олимпийские дисциплины по сезонам.
    row_id - число порядкового номера строки;
    discipline - наименование олимпийский дисциплины на английском (полностью маленькими буквами);
    season - сезон дисциплины (summer / winter);

    Укажите не менее чем по 5 дисциплин для каждого сезона. Сохраните DataFrame в csv-файл, разделитель колонок табуляция, первая строка должна содержать название колонок. Данные должны быть сохранены в виде 1 csv-файла а не множества маленьких.

"""

# данные для df
data = [
    (1, 'athletics', 'summer'),
    (2, 'swimming', 'summer'),
    (3, 'gymnastics', 'summer'),
    (4, 'volleyball', 'summer'),
    (5, 'cycling', 'summer'),
    (6, 'skiing', 'winter'),
    (7, 'bobsleigh', 'winter'),
    (8, 'figure skating', 'winter'),
    (9, 'ice hockey', 'winter'),
    (9, 'snowboarding', 'winter')
]

# создаём df
df = spark.createDataFrame(data, ['row_id', 'discipline', 'season'])

# сохраняем в один csv файл с заголовками и разделителями ,
df.coalesce(1).write\
  .mode('overwrite')\
  .options(header='True', sep=',')\
  .csv(r'olympic_sport.csv')

"""
2. Прочитайте исходный файл "Athletes.csv". Посчитайте в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие. Результат сохраните в формате parquet.
"""

# путь до файла Athletes.csv
path = r'./Athletes.csv'

# читаем файл Athletes.csv
athletes = spark.read\
           .options(header='True', sep = ';')\
           .csv(path)

# переводим во вьюшку для работы в spark.sql
athletes.createOrReplaceTempView('athletes')

# считаем количество спортсменов в каждой дисциплине
athletes = spark.sql("""
        select
            Discipline as discipline,
            count(Name) as count_members
        from athletes
        group by Discipline
        order by count_members desc
""")

# сохраняем в Parquet
athletes.coalesce(1).write\
        .mode('overwrite') \
        .parquet('athletes.parquet')

"""
Прочитайте исходный файл "Athletes.csv"
Посчитайте в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие
Получившийся результат нужно объединить с сгенерированным вами DataFrame из 1-го задания и в итоге вывести количество участников, только по тем дисциплинам, что есть в вашем сгенерированном DataFrame
Результат сохраните в формате parquet.
"""

# переведём сгенерированный df в spark.sql
df.createOrReplaceTempView('df')

# объединим таблицы athletes и df
merge_df = spark.sql("""
      -- группировка участников по дисциплинам
      with group_ath as (
        select Discipline as discipline, count(Name) as count_members
        from athletes
        group by Discipline
        order by count_members desc
      )

      -- объединение с df, у которого initcap(discipline)
      select g_ath.discipline, count_members
      from group_ath as g_ath
      join (select row_id, initcap(discipline) as discipline, season from df) as df
      on g_ath.Discipline = df.discipline
""")

# смотрим на результат
merge_df.show()

# сохраняем в Parquet
merge_df.coalesce(1).write\
        .mode('overwrite') \
        .parquet('merge_df.parquet')