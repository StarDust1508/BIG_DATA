# ⚡ PySpark Cheat Sheet

> Команды, которые покрывают 90% реальной работы.

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
```

## Сессия
```python
spark = (
    SparkSession.builder
    .appName("App")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
```

## Чтение / запись
```python
df = spark.read.csv("path.csv", header=True, inferSchema=True)
df = spark.read.parquet("path.parquet")
df = spark.read.json("path.json")
df = spark.read.option("multiline", True).json("path.json")

# Запись
df.write.mode("overwrite").parquet("out.parquet")
df.write.mode("append").partitionBy("year", "month").parquet("out")
df.write.csv("out", header=True)
```

## Разведка
```python
df.printSchema()
df.show(10, truncate=False)
df.describe().show()
df.summary("count", "mean", "stddev").show()
df.count()
df.columns
df.dtypes
```

## Выборка / фильтр
```python
df.select("a", "b", F.col("c") * 2)
df.filter(F.col("amount") > 1000)
df.where("amount > 1000 AND currency = 'RUB'")
df.dropDuplicates(["id"])
df.dropna(subset=["client_id"])
df.fillna({"amount": 0, "name": "unknown"})
```

## Колонки
```python
df = df.withColumn("amount_usd", F.col("amount") / 90)
df = df.withColumnRenamed("client_id", "client")
df = df.drop("tmp_col")
df = df.withColumn("ts", F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss"))
```

## Агрегации
```python
df.groupBy("category").agg(
    F.count("*").alias("n"),
    F.sum("amount").alias("total"),
    F.avg("amount").alias("avg"),
    F.expr("percentile_approx(amount, 0.5)").alias("median"),
)
```

## Соединения
```python
joined = a.join(b, on="id", how="inner")
# how: inner | left | right | outer | left_semi | left_anti

# Broadcast join — если b маленький
from pyspark.sql.functions import broadcast
a.join(broadcast(b), "id")
```

## Окна
```python
from pyspark.sql.window import Window

w = Window.partitionBy("client").orderBy(F.col("ts"))
df = df.withColumn("running_total", F.sum("amount").over(w))
df = df.withColumn("rank", F.row_number().over(w))
df = df.withColumn("prev_amount", F.lag("amount", 1).over(w))
```

## SQL
```python
df.createOrReplaceTempView("tx")
spark.sql("""
    SELECT category, SUM(amount) AS total
    FROM tx
    WHERE currency = 'RUB'
    GROUP BY category
""").show()
```

## Типы и схемы
```python
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("ts", TimestampType(), True),
])
df = spark.read.csv("f.csv", schema=schema, header=True)
```

## Производительность

| Действие | Эффект |
|---|---|
| `cache()` / `persist()` | держать в памяти между несколькими actions |
| `repartition(N)` | новый shuffle, ровные партиции |
| `coalesce(N)` | уменьшить партиции БЕЗ shuffle |
| `broadcast(small_df)` | join без shuffle, если small_df < ~10 МБ |
| `partitionBy(...)` при записи | физическое разбиение в файловой системе |
| `EXPLAIN`, `df.explain()` | посмотреть физический план |

## Типичные «грабли»

- `inferSchema=True` дважды читает файл. Лучше явно задать схему.
- `collect()` тянет ВСЕ данные на драйвер — упадёт на больших.
- `udf` медленнее встроенных функций. Заменяйте на `F.*`.
- `groupBy().count().show()` хорошо, а `.collect()` — плохо.
- Партиций по умолчанию `spark.sql.shuffle.partitions=200`. Для маленьких — много, для больших — мало. Тюньте.

## Полезное
```python
df.repartition(8).rdd.getNumPartitions()
df.rdd.glom().map(len).collect()  # размер каждой партиции
spark.catalog.listTables()
spark.stop()
```
