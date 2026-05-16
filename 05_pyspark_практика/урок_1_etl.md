# Урок 5.1 — Полноценный ETL-пайплайн

> ETL = Extract → Transform → Load. Это «хлеб с маслом» дата-инженера. После урока вы строите свой первый продакшен-стиль pipeline.

---

## Часть 1. Зачем ETL и из чего состоит

```
ИСТОЧНИКИ  →  EXTRACT  →  TRANSFORM  →  LOAD  →  ХРАНИЛИЩЕ
 (Postgres,                                       (Parquet,
  логи, API,                                       Delta,
  CSV)                                             DWH)
```

ETL — это **процесс**, не одна операция. У него есть:
- **Расписание** (раз в час, раз в день, по событию).
- **Идемпотентность** (повторный запуск даёт тот же результат).
- **Мониторинг** (метрики, логи, оповещения).
- **Версии** (схемы, кода, данных).
- **Качество** (тесты, валидация).

Spark — это **только инструмент** для T (transform). E и L часто делают другие средства (Airflow, Kafka, dbt). Но в Spark можно весь pipeline целиком.

---

## Часть 2. Каноничная структура pipeline'а

```python
def run_pipeline(spark, run_date):
    raw = extract(spark, run_date)        # E: чтение из источника
    cleaned = clean(raw)                   # T1: очистка
    enriched = enrich(cleaned, run_date)   # T2: обогащение
    aggregated = aggregate(enriched)       # T3: агрегация
    load(aggregated, run_date)             # L: запись

    return aggregated.count()              # для мониторинга
```

Каждая функция:
- принимает DataFrame, возвращает DataFrame;
- не имеет «побочных эффектов» кроме I/O в начале/конце;
- легко тестируется (можно подсунуть маленький DataFrame).

---

## Часть 3. ETL-стиль написания кода

Я рекомендую такой стиль:

```python
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def clean_transactions(df: DataFrame) -> DataFrame:
    """Нормализация транзакций.

    - убирает дубликаты по (client_id, ts)
    - приводит category к нижнему регистру
    - приводит amount к double, отрицательные → null (битые)
    - парсит ts в timestamp
    """
    return (df
        .dropDuplicates(["client_id", "ts"])
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("amount",
            F.when(F.col("amount") < 0, None)
             .otherwise(F.col("amount").cast("double")))
        .withColumn("ts", F.to_timestamp("ts"))
    )
```

Преимущества:
- Каждая функция «читается как абзац».
- Тесты для функции писать просто.
- `chain` через несколько таких функций — это и есть pipeline.

---

## Часть 4. Идемпотентность

**Идемпотентность** — это когда повторный запуск пайплайна не ломает данные и даёт тот же результат.

Например, плохо:
```python
df.write.mode("append").parquet("out/")    # каждый запуск ДОПИСЫВАЕТ
```
Перезапуск удвоит данные. Если у вас retry — плохо.

Хорошо:
```python
df.write.mode("overwrite").parquet(f"out/dt={run_date}/")
```
Партиция конкретной даты перезаписывается целиком. Можно перезапускать сколько угодно.

Эту идею называют **«overwrite partition»** — стандартный паттерн в Big Data.

---

## Часть 5. Партиционированный вывод по дате

Большие пайплайны почти всегда пишут так:

```
output_dir/
   dt=2026-05-15/
      part-00000.parquet
      part-00001.parquet
   dt=2026-05-16/
      part-00000.parquet
   ...
```

```python
df.write \
  .mode("overwrite") \
  .partitionBy("dt") \
  .parquet("output_dir/")
```

Преимущества:
- При чтении `WHERE dt = '...'` — Spark прочитает только нужную партицию.
- Перезапуск конкретного дня — только эта партиция.
- История остаётся.

⚠️ В Spark есть нюанс: при `mode("overwrite")` он по умолчанию **перезаписывает ВСЁ**, а не только указанную партицию. Нужно:
```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

Теперь `overwrite` затрагивает только те партиции, что фактически приходят в записи. Это и есть «dynamic partition overwrite» — то, чего обычно хотят.

---

## Часть 6. Параметризация по дате

```python
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True)   # 2026-05-16
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ETL").getOrCreate()
    run_pipeline(spark, args.run_date)
```

Запуск:
```bash
spark-submit my_etl.py --run-date 2026-05-16
```

В Airflow:
```python
BashOperator(
    bash_command="spark-submit my_etl.py --run-date {{ ds }}",
)
```

`{{ ds }}` — это шаблон Airflow, который подставит логическую дату запуска.

---

## Часть 7. Логи и метрики

Spark пишет в `stderr` свои логи (level WARN/ERROR/INFO). Поверх рекомендую:

```python
import logging
log = logging.getLogger("etl")
log.setLevel(logging.INFO)

log.info(f"Reading source for {run_date}")
raw = extract(spark, run_date)
log.info(f"Rows extracted: {raw.count():,}")
# ...
```

Метрики, которые стоит собирать в каждом ETL:
- `rows_read`
- `rows_written`
- `duration_sec`
- `rejected_rows` (некачественные)
- Хеш входных файлов (для версионирования)

В реальности это всё пишется в Prometheus, Datadog или аналоги.

---

## Часть 8. Тесты для Spark-кода

Spark-функции тестируются как обычный Python:

```python
def test_clean_amount_negative_becomes_null(spark):
    df = spark.createDataFrame([(1, -10.0), (2, 5.0)], ["id", "amount"])
    out = clean_amount(df)
    rows = out.collect()
    assert rows[0]["amount"] is None
    assert rows[1]["amount"] == 5.0
```

Spark-сессия для тестов одна на сессию pytest (фикстура).

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()
```

---

## Часть 9. Юридический угол

В каждом продакшен-ETL должны быть «контрольные точки» по праву:

1. **На входе** (Extract): минимизация — берём только нужные поля.
2. **После Extract**: псевдонимизация ПДн (см. урок 5.8).
3. **Между стадиями**: данные шифруются at rest (если хранятся).
4. **На выходе**: партиционирование по дате → удобно реализовать «храним 365 дней, потом удаляем».
5. **Audit log**: кто запустил, какие параметры, что прочитал, что записал.

В критичных пайплайнах добавляют **DQ (data quality) проверки**:
```python
assert df.filter(F.col("amount") < 0).count() == 0, "Found negative amounts!"
assert df.filter(F.col("client_id").isNull()).count() == 0, "Found null client_ids!"
```

Если проверка падает — pipeline остановлен, данные не публикуются.

---

## Часть 10. Шаблон полного скрипта

См. [практика_1_etl.py](./практика_1_etl.py). Структура:

```
extract(spark, date) → raw_df
   ↓
clean(raw_df) → cleaned
   ↓
enrich(cleaned, clients) → enriched
   ↓
aggregate(enriched) → daily_agg
   ↓
load(daily_agg, date) → write_parquet_partitioned
```

---

## ✅ Самопроверка

1. Что такое идемпотентность и почему она важна?
2. В чём проблема `mode("append")` и как её решает `partitionOverwriteMode=dynamic`?
3. Как параметризовать ETL по дате?
4. Назовите 3 метрики, которые имеет смысл собирать в каждом ETL.
5. Что такое DQ-проверка и где её ставить?

---

## ▶️ Дальше

[Урок 5.2 — Очистка и нормализация в Spark](./урок_2_очистка.md)
