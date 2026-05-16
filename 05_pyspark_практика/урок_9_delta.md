# Урок 5.9 — Delta Lake: транзакции в Data Lake

> Финал модуля. Delta Lake превращает обычные Parquet-папки в полноценную транзакционную таблицу. Это технология, которую вы будете использовать в 2026+ почти везде.

---

## Часть 1. Зачем поверх Parquet ещё что-то

Голый Parquet не умеет:
- `UPDATE` и `DELETE` отдельных записей.
- Транзакции (`ACID`).
- Time travel («покажи таблицу 7 дней назад»).
- Schema evolution с гарантиями.
- Merge (UPSERT).

Это всё нужно в реальной жизни. Особенно для:
- **Compliance:** GDPR требует удаления.
- **Корректность:** не хочется «полузаписанных» таблиц после падения.
- **Дебаг:** «что было до того, как сломали?».

---

## Часть 2. Три «Lakehouse»-формата

| Формат | От кого | Особенности |
|--------|---------|-------------|
| **Delta Lake** | Databricks | Стандарт де-факто, open source с 2019 |
| **Apache Iceberg** | Netflix → ASF | Hidden partitioning, snapshot isolation |
| **Apache Hudi** | Uber → ASF | Upsert-оптимизированный |

В курсе берём Delta — наиболее популярный в open source мире.

---

## Часть 3. Установка

В Spark 3.5+:

```bash
pip install delta-spark==3.2.0
```

В коде:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("DeltaDemo")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

---

## Часть 4. Запись и чтение

```python
# Write
df.write.format("delta").mode("overwrite").save("/data/my_delta/")

# Read
df = spark.read.format("delta").load("/data/my_delta/")

# Как Hive table
df.write.format("delta").saveAsTable("my_db.my_table")
df2 = spark.table("my_db.my_table")
```

---

## Часть 5. DELETE и UPDATE

Этого нет в голом Parquet, но есть в Delta:

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/data/my_delta/")

# DELETE
dt.delete("client_id = 12345")

# UPDATE
dt.update(
    condition = "client_id = 12345",
    set = {"email": F.lit("erased@deleted.local")}
)
```

Через SQL:
```python
spark.sql("DELETE FROM delta.`/data/my_delta/` WHERE client_id = 12345")
spark.sql("UPDATE delta.`/data/my_delta/` SET status='inactive' WHERE last_seen < '2024-01-01'")
```

---

## Часть 6. MERGE (upsert)

Самая мощная операция: «если есть — обнови, если нет — вставь».

```python
updates = spark.read.parquet("/incoming/today/")

(DeltaTable.forPath(spark, "/data/clients/")
    .alias("target")
    .merge(updates.alias("source"), "target.client_id = source.client_id")
    .whenMatchedUpdate(set={
        "email": "source.email",
        "updated_at": F.current_timestamp(),
    })
    .whenNotMatchedInsert(values={
        "client_id": "source.client_id",
        "email":     "source.email",
        "created_at": F.current_timestamp(),
    })
    .execute())
```

Один MERGE заменяет сложную логику «найти, изменить, вставить». Идеомпотентен, транзакционен.

---

## Часть 7. Time travel

Можно прочитать таблицу **на момент в прошлом**:

```python
# По версии
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/data/my_delta/")

# По времени
df_old = spark.read.format("delta") \
    .option("timestampAsOf", "2026-05-01 10:00:00") \
    .load("/data/my_delta/")
```

Применения:
- Дебаг («что было до того, как сломали»).
- Reproducible analytics («тот же отчёт, что и месяц назад»).
- Audit («какие данные мы использовали для этой модели»).

---

## Часть 8. История изменений

```python
dt.history().show(truncate=False)
```

Покажет, кто и когда писал в таблицу, какие операции (INSERT/UPDATE/DELETE/MERGE), сколько строк затронуто.

Это **журнал изменений из коробки**. Для compliance — золото.

---

## Часть 9. Schema evolution

```python
# Добавилась новая колонка
new_data.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/data/my_delta/")

# Полная замена схемы (с осторожностью)
new_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/data/my_delta/")
```

Без `mergeSchema` Delta откажется писать, если схемы не совпадают. Это **защита от случайной порчи**.

---

## Часть 10. VACUUM — очистка истории

Delta хранит **все версии**. Это занимает место. Когда вам не нужны старые версии:

```python
dt.vacuum(168)   # удалить файлы старше 168 часов (7 дней)
```

⚠️ После vacuum'а time travel в эту глубину больше не работает. Найдите баланс между retention для compliance и стоимостью хранения.

---

## Часть 11. «Право на забвение» через Delta

Реальный сценарий по GDPR Art. 17:

```python
def forget_user(client_id: int):
    spark.sql(f"""
        DELETE FROM delta.`/data/clients/`
        WHERE client_id = {client_id}
    """)
    spark.sql(f"""
        DELETE FROM delta.`/data/transactions/`
        WHERE client_id = {client_id}
    """)
    # vacuum, чтобы данные ушли физически (по retention policy)
    DeltaTable.forPath(spark, "/data/clients/").vacuum(0)
    DeltaTable.forPath(spark, "/data/transactions/").vacuum(0)
    log.info(f"User {client_id} erased per GDPR Art. 17 at {datetime.now()}")
```

Без Delta вам пришлось бы:
- Найти все Parquet-файлы с этим клиентом (сложно).
- Прочитать их.
- Записать обратно без нужных строк.
- Перестроить партиции.

С Delta — две строки SQL.

---

## Часть 12. Сравнительная таблица «Parquet vs Delta»

| | Голый Parquet | Delta Lake |
|---|---|---|
| Чтение | ✅ | ✅ |
| Запись | ✅ | ✅ |
| DELETE | ❌ | ✅ |
| UPDATE | ❌ | ✅ |
| MERGE / upsert | ❌ | ✅ |
| Транзакции | ❌ | ✅ ACID |
| Time travel | ❌ | ✅ |
| Schema evolution | частично | ✅ |
| Audit log | ❌ | ✅ |
| Размер на диске | минимальный | + ~10% overhead |
| Совместимость | везде | везде, где есть Delta |
| Стоимость освоения | низкая | средняя |

---

## ✅ Самопроверка

1. Что Delta даёт **поверх** Parquet?
2. Чем MERGE отличается от обычного UPDATE?
3. Что такое time travel и где он полезен?
4. Почему vacuum опасен, если хочется time travel?
5. Как Delta помогает в «праве на забвение»?

---

## ✅ Итог модуля 05

Поздравляю — это был **самый большой** модуль курса. Теперь вы:

- Строите ETL-пайплайны в Spark.
- Знаете очистку, нормализацию, joins, windows, partitioning.
- Понимаете тюнинг и skew.
- Кэшируете осмысленно.
- Псевдонимизируете ПДн в Spark.
- Имеете базовое представление о Delta Lake.

---

**Дальше:**
- Практики (см. список в [README модуля](./README.md)).
- Квиз модуля 05.
- Затем [06 Spark MLlib](../06_spark_MLlib/).
