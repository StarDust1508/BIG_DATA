# Урок 4.4 — Spark SQL и Catalyst-оптимизатор

> Spark SQL — это «волшебство», превращающее ваш простой код в эффективный распределённый план. Понять Catalyst = понять, почему Spark быстр.

---

## Часть 1. Два способа: DataFrame API и SQL

В Spark **DataFrame API и SQL — это одно и то же** под капотом. Catalyst переводит и то, и другое в один AST и оптимизирует одинаково.

### DataFrame API
```python
df = spark.read.parquet("tx/")
result = (df
    .filter("currency = 'RUB'")
    .groupBy("category")
    .agg(F.sum("amount").alias("total"))
    .orderBy(F.col("total").desc())
)
result.show()
```

### Spark SQL — точно такой же результат
```python
df.createOrReplaceTempView("tx")
result = spark.sql("""
    SELECT category, SUM(amount) AS total
    FROM tx
    WHERE currency = 'RUB'
    GROUP BY category
    ORDER BY total DESC
""")
result.show()
```

**Когда какой подход:**
- SQL — для аналитиков и читаемости сложных запросов.
- DataFrame API — для программных трансформаций, переиспользования функций.
- Часто **смешивают**: вычистили в DataFrame API, потом сложный отчёт SQL'ем.

---

## Часть 2. Catalyst — что он делает

Catalyst — это компилятор-оптимизатор Spark SQL. На вашем простом запросе он выполняет 4 фазы:

```
1. Parsing            (текст SQL → AST)
2. Analysis           (резолв имён колонок, проверка типов)
3. Logical optimization (правила: predicate pushdown, column pruning, ...)
4. Physical planning   (выбор алгоритмов: broadcast vs sort-merge join)
```

Результат: эффективный физический план, который реально и исполняется.

### Простой пример оптимизации

Вы написали:
```python
df.filter("a > 5").select("a", "b").filter("b < 100").show()
```

Catalyst оптимизирует в:
```python
df.select("a", "b").filter("a > 5 AND b < 100").show()
```

И при чтении Parquet передаст фильтр **в сам файл** (predicate pushdown):
```
read parquet only rows where (a > 5 AND b < 100), only columns (a, b)
```

То есть из 100 ГБ диска прочитается, может быть, 1 ГБ. **Это и есть «магия Spark»**.

---

## Часть 3. Как посмотреть план — `explain`

```python
result.explain(True)
```

Покажет 4 плана:

```
== Parsed Logical Plan ==      (как написали)
== Analyzed Logical Plan ==    (с типами)
== Optimized Logical Plan ==   (после правил)
== Physical Plan ==            (что реально исполнится)
```

Это главный инструмент тюнинга. Если запрос медленный — `explain`, читаем физический план, ищем bottleneck.

### Что искать в физическом плане

- `BroadcastHashJoin` — хорошо (маленькая таблица в памяти всех executor'ов).
- `SortMergeJoin` — большой и дорогой.
- `Exchange` — это shuffle. Меньше = быстрее.
- `Scan parquet` — обратите внимание на `PushedFilters` и `ReadSchema`.

---

## Часть 4. Predicate pushdown

Главная оптимизация для аналитики. Идея:

```
БЕЗ pushdown:
   Spark читает все 100 ГБ → фильтрует → оставляет 1%

С pushdown:
   Parquet-движок сам пропускает блоки, не подходящие под фильтр → 
   читается только 1 ГБ
```

Работает для:
- Parquet, ORC (отлично).
- JSON, CSV — частично или никак.
- JDBC — если БД поддерживает.

Это ещё один аргумент за Parquet, как мы обсуждали в [уроке 1.4](../01_основы_BigData/урок_4_форматы_хранения.md).

---

## Часть 5. Column pruning

```python
df.select("a", "b").filter("a > 5")
```

Catalyst говорит Parquet'у: «читай только колонки a и b». Колонки c, d, e — даже не открывает.

Если у вас Parquet с 200 колонками, а запросу нужно 5 — Spark читает только 2.5% диска.

---

## Часть 6. Partition pruning

Если данные **физически партиционированы** по диску (Hive-style: `/year=2025/month=05/`), Spark прочитает только нужные папки.

```python
df = spark.read.parquet("/data/transactions/")     # партиционировано по year=, month=

# Catalyst видит фильтр и читает только year=2026, month=05
df.filter("year = 2026 AND month = 5").show()
```

---

## Часть 7. Broadcast join

Когда одна из таблиц **маленькая** (≤ 10 МБ по умолчанию), Spark копирует её на каждого executor'а. Тогда join происходит **без shuffle** — каждый executor берёт свою партицию большой таблицы и стыкует с локальной копией маленькой.

```python
from pyspark.sql.functions import broadcast

big.join(broadcast(small), "id")
```

⚠️ Если ошибочно сделать `broadcast` для большой — driver упадёт по памяти.

Настройка автоматики:
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)   # 10 МБ
```

---

## Часть 8. AQE — Adaptive Query Execution

С Spark 3.0+ Catalyst умеет **переоптимизировать запрос во время выполнения**. Включён по умолчанию:

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

Что умеет:
- Слить мелкие партиции, если их слишком много.
- Переключить sort-merge на broadcast, если узнал реальный размер.
- Лечить skewed join (когда один ключ — миллиарды строк).

В 99% случаев — оставляем включённым.

---

## Часть 9. SQL-возможности, которые часто забывают

### Window functions
```sql
SELECT
    client_id, ts, amount,
    SUM(amount) OVER (PARTITION BY client_id ORDER BY ts) AS running_total,
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ts) AS rn
FROM tx
```

### CTE (WITH ...)
```sql
WITH big_clients AS (
    SELECT client_id, SUM(amount) AS total
    FROM tx
    GROUP BY client_id
    HAVING total > 100000
)
SELECT t.* FROM tx t
JOIN big_clients b USING (client_id)
```

### CASE WHEN
```sql
SELECT
    CASE
        WHEN amount < 100   THEN 'small'
        WHEN amount < 10000 THEN 'medium'
        ELSE 'big'
    END AS bucket,
    COUNT(*) AS n
FROM tx
GROUP BY bucket
```

### explode (JSON / массивы)
```sql
SELECT id, exploded
FROM (SELECT id, EXPLODE(items) AS exploded FROM orders)
```

---

## Часть 10. Регистрация UDF в SQL

```python
@F.udf("string")
def my_upper(s):
    return s.upper() if s else None

spark.udf.register("my_upper", my_upper)

spark.sql("SELECT my_upper(name) FROM tx").show()
```

⚠️ Помните: Python UDF медленные. Лучше использовать `F.upper` или другие встроенные.

---

## ✅ Самопроверка

1. Что делает Catalyst и какие фазы он проходит?
2. Что такое predicate pushdown?
3. Где смотреть план запроса?
4. Что значит `Exchange` в физическом плане?
5. Когда полезен broadcast join?
6. Что такое AQE и какие задачи он решает?

---

## ▶️ Дальше

[Урок 4.5 — Lazy evaluation на примерах](./урок_5_lazy.md)
