# 🏎 Performance: реальные сценарии тюнинга

> Spark UI на конкретных кейсах. От симптома до диагноза до лекарства.

---

## 1. Как читать Spark UI

http://localhost:4040 пока активна SparkSession.

Главные вкладки и что в них искать:

### Jobs tab
Список всех jobs (action'ов). Что смотреть:
- **Duration** — какой job дольше.
- **Failed jobs** — есть ли красные.

### Stages tab
Внутри job'а — stages. Здесь находим медленный.

### Stage detail
Кликаем на конкретный stage:
- **Tasks count** — сколько тасков.
- **Median / Min / Max duration** — большой gap = skew.
- **Input / Shuffle Read / Shuffle Write** — IO.
- **Aggregated Metrics by Executor** — какой executor пыхтит.

### SQL tab
DAG запроса с временами. Самое информативное:
- **Время на узел** — где затык.
- **Number of output rows** — где взрыв.
- **Spilled to disk** — нет ли вытеснений из памяти.

### Storage tab
Закэшированные DataFrames.

### Executors tab
- **Memory used** — близко к лимиту?
- **GC time** — > 10% от total time = проблема.
- **Failed tasks** — есть retry?

---

## 2. Кейс 1: «Pipeline работает 2 часа, должен 20 минут»

### Симптомы
- Один stage висит долго.
- Один executor green, остальные idle.
- В Spark UI Stage detail: 1 task — 1.5ч, остальные 199 — 30сек.

### Диагноз
**Skew по join-ключу.** Один ключ (например, NULL или «system_user») имеет 10М записей, остальные — десятки.

### Лечение
1. **Включить AQE** (если выключен):
   ```python
   spark.conf.set("spark.sql.adaptive.enabled", True)
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
   ```
2. **Фильтр**:
   ```python
   df = df.filter(F.col("client_id").isNotNull())   # выкинуть мусор
   ```
3. **Salt-trick** для горячих ключей:
   ```python
   df = df.withColumn("salt", (F.rand() * 10).cast("int"))
   df = df.withColumn("client_salted", F.concat_ws("_", "client_id", "salt"))
   # join по client_salted, потом drop salt
   ```

### Как проверить
```python
df.groupBy("client_id").count().orderBy(F.col("count").desc()).show(5)
```

Если топ-1 в 100× больше топ-10 — skew подтверждён.

---

## 3. Кейс 2: «Spark часто перезапускает executor'ов»

### Симптомы
- В UI: executor'ы появляются и исчезают.
- В логах: `OutOfMemoryError`, `Container killed by YARN for exceeding memory`.
- Job в итоге успешен, но в 3× медленнее ожидаемого.

### Диагноз
**Executor'ы падают по памяти.** Spark перезапускает, делает retry — отсюда замедление.

### Возможные причины
1. **Слишком большие партиции** (по 5 ГБ каждая).
2. **Cache без `unpersist`** забил storage.
3. **Сильный skew** — одна партиция огромная.
4. **Python UDF** забирает много RAM в Python-процессе.

### Лечение
1. Увеличить executor memory:
   ```
   spark.executor.memory = 8g
   ```
2. Уменьшить размер партиций:
   ```python
   df = df.repartition(2000)   # больше партиций — меньше каждая
   ```
3. Заменить Python UDF на встроенные `F.*`.
4. Включить spill на диск:
   ```python
   spark.memory.fraction = 0.6   # default
   ```

---

## 4. Кейс 3: «Driver падает с OOM»

### Симптомы
- `Driver killed because total memory exceeded.`
- Job не успевает дойти до записи.

### Диагноз
- **`collect()` на больших данных** — тянет всё на драйвер.
- **`toPandas()`** на больших данных — то же самое.
- **Broadcast** слишком большой таблицы.
- **Слишком много партиций** — массивные структуры на driver'е.

### Лечение
```python
# Вместо
result = df.collect()
for row in result: process(row)

# Используйте
df.foreach(process)
# или
df.toLocalIterator()
```

```python
# Никогда:
df.toPandas()   # на 1 ТБ → OOM мгновенно

# Если очень надо:
df.sample(0.01).toPandas()
```

Контроль числа партиций перед write:
```python
df.coalesce(10).write.parquet(...)   # не миллион файлов
```

---

## 5. Кейс 4: «Spark «думает» 5 минут перед стартом»

### Симптомы
- Job отображается, но 5 мин нет stages.
- На SparkSession.builder...getOrCreate() тратится много времени.

### Диагноз
- **inferSchema=True** на больших файлах — двойное чтение.
- **Считается catalog** для тысяч партиций.
- **Slow Hive Metastore** — медленная база метаданных.

### Лечение
```python
# Явная схема вместо inferSchema
from pyspark.sql.types import *
schema = StructType([...])
df = spark.read.schema(schema).csv("file.csv")

# Если папок партиций тысячи — partitionDiscovery медленный
spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.threshold", "32")
```

---

## 6. Кейс 5: «Job упирается в чтение, а не в compute»

### Симптомы
- Stages быстрые (< 1 мин), кроме первого «Scan parquet» — 30 минут.
- Executor'ы стоят на «Scheduler Delay» или «Task Deserialization».

### Диагноз
- **Слишком много мелких файлов** в источнике.
- **CSV** вместо Parquet.
- **Нет partition pruning** — читается всё.

### Лечение
```python
# Если file partitioning — фильтр по партиции
df.filter("dt = '2026-05-15'")   # Spark прочитает одну папку

# Если мелких файлов много — coalesce при записи
upstream.coalesce(100).write.parquet(...)

# Перепаковать существующие файлы
# (одноразовый script)
df = spark.read.parquet("old/")
df.coalesce(100).write.parquet("new/")
```

---

## 7. Кейс 6: «Запрос с window function висит»

### Симптомы
- `df.withColumn("ma", F.avg("amount").over(window))` — 30 минут.

### Диагноз
- **partitionBy** в window порождает shuffle.
- **partitionBy(F.lit(1))** = глобальная сортировка = одна партиция.

### Лечение
```python
# ❌ Все данные в одну партицию
w = Window.partitionBy(F.lit(1)).orderBy("ts")

# ✅ Натуральный partition
w = Window.partitionBy("client_id").orderBy("ts")
```

Если глобальный window действительно нужен — рассмотрите альтернативы (например, `approxQuantile` для median вместо percentile через window).

---

## 8. Кейс 7: «Spark пишет миллионы мелких файлов»

### Симптомы
- В output папке `_temporary/` с тысячами файлов по 1 МБ.
- Финальная запись медленная.
- Downstream чтение медленное.

### Диагноз
- Слишком много **in-memory partitions** при записи.
- Если `partitionBy("year", "month")` × `200 in-memory partitions` = 200 × 12 = **2400 файлов**.

### Лечение
```python
# Repartition по тем же ключам перед записью
df.repartition("year", "month") \
  .write.partitionBy("year", "month") \
  .parquet(...)
# Теперь по одному файлу на каждый year/month

# Или контроль через config
spark.conf.set("spark.sql.files.maxRecordsPerFile", 1_000_000)
```

---

## 9. Кейс 8: «Memory error при groupBy с большим состоянием»

### Симптомы
- В streaming: state увеличивается, executor OOM.
- В batch: groupBy с миллионами уникальных ключей.

### Диагноз
- **State агрегации** не помещается в RAM.

### Лечение
1. **Approximate агрегации**:
   ```python
   df.agg(F.approx_count_distinct("user_id"))    # вместо countDistinct
   df.agg(F.expr("percentile_approx(amount, 0.5)"))   # вместо percentile
   ```
2. **Watermark в streaming** — выкидывать старый state.
3. **Pre-aggregation**: сначала агрегировать локально (например, по часам), потом по дням.

---

## 10. Кейс 9: «10× медленнее, чем коллега на той же задаче»

### Симптомы
- Ваш код 30 мин, его — 3 мин.
- На тех же данных, той же машине.

### Диагноз
Часто: **Python UDF**.

```python
# Ваш код
@F.udf("string")
def my_upper(s):
    return s.upper() if s else None
df = df.withColumn("name_upper", my_upper("name"))

# Его код
df = df.withColumn("name_upper", F.upper("name"))
```

Разница: **5-10×** на больших данных. Все встроенные `F.*` работают на JVM, без сериализации в Python.

Аналогично: `applyInPandas` + Pandas UDF — лучше обычного UDF.

---

## 11. Кейс 10: «Caching сделал хуже»

### Симптомы
- Добавил `cache()`, ожидал ускорения.
- Получил OOM или замедление.

### Диагноз
- **Кэшируете слишком много** → storage memory переполнен → spill на диск.
- **Кэшируете однократно используемое** → накладные расходы > выгода.

### Лечение
1. **Cache только то, что используется > 1 раза**.
2. **Unpersist** после использования:
   ```python
   df_cached = df.transform(big_step).cache()
   df_cached.count()    # триггер
   # ... используется в 3 местах
   df_cached.unpersist()    # освободить
   ```
3. **Storage level**: `MEMORY_AND_DISK_SER` — серилизованно, занимает 50% меньше.

---

## 12. Шаблон диагностики

Когда что-то медленно — делайте по списку:

```
1. Spark UI открыт?
2. Какой stage медленный?
3. Skew: median ≈ max? Если нет — лечите skew.
4. Память: GC time > 10%? Spilled to disk? Если да — больше памяти или меньше партиций.
5. CPU: tasks ждут друг друга? — Увеличьте параллелизм.
6. IO: scan parquet долгий? — Проблема с источником (мелкие файлы / CSV / без pruning).
7. UDF: есть Python UDF? — Замените на F.*.
8. Plan: explain показывает Exchange? — Подумайте, нужен ли shuffle.
9. AQE: включен? — Включите.
10. Memory: collect/toPandas/broadcast больших? — Перепишите.
```

---

## 13. Полезные команды

```python
# Информация о партициях
df.rdd.getNumPartitions()
df.rdd.glom().map(len).collect()    # размер каждой

# План
df.explain(True)
df.explain("formatted")              # читабельнее

# Размер DataFrame в памяти (приблизительно)
df.cache()
df.count()
# в Spark UI Storage — точный размер

# Список JIRA по slowness — открытые баги Spark
# https://issues.apache.org/jira/projects/SPARK
```

---

## 14. Спарк-тюнинг — главное правило

> **Сначала исправьте алгоритм. Потом — конфигурацию.**

Не нужно крутить `spark.executor.memory`, если у вас `df.collect()` на 1 ТБ. Алгоритм первичен.

90% «медленный Spark» — это:
- Python UDF.
- collect/toPandas.
- inferSchema.
- Skew без AQE.
- Маленькие файлы / CSV.

Это **алгоритмические** проблемы. Конфигурация Spark здесь не поможет.

---

## 15. Чек-лист «перформант я Spark»

- [ ] Открываю Spark UI и читаю.
- [ ] Знаю, что искать в Stage detail.
- [ ] Различаю skew от ровной нагрузки.
- [ ] Понимаю GC pauses.
- [ ] Не использую Python UDF без необходимости.
- [ ] Не делаю collect() / toPandas() на больших.
- [ ] Использую approximate агрегаты, где уместно.
- [ ] Знаю про partition pruning и file pruning.
- [ ] Использую broadcast для маленьких таблиц.
- [ ] Cache только многократно используемое.

После этого можно тюнить даже сложные pipeline'ы.
