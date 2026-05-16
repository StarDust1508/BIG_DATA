# ⚙️ Spark Internals — что внутри

> Глубокая теория про Spark, которая отличает middle от senior. Catalyst, Tungsten, code generation, память JVM, off-heap, GC, sortMerge vs broadcast.

---

## 1. Архитектура изнутри: 5 слоёв Spark

```
┌────────────────────────────────────────────┐
│  Spark SQL / DataFrame / Dataset API       │  ← вы пишете здесь
├────────────────────────────────────────────┤
│  Catalyst — оптимизатор запросов            │
├────────────────────────────────────────────┤
│  Tungsten — execution engine                │
│  (whole-stage codegen, off-heap memory)     │
├────────────────────────────────────────────┤
│  Spark Core / RDD API                       │
├────────────────────────────────────────────┤
│  Cluster Manager (YARN / K8s / Standalone)  │
└────────────────────────────────────────────┘
```

---

## 2. Catalyst — 4 фазы оптимизации

### Фаза 1: Parsing
Текст SQL / Python API → AST.

### Фаза 2: Analysis
Резолв имён колонок, проверка типов, использует Catalog (метаданные).

### Фаза 3: Logical Optimization
Применение правил:
- **Predicate pushdown** — фильтр в источник.
- **Column pruning** — не читать лишние колонки.
- **Constant folding** — `3 + 5` → `8` на этапе компиляции.
- **Boolean simplification** — `x AND TRUE` → `x`.
- **Join reordering** — поменять порядок join'ов для эффективности.
- **Subquery elimination** — превратить subquery в join.

### Фаза 4: Physical Planning
Выбор конкретного алгоритма:
- BroadcastHashJoin vs SortMergeJoin
- HashAggregate vs SortAggregate
- Partial vs Final aggregation

Catalyst — это «генеральный директор» Spark. Он смотрит ваш код и говорит executor'ам что **на самом деле** делать.

```python
df.explain(True)
# == Parsed Logical Plan ==      что вы написали
# == Analyzed Logical Plan ==    с типами
# == Optimized Logical Plan ==   после правил
# == Physical Plan ==            что реально выполнится
```

---

## 3. Tungsten — execution engine

С 2015 года Spark получил **сильное** ускорение за счёт Tungsten.

### 3.1. Whole-stage code generation

Вместо интерпретации DataFrame API «row-by-row», Tungsten **генерирует Java-байткод** для целого stage:

Было:
```
filter → project → aggregate
для каждой строки: вызов виртуального метода × 3
```

Стало:
```java
// Сгенерированный код:
while (rows.hasNext()) {
    Row r = rows.next();
    if (r.getInt(0) > 100) {       // filter inlined
        int a = r.getInt(0) * 2;    // project inlined
        accumulator.add(a);         // aggregate inlined
    }
}
```

Это даёт **2-10×** ускорение на «горячем» коде.

### 3.2. Off-heap memory

JVM-объекты тяжёлые (метаданные, GC). Tungsten хранит данные в **off-heap** памяти (через Unsafe API) в плотном бинарном формате:

```
On-heap Row:        | obj_header | type_info | array_of_objects(...) |
Off-heap Tungsten:  | 5 байт | 8 байт int | 8 байт long | ... |
```

Экономия памяти 2-5×, плюс **GC не трогает** этот пласт.

### 3.3. Cache-aware computation

Tungsten знает про L1/L2 cache процессоров и оптимизирует доступ.

---

## 4. Память executor'а

```
Total executor memory (spark.executor.memory = 8g)
│
├─ Reserved (300 MB) — для системы
│
└─ Usable memory (~7.7 GB)
   ├─ User memory (40%)
   │  └─ UDF, переменные, словари в коде
   │
   └─ Spark memory (60%)
      ├─ Storage memory (50%)
      │  └─ cache(), broadcast переменные
      │
      └─ Execution memory (50%)
         └─ shuffle, joins, aggregations, sort buffer
```

⚠️ Storage и Execution «делят» Spark memory **динамически**. Если cache забил storage, а execution требует места — Spark выселит cache (если возможно) или фолбэк на диск.

### Конфигурация
```python
# Доля памяти для Spark (vs user)
spark.memory.fraction = 0.6

# Доля Spark memory для Storage (vs Execution)
spark.memory.storageFraction = 0.5

# Использовать off-heap
spark.memory.offHeap.enabled = true
spark.memory.offHeap.size = 4g
```

---

## 5. Shuffle — что происходит под капотом

Когда у вас `groupBy` или `join`:

### 5.1. Map side
Каждый executor:
1. Запускает map-task, обрабатывает свою партицию.
2. Партиционирует выход по hash(key) на **N бакетов** (N = `shuffle.partitions`).
3. Сортирует каждый бакет.
4. Пишет на **локальный диск** executor'а.

### 5.2. Reduce side
Каждый reduce-task:
1. Тащит свои куски с других executor'ов **по сети** (через Shuffle Service или прямо).
2. Merge-sort'ит их.
3. Применяет агрегацию / join.

### 5.3. Shuffle Service

Внешний сервис рядом с executor'ом, который **продолжает** отдавать shuffle-данные, даже если executor умер. Без него — потеря executor'а = пересчёт map-stage.

```python
spark.shuffle.service.enabled = true
```

В Databricks/EMR — включён по умолчанию.

### 5.4. Стоимость shuffle

- **Disk I/O** на map-side (запись бакетов).
- **Network I/O** на reduce-side (передача).
- **CPU** на сериализацию / десериализацию.
- **Memory** на merge-sort.

Это **самая дорогая** операция в Spark.

---

## 6. Join algorithms — выбор Catalyst

### 6.1. Broadcast Hash Join
- **Когда:** одна таблица < `spark.sql.autoBroadcastJoinThreshold` (10 MB по умолчанию).
- **Как:** маленькая таблица копируется на все executor'ы, hash-таблица в RAM, join без shuffle.
- **Стоимость:** O(N) одна сторона.

### 6.2. Sort Merge Join
- **Когда:** обе таблицы большие.
- **Как:** обе шафлятся по hash(key), сортируются, merge-join.
- **Стоимость:** 2× shuffle + sort. Дорого.

### 6.3. Shuffle Hash Join
- **Когда:** обе таблицы средние, не нужна сортировка.
- **Как:** shuffle обе, hash на меньшей.
- **Менее распространён** — обычно SMJ или BHJ.

### 6.4. Broadcast Nested Loop Join
- **Когда:** нет ключа равенства, или cross-join.
- **Как:** каждая строка одной × каждая строка другой.
- **Опасно:** O(N×M). Избегать.

### Подсказки Catalyst'у
```python
# Подсказать broadcast
from pyspark.sql.functions import broadcast
big.join(broadcast(small), "id")

# Hint в SQL
SELECT /*+ BROADCAST(small) */ * FROM big JOIN small USING (id);

# Запретить broadcast
SELECT /*+ MERGE(small) */ * FROM big JOIN small USING (id);
```

---

## 7. AQE — Adaptive Query Execution (Spark 3.0+)

Революция. Catalyst планирует **статически**, AQE **переоптимизирует во время выполнения**.

### Что умеет

#### 7.1. Coalescing мелких партиций
После shuffle Catalyst планирует `shuffle.partitions=200`. Но если данных мало — это 200 крохотных партиций → overhead. AQE объединит их в 8.

#### 7.2. Skewed join optimization
Если одна партиция в 10× больше других — AQE автоматически её разрежет.

#### 7.3. Dynamic switching SMJ → BHJ
Catalyst подумал «обе таблицы большие» → SMJ. В runtime оказалось, что одна — 5 МБ → AQE переключит на BroadcastHashJoin.

### Настройки
```python
spark.sql.adaptive.enabled = true                              # default (3.0+)
spark.sql.adaptive.coalescePartitions.enabled = true            # default
spark.sql.adaptive.skewJoin.enabled = true                       # default
spark.sql.adaptive.localShuffleReader.enabled = true             # default
```

В 99% — оставлять включённым.

---

## 8. Garbage Collection (GC)

JVM использует GC. На больших данных GC может стать **главной проблемой**:

### Симптомы
- В Spark UI: tasks «висят» на этапе GC.
- Время GC > 10% от общего времени executor'а.
- OOM ошибки.

### Диагностика
В spark-submit добавить:
```bash
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
```

Лог покажет, что и сколько GC чистит.

### Лечение

1. **Использовать G1GC** (вместо CMS):
   ```
   --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"
   ```
2. **Off-heap memory** — Tungsten уже использует.
3. **Меньше executor memory + больше executor'ов** — мелкие GC паузы вместо больших.
4. **Избегать `collect()` и больших broadcast'ов**.
5. **Использовать `serializer=KryoSerializer`** (быстрее Java):
   ```
   spark.serializer = org.apache.spark.serializer.KryoSerializer
   ```

---

## 9. Сериализация

Когда executor'ы обмениваются данными — сериализация.

### Java Serializer (default)
- Универсальный, но медленный.
- Большой размер сериализованных данных.

### Kryo Serializer
- 2-10× быстрее.
- Меньше данных по сети.
- Требует регистрации классов:
   ```python
   spark.conf.set("spark.kryo.classesToRegister", "com.MyClass1,com.MyClass2")
   ```

В реальной работе с Kryo: производительность shuffle ↑ заметно.

---

## 10. Partition pruning, predicate pushdown глубоко

### 10.1. Partition pruning
Если данные физически партиционированы (`/dt=2025-05-15/`), Spark **не открывает** ненужные папки.

```python
df.filter("dt = '2025-05-15'")   # читает ОДНУ папку
```

### 10.2. File pruning (Parquet/ORC)
Parquet хранит min/max каждой колонки в footer файла. Spark читает footer, и если фильтр не пересекается с диапазоном — **пропускает файл**.

```python
df.filter("amount > 1000000")
# Файлы с max(amount) <= 1000000 не открываются
```

### 10.3. Row group / column chunk pruning
Внутри Parquet-файла данные разбиты на «row groups» (~ 128 МБ). Каждый имеет свои min/max. Spark может пропускать row groups, не читая их.

### 10.4. Column pruning
В колоночных форматах читаются **только нужные колонки**.

```python
df.select("a", "b")   # читает только колонки a и b с диска
```

---

## 11. Dynamic Partition Pruning (DPP)

Появилось в Spark 3.0. Пример:

```python
# orders партиционирован по dt
orders = spark.read.parquet("/data/orders/")    # 5 TB
small = spark.read.parquet("/data/promo/")       # 10 МБ, имеет dt

orders.join(broadcast(small), ["dt", "product_id"])
```

Без DPP: Spark прочитал бы все 5 TB orders, потом join.
С DPP: Spark **сначала** соберёт уникальные `dt` из `small`, потом прочитает только те партиции `orders`.

Это уменьшает чтение на порядок.

```python
spark.sql.optimizer.dynamicPartitionPruning.enabled = true   # default
```

---

## 12. Bucketing — продвинутая оптимизация

Если две таблицы **одинаково** забакетированы по join-ключу, join можно сделать **без shuffle**.

```python
# При записи
orders.write.bucketBy(100, "user_id").sortBy("ts").saveAsTable("orders")
events.write.bucketBy(100, "user_id").sortBy("ts").saveAsTable("events")

# Join без shuffle
spark.table("orders").join(spark.table("events"), "user_id")
```

Catalyst видит, что обе таблицы забакетированы одинаково — не делает shuffle.

⚠️ Работает только с Hive-таблицами (`saveAsTable`).

---

## 13. Spark UI глубоко

### Stages tab
- **Duration** — общее время stage.
- **Tasks: Succeeded/Failed/Total** — статистика тасков.
- **Input/Shuffle Read/Shuffle Write** — IO.
- **Locality Level** — `PROCESS_LOCAL` (best), `NODE_LOCAL`, `RACK_LOCAL`, `ANY` (worst).

### SQL tab
- Полный DAG с временами на каждой операции.
- Можно «потыкать» и увидеть, где задержка.

### Storage tab
- Какие DataFrame закэшированы.
- Сколько занимают в RAM / диске / off-heap.

### Executors tab
- Использование памяти на каждом.
- GC time.
- Failed tasks.

### Streaming tab (если стриминг)
- Batch durations.
- Input rate.
- Processing rate.

---

## 14. Чек-лист «знаю Spark глубоко»

- [ ] Понимаю, что делает Catalyst и читаю физический план.
- [ ] Знаю про Tungsten и whole-stage codegen.
- [ ] Понимаю архитектуру памяти executor'а.
- [ ] Различаю BroadcastHashJoin / SortMergeJoin.
- [ ] Знаю про AQE и его 3 главных оптимизации.
- [ ] Понимаю, как работает shuffle (map → disk → network → reduce).
- [ ] Знаю про partition / file / row group pruning.
- [ ] Различаю partition (in-memory) и Hive-partitioning (on-disk) и bucketing.
- [ ] Умею диагностировать GC pauses.
- [ ] Использую Kryo сериализацию там, где важна скорость.

После этого — middle/senior уровень владения Spark.

---

## 15. Что почитать дальше

- **«Spark: The Definitive Guide»** — Чемберс и Захариа. Главы 14–19 — internals.
- **Apache Spark Internals (Jacek Laskowski)** — бесплатно: https://books.japila.pl/apache-spark-internals/
- **«High Performance Spark»** — Холден Карау, Рейчел Ворвик.
- **Databricks Engineering Blog** — кейсы внутренней инженерии.
- Презентации с **Spark Summit / Data + AI Summit** на YouTube.
