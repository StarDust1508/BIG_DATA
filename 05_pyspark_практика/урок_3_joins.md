# Урок 5.3 — Joins в Spark

> Join — самая частая операция в реальных пайплайнах и **самая дорогая**, потому что вызывает shuffle. Разбираемся, как делать правильно.

---

## Часть 1. Виды join

| Тип | Что возвращает |
|-----|----------------|
| `inner` (default) | только совпадающие ключи в обеих |
| `left` | все из левой, к ним подтягиваем |
| `right` | все из правой |
| `outer` (=`full`) | всё из обеих, null где нет совпадения |
| `left_semi` | строки A, у которых есть match в B; колонки B не возвращаются |
| `left_anti` | строки A, у которых **нет** match в B |
| `cross` | декартово произведение |

```python
a.join(b, "id")                    # inner
a.join(b, "id", "left")
a.join(b, ["id1", "id2"], "inner")  # composite key
a.join(b, a.id == b.client_id)      # разные имена ключей
```

---

## Часть 2. Anti-join — суперполезный

«Что есть в A, но нет в B»:

```python
# Транзакции «осиротевших» клиентов (нет в clients)
tx.join(clients, "client_id", "left_anti")

# Эквивалент через filter (часто медленнее на больших данных)
tx.filter(~F.col("client_id").isin([r.client_id for r in clients.collect()]))
```

Anti-join гораздо эффективнее, чем `isin` со списком из `collect()` — last collect стянет всё на driver.

---

## Часть 3. Алгоритмы под капотом

Spark выбирает алгоритм автоматически:

| Алгоритм | Когда применяется | Стоимость |
|----------|-------------------|-----------|
| **BroadcastHashJoin** | Если одна таблица < `autoBroadcastJoinThreshold` (10 МБ) | Нет shuffle, быстро |
| **SortMergeJoin** | Обе таблицы большие | Shuffle, сортировка — медленно |
| **ShuffleHashJoin** | Реже, специальный случай | Shuffle, хеш-таблица |
| **BroadcastNestedLoopJoin** | Cross-join или сложные условия | Очень медленно, избегать |

Посмотреть какой выбран — через `explain()`:
```python
joined.explain(False)
# == Physical Plan ==
# *(2) BroadcastHashJoin [client_id#1], [client_id#11], Inner, BuildRight
# ...
```

---

## Часть 4. Broadcast join — пишем явно

Если одна таблица «маленькая» (≤ 100 МБ обычно норм), copy её на всех executor'ов:

```python
from pyspark.sql.functions import broadcast

big.join(broadcast(small), "id")
```

Это исключает shuffle большой таблицы → огромный выигрыш.

⚠️ Если ошиблись и попробовали broadcast'ить большую — driver упадёт по памяти. Поэтому Spark по умолчанию делает это только < 10 МБ. Можно увеличить:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)  # 100 МБ
```

---

## Часть 5. Skew в join (перекос)

Если у одного ключа **миллион значений**, а у других — десятки, этот ключ «уходит» на один executor → тот сидит часами, остальные простаивают.

```
Ключ "X"    → 1_000_000 строк  → 1 executor пыхтит 2 часа
Ключи Y..Z  → по 100 строк      → 99 executor'ов спят
```

### Симптомы

- В Spark UI: один stage висит с 1 «in-flight» task'ом часами.
- Все executor'ы в idle кроме одного.

### Решения

**1. Включить AQE skew join (Spark 3.0+):**
```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```
AQE автоматически разобьёт «перекошенную» партицию на несколько.

**2. Salt-trick** (вручную):
Добавляем случайный суффикс к перекошенному ключу:
```python
# Большая таблица: добавляем salt
big = big.withColumn("salt", (F.rand() * 10).cast("int"))
big = big.withColumn("salted_key", F.concat_ws("_", "client_id", "salt"))

# Маленькая таблица: размножаем на 10 копий с разным salt
small = small.crossJoin(spark.range(10).withColumnRenamed("id", "salt"))
small = small.withColumn("salted_key", F.concat_ws("_", "client_id", "salt"))

# Join по salted_key
big.join(small, "salted_key").drop("salt", "salted_key")
```

Это уменьшает «горячий» ключ в 10 раз → нагрузка распределяется.

---

## Часть 6. Несколько ключей и неравенства

```python
# Composite key
a.join(b, ["country", "city"])

# Условный join (НЕ равенство — careful!)
a.join(b, (a.user_id == b.user_id) & (a.ts < b.ts))

# Range join (часто медленный)
a.join(b, (a.ts >= b.start) & (a.ts <= b.end))
```

Range joins — отдельная тема. Spark 3.5+ имеет «range join optimizer», но в общем случае лучше избегать.

---

## Часть 7. Self-join

«Найти пары записей одного клиента в течение часа»:

```python
a = tx.alias("a")
b = tx.alias("b")
pairs = a.join(b,
    (F.col("a.client_id") == F.col("b.client_id")) &
    (F.col("a.ts") < F.col("b.ts")) &
    ((F.col("b.ts") - F.col("a.ts")) < F.expr("INTERVAL 1 HOUR")))
```

⚠️ Self-join на больших таблицах — взрыв. Часто лучше через window function.

---

## Часть 8. Дубликаты и неуникальные ключи

Если ключ **неуникален в правой таблице** — после join строк станет **больше**, чем в левой.

```python
# Контроль
print("Left:", a.count())
joined = a.join(b, "id", "left")
print("After join:", joined.count())   # если больше — там дубли по ключу в b
```

Дедупликация перед join:
```python
b_unique = b.dropDuplicates(["id"])   # оставит произвольную из дубликатов
# или с приоритетом (по window'у)
```

---

## Часть 9. Шпаргалка «как выбирать»

```
Один из датасетов маленький (< 100 МБ)?
   ├── Да ──► BROADCAST join. Никакого shuffle.
   └── Нет, оба большие ─┐
                         │
                Есть skew по ключу?
                   ├── Да ──► включи AQE skewJoin + при необходимости salt
                   └── Нет ──► обычный SortMergeJoin
                                    │
                                    ▼
                          Партиционируй обе по ключу
                          одинаково для лучшей локальности.
```

---

## ✅ Самопроверка

1. Когда `left_anti` лучше, чем `filter(~isin(...))`?
2. Что такое broadcast join и каковы его ограничения?
3. Что такое skew и как с ним бороться?
4. Что произойдёт, если в правой таблице ключ неуникален?
5. Зачем в self-join добавляют условие `a.ts < b.ts`?

---

## ▶️ Дальше

[Урок 5.4 — Window functions](./урок_4_windows.md)
