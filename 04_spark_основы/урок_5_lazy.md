# Урок 4.5 — Lazy evaluation на конкретных примерах

> Тема короткая, но её недопонимание приводит к 50% всех багов начинающих в Spark.

---

## Часть 1. Lazy — это не «глюк», это фича

```python
df = spark.read.csv("100gb.csv")          # 0 секунд
df = df.filter("amount > 100")             # 0 секунд
df = df.groupBy("city").count()             # 0 секунд

print(df)                                  # 0 секунд — выводит DataFrame[...]
                                            # данные НЕ загружены, ничего НЕ посчитано

df.show()                                  # ВОТ ТЕПЕРЬ Spark начинает работу
```

Это поведение **не баг**. Если бы Spark считал каждое `.filter` сразу — он не мог бы оптимизировать pipeline.

---

## Часть 2. Action vs Transformation в одном списке

| Transformation (lazy) | Action (eager) |
|----------------------|----------------|
| `select`, `filter`, `where` | `show`, `count`, `collect` |
| `groupBy`, `agg`, `join` | `take`, `first`, `head` |
| `withColumn`, `drop` | `write`, `save` |
| `orderBy`, `distinct` | `foreach`, `foreachPartition` |
| `union`, `intersect` | `toPandas` |
| `repartition`, `coalesce` | `describe().show()` |
| `cache`, `persist` (отложенная!) | `toLocalIterator` |

Запомните: **action возвращает не-DataFrame** (число, список, ничего/save). Transformation возвращает DataFrame.

---

## Часть 3. «Скрытые» action'ы

Иногда вы не подозреваете, что делаете action:

```python
print(df)                          # не action (выведет план)
print(df.count())                  # action!

for row in df.collect():           # action!
    print(row)

df.cache()                          # это маркировка, НЕ action
df.cache().count()                  # вот тут реально кэширует
```

Особенно — `display(df)` в Databricks или `df` в последней ячейке Jupyter — оба запускают `show()`.

---

## Часть 4. Side effect: каждый action может перевычислять с нуля

```python
df = spark.read.csv("big.csv")
df = df.filter("complex condition")

df.count()   # 1-й action — Spark читает CSV, фильтрует, считает
df.count()   # 2-й action — Spark СНОВА читает CSV, СНОВА фильтрует, считает
```

Это часто **не то**, что вы хотите. Решение: `cache`.

```python
df = df.cache()
df.count()   # 1: читает + кэширует + считает
df.count()   # 2: из кэша — быстро
df.count()   # 3: из кэша — быстро
```

---

## Часть 5. Конкретный пример «WTF почему так медленно»

Распространённый антипаттерн:

```python
df = spark.read.csv("big.csv")
df = df.filter("...")
df = df.join(other, "id")

# Анализ внутри цикла
for category in df.select("category").distinct().collect():    # ACTION 1
    sub = df.filter(F.col("category") == category[0])
    print(category[0], sub.count())                              # ACTION на каждой итерации
```

Если 10 категорий — Spark **10 раз** перечитает большой CSV, **10 раз** сделает join. Кошмар.

Правильно:
```python
df = df.cache()
df.count()    # триггер кэша
for category in df.select("category").distinct().collect():
    print(category[0], df.filter(F.col("category") == category[0]).count())
```

Ещё лучше — без цикла:
```python
df.groupBy("category").count().show()
```

Одно action, оптимизированное Catalyst'ом.

---

## Часть 6. Lineage и отказоустойчивость

Каждый DataFrame помнит, **как был построен**. Это называется **lineage** (родословная).

Зачем: если executor падает, Spark смотрит lineage и **перевычисляет** потерянный кусок с предыдущего checkpoint'а. Не нужно перезапускать всю задачу.

```python
df = spark.read.csv("a.csv")
df = df.filter(...)
df = df.join(other, "id")
df = df.groupBy(...).agg(...)
# Lineage: csv → filter → join → groupBy
```

Это и есть «Resilient» в R**D**D и «**отказоустойчивость**».

---

## Часть 7. Когда транформация неожиданно eager

Несколько операций lazy-делают вид, что lazy, но втайне делают работу:

- `inferSchema=True` в `read.csv` — читает файл, чтобы понять типы.
- `df.cache()` — сама по себе lazy. Кэшируется на первом action.
- `df.checkpoint()` — eager (если установлен checkpoint dir).
- `df.persist(StorageLevel.MEMORY_ONLY)` — lazy. Триггерится на action.

---

## Часть 8. Практические правила

1. **Один action в конце** — золотое правило.
2. Если нужно несколько action'ов на одном DataFrame — **cache** его.
3. После cache — обязательно один action (`count()`), чтобы кэш прогрелся.
4. Используйте `explain` для проверки плана **до** action'а — это быстро.
5. Избегайте циклов с action'ами внутри. Замените на `groupBy` или `join`.

---

## ✅ Самопроверка

1. Что произойдёт, если написать `df = df.filter("a > 5")` и не вызвать никакой action?
2. Сколько раз Spark прочтёт CSV, если вы вызовете `count()` дважды без cache?
3. Что значит «lineage» в контексте DataFrame?
4. `print(df)` — это action?
5. Когда cache — обязательный?
6. Назовите 3 неочевидных action'а.

---

## ▶️ Дальше

[Урок 4.6 — Pandas vs Spark: бенчмарк и выбор](./урок_6_pandas_vs_spark.md)
