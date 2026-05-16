# Урок 5.4 — Window functions: скользящие, ранки, лаги

> Это «суперсила» SQL и Spark. Когда `groupBy` мало — берёте window.

---

## Часть 1. В чём идея окна

`groupBy` агрегирует и **схлопывает** строки. Window — это «вычислить агрегат **сохранив все строки**».

```python
# groupBy: возвращает 5 строк (по числу категорий)
df.groupBy("category").agg(F.avg("amount"))

# window: возвращает все исходные строки + новую колонку с avg по категории
from pyspark.sql.window import Window
w = Window.partitionBy("category")
df.withColumn("avg_amount_in_cat", F.avg("amount").over(w))
```

---

## Часть 2. Анатомия Window

```python
from pyspark.sql.window import Window

w = (Window
    .partitionBy("client_id")           # как groupBy — «по чему делим»
    .orderBy(F.col("ts"))                # в каком порядке
    .rowsBetween(Window.unboundedPreceding, Window.currentRow))   # «рамка»
```

| Элемент | Что значит |
|---------|------------|
| `partitionBy` | внутри какой группы считаем |
| `orderBy` | в каком порядке (для running, lag/lead, rank) |
| `rowsBetween` | сколько строк назад и вперёд (по позициям) |
| `rangeBetween` | по значениям колонки (например, по времени) |

---

## Часть 3. Типы окон

### 3.1. Без сортировки — агрегат на партиции

```python
w = Window.partitionBy("client_id")
df = df.withColumn("client_avg",   F.avg("amount").over(w))
df = df.withColumn("client_total", F.sum("amount").over(w))
df = df.withColumn("client_n",     F.count("*").over(w))
df = df.withColumn("share_in_client",  F.col("amount") / F.col("client_total"))
```

Часто используется для **нормализации внутри группы**: «доля транзакции в обороте клиента».

### 3.2. С сортировкой — running aggregate

```python
w = Window.partitionBy("client_id").orderBy("ts")
df = df.withColumn("running_total", F.sum("amount").over(w))
df = df.withColumn("running_avg",   F.avg("amount").over(w))
```

Накопительная сумма каждого клиента по времени. Используется в финансах, лояльности.

### 3.3. Rolling — фиксированное «окно»

```python
# 7 строк назад + текущая = 8 строк
w = Window.partitionBy("client_id").orderBy("ts").rowsBetween(-7, 0)
df = df.withColumn("avg_last_8_tx", F.avg("amount").over(w))

# По времени (range): «последние 30 дней»
w = (Window
    .partitionBy("client_id")
    .orderBy(F.col("ts").cast("long"))
    .rangeBetween(-30 * 86400, 0))   # 30 дней в секундах
df = df.withColumn("sum_last_30d", F.sum("amount").over(w))
```

⚠️ `rangeBetween` требует, чтобы `orderBy` была по **числовой** колонке (или приведена). Поэтому `.cast("long")` для timestamp.

---

## Часть 4. Ранки и порядковые

```python
w = Window.partitionBy("client_id").orderBy(F.col("amount").desc())

df = df.withColumn("row_number", F.row_number().over(w))
df = df.withColumn("rank",       F.rank().over(w))
df = df.withColumn("dense_rank", F.dense_rank().over(w))
df = df.withColumn("percent_rank", F.percent_rank().over(w))
df = df.withColumn("ntile_4",    F.ntile(4).over(w))   # квартиль
```

| Функция | Поведение при ничьих (5,5,3) |
|---------|------------------------------|
| `row_number` | 1, 2, 3 (произвольно) |
| `rank` | 1, 1, 3 (пропуски) |
| `dense_rank` | 1, 1, 2 (без пропусков) |
| `percent_rank` | 0.0, 0.0, 1.0 |
| `ntile(N)` | бакетирование в N квантилей |

**Топ-N в группе** — классический паттерн:
```python
# Топ-3 транзакции каждого клиента
w = Window.partitionBy("client_id").orderBy(F.col("amount").desc())
top3 = (df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .drop("rn"))
```

---

## Часть 5. Lag и Lead

«Предыдущее» и «следующее» значение внутри партиции.

```python
w = Window.partitionBy("client_id").orderBy("ts")

df = df.withColumn("prev_amount", F.lag("amount", 1).over(w))
df = df.withColumn("next_amount", F.lead("amount", 1).over(w))
df = df.withColumn("amount_change", F.col("amount") - F.col("prev_amount"))

# Время до следующей транзакции
df = df.withColumn("next_ts", F.lead("ts", 1).over(w))
df = df.withColumn("seconds_to_next",
    F.unix_timestamp("next_ts") - F.unix_timestamp("ts"))
```

Классические применения: разница между последовательными событиями, sessionization.

---

## Часть 6. First и Last

«Первое» и «последнее» значение в окне:

```python
w = Window.partitionBy("client_id").orderBy("ts")

df = df.withColumn("first_tx_amount", F.first("amount").over(w))
df = df.withColumn("last_tx_amount", F.last("amount").over(
    w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
```

⚠️ `last()` без полной рамки даст не то, что вы ожидаете. Указывайте `rowsBetween(unboundedPreceding, unboundedFollowing)` явно для «глобального last в партиции».

---

## Часть 7. Sessionization — пример из жизни

«Группировать события в сессии: новая сессия если > 30 минут разрыв».

```python
w = Window.partitionBy("client_id").orderBy("ts")

df = (df
    .withColumn("prev_ts", F.lag("ts").over(w))
    .withColumn("gap_sec",
        F.unix_timestamp("ts") - F.unix_timestamp("prev_ts"))
    .withColumn("new_session",
        F.when((F.col("gap_sec") > 1800) | F.col("prev_ts").isNull(), 1).otherwise(0))
    .withColumn("session_id",
        F.sum("new_session").over(w))
)
```

Классический паттерн web/mobile-аналитики и фрод-детекта.

---

## Часть 8. Производительность

Окно = shuffle + сортировка по `partitionBy`. На больших данных:

- **Партиционируйте data заранее** — `df.repartition("client_id")` перед операциями.
- **Не используйте глобальные окна** (`partitionBy(F.lit(1))`) на больших данных.
- **Cache промежуточный результат**, если используете window несколько раз.

---

## Часть 9. Window vs groupBy — когда что

| Хотите | Берите |
|--------|--------|
| Получить агрегат, сжав таблицу | `groupBy().agg()` |
| Добавить агрегат к каждой строке | `Window.partitionBy(...)` |
| Накопительная сумма / running | window с `orderBy` |
| Rolling за N последних строк | window с `rowsBetween` |
| Топ-N в группе | window + `row_number` + filter |
| Сравнить с предыдущей строкой | `lag` |

---

## ✅ Самопроверка

1. Чем window отличается от groupBy?
2. Что нужно для running sum по времени?
3. Чем `rank` отличается от `dense_rank`?
4. Какой паттерн для «топ-3 на группу»?
5. Как сделать sessionization (разделить на сессии по 30-минутному gap)?

---

## ▶️ Дальше

[Урок 5.5 — Партиционирование и бакетирование](./урок_5_партиционирование.md)
