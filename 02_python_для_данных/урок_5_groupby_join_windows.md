# Урок 2.5 — Группировки, join, окна

> Эти три приёма покрывают 70% аналитики. Они работают **одинаково идейно** в Pandas, SQL и PySpark.

---

## Часть 1. Groupby — «разделяй и считай»

Идея в трёх словах: **split → apply → combine**.

```
df
│
├── groupby("category")        # split
│     │
│     ├── transfer  → [рядов] → sum()/mean()/count()   # apply
│     ├── payment   → [рядов] → ...
│     └── deposit   → [рядов] → ...
│
└── combine результаты в одну таблицу
```

### Простые агрегации

```python
df.groupby("category")["amount"].sum()
df.groupby("category")["amount"].mean()
df.groupby("category")["amount"].count()
df.groupby("category").size()              # размер группы
df.groupby(["category", "currency"]).sum()  # несколько ключей
```

### Несколько метрик за раз — `agg`

```python
df.groupby("category").agg(
    total = ("amount", "sum"),
    avg   = ("amount", "mean"),
    n     = ("id", "count"),
    p95   = ("amount", lambda x: x.quantile(0.95)),
)
```

### Apply — произвольная функция

```python
def top3(group):
    return group.nlargest(3, "amount")

df.groupby("client").apply(top3)
```

⚠️ `apply` — мощно, но **медленно** (по сравнению с встроенными `agg`). На больших данных избегайте.

### Transform — вернуть результат той же формы

Часто нужно для нормализации внутри группы:

```python
# Z-score внутри категории
df["amount_z"] = df.groupby("category")["amount"].transform(
    lambda x: (x - x.mean()) / x.std()
)
```

---

## Часть 2. Pivot table

Excel-стиль:

```python
df.pivot_table(
    index="city",
    columns="category",
    values="amount",
    aggfunc="sum",
    fill_value=0,
    margins=True,             # ИТОГО
    margins_name="Всего",
)
```

Полезно для отчётов «городá × категории → суммы».

---

## Часть 3. Join (merge)

Подразумеваем, что у вас есть две таблицы и общий ключ.

```python
clients = pd.DataFrame({"client_id": [1, 2, 3], "name": ["A", "B", "C"]})
tx      = pd.DataFrame({"client_id": [1, 1, 2, 4], "amount": [10, 20, 30, 40]})
```

### Виды join

| Тип | Что берём |
|-----|-----------|
| `inner` (default) | только совпадающие ключи в обеих |
| `left` | все из левой, к ним подтягиваем |
| `right` | все из правой |
| `outer` | всё из обеих, NaN где нет совпадения |
| `cross` | декартово произведение (осторожно — раздувает!) |

```python
pd.merge(clients, tx, on="client_id", how="left")
pd.merge(clients, tx, left_on="id", right_on="client_id")     # разные имена
clients.merge(tx, on="client_id", how="inner")                # метод
```

### Concat — склейка
```python
pd.concat([df1, df2], axis=0)    # вертикально (друг под другом)
pd.concat([df1, df2], axis=1)    # горизонтально (рядом)
```

### Лайфхак для отладки

После любого `merge` сравните `.shape` ДО и ПОСЛЕ. Если строк стало больше — у вас **дубли по ключу** (это один из самых частых багов).

```python
print("До:", tx.shape)
result = tx.merge(clients, on="client_id")
print("После:", result.shape)
```

---

## Часть 4. Anti-join (то, чего нет)

Иногда нужно «всё, чего нет в другой таблице»:

```python
# Транзакции «осиротевших» клиентов (нет в справочнике clients)
orphans = tx.merge(clients, on="client_id", how="left", indicator=True)
orphans = orphans[orphans["_merge"] == "left_only"]
```

Или быстрее через `isin`:
```python
orphans = tx[~tx["client_id"].isin(clients["client_id"])]
```

---

## Часть 5. Window functions (оконные)

«Оконные» функции считают что-то на скользящем подмножестве строк.

### Rolling — скользящие
```python
df["ma7"]  = df["amount"].rolling(7).mean()
df["sum7"] = df["amount"].rolling(7).sum()
df["max7"] = df["amount"].rolling(7, min_periods=1).max()
```

### Expanding — нарастающие
```python
df["cum_sum"] = df["amount"].expanding().sum()
df["cum_max"] = df["amount"].expanding().max()
```

### Rank внутри группы
```python
df["rank_in_client"] = df.groupby("client")["amount"].rank(method="dense", ascending=False)

# Топ-3 транзакций каждого клиента
top3 = df[df.groupby("client")["amount"].rank(method="first", ascending=False) <= 3]
```

### Lag / Lead (предыдущее/следующее значение)
```python
df["prev_amount"] = df.groupby("client")["amount"].shift(1)
df["next_amount"] = df.groupby("client")["amount"].shift(-1)

# Разница с предыдущей транзакцией
df["delta"] = df["amount"] - df["prev_amount"]
```

### Cumulative — внутри группы
```python
df["cum_sum_per_client"] = df.groupby("client")["amount"].cumsum()
```

⚠️ **Перед оконными функциями почти всегда нужна сортировка**. Например, `df.sort_values(["client", "ts"])`.

---

## Часть 6. Параллели с PySpark

| Pandas | PySpark |
|--------|---------|
| `df.groupby("x").agg(...)` | `df.groupBy("x").agg(...)` |
| `pd.merge(a, b, on=...)` | `a.join(b, "key")` |
| `df["x"].rolling(7).mean()` | `F.avg("x").over(Window.orderBy(...).rowsBetween(-6, 0))` |
| `df["x"].shift(1)` | `F.lag("x", 1).over(window)` |
| `df.pivot_table(...)` | `df.groupBy().pivot(...).agg(...)` |

Если вы натренируетесь думать в этих категориях на Pandas — PySpark будет «таким же, только через `groupBy` с большой B». 90% синтаксиса легко переносится.

---

## Часть 7. Типичные ошибки

1. **`groupby` потерял колонку.** После `.sum()` остаются только числовые → используйте `.agg({"col": ...})`.
2. **`merge` дублировал строки.** Проверьте уникальность ключа в обеих таблицах.
3. **`apply` тормозит.** Замените на `agg` или векторные операции.
4. **Окно без сортировки.** Результат непредсказуем.
5. **`pivot` на колонке с миллионом уникальных значений.** Получится 1М колонок и взрыв памяти.

---

## ✅ Самопроверка

1. Что выведет `df.groupby("x").transform("mean")` — таблицу той же формы или меньшей?
2. После `inner join` строк стало больше, чем в левой таблице. Это норма? Почему?
3. Чем `rolling(7)` отличается от `expanding()`?
4. Как посчитать «топ-3 транзакции каждого клиента»?
5. Что такое anti-join и зачем он?
6. Зачем сортировать перед `shift()` или `rolling()`?

---

## ▶️ Дальше

[Урок 2.6 — Производительность Pandas](./урок_6_производительность.md)
