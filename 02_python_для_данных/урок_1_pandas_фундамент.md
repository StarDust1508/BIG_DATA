# Урок 2.1 — Pandas: фундамент

> Pandas — это «Excel на стероидах» в Python. Овладев им, вы будете на 90% готовы к PySpark DataFrame API: синтаксис очень похож.

---

## Часть 1. Зачем именно Pandas

Альтернативы есть: чистый Python (`list`, `dict`), `numpy`, `polars`, `dask`, `pyspark`.
Pandas — **золотой стандарт** аналитики на одной машине:

- Большая экосистема (читает CSV, Excel, JSON, SQL, Parquet, HDF5...).
- Богатый API: groupby, join, окна, временные ряды.
- Простой для интерактивной разведки.
- Совместим практически со всем ML-стэком Python.

**Лимит:** один процесс, одна машина, помещается в RAM. Для большего — PySpark, Polars, Dask.

---

## Часть 2. Две главных структуры

### Series — одномерный массив с индексом

```python
import pandas as pd

s = pd.Series([10, 20, 30, 40], index=["a", "b", "c", "d"])
print(s)
# a    10
# b    20
# c    30
# d    40
# dtype: int64

s["b"]            # 20
s[s > 15]         # b, c, d
s.mean()          # 25.0
```

Series — это **named array**. Имя у каждого значения. Похоже на словарь, но в разы быстрее.

### DataFrame — двумерная таблица

```python
df = pd.DataFrame({
    "name":  ["Аня", "Боря", "Витя"],
    "age":   [25, 31, 19],
    "city":  ["Москва", "Питер", "Казань"],
})
```

DataFrame — это словарь Series, разделяющих общий **индекс** строк. Под капотом — numpy-массивы по колонкам.

---

## Часть 3. Индекс — главное, что отличает Pandas от Spark

В Pandas у каждой строки есть **индекс** (по умолчанию 0, 1, 2, ...). Можно сделать своим:

```python
df = df.set_index("name")
df.loc["Аня"]            # доступ по имени
df.reset_index()         # сбросить, индекс снова 0..N
```

**Важно:** в Spark DataFrame **нет** индекса. Это первое, что путает после Pandas. В Spark «индекс» — это просто колонка.

---

## Часть 4. Типы данных (dtypes)

Pandas наследует типы NumPy + добавляет свои:

| dtype | Что это | Размер |
|-------|---------|--------|
| `int8`, `int16`, `int32`, `int64` | целые | 1–8 байт |
| `uint8`, ... | беззнаковые целые | 1–8 байт |
| `float32`, `float64` | дробные | 4 или 8 байт |
| `bool` | True/False | 1 байт |
| `object` | Python-объект (часто строка) | много |
| `string` (новый) | оптимизированные строки | компактнее |
| `category` | словарь категорий | очень компактно |
| `datetime64[ns]` | дата+время | 8 байт |
| `timedelta64[ns]` | интервал | 8 байт |

```python
df.dtypes              # посмотреть
df["age"] = df["age"].astype("int16")
df["city"] = df["city"].astype("category")    # экономия памяти ×10
```

💡 **Совет:** на любом «большом» датасете сразу преобразуйте `object` → `category` для категорий и подберите минимальный `int`. Часто это сокращает память в 3–5 раз.

---

## Часть 5. Создание и базовые операции

### Создание
```python
# Из словаря
pd.DataFrame({"a": [1, 2], "b": [3, 4]})

# Из списка словарей
pd.DataFrame([{"a": 1, "b": 3}, {"a": 2, "b": 4}])

# Из numpy-массива
import numpy as np
pd.DataFrame(np.random.rand(3, 4), columns=list("abcd"))

# Из файла
pd.read_csv("file.csv")
pd.read_parquet("file.parquet")
pd.read_excel("file.xlsx")
```

### Выборка
```python
df["age"]                       # колонка → Series
df[["age", "city"]]              # несколько колонок → DataFrame

df.iloc[0]                       # 1-я строка по позиции
df.iloc[0:3]                     # 3 первых строки
df.iloc[0, 1]                    # ячейка (строка 0, колонка 1)

df.loc["Аня"]                    # по индексу
df.loc["Аня", "age"]             # ячейка по индексу+колонке

df[df["age"] > 25]               # фильтр булевый
df.query("age > 25 and city == 'Москва'")
```

### Изменение
```python
df["adult"] = df["age"] >= 18                  # новая колонка
df["age"] = df["age"] + 1                       # пересчёт
df.rename(columns={"age": "возраст"})           # переименование
df.drop(columns=["adult"])                      # удаление колонки
df.drop(index=["Аня"])                          # удаление строки
```

---

## Часть 6. Аналог Excel-формул

Pandas — это электронная таблица «изнутри Python». Примерные параллели:

| Excel | Pandas |
|-------|--------|
| `=A1+B1` | `df["sum"] = df["a"] + df["b"]` |
| `VLOOKUP` | `df.merge(other, on="key")` |
| Сводная таблица | `df.pivot_table(...)` или `df.groupby(...).agg(...)` |
| Фильтр (autofilter) | `df[df["x"] > 5]` |
| Сортировка | `df.sort_values("x")` |
| Условный формат | `df.style.background_gradient()` |

Поэтому юристам, которые работали в Excel, Pandas даётся быстрее, чем кажется.

---

## Часть 7. Грабли, на которые наступают все

### 7.1. Copy vs view (`SettingWithCopyWarning`)
```python
subset = df[df["age"] > 18]
subset["age"] = 0   # ⚠️ может быть копия или вью — поведение неопределено
```
Решение:
```python
subset = df[df["age"] > 18].copy()
subset["age"] = 0
```

### 7.2. `inplace=True` — не делайте без причины
```python
df.dropna(inplace=True)   # ❌ скрытые мутации
df = df.dropna()           # ✅ явный, читаемый код
```

### 7.3. `for` по `iterrows()` — медленный
```python
# ❌ Медленно
for _, row in df.iterrows():
    df.loc[row.name, "x"] = row["a"] + row["b"]

# ✅ Векторно
df["x"] = df["a"] + df["b"]
```

В Pandas правило: **если можно сделать векторно — делайте векторно**. Это в 100× быстрее.

### 7.4. NaN — это float, а не None
```python
df["age"].fillna(0)
df["age"].dropna()
df["age"].isna().sum()    # сколько NaN
```

Раньше в Pandas нельзя было иметь `NaN` в `int`-колонке (она становилась float). С 1.0+ есть **nullable Int64**:
```python
df["age"] = df["age"].astype("Int64")   # с заглавной — поддерживает NaN
```

---

## Часть 8. Что такое «настоящие» Pandas-данные

В голове держите такую модель:

```
DataFrame
├── index (метка для каждой строки)
└── columns
    ├── col_a → Series (numpy array + dtype + name)
    ├── col_b → Series
    └── ...
```

Каждая Series — это numpy-массив. Поэтому когда вы делаете `df["x"] + df["y"]`, это уже **векторная операция в C**, а не Python-цикл.

Это объясняет, **почему** Pandas быстр и **почему** «numpy-style» код на нём такой эффективный.

---

## ✅ Самопроверка

1. Чем Series отличается от dict?
2. Что выведет `df.iloc[1]` и чем оно отличается от `df.loc["b"]`?
3. Почему стоит избегать `for ... in df.iterrows()`?
4. Чем `category` лучше `object` для колонки с 5 уникальными значениями на 10М строк?
5. Что такое `SettingWithCopyWarning` и как от него избавиться?
6. Какая dtype сэкономит память для колонки с возрастом (0–120)?

---

## ▶️ Дальше

Переходите к [Уроку 2.2 — NumPy](./урок_2_numpy.md).
