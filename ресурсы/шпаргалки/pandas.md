# 🐼 Pandas Cheat Sheet

> Минимум, который надо знать **до** Spark. Распечатайте и положите рядом.

```python
import pandas as pd
import numpy as np
```

## Создание / загрузка
```python
df = pd.read_csv("file.csv")
df = pd.read_parquet("file.parquet")
df = pd.read_excel("file.xlsx", sheet_name=0)
df = pd.read_json("file.json")

# Из словаря
df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

# Запись
df.to_csv("out.csv", index=False)
df.to_parquet("out.parquet")
```

## Разведка
```python
df.shape          # (rows, cols)
df.dtypes
df.head(10)
df.tail(5)
df.sample(5)
df.describe(include="all")
df.info()
df.columns
df.memory_usage(deep=True).sum() / 1024**2  # МБ
```

## Выборка
```python
df["col"]                              # колонка
df[["col1", "col2"]]                   # несколько колонок
df.iloc[0]                             # строка по позиции
df.loc[df["col"] > 5]                  # фильтр
df.query("col > 5 and other == 'x'")   # SQL-подобный
df.loc[:, df.dtypes == "float64"]      # все float-колонки
```

## Очистка
```python
df.isna().sum()                # сколько пропусков
df.dropna(subset=["col"])
df.fillna({"col": 0, "name": "unknown"})
df.drop_duplicates(subset=["id"])
df["col"] = df["col"].str.strip().str.lower()
df["col"] = df["col"].astype("int64")
```

## Группировки
```python
df.groupby("category")["amount"].sum()
df.groupby(["a", "b"]).agg(
    total=("amount", "sum"),
    avg=("amount", "mean"),
    n=("id", "count"),
)
```

## Соединения
```python
pd.merge(left, right, on="id", how="inner")    # inner | left | right | outer
pd.concat([df1, df2], axis=0)   # вертикально
pd.concat([df1, df2], axis=1)   # горизонтально
```

## Время
```python
df["ts"] = pd.to_datetime(df["ts"])
df["year"]  = df["ts"].dt.year
df["month"] = df["ts"].dt.month
df["wday"]  = df["ts"].dt.day_name()
df.set_index("ts").resample("D")["amount"].sum()
```

## Apply / map
```python
df["len"] = df["text"].str.len()
df["bucket"] = df["amount"].apply(lambda x: "big" if x > 1e4 else "small")
df["upper"] = df["name"].map(str.upper)
```

## Окна (rolling)
```python
df["ma7"]  = df["amount"].rolling(7).mean()
df["cum"]  = df["amount"].cumsum()
df["rank"] = df.groupby("client")["amount"].rank(method="dense")
```

## Производительность

| Симптом | Что делать |
|---|---|
| `for row in df.iterrows()` медленный | переписать через векторные операции |
| Память не хватает | использовать `chunksize=` в `read_csv`, или `dask`, или `pyspark` |
| `apply` медленный | проверить, нельзя ли заменить на `numpy` |
| Парсинг дат медленный | передать `format="%Y-%m-%d"` |
