# 🧪 Тестирование Spark-кода

> Production-код = код с тестами. Если ваш Spark-pipeline без тестов, он скоро сломается, и вы не заметите.

---

## 1. Зачем тестировать Spark

«Spark-код медленный, я просто запущу и посмотрю» — стандартная отговорка. И стандартная ошибка.

**Что ломается в Spark без тестов:**
- Изменили схему — пайплайн молча выдаёт мусор.
- Поменяли логику фильтра — потеряли 10% данных.
- Добавили join — получили дубликаты.
- Refactoring сломал агрегацию — отчёт неправильный, заказчик заметил через месяц.

Тесты ловят это **до прода**.

---

## 2. Стэк для тестирования

```bash
pip install pytest pytest-spark chispa great-expectations
```

| Инструмент | Для чего |
|------------|---------|
| `pytest` | Базовый test runner |
| `pytest-spark` | Spark-фикстуры |
| `chispa` | Сравнение DataFrame'ов |
| `great-expectations` | Data Quality «extra-mile» |

---

## 3. Пишем код, который можно тестировать

### ❌ Плохо: всё в `main()`
```python
def main():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("input.csv")
    df = df.filter("amount > 0").withColumn(...).groupBy(...).agg(...)
    df.write.parquet("output/")
```

Это **нельзя тестировать**: всё намертво связано с файлом, путём, главной функцией.

### ✅ Хорошо: чистые функции `DataFrame → DataFrame`

```python
def clean_amounts(df: DataFrame) -> DataFrame:
    """Удаляет строки с отрицательными или null amount."""
    return df.filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))


def add_segment(df: DataFrame, clients: DataFrame) -> DataFrame:
    """Присоединяет сегмент клиента."""
    return df.join(F.broadcast(clients.select("client_id", "segment")),
                    "client_id", "left")


def daily_aggregate(df: DataFrame) -> DataFrame:
    return df.groupBy("segment", "category").agg(
        F.sum("amount").alias("total"),
        F.count("*").alias("n"),
    )


def main():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("input.csv")
    clients = spark.read.parquet("clients/")

    cleaned = clean_amounts(df)
    enriched = add_segment(cleaned, clients)
    agg = daily_aggregate(enriched)
    agg.write.parquet("output/")
```

Теперь каждую функцию **легко тестировать**.

---

## 4. Фикстура SparkSession для тестов

`tests/conftest.py`:
```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
        .appName("test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "1")    # быстрее в тестах
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
```

`scope="session"` — одна SparkSession на все тесты сессии pytest. Создавать на каждый тест долго.

---

## 5. Простой тест

`tests/test_clean.py`:
```python
import pytest
import pyspark.sql.functions as F
from my_module import clean_amounts


def test_removes_null_amount(spark):
    df = spark.createDataFrame(
        [(1, 100.0), (2, None), (3, 50.0)],
        ["id", "amount"],
    )
    result = clean_amounts(df)
    assert result.count() == 2
    assert result.filter(F.col("amount").isNull()).count() == 0


def test_removes_negative_amount(spark):
    df = spark.createDataFrame(
        [(1, 100.0), (2, -50.0), (3, 0.0)],
        ["id", "amount"],
    )
    result = clean_amounts(df)
    assert result.count() == 2
    assert result.filter("amount < 0").count() == 0


def test_keeps_zero_amount(spark):
    """0 не считается отрицательным — должно остаться."""
    df = spark.createDataFrame([(1, 0.0)], ["id", "amount"])
    result = clean_amounts(df)
    assert result.count() == 1
```

Запуск:
```bash
pytest tests/ -v
```

---

## 6. Chispa — удобное сравнение DataFrame'ов

```python
from chispa.dataframe_comparer import assert_df_equality


def test_daily_aggregate(spark):
    input_df = spark.createDataFrame(
        [("premium", "transfer", 100.0),
         ("premium", "transfer", 200.0),
         ("mass", "transfer", 50.0)],
        ["segment", "category", "amount"],
    )

    expected = spark.createDataFrame(
        [("premium", "transfer", 300.0, 2),
         ("mass", "transfer", 50.0, 1)],
        ["segment", "category", "total", "n"],
    )

    result = daily_aggregate(input_df)

    assert_df_equality(
        result.orderBy("segment", "category"),
        expected.orderBy("segment", "category"),
        ignore_row_order=True,
        ignore_column_order=True,
    )
```

Chispa красиво показывает разницу: какая строка отличается, в какой колонке.

---

## 7. Тесты схем

«Хочу убедиться, что после трансформации схема **именно такая**».

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def test_aggregate_schema(spark):
    input_df = spark.createDataFrame(
        [("a", 1.0)], ["x", "amount"],
    )
    result = aggregate(input_df)

    expected_schema = StructType([
        StructField("x", StringType(), True),
        StructField("total", DoubleType(), True),
        StructField("n", LongType(), False),
    ])

    assert result.schema == expected_schema, f"got: {result.schema}"
```

---

## 8. Тесты edge-cases

Самые ценные тесты — на «странные» случаи:

```python
def test_empty_input(spark):
    """Пустой DF не должен падать."""
    df = spark.createDataFrame([], schema=spark.createDataFrame(
        [(1, 100.0)], ["id", "amount"]).schema)
    result = clean_amounts(df)
    assert result.count() == 0


def test_all_null(spark):
    df = spark.createDataFrame(
        [(1, None), (2, None)],
        spark.createDataFrame([(1, 100.0)], ["id", "amount"]).schema,
    )
    result = clean_amounts(df)
    assert result.count() == 0


def test_duplicate_keys_in_join(spark):
    """Если в правой таблице есть дубли — мы их ловим."""
    left = spark.createDataFrame([(1, "a")], ["id", "x"])
    right = spark.createDataFrame([(1, "y1"), (1, "y2")], ["id", "y"])
    result = add_segment(left, right)
    # Будут дубли — это ожидаемо или нет?
    assert result.count() == 2     # явно ожидаем 2
```

---

## 9. Data Quality тесты — Great Expectations

```bash
pip install great-expectations
```

```python
import great_expectations as ge

# Конвертация Spark DataFrame
ge_df = ge.dataset.SparkDFDataset(df)

# Ожидания
ge_df.expect_column_to_exist("amount")
ge_df.expect_column_values_to_be_of_type("amount", "DoubleType")
ge_df.expect_column_values_to_not_be_null("client_id")
ge_df.expect_column_values_to_be_between("amount", min_value=0, max_value=10_000_000)
ge_df.expect_column_values_to_be_in_set("category", ["transfer", "payment", "deposit", "withdrawal"])
ge_df.expect_column_value_lengths_to_equal("phone", 11)

# Проверка
results = ge_df.validate()
assert results.success, f"DQ failed: {results}"
```

Это можно встроить **прямо в pipeline** перед записью.

---

## 10. Альтернатива — простые assert'ы в коде

Если не хочется тащить Great Expectations:

```python
def assert_data_quality(df: DataFrame, ctx: str = ""):
    n = df.count()
    if n == 0:
        raise ValueError(f"{ctx}: empty DataFrame")

    null_clients = df.filter(F.col("client_id").isNull()).count()
    if null_clients > 0:
        raise ValueError(f"{ctx}: {null_clients} null client_id")

    neg_amounts = df.filter("amount < 0").count()
    if neg_amounts > n * 0.001:
        raise ValueError(f"{ctx}: too many negative amounts: {neg_amounts}/{n}")
```

Это работает без зависимостей и для маленьких проектов подходит.

---

## 11. Тестирование UDF

```python
def normalize_phone(phone: str) -> str | None:
    if not phone:
        return None
    digits = "".join(c for c in phone if c.isdigit())
    if not digits:
        return None
    if digits[0] == "8":
        digits = "7" + digits[1:]
    if len(digits) != 11 or digits[0] != "7":
        return None
    return "+" + digits


# Это обычная Python-функция. Тестируем без Spark вообще!
def test_normalize_phone():
    assert normalize_phone("+7 (916) 123-45-67") == "+79161234567"
    assert normalize_phone("89161234567") == "+79161234567"
    assert normalize_phone("123") is None
    assert normalize_phone("") is None
    assert normalize_phone(None) is None
```

Если ваша Python-логика отделена от Spark — можно её юнит-тестировать **в 100× быстрее** обычных Spark-тестов.

---

## 12. Mock'и для внешних систем

Если pipeline тянет данные из БД или API, в тестах их подменяют:

```python
from unittest.mock import patch

@patch("my_module.fetch_from_api")
def test_pipeline_with_api(mock_fetch, spark):
    mock_fetch.return_value = spark.createDataFrame([(1, "test")], ["id", "x"])
    result = pipeline_step(spark)
    assert result.count() == 1
```

---

## 13. Интеграционные тесты

Юнит-тесты — каждая функция отдельно.
Интеграционный — весь pipeline на маленьком наборе данных.

```python
def test_full_pipeline(spark, tmp_path):
    # Подготовка входных данных
    input_df = spark.createDataFrame([...], ["..."])
    input_df.write.csv(str(tmp_path / "input.csv"))

    # Запуск pipeline
    run_pipeline(
        spark=spark,
        input_path=str(tmp_path / "input.csv"),
        output_path=str(tmp_path / "output/"),
    )

    # Проверка выхода
    result = spark.read.parquet(str(tmp_path / "output/"))
    assert result.count() > 0
    assert "total" in result.columns
```

`tmp_path` — встроенная фикстура pytest для временной директории.

---

## 14. CI/CD: запуск тестов в GitHub Actions

`.github/workflows/test.yml`:
```yaml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Set up Java
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '17'

      - name: Install deps
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-spark chispa

      - name: Run tests
        run: pytest tests/ -v
```

Теперь каждый PR автоматически проверяется.

---

## 15. Чек-лист тестового pipeline

- [ ] Все «преобразовательные» функции — чистые `DataFrame → DataFrame`.
- [ ] Есть SparkSession-фикстура с `scope="session"`.
- [ ] Юнит-тесты на каждую функцию.
- [ ] Тесты на edge-cases (пустой DF, все null, дубликаты).
- [ ] Тест на схему.
- [ ] Интеграционный тест на маленьких данных.
- [ ] DQ-проверки в самом pipeline.
- [ ] CI запускает тесты на каждый PR.

После этого ваш Spark-код можно гордо называть «production-grade».

---

## 16. Типичный размер тестов

В реальном проекте на каждую функцию pipeline'а — 3–7 тестов:
- 1 happy path
- 2–3 edge case (empty, null, duplicate)
- 1 на схему
- 1 на DQ

Общий объём: `тестов ≈ 2× функций`.

Это нормально. Тесты не пишут «потому что надо» — они спасают вас от ночных дежурств.
