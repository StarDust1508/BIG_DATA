# 04. Apache Spark — основы

> Сердце курса. Здесь концепции 01–03 встречаются с реальным кодом. После этого модуля вы можете писать понятный, эффективный PySpark.

---

## 🎯 Цели модуля

После модуля вы:

- Понимаете архитектуру Spark: driver, executor, partition, stage, task, shuffle, DAG.
- Уверенно используете **DataFrame API** и Spark SQL.
- Знаете, что такое **lazy evaluation**, разницу transformation/action.
- Видите план выполнения через `.explain()` и понимаете его.
- Знаете, **когда** Spark лучше Pandas, а когда — наоборот.
- Понимаете Catalyst-оптимизатор и AQE.

---

## 📚 Уроки

| # | Урок | Время |
|---|------|-------|
| 4.1 | [Архитектура Spark](./урок_1_архитектура.md) | 50 мин |
| 4.2 | [SparkSession и RDD](./урок_2_session_rdd.md) | 35 мин |
| 4.3 | [DataFrame API — ядро](./урок_3_dataframe.md) | 60 мин |
| 4.4 | [Spark SQL и Catalyst](./урок_4_spark_sql.md) | 40 мин |
| 4.5 | [Lazy evaluation на примерах](./урок_5_lazy.md) | 25 мин |
| 4.6 | [Pandas vs Spark: выбор](./урок_6_pandas_vs_spark.md) | 30 мин |

---

## 🧪 Практика

| # | Файл | Что делает |
|---|------|------------|
| П1 | [практика_1_первые_шаги.py](./практика_1_первые_шаги.py) | DataFrame: select, filter, groupBy, join |
| П2 | [практика_2_sql.py](./практика_2_sql.py) | те же задачи через Spark SQL + window |
| П3 | [практика_3_pandas_vs_spark.py](./практика_3_pandas_vs_spark.py) | бенчмарк на 10М строк |
| П4 | [практика_4_explain.py](./практика_4_explain.py) | читаем физический план |

Данные берутся из модуля 02 (`datasets/clients.csv`, `datasets/transactions.csv`). Если их нет — сначала запустите `02_python_для_данных/практика_1_eda.py`.

---

## 🧠 Контрольная точка

- 📝 [квиз_модуль_04.md](./квиз_модуль_04.md) — 20 вопросов.
- 📓 [мои_заметки.md](./мои_заметки.md) — конспект.

---

## 🛠 Что нужно установить (бесплатно)

**Главный модуль курса — здесь нужно убедиться, что Spark реально работает.**

### Минимум для локального запуска

1. **Python 3.10+** — модуль 00.
2. **Java 17** — модуль 00. ⚠️ Без Java Spark не запустится.
3. **PySpark** — `pip install pyspark==3.5.0` (уже в `requirements.txt`).

Проверка:
```bash
python 00_введение/проверка_окружения.py
python 00_введение/hello_spark.py
```

Если `hello_spark.py` отработал — вы готовы к модулю 04.

### Для серьёзной работы (опционально)

- **Spark UI** доступен в браузере на http://localhost:4040 пока активна `SparkSession`. Откройте сразу при запуске — увидите план запросов, метрики стадий, executor'ы. Главный инструмент тюнинга.
- **Удобный просмотрщик Parquet**: `pip install parquet-tools` (CLI) или расширение для VS Code «Parquet Viewer».

### Если локально не получается — Databricks Community ⭐

Полноценный Spark **бесплатно в облаке**, никакой установки:

1. Регистрация: https://community.cloud.databricks.com/
2. Создаёте Cluster (Single Node, 15 ГБ).
3. Создаёте Notebook → Python → пишете PySpark код.

Это **профессиональный путь**: реальные команды дата-инженеров часто работают именно в Databricks.

### Google Colab — для коротких экспериментов

```python
!pip install pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
```

Лимит: 12 часов сессии, потом сбрасывается. Для долгих ETL не подходит, для уроков — отлично.

---

## 🔗 Связи

- Опирается на: [02 Pandas](../02_python_для_данных/), [03 Hadoop](../03_hadoop_HDFS/), [01.4 Форматы](../01_основы_BigData/урок_4_форматы_хранения.md).
- Готовит к: [05 PySpark практика](../05_pyspark_практика/), [06 MLlib](../06_spark_MLlib/).
- Шпаргалка: [pyspark.md](../ресурсы/шпаргалки/pyspark.md).

---

## ⏱️ Время

~6–7 дней по 1.5 часа.

**Дальше:** [05_pyspark_практика](../05_pyspark_практика/)
