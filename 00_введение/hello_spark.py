"""
Первый запуск PySpark.

Что делает скрипт:
1. Создаёт локальную сессию Spark.
2. Делает маленький DataFrame в памяти.
3. Применяет агрегацию (group by + count).
4. Печатает результат.

Запуск:  python hello_spark.py
"""
from pyspark.sql import SparkSession


def main() -> None:
    # SparkSession — это «точка входа» во все API Spark.
    # local[*] — запуск на локальной машине, * = все доступные ядра.
    spark = (
        SparkSession.builder
        .appName("Hello_Spark")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Уменьшим шум в логах
    spark.sparkContext.setLogLevel("ERROR")

    print("\nSpark версия:", spark.version)
    print("Параллелизм (ядер):", spark.sparkContext.defaultParallelism)

    # Маленький учебный датасет: судебные дела по категориям.
    # (Это «привет, юрист!» — далее в курсе мы используем больше юридических примеров.)
    data = [
        ("гражданское", "Иванов"),
        ("уголовное", "Петров"),
        ("гражданское", "Сидоров"),
        ("административное", "Иванов"),
        ("гражданское", "Иванов"),
        ("уголовное", "Сидоров"),
    ]
    df = spark.createDataFrame(data, schema=["категория", "судья"])

    print("\n📋 Исходные данные:")
    df.show(truncate=False)

    print("📊 Сколько дел в каждой категории:")
    df.groupBy("категория").count().orderBy("count", ascending=False).show()

    print("👨‍⚖️ Сколько дел рассмотрел каждый судья:")
    df.groupBy("судья").count().orderBy("count", ascending=False).show()

    spark.stop()
    print("✅ Готово! PySpark работает.\n")


if __name__ == "__main__":
    main()
