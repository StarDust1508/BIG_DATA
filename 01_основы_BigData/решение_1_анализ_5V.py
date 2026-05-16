"""
✅ Решение Практики 1

Открывайте только ПОСЛЕ своей попытки в практика_1_анализ_5V.py!

Здесь — эталонный разбор того же датасета.
Запуск:  python решение_1_анализ_5V.py
"""
from pathlib import Path
import pandas as pd

CSV_PATH = Path(__file__).resolve().parent.parent / "datasets" / "transactions_sample.csv"


def main() -> None:
    if not CSV_PATH.exists():
        print(f"❌ Сначала запустите практика_1_анализ_5V.py — нужен файл {CSV_PATH}")
        return

    print("📂 Загрузка датасета...")
    df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])

    # --- TODO 1. Базовая разведка ---
    print("\n--- 1. Разведка ---")
    print("Shape:", df.shape)
    print("\nТипы:")
    print(df.dtypes)
    print("\nГолова:")
    print(df.head())
    print("\nОписание:")
    print(df.describe(include="all", datetime_is_numeric=True))

    # --- TODO 2. Пропуски в client_id ---
    print("\n--- 2. Пропуски ---")
    missing = df["client_id"].isna() | (df["client_id"] == "")
    print(f"Пустых client_id: {missing.sum()} ({missing.mean() * 100:.2f}%)")
    print("Стратегия: либо удалить (если их мало), либо пометить как 'UNKNOWN' "
          "и анализировать отдельно — это может быть фрод.")

    # --- TODO 3. Выбросы по сумме ---
    print("\n--- 3. Выбросы по amount ---")
    threshold = df["amount"].quantile(0.99)
    outliers = df[df["amount"] > threshold]
    print(f"99-й перцентиль: {threshold:,.2f}")
    print(f"Выбросов (>{threshold:,.0f}): {len(outliers)}")
    print(outliers.nlargest(5, "amount"))

    # --- TODO 4. Аномалии в category ---
    print("\n--- 4. Категории ---")
    cats = df["category"].value_counts()
    print(cats)
    print("\nЗаметьте: 'ПЕРЕВОД' и 'перевод' — это одно и то же,"
          "\nно для машины это разные значения. Нужна нормализация (lower-case).")

    # --- TODO 5. Big Data? ---
    print("\n--- 5. Это Big Data? ---")
    print(
        """
        Volume   : 50 тыс. строк ~ несколько МБ — НЕТ, в Pandas помещается.
        Velocity : файл статичный — НЕТ.
        Variety  : только табличные данные — НЕТ.
        Veracity : ЕСТЬ! Пропуски, опечатки, выбросы.
        Value    : ЕСТЬ — можно искать фрод, профили клиентов.

        Вердикт: это «обычные» данные, но с уже видимыми проблемами Veracity.
        Если бы строк было 50 МЛРД — pd.read_csv лёг бы по памяти, и нам бы
        потребовался Spark с распределённым чтением (chunked, parquet, partitions).
        """
    )


if __name__ == "__main__":
    main()
