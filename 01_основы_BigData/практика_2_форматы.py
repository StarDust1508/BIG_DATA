"""
🧪 Практика 2 — Сравнение форматов: CSV, JSON, Parquet

Что делает скрипт:
  1. Берёт датасет из практики 1 (transactions_sample.csv).
  2. Сохраняет его в JSON и Parquet.
  3. Замеряет размер на диске и время чтения.
  4. Демонстрирует, почему Parquet — стандарт в Spark.

Если файла практики 1 нет — сначала запустите практика_1_анализ_5V.py.

Запуск:  python практика_2_форматы.py

Требования: pandas + pyarrow (см. requirements.txt)
"""
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATASETS = ROOT / "datasets"
CSV_PATH = DATASETS / "transactions_sample.csv"
JSON_PATH = DATASETS / "transactions_sample.json"
PARQUET_PATH = DATASETS / "transactions_sample.parquet"


def size_mb(path: Path) -> float:
    return path.stat().st_size / 1024 / 1024


def time_read(fn, *args, **kwargs) -> tuple[float, pd.DataFrame]:
    t0 = time.perf_counter()
    df = fn(*args, **kwargs)
    return time.perf_counter() - t0, df


def main() -> None:
    if not CSV_PATH.exists():
        print(f"❌ Сначала запустите практика_1_анализ_5V.py — нужен {CSV_PATH}")
        return

    # 1. Загрузка исходного CSV
    print("📂 Читаю CSV...")
    df = pd.read_csv(CSV_PATH, parse_dates=["timestamp"])
    print(f"   Строк: {len(df):,}  Колонок: {df.shape[1]}")

    # 2. Сохранение в разные форматы
    print("\n💾 Сохраняю в разные форматы...")
    df.to_json(JSON_PATH, orient="records", date_format="iso")
    df.to_parquet(PARQUET_PATH, compression="snappy")

    # 3. Размер
    print("\n📏 Размер на диске:")
    print(f"   CSV     : {size_mb(CSV_PATH):8.2f} МБ")
    print(f"   JSON    : {size_mb(JSON_PATH):8.2f} МБ")
    print(f"   Parquet : {size_mb(PARQUET_PATH):8.2f} МБ")

    print(
        f"\n   Parquet примерно в "
        f"{size_mb(CSV_PATH) / size_mb(PARQUET_PATH):.1f}× "
        f"меньше CSV."
    )

    # 4. Скорость чтения всего файла
    print("\n⏱️  Время полного чтения (3 повторения):")

    def avg(fn, *args, **kwargs):
        times = []
        for _ in range(3):
            t, _ = time_read(fn, *args, **kwargs)
            times.append(t)
        return sum(times) / len(times)

    t_csv = avg(pd.read_csv, CSV_PATH)
    t_json = avg(pd.read_json, JSON_PATH)
    t_parquet = avg(pd.read_parquet, PARQUET_PATH)
    print(f"   CSV     : {t_csv * 1000:7.0f} мс")
    print(f"   JSON    : {t_json * 1000:7.0f} мс")
    print(f"   Parquet : {t_parquet * 1000:7.0f} мс  (быстрее CSV "
          f"в {t_csv / t_parquet:.1f}×)")

    # 5. Чтение только одной колонки
    print("\n🎯 Чтение ТОЛЬКО колонки 'amount':")

    def csv_one_col(p):
        return pd.read_csv(p, usecols=["amount"])

    def parquet_one_col(p):
        return pd.read_parquet(p, columns=["amount"])

    t_csv_c = avg(csv_one_col, CSV_PATH)
    t_pq_c = avg(parquet_one_col, PARQUET_PATH)
    print(f"   CSV     : {t_csv_c * 1000:7.0f} мс — всё равно читает весь файл")
    print(f"   Parquet : {t_pq_c * 1000:7.0f} мс — читает ТОЛЬКО колонку amount")
    print(f"   Разница в {t_csv_c / t_pq_c:.1f}× — это магия колоночного формата.")

    print(
        """
🧠 Что это значит:
  • На малых данных разница не радикальная — но на 100 ГБ Parquet
    может быть в 50–100× быстрее CSV.
  • Predicate pushdown (фильтрация на уровне файла) и колоночное
    чтение — это фундамент скорости Spark SQL.
  • Поэтому правило: внутри Big Data пайплайна — Parquet или ORC.
    CSV — только на границах системы (импорт/экспорт).
        """
    )


if __name__ == "__main__":
    main()
