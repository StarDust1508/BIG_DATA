"""
✅ Решение Практики 2 модуля 02 — Очистка

Запуск: python решение_2_очистка.py
"""
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DIRTY = ROOT / "datasets" / "dirty_invoices.csv"
CLEAN = ROOT / "datasets" / "clean_invoices.parquet"


def parse_amount(s):
    if pd.isna(s):
        return None
    s = str(s).replace(" ", "").replace(" ", "").replace("₽", "")
    if "," in s and s.count(",") == 1 and "." not in s:
        s = s.replace(",", ".")
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def parse_date(s):
    if pd.isna(s) or str(s).strip() == "":
        return pd.NaT
    s = str(s).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return pd.to_datetime(s, format=fmt)
        except ValueError:
            continue
    return pd.NaT


def normalize_phone(p):
    if pd.isna(p):
        return None
    digits = re.sub(r"\D", "", str(p))
    if not digits:
        return None
    if digits.startswith("8"):
        digits = "7" + digits[1:]
    if not digits.startswith("7"):
        digits = "7" + digits
    if len(digits) != 11:
        return None
    return "+" + digits


CATEGORY_MAP = {
    "канцелярия": "канцелярия",
    "техника":    "техника",
    "технника":   "техника",
    "техника.":   "техника",
    "тех ника":   "техника",
    "услуги":     "услуги",
    "транспорт":  "транспорт",
}


def normalize_category(c):
    if pd.isna(c):
        return None
    s = re.sub(r"[^а-яё ]+", "", str(c).lower().strip())
    s = re.sub(r"\s+", " ", s).strip()
    return CATEGORY_MAP.get(s, s)


def main() -> None:
    if not DIRTY.exists():
        print("❌ Сначала запустите практика_2_очистка.py")
        return

    # 1
    df = pd.read_csv(DIRTY, sep=";", encoding="cp1251")
    n_before = len(df)
    print(f"Загружено: {n_before:,}")

    # 2
    df["date"] = df["date"].apply(parse_date)

    # 3
    df["amount"] = df["amount"].apply(parse_amount)

    # 4
    df["phone"] = df["phone"].apply(normalize_phone)

    # 5
    df["email"] = df["email"].astype(str).str.strip().str.lower()

    # 6
    df["category"] = df["category"].apply(normalize_category)

    # 7
    before_dups = len(df)
    df = df.drop_duplicates()
    print(f"Удалено дубликатов: {before_dups - len(df)}")

    # 8
    df.to_parquet(CLEAN, compression="snappy")

    # 9
    print("\n=== Отчёт ===")
    print(f"Строк до:  {n_before:,}")
    print(f"Строк после: {len(df):,}")
    print("\nПропуски по колонкам:")
    print(df.isna().sum())
    print("\nУникальные category:")
    print(df["category"].value_counts())
    print(f"\n✅ Сохранено: {CLEAN}")


if __name__ == "__main__":
    main()
