"""
✅ Решение Практики 3 модуля 02 — Обезличивание

Запуск: python решение_3_обезличивание.py
"""
import hashlib
import os
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PII = ROOT / "datasets" / "clients_pii.csv"
ANON = ROOT / "datasets" / "clients_anonymous.parquet"
MAPPING = ROOT / "datasets" / "mapping.parquet"


def get_salt() -> str:
    salt = os.environ.get("PSEUDO_SALT")
    if not salt:
        print("⚠️  PSEUDO_SALT не задана в окружении. Использую 'dev-salt'.")
        print("   В реальном проекте: export PSEUDO_SALT='<длинная случайная строка>'")
        return "dev-salt"
    return salt


def make_token(value: str, salt: str) -> str:
    return hashlib.sha256((salt + str(value)).encode()).hexdigest()[:16]


def pseudonymize(df: pd.DataFrame, salt: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    # Возраст
    today = pd.Timestamp(date.today())
    df["birth_date"] = pd.to_datetime(df["birth_date"])
    df["age"] = ((today - df["birth_date"]).dt.days // 365).astype(int)
    # Токен и маппинг
    df["token"] = df["client_id"].apply(lambda x: make_token(x, salt))
    mapping = df[["client_id", "token"]].copy()
    # Удаляем ПДн
    drop_cols = ["last_name", "first_name", "middle_name", "passport", "snils",
                 "phone", "email", "street", "birth_date", "client_id"]
    df = df.drop(columns=drop_cols)
    return df, mapping


def k_anonymity(df: pd.DataFrame, quasi_cols: list[str], k: int = 3) -> None:
    counts = df.groupby(quasi_cols, observed=True).size()
    bad = counts[counts < k]
    print(f"\nK-анонимность (k={k}) по {quasi_cols}:")
    print(f"  всего групп: {len(counts)}")
    print(f"  групп с k<{k}: {len(bad)}")
    print(f"  записей в проблемных группах: {bad.sum()}")
    if len(bad):
        print("\nПримеры таких групп:")
        print(bad.head())


def main() -> None:
    if not PII.exists():
        print("❌ Сначала запустите практика_3_обезличивание.py")
        return

    salt = get_salt()
    df = pd.read_csv(PII)
    print(f"Загружено: {len(df):,} записей с ПДн")

    anon, mapping = pseudonymize(df, salt)
    print(f"После обезличивания: {anon.shape}  колонки: {list(anon.columns)}")

    # K-анонимность с грубой группировкой salary
    anon["salary_band"] = (anon["salary"] // 50_000) * 50_000
    k_anonymity(anon, ["age", "city", "salary_band"], k=3)

    # Огрубление возраста — повысит k-анонимность
    anon["age_band"] = (anon["age"] // 5) * 5
    k_anonymity(anon, ["age_band", "city", "salary_band"], k=3)

    # Сохранение
    anon.to_parquet(ANON, compression="snappy")
    mapping.to_parquet(MAPPING, compression="snappy")
    print(f"\n✅ {ANON.name}  — для аналитики (без ПДн напрямую)")
    print(f"✅ {MAPPING.name}    — секретный маппинг (хранить отдельно!)")

    print(
        """
=== Юридическое резюме ===
• Это псевдонимизация (маппинг существует) → данные ВСЁ ЕЩЁ ПДн по 152-ФЗ/GDPR.
• Защита всё равно обязательна, но круг лиц с доступом к сырым ПДн сужен.
• mapping.parquet — это «ключ». Хранить:
    - отдельно от анонимных данных,
    - с шифрованием at rest,
    - с audit log доступа,
    - доступ только у DPO и узкого круга лиц.
• Если нужно полностью обезличить (вывести из режима ПДн) — удалите
  mapping и подтвердите, что k-анонимность достаточна.
        """
    )


if __name__ == "__main__":
    main()
