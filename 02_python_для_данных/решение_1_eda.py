"""
✅ Решение Практики 1 модуля 02 — EDA

Открывайте после своей попытки.

Запуск:  python решение_1_eda.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CLIENTS = ROOT / "datasets" / "clients.csv"
TX = ROOT / "datasets" / "transactions.csv"


def main() -> None:
    if not CLIENTS.exists() or not TX.exists():
        print("❌ Сначала запустите практика_1_eda.py")
        return

    # --- TODO 1 ---
    print("=== 1. Загрузка с типами ===")
    clients = pd.read_csv(
        CLIENTS,
        dtype={"client_id": "int32", "age": "int8",
               "city": "category", "segment": "category"},
        parse_dates=["registered_at"],
    )
    tx = pd.read_csv(
        TX,
        dtype={"client_id": "int32", "category": "category"},
        parse_dates=["ts"],
    )
    print(clients.dtypes)
    print(tx.dtypes)

    # --- TODO 2 ---
    print("\n=== 2. EDA ===")
    print("clients:", clients.shape, "  tx:", tx.shape)
    print(clients.describe(include="all", datetime_is_numeric=True))
    print("\nТоп-5 городов:")
    print(clients["city"].value_counts().head(5))
    print("\nСегменты (%):")
    print((clients["segment"].value_counts(normalize=True) * 100).round(1))

    # --- TODO 4 ---
    print("\n=== 4. Аналитика ===")
    msk_pit = clients["city"].isin(["Москва", "Санкт-Петербург"]).mean() * 100
    print(f"a) Доля Мск+СПб: {msk_pit:.1f}%")
    print("b) Средний доход по сегментам:")
    print(clients.groupby("segment", observed=True)["monthly_income"].mean().round(0))

    p99 = tx["amount"].quantile(0.99)
    big = tx[tx["amount"] > p99]
    big_seg = big.merge(clients[["client_id", "segment"]], on="client_id")
    print(f"c) Распределение крупных (>{p99:,.0f}) по сегментам:")
    print(big_seg["segment"].value_counts(normalize=True).round(3))

    # --- TODO 5 ---
    print("\n=== 5. Качество ===")
    empty_city = clients["city"].isna().sum() + (clients["city"] == "").sum()
    print(f"a) Пустых city: {empty_city}")
    dups = tx.duplicated().sum()
    print(f"b) Полных дубликатов в tx: {dups}")
    print(f"c) Уникальные category: {tx['category'].nunique()} → {tx['category'].unique()}")

    # --- TODO 3 ---  (графики) — для notebook
    print(
        """
=== 3. Графики — для notebook ===
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme()
sns.histplot(clients["age"], bins=30); plt.show()
sns.boxplot(x="segment", y="monthly_income", data=clients); plt.show()
tx.nlargest(10, "amount").plot.bar(x="ts", y="amount"); plt.show()
tx.set_index("ts")["amount"].resample("D").sum().plot(); plt.show()
        """
    )


if __name__ == "__main__":
    main()
