"""
🧩 Практика 1 модуля 02 — EDA

Сценарий: вам передали датасет клиентов и их транзакций.
Нужно провести разведочный анализ и ответить на бизнес-вопросы.

Что делает скрипт:
  1. Генерирует синтетические данные двух связанных таблиц.
  2. Сохраняет их в datasets/ как clients.csv и transactions.csv.
  3. Даёт TODO-вопросы внизу — вы их решаете самостоятельно.

Решение — в файле решение_1_eda.py.

Запуск:  python практика_1_eda.py
"""
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

random.seed(7)
np.random.seed(7)

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "datasets"
DATA.mkdir(exist_ok=True)

CLIENTS_PATH = DATA / "clients.csv"
TX_PATH = DATA / "transactions.csv"

# ---------------------------------------------------------------------------
# Генерация
# ---------------------------------------------------------------------------
N_CLIENTS = 2_000
CITIES = ["Москва", "Санкт-Петербург", "Казань", "Новосибирск", "Екатеринбург",
          "Нижний Новгород", "Самара", "Ростов-на-Дону"]
SEGMENTS = ["mass", "mass-affluent", "premium"]
SEG_WEIGHTS = [0.70, 0.25, 0.05]


def gen_clients() -> pd.DataFrame:
    ids = np.arange(1, N_CLIENTS + 1)
    ages = np.clip(np.random.normal(40, 12, N_CLIENTS), 18, 90).astype(int)
    cities = np.random.choice(CITIES, N_CLIENTS, p=None)
    segments = np.random.choice(SEGMENTS, N_CLIENTS, p=SEG_WEIGHTS)
    incomes = []
    for s in segments:
        if s == "mass":
            incomes.append(np.random.lognormal(10.5, 0.4))
        elif s == "mass-affluent":
            incomes.append(np.random.lognormal(11.5, 0.3))
        else:
            incomes.append(np.random.lognormal(12.5, 0.4))
    incomes = np.array(incomes).round(0)
    registered = [datetime(2022, 1, 1) + timedelta(days=int(np.random.randint(0, 1200)))
                  for _ in ids]
    # Намеренно вставляем грязь
    for i in range(N_CLIENTS):
        if np.random.rand() < 0.03:
            cities[i] = ""          # пропуск
    return pd.DataFrame({
        "client_id": ids,
        "age": ages,
        "city": cities,
        "segment": segments,
        "monthly_income": incomes,
        "registered_at": registered,
    })


def gen_transactions(clients: pd.DataFrame) -> pd.DataFrame:
    rows = []
    cats = ["transfer", "payment", "withdrawal", "deposit"]
    start = datetime(2026, 1, 1)
    for _, c in clients.iterrows():
        n = int(np.random.poisson(40 if c["segment"] != "premium" else 80))
        for _ in range(n):
            day = np.random.randint(0, 120)
            ts = start + timedelta(days=int(day), seconds=int(np.random.randint(0, 86400)))
            cat = np.random.choice(cats, p=[0.45, 0.30, 0.15, 0.10])
            base = c["monthly_income"] / 30 * np.random.lognormal(0, 0.7)
            if cat == "withdrawal":
                base *= 2
            # Иногда «крупные» транзакции — потенциальные аномалии
            if np.random.rand() < 0.01:
                base *= 50
            rows.append((c["client_id"], ts.isoformat(), cat, round(base, 2)))
    df = pd.DataFrame(rows, columns=["client_id", "ts", "category", "amount"])
    # Грязь: дубликаты и неправильный регистр
    dups = df.sample(frac=0.001).copy()
    df = pd.concat([df, dups], ignore_index=True)
    mask = np.random.rand(len(df)) < 0.005
    df.loc[mask, "category"] = df.loc[mask, "category"].str.upper()
    return df


def main() -> None:
    print("⚙️  Генерация...")
    clients = gen_clients()
    tx = gen_transactions(clients)
    clients.to_csv(CLIENTS_PATH, index=False)
    tx.to_csv(TX_PATH, index=False)
    print(f"✅ clients.csv      : {len(clients):,} строк")
    print(f"✅ transactions.csv : {len(tx):,} строк")

    print(
        """
============================================================
🧠 ВАШИ ЗАДАНИЯ (ответы — в решение_1_eda.py)
============================================================

TODO 1. Загрузите оба CSV в Pandas с правильными типами:
        - client_id    : int32
        - age          : int8
        - city         : category
        - segment      : category
        - registered_at: datetime
        - ts           : datetime

TODO 2. Сделайте EDA (выведите):
        - shape и dtypes
        - df.describe(include='all')
        - топ-5 городов по количеству клиентов
        - распределение сегментов (в процентах)

TODO 3. Постройте графики (matplotlib/seaborn):
        a) Гистограмма возрастов клиентов.
        b) Boxplot monthly_income по сегментам.
        c) Топ-10 транзакций по amount (bar chart).
        d) Динамика суммы транзакций по дням (line chart).

TODO 4. Аналитика:
        a) Сколько % клиентов из Москвы и Питера вместе?
        b) Средний месячный доход по сегментам?
        c) В каком сегменте больше всего «крупных» транзакций (>P99)?

TODO 5. Качество данных:
        a) Сколько строк с пустым city?
        b) Есть ли дубликаты в transactions? Сколько?
        c) Сколько уникальных значений у category (должно быть 4)?

После решения — сверьтесь с решение_1_eda.py.
        """
    )


if __name__ == "__main__":
    main()
