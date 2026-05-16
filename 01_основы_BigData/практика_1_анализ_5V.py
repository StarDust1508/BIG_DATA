"""
🧩 Практика 1 — Анализ датасета по модели 5V

Сценарий: вы устроились дата-аналитиком в юридическую фирму.
Вам передали лог транзакций клиентов. Нужно ответить на вопросы:

   1. Это «Big Data» или нет?
   2. Какие из 5V здесь «выстреливают»?
   3. Какие проблемы с качеством данных вы видите?

Скрипт делает две вещи:
  • Генерирует синтетический CSV-файл (~50 000 строк) в ../datasets/
  • Выводит подсказки и вопросы для самопроверки.

ВАШИ ЗАДАНИЯ — внизу файла (отмечены TODO).
Решение — в файле решение_1_анализ_5V.py (открывайте только ПОСЛЕ своей попытки).
"""
import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

# -- Куда положить датасет ---------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "datasets"
DATASET_DIR.mkdir(exist_ok=True)
CSV_PATH = DATASET_DIR / "transactions_sample.csv"

# -- Генерация синтетических данных -----------------------------------------
N_ROWS = 50_000
CLIENTS = [f"C{idx:05d}" for idx in range(1, 1001)]
CATEGORIES = ["перевод", "оплата_услуг", "снятие_наличных", "пополнение"]
CURRENCIES = ["RUB", "USD", "EUR"]
START = datetime(2026, 1, 1)


def make_row(i: int) -> list:
    client = random.choice(CLIENTS)
    # Нарочно вставляем «грязь»: иногда пропуски, дубликаты сумм, опечатки
    client = client if random.random() > 0.02 else ""        # 2% пропусков
    amount = round(random.expovariate(1 / 5000), 2)
    if random.random() < 0.01:                                # 1% подозрительно крупных
        amount *= 100
    currency = random.choices(CURRENCIES, weights=[0.85, 0.1, 0.05])[0]
    cat = random.choice(CATEGORIES)
    if random.random() < 0.005:                                # 0.5% опечаток в категории
        cat = cat.upper()
    ts = START + timedelta(seconds=random.randint(0, 60 * 60 * 24 * 120))
    return [i, client, ts.isoformat(), cat, amount, currency]


def generate() -> None:
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "client_id", "timestamp", "category", "amount", "currency"])
        for i in range(1, N_ROWS + 1):
            writer.writerow(make_row(i))
    size_mb = os.path.getsize(CSV_PATH) / 1024 / 1024
    print(f"✅ Сгенерирован файл: {CSV_PATH}")
    print(f"   Строк: {N_ROWS:,}")
    print(f"   Размер: {size_mb:.2f} МБ")


# ============================================================================
# 🧠 ЗАДАНИЯ
# ============================================================================
def tasks() -> None:
    print("\n" + "=" * 60)
    print("🧠 ЗАДАНИЯ — попробуйте сами, потом сверьтесь с решением")
    print("=" * 60)

    print(
        """
TODO 1. Прочитайте файл transactions_sample.csv с помощью pandas.
        Выведите: shape, dtypes, df.head(), df.describe().

TODO 2. Посчитайте долю строк, где client_id пустой.
        Сколько таких записей? Что бы вы предложили с ними делать?

TODO 3. Найдите явные «выбросы» по полю amount (например,
        сумму выше 99-го перцентиля). Сколько их и какие?

TODO 4. Найдите аномалии в поле category — должны быть только
        4 значения, но из-за опечаток их больше. Покажите все
        уникальные категории.

TODO 5. Ответьте письменно (в мои_заметки.md) на вопросы:
        a) Этот датасет — Big Data?
        b) Какие из 5V проявляются?
        c) Если бы строк было не 50 000, а 50 миллиардов —
           что бы сломалось в вашем коде из TODO 1?

После этого откройте решение_1_анализ_5V.py для сверки.
"""
    )


if __name__ == "__main__":
    generate()
    tasks()
