"""
🧩 Практика 1 модуля 01 — Анализ датасета по модели 5V

╔══════════════════════════════════════════════════════════════════╗
║  📖 ЧТО ЭТО ЗА ФАЙЛ?                                              ║
║                                                                  ║
║  Этот файл — генератор данных + список заданий.                  ║
║  Запустите ОДИН раз — он создаст датасет и покажет, что делать.  ║
║                                                                  ║
║  Решение пишите в ДРУГОМ файле:  моё_решение_1.py                ║
╚══════════════════════════════════════════════════════════════════╝

📋 КАК РАБОТАТЬ — за 4 шага:

   ШАГ 1.  Откройте Terminal, активируйте venv:
           cd ~/Desktop/BIG_DATA
           source .venv/bin/activate

   ШАГ 2.  Запустите этот файл — создастся датасет и появится список TODO:
           python 01_основы_BigData/практика_1_анализ_5V.py

   ШАГ 3.  Скопируйте шаблон в свой рабочий файл:
           cp 01_основы_BigData/моё_решение_шаблон.py 01_основы_BigData/моё_решение_1.py

   ШАГ 4.  Откройте моё_решение_1.py в VS Code, заполняйте TODO,
           после каждого запускайте и проверяйте:
           python 01_основы_BigData/моё_решение_1.py

🆘 Если что-то непонятно — см. ресурсы/КАК_ПРОХОДИТЬ_КУРС.md
"""
import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "datasets"
DATASET_DIR.mkdir(exist_ok=True)
CSV_PATH = DATASET_DIR / "transactions_sample.csv"

N_ROWS = 50_000
CLIENTS = [f"C{idx:05d}" for idx in range(1, 1001)]
CATEGORIES = ["перевод", "оплата_услуг", "снятие_наличных", "пополнение"]
CURRENCIES = ["RUB", "USD", "EUR"]
START = datetime(2026, 1, 1)


def make_row(i: int) -> list:
    client = random.choice(CLIENTS)
    client = client if random.random() > 0.02 else ""
    amount = round(random.expovariate(1 / 5000), 2)
    if random.random() < 0.01:
        amount *= 100
    currency = random.choices(CURRENCIES, weights=[0.85, 0.1, 0.05])[0]
    cat = random.choice(CATEGORIES)
    if random.random() < 0.005:
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


def show_tasks() -> None:
    print()
    print("=" * 64)
    print("📝 ЗАДАНИЯ — копируйте шаблон и пишите код в моё_решение_1.py")
    print("=" * 64)
    print(
        """
TODO 1.  Прочитайте файл transactions_sample.csv с помощью pandas.
         Выведите: shape, dtypes, df.head().

TODO 2.  Посчитайте долю строк, где client_id пустой.
         Сколько таких записей?

TODO 3.  Найдите выбросы по amount (выше 99-го перцентиля).
         Сколько их и какие топ-5?

TODO 4.  Покажите ВСЕ уникальные значения в колонке "category".
         Должно быть 4 значения. Если больше — найдите опечатки.

TODO 5.  Письменно (в комментариях) ответьте:
         а) Это Big Data? Какие из 5V проявляются?
         б) Что сломается, если строк будет 50 МЛРД?
         в) Зачем такой датасет в бизнесе?

────────────────────────────────────────────────────────────────
🚀 Дальше — два действия:

   1) Создайте свой файл-решение (если ещё не создали):

      cp 01_основы_BigData/моё_решение_шаблон.py 01_основы_BigData/моё_решение_1.py

   2) Откройте моё_решение_1.py в VS Code и заполняйте TODO.
      Запускайте после каждого:

      python 01_основы_BigData/моё_решение_1.py

📖 Эталон — открыть ПОСЛЕ ваших попыток:
   01_основы_BigData/решение_1_анализ_5V.py
        """
    )


if __name__ == "__main__":
    generate()
    show_tasks()
