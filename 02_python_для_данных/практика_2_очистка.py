"""
🧩 Практика 2 модуля 02 — Очистка «грязного» CSV

Сценарий: коллега из бухгалтерии прислал выгрузку. Она ужасна:
кодировка какая-то, формат дат «русский», суммы со пробелами и запятыми,
телефоны разной формы, опечатки в категориях.

Что делает скрипт:
  1. Создаёт CSV с реалистичной «грязью» в datasets/dirty_invoices.csv.
  2. Даёт TODO — почистить и привести к нормальному виду.

Решение — в решение_2_очистка.py.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(11)

ROOT = Path(__file__).resolve().parent.parent
PATH = ROOT / "datasets" / "dirty_invoices.csv"
PATH.parent.mkdir(exist_ok=True)

CATEGORIES_GOOD = ["канцелярия", "техника", "услуги", "транспорт"]
CATEGORIES_DIRTY = (
    CATEGORIES_GOOD
    + ["Канцелярия", "КАНЦЕЛЯРИЯ", " канцелярия ", "канцелярия!"]
    + ["технника", "тех ника", "услуги."]
)


def gen() -> None:
    rows = []
    start = date(2025, 1, 1)
    for i in range(1, 5001):
        d = start + timedelta(days=random.randint(0, 365))
        date_str = random.choice([
            d.strftime("%d.%m.%Y"),
            d.strftime("%Y-%m-%d"),
            d.strftime("%d/%m/%Y"),
            "",                  # пропуск даты
        ])
        amount = random.expovariate(1 / 5000) + 100
        amount_str = random.choice([
            f"{amount:,.2f}".replace(",", " "),         # "1 234.56"
            f"{amount:.2f}".replace(".", ","),          # "1234,56"
            f"{amount:.0f} руб",                        # "1234 руб"
            f"{amount:.2f}",
        ])
        phone = "+7" + "".join(random.choices("0123456789", k=10))
        phone = random.choice([
            phone,
            phone.replace("+7", "8"),
            phone[0:2] + " " + phone[2:5] + " " + phone[5:8] + "-" + phone[8:10] + "-" + phone[10:],
            "",                  # пропуск
        ])
        email = f"user{i}@" + random.choice(["mail.ru", "yandex.ru", "gmail.com", "MAIL.RU"])
        if random.random() < 0.02:
            email = email.upper()
        cat = random.choice(CATEGORIES_DIRTY)
        rows.append([i, date_str, amount_str, phone, email, cat])

    # Дубликаты
    for _ in range(50):
        rows.append(random.choice(rows))

    with PATH.open("w", encoding="cp1251", newline="") as f:    # коварная кодировка!
        w = csv.writer(f, delimiter=";")
        w.writerow(["id", "date", "amount", "phone", "email", "category"])
        w.writerows(rows)
    print(f"✅ Создан грязный CSV: {PATH}")
    print(f"   ⚠️  Кодировка: cp1251, разделитель: ;")


def main() -> None:
    gen()
    print(
        """
============================================================
🧠 ЗАДАНИЯ
============================================================

TODO 1. Прочитайте файл правильно (encoding, sep).
TODO 2. Парсите дату — учитывая 3 разных формата.
TODO 3. Приведите amount к числу:
        - удалите пробелы и валютные знаки
        - запятая → точка
        - результат: float
TODO 4. Нормализуйте телефон к формату +7XXXXXXXXXX (10 цифр).
        Используйте regex.
TODO 5. Нормализуйте email: lower-case, strip.
TODO 6. Нормализуйте category:
        - lower-case, strip, убрать знаки препинания
        - привести опечатки к 4 «правильным» категориям
TODO 7. Уберите полные дубликаты.
TODO 8. Сохраните в чистый Parquet: datasets/clean_invoices.parquet
TODO 9. Распечатайте «отчёт об очистке»:
        - строк было / стало
        - сколько дубликатов удалено
        - сколько пропусков по каждой колонке после очистки
        - перечень уникальных category
        """
    )


if __name__ == "__main__":
    main()
