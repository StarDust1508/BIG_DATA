"""
🧩 Практика 3 модуля 02 — Обезличивание ПДн

Сценарий: вам передали выгрузку клиентов с прямыми идентификаторами
(ФИО, email, телефон, паспорт). Нужно подготовить версию для аналитической
команды — обезличенную, но всё ещё пригодную для аналитики.

Что делает скрипт:
  1. Генерирует датасет с реалистичными ПДн.
  2. Сохраняет в datasets/clients_pii.csv.
  3. Даёт TODO — построить pipeline псевдонимизации.

Решение — в решение_3_обезличивание.py.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(17)

ROOT = Path(__file__).resolve().parent.parent
PATH = ROOT / "datasets" / "clients_pii.csv"
PATH.parent.mkdir(exist_ok=True)

LAST_NAMES  = ["Иванов", "Петров", "Сидоров", "Козлов", "Соколов", "Лебедев",
               "Морозов", "Васильев", "Кузнецов", "Попов", "Новиков"]
FIRST_NAMES = ["Иван", "Алексей", "Дмитрий", "Сергей", "Андрей", "Михаил",
               "Анна", "Мария", "Елена", "Ольга", "Наталья"]
MIDDLES_M   = ["Иванович", "Петрович", "Алексеевич", "Сергеевич", "Дмитриевич"]
MIDDLES_F   = ["Ивановна", "Петровна", "Алексеевна", "Сергеевна", "Дмитриевна"]
CITIES      = ["Москва", "Санкт-Петербург", "Казань", "Новосибирск", "Самара"]
STREETS     = ["Ленина", "Пушкина", "Гагарина", "Победы", "Мира", "Советская"]


def gen() -> None:
    rows = []
    for i in range(1, 3001):
        is_female = random.random() < 0.5
        last = random.choice(LAST_NAMES) + ("а" if is_female else "")
        first = random.choice(FIRST_NAMES)
        middle = random.choice(MIDDLES_F if is_female else MIDDLES_M)
        bd = date(1960, 1, 1) + timedelta(days=random.randint(0, 365 * 50))
        passport = f"{random.randint(1000, 9999)} {random.randint(100000, 999999)}"
        snils = f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(100, 999)} {random.randint(10, 99)}"
        phone = "+79" + "".join(random.choices("0123456789", k=9))
        email = f"{first.lower()}.{last.lower()}{i}@" + random.choice(["mail.ru", "gmail.com"])
        city = random.choice(CITIES)
        street = random.choice(STREETS) + ", д. " + str(random.randint(1, 100))
        salary = random.randint(40_000, 500_000)
        rows.append([
            i, last, first, middle, bd.isoformat(),
            passport, snils, phone, email, city, street, salary,
        ])

    with PATH.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "client_id", "last_name", "first_name", "middle_name", "birth_date",
            "passport", "snils", "phone", "email", "city", "street", "salary",
        ])
        w.writerows(rows)
    print(f"✅ Создан файл с ПДн: {PATH}")


def main() -> None:
    gen()
    print(
        """
============================================================
🧠 ЗАДАНИЯ
============================================================

Аналитической команде нужно знать: возраст, город, доход, чтобы
строить сегментацию. ФИО, паспорт, email — не нужны.
БЕЗОПАСНОСТЬ команда должна сохранить возможность вернуться к ПДн.

TODO 1. Прочитайте clients_pii.csv.

TODO 2. Создайте функцию pseudonymize(df, salt) -> df, которая:
        - удаляет: last_name, first_name, middle_name, passport,
          snils, phone, email, street
        - заменяет client_id на token = sha256(salt + client_id)[:16]
        - оставляет: city, salary
        - добавляет колонку age (вычислите из birth_date), удаляет birth_date
        - возвращает копию df (не мутирует оригинал)

TODO 3. Соль возьмите из переменной окружения PSEUDO_SALT
        (если не задана — используйте 'dev-salt' и НАПИШИТЕ предупреждение).

TODO 4. Создайте маппинг-таблицу:
        clients_pii[["client_id", "token"]]  (для возможного back-lookup)
        и сохраните её в datasets/mapping.parquet
        (в реальном проекте — в защищённое хранилище!).

TODO 5. Сохраните обезличенный датасет в datasets/clients_anonymous.parquet.

TODO 6. Проверьте k-анонимность по (age, city, salary_band) где
        salary_band = (salary // 50_000) * 50_000.
        Найдите все группы с k < 3.
        Что бы вы сделали с такими записями?

TODO 7. Юридический чек-лист (письменно в комментариях):
        - Это псевдонимизация или анонимизация? Почему?
        - ПДн ли это с точки зрения 152-ФЗ?
        - Где хранить mapping.parquet?

После решения — сверьтесь с решение_3_обезличивание.py.
        """
    )


if __name__ == "__main__":
    main()
