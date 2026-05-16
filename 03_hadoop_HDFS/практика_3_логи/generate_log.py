"""
Генератор синтетического access.log в формате Apache Common Log.

Запуск:
    python3 generate_log.py

Создаст файл access.log в текущей папке.
"""
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

N = 50_000

# Несколько «активных» клиентов с большим весом + длинный хвост случайных
HOT_IPS = [f"10.0.0.{i}" for i in range(1, 11)]
COLD_IPS = [f"192.168.{i}.{j}" for i in range(1, 6) for j in range(1, 50)]

METHODS = ["GET", "POST", "PUT", "DELETE"]
PATHS = [
    "/", "/login", "/api/users", "/api/orders", "/api/products",
    "/static/main.css", "/static/app.js", "/health", "/admin", "/api/search",
]
STATUSES = [200, 200, 200, 200, 200, 301, 302, 304, 401, 403, 404, 500, 503]

START = datetime(2026, 5, 1, 0, 0, 0)


def main() -> None:
    out = Path(__file__).resolve().parent / "access.log"
    with out.open("w", encoding="utf-8") as f:
        for i in range(N):
            # 80% — hot IPs, 20% — cold
            ip = random.choice(HOT_IPS) if random.random() < 0.8 else random.choice(COLD_IPS)
            ts = START + timedelta(seconds=i * 2 + random.randint(0, 5))
            ts_str = ts.strftime("%d/%b/%Y:%H:%M:%S +0000")
            method = random.choices(METHODS, weights=[0.8, 0.15, 0.03, 0.02])[0]
            path = random.choice(PATHS)
            status = random.choice(STATUSES)
            size = random.randint(100, 50_000) if status < 400 else random.randint(50, 500)
            f.write(f'{ip} - - [{ts_str}] "{method} {path} HTTP/1.1" {status} {size}\n')
    print(f"✅ Создан {out} ({N:,} строк)")
    print(f"   Размер: {out.stat().st_size / 1024 / 1024:.2f} МБ")


if __name__ == "__main__":
    main()
