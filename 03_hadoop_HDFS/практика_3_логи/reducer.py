#!/usr/bin/env python3
"""
Универсальный reducer: суммирует значения для одинаковых ключей.

Вход: отсортированные пары "key\\tN" (по stdin)
Выход: "key\\tsum"
"""
import sys


def main() -> None:
    current: str | None = None
    total = 0

    for line in sys.stdin:
        try:
            key, n = line.rstrip("\n").split("\t", 1)
            n = int(n)
        except ValueError:
            continue
        if current == key:
            total += n
        else:
            if current is not None:
                print(f"{current}\t{total}")
            current = key
            total = n
    if current is not None:
        print(f"{current}\t{total}")


if __name__ == "__main__":
    main()
