#!/usr/bin/env python3
"""
Reducer для задания 3: считает СРЕДНЕЕ значение по каждому ключу.

Внимание: для среднего combiner не равен reducer'у напрямую,
т.к. avg не ассоциативна без передачи (sum, count).
В учебных целях здесь не оптимизируем — просто копим в памяти.
"""
import sys


def main() -> None:
    current: str | None = None
    total = 0
    n = 0

    for line in sys.stdin:
        try:
            key, value = line.rstrip("\n").split("\t", 1)
            value = int(value)
        except ValueError:
            continue
        if current == key:
            total += value
            n += 1
        else:
            if current is not None and n:
                print(f"{current}\t{total / n:.2f}")
            current = key
            total = value
            n = 1
    if current is not None and n:
        print(f"{current}\t{total / n:.2f}")


if __name__ == "__main__":
    main()
