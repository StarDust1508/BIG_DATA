#!/usr/bin/env python3
"""
Mapper для задания 3: выдаёт пары status\\tbytes — для подсчёта
средней «длины ответа» по каждому статусу.
"""
import sys


def main() -> None:
    for line in sys.stdin:
        tokens = line.rstrip("\n").split(" ")
        if len(tokens) < 2:
            continue
        status = tokens[-2]
        size = tokens[-1]
        if status.isdigit() and size.isdigit():
            print(f"{status}\t{size}")


if __name__ == "__main__":
    main()
