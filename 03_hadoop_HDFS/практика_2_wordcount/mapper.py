#!/usr/bin/env python3
"""
WordCount — MAPPER.

Читает строки из stdin, разбивает на слова, выводит "слово\\t1".
"""
import sys


def main() -> None:
    for line in sys.stdin:
        for raw in line.strip().lower().split():
            # Чистим пунктуацию по краям
            word = "".join(c for c in raw if c.isalpha())
            if word:
                print(f"{word}\t1")


if __name__ == "__main__":
    main()
