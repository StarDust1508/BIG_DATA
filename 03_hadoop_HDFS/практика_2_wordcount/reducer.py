#!/usr/bin/env python3
"""
WordCount — REDUCER.

Принимает отсортированный stdin вида "слово\\tN".
Идёт построчно, суммирует значения для одинаковых ключей.
"""
import sys


def main() -> None:
    current_word: str | None = None
    current_count = 0

    for line in sys.stdin:
        try:
            word, n = line.rstrip("\n").split("\t", 1)
            n = int(n)
        except ValueError:
            continue

        if current_word == word:
            current_count += n
        else:
            if current_word is not None:
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = n

    if current_word is not None:
        print(f"{current_word}\t{current_count}")


if __name__ == "__main__":
    main()
