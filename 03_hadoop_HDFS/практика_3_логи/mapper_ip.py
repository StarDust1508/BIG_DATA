#!/usr/bin/env python3
"""
Mapper: достаёт IP-адрес и выдаёт ip\t1.
"""
import sys


def main() -> None:
    for line in sys.stdin:
        # IP — первый «слово» в строке access.log
        parts = line.split(" ", 1)
        if not parts:
            continue
        ip = parts[0].strip()
        if ip:
            print(f"{ip}\t1")


if __name__ == "__main__":
    main()
