#!/usr/bin/env python3
"""
Mapper: достаёт HTTP-статус и выдаёт status\t1.

В Apache Common Log формате статус — вторая с конца цифра в строке:
   ... "GET /path HTTP/1.1" 200 1532
                              ^^^
"""
import sys


def main() -> None:
    for line in sys.stdin:
        tokens = line.rstrip("\n").split(" ")
        if len(tokens) < 2:
            continue
        # Статус — предпоследний токен
        status = tokens[-2]
        if status.isdigit():
            print(f"{status}\t1")


if __name__ == "__main__":
    main()
