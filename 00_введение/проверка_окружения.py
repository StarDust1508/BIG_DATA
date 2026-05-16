"""
Скрипт-самопроверка окружения.
Запуск:  python проверка_окружения.py

Проверяет:
1. Версию Python (нужно 3.10+)
2. Наличие Java (через переменную окружения JAVA_HOME)
3. Установленные ключевые библиотеки
"""
import os
import sys
import shutil
import subprocess


def check(label: str, ok: bool, detail: str = "") -> None:
    """Печатает строку результата проверки."""
    mark = "✅" if ok else "❌"
    print(f"{mark} {label}{(' — ' + detail) if detail else ''}")


def main() -> int:
    print("=" * 60)
    print("🔍 Проверка окружения для курса Big Data")
    print("=" * 60)

    failures = 0

    # 1. Python
    py_ok = sys.version_info >= (3, 10)
    check(
        f"Python {sys.version_info.major}.{sys.version_info.minor}",
        py_ok,
        "нужен 3.10+" if not py_ok else "",
    )
    failures += not py_ok

    # 2. Java
    java_path = shutil.which("java")
    if java_path:
        try:
            result = subprocess.run(
                ["java", "-version"], capture_output=True, text=True, timeout=5
            )
            version_line = (result.stderr or result.stdout).splitlines()[0]
            check("Java установлена", True, version_line)
        except Exception as exc:
            check("Java установлена", False, str(exc))
            failures += 1
    else:
        check("Java установлена", False, "не найдена в PATH")
        failures += 1

    java_home = os.environ.get("JAVA_HOME")
    check("JAVA_HOME задан", bool(java_home), java_home or "переменная пустая")
    if not java_home:
        failures += 1

    # 3. Библиотеки
    libs = [
        "numpy",
        "pandas",
        "matplotlib",
        "pyspark",
        "pyarrow",
        "sklearn",
        "jupyter",
    ]
    for name in libs:
        try:
            mod = __import__(name)
            version = getattr(mod, "__version__", "n/a")
            check(f"Библиотека {name}", True, version)
        except ImportError:
            check(f"Библиотека {name}", False, "не установлена")
            failures += 1

    print("=" * 60)
    if failures == 0:
        print("🎉 Всё готово! Запускайте hello_spark.py")
        return 0
    print(f"⚠️  Не пройдено проверок: {failures}. См. УСТАНОВКА.md")
    return 1


if __name__ == "__main__":
    sys.exit(main())
