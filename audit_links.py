"""
Аудитор ссылок в учебном репозитории.

Проходит по всем .md файлам и проверяет, что относительные ссылки указывают
на существующие файлы.

Запуск:
    python3 audit_links.py
"""
import re
import sys
from pathlib import Path


# Регулярка для markdown-ссылок: [текст](относительный/путь.md)
LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

# Исключения: внешние URL и спецссылки, которые не проверяем
SKIP_PREFIX = ("http://", "https://", "mailto:", "computer://", "#")


def is_external(url: str) -> bool:
    return url.startswith(SKIP_PREFIX)


def check_file(md_file: Path, root: Path) -> list[tuple[str, str]]:
    """Возвращает список (текст_ссылки, неработающий_путь)."""
    broken = []
    text = md_file.read_text(encoding="utf-8")
    for match in LINK_RE.finditer(text):
        link_text, link_url = match.group(1), match.group(2)

        # Убираем якорь
        link_url = link_url.split("#", 1)[0].strip()

        if not link_url or is_external(link_url):
            continue

        # Резолвим относительно файла
        target = (md_file.parent / link_url).resolve()

        if not target.exists():
            broken.append((link_text, link_url))
    return broken


def main() -> int:
    root = Path(__file__).resolve().parent
    total_files = 0
    total_links = 0
    total_broken = 0
    broken_files = 0

    for md_file in sorted(root.rglob("*.md")):
        if ".venv" in md_file.parts or "__pycache__" in md_file.parts:
            continue
        total_files += 1
        broken = check_file(md_file, root)
        # Счёт всех ссылок для статистики
        n_links = len(LINK_RE.findall(md_file.read_text(encoding="utf-8")))
        total_links += n_links
        if broken:
            broken_files += 1
            print(f"\n❌ {md_file.relative_to(root)}")
            for text, url in broken:
                print(f"   [{text}]({url}) — НЕ НАЙДЕНО")
                total_broken += 1

    print("\n" + "=" * 60)
    print("📊 Аудит ссылок")
    print("=" * 60)
    print(f"Проверено markdown-файлов: {total_files}")
    print(f"Всего ссылок (включая внешние): {total_links}")
    print(f"Битых внутренних ссылок: {total_broken}")
    print(f"Файлов с битыми ссылками: {broken_files}")

    if total_broken == 0:
        print("\n✅ Все внутренние ссылки рабочие.")
        return 0
    print("\n⚠️  Есть битые ссылки. См. выше.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
