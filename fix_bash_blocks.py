"""
Аккуратная массовая правка markdown-файлов курса BIG_DATA.

ТОЛЬКО трогает блоки с ЯВНЫМ языком bash/sh/shell/zsh.
НЕ трогает: ``` (без языка), ```python, ```sql, ```yaml и т.п. —
там # это часть синтаксиса.

Что делает:
  1. Удаляет полные строки-комментарии `# ...` из bash-блоков.
  2. Обрезает inline `cmd  # пояснение` → `cmd`.
  3. Заменяет «путь к Desktop/БигДата» → `~/Desktop/BIG_DATA`.
  4. Унифицирует пути `~/Desktop/БигДата` → `~/Desktop/BIG_DATA` в bash-блоках.

Запуск из корня курса:
    python3 fix_bash_blocks.py

ВАЖНО: перед запуском — `git status` должен быть чистым,
тогда `git diff` после прогона покажет точно что изменилось.
"""
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
EXCLUDE = {".git", ".venv", "__pycache__", "datasets", "node_modules"}

# Языки, в которых # — это shell-комментарий и его надо вычистить
SHELL_LANGS = {"bash", "sh", "shell", "zsh", "console"}


def is_excluded(path: Path) -> bool:
    return any(part in EXCLUDE for part in path.parts)


def fix_shell_block(block_lines: list[str]) -> tuple[list[str], int]:
    """Чистит shell-блок. Возвращает (новые_строки, число_изменений)."""
    cleaned = []
    n_changes = 0
    for line in block_lines:
        stripped = line.strip()

        # Полная строка-комментарий (# ...) — удалить
        if stripped.startswith("#") and not stripped.startswith("#!"):
            n_changes += 1
            continue

        # Пустая строка после удаления — пропустим если предыдущая тоже пустая
        if stripped == "" and cleaned and cleaned[-1].strip() == "":
            continue

        # Inline-комментарий `command  # explanation`
        # (только если # после 2+ пробелов — это стандартный приём)
        if re.search(r"\s{2,}#\s", line):
            line_clean = re.sub(r"\s{2,}#.*$", "", line.rstrip("\n")).rstrip() + "\n"
            if line_clean != line:
                n_changes += 1
            line = line_clean

        cleaned.append(line)

    # Убрать висящие пустые строки в начале/конце блока
    while cleaned and cleaned[0].strip() == "":
        cleaned.pop(0)
    while cleaned and cleaned[-1].strip() == "":
        cleaned.pop()

    return cleaned, n_changes


def fix_markdown(text: str) -> tuple[str, int]:
    """Возвращает (новый_текст, число_правок)."""
    lines = text.splitlines(keepends=True)
    out = []
    i = 0
    changes = 0

    while i < len(lines):
        line = lines[i]
        # Ищем начало code-блока с явным языком
        m = re.match(r"^(\s*)```([a-zA-Z][a-zA-Z0-9]*)\s*$", line.rstrip("\n"))
        if m:
            indent = m.group(1)
            lang = m.group(2).lower()
            if lang in SHELL_LANGS:
                # Это shell-блок — обработать
                out.append(line)
                i += 1
                block_lines = []
                while i < len(lines) and lines[i].rstrip("\n").strip() != "```":
                    block_lines.append(lines[i])
                    i += 1
                cleaned, n = fix_shell_block(block_lines)
                changes += n
                out.extend(cleaned)
                if i < len(lines):
                    out.append(lines[i])  # закрывающий ```
                i += 1
                continue

        out.append(line)
        i += 1

    new_text = "".join(out)

    # Глобальные замены плейсхолдеров
    replacements = [
        # Точные плейсхолдеры
        ('cd "путь к Desktop/БигДата"', "cd ~/Desktop/BIG_DATA"),
        ('cd "путь к проекту/БигДата"', "cd ~/Desktop/BIG_DATA"),
        ('cd "путь к БигДата"', "cd ~/Desktop/BIG_DATA"),
        # Унификация имени папки в командах
        ("cd ~/Desktop/БигДата", "cd ~/Desktop/BIG_DATA"),
    ]
    for old, new in replacements:
        n_occur = new_text.count(old)
        if n_occur > 0:
            new_text = new_text.replace(old, new)
            changes += n_occur

    return new_text, changes


def main():
    total_files = 0
    changed_files = []

    for md_file in sorted(ROOT.rglob("*.md")):
        if is_excluded(md_file):
            continue
        total_files += 1
        original = md_file.read_text(encoding="utf-8")
        new_text, n_changes = fix_markdown(original)
        if n_changes > 0:
            md_file.write_text(new_text, encoding="utf-8")
            changed_files.append((md_file.relative_to(ROOT), n_changes))

    print("=" * 60)
    print(f"📊 Просмотрено markdown-файлов: {total_files}")
    print(f"🔧 Изменено: {len(changed_files)}")
    print("=" * 60)
    for path, n in sorted(changed_files, key=lambda x: -x[1]):
        print(f"  ✏️  {path}  ({n} правок)")
    print()
    print("✅ Готово.")
    print()
    print("Дальше:")
    print("  1. python3 audit_links.py")
    print("  2. git diff   — посмотреть изменения")
    print("  3. git add . && git commit -m '...' && git push")


if __name__ == "__main__":
    main()
