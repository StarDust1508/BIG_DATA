# 🍎 Apple Silicon (M1 / M2 / M3 / M4): особенности установки

> Если у вас Mac с чипом Apple Silicon — прочитайте этот раздел. На Intel-Mac можно пропустить.
> Положите в `ресурсы/APPLE_SILICON.md` или вставьте раздел в `УСТАНОВКА.md`.

---

## Как узнать, у меня M-чип или Intel

В Terminal:

```
uname -m
```

- `arm64` → M1/M2/M3/M4. **Этот раздел для вас.**
- `x86_64` → Intel. Можете пропустить.

Или: 🍎 → About This Mac. Если написано «Apple M1/M2/M3/M4» — arm64.

---

## ✅ Что работает «из коробки»

- **Python 3.11** через Homebrew — нативный arm64. ✅
- **Java 17 OpenJDK** через Homebrew — нативный arm64. ✅
- **PySpark 3.5+** — поддерживает arm64. ✅
- **pandas, numpy** — нативные wheel для arm64. ✅

То есть **основная установка** проходит без специальных шагов.

---

## ⚠️ Где бывают проблемы

### 1. Старый Python от Anaconda / Miniconda

Если на машине стоит **Anaconda x86_64**, он использует Rosetta (эмуляцию Intel). Это медленно и иногда конфликтует.

**Проверьте:**

```
python3 -c "import platform; print(platform.machine())"
```

Если показывает `x86_64`, а вы на arm64 — у вас **эмулированный Python**. Это часто причина странных багов.

**Решение:**

- Удалите Anaconda или используйте **Miniforge** (arm64-нативный аналог).
- Или используйте **Homebrew Python**, как в нашей инструкции.

### 2. pyarrow / fastparquet — может потребовать компиляции

На некоторых версиях macOS pyarrow ставится из исходников. Если не хватает компилятора:

```
ERROR: Failed building wheel for pyarrow
clang: error: no such file or directory: ...
```

**Решение:** установите Xcode Command Line Tools:

```
xcode-select --install
```

Откроется окно установки, нажмите «Install». Подождите 5–10 минут.

После повторите:

```
pip install -r requirements.txt
```

### 3. JVM не запускается с `bad CPU type`

Очень редко на старых версиях Homebrew Java может быть x86_64 (эмулированной).

**Проверьте:**

```
file $(which java)
```

Должно показать `Mach-O 64-bit executable arm64`. Если показывает `x86_64` — переустановите Java:

```
brew uninstall openjdk@17
```

```
brew install openjdk@17
```

### 4. TensorFlow / Torch на Apple Silicon — отдельная тема

В нашем курсе мы это **не используем**, но для общего ML:

- **TensorFlow**: `pip install tensorflow-macos` (отдельный пакет для arm64).
- **PyTorch**: с 2.0+ работает нативно, обычный `pip install torch`.

Если столкнётесь — пишите.

### 5. `pip` ставит x86_64 wheel вместо arm64

Редко, но бывает.

**Проверьте**, что pip берёт правильную архитектуру:

```
pip --version
```

Должно показать путь к Python в `.venv/lib/python3.11/...` (если venv активен).

Принудительно поставить arm64-версию (на примере `pyarrow`):

```
pip install --platform macosx_11_0_arm64 --only-binary=:all: pyarrow
```

---

## 🚀 Бонус: Spark на Apple Silicon — быстрее ли?

Тесты на типичных задачах:
- **M1 Pro (10 ядер) vs Intel i7 (8 ядер)** — обычно M1 на 30–50% быстрее на одинаковых данных.
- **M2 Pro / M3 Pro** — ещё быстрее, особенно на параллельных задачах.

Spark в `local[*]` режиме на M-чипе — отличный вариант для разработки на 5–50 ГБ данных.

---

## ⛔ Что **НЕ** работает на Apple Silicon

- **Docker Desktop** — работает, но контейнеры x86_64 идут через Rosetta. Если вам нужен Hadoop в Docker — образ `bde2020/hadoop-namenode` x86_64. Запустится, но медленно. Альтернативы:
  - **Rancher Desktop** или **OrbStack** — лучшая нативная производительность arm64.
  - Образы с тегом `arm64` если есть (для Hadoop их пока нет официальных).
- **Старые версии Java (8, 11) через Homebrew** — иногда только x86_64. Используйте Java 17+.
- **Hive Server 2 классический** — х86_64 only, через Rosetta.

---

## ⚡ Финальный чек-лист для Apple Silicon

```
uname -m              → arm64
python3 -c "import platform; print(platform.machine())" → arm64
file $(which java)    → arm64
file $(which python3) → arm64
```

Если все 4 ответа `arm64` — у вас **нативная** установка, всё будет работать быстро и без эмуляции.

---

## 🆘 Если что-то странное

Типичные ошибки именно на Apple Silicon:
- `bad CPU type in executable`
- `Symbol not found: _OBJC_CLASS_$_...`
- `wheel is not supported on this platform`

Все они — про несовпадение архитектуры. Решение:
1. Удалить виновный пакет.
2. Удалить Anaconda (если есть).
3. Использовать Homebrew Python + venv.
4. Поставить заново.

В 99% случаев это лечит.
