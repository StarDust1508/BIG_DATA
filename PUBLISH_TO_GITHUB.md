# 🚀 Публикация курса на GitHub

> Пошаговая инструкция. Курс уже подготовлен — осталось запушить.

Целевой репозиторий: **https://github.com/StarDust1508/BIG_DATA**

---

## ⚡ Быстрый путь (если уже разбираетесь в Git)

В терминале на вашем Mac:

```bash
cd ~/Desktop/БигДата

# Если git ещё не инициализирован — в этом курсе уже сделано Claude,
# но если что — повторите:
git init
git branch -M main

# Подключите ваш GitHub-репозиторий (если ещё не подключён)
git remote add origin https://github.com/StarDust1508/BIG_DATA.git

# Закоммитьте всё
git add .
git commit -m "Initial commit: полный курс Big Data на русском"

# Запушьте
git push -u origin main
```

Если репозиторий не пустой и пуш отклонён — см. ниже **«Если возникли конфликты»**.

---

## 📝 Пошаговая инструкция

### Шаг 1: Проверьте, что у вас стоит Git

В терминале:
```bash
git --version
```

Если выводится версия — Git стоит. Если нет:
- **macOS:** `brew install git` (или установится при первой команде через Xcode CLT).
- **Linux:** `sudo apt install git`.
- **Windows:** https://git-scm.com/download/win

---

### Шаг 2: Настройте Git (один раз в жизни)

```bash
git config --global user.name "Ваше Имя"
git config --global user.email "Av9272779188@gmail.com"
```

Имя и email будут видны в коммитах. На GitHub email можно скрыть в настройках.

---

### Шаг 3: Создайте репозиторий на GitHub

Скорее всего вы это уже сделали (раз дали ссылку). Если **не** — зайдите на https://github.com/new:
- Repository name: `BIG_DATA`
- Description: «Полный курс по Big Data на русском — от концепций до распределённого ML на Apache Spark»
- Public ✅
- НЕ ставьте галочки на «Add a README file» / «Add .gitignore» / «Choose a license» — у нас уже есть свои.

---

### Шаг 4: Подготовка к пушу (одна команда)

В Terminal перейдите в папку курса:
```bash
cd ~/Desktop/БигДата
```

Проверьте, что вы в правильной папке:
```bash
ls -la
# Должны увидеть: README.md, requirements.txt, 00_введение, 01_основы_BigData, ...
```

---

### Шаг 5: Аутентификация на GitHub

Современный способ — **personal access token (PAT)**:

1. Зайдите на https://github.com/settings/tokens
2. **Generate new token (classic)**.
3. Note: «BIG_DATA push».
4. Expiration: 90 days.
5. Scopes: ☑ **repo** (полный доступ).
6. Generate token.
7. **Скопируйте токен сразу** — он показывается ОДИН раз.

Сохраните токен в **безопасном месте** (Keychain, 1Password). Это пароль.

Альтернатива: **SSH-ключ**. Удобнее в долгосрочной перспективе.
- https://docs.github.com/ru/authentication/connecting-to-github-with-ssh

---

### Шаг 6: Команды для публикации

```bash
cd ~/Desktop/БигДата

# Инициализация (если ещё не сделано)
git init
git branch -M main

# Связь с GitHub
git remote add origin https://github.com/StarDust1508/BIG_DATA.git
# Если remote уже есть — выполнит ошибку, тогда:
# git remote set-url origin https://github.com/StarDust1508/BIG_DATA.git

# Посмотреть, что Git готов закоммитить
git status

# Добавить все файлы
git add .

# Закоммитить
git commit -m "Initial commit: полный курс Big Data на русском, 24000+ строк"

# Запушить
git push -u origin main
```

При `git push` спросит:
- **Username:** ваш GitHub-логин (`StarDust1508`).
- **Password:** ваш токен из шага 5 (не пароль GitHub!).

---

### Шаг 7: Проверьте, что всё запушилось

Откройте https://github.com/StarDust1508/BIG_DATA — должны увидеть все файлы.

---

## 🛠 Что уже подготовлено в репозитории

- ✅ `.gitignore` — не коммитятся датасеты, кэши, секреты.
- ✅ `LICENSE` — MIT с правовым дисклеймером.
- ✅ `CONTRIBUTING.md` — открыто к доработкам.
- ✅ `README.md` — обновлённый с badge'ами.
- ✅ `audit_links.py` — проверка ссылок (370 ссылок — 0 битых).
- ✅ Все Python-скрипты прошли проверку синтаксиса.

---

## ⚠️ Если возникли конфликты

### Если GitHub-репо не пустой (есть README.md от GitHub)

Объедините истории:
```bash
git pull origin main --rebase --allow-unrelated-histories
# Решите конфликты, если они есть (откройте файлы с маркерами <<<<<<<)
git push -u origin main
```

Или **сначала затрите** контент GitHub:
```bash
git pull origin main --rebase
git push -f origin main    # ⚠️ перезапишет GitHub полностью
```

### Если push выдаёт «authentication failed»

- Проверьте логин/токен.
- Убедитесь, что токен имеет права **repo**.
- Если используется SSH, проверьте `~/.ssh/config` и `ssh -T git@github.com`.

### Если push выдаёт «remote: Repository not found»

- Опечатка в URL.
- Репозиторий приватный, а у токена нет прав.
- Имя репо отличается. Проверьте: https://github.com/StarDust1508/BIG_DATA → существует ли.

---

## 🎯 После публикации

Сделайте репозиторий «живым»:

1. **Включите GitHub Pages** для красивого view (Settings → Pages → main branch / root).
2. **Добавьте Topics** на странице репо: `big-data`, `pyspark`, `russian-course`, `mlops`, `data-engineering`, `learning`.
3. **Прикрепите ссылку** в LinkedIn / HH.ru / резюме.
4. **Поставьте звезду** (сам себе) — пусть будет 1 ⭐.

---

## 📞 Альтернатива через GitHub Desktop (если в командной строке неуверенно)

1. Скачать https://desktop.github.com/
2. Открыть **GitHub Desktop**, войти в аккаунт `StarDust1508`.
3. File → Add Local Repository → выберите `~/Desktop/БигДата`.
4. GitHub Desktop предложит «опубликовать репозиторий».
5. Выберите имя `BIG_DATA`, поставьте «public».
6. Жмёте Publish.

Всё. Никаких токенов, никакого терминала.

---

## 💬 Финальный совет

После публикации **обновите README** — добавьте абзац «о себе» и контакты, чтобы человек, попавший в репозиторий, понимал, что это **ваш** курс/портфолио, и мог связаться.

Удачи! 🚀
