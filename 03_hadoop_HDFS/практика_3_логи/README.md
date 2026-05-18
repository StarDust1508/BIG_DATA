# 🧩 Практика 3 — Аналитика логов через MR

> Сценарий: вам передали лог веб-сервера. Нужно посчитать топ-IP и распределение HTTP-кодов через MR-подход.
> Это типовая задача, которую исторически делали в Hadoop.

---

## Файлы

- `generate_log.py` — генерирует синтетический access.log.
- `mapper_ip.py` — выдаёт пары `(ip, 1)`.
- `mapper_status.py` — выдаёт пары `(status_code, 1)`.
- `reducer.py` — общий reducer-«сумматор», подходит для обоих случаев.
- `README.md` — этот файл.

---

## Подготовка

```bash
python3 generate_log.py
```

Создастся файл `access.log` на ~50 000 строк формата Apache Common Log:

```
192.168.1.42 - - [01/May/2026:10:13:47 +0000] "GET /api/users HTTP/1.1" 200 1532
10.0.0.7 - - [01/May/2026:10:13:48 +0000] "POST /login HTTP/1.1" 401 234
...
```

---

## Задание 1. Топ-10 IP по числу запросов

```bash
cat access.log | python3 mapper_ip.py | sort | python3 reducer.py | sort -k2 -n -r | head -10
```

Объяснение пайплайна:
1. `mapper_ip.py` — извлекает IP и выдаёт `ip\t1`.
2. `sort` — сортирует по ключу (для reducer'а).
3. `reducer.py` — суммирует.
4. `sort -k2 -n -r` — финальная сортировка по числу в обратном порядке.
5. `head -10` — топ.

---

## Задание 2. Распределение HTTP-кодов

```bash
cat access.log | python3 mapper_status.py | sort | python3 reducer.py
```

Вы должны увидеть что-то типа:
```
200  41523
301   3210
401   1532
404   2845
500    890
```

---

## Задание 3. Самостоятельно: средняя «длина ответа» (bytes) по статусу

Подсказки:
- В mapper'е выдавайте `status\tbytes` (вторая колонка — число байт).
- В reducer'е считайте не просто сумму, а среднее: накапливайте сумму и количество, потом делите.

Ответ запишите в [мои_заметки.md](../мои_заметки.md).

---

## Задание 4. (Опционально) Реальный Hadoop

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper_ip.py,reducer.py \
    -input  /user/study/access.log \
    -output /user/study/top_ips \
    -mapper  "python3 mapper_ip.py" \
    -reducer "python3 reducer.py"
```

---

## ✅ Чек-лист

- [ ] Локальный MR работает для топ-IP.
- [ ] Понятно, что pipeline `sort | reducer` имитирует shuffle.
- [ ] Задание 3 (среднее по статусу) сделано.
- [ ] Видна разница между «сырыми данными» и «компактным агрегатом» — это и есть цель MR.
