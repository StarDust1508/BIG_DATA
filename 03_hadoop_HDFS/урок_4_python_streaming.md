# Урок 3.4 — MR на Python через Hadoop Streaming

> Hadoop изначально Java. Но мы — Python-разработчики. Spasибо Hadoop Streaming — можно писать mapper и reducer на любом языке, общающемся через stdin/stdout.

---

## Часть 1. Идея Hadoop Streaming

```
┌──────────────┐    stdin     ┌─────────────────┐
│   Hadoop     │ ───────────→ │  mapper.py      │
│   рантайм    │              │  (любой язык)   │
│              │ ←─────────── │                 │
└──────────────┘    stdout    └─────────────────┘
```

Фреймворк подаёт строки в mapper по stdin, читает результат по stdout, делает shuffle, передаёт в reducer таким же образом.

Это значит, что **mapper.py — это просто скрипт**, читающий stdin построчно. Можно запустить его без Hadoop вообще, через pipe:

```bash
cat input.txt | python mapper.py | sort | python reducer.py
```

Это **бесценный учебный трюк**: вы можете отрабатывать MR-логику без поднятия кластера.

---

## Часть 2. WordCount пошагово

### mapper.py
```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    for word in line.strip().lower().split():
        # Чистим пунктуацию
        word = "".join(c for c in word if c.isalpha())
        if word:
            print(f"{word}\t1")     # KEY \t VALUE
```

### reducer.py
```python
#!/usr/bin/env python3
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    word, count = line.strip().split("\t", 1)
    count = int(count)
    if current_word == word:
        current_count += count
    else:
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

if current_word is not None:
    print(f"{current_word}\t{current_count}")
```

⚠️ Обратите внимание: reducer.py **опирается на то, что одинаковые ключи идут подряд**. Hadoop это гарантирует (он сортирует перед reducer'ом). Локально мы добиваемся этого через `sort`.

### Локальный запуск
```bash
echo "hello world hello spark spark hadoop spark" \
  | python3 mapper.py \
  | sort \
  | python3 reducer.py
```

Вывод:
```
hadoop  1
hello   2
spark   3
world   1
```

### Запуск на настоящем Hadoop

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -input  /user/me/text.txt \
    -output /user/me/wordcount_out \
    -mapper  "python3 mapper.py" \
    -reducer "python3 reducer.py"

# Посмотреть результат
hdfs dfs -cat /user/me/wordcount_out/part-00000 | head
```

---

## Часть 3. Реальная задача: топ-N клиентов по сумме транзакций

Допустим, у нас в HDFS лежит CSV `transactions.csv` с миллиардами строк.

Цель: найти топ-100 клиентов по суммарному `amount` за период.

### mapper.py
```python
import sys, csv

reader = csv.reader(sys.stdin)
next(reader, None)   # header
for row in reader:
    try:
        _, client_id, _, _, amount, _ = row
        print(f"{client_id}\t{amount}")
    except (ValueError, IndexError):
        continue   # битая строка — пропускаем
```

### reducer.py
```python
import sys

current = None
total = 0.0
results = []   # топ собираем тут

for line in sys.stdin:
    key, value = line.strip().split("\t")
    if current == key:
        total += float(value)
    else:
        if current is not None:
            results.append((current, total))
        current = key
        total = float(value)
if current is not None:
    results.append((current, total))

# Топ-100 по total
results.sort(key=lambda x: x[1], reverse=True)
for client_id, total in results[:100]:
    print(f"{client_id}\t{total:.2f}")
```

⚠️ Этот reducer работает в одном экземпляре — он хранит весь топ в памяти. На очень больших кластерах с многими редьюсерами это **не сработает** — нужен второй MR-step или Spark.

---

## Часть 4. Combiner на Python

Если хотим уменьшить сеть, можно сделать combiner — тот же reducer, локально на маппере:

```bash
hadoop jar ... \
    -mapper   "python3 mapper.py"  \
    -combiner "python3 reducer.py" \
    -reducer  "python3 reducer.py" \
    ...
```

Combiner и reducer могут быть **одинаковым кодом**, если операция (сумма) ассоциативна.

---

## Часть 5. Отладочные приёмы

1. **Локальный pipe** — главный друг.
   ```bash
   head -1000 big.csv | python3 mapper.py | sort | python3 reducer.py
   ```

2. **Логирование в stderr** — Hadoop читает stdout, а stderr выводит в логи задачи.
   ```python
   import sys
   print("debug:", x, file=sys.stderr)
   ```

3. **Counter'ы** через специальный синтаксис:
   ```python
   sys.stderr.write("reporter:counter:MyGroup,bad_rows,1\n")
   ```
   Появится в счётчиках Hadoop UI.

---

## Часть 6. Альтернативы

Голый Hadoop Streaming — это «низкий уровень». В Python есть обёртки:

- **mrjob** (Yelp) — Python-обёртка для MR. Работает в локальном, EMR, Hadoop режимах.
- **dumbo** — старая, не рекомендую.
- **Pig / Hive** — высокоуровневые языки, SQL-подобные. Для МR не пишем — пишем запрос.

Но: для **новых проектов** ни одно из этого не оптимально. Берите PySpark.

---

## Часть 7. Что MR-парадигма даст в Spark

Когда дойдёте до Spark, увидите знакомые слова:

| MR | Spark |
|----|-------|
| Map | `df.select` / `df.withColumn` |
| Reduce | `df.groupBy(...).agg(...)` |
| Combiner | автоматически в `reduceByKey` / Catalyst |
| Shuffle | то же самое, но оптимизирован |
| Partitioner | `repartition`, `bucketBy` |

Spark в каком-то смысле — это «MR в памяти + умный оптимизатор». Но фундаментально та же модель.

---

## ✅ Самопроверка

1. Что такое Hadoop Streaming?
2. Почему reducer полагается на сортировку перед ним?
3. Зачем нужен combiner? Когда его НЕЛЬЗЯ использовать?
4. Как локально отрепетировать MR без кластера?
5. Что произойдёт, если в reducer'е накапливать всё в память при больших данных?

---

## ▶️ Дальше

[Урок 3.5 — Когда Hadoop уже не нужен](./урок_5_hadoop_сегодня.md)
