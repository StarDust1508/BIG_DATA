# 🧩 Практика 2 — WordCount на Python (Hadoop Streaming)

> Классический «hello world» MapReduce. Считаем, сколько раз каждое слово встретилось в тексте.

---

## Файлы

- `mapper.py` — превращает строку в пары `слово\t1`.
- `reducer.py` — суммирует пары с одинаковым ключом.
- `sample.txt` — учебный текст для запуска.

---

## Задание 1 — Локальный запуск (без Hadoop)

Это **главный учебный шаг**. Hadoop сам по себе не нужен, чтобы понять MR.

Из папки `практика_2_wordcount/`:

```bash
# Простейший вариант
cat sample.txt | python3 mapper.py | sort | python3 reducer.py
```

Ожидаемый вывод (отсортированно по словам):
```
a       3
and     4
apache  2
banking 1
basics  1
between 1
big     5
buzzword 1
by      1
change  1
```

Если у вас вышло то же — pipeline работает корректно.

---

## Задание 2 — Свой текст

Попробуйте на любом большом тексте на русском или английском:

```bash
# Например, скачать «Войну и мир» или взять README проекта
cat ../README.md | python3 mapper.py | sort | python3 reducer.py | head -20
```

Замечание: на русском **придётся подкрутить** очистку пунктуации (она оставляет только латиницу). Это первое задание на **доработку**: модифицируйте mapper.py, чтобы он работал и с кириллицей.

---

## Задание 3 — Запуск в Hadoop

При наличии живого кластера:

```bash
# 1. Положить текст и скрипты
hdfs dfs -put sample.txt /user/study/

# 2. Запустить
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files mapper.py,reducer.py \
    -input  /user/study/sample.txt \
    -output /user/study/wc_out \
    -mapper  "python3 mapper.py" \
    -combiner "python3 reducer.py" \
    -reducer  "python3 reducer.py"

# 3. Посмотреть результат
hdfs dfs -cat /user/study/wc_out/part-00000 | head
```

Заметили `-combiner`? Тот же reducer.py работает как combiner — суммируем локально на маппере, экономим shuffle.

---

## Задание 4 — Стресс-тест (если хочется)

Сгенерируйте большой файл и прогоните локально:

```bash
# Сгенерировать 100 МБ повторяющегося текста
python3 -c "import sys; print((open('sample.txt').read() + ' ') * 50000)" > big.txt

# Запустить
time cat big.txt | python3 mapper.py | sort | python3 reducer.py > /dev/null
```

Засекаете время. Это даст ощущение, **как медленно** MR работает локально без распределёнки.

---

## ✅ Что проверить

- [ ] Локальный запуск работает и даёт правдоподобный вывод.
- [ ] Понимаю, зачем перед reducer'ом стоит `sort`.
- [ ] Понимаю, что combiner = reducer для ассоциативных операций.
- [ ] Доработка под кириллицу не вызывает ужаса.
- [ ] (опционально) Запуск в реальном Hadoop через streaming.
