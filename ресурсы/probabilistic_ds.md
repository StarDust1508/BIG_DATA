# 🎲 Probabilistic Data Structures

> Структуры, которые жертвуют 1% точности ради 1000× экономии памяти и времени. Реальная инженерия Big Data.

---

## 1. Зачем приближённые алгоритмы

Точный `COUNT(DISTINCT user_id)` на 100 ГБ требует **держать в памяти все уникальные user_id**. На 1 ТБ — уже не помещается. А ответ обычно нужен «примерно».

**Probabilistic data structures** дают:
- 99% точность.
- Постоянная или логарифмическая память.
- Возможность объединять результаты (mergeable).
- Параллельное обновление.

Используются везде: Spark, Cassandra, Redis, BigQuery, Snowflake, Druid.

---

## 2. Сравнительная таблица

| Структура | Что считает | Точность | Память |
|-----------|-------------|----------|--------|
| **HyperLogLog (HLL)** | Уникальные элементы (`COUNT DISTINCT`) | ~98% | ~1.5 КБ |
| **Bloom filter** | «Есть ли элемент в множестве?» | без false negative, ~1% false positive | ~10 МБ на 10М элементов |
| **Count-Min Sketch** | Частота элемента | ε погрешность | ~КБ на тысячи элементов |
| **t-digest** | Перцентили | ~99% | ~1 КБ |
| **MinHash** | Похожесть множеств | хорошая | пропорциональна точности |

---

## 3. HyperLogLog — счётчик уникальных

### Идея

Когда вы видите случайные 64-битные хеши, числе **уникальных** значений можно оценить по позиции **самого длинного хвоста нулей** в их хешах.

Это работает, потому что: длинные хвосты редки → длинный хвост = много значений.

### В Spark

```python
import pyspark.sql.functions as F

# Точно (медленно, много памяти)
df.agg(F.countDistinct("user_id")).show()

# Приближённо (HLL под капотом)
df.agg(F.approx_count_distinct("user_id", rsd=0.05)).show()
# rsd = relative standard deviation, 0.05 = 5% погрешность
```

На 1 миллиарде уникальных user_id:
- `countDistinct`: 30+ секунд, OOM на маленьком кластере.
- `approx_count_distinct`: 5 секунд, ~98% точности.

### Где ещё HLL

- **BigQuery**: функция `HLL_COUNT.MERGE`.
- **Redis**: команды `PFADD`, `PFCOUNT`.
- **Cassandra**: для распределённых счётчиков.
- **Druid**: для real-time агрегации.

---

## 4. Bloom filter — «возможно есть, точно нет»

### Идея

Очень компактная структура для проверки «принадлежит ли элемент множеству»:
- `contains(x)`:
  - **false** → точно нет.
  - **true** → возможно есть (есть шанс false positive).

### Алгоритм
1. Битовый массив длины M, изначально нули.
2. K хеш-функций, каждая мапит элемент в позицию `[0, M)`.
3. При вставке `add(x)`: ставим 1 в K позициях.
4. При проверке `contains(x)`: если хотя бы в одной позиции 0 → точно нет.

### Применения

- **Spark/Parquet**: на уровне колонки в Parquet можно хранить bloom filter, и при `WHERE col = X` Spark пропускает файл, не глядя.
- **Cassandra, RocksDB, LevelDB**: чтобы не искать данные на диске напрасно.
- **Веб-краулеры**: «эту страницу уже видел?».
- **CDN**: «этот URL уже в кэше?».

### Spark Parquet bloom filter

С Spark 3.3+:
```python
df.write.option("parquet.bloom.filter.enabled#col_name", "true") \
        .option("parquet.bloom.filter.expected.ndv#col_name", "1000000") \
        .parquet("out/")
```

Теперь запросы `WHERE col_name = 'x'` могут пропускать файлы.

### Минимальный пример Bloom filter на Python
```python
import mmh3   # pip install mmh3

class BloomFilter:
    def __init__(self, size: int = 10_000, k: int = 5):
        self.size = size
        self.k = k
        self.bits = bytearray(size // 8 + 1)

    def _hashes(self, value: str):
        for i in range(self.k):
            yield mmh3.hash(value, i) % self.size

    def add(self, value: str):
        for pos in self._hashes(value):
            self.bits[pos // 8] |= (1 << (pos % 8))

    def contains(self, value: str) -> bool:
        for pos in self._hashes(value):
            if not (self.bits[pos // 8] & (1 << (pos % 8))):
                return False
        return True

bf = BloomFilter(size=100_000, k=5)
bf.add("user_1")
bf.add("user_2")
print(bf.contains("user_1"))   # True (точно)
print(bf.contains("user_99"))  # False или True (если false positive)
```

---

## 5. Count-Min Sketch — частота элемента

### Идея

«Этот URL встречался сколько раз?» — точный ответ требует hash map, размер которого = число уникальных URL. CMS даёт приблизительный ответ с фиксированной памятью.

### Алгоритм
- Матрица счётчиков `d × w`.
- d хеш-функций. Каждая мапит элемент в столбец `[0, w)`.
- `add(x)`: увеличиваем счётчик во всех d строках в соответствующих столбцах.
- `count(x)`: берём минимум из d значений.

Гарантия: оценка ≥ реального значения; ошибка `<= ε × total_count` с вероятностью `>= 1 - δ`.

### В Spark

В Spark MLlib есть `CountMinSketch`:
```python
from pyspark.util import CountMinSketch
cms = df.rdd.map(lambda r: r.user_id).countMinSketch(0.001, 0.99, 42)
print(cms.estimateCount("user_42"))
```

Используется для:
- Top-K элементов в потоке.
- Анализа частот без полной таблицы.

---

## 6. t-digest — перцентили

Точные перцентили на 1 ТБ требуют sort, что дорого. t-digest даёт быструю оценку P50/P95/P99 с минимальной памятью.

В Spark:
```python
df.agg(F.expr("percentile_approx(amount, 0.99, 100)")).show()
# 100 — accuracy parameter, чем выше — точнее, но больше памяти
```

`approxQuantile` тоже использует t-digest:
```python
qs = df.approxQuantile("amount", [0.5, 0.95, 0.99], 0.001)
print(qs)   # [медиана, P95, P99]
```

Применения:
- Latency-метрики (P99 ответа API).
- Финансовые перцентили (P95 транзакций для outlier detection).
- Любая дашбордная аналитика.

---

## 7. MinHash + LSH — похожесть множеств

«Найти похожих пользователей» = найти множества с большим пересечением. Точное сравнение всех пар — O(N²).

**MinHash** + **Locality Sensitive Hashing (LSH)** даёт O(N) поиск похожих.

В Spark MLlib:
```python
from pyspark.ml.feature import MinHashLSH, MinHashLSHModel

mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
model = mh.fit(df_with_vectors)

# Найти похожие на конкретный
similar = model.approxNearestNeighbors(df_with_vectors, key_vector, numNearestNeighbors=10)
```

Применения:
- Дедупликация документов.
- Похожие пользователи / товары.
- Plagiarism detection.

---

## 8. Когда какую брать

```
Нужно «сколько уникальных»?           → HyperLogLog (approx_count_distinct)
Нужно «есть ли X в множестве»?         → Bloom filter
Нужно «как часто встречается X»?       → Count-Min Sketch
Нужно перцентили?                       → t-digest (percentile_approx)
Нужно похожие элементы (similarity)?    → MinHash + LSH
Нужно top-K?                            → SpaceSaving (вариант CMS) или heavy hitters
```

---

## 9. Слияние (merge) — суперсила

Все эти структуры **mergeable**:
- HLL двух партиций можно объединить → HLL целого датасета.
- Bloom filter двух партиций можно объединить.
- Count-Min Sketch — тоже.

Это значит: можно считать **параллельно на 1000 машинах**, потом объединить sketches.

```python
# Каждый день считаем HLL уникальных
daily_hll = []
for day in days:
    daily_hll.append(spark.read.parquet(f"data/dt={day}").agg(F.approx_count_distinct("user_id")))

# Можно слить (не точно показано — Spark API имеет свои примитивы)
```

---

## 10. Реальный кейс

Рекламная сеть считает «сколько уникальных пользователей видело наше объявление за месяц».

- Точно: hash set на 1 миллиард user_id → 30 ГБ памяти.
- Приближённо HLL: 1.5 КБ на одну компанию → 30 МБ на 20 000 компаний.
- Скорость: точно — часы. HLL — секунды.

Это работает в реальных рекламных сетях (Google, Facebook).

---

## 11. Чек-лист

- [ ] Использую `approx_count_distinct` вместо `countDistinct` для больших данных.
- [ ] Использую `percentile_approx` для дашбордов.
- [ ] Знаю про bloom filter в Parquet (опция `parquet.bloom.filter.enabled`).
- [ ] Понимаю trade-off «точность vs память».

---

## 12. Что почитать

- **«Probabilistic Data Structures and Algorithms for Big Data Applications»** — Andrii Gakhov.
- **Высоконагруженные приложения** (Кляйпман) — глава про probabilistic.
- **Twitter Engineering Blog** — много примеров.
