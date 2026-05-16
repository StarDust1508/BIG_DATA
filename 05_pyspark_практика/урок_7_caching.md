# Урок 5.7 — Caching и persist

> Тема короткая, но кэширование — это рычаг, который ускоряет ваш Spark **в разы** или **тормозит**, если применять не там, где надо.

---

## Часть 1. Зачем нужен cache

По умолчанию Spark **перевычисляет** DataFrame на каждый action.

```python
df = spark.read.parquet("big.parquet").filter("complex condition")

df.count()    # читает Parquet + фильтрует
df.count()    # СНОВА читает Parquet + СНОВА фильтрует
df.write.parquet("out/")    # И ТРЕТИЙ раз...
```

Если ваш DataFrame используется **больше одного раза** — кэшируйте.

```python
df = df.cache()
df.count()    # читает + кэширует
df.count()    # из кэша, мгновенно
df.write.parquet("out/")    # из кэша
```

---

## Часть 2. cache vs persist

```python
df.cache()                                   # = persist(MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY)         # только в RAM
df.persist(StorageLevel.MEMORY_AND_DISK)     # RAM + spill на диск
df.persist(StorageLevel.DISK_ONLY)           # только диск
df.persist(StorageLevel.MEMORY_ONLY_SER)      # сериализованно — меньше памяти
```

| Уровень | Размер | Скорость |
|---------|--------|----------|
| `MEMORY_ONLY` | оригинальный | быстро, но может не влезть |
| `MEMORY_AND_DISK` (default) | до RAM, потом диск | универсально |
| `MEMORY_ONLY_SER` | ~50% от оригинала | медленнее (десериализация) |
| `DISK_ONLY` | мало RAM | медленно, но надёжно |

**На 99% случаев — `cache()` (= `MEMORY_AND_DISK`).** Это разумный default.

---

## Часть 3. Cache ленив

```python
df = df.cache()    # ПОКА ничего не сделал
df.count()         # ВОТ ТЕПЕРЬ Spark начал кэшировать
```

Запомните: после `cache()` **обязательно** один «прогревочный» action, иначе кэш не наполнится.

---

## Часть 4. Когда кэшировать

✅ **Да:**
- DataFrame используется в **≥ 2 action'ах**.
- DataFrame — результат **дорогих** трансформаций (joins, агрегации).
- Несколько ветвлений из одного df.
- Помещается в память кластера.

❌ **Нет:**
- DataFrame используется один раз.
- Очень большой, не помещается без сильного spill на диск.
- В коротких пайплайнах, где cache не успеет окупиться.

---

## Часть 5. unpersist — освобождаем место

```python
df.unpersist()
```

Когда:
- Закончили работу с df, дальше нужна память.
- Память кластера переполняется.

⚠️ Если не вызвать `unpersist`, кэш живёт до конца сессии или **пока executor не вытеснит** его (LRU).

---

## Часть 6. Спарк UI: что закэшировано

В Spark UI вкладка **Storage** показывает все кэшированные DataFrame'ы:
- Размер в памяти и на диске.
- Уровень сериализации.
- На каких executor'ах хранится.

Полезно для диагностики «куда делась память».

---

## Часть 7. Альтернатива: checkpoint

Иногда вместо cache используется `checkpoint`:

```python
spark.sparkContext.setCheckpointDir("/tmp/checkpoint")

df = (df
    .filter(...)
    .join(other, "id")
    .checkpoint())     # eager: запишет на диск немедленно
```

**Отличия cache от checkpoint:**

| | cache | checkpoint |
|---|---|---|
| Хранение | RAM/диск executor'ов | Stable storage (HDFS/S3) |
| Lineage | Сохраняется | Усекается |
| После сессии | Теряется | Сохраняется |
| Применение | Несколько action'ов | Длинный lineage |

`checkpoint` полезен в длинных цепочках, чтобы Spark не «помнил» 50 шагов lineage (это дорого само по себе).

---

## Часть 8. Распространённые ошибки

### 8.1. Cache на устаревшем df
```python
df = df.cache()
df.count()
df = df.filter(...)     # это НОВЫЙ DataFrame, не кэшированный
df.count()              # снова читает Parquet
```

Правильно:
```python
df = df.filter(...).cache()
df.count()
df.count()              # из кэша
```

### 8.2. Кэширование «всего подряд»
```python
df1.cache(); df2.cache(); df3.cache(); df4.cache()
# Память executor'ов забита → execution не хватает места → spill на диск → медленно
```

### 8.3. Забыли unpersist
В долгом pipeline'е накапливаются кэши, ничего не освобождается → executor падает по памяти.

---

## Часть 9. Best practices в одном списке

1. Cache только то, что используется ≥ 2 раза.
2. После `cache()` — один «прогревочный» `count()` или `take(1)`.
3. `unpersist()` когда DataFrame больше не нужен.
4. Используйте `MEMORY_AND_DISK` (default cache).
5. Не кэшируйте промежуточные cheap-результаты.
6. Проверяйте Spark UI вкладку Storage.
7. В длинных pipeline'ах рассмотрите `checkpoint`.

---

## ✅ Самопроверка

1. Зачем нужен `cache()`?
2. Что произойдёт, если вызвать только `df.cache()` без action?
3. Чем `cache` отличается от `checkpoint`?
4. В каких случаях cache **не** помогает?
5. Какой default storage level в `cache()`?

---

## ▶️ Дальше

[Урок 5.8 — Псевдонимизация ПДн в Spark](./урок_8_псевдонимизация.md)
