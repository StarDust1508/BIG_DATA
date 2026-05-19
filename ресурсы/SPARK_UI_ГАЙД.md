# 🖥 Spark UI: что смотреть в реальной работе

> Spark UI — главный инструмент тюнинга. Этот гайд показывает, **что искать** и **что значат цифры**.
> Положите в `ресурсы/SPARK_UI_ГАЙД.md`.

---

## 1. Как открыть Spark UI

Spark UI работает **только пока активна SparkSession**.

В вашем Python-скрипте после `getOrCreate()` Spark пишет в логи:

```
SparkUI available at http://localhost:4040
```

Если порт 4040 занят — будет 4041, 4042... Смотрите в свои логи.

Откройте этот URL в браузере. Увидите интерфейс с 7 вкладками сверху:

```
+------------------------------------------+
| Jobs | Stages | Storage | Environment |  |
| Executors | SQL/DataFrame | Streaming   |
+------------------------------------------+
```

⚠️ Когда `spark.stop()` или скрипт закончился — UI закрывается. Сохраняйте скриншоты до завершения.

---

## 2. Вкладка JOBS — общая картина

Здесь список всех **jobs** — каждый action (`show`, `count`, `collect`, `write`) = один job.

```
Job Id  Description           Submitted    Duration  Stages  Tasks
0       count at <ipython>    20:00:01     2.1s      1/1     8/8
1       write at <ipython>    20:00:15     45s       4/4     200/200
2       show at <ipython>     20:01:05     0.3s      1/1     1/1
```

**На что смотреть:**

🔍 **Duration** — какой job самый медленный? С него начинать тюнинг.

🔍 **Stages X/Y** — все ли стадии успешны? Если `3/4` — одна не дошла.

🔍 **Tasks X/Y** — все ли таски прошли? Если `199/200` — одна упала, идите в Stages смотреть детали.

🔍 **Description** — что это за job (ищется по последнему вызванному action).

---

## 3. Вкладка STAGES — где конкретно тормозит

Каждый job разбит на stages. **Stage** = всё что между двумя shuffle.

Кликните на конкретный stage — увидите детали:

```
Stage Id  Description       Duration   Tasks (1/1/0/0)  Input    Shuffle Read  Shuffle Write
0         Scan parquet      2s         8 (8 succeeded)  500 MB   -             125 MB
1         Exchange + Sort   30s        200 (200 succ.)  -        125 MB        80 MB
2         HashAggregate     5s         200 (200 succ.)  -        80 MB         -
```

**Главные метрики:**

🔥 **Tasks: x/x/y/z** = succeeded/total/failed/skipped. Если есть `failed > 0` — катастрофа, читать логи.

🔥 **Duration vs Tasks** — если у вас 200 tasks и 200 секунд — ~1 секунда на task, ровно. Если 1 task 200 секунд + 199 по секунде — это **skew**.

🔥 **Shuffle Write** на одной стадии = **Shuffle Read** на следующей. Если эти числа большие → shuffle дорогой → подумать о broadcast/bucketing.

🔥 **Input** — сколько прочиталось с диска. Если 500 МБ, а ваш файл 50 ГБ — отлично, predicate pushdown работает.

---

## 4. Stage Detail — самое важное

Кликните на stage. Внизу таблица **Tasks**:

```
Index  Status   Duration  GC Time   Input  Shuffle Read  Errors
0      SUCCESS  1.2s      50ms      100MB  -             -
1      SUCCESS  1.5s      40ms      100MB  -             -
2      SUCCESS  1.1s      30ms      100MB  -             -
...
199    SUCCESS  120s      8s        4 GB   -             -      ⚠️ SKEW!
```

🚨 **Если одна таска в 100× дольше остальных** — это **skew**. У одного ключа гораздо больше данных.

🚨 **Если GC Time > 10% от Duration** — JVM Garbage Collector давится. Нужно больше памяти executor'у.

🚨 **Если Input у одной таски кардинально больше** — partition skew.

**Сверху Stage:** Summary Metrics (min/25%/median/75%/max). Если max в 10× больше median — skew.

```
Metric           Min   25%   Median  75%   Max
Task Duration    1.0s  1.1s  1.2s    1.4s  120s   ← skew!
Input            100M  100M  100M    100M  4G     ← skew!
```

---

## 5. Вкладка SQL / DataFrame — план запроса

Самая полезная вкладка для DataFrame-кода. Показывает **физический план** каждого запроса.

```
== Physical Plan ==
*(2) HashAggregate(keys=[city], functions=[sum(amount)])
+- Exchange hashpartitioning(city, 200)
   +- *(1) HashAggregate(keys=[city], functions=[partial_sum(amount)])
      +- *(1) Filter (isnotnull(amount) AND (amount > 1000))
         +- *(1) FileScan parquet [city,amount]
              Batched: true
              PushedFilters: [IsNotNull(amount), GreaterThan(amount,1000)]  ✅
              ReadSchema: struct<city:string,amount:double>  ✅
```

🟢 **Что хорошо видеть:**
- `PushedFilters` — фильтр ушёл в Parquet, не читаем лишнее.
- `ReadSchema` со всего 2 колонками — читаем только нужное.
- `*(1)`, `*(2)` — whole-stage codegen работает (быстро).
- `BroadcastHashJoin` — маленькая таблица не shuffle'ится.

🔴 **Что плохо видеть:**
- `Exchange` после каждой операции — много shuffle.
- `SortMergeJoin` на маленькой таблице — должен был быть Broadcast.
- `Filter` после `Project` без pushdown — фильтр не дошёл до источника.
- `BroadcastNestedLoopJoin` — почти всегда плохо, переписать.

---

## 6. Вкладка EXECUTORS — диагностика памяти

Здесь видно состояние каждого worker.

```
Executor ID  Cores  Memory      Active Tasks  GC Time  Input    Errors
driver       *      512MB used  -             -        -        -
0            4      6.5/8 GB    2             1.2s     500MB    0
1            4      7.8/8 GB    0             45s      500MB    3
2            4      2.1/8 GB    1             0.5s     500MB    0
```

🚨 **Memory `7.8/8 GB`** — почти забит → следующий task будет spill на диск или OOM.

🚨 **GC Time 45s из 60s времени работы** = JVM 75% времени собирает мусор → производительность ужасная.

🚨 **Errors > 0** — таски падали → есть retry, замедление.

🚨 **Один executor `2.1/8 GB`, остальные `7.8/8 GB`** — неравномерная нагрузка → skew или плохое партиционирование.

---

## 7. Вкладка STORAGE — что закэшировано

Если делаете `df.cache()` — увидите его здесь:

```
RDD Name        Storage Level     Cached Partitions  Size in Memory  Size on Disk
df_clients      MEMORY_AND_DISK   8/8                450 MB          0 B
df_transactions MEMORY_AND_DISK   200/200            2.1 GB          0 B
df_temp         MEMORY_AND_DISK   45/200             ⚠️             1.2 GB
```

🚨 **Cached Partitions `45/200`** — закэшировано только 45 из 200 партиций. Памяти не хватает → cache не работает как ожидалось.

🚨 **Size on Disk > 0** — данные **выгружены** на диск (spill). Cache в RAM не поместился. Часть запросов медленные.

**Лечение:**
- Больше памяти executor'у.
- Кэшируйте меньше DataFrames.
- Используйте `MEMORY_AND_DISK_SER` (сериализованно, занимает 2× меньше).
- Делайте `unpersist()` для тех df, что больше не нужны.

---

## 8. Вкладка ENVIRONMENT — конфигурация

Все параметры конфигурации Spark.

Что искать перед сложным запросом:

```
spark.sql.shuffle.partitions      = 200         ← сколько партиций после shuffle
spark.sql.adaptive.enabled        = true        ← AQE включён?
spark.sql.adaptive.skewJoin.enabled = true      ← AQE skew job?
spark.driver.memory               = 1g          ← мало для prod
spark.executor.memory             = 1g          ← мало
spark.default.parallelism         = 8           ← по числу ядер
```

Если вы тюните и непонятно почему медленно — проверьте, что AQE включен.

---

## 9. Вкладка STREAMING (если есть)

Видна только если есть streaming-query.

```
Active Queries
Name           Run ID    Input Rate     Process Rate   Batch Duration
my_streaming   abc123    10000 rows/s   8000 rows/s    1.5s
```

🚨 **Input Rate > Process Rate** — backpressure. Очередь будет расти, в итоге OOM.

🚨 **Batch Duration > Trigger Interval** — не успеваете обрабатывать. Уменьшайте интервал триггера или давайте больше ресурсов.

---

## 10. 5 паттернов «что не так»

### Паттерн A — Один task намного дольше остальных

В **Stages → Detail**: median 2с, max 200с. Это **skew** по ключу.

**Что делать:**
1. Включить AQE skewJoin.
2. Найти горячий ключ: `df.groupBy("key").count().orderBy(F.desc("count")).show(10)`.
3. Применить salt-trick или фильтрацию.

### Паттерн B — Все tasks по 30+ секунд при маленьких данных

Probably слишком мало in-memory партиций (или одна гигантская).

**Что делать:** `df.repartition(N)` где N ≈ 2-4 × ядер.

### Паттерн C — GC Time > 30% Duration

JVM захлёбывается мусором.

**Что делать:**
- Больше памяти executor'у.
- Меньше cache.
- `spark.serializer = org.apache.spark.serializer.KryoSerializer`.

### Паттерн D — Куча мелких файлов на выходе

В Storage / на диске: 5000 файлов по 1 МБ.

**Что делать:** `df.coalesce(100).write...` перед записью.

### Паттерн E — Запрос работает, но **медленнее** чем Pandas на тех же данных

Probably ваши данные **слишком маленькие** для Spark (< 1 ГБ). Spark overhead убивает.

**Что делать:** на маленьких — используйте Pandas или Polars. Spark — это для больших данных.

---

## 11. Поток диагностики «мой Spark медленный»

```
   Открыть Spark UI
        │
        ▼
   Jobs — какой самый долгий?
        │
        ▼
   Stages — какой stage в этом job самый долгий?
        │
        ▼
   Stage Detail — Summary Metrics
        │
   ┌────┴─────┐
   ▼          ▼
 Skew?     Большой Input?
   │          │
   ▼          ▼
 AQE +      Predicate
 salt       pushdown
            ушёл?
                │
                ▼
            Используйте
            Parquet
```

---

## 12. Spark UI «через раз» — нужен History Server

Spark UI живёт только при активной сессии. Чтобы видеть **прошлые запуски**:

1. Включите event logging в SparkSession:

```python
spark = SparkSession.builder \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .getOrCreate()
```

2. Запустите History Server:

```
$SPARK_HOME/sbin/start-history-server.sh
```

3. Откройте http://localhost:18080 — там история всех приложений.

---

## 13. Чек-лист «здоровый Spark job»

После запуска посмотрите в UI:

- [ ] Все таски `SUCCESS`, 0 `FAILED`.
- [ ] Median и Max Duration близки (нет skew).
- [ ] GC Time < 10% от Duration.
- [ ] Memory `5/8 GB` (есть запас, не 7.8/8).
- [ ] PushedFilters в SQL plan'е есть.
- [ ] Только колонки которые нужны в ReadSchema.
- [ ] BroadcastHashJoin (если одна сторона маленькая).
- [ ] Нет лишних Exchange после каждой операции.

Если все галочки — pipeline здоровый.

---

## 📸 Сделайте свои скриншоты

Я не могу приложить скриншоты в этот гайд (sandbox без браузера). Когда вы запустите Spark локально:

1. Откройте http://localhost:4040.
2. Сделайте скриншот каждой вкладки.
3. Сохраните в `ресурсы/screenshots/`.
4. Можно их вставить обратно в этот файл (в markdown через `![](screenshots/jobs.png)`).

Тогда будущий читатель сразу увидит, **что искать**.
