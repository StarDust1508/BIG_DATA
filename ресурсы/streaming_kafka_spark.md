# 🌊 Streaming: Kafka + Spark Structured Streaming

> Мини-курс по потоковой обработке. После него вы понимаете концепции watermarks, exactly-once, late events, и можете построить простой streaming pipeline.

---

## 1. Зачем streaming

Batch ETL отвечает на вопрос «что было». Streaming — **«что происходит прямо сейчас»**.

| Сценарий | Подход |
|----------|--------|
| Отчёты раз в день | Batch |
| KPI обновляются раз в час | Mini-batch |
| Фрод-детект в момент транзакции | Streaming |
| Live-аналитика на дашборде | Streaming |
| Real-time recommendation | Streaming |

---

## 2. Архитектура типичной streaming-системы

```
[приложения]
   │
   ├──► Kafka (producer'ы шлют события)
   │
   ▼
┌─────────────────────┐
│ Kafka cluster        │
│ topic: events         │
│   partition 0         │
│   partition 1         │
│   partition 2         │
└─────────┬─────────────┘
          │ consumer'ы читают
          ▼
┌───────────────────────────────┐
│ Spark Structured Streaming     │
│ или Flink                       │
│   - parse, clean                │
│   - aggregate                   │
│   - join with static            │
│   - alert / write               │
└───────────┬─────────────────────┘
            │
   ┌────────┴────────┐
   ▼                  ▼
S3/HDFS         ML-inference / Alerts
(audit, history)   (real-time decisions)
```

---

## 3. Kafka в одном абзаце

**Kafka** — распределённый, отказоустойчивый брокер сообщений.

Базовые понятия:
- **Topic** — именованный поток сообщений.
- **Partition** — параллельные «потоки» в топике.
- **Producer** — пишет в топик.
- **Consumer** — читает из топика.
- **Consumer group** — несколько consumer'ов, делящих партиции между собой.
- **Offset** — позиция consumer'а в партиции.
- **Retention** — как долго хранится сообщение (часы/дни/вечно).

Гарантии:
- Внутри одной партиции — порядок сохраняется.
- Между партициями — нет.
- Сообщение **не удаляется** после прочтения (как в очередях RabbitMQ) — остаётся до retention.

---

## 4. Kafka в Docker за 2 минуты

`docker-compose.yml`:
```yaml
version: "3"
services:
  kafka:
    image: bitnami/kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: 0
      KAFKA_CFG_PROCESS_ROLES: controller,broker
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 0@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      ALLOW_PLAINTEXT_LISTENER: "yes"
```

```bash
docker compose up -d

docker exec -it kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic transactions --partitions 3
```

---

## 5. Kafka Producer на Python

```bash
pip install kafka-python
```

```python
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    event = {
        "client_id": random.randint(1, 1000),
        "amount": round(random.expovariate(1/5000), 2),
        "ts": time.time(),
        "category": random.choice(["payment", "transfer", "withdrawal"]),
    }
    producer.send("transactions", event)
    time.sleep(0.1)
```

Этот скрипт шлёт ~10 событий в секунду в Kafka.

---

## 6. Spark Structured Streaming — основы

С Spark 2.0+ API streaming = почти такой же, как batch. Один DataFrame, только бесконечный.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("StreamingDemo") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Чтение потока из Kafka
df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load())

# Парсинг JSON
schema = StructType() \
    .add("client_id", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("ts", DoubleType()) \
    .add("category", StringType())

df = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("d")
).select("d.*")

# Простая обработка
agg = df.groupBy("category").count()

# Запись (на консоль для отладки)
query = (agg.writeStream
    .outputMode("complete")
    .format("console")
    .start())

query.awaitTermination()
```

---

## 7. Output modes

| Mode | Что значит | Когда |
|------|-----------|-------|
| **append** | Только новые строки | Если агрегаций нет (фильтры, map) |
| **complete** | Полный результат на каждый batch | Глобальные агрегации |
| **update** | Только изменённые | Агрегации с водяным знаком |

---

## 8. Windowing — оконные агрегации по времени

```python
from pyspark.sql.functions import window

# Сколько транзакций в каждой 5-минутке
windowed = (df
    .withColumn("ts_dt", col("ts").cast("timestamp"))
    .groupBy(window("ts_dt", "5 minutes"))
    .count())
```

Типы окон:
- **Tumbling** (без overlap): `window(ts, "5 minutes")` — 0:00-0:05, 0:05-0:10, ...
- **Sliding** (с overlap): `window(ts, "5 minutes", "1 minute")` — 0:00-0:05, 0:01-0:06, ...
- **Session** (динамические): `session_window(ts, "30 seconds")` — новое окно после паузы.

---

## 9. Watermarks — обработка опоздавших событий

Реальные данные приходят **не в порядке**. Событие из 10:00 может прилететь в 10:05 из-за сети.

**Watermark** говорит Spark: «я гарантирую, что после X я не буду ждать события старше X - threshold».

```python
windowed = (df
    .withColumn("ts_dt", col("ts").cast("timestamp"))
    .withWatermark("ts_dt", "10 minutes")    # ждём опоздания до 10 минут
    .groupBy(window("ts_dt", "5 minutes"))
    .count())
```

Что это даёт:
- Spark **закрывает окно** через 10 минут после его конца.
- Опоздавшие позже — отбрасываются.
- Память освобождается (state для старых окон удаляется).

Без watermark Spark копил бы state бесконечно → OOM.

---

## 10. Exactly-once semantics

Один из самых важных терминов в streaming.

| Semantic | Что это | Цена |
|----------|---------|------|
| **At-most-once** | Сообщение может потеряться, но не повторится | Дёшево, опасно |
| **At-least-once** | Не потеряется, может повториться | Стандарт, нужна идемпотентность |
| **Exactly-once** | Ровно один раз | Сложно, дорого |

Spark Structured Streaming даёт **exactly-once**, если:
- Source поддерживает offset'ы (Kafka — да).
- Sink идемпотентен (Parquet — да; внешний REST API — нет).
- Checkpoint включён.

```python
query = (df.writeStream
    .format("parquet")
    .option("path", "/data/output")
    .option("checkpointLocation", "/data/checkpoint")   # обязательно!
    .start())
```

Checkpoint хранит offset'ы Kafka и state. При перезапуске Spark продолжит с правильного места.

---

## 11. Join со static-таблицей

Поток + статическая таблица (например, справочник клиентов):
```python
clients_static = spark.read.parquet("/data/clients/")

enriched = df.join(clients_static, "client_id")
```

Это **streaming join** — каждое новое событие сразу обогащается из static.

---

## 12. Stream-stream join

Два потока джойнятся друг с другом:
```python
clicks = spark.readStream.format("kafka").option(...).load()
impressions = spark.readStream.format("kafka").option(...).load()

clicks_w = clicks.withWatermark("ts", "1 hour")
imps_w = impressions.withWatermark("ts", "1 hour")

joined = clicks_w.join(
    imps_w,
    expr("""
        clicks.ad_id = impressions.ad_id AND
        clicks.ts BETWEEN impressions.ts AND impressions.ts + INTERVAL 5 MINUTES
    """),
    "inner",
)
```

Используется для:
- Attribution (показ → клик → покупка).
- Сессионизации (клик-стрим).

⚠️ Stream-stream join требует watermark на обеих сторонах.

---

## 13. Late events

Что Spark делает с опоздавшими (после watermark):
- **append** mode + watermark — отбрасывает.
- **update** mode — обновляет соответствующее окно (если оно ещё не закрыто).

Хотите специально ловить late events? Используйте `lateDataDF` или дублирующий sink без watermark.

---

## 14. Checkpoint и восстановление

```python
query = (df.writeStream
    .format("parquet")
    .option("path", "out/")
    .option("checkpointLocation", "ckpt/")     # ← важно
    .trigger(processingTime="1 minute")         # каждую минуту
    .start())
```

Если приложение упало — при перезапуске оно:
1. Читает checkpoint.
2. Знает, на каком offset Kafka остановилось.
3. Продолжает с того же места.
4. Восстанавливает state агрегаций.

**Чекпойнты — обязательны** для prod.

---

## 15. Триггеры (когда обрабатывать)

```python
# По умолчанию: как можно быстрее
.trigger(processingTime="1 minute")            # каждую минуту
.trigger(once=True)                             # один раз — для batch-like
.trigger(continuous="1 second")                  # continuous mode (experimental)
.trigger(availableNow=True)                     # обработать всё накопленное и стоп
```

`processingTime` — типичный prod-выбор.

---

## 16. Когда НЕ брать Spark Streaming

**Альтернативы:**

- **Apache Flink** — настоящий streaming, lower latency (мс vs секунды).
- **Kafka Streams** — JVM-библиотека, если уже на Java.
- **AWS Kinesis Data Analytics** — managed Flink.
- **Kafka KSQL / ksqlDB** — SQL поверх Kafka.

Spark Streaming хорош, когда:
- У вас уже Spark-кластер.
- Latency «секунды» допустима.
- Логика похожа на batch (можно использовать тот же код).

---

## 17. Минимальный production-стриминг

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum as _sum
from pyspark.sql.types import (
    StructType, IntegerType, DoubleType, StringType, TimestampType,
)


def main():
    spark = (SparkSession.builder
        .appName("StreamingTxAggregator")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/ckpt")
        .getOrCreate())

    schema = (StructType()
        .add("client_id", IntegerType())
        .add("amount", DoubleType())
        .add("ts", TimestampType())
        .add("category", StringType()))

    raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "latest")
        .load())

    parsed = raw.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

    agg = (parsed
        .withWatermark("ts", "10 minutes")
        .groupBy(window("ts", "5 minutes"), "category")
        .agg(count("*").alias("n"), _sum("amount").alias("total")))

    query = (agg.writeStream
        .outputMode("update")
        .format("parquet")
        .option("path", "/data/streaming_agg/")
        .option("checkpointLocation", "/data/streaming_agg/_ckpt/")
        .trigger(processingTime="1 minute")
        .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
```

Это **полный production pipeline** на 50 строк.

---

## 18. Чек-лист «знаю streaming»

- [ ] Понимаю разницу batch/mini-batch/true streaming.
- [ ] Знаю Kafka: topic, partition, consumer group, offset.
- [ ] Умею писать Spark Structured Streaming.
- [ ] Различаю output modes (append/complete/update).
- [ ] Понимаю watermark и зачем он.
- [ ] Знаю про exactly-once и его условия (checkpoint, идемпотентность).
- [ ] Различаю tumbling / sliding / session windows.
- [ ] Понимаю, когда Spark Streaming, а когда Flink/Kafka Streams.

После этого — можно браться за streaming-задачи на работе.

---

## 19. Что почитать дальше

- **«Streaming Systems»** — Тайлер Акидау (Google). Главная книга по теме.
- **Kafka: The Definitive Guide** — Гарретт Шапиро.
- **Spark Streaming Programming Guide** — официальный.
- **Confluent Blog** — лучший контент про Kafka.
