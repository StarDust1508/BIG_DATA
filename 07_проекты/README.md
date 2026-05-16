# 07. Сквозные проекты

> Финал курса. Три проекта, которые соединяют всё пройденное. После них у вас есть портфолио, которое можно показать.

---

## 🎯 Цели

Три законченных проекта, каждый соединяет:
- ETL в Spark.
- ML на распределённых данных или классическую инженерию.
- Правовую обработку (обезличивание, документация).
- Структуру repository-ready: код, README, метрики, Model/Data Card.

---

## 📦 Проекты

### Проект 1 — [Детекция аномалий в банковских транзакциях](./проект_1_аномалии/)
- **Технологии:** PySpark ETL + Spark MLlib (Random Forest).
- **Правовая часть:** псевдонимизация client_id через SHA-256 с солью, AI Act категория «высокий риск», human review.
- **Артефакты:** scored parquet, Model Card, audit log.
- **Один файл-точка входа:** `python3 pipeline.py --run-date 2026-05-15`.

### Проект 2 — [Классификация юридических документов](./проект_2_юр_документы/)
- **Технологии:** PySpark + TF-IDF + Logistic Regression (multinomial).
- **Особенность:** объяснимость на каждое решение — топ-слов по классам.
- **Юр.домен:** иск / претензия / запрос / уведомление / иное.
- **Один файл:** `python3 pipeline.py`.

### Проект 3 — [ETL для регулярной отчётности](./проект_3_etl_отчётность/)
- **Технологии:** чистая инженерия данных, без ML.
- **Особенность:** production-style: schema validation, DQ assertions, idempotent partitioning, audit log, metrics.json.
- **Бонус:** [пример оркестрации в Airflow / Prefect / Dagster / cron](./проект_3_etl_отчётность/orchestration_example.md).
- **Один файл:** `python3 pipeline.py --run-date YYYY-MM-DD`.

---

## 📋 Шаблоны для портфолио

В [ресурсы/шаблоны/](../ресурсы/шаблоны/):
- [MODEL_CARD_TEMPLATE.md](../ресурсы/шаблоны/MODEL_CARD_TEMPLATE.md)
- [DATA_CARD_TEMPLATE.md](../ресурсы/шаблоны/DATA_CARD_TEMPLATE.md)

Эти шаблоны — стандарт документации в индустрии (Google Model Cards). Используйте их для своих будущих проектов.

---

## ✅ Критерии «готового проекта»

1. **Понятный README**: цель, как запустить, что получится.
2. **Воспроизводимо:** `pip install -r requirements.txt && python pipeline.py`.
3. **Метрики записаны** (а не «у меня получилось хорошо»).
4. **ПДн обезличены**, если данные содержат их.
5. **Версии модели + данных** зафиксированы.
6. **Audit log** на критичных операциях.
7. **Структурированный код**: отдельные функции `extract`, `clean`, `train`, `load`.
8. **Один точка входа** — `pipeline.py`.

---

## 🛠 Что нужно установить (бесплатно)

Всё то же, что в предыдущих модулях: Python, Java, PySpark.

Опционально для серьёзности:
- **pytest** (`pip install pytest`) — для unit-тестов pipeline-функций.
- **pre-commit + ruff** (`pip install pre-commit ruff`) — автоматическая проверка кода.
- **Apache Airflow / Prefect / Dagster** — оркестрация (см. в проекте 3).
- **Docker** — упаковать проект в контейнер (см. ниже).

---

## 🐳 Бонус: упаковать проект в Docker

В корне проекта `Dockerfile`:
```dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ENTRYPOINT ["python", "pipeline.py"]
```

Билд и запуск:
```bash
docker build -t anomaly-pipeline .
docker run --rm -v "$(pwd)/datasets:/app/datasets" \
    anomaly-pipeline --run-date 2026-05-15
```

Это **профессиональный путь** доставки pipeline'ов в production.

---

## 🚀 Как выложить как портфолио

1. Создайте публичный репозиторий на GitHub.
2. Залейте курс целиком ИЛИ только модуль 07.
3. В README.md репозитория опишите:
   - какие проекты сделаны;
   - какие технологии освоены;
   - ссылку на этот курс.
4. Прикрепите ссылку в LinkedIn / hh.ru / резюме.

Это — реальное **доказательство** ваших навыков. Не «сертификат», а живой код.

---

## 🔗 Связи

- Опирается на: **ВСЕ** предыдущие модули.
- Правовая часть: [правовые_аспекты.md §8 Чеклист](../ресурсы/правовые_аспекты.md#8-чеклист).
- Шаблоны: [ресурсы/шаблоны/](../ресурсы/шаблоны/).
- Книги для дальнейшего развития: [книги_и_курсы.md](../ресурсы/книги_и_курсы.md).

---

## ⏱️ Время

- 1 неделя на проект 1.
- 4–5 дней на проект 2.
- 4–5 дней на проект 3.

Можно делать в любом порядке. Можно — параллельно.

---

После 07 курс закончен. У вас:
- Понимание Big Data как индустрии.
- Уверенные руки в PySpark.
- Опыт ML на распределёнке.
- Юридическая грамотность.
- Готовое портфолио.

🎉 Поздравляю!

---

## ➡️ Что дальше (за рамками курса)

- **Streaming**: Spark Structured Streaming, Apache Flink, Kafka.
- **Lakehouse-форматы**: Delta Lake, Iceberg, Hudi — глубже.
- **Оркестрация**: Airflow + dbt — стандарт индустрии.
- **Cloud**: Databricks, Snowflake, BigQuery — production-сервисы.
- **MLOps**: MLflow, Kubeflow, Vertex AI.
- **Сертификации**: Databricks Spark Developer, AWS Data Analytics.
- **Глубокий ML**: PyTorch для нейросетей.

См. [книги_и_курсы.md](../ресурсы/книги_и_курсы.md) для рекомендаций.
