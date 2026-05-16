# 🎯 Проект 3 — ETL для регулярной отчётности

> Не ML, чистая инженерия данных. Самый «производственный» из трёх проектов.

---

## 📋 Бизнес-постановка

**Заказчик:** финансовый отдел и Compliance.

**Цель:** ежедневный pipeline формирует:
- Дневной свод транзакций по сегментам.
- Топ-100 контрагентов по обороту.
- KPI просрочки.
- Регуляторная отчётность для надзора (имитация: агрегаты по правилам ФинМон).

**SLA:** пайплайн должен закончиться к 8:00 каждый день.

---

## 🏗 Архитектура

```
[ежедневный cron / Airflow]
     │
     ▼
pipeline.py --run-date YYYY-MM-DD
     │
     ├─► Чтение сырых CSV из datasets/
     │    (имитация — в проде: extract из DWH через JDBC)
     │
     ├─► Валидация схемы (Schema validation)
     ├─► DQ-проверки (assertion'ы)
     ├─► Очистка
     ├─► Обогащение (join с справочниками)
     ├─► Агрегации (window, group)
     │
     ├─► Запись в Parquet с partitionBy(dt) — для аналитиков
     ├─► Запись в "отчёты/" — CSV для регулятора (имитация)
     ├─► Метрики в metrics.json
     └─► Audit log
```

Идемпотентность гарантирована через `dynamic partition overwrite`.

---

## 📂 Структура

```
проект_3_etl_отчётность/
├── README.md
├── pipeline.py
├── DATA_CARD.md
├── orchestration_example.md   # как запускать в Airflow
└── tests/
    └── test_transforms.py     # unit tests
```

---

## 🛠 Что нужно установить

Минимум как в модуле 05: PySpark, pandas. Всё в `requirements.txt`.

Для оркестрации (опционально):
```bash
pip install apache-airflow
```
Airflow тяжёлый — ставить только если планируете изучать.

Бесплатные альтернативы: Prefect, Dagster, обычный cron + bash.

---

## ▶️ Запуск

```bash
cd 07_проекты/проект_3_etl_отчётность
python3 pipeline.py --run-date 2026-05-15
```

Через `spark-submit`:
```bash
spark-submit --master local[*] --driver-memory 2g pipeline.py --run-date 2026-05-15
```

Результаты:
- `datasets/etl_reports/dt=2026-05-15/segments.parquet`
- `datasets/etl_reports/dt=2026-05-15/top100_counterparties.parquet`
- `datasets/etl_reports/dt=2026-05-15/kpi.json`
- `audit.log`

---

## ⚖️ Правовая часть

- [ ] Данные пришли с уже псевдонимизированным client_id (из проекта 1 или модуля 02).
- [ ] Regulatory отчётность отправляется в защищённое хранилище.
- [ ] Аудит каждого запуска — кто, когда, какие даты.
- [ ] Retention 3 года для отчётности по ФинМон / 152-ФЗ.

---

## 🎓 Чему учит проект

- Структура production ETL-пайплайна.
- Идемпотентность.
- Schema validation.
- DQ-проверки.
- Метрики мониторинга.
- Audit log.
- Параметризация по дате.
