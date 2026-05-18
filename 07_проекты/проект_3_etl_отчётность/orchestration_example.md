# 🎼 Оркестрация ETL (пример)

Как этот pipeline запускать «по-взрослому».

---

## Вариант 1 — cron (просто)

```bash
0 6 * * * cd /path/БигДата/07_проекты/проект_3_etl_отчётность && \
    /path/.venv/bin/python pipeline.py --run-date $(date +\%Y-\%m-\%d) >> cron.log 2>&1
```

Запускает каждый день в 6:00. Подходит для маленьких pipeline'ов.

Минус: нет ретраев, нет мониторинга, нет зависимостей между задачами.

---

## Вариант 2 — Apache Airflow

```python
# dags/etl_reports.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

with DAG(
    "etl_reports_daily",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["etl", "reporting"],
) as dag:
    run_etl = BashOperator(
        task_id="run_pipeline",
        bash_command=(
            "cd /opt/bigdata/projects/etl && "
            "spark-submit --master local[*] --driver-memory 2g "
            "pipeline.py --run-date {{ ds }}"
        ),
    )
```

`{{ ds }}` — Airflow подставит логическую дату запуска.

Airflow даёт:
- ретраи
- SLA-алерты
- зависимости между задачами
- UI с историей запусков
- backfill за прошедшие даты

---

## Вариант 3 — Prefect (более лёгкий)

```python
from prefect import flow, task
from prefect.schedules import CronSchedule
import subprocess


@task
def run_etl(date: str):
    subprocess.run(["python", "pipeline.py", "--run-date", date], check=True)


@flow(name="etl_reports_daily",
      schedule=CronSchedule(cron="0 6 * * *"))
def etl_flow(date: str = None):
    run_etl(date)


if __name__ == "__main__":
    etl_flow.serve()
```

Prefect проще в установке (`pip install prefect`) и хорошо подходит для маленьких команд.

---

## Вариант 4 — Dagster (современный)

Похож на Prefect, но с акцентом на «assets» (что произвелось) вместо «tasks» (что запустилось):

```python
from dagster import asset, ScheduleDefinition, define_asset_job
import subprocess


@asset
def daily_etl():
    subprocess.run(["python", "pipeline.py",
                    "--run-date", "{date}"], check=True)


schedule = ScheduleDefinition(
    cron_schedule="0 6 * * *",
    job=define_asset_job("etl_job", selection="*"),
)
```

---

## Какой брать

| Размер команды | Рекомендация |
|----------------|--------------|
| 1 человек, < 5 пайплайнов | cron |
| 2–10 человек, регулярные ETL | Airflow или Prefect |
| Большая команда, фокус на data assets | Dagster |
| В Databricks | Databricks Workflows |

Для учебного проекта — **достаточно cron**.
