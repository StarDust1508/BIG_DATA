# 🚀 MLOps углублённо

> ML модель в Jupyter — это 10% работы. Остальные 90% — превратить её в продакт. Этот гайд — про оставшиеся 90%.

---

## 1. Что такое MLOps

**MLOps = DevOps + ML**. Дисциплина управления жизненным циклом ML-моделей:

```
Data → Training → Validation → Deployment → Monitoring → Retraining → ...
                                                    │
                                                    └─→ обратно к Data
```

Без MLOps:
- Модель «приносит результаты», но в проде ведёт себя иначе.
- Никто не знает, какая модель в проде сейчас.
- Метрики качества падают, никто не замечает.
- Переобучить — это «звоните Васе, он 3 месяца назад код писал».

С MLOps:
- Версионирование данных, кода, моделей.
- Воспроизводимость экспериментов.
- A/B-тесты в проде.
- Автоматическое retraining + canary deployment.
- Метрики качества + drift detection в дашборде.

---

## 2. Карта MLOps инструментов

```
┌────────────────────────────────────────────────┐
│ Experiment tracking                             │
│   MLflow / W&B / Neptune                        │
├────────────────────────────────────────────────┤
│ Model registry                                  │
│   MLflow Registry / Vertex AI / SageMaker       │
├────────────────────────────────────────────────┤
│ Feature store                                   │
│   Feast / Tecton / Databricks Feature Store    │
├────────────────────────────────────────────────┤
│ Pipeline orchestration                          │
│   Airflow / Kubeflow / Metaflow / SageMaker     │
├────────────────────────────────────────────────┤
│ Serving                                         │
│   TF Serving / TorchServe / Ray Serve / KServe │
├────────────────────────────────────────────────┤
│ Monitoring                                      │
│   Evidently / NannyML / Arize / WhyLabs         │
├────────────────────────────────────────────────┤
│ Data versioning                                 │
│   DVC / lakeFS / Pachyderm                      │
└────────────────────────────────────────────────┘
```

Для одного человека это много. Начинайте с **MLflow** — он покрывает 60% случаев.

---

## 3. MLflow — стартовая точка

```bash
pip install mlflow
mlflow server --backend-store-uri sqlite:///mlflow.db
```

UI: http://localhost:5000

### Experiment tracking

```python
import mlflow
import mlflow.spark

mlflow.set_experiment("credit_scoring")

with mlflow.start_run():
    mlflow.log_param("n_trees", 100)
    mlflow.log_param("max_depth", 8)

    # Обучение
    model = pipeline.fit(train)
    pred = model.transform(test)
    auc = evaluator.evaluate(pred)

    mlflow.log_metric("roc_auc", auc)
    mlflow.spark.log_model(model, "model")

    # Тэг (для фильтрации)
    mlflow.set_tag("dataset", "tx_2026_05")
```

После запуска: в UI видны все эксперименты, можно сравнивать.

### Model Registry

```python
# Регистрируем модель
model_uri = "runs:/<run_id>/model"
mv = mlflow.register_model(model_uri, "credit_scoring")

# Переводим в Staging / Production
client = mlflow.tracking.MlflowClient()
client.transition_model_version_stage(
    name="credit_scoring",
    version=mv.version,
    stage="Production",
)

# Загрузка production-модели
loaded = mlflow.spark.load_model("models:/credit_scoring/Production")
```

Это **версионированный реестр моделей**. В любой момент знаете, что в проде, как откатиться, и кто перевёл в Production.

---

## 4. Воспроизводимость — главный принцип

Любой эксперимент должен быть воспроизводим. Это значит зафиксированы:

1. **Код** — git commit hash.
2. **Данные** — версия / hash.
3. **Гиперпараметры** — в MLflow.
4. **Окружение** — Python version, библиотеки (`requirements.txt` или `pyproject.toml`).
5. **Случайные seeds** — `seed=42` везде.

```python
import os
import subprocess

run_metadata = {
    "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode(),
    "data_hash": hashlib.sha256(open("train.csv", "rb").read()).hexdigest(),
    "python_version": sys.version,
    "user": os.getenv("USER"),
}
mlflow.log_params(run_metadata)
```

Это позволяет сказать: «версия 1.7 модели обучена 15.04.2026, на коммите abc123, на данных с hash def456». Можно повторить, можно объяснить регулятору.

---

## 5. Feature Store

Проблема: «обучили модель на features, в проде они **другие**».

Причины:
- Training pipeline и inference pipeline пишут разные люди.
- Логика normalization отличается.
- В training есть будущие данные (data leakage).

Решение: **единое место для features** = Feature Store.

```python
# feast/feature_store.yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
    type: redis
    connection_string: localhost:6379

# Регистрируем feature view
@feature_view(
    entities=["user"],
    ttl=Duration(seconds=86400),
)
def user_features(df):
    return df

# Использование в training
features = fs.get_historical_features(
    entity_df=train_users,
    features=["user_features:age", "user_features:income"],
).to_df()

# В inference (online)
features = fs.get_online_features(
    features=["user_features:age", "user_features:income"],
    entity_rows=[{"user_id": 42}],
).to_dict()
```

Гарантия: training и inference берут features из одного места → нет drift из-за кода.

### Альтернативы Feast
- **Tecton** — managed.
- **Databricks Feature Store** — если уже на Databricks.
- **Своё на Redis + Postgres** — для маленьких команд.

---

## 6. Data versioning

Код версионируется Git'ом. А данные?

### DVC (Data Version Control)
```bash
pip install dvc
dvc init
dvc add data/train.csv
git add data/train.csv.dvc .gitignore
git commit -m "Add train v1"

# Через год
dvc checkout <старый коммит>   # вернёт ту версию данных
```

DVC хранит метаданные в Git, сами файлы — в S3/GCS/локально.

### lakeFS
Git-подобная семантика для S3. Бранчи, коммиты, merge.

### Простой подход
Если без инструментов:
```
data/
   train_2026-05-15_v1.parquet
   train_2026-05-22_v2.parquet
   train_2026-06-01_v3.parquet
```

И в Model Card записываете, на какой версии обучили.

---

## 7. Model serving

Как «отдавать предсказания» в проде:

### Batch serving
- Раз в день/час Spark считает scores на всю базу.
- Записывает в БД / S3.
- Приложение читает готовые scores.
- **Просто, надёжно, low-latency не нужен.**

### Online serving (real-time)
- REST API на одном сервере.
- Низкая латентность (10-100 мс).
- Auto-scaling, load balancer.
- **Сложнее, нужно следить за uptime.**

### Streaming inference
- Kafka events → Spark Streaming → ML model → результат в Kafka.
- Гибрид batch и online.

### Инструменты
- **TF Serving / TorchServe** — для своих фреймворков.
- **Ray Serve** — универсальный.
- **KServe** на Kubernetes — production-grade.
- **MLflow Serving** — из коробки `mlflow models serve`.
- **FastAPI + uvicorn** — самый простой, кастомный.

```python
# fastapi_serve.py
from fastapi import FastAPI
import mlflow.spark
import pandas as pd

app = FastAPI()
model = mlflow.spark.load_model("models:/credit_scoring/Production")

@app.post("/predict")
def predict(features: dict):
    df = pd.DataFrame([features])
    pred = model.transform(spark.createDataFrame(df))
    return {"prediction": pred.collect()[0]["prediction"]}
```

Запуск: `uvicorn fastapi_serve:app`. И сразу есть REST API.

---

## 8. Monitoring модели в production

Самое **забытое** в индустрии. Без этого ML не существует в проде.

### 8.1. Data drift

Входные данные **меняются**. Распределение возраста клиентов сегодня не такое, как полгода назад.

```python
from scipy.stats import ks_2samp

# Reference (training) и production выборки
reference = train_df["age"].values
current = production_df["age"].values

stat, p_value = ks_2samp(reference, current)
if p_value < 0.05:
    alert("Data drift detected for 'age'!")
```

Метрики:
- **KS test** — для непрерывных.
- **Chi-squared** — для категориальных.
- **PSI (Population Stability Index)** — банковская классика.

### 8.2. Concept drift

Связь X→Y меняется. Например, «после COVID кредитный риск иначе оценивается».

Обнаружить: следить за качеством на свежей разметке.

### 8.3. Performance метрики

- **Latency** — P50, P95, P99.
- **Throughput** — RPS.
- **Error rate** — 5xx.
- **Saturation** — CPU, memory.

### 8.4. Business метрики

Главное. ML модель должна приносить пользу:
- Конверсия.
- Revenue per user.
- Fraud catch rate.
- Customer churn.

### Инструменты мониторинга
- **Evidently AI** (open source) — drift + reports.
- **NannyML** (open source) — performance estimation без свежей разметки.
- **Arize / WhyLabs / Fiddler** — managed.
- **Grafana + Prometheus** — общий.

```python
from evidently.report import Report
from evidently.metrics import DataDriftPreset

report = Report(metrics=[DataDriftPreset()])
report.run(reference_data=ref_df, current_data=prod_df)
report.save_html("drift_report.html")
```

Откройте HTML — увидите визуальный отчёт по каждой колонке.

---

## 9. A/B тесты ML моделей

«Стала ли новая модель лучше старой» — нельзя ответить только на оффлайн метриках.

### Канарейка
1. 95% трафика идёт на старую (v1).
2. 5% — на новую (v2).
3. Сравниваем бизнес-метрики.
4. Постепенно увеличиваем v2 → 50% → 100%.

### Shadow mode
1. Старая модель решает (её результат идёт в продакт).
2. Новая модель параллельно делает свой прогноз (просто логируется).
3. Сравниваем оффлайн.

### Платформы
- **Optimizely** — фичефлаги + A/B.
- **GrowthBook** — open source.
- **Своё через LaunchDarkly** + Kafka logging.

---

## 10. Канареечное развёртывание

```
1. CI прогоняет тесты → собирает Docker image.
2. Деплой в staging (5% production trafic).
3. Метрики сравниваются automatically.
4. Если ок — раскатка на 50%.
5. Если ок — 100%.
6. Если плохо — rollback за 5 минут.
```

Инструменты: Argo Rollouts, Flagger, Spinnaker. В Databricks — Workflows + Model Registry stages.

---

## 11. Retraining — автоматическое переобучение

Triggers:
- **По расписанию** — раз в неделю / месяц.
- **По drift** — если drift > threshold.
- **По объёму** — накопилось N новых меток.
- **По метрикам** — качество упало.

```
[Airflow DAG: weekly_retrain]
   1. Тянет свежие данные (последние 90 дней).
   2. Trains новую модель.
   3. Тестирует на holdout.
   4. Если AUC_new > AUC_prod - 0.01 → регистрирует как Staging.
   5. Если canary 24ч прошла → промоушн в Production.
   6. Шлёт slack-уведомление.
```

---

## 12. CI/CD для ML

Pipeline на GitHub Actions:

```yaml
name: ml-cicd

on: [pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: '3.11'}
      - run: pip install -r requirements.txt
      - run: pytest tests/unit/

  train:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - run: python train.py --data data/train.csv --output model/
      - run: python evaluate.py model/ test.csv
      - run: |
          if [ "$(cat metrics.json | jq '.roc_auc')" -lt "0.75" ]; then
              echo "Model quality too low"
              exit 1
          fi
      - uses: actions/upload-artifact@v4
        with:
          name: model
          path: model/
```

Каждый PR автоматически:
1. Запускает тесты.
2. Обучает модель.
3. Проверяет качество.
4. Сохраняет артефакт.

---

## 13. Этика и AI Act в MLOps

В каждом MLOps процессе должны быть:

- **Model Card** + **Data Card** для каждой production модели.
- **Audit log** на каждый inference (вход, выход, версия модели).
- **Возможность объяснить** конкретное решение.
- **Human-in-the-loop** для high-stakes решений.
- **Возможность откатить** при обнаружении bias.

Это **не просто хорошо** — это **обязательно** по AI Act EU.

---

## 14. Чек-лист «production-ML»

- [ ] Все эксперименты в MLflow / W&B.
- [ ] Версии: код (git), данные (DVC/lakeFS), модель (registry).
- [ ] Воспроизводимый pipeline (Airflow / Kubeflow).
- [ ] CI/CD проверяет качество.
- [ ] Канареечное развёртывание.
- [ ] Drift monitoring.
- [ ] Audit log inference.
- [ ] Model Card + Data Card.
- [ ] Retraining trigger.
- [ ] Rollback процедура.

Если у вас всё это — вы senior MLOps engineer.

---

## 15. Что почитать

- **«Machine Learning Engineering»** — Andriy Burkov. Главная книга MLOps.
- **«Designing Machine Learning Systems»** — Chip Huyen.
- **«Effective MLOps»** — серия онлайн.
- **MLOps Community** — Slack: https://mlops.community/
- **Made With ML** — открытый курс: https://madewithml.com/
