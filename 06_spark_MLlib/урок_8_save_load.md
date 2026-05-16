# Урок 6.8 — Сохранение и загрузка моделей

> Обучили — надо использовать. В production обучение и инференс — это разные процессы. Модель «упаковывается» и переезжает.

---

## Часть 1. Сохранение Pipeline-модели

```python
model.write().overwrite().save("models/credit_scoring_v1")
```

Это создаст папку `credit_scoring_v1/` со всем необходимым:
- метаданные;
- параметры каждого этапа;
- веса модели;
- словари индексаторов и vectorizer'ов.

---

## Часть 2. Загрузка

```python
from pyspark.ml import PipelineModel

loaded = PipelineModel.load("models/credit_scoring_v1")
pred = loaded.transform(new_data)
```

Если сохраняли отдельный Estimator (не Pipeline) — используйте его класс:

```python
from pyspark.ml.classification import LogisticRegressionModel

lr_model = LogisticRegressionModel.load("models/lr_v1")
```

---

## Часть 3. Версионирование

В каждой модели должна быть **версия**. Минимум:

```
models/
   credit_scoring_v1/
   credit_scoring_v2/
   credit_scoring_v3/
   ...
```

Лучше — semver + дата:
```
models/
   credit_scoring/
      v1.0_2026-05-15/
      v1.1_2026-05-22/
      v2.0_2026-06-01/
   latest -> v2.0_2026-06-01
```

---

## Часть 4. Метаданные модели

Помимо самого артефакта, рядом с моделью храните:

```
credit_scoring_v1/
   model/             # сам PipelineModel
   metadata.json      # дата, автор, git commit, размер train
   metrics.json       # AUC, F1, PR-AUC, confusion matrix
   feature_schema.json
   training_data_hash # для воспроизводимости
   MODEL_CARD.md      # описание модели (см. урок 6.9)
```

Пример `metadata.json`:
```json
{
  "version": "1.0.0",
  "trained_at": "2026-05-15T10:00:00",
  "author": "ml-team@company.com",
  "git_commit": "a3f2c1d",
  "training_samples": 1_234_567,
  "features": ["age", "income", "tx_count", "city_idx"],
  "algorithm": "RandomForestClassifier(numTrees=200, maxDepth=10)",
  "framework": "pyspark.ml 3.5.0"
}
```

---

## Часть 5. Конвейер обновления модели

Production-сценарий:

```
[раз в неделю] →  train.py
                     ↓
                  обучает на свежих данных
                     ↓
                  сохраняет модель в S3
                     ↓
                  валидация на hold-out наборе
                     ↓
                  если AUC > предыдущей → канарейка → раскатка
                  иначе → откат
```

Реальные системы используют ML model registry (MLflow, Sagemaker Model Registry, Vertex AI).

---

## Часть 6. Инференс без Spark

Иногда хочется использовать Spark-модель **без Spark** в проде (например, в маленьком REST-сервисе).

Опции:
- **MLeap** — преобразует Spark Pipeline в lightweight-формат, можно загружать на Java/Scala без Spark.
- **ONNX** — экспорт в стандартный формат, использовать в любом языке.
- **Конвертировать в scikit-learn / XGBoost** — после обучения переписать модель.

Для большинства MLlib-моделей самый простой путь — `MLeap`. Но если нужно production без Spark — стоит сразу использовать scikit-learn / XGBoost.

---

## Часть 7. Подводные камни сериализации

1. **Spark-версия должна совпадать.** Модель, сохранённая в Spark 3.5, может не прочитаться Spark 3.0.
2. **UDF не сериализуются.** Если в Pipeline была кастомная UDF — она потеряется. Решение: либо не используйте UDF в Pipeline, либо используйте `pyspark.ml.Transformer`-подкласс.
3. **Внешние данные.** Если ваш Pipeline зависит от broadcast-переменной — она тоже должна быть в инференс-окружении.
4. **Локальные пути.** Сохраняйте на S3/HDFS, не в локальную папку — иначе модель «не уедет».

---

## Часть 8. Юридический угол

Модель — это **результат обработки ПДн** (если данные содержали ПДн). Это значит:

- Версии модели и данных должны быть воспроизводимыми.
- Для каждой production-модели нужен **MODEL_CARD** с описанием (см. урок 6.9).
- Аудит-лог должен фиксировать: кто обучил, на каких данных, какая метрика была одобрена для production.
- При смене модели — уведомлять субъектов, если изменилась логика автоматизированного решения (AI Act).

---

## ✅ Самопроверка

1. Как сохранить и загрузить PipelineModel?
2. Что должно лежать **рядом** с моделью кроме самой?
3. Зачем `metadata.json` и что в нём писать?
4. Какой инструмент позволяет использовать Spark-модель без Spark?
5. Что произойдёт, если в Pipeline была кастомная Python UDF, а потом загрузить модель в другом проекте?

---

## ▶️ Дальше

[Урок 6.9 — Объяснимость и документация для AI Act](./урок_9_объяснимость.md)
