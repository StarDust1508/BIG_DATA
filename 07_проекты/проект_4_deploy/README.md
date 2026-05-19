# 🚀 Проект 4 — Деплой ML-модели через FastAPI

> Завершающий шаг ML-проекта: модель учится → сохраняется → отдаётся через REST API.
> В реальной работе это **обязательный** этап после обучения. Без деплоя модель — просто файл.

---

## 🎯 Что вы получите

После прохождения:
- Поняли, как модель работает «в продакшене».
- Сделали REST API, который **реально работает**.
- Узнали про FastAPI + Pydantic + uvicorn.
- Увидели, как встраивать **юридические требования** (Art. 22 GDPR) в API.

---

## 📦 Что внутри

- `проект_4_deploy_fastapi.py` — один файл, два режима: `train` и `serve`.
- `requirements.txt` — добавьте: `fastapi`, `uvicorn`, `joblib`, `pydantic`.
- `README.md` — этот файл.

---

## 🛠 Установка

```
pip install fastapi uvicorn scikit-learn joblib pydantic
```

---

## ▶️ Запуск (3 шага)

### Шаг 1 — Обучить модель

```
python проект_4_deploy_fastapi.py train
```

Скрипт:
- Сгенерирует 10 000 синтетических заявок на кредит.
- Обучит RandomForest (n=100, depth=8).
- Сохранит модель в `credit_model.joblib`.
- Сохранит метаданные в `model_metadata.json` (версия, дата, метрики).

Покажет ROC-AUC на тесте (~0.83).

### Шаг 2 — Запустить API

```
python проект_4_deploy_fastapi.py serve
```

В терминале:

```
🚀 Запуск API на http://localhost:8000
📖 Swagger UI: http://localhost:8000/docs
```

### Шаг 3 — Проверить

Откройте в браузере **http://localhost:8000/docs** — это интерактивная Swagger-документация. Можно прямо в браузере тестировать.

Или через curl в другом терминале:

```
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"age": 35, "income": 80000, "debt_ratio": 0.3, "n_loans": 1}'
```

Ответ:

```json
{
  "probability_default": 0.087,
  "decision": "approve",
  "model_version": "1.0.0",
  "explanation": "Низкий риск: возраст, доход и долговая нагрузка в норме."
}
```

---

## 🧪 Тестовые случаи

### Низкий риск → approve

```
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"age": 45, "income": 150000, "debt_ratio": 0.2, "n_loans": 0}'
```

### Средний риск → manual_review

```
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"age": 30, "income": 50000, "debt_ratio": 0.55, "n_loans": 2}'
```

### Высокий риск → reject

```
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"age": 22, "income": 25000, "debt_ratio": 1.2, "n_loans": 4}'
```

В случае reject вы увидите **explanation** — это требование GDPR Art. 22 (право знать причину автоматизированного решения).

---

## 🧠 Что важно понять

### 1. Schema validation на входе

FastAPI + Pydantic **автоматически** валидируют JSON:
- Если возраст < 18 → 422 Unprocessable Entity.
- Если поле отсутствует → 422.
- Если тип неверный → 422.

Это спасает от багов в проде. В обычном Flask вам пришлось бы писать всё это руками.

### 2. Логирование каждого инференса

В скрипте есть:

```python
print(f"[{datetime.utcnow().isoformat()}] predict: features={...}, p={...}, decision={...}")
```

В реальном проде это идёт в **БД / Kafka** как audit log. Это требование для:
- AI Act (трейсабельность high-risk систем).
- GDPR Art. 22 (право на пересмотр).
- Внутренний compliance.

### 3. Explanation в ответе

Объяснение **на русском, для конечного пользователя**. Не «feature importance вектор», а человеческие слова: «причина: высокая долговая нагрузка».

Это и есть **Art. 22 GDPR в практике**.

### 4. Decision tiers

Не «predict 0/1», а **три уровня**:
- approve (вероятность дефолта < 20%)
- manual_review (20–50%)
- reject (> 50%)

Зона «manual_review» — место, где **человек** принимает решение. Это критично для AI Act: high-risk решения должны иметь human-in-the-loop.

---

## 🐳 Дополнительно: Docker контейнер

Это для тех, кто хочет «по-взрослому». Создайте `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY проект_4_deploy_fastapi.py .
COPY credit_model.joblib .
COPY model_metadata.json .

EXPOSE 8000
CMD ["python", "проект_4_deploy_fastapi.py", "serve"]
```

Билд и запуск:

```
docker build -t credit-api .
```

```
docker run -p 8000:8000 credit-api
```

Теперь API в контейнере — готов к деплою в Kubernetes / Cloud Run / etc.

---

## ⚖️ Юридический чек-лист перед production

- [ ] Audit log пишется в БД (не только в stdout).
- [ ] Версия модели в каждом ответе.
- [ ] Explanation на языке клиента.
- [ ] HTTPS обязательно (не HTTP в проде).
- [ ] Rate limiting (1000 req/час на клиента).
- [ ] Authentication (API key или OAuth).
- [ ] DPIA проведено и зафиксировано.
- [ ] Документация модели (Model Card) опубликована.
- [ ] Канал для оспаривания решений (Art. 22 GDPR).

---

## 🚀 Что дальше

В реальной работе после простого FastAPI:
- **Канареечный деплой** — 5% трафика на новую модель, 95% на старую.
- **Метрики** — Prometheus, latency p50/p95/p99, error rate.
- **Drift detection** — Evidently, NannyML, WhyLabs.
- **Feature Store** — Feast, чтобы фичи в проде и в обучении были одинаковые.
- **Model registry** — MLflow, чтобы знать, какая модель в проде.
- **A/B-тестирование** — две модели параллельно, сравнение бизнес-метрик.

См. [ресурсы/mlops_углублённо.md](../../ресурсы/mlops_углублённо.md).

---

## 🆘 Если что-то не работает

| Ошибка | Решение |
|--------|---------|
| `ModuleNotFoundError: No module named 'fastapi'` | `pip install fastapi uvicorn` |
| `Address already in use` (8000 занят) | `lsof -i :8000` найдёт процесс, `kill -9 PID` |
| `Connection refused` при curl | API ещё не стартовал, подождите 2 секунды |
| Модель не загружается | Сначала `python ... train`, потом `... serve` |
| `OSError: [Errno 48]` | Порт занят. Используйте другой: `uvicorn.run(..., port=8001)` |

---

После этого мини-проекта **вы умеете деплоить ML-модель**. Это то, что отличает «умеющего ML» от «использующего ML в проде».
