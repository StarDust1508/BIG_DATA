"""
🚀 Мини-проект 4 — Деплой ML-модели через FastAPI

Это **завершающий шаг** ML-проекта: обучили модель → запустили REST API → принимает запросы.

Что делает скрипт:
  1. Обучает простую модель кредитного скоринга на синтетике.
  2. Сохраняет модель (через joblib).
  3. Запускает FastAPI сервер на http://localhost:8000.
  4. Принимает POST /predict с JSON фичами → отдаёт вероятность дефолта.

Требования:
    pip install fastapi uvicorn scikit-learn joblib

Запуск:
    # 1. Обучить и сохранить модель
    python проект_4_deploy_fastapi.py train

    # 2. Запустить API
    python проект_4_deploy_fastapi.py serve

    # 3. В другом терминале — проверить
    curl -X POST http://localhost:8000/predict \\
         -H "Content-Type: application/json" \\
         -d '{"age": 35, "income": 80000, "debt_ratio": 0.3, "n_loans": 1}'

Положите файл в 07_проекты/проект_4_deploy_fastapi/
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np

MODEL_PATH = Path("./credit_model.joblib")
METADATA_PATH = Path("./model_metadata.json")


# ─── Обучение ──────────────────────────────────────────────────────────

def train_and_save():
    """Обучает простую модель кредитного скоринга на синтетике."""
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split

    print("📊 Генерирую синтетический датасет...")
    rng = np.random.default_rng(42)
    n = 10_000

    age = rng.normal(40, 12, n).clip(21, 70)
    income = rng.lognormal(10.5, 0.6, n).clip(20_000, 1_000_000)
    debt_ratio = rng.beta(2, 5, n) * 1.5
    n_loans = rng.poisson(1.5, n)

    # Скрытая истина: дефолт зависит от факторов
    logit = (
        -3.0
        - 0.02 * (age - 40)
        - 0.000002 * income
        + 2.5 * debt_ratio
        + 0.3 * n_loans
        + rng.normal(0, 0.5, n)
    )
    p = 1 / (1 + np.exp(-logit))
    y = (rng.random(n) < p).astype(int)

    X = np.column_stack([age, income, debt_ratio, n_loans])
    feature_names = ["age", "income", "debt_ratio", "n_loans"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print(f"📐 Обучаю RandomForest на {len(X_train):,} строках...")
    model = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42)
    model.fit(X_train, y_train)

    pred_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, pred_proba)
    print(f"🏆 ROC-AUC на тесте: {auc:.4f}")

    # Сохранение
    joblib.dump(model, MODEL_PATH)
    metadata = {
        "model_version": "1.0.0",
        "trained_at": datetime.utcnow().isoformat(),
        "algorithm": "RandomForestClassifier(n=100, depth=8)",
        "feature_names": feature_names,
        "n_train": len(X_train),
        "n_test": len(X_test),
        "test_roc_auc": auc,
        "default_rate": float(y.mean()),
    }
    METADATA_PATH.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"💾 Модель: {MODEL_PATH.resolve()}")
    print(f"📋 Метаданные: {METADATA_PATH.resolve()}")


# ─── REST API ───────────────────────────────────────────────────────────

def serve():
    """Запускает FastAPI сервер."""
    try:
        from fastapi import FastAPI, HTTPException
        from pydantic import BaseModel, Field
        import uvicorn
    except ImportError:
        print("❌ Установите FastAPI: pip install fastapi uvicorn")
        sys.exit(1)

    if not MODEL_PATH.exists():
        print("❌ Модели нет. Сначала: python проект_4_deploy_fastapi.py train")
        sys.exit(1)

    model = joblib.load(MODEL_PATH)
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))

    app = FastAPI(
        title="Credit Scoring API",
        description="Демонстрационный API для скоринга кредитного риска",
        version=metadata["model_version"],
    )

    class CreditRequest(BaseModel):
        age: float = Field(..., ge=18, le=100, description="Возраст заявителя")
        income: float = Field(..., gt=0, description="Месячный доход в рублях")
        debt_ratio: float = Field(..., ge=0, le=2, description="Debt-to-income (0..2)")
        n_loans: int = Field(..., ge=0, le=20, description="Кол-во открытых кредитов")

        class Config:
            json_schema_extra = {
                "example": {
                    "age": 35,
                    "income": 80000,
                    "debt_ratio": 0.3,
                    "n_loans": 1,
                }
            }

    class CreditResponse(BaseModel):
        probability_default: float = Field(..., description="Вероятность дефолта (0..1)")
        decision: str = Field(..., description="approve / reject / manual_review")
        model_version: str
        explanation: str = Field(..., description="Объяснение для клиента (требование GDPR Art. 22)")

    @app.get("/")
    def root():
        return {"service": "credit-scoring", "model_version": metadata["model_version"]}

    @app.get("/health")
    def health():
        return {"status": "ok", "model_loaded": True}

    @app.get("/metadata")
    def get_metadata():
        return metadata

    @app.post("/predict", response_model=CreditResponse)
    def predict(req: CreditRequest):
        try:
            features = np.array([[req.age, req.income, req.debt_ratio, req.n_loans]])
            p_default = float(model.predict_proba(features)[0, 1])

            # Бизнес-логика порогов
            if p_default < 0.20:
                decision = "approve"
                explanation = "Низкий риск: возраст, доход и долговая нагрузка в норме."
            elif p_default < 0.50:
                decision = "manual_review"
                explanation = "Средний риск: требуется проверка кредитного специалиста."
            else:
                decision = "reject"
                explanation = (
                    "Высокий риск дефолта. Основные факторы: "
                    + ("высокая долговая нагрузка; " if req.debt_ratio > 0.5 else "")
                    + (f"молодой возраст ({int(req.age)}); " if req.age < 25 else "")
                    + (f"низкий доход ({int(req.income)}₽); " if req.income < 30000 else "")
                    + "вы имеете право на пересмотр решения с участием специалиста (GDPR Art. 22)."
                )

            # Audit log в реальном проде идёт в БД/Kafka
            print(f"[{datetime.utcnow().isoformat()}] predict: "
                  f"features={features.tolist()[0]}, p={p_default:.4f}, decision={decision}")

            return CreditResponse(
                probability_default=p_default,
                decision=decision,
                model_version=metadata["model_version"],
                explanation=explanation,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    print(f"🚀 Запуск API на http://localhost:8000")
    print(f"📖 Swagger UI: http://localhost:8000/docs")
    print(f"📋 Метаданные: http://localhost:8000/metadata")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


# ─── main ───────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python проект_4_deploy_fastapi.py train  # обучить и сохранить")
        print("  python проект_4_deploy_fastapi.py serve  # запустить API")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "train":
        train_and_save()
    elif cmd == "serve":
        serve()
    else:
        print(f"Неизвестная команда: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
