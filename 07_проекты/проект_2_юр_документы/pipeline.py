"""
Проект 2 — Классификация юридических документов
=================================================

Сквозной NLP pipeline: TF-IDF + Logistic Regression + объяснимость.

Синтетический корпус генерируется при первом запуске.

Запуск:
    python3 pipeline.py
"""
from __future__ import annotations

import json
import logging
import random
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    RegexTokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer,
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
DATA = ROOT / "datasets"
DATA.mkdir(exist_ok=True)

CORPUS_PATH = DATA / "legal_docs.csv"
PRED_PATH = DATA / "legal_docs_predictions.parquet"
MODEL_CARD = HERE / "MODEL_CARD.md"


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("legal-pipeline")


# ─── Корпус ─────────────────────────────────────────────────────────────────
TEMPLATES = {
    "иск": [
        "Истец просит взыскать с ответчика сумму задолженности в размере {n} рублей по договору поставки.",
        "Прошу суд признать недействительным договор аренды от {date} и взыскать убытки.",
        "Истец требует расторгнуть договор подряда в связи с существенным нарушением условий.",
        "Прошу взыскать неустойку за просрочку поставки товара по договору номер {n}.",
        "Истец просит суд обязать ответчика передать имущество и взыскать компенсацию.",
    ],
    "претензия": [
        "Настоящей претензией требуем оплатить задолженность по договору в течение 10 дней.",
        "В претензионном порядке требуем устранить недостатки выполненных работ.",
        "Просим в досудебном порядке вернуть переплату по договору в размере {n} руб.",
        "Направляем претензию в связи с нарушением сроков поставки.",
        "В порядке досудебного урегулирования предлагаем расторгнуть договор.",
    ],
    "запрос документов": [
        "Просим предоставить копии актов выполненных работ за период.",
        "В рамках проверки запрашиваем платёжные документы по контрагенту.",
        "Просим предоставить заверенные копии договоров и приложений.",
        "Запрос на предоставление выписки из ЕГРЮЛ и учредительных документов.",
        "Просим направить копии первичных документов по операциям.",
    ],
    "уведомление": [
        "Уведомляем о намерении расторгнуть договор по истечении срока.",
        "Доводим до сведения об изменении банковских реквизитов с {date}.",
        "Уведомляем о начале процедуры реорганизации.",
        "Извещаем о смене юридического адреса организации.",
        "Уведомление о приостановке оказания услуг по техническим причинам.",
    ],
    "иное": [
        "Поздравляем с праздником и желаем успехов в работе.",
        "Приглашаем на встречу для обсуждения дальнейшего сотрудничества.",
        "Информируем о новых предложениях и условиях обслуживания.",
        "Сообщение от службы поддержки клиентов.",
        "Благодарность за многолетнее сотрудничество.",
    ],
}


def generate_corpus_if_needed() -> None:
    if CORPUS_PATH.exists():
        return
    log.info(f"Генерирую корпус: {CORPUS_PATH}")
    random.seed(11)
    rows = ["text,category"]
    for cat, templates in TEMPLATES.items():
        for _ in range(150):     # 150 экземпляров каждой категории
            tmpl = random.choice(templates)
            text = tmpl.format(
                n=random.randint(10_000, 5_000_000),
                date=f"2026-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            )
            # Эскейпим запятые для CSV
            text = text.replace(",", "")
            rows.append(f"{text},{cat}")
    CORPUS_PATH.write_text("\n".join(rows), encoding="utf-8")
    log.info(f"  ✅ {len(rows)-1} документов")


# ─── Pipeline ───────────────────────────────────────────────────────────────
def main() -> None:
    generate_corpus_if_needed()

    spark = (
        SparkSession.builder
        .appName("Project2_LegalDocs")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.csv(str(CORPUS_PATH), header=True, inferSchema=True)
    log.info(f"Корпус: {df.count():,} документов")
    df.groupBy("category").count().orderBy("category").show()

    # ── Pipeline ────────────────────────────────────────────────────────
    label_idx = StringIndexer(inputCol="category", outputCol="label",
                                handleInvalid="keep")
    tok = RegexTokenizer(inputCol="text", outputCol="words_raw",
                          pattern=r"\W+", toLowercase=True, minTokenLength=3)
    stop_ru = StopWordsRemover.loadDefaultStopWords("russian")
    stop = StopWordsRemover(inputCol="words_raw", outputCol="words",
                              stopWords=stop_ru)
    cv = CountVectorizer(inputCol="words", outputCol="raw_tf",
                          vocabSize=2000, minDF=1)
    idf = IDF(inputCol="raw_tf", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label",
                              maxIter=100, regParam=0.05,
                              family="multinomial")

    pipeline = Pipeline(stages=[label_idx, tok, stop, cv, idf, lr])

    train, test = df.randomSplit([0.8, 0.2], seed=42)
    log.info(f"обучающая выборка={train.count()}  тестовая={test.count()}")

    model = pipeline.fit(train)
    pred = model.transform(test)

    # ── Метрики ─────────────────────────────────────────────────────────
    ev_f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    ev_acc = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    f1 = ev_f1.evaluate(pred)
    acc = ev_acc.evaluate(pred)
    log.info(f"F1={f1:.4f}  Точность={acc:.4f}")

    pred.select("text", "category", "label", "prediction").show(8, truncate=80)

    # ── Объяснимость: топ-слов на каждый класс ──────────────────────────
    cv_model = model.stages[3]
    lr_model = model.stages[-1]
    vocab = cv_model.vocabulary
    coef_matrix = lr_model.coefficientMatrix.toArray()
    label_indexer = model.stages[0]
    label_names = list(label_indexer.labels)

    explanations: dict[str, list[tuple[str, float]]] = {}
    for class_idx, class_name in enumerate(label_names):
        coefs = coef_matrix[class_idx]
        top = sorted(zip(vocab, coefs), key=lambda x: -x[1])[:8]
        explanations[class_name] = [(w, float(c)) for w, c in top]

    log.info("\nТоп слов для каждого класса:")
    for cls, words in explanations.items():
        log.info(f"  📁 {cls}:")
        for w, c in words[:5]:
            log.info(f"     {w:30s}  {c:+.3f}")

    # ── Запись ──────────────────────────────────────────────────────────
    pred.write.mode("overwrite").parquet(str(PRED_PATH))
    log.info(f"💾 предсказания: {PRED_PATH}")

    # ── Model Card ──────────────────────────────────────────────────────
    md_lines = [
        "# MODEL CARD — Legal Documents Classifier v1.0",
        "",
        "## 1. Назначение",
        "Мультиклассовая классификация входящих документов по 5 типам:",
        "иск, претензия, запрос документов, уведомление, иное.",
        "",
        "## 2. Архитектура",
        "- Алгоритм: Multinomial Logistic Regression",
        "- Pipeline: StringIndexer → RegexTokenizer → StopWordsRemover (RU)",
        "  → CountVectorizer → IDF → LR",
        "",
        "## 3. Данные",
        f"- Корпус: синтетический ({df.count()} документов)",
        "- В production — заменить на реальные размеченные документы.",
        "",
        "## 4. Метрики на test",
        f"- F1 (weighted): **{f1:.4f}**",
        f"- Accuracy: **{acc:.4f}**",
        "",
        "## 5. Объяснимость",
        "Топ слов по каждому классу (положительный коэффициент = свидетельство в пользу класса):",
        "",
    ]
    for cls, words in explanations.items():
        md_lines.append(f"### {cls}")
        for w, c in words[:5]:
            md_lines.append(f"- `{w}` ({c:+.3f})")
        md_lines.append("")

    md_lines.extend([
        "## 6. Ограничения",
        "- Корпус синтетический и небольшой.",
        "- Не учитывается морфология (лемматизация).",
        "- При max_probability < 0.6 → класс 'иное' → ручная маршрутизация.",
        "",
        "## 7. Юр.основа",
        "- Документы могут содержать ПДн (ФИО). Перед обучением — обезличить.",
        "- AI Act категория: ограниченный риск (классификация без автоматических решений).",
        "",
        f"## 8. Версия",
        f"- Тренирован: {datetime.utcnow().isoformat()}",
        f"- Owner: legal-tech@company.example",
    ])

    MODEL_CARD.write_text("\n".join(md_lines), encoding="utf-8")
    log.info(f"📄 MODEL_CARD.md: {MODEL_CARD}")

    spark.stop()
    log.info("✅ Pipeline завершён.")


if __name__ == "__main__":
    main()
