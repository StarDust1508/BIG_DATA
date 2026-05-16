"""
🧩 Практика 4 модуля 06 — Классификация текстов через TF-IDF + LR

Сценарий: бинарная классификация коротких сообщений как «жалоба» или «нейтральное».
Используем синтетический корпус.

Запуск:
    python3 практика_4_text.py
"""
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    RegexTokenizer, StopWordsRemover, CountVectorizer, IDF,
)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# Маленький учебный корпус
DATA = [
    # Жалобы (label=1)
    ("Не приходит платёж уже неделю, исправьте срочно!", 1),
    ("Очень долго ждать ответа, отвратительное обслуживание", 1),
    ("Списали деньги без причины, требую возврат", 1),
    ("Сайт не работает, не могу зайти в личный кабинет!", 1),
    ("Самый ужасный банк, везде обман и комиссии", 1),
    ("Жалуюсь на сотрудника, был грубым и некомпетентным", 1),
    ("Карта не работает, отказали в обслуживании", 1),
    ("Без объяснений заблокировали счёт, верните доступ", 1),
    ("Звонил пять раз, никто не отвечает, полное равнодушие", 1),
    ("Деньги пропали со счёта, требую разобраться", 1),
    # Нейтральные (label=0)
    ("Спасибо за помощь, всё прошло хорошо", 0),
    ("Хочу узнать о тарифах вашей карты", 0),
    ("Подскажите, как получить выписку за месяц", 0),
    ("Обновили приложение, теперь удобнее", 0),
    ("Вопрос по условиям ипотеки, можно консультацию?", 0),
    ("Открыл вклад, всё прошло быстро", 0),
    ("Хочу подключить уведомления о тратах", 0),
    ("Какие документы нужны для оформления кредита?", 0),
    ("Перевёл деньги другу, всё прошло", 0),
    ("Хорошее приложение, нравится дизайн", 0),
]


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("M06_P4_Text")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.createDataFrame(DATA, ["text", "label"])
    train, test = df.randomSplit([0.7, 0.3], seed=11)
    print(f"train={train.count()}  test={test.count()}")

    stop_ru = StopWordsRemover.loadDefaultStopWords("russian")

    pipeline = Pipeline(stages=[
        RegexTokenizer(inputCol="text", outputCol="words_raw",
                        pattern=r"\W+", toLowercase=True, minTokenLength=2),
        StopWordsRemover(inputCol="words_raw", outputCol="words",
                          stopWords=stop_ru),
        CountVectorizer(inputCol="words", outputCol="raw_tf",
                         vocabSize=1000, minDF=1),
        IDF(inputCol="raw_tf", outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label",
                             maxIter=50, regParam=0.05),
    ])

    model = pipeline.fit(train)
    pred = model.transform(test)
    pred.select("text", "label", "prediction", "probability").show(10, truncate=False)

    ev = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    print(f"ROC-AUC: {ev.evaluate(pred):.4f}")

    # Объяснимость: топ-слов по модели
    cv_model = model.stages[2]   # CountVectorizer
    lr_model = model.stages[-1]
    vocab = cv_model.vocabulary
    coefs = lr_model.coefficients.toArray()

    pairs = sorted(zip(vocab, coefs), key=lambda x: x[1])
    print("\nТоп слов, толкающих к 'нейтральное':")
    for w, c in pairs[:5]:
        print(f"  {w:20s}  {c:+.3f}")
    print("\nТоп слов, толкающих к 'жалоба':")
    for w, c in pairs[-5:]:
        print(f"  {w:20s}  {c:+.3f}")

    spark.stop()


if __name__ == "__main__":
    main()
