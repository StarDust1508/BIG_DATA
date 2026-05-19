# 🆘 Troubleshooting — 20 типичных ошибок PySpark

> Открывайте этот файл, когда PySpark отказывается работать.
> Каждая ошибка → симптом → диагноз → готовое решение.
> Положите в `ресурсы/TROUBLESHOOTING.md` в курсе.

---

## 🟥 ОШИБКА 1: `JAVA_HOME is not set`

**Симптом:**
```
JAVA_HOME is not set
```

**Диагноз:** Spark не нашёл Java. Java может быть установлена, но переменная не прописана.

**Решение (macOS):**

```
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc
```

```
source ~/.zshrc
```

```
echo $JAVA_HOME
```

Должно показать путь типа `/opt/homebrew/opt/openjdk@17/...`. Если пусто — Java не установлена, поставьте: `brew install openjdk@17`.

---

## 🟥 ОШИБКА 2: `command not found: java`

**Симптом:**
```
zsh: command not found: java
```

**Диагноз:** Java вообще нет в PATH.

**Решение:**

```
brew install openjdk@17
```

После установки:

```
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

(Mac спросит пароль — печатайте вслепую.)

```
java -version
```

Должно показать `openjdk version "17.0.x"`.

---

## 🟥 ОШИБКА 3: `ModuleNotFoundError: No module named 'pyspark'`

**Симптом:**
```
ModuleNotFoundError: No module named 'pyspark'
```

**Диагноз:** Либо `pyspark` не установлен в текущий Python, либо venv не активирован.

**Решение:**

Проверьте, что venv активен — в начале строки должно быть `(.venv)`:

```
source .venv/bin/activate
```

Поставьте pyspark:

```
pip install pyspark==3.5.0
```

Или сразу всё из requirements.txt:

```
pip install -r requirements.txt
```

---

## 🟥 ОШИБКА 4: `wheel build failed` при `pip install`

**Симптом:**
```
ERROR: Failed building wheel for pyarrow
Failed to build pyarrow
ERROR: Could not build wheels for pyarrow which use PEP 517 and cannot be installed directly
```

**Диагноз:** pip пытается скомпилировать пакет из исходников, но не хватает компилятора.

**Решение (macOS):**

Поставьте Xcode Command Line Tools:

```
xcode-select --install
```

Откроется окно — нажмите «Install». Подождите 5–10 минут.

После установки повторите:

```
pip install -r requirements.txt
```

**Альтернатива:** использовать `uv` — быстрее и реже падает:

```
pip install uv
```

```
uv pip install -r requirements.txt
```

---

## 🟥 ОШИБКА 5: `Cannot connect to py4j gateway`

**Симптом:**
```
py4j.protocol.Py4JNetworkError: An error occurred while trying to connect to the Java server
```

**Диагноз:** JVM не стартовал или не открыл порт. Часто из-за прав или конфликта портов.

**Решение:**

1. Закройте все Spark-сессии в других терминалах.
2. Перезапустите Terminal целиком.
3. Проверьте, что Java работает:

```
java -version
```

4. Запустите Spark с явной памятью драйвера:

```python
spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
```

---

## 🟥 ОШИБКА 6: `PythonRuntimeException: An exception was raised by the Python Proxy`

**Симптом:**
```
PythonRuntimeException: An exception was raised by the Python Proxy.
Return Message: An error occurred while calling ...
```

**Диагноз:** обычно — несовпадение версий Python между driver'ом и worker'ом, или ваш UDF упал на конкретной строке данных.

**Решение:**

1. Убедитесь, что Python один и тот же:

```python
import sys
print(sys.executable)
```

Этот же путь должен быть в `PYSPARK_PYTHON`:

```
echo $PYSPARK_PYTHON
```

Если пусто — пропишите в `.zshrc`:

```
echo 'export PYSPARK_PYTHON=$(which python3)' >> ~/.zshrc
```

```
source ~/.zshrc
```

2. Если ваш UDF падает — оберните в try/except и логируйте проблемные строки.

---

## 🟥 ОШИБКА 7: `OutOfMemoryError: Java heap space`

**Симптом:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Диагноз:** JVM не хватает памяти.

**Решение:**

1. Дайте драйверу больше памяти:

```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

2. Если делаете `collect()` или `toPandas()` на большом df — НЕ делайте. Используйте `df.show(20)` или `df.take(100)`.

3. Если падает на shuffle — увеличьте партиции:

```python
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

---

## 🟥 ОШИБКА 8: `Permission denied` на macOS

**Симптом:**
```
PermissionError: [Errno 1] Operation not permitted: '/tmp/spark-...'
```

**Диагноз:** macOS блокирует Terminal к диску.

**Решение:**

1. System Preferences → Privacy & Security → Full Disk Access.
2. Нажмите `+` → добавьте Terminal (или iTerm).
3. ✅ Поставьте галочку.
4. Перезапустите Terminal.

---

## 🟥 ОШИБКА 9: `winutils.exe not found` (Windows)

**Симптом:**
```
java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
```

**Диагноз:** на Windows Spark требует «винутилс» — кусок Hadoop.

**Решение:**

1. Скачайте https://github.com/cdarlint/winutils
2. Распакуйте папку `hadoop-3.3.x` в `C:\hadoop`.
3. В PowerShell (от админа):

```
[System.Environment]::SetEnvironmentVariable('HADOOP_HOME', 'C:\hadoop', 'User')
[System.Environment]::SetEnvironmentVariable('Path', $env:Path + ';C:\hadoop\bin', 'User')
```

4. Закройте все терминалы и откройте заново.

---

## 🟥 ОШИБКА 10: `python3 --version` показывает 3.9 вместо 3.11

**Симптом:** Homebrew поставил Python 3.11, но команда `python3` использует старый.

**Диагноз:** Старый Python в PATH стоит раньше нового.

**Решение:**

```
echo 'export PATH="/opt/homebrew/opt/python@3.11/libexec/bin:$PATH"' >> ~/.zshrc
```

```
source ~/.zshrc
```

```
python3 --version
```

Должно быть 3.11.x.

---

## 🟥 ОШИБКА 11: `inferSchema` тормозит чтение CSV

**Симптом:** `spark.read.csv(..., inferSchema=True)` — час на 1 ГБ файле.

**Диагноз:** `inferSchema=True` читает файл **дважды**: один раз для определения типов, второй для чтения.

**Решение:** задайте схему явно:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("id",     IntegerType(), False),
    StructField("name",   StringType(),  True),
    StructField("amount", DoubleType(),  True),
])

df = spark.read.csv("file.csv", header=True, schema=schema)
```

Или используйте Parquet вместо CSV — там схема уже встроена.

---

## 🟥 ОШИБКА 12: `df.show()` ничего не показывает или пустая таблица

**Симптом:** Spark отрабатывает, но в выводе пусто.

**Диагноз:** обычно — все строки отфильтровались (баг в фильтре), либо проблема со `show` на streaming-df.

**Решение:**

```python
print("Rows:", df.count())
df.printSchema()
df.show(5, truncate=False)
```

Если `count() == 0` — проверьте фильтр.
Если `count() > 0`, а `show()` пустой — типы колонок неправильные. Сделайте `df.dtypes`.

---

## 🟥 ОШИБКА 13: `collect()` или `toPandas()` крашит Python

**Симптом:** Python зависает или падает с OOM при `collect()`.

**Диагноз:** Эти методы тянут **ВСЕ данные** на driver. На больших датасетах — OOM.

**Решение:** **Никогда** не делайте `collect()` или `toPandas()` без `.limit()`:

```python
# Плохо
result = df.collect()

# Хорошо
sample = df.limit(100).collect()
# или
sample = df.sample(0.001).toPandas()
```

Если **вправду** нужно итерироваться по большому df:

```python
for row in df.toLocalIterator():
    process(row)
```

---

## 🟥 ОШИБКА 14: Spark UI недоступен (http://localhost:4040)

**Симптом:** Spark работает, но http://localhost:4040 показывает «не удалось подключиться».

**Диагноз:**

- Сессия Spark закрыта — UI живёт **только** пока активен `spark`.
- Порт 4040 занят (например, другой Spark) → Spark открывает на 4041, 4042 ...

**Решение:**

1. Откройте логи Spark при старте — там пишет `SparkUI available at http://...:4041`.
2. Держите сессию живой:

```python
spark = SparkSession.builder.getOrCreate()
# ... ваш код ...
input("Press Enter to stop Spark...")  # держим до клавиши
spark.stop()
```

---

## 🟥 ОШИБКА 15: Долгий старт Spark (30+ секунд) каждый раз

**Симптом:** запуск любого PySpark-скрипта 30+ секунд только на старт.

**Диагноз:** JVM запускается долго. Это **нормально** для local mode. На кластере один раз стартует — потом быстро.

**Решение:**

1. Для интерактивной работы — используйте **Jupyter notebook** или **IPython**: запустили сессию один раз, дальше переиспользуете.

2. Сделайте «прогрев» в начале скрипта:

```python
spark = SparkSession.builder.appName("App").master("local[*]").getOrCreate()
spark.sql("SELECT 1").count()  # короткий запрос для прогрева
```

3. Если очень критично — посмотрите Spark Connect (3.4+) для отдельного long-running JVM.

---

## 🟥 ОШИБКА 16: `git clone` падает с `SSL certificate problem`

**Симптом:**
```
fatal: unable to access 'https://github.com/...': SSL certificate problem
```

**Диагноз:** настройки SSL/прокси.

**Решение (временно):**

```
git config --global http.sslVerify false
```

(потом верните `true`)

Или используйте SSH:

```
git clone git@github.com:StarDust1508/BIG_DATA.git
```

---

## 🟥 ОШИБКА 17: `git push` спрашивает пароль

**Симптом:** при `git push` Mac спрашивает пароль, ваш обычный пароль GitHub не работает.

**Диагноз:** с 2021 года GitHub запретил пароль для push. Нужен **Personal Access Token**.

**Решение:**

1. Откройте https://github.com/settings/tokens
2. **Generate new token (classic)**.
3. Scope: ☑ **repo**.
4. Скопируйте токен.
5. При следующем `git push` вместо пароля введите **токен**.

Mac сохранит его в Keychain — больше спрашивать не будет.

---

## 🟥 ОШИБКА 18: `zsh: command not found: #`

**Симптом:** копируете строку с `#` — zsh ругается.

**Диагноз:** вы скопировали блок кода **вместе с комментарием**. В Terminal `#` — это команда, не комментарий (в интерактивном режиме).

**Решение:** копируйте **только сами команды**, без строк, начинающихся с `#`. В нашем курсе после исправления — `#`-строк в bash-блоках больше нет.

---

## 🟥 ОШИБКА 19: `ImportError: cannot import name 'soft_unicode' from 'markupsafe'`

**Симптом:** конкретная ошибка с библиотекой `markupsafe` при импорте `jinja2`.

**Диагноз:** конфликт версий между установленными пакетами.

**Решение:**

```
pip install --upgrade markupsafe jinja2
```

Или пересоздайте venv с нуля:

```
deactivate
```

```
rm -rf .venv
```

```
python3 -m venv .venv
```

```
source .venv/bin/activate
```

```
pip install -r requirements.txt
```

---

## 🟥 ОШИБКА 20: Conda и venv конфликтуют

**Симптом:** venv создан, но `pip install` ставит пакеты не туда, или Python использует conda-окружение.

**Диагноз:** на машине стоят сразу conda и Python от Homebrew. Они «делят» PATH.

**Решение:**

1. Деактивируйте conda:

```
conda deactivate
```

2. Если в `.zshrc` есть `conda init` — закомментируйте эти строки (или удалите).

3. Перезапустите Terminal.

4. Используйте только Homebrew Python + venv:

```
which python3
```

Должен показать `/opt/homebrew/...`, не `/Users/.../miniconda3/...`.

---

## 🆘 Если ничего из этого не помогло

1. **Скопируйте полный текст ошибки** (включая stacktrace).
2. Найдите её в Google — обычно есть Stack Overflow ответ.
3. Откройте Issue в репозитории: https://github.com/StarDust1508/BIG_DATA/issues
4. Приложите:
   - Полный stacktrace.
   - Версии: `python --version`, `java -version`, `pip show pyspark`.
   - macOS / Linux / Windows.
   - На какой команде упало.

---

## 🧠 Главное правило

**90% ошибок Spark** — это **5 причин**:

1. Java не установлена / JAVA_HOME не прописан.
2. venv не активирован.
3. Версии Python отличаются у driver и worker.
4. Память забита `collect()` / `broadcast()` / cache().
5. Старая Python-сессия не закрыта.

Если ошибка — пройдитесь по этим 5 причинам, в 90% случаев одна из них и есть проблема.
