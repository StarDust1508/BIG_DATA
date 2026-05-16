# 🧩 Практика 1 — HDFS-упражнения

> Эти задания выполняются **внутри Docker-контейнера с Hadoop** (см. урок 3.2).
> Если у вас нет возможности поднять Hadoop — пробегите задания «в голове» по шпаргалке.

---

## Подготовка

Поднять кластер по инструкции из урока 3.2:
```bash
docker-compose up -d
docker exec -it namenode bash
```

Загрузить учебный файл (внутри контейнера или с хоста):
```bash
# Скопировать transactions_sample.csv из локального датасета в контейнер
docker cp datasets/transactions_sample.csv namenode:/tmp/

# Внутри контейнера
hdfs dfs -mkdir -p /user/study
hdfs dfs -put /tmp/transactions_sample.csv /user/study/
```

---

## Упражнения

### 1. Базовая навигация
Ответьте командами HDFS:

- a) Сколько файлов в `/user/study`?
- b) Какой размер у `transactions_sample.csv` в МБ?
- c) Какова дата модификации файла?

<details>
<summary>Подсказка</summary>

```bash
hdfs dfs -ls /user/study
hdfs dfs -du -h /user/study
```
</details>

---

### 2. Структура блоков

- a) Создайте файл размером 300 МБ:
  ```bash
  dd if=/dev/urandom of=/tmp/big.bin bs=1M count=300
  hdfs dfs -put /tmp/big.bin /user/study/
  ```
- b) Узнайте, на сколько блоков он разбит.
- c) Какой размер последнего блока?

<details>
<summary>Подсказка</summary>

```bash
hdfs fsck /user/study/big.bin -files -blocks -locations
```
</details>

---

### 3. Репликация

- a) Узнайте текущий replication factor у `big.bin`.
- b) Поменяйте его на 2 (если в кластере хотя бы 2 DataNode'а).
- c) Проверьте через `fsck`, что изменилось.

<details>
<summary>Подсказка</summary>

```bash
hdfs dfs -ls /user/study/        # колонка после permissions = factor
hdfs dfs -setrep 2 /user/study/big.bin
hdfs fsck /user/study/big.bin -files -blocks -locations
```
</details>

---

### 4. Структура папок (Hive-стиль)

Создайте партиционированную структуру:
```
/user/study/transactions/
   year=2026/
      month=01/
      month=02/
   year=2025/
      month=12/
```

И положите в каждую папку какой-нибудь маленький CSV.

<details>
<summary>Подсказка</summary>

```bash
for y in 2025 2026; do
  for m in 01 02 03 12; do
    hdfs dfs -mkdir -p /user/study/transactions/year=$y/month=$m
  done
done
hdfs dfs -ls -R /user/study/transactions/
```
</details>

---

### 5. Права доступа

- a) Создайте папку `/user/study/private`.
- b) Дайте ей права `750`.
- c) Сделайте владельцем `study:study`.
- d) Проверьте через `hdfs dfs -ls`.

---

### 6. Размер кластера и здоровье

- a) Сколько всего DataNode'ов?
- b) Сколько ГБ свободно в кластере?
- c) Какие блоки «under-replicated» (если есть)?

<details>
<summary>Подсказка</summary>

```bash
hdfs dfsadmin -report
hdfs fsck /
```
</details>

---

### 7. Балансировка (опционально)

В реальном кластере — обязательная процедура:
```bash
hdfs balancer -threshold 5
```

Не выполняйте без необходимости в учебном — это длительная операция.

---

### 8. Удаление

- a) Удалите `big.bin`.
- b) Куда он деется (см. `.Trash`)?
- c) Очистите trash командой:
  ```bash
  hdfs dfs -expunge
  ```

---

## ✅ Чек-лист «знаю HDFS»

- [ ] Могу создать/удалить файл и папку.
- [ ] Понимаю, что такое блоки и как их посмотреть.
- [ ] Знаю, как менять replication factor.
- [ ] Понимаю, что NameNode хранит мета, а DataNode — блоки.
- [ ] Знаю, что HDFS наследует POSIX-права + ACL.

После выполнения упражнений отметьте в [../ХОД_ИЗУЧЕНИЯ.md](../ХОД_ИЗУЧЕНИЯ.md).

---

**Дальше:** [Практика 2 — WordCount на Python](./практика_2_wordcount/) → реальный MapReduce.
