# Урок 3.2 — HDFS на практике

> Без живого Hadoop команды можно только зачитывать. Поднимем мини-кластер за 5 минут в Docker и пощупаем настоящий HDFS.

---

## Часть 1. Поднимаем кластер в Docker

Способ 1 — **Docker Compose** (рекомендую). Создайте файл `docker-compose.yml`:

```yaml
version: "3"

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    restart: always
    ports:
      - "9870:9870"          # Web UI
      - "9000:9000"          # RPC
    volumes:
      - ./hadoop_namenode:/hadoop/dfs/name
    environment:
      CLUSTER_NAME: bigdata-learn
    env_file:
      - hadoop.env

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    restart: always
    volumes:
      - ./hadoop_datanode:/hadoop/dfs/data
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    env_file:
      - hadoop.env
```

И `hadoop.env`:
```
CORE_CONF_fs_defaultFS=hdfs://namenode:9000
CORE_CONF_hadoop_http_staticuser_user=root
HDFS_CONF_dfs_replication=1
```

(replication=1, потому что у нас один DataNode.)

Запуск:
```bash
docker-compose up -d
```

Зайти в namenode:
```bash
docker exec -it namenode bash
```

Открыть Web UI: http://localhost:9870

Способ 2 — **без Docker**: ставите Hadoop вручную (документация Apache). Сложнее, муторнее. На учебном этапе не рекомендую.

Способ 3 — **облако**: AWS EMR, Yandex DataProc. Платно, но 1 час обучения стоит копейки.

---

## Часть 2. Базовые команды HDFS

Внутри контейнера или на машине с Hadoop:

```bash
# Список корня
hdfs dfs -ls /

# Создать папку
hdfs dfs -mkdir -p /user/me/raw

# Положить файл
hdfs dfs -put localfile.csv /user/me/raw/

# Прочитать
hdfs dfs -cat /user/me/raw/localfile.csv | head

# Размер
hdfs dfs -du -h /user/me/

# Удалить
hdfs dfs -rm /user/me/raw/localfile.csv
hdfs dfs -rm -r /user/me/                  # рекурсивно
```

Шпаргалка со всеми командами: [ресурсы/шпаргалки/hdfs_hadoop.md](../ресурсы/шпаргалки/hdfs_hadoop.md).

---

## Часть 3. Посмотреть, как файл разбит на блоки

```bash
# Создаём файл побольше — например, 500 МБ
dd if=/dev/urandom of=/tmp/big.bin bs=1M count=500

# Кладём в HDFS
hdfs dfs -put /tmp/big.bin /user/me/

# Смотрим блоки
hdfs fsck /user/me/big.bin -files -blocks -locations
```

Получите примерно такое:
```
/user/me/big.bin 524288000 bytes, replicated: replication=1, 4 block(s):
  0. BP-...:blk_1073741825_1001 len=134217728 Live_repl=1  [DatanodeInfoWithStorage[172.18.0.3:9866,...]]
  1. BP-...:blk_1073741826_1002 len=134217728 Live_repl=1  [DatanodeInfoWithStorage[172.18.0.3:9866,...]]
  2. BP-...:blk_1073741827_1003 len=134217728 Live_repl=1  [...]
  3. BP-...:blk_1073741828_1004 len= 121635816 Live_repl=1 [...]
```

500 МБ ÷ 128 МБ = ~4 блока. Видим, как Hadoop разрезал.

---

## Часть 4. Репликация

```bash
hdfs dfs -setrep 3 /user/me/big.bin       # 3 копии
hdfs dfs -setrep 1 /tmp                    # 1 копия (хватит для tmp)
hdfs dfsadmin -report                       # отчёт о DataNode'ах
```

Replication=3 — стандарт для prod. Replication=1 — учебно/dev.

---

## Часть 5. Права доступа

HDFS наследует POSIX-модель:

```bash
hdfs dfs -chmod 750 /user/me/sensitive/    # rwxr-x---
hdfs dfs -chown analytics:analytics /user/me/sensitive/
hdfs dfs -setfacl -m user:bob:r-- /user/me/sensitive/
```

Этого достаточно для базовой защиты. Для серьёзного — Apache Ranger / Sentry с политиками и audit log.

---

## Часть 6. Что смотреть в Web UI

http://localhost:9870 — Namenode UI.

Полезные вкладки:
- **Overview** — здоровье кластера, число DataNode'ов, объём.
- **Datanodes** — список нод, использование диска.
- **Utilities → Browse file system** — навигация по HDFS через браузер.

---

## Часть 7. Боль реальной эксплуатации

Когда вы будете работать с реальным Hadoop, столкнётесь с такими «классиками»:

| Проблема | Симптом | Решение |
|---|---|---|
| **Small files problem** | Миллион мелких файлов → NameNode тормозит | `hadoop archive`, перепаковка в Parquet |
| **Skew по блокам** | Один DataNode заполнен на 90%, другие на 20% | `hdfs balancer` |
| **Replication < target** | После падения ноды копий стало 2 вместо 3 | Hadoop сам докопирует |
| **NameNode OOM** | NameNode крашится | Дать больше heap, разбить кластер на федерации |
| **Slow disks** | Один DN тормозит → весь кластер тормозит | мониторинг, замена дисков |

Это к тому, что Hadoop — не «поставил и забыл». Поэтому облако с managed-сервисами (EMR/Dataproc) часто стоит дороже, но окупается отсутствием DevOps-головной боли.

---

## Часть 8. HDFS vs S3

| | HDFS | S3 (object storage) |
|---|---|---|
| Развёртывание | свой кластер | managed |
| Стоимость | дорого на железо | дёшево на хранение |
| Производительность | высокая, локальность | средняя, сетевая |
| Транзакции / атомичность | да (с оговорками) | нет (eventual consistency) |
| Метаданные | NameNode | бесконечная масштабируемость |
| Эволюция | замедление | стандарт de facto |

В новых проектах **S3 (или совместимый MinIO/GCS/Azure Blob) почти всегда побеждает**. HDFS остаётся в on-prem-кластерах.

---

## ✅ Самопроверка

1. Какой командой посмотреть, на каких блоках лежит файл?
2. Зачем нужна репликация и какое значение по умолчанию?
3. Что такое «small files problem»?
4. Чем S3 проще, чем HDFS, и в чём проигрывает?
5. Через какую утилиту балансировать использование DataNode'ов?

---

## ▶️ Дальше

[Урок 3.3 — MapReduce: парадигма](./урок_3_mapreduce.md)
