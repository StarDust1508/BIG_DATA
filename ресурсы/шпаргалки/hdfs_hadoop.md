# 🗄️ HDFS / Hadoop Cheat Sheet

> Основные команды HDFS. Они почти один в один похожи на UNIX-команды.

## Файловые операции (`hdfs dfs` ~ `hadoop fs`)

```bash
hdfs dfs -ls /
hdfs dfs -ls -R /user
hdfs dfs -mkdir -p /user/me/data
hdfs dfs -put local.csv /user/me/
hdfs dfs -copyFromLocal *.csv /data/
hdfs dfs -get /user/me/file.csv ./
hdfs dfs -rm /user/me/file.csv
hdfs dfs -rm -r /user/me/
hdfs dfs -cat /user/me/file.txt
hdfs dfs -tail /user/me/file.txt
hdfs dfs -du -h /user
hdfs dfs -df -h
hdfs dfs -count /data
hdfs dfs -chmod 750 /data/private
hdfs dfs -chown user:group /data
```

## Информация о кластере

```bash
hdfs dfsadmin -report
hdfs fsck /
hdfs fsck /file -files -blocks -locations
hdfs version
```

## Репликация

```bash
hdfs dfs -setrep -R 3 /data
hdfs dfs -setrep 1 /tmp
```

## Архив

```bash
hadoop archive -archiveName logs.har -p /logs/2026 /archive
hdfs dfs -ls har:///archive/logs.har
```

## YARN (для запуска MR / Spark)

```bash
yarn application -list
yarn application -kill <id>
yarn logs -applicationId <id>
yarn node -list -all
```

## MapReduce streaming (учебный пример Python-MR)

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input  /data/text.txt \
    -output /out/wordcount \
    -mapper  "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file mapper.py -file reducer.py
```

## Архитектура (для шпаргалки)

```
HDFS:
  NameNode      — мета (один; HA — два с ZK)
  DataNode × N  — блоки (по умолчанию 128 МБ, реплика ×3)

YARN:
  ResourceManager — распределяет ресурсы
  NodeManager × N — на каждой ноде
  ApplicationMaster — на каждое приложение

MapReduce job:
  Map  → Combine → Shuffle/Sort → Reduce
```

## Куда смотреть

- NameNode UI: `http://namenode:9870`
- YARN UI: `http://resourcemanager:8088`
- HDFS логи: `$HADOOP_HOME/logs/`
