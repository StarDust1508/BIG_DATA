# 🔥 Глубокий SQL для Big Data

> SQL — самый важный язык дата-инженера. Этот гайд — что должен уметь middle/senior.

Все примеры — Spark SQL, но работают почти везде (PostgreSQL, BigQuery, Snowflake).

---

## 1. Уровни владения SQL

| Уровень | Что умеешь |
|---------|-----------|
| Junior | SELECT, WHERE, GROUP BY, JOIN, базовые агрегаты |
| **Middle** | **window functions, CTE, conditional aggregation, EXPLAIN, optimization** |
| Senior | recursive CTE, lateral, pivot/unpivot, query rewriting, query planner internals |

Большинство курсов застревает на Junior. Мы — на Middle+.

---

## 2. CTE (Common Table Expressions)

```sql
WITH big_clients AS (
    SELECT client_id, SUM(amount) AS total
    FROM transactions
    GROUP BY client_id
    HAVING total > 100000
),
big_clients_with_segments AS (
    SELECT b.client_id, b.total, c.segment
    FROM big_clients b
    JOIN clients c USING (client_id)
)
SELECT segment, COUNT(*) AS n, SUM(total) AS gross
FROM big_clients_with_segments
GROUP BY segment;
```

**Зачем:**
- Читаемость многоступенчатых запросов.
- Возможность переиспользовать промежуточный результат.
- Не создаёт временную таблицу (это просто синтаксис).

⚠️ В Spark CTE **не материализуется** автоматически. Если запрос ссылается на CTE дважды — Spark пересчитает. Решение: вынести в `df.cache()`.

---

## 3. Window functions — глубоко

### 3.1. Ranking

```sql
SELECT
    client_id, ts, amount,
    -- Простой порядковый номер
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ts) AS rn,
    -- Ранг с пропусками при ничьих
    RANK()       OVER (PARTITION BY client_id ORDER BY amount DESC) AS rk,
    -- Ранг без пропусков
    DENSE_RANK() OVER (PARTITION BY client_id ORDER BY amount DESC) AS drk,
    -- Перцентильный ранг (0..1)
    PERCENT_RANK() OVER (PARTITION BY client_id ORDER BY amount) AS pct,
    -- Деление на N бакетов (квартили = NTILE(4))
    NTILE(4)      OVER (ORDER BY amount) AS quartile
FROM transactions;
```

### 3.2. Сравнение со «соседними» строками

```sql
SELECT
    client_id, ts, amount,
    LAG(amount, 1)  OVER (PARTITION BY client_id ORDER BY ts) AS prev,
    LEAD(amount, 1) OVER (PARTITION BY client_id ORDER BY ts) AS next,
    amount - LAG(amount, 1) OVER (PARTITION BY client_id ORDER BY ts) AS delta
FROM transactions;
```

### 3.3. Running и rolling агрегаты

```sql
SELECT
    client_id, ts, amount,
    -- Running total (накопительная)
    SUM(amount) OVER (
        PARTITION BY client_id ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,

    -- Rolling 7 строк
    AVG(amount) OVER (
        PARTITION BY client_id ORDER BY ts
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_last7,

    -- Rolling по времени — последние 30 дней
    SUM(amount) OVER (
        PARTITION BY client_id ORDER BY UNIX_TIMESTAMP(ts)
        RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW
    ) AS sum_30days
FROM transactions;
```

⚠️ `ROWS BETWEEN` — по позициям. `RANGE BETWEEN` — по значениям. Это очень разное.

### 3.4. Первое / последнее значение в окне

```sql
SELECT
    client_id, ts, amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY client_id ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_amount,
    LAST_VALUE(amount) OVER (
        PARTITION BY client_id ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_amount
FROM transactions;
```

⚠️ Для `LAST_VALUE` обязательно `UNBOUNDED FOLLOWING` — иначе по умолчанию окно «до текущей строки», и `LAST` = текущая.

---

## 4. Conditional aggregation — мощный приём

Считать несколько метрик одним запросом без подзапросов:

```sql
SELECT
    segment,
    COUNT(*) AS total_clients,
    COUNT(CASE WHEN age < 30 THEN 1 END) AS young,
    COUNT(CASE WHEN age BETWEEN 30 AND 50 THEN 1 END) AS middle,
    COUNT(CASE WHEN age > 50 THEN 1 END) AS senior,
    AVG(CASE WHEN gender = 'M' THEN monthly_income END) AS avg_income_M,
    AVG(CASE WHEN gender = 'F' THEN monthly_income END) AS avg_income_F,
    SUM(CASE WHEN city IN ('Москва', 'СПб') THEN monthly_income ELSE 0 END)
        AS sum_capital_income
FROM clients
GROUP BY segment;
```

Альтернатива через `FILTER` (PostgreSQL, BigQuery, не Spark):
```sql
COUNT(*) FILTER (WHERE age < 30) AS young
```

В Spark — только через `CASE WHEN`.

---

## 5. Pivot / Unpivot

### Pivot — «развернуть» строки в колонки

В Spark SQL:
```sql
SELECT *
FROM (
    SELECT category, currency, amount FROM transactions
)
PIVOT (
    SUM(amount) FOR currency IN ('RUB' AS rub, 'USD' AS usd, 'EUR' AS eur)
);
```

Или через PySpark:
```python
df.groupBy("category").pivot("currency").sum("amount")
```

### Unpivot — обратно «колонки в строки»

В Spark SQL:
```sql
SELECT *
FROM transactions_pivoted
UNPIVOT (
    amount FOR currency IN (rub, usd, eur)
);
```

---

## 6. Рекурсивные CTE

Spark поддерживает с 3.4+. Классика — обход иерархии:

```sql
WITH RECURSIVE org_hierarchy AS (
    -- Базовый случай: корни (CEO без manager_id)
    SELECT
        employee_id,
        name,
        manager_id,
        1 AS level,
        CAST(name AS STRING) AS path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Рекурсивный шаг: подчинённые предыдущего уровня
    SELECT
        e.employee_id,
        e.name,
        e.manager_id,
        h.level + 1,
        h.path || ' > ' || e.name
    FROM employees e
    JOIN org_hierarchy h ON e.manager_id = h.employee_id
)
SELECT * FROM org_hierarchy ORDER BY level, name;
```

Применения:
- Иерархии организаций / категорий товаров.
- Графовые задачи (BFS).
- Числовые последовательности (генерация дат).

---

## 7. LATERAL / EXPLODE

«Для каждой строки таблицы развернуть массив в подстроки».

В Spark SQL:
```sql
SELECT id, item
FROM orders
LATERAL VIEW EXPLODE(items) tbl AS item;
```

В PySpark:
```python
from pyspark.sql.functions import explode
df.select("id", explode("items").alias("item"))
```

---

## 8. EXPLAIN — читаем план

В Spark:
```sql
EXPLAIN [EXTENDED | FORMATTED]
SELECT category, SUM(amount) FROM tx WHERE currency='RUB' GROUP BY category;
```

Что искать:
- **`Filter`** — где применён WHERE.
- **`Aggregate`** — как сгруппировано.
- **`Exchange`** — где shuffle (= дорого).
- **`BroadcastHashJoin`** — хорошо.
- **`SortMergeJoin`** — нормально.
- **`PushedFilters`** — фильтр пошёл в Parquet (отлично).
- **`ReadSchema`** — только нужные колонки читаются.

В реальной работе: 80% оптимизаций — это посмотреть EXPLAIN и переписать запрос.

---

## 9. Оптимизация запросов

### 9.1. Предикат как можно раньше

❌ Плохо:
```sql
SELECT * FROM (
    SELECT t.*, c.segment FROM tx t JOIN clients c USING (client_id)
)
WHERE segment = 'premium';
```

✅ Хорошо:
```sql
SELECT t.*, c.segment
FROM tx t
JOIN (SELECT * FROM clients WHERE segment = 'premium') c USING (client_id);
```

(Catalyst часто сам это делает, но не всегда. Лучше помочь.)

### 9.2. EXISTS вместо IN с подзапросом

```sql
-- Часто медленнее
SELECT * FROM clients
WHERE client_id IN (SELECT client_id FROM transactions WHERE amount > 1000000);

-- Обычно быстрее
SELECT * FROM clients c
WHERE EXISTS (
    SELECT 1 FROM transactions t
    WHERE t.client_id = c.client_id AND t.amount > 1000000
);
```

### 9.3. Anti-join вместо `NOT IN`

```sql
-- Опасно: NOT IN ломается на NULL
SELECT * FROM clients
WHERE client_id NOT IN (SELECT client_id FROM blacklist);

-- Безопасно
SELECT c.* FROM clients c
LEFT JOIN blacklist b USING (client_id)
WHERE b.client_id IS NULL;
```

### 9.4. Distinct vs Group By

`SELECT DISTINCT a, b` и `SELECT a, b GROUP BY a, b` — обычно эквивалентны в Spark, но в некоторых СУБД GROUP BY быстрее.

### 9.5. Не используйте SELECT *

В колоночных хранилищах (Parquet, ORC) `SELECT *` читает все колонки, даже если 99% не нужны. Всегда явный список.

---

## 10. Типичные задачи с интервью

### 10.1. Найти топ-N в каждой группе
```sql
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY group_col ORDER BY value DESC) AS rn
    FROM data
)
WHERE rn <= 3;
```

### 10.2. Найти пропущенные числа в последовательности
```sql
WITH RECURSIVE numbers(n) AS (
    SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n < 1000
)
SELECT n FROM numbers
WHERE n NOT IN (SELECT id FROM mytable);
```

### 10.3. Удалить дубликаты
```sql
-- Используем CTE с window
WITH dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY ts DESC) AS rn
    FROM mytable
)
SELECT * EXCEPT (rn) FROM dedup WHERE rn = 1;
```

### 10.4. Пользователи, которые ничего не купили за 30 дней
```sql
SELECT u.*
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
    AND o.ts > CURRENT_DATE - INTERVAL 30 DAY
WHERE o.user_id IS NULL;
```

### 10.5. Скользящее среднее с заполнением пропусков по дням
```sql
WITH days AS (
    SELECT explode(sequence(date_start, date_end, interval 1 day)) AS d
),
daily AS (
    SELECT d.d AS day, COALESCE(SUM(amount), 0) AS total
    FROM days d
    LEFT JOIN orders o ON DATE(o.ts) = d.d
    GROUP BY d.d
)
SELECT day, total,
       AVG(total) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7
FROM daily;
```

### 10.6. Sessionization (новая сессия при разрыве > 30 мин)

```sql
WITH events_with_gap AS (
    SELECT *,
        UNIX_TIMESTAMP(ts) - UNIX_TIMESTAMP(LAG(ts) OVER (PARTITION BY user_id ORDER BY ts)) AS gap_sec
    FROM events
),
sessions AS (
    SELECT *,
        SUM(CASE WHEN gap_sec > 1800 OR gap_sec IS NULL THEN 1 ELSE 0 END)
            OVER (PARTITION BY user_id ORDER BY ts) AS session_id
    FROM events_with_gap
)
SELECT user_id, session_id, MIN(ts) AS session_start, MAX(ts) AS session_end,
       COUNT(*) AS n_events
FROM sessions
GROUP BY user_id, session_id;
```

### 10.7. Cohort analysis

```sql
WITH cohorts AS (
    SELECT user_id, DATE_TRUNC('month', MIN(ts)) AS cohort
    FROM events
    GROUP BY user_id
),
activity AS (
    SELECT c.cohort,
           DATE_TRUNC('month', e.ts) AS month,
           COUNT(DISTINCT e.user_id) AS active
    FROM events e
    JOIN cohorts c USING (user_id)
    GROUP BY c.cohort, DATE_TRUNC('month', e.ts)
)
SELECT cohort, month,
       active * 100.0 / FIRST_VALUE(active) OVER (PARTITION BY cohort ORDER BY month) AS retention
FROM activity
ORDER BY cohort, month;
```

---

## 11. Подводные камни Spark SQL

1. **`UNION` vs `UNION ALL`** — `UNION` делает distinct (дорого). `UNION ALL` — просто склейка. Чаще нужен второй.

2. **`COUNT(DISTINCT col)`** на больших данных — медленный. Используйте `approx_count_distinct(col)` (HyperLogLog).

3. **`ORDER BY`** без `LIMIT` — глобальный shuffle. Часто это **не то**, что вы хотите. Замените на `sortWithinPartitions`.

4. **Implicit casts** — будьте осторожны. `WHERE int_col = '123'` сравнивает int со string. Может отключить partition pruning.

5. **NULL в SQL** — особый зверь. `NULL = NULL` → `NULL` (не true!). Используйте `IS NULL`, `IS NOT DISTINCT FROM`.

6. **Подзапросы в SELECT** — могут быть **очень** медленными. Часто можно переписать через JOIN.

---

## 12. Где практиковаться

- **HackerRank SQL** — простые задачи.
- **LeetCode SQL** (бесплатные) — middle-уровень.
- **DataLemur** — middle/senior, ориентирован на интервью.
- **StrataScratch** — реальные интервью-задачи компаний.
- **SQL Murder Mystery** — игровой формат.

---

## ✅ Чек-лист «знаю SQL для Big Data»

- [ ] Пишу запросы с window functions без подсказок.
- [ ] Знаю разницу `ROWS BETWEEN` и `RANGE BETWEEN`.
- [ ] Использую CTE для читаемости.
- [ ] Знаю, что такое EXPLAIN и читаю его.
- [ ] Не использую `SELECT *`.
- [ ] Знаю про anti-join вместо `NOT IN`.
- [ ] Умею делать sessionization и cohort analysis.
- [ ] Понимаю pivot/unpivot.
- [ ] Знаю про NULL-семантику.

После этого можно идти на интервью на middle-уровень.
