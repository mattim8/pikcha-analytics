-- базы
CREATE DATABASE IF NOT EXISTS piccha_raw;
CREATE DATABASE IF NOT EXISTS piccha_mart;

-- RAW MergeTree: храним здесь цельный JSON одной строкой
CREATE TABLE IF NOT EXISTS piccha_raw.stores_raw
(
  ingest_time DateTime DEFAULT now(),
  source String,
  payload String
) ENGINE = MergeTree ORDER BY ingest_time;

CREATE TABLE IF NOT EXISTS piccha_raw.products_raw
(
  ingest_time DateTime DEFAULT now(),
  source String,
  payload String
) ENGINE = MergeTree ORDER BY ingest_time;

CREATE TABLE IF NOT EXISTS piccha_raw.customers_raw
(
  ingest_time DateTime DEFAULT now(),
  source String,
  payload String
) ENGINE = MergeTree ORDER BY ingest_time;

CREATE TABLE IF NOT EXISTS piccha_raw.purchases_raw
(
  ingest_time DateTime DEFAULT now(),
  source String,
  payload String
) ENGINE = MergeTree ORDER BY ingest_time;

-- Kafka источники (адрес брокера как в docker-compose, используем ENGINE = Kafka, подключаемся к брокеру как консьюмер с именем группы kafka_group_name,
-- читает сообщения батчами и отдает MV; по поводу RawBLOB - KafkaEngine может сама парсить сообщения в разные форматы или просто принимать,
-- RawBLOB не позволяет ничего парсить, а отдает одной строкой в payload String, MV перепишет payload в RAW)
CREATE TABLE IF NOT EXISTS piccha_raw.kafka_stores (payload String)
ENGINE = Kafka
SETTINGS kafka_broker_list='kafka:9092',
         kafka_topic_list='stores',
         kafka_group_name='ch_stores',
         kafka_format='RawBLOB';

CREATE TABLE IF NOT EXISTS piccha_raw.kafka_products (payload String)
ENGINE = Kafka
SETTINGS kafka_broker_list='kafka:9092',
         kafka_topic_list='products',
         kafka_group_name='ch_products',
         kafka_format='RawBLOB';

CREATE TABLE IF NOT EXISTS piccha_raw.kafka_customers (payload String)
ENGINE = Kafka
SETTINGS kafka_broker_list='kafka:9092',
         kafka_topic_list='customers',
         kafka_group_name='ch_customers',
         kafka_format='RawBLOB';

CREATE TABLE IF NOT EXISTS piccha_raw.kafka_purchases (payload String)
ENGINE = Kafka
SETTINGS kafka_broker_list='kafka:9092',
         kafka_topic_list='purchases',
         kafka_group_name='ch_purchases',
         kafka_format='RawBLOB';

-- MVs: из Kafka в RAW MergeTree(KafkaEngine читает из топика и отдает сюда, у нас тут есть запрос SELECT...,
-- а MV уже пишет в RAW(MergeTree))
CREATE MATERIALIZED VIEW IF NOT EXISTS piccha_raw.mv_stores_raw
TO piccha_raw.stores_raw AS
SELECT now() AS ingest_time, 'stores' AS source, payload FROM piccha_raw.kafka_stores;

CREATE MATERIALIZED VIEW IF NOT EXISTS piccha_raw.mv_products_raw
TO piccha_raw.products_raw AS
SELECT now() AS ingest_time, 'products' AS source, payload FROM piccha_raw.kafka_products;

CREATE MATERIALIZED VIEW IF NOT EXISTS piccha_raw.mv_customers_raw
TO piccha_raw.customers_raw AS
SELECT now() AS ingest_time, 'customers' AS source, payload FROM piccha_raw.kafka_customers;

CREATE MATERIALIZED VIEW IF NOT EXISTS piccha_raw.mv_purchases_raw
TO piccha_raw.purchases_raw AS
SELECT now() AS ingest_time, 'purchases' AS source, payload FROM piccha_raw.kafka_purchases;


                                              /* ==================== MART.SQL ==================== */

CREATE DATABASE IF NOT EXISTS piccha_mart;

-- MART: чистые витрины.
-- MV делает: JSON → извлечение → lower → типизация → валидация (WHERE) → вставка.
-- ReplacingMergeTree(ingest_time) оставляет последнюю версию по ключу сортировки
-- во время фоновых merge; FINAL не нужен, т.к. дубли гасим ещё на этапе MV.

CREATE TABLE IF NOT EXISTS piccha_mart.purchases_clean
(
  purchase_id   String,
  store_id      String,
  customer_id   String,
  purchase_dt   DateTime,
  total_amount  Decimal(10,2),
  paid_cash     UInt8,
  paid_card     UInt8,
  delivery      UInt8,
  ingest_time   DateTime
)
ENGINE = ReplacingMergeTree(ingest_time)
ORDER BY (purchase_dt, purchase_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS piccha_mart.mv_purchases_clean
TO piccha_mart.purchases_clean AS
SELECT
  lowerUTF8(JSON_VALUE(payload,'$.purchase_id'))                                  AS purchase_id,
  lowerUTF8(JSON_VALUE(payload,'$.store.store_id'))                               AS store_id,
  lowerUTF8(JSON_VALUE(payload,'$.customer.customer_id'))                         AS customer_id,
  parseDateTimeBestEffortOrNull(JSON_VALUE(payload,'$.purchase_datetime'))        AS purchase_dt,
  toDecimal64OrZero(JSON_VALUE(payload,'$.total_amount'), 2)                      AS total_amount,
  toUInt8OrZero(JSON_VALUE(payload,'$.paid_cash'))                                 AS paid_cash,
  toUInt8OrZero(JSON_VALUE(payload,'$.paid_card'))                                 AS paid_card,
  toUInt8OrZero(JSON_VALUE(payload,'$.delivery'))                                  AS delivery,
  ingest_time
FROM piccha_raw.purchases_raw
WHERE
  nullIf(lowerUTF8(JSON_VALUE(payload,'$.purchase_id')),'') IS NOT NULL
  AND nullIf(lowerUTF8(JSON_VALUE(payload,'$.store.store_id')),'') IS NOT NULL
  AND nullIf(lowerUTF8(JSON_VALUE(payload,'$.customer.customer_id')),'') IS NOT NULL
  AND parseDateTimeBestEffortOrNull(JSON_VALUE(payload,'$.purchase_datetime')) IS NOT NULL
  AND parseDateTimeBestEffortOrNull(JSON_VALUE(payload,'$.purchase_datetime')) <= now();

/* ==================== CUSTOMERS ==================== */
CREATE TABLE IF NOT EXISTS piccha_mart.customers_clean
(
  customer_id     String,
  birth_date      Date,
  registration_dt DateTime,
  home_store_id   String,
  ingest_time     DateTime
)
ENGINE = ReplacingMergeTree(ingest_time)
ORDER BY customer_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS piccha_mart.mv_customers_clean
TO piccha_mart.customers_clean AS
SELECT
  lowerUTF8(JSON_VALUE(payload,'$.customer_id'))                                   AS customer_id,
  toDate(parseDateTimeBestEffortOrNull(JSON_VALUE(payload,'$.birth_date')))        AS birth_date,
  parseDateTimeBestEffortOrNull(JSON_VALUE(payload,'$.registration_date'))         AS registration_dt,
  lowerUTF8(JSON_VALUE(payload,'$.home_store_id'))                                  AS home_store_id,
  ingest_time
FROM piccha_raw.customers_raw
WHERE
  nullIf(lowerUTF8(JSON_VALUE(payload,'$.customer_id')),'') IS NOT NULL
  AND toDate(parseDateTimeBestEffortOrNull(JSON_VALUE(payload,'$.birth_date'))) IS NOT NULL
  AND parseDateTimeBestEffortOrNull(JSON_VALUE(payload,'$.registration_date'))  IS NOT NULL
  AND toDate(parseDateTimeBestEffortOrNull(JSON_VALUE(payload,'$.birth_date'))) <= today()
  AND parseDateTimeBestEffortOrNull(JSON_VALUE(payload,'$.registration_date'))  <= now();

/* ====================== STORES ====================== */
CREATE TABLE IF NOT EXISTS piccha_mart.stores_clean
(
  store_id    String,
  store_name  String,
  city        String,
  ingest_time DateTime
)
ENGINE = ReplacingMergeTree(ingest_time)
ORDER BY store_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS piccha_mart.mv_stores_clean
TO piccha_mart.stores_clean AS
SELECT
  lowerUTF8(JSONExtractString(payload,'store_id'))      AS store_id,
  lowerUTF8(JSONExtractString(payload,'store_name'))    AS store_name,
  lowerUTF8(JSON_VALUE(payload,'$.location.city'))      AS city,
  ingest_time
FROM piccha_raw.stores_raw
WHERE nullIf(lowerUTF8(JSONExtractString(payload,'store_id')),'') IS NOT NULL;


-- raw и mart в одном файле, чтобы запускать сразу всё вместе, как требуется по ТЗ