-- ============================================================
-- TikTok Live Pipeline - ClickHouse Schema Initialization
--
-- Run this ONCE via the ClickHouse web UI or clickhouse-client
-- after deploying ClickHouse from the Dokploy template.
--
-- NOTE: The Kafka broker 'redpanda:9092' is reachable because both
--       ClickHouse and Redpanda are on dokploy-network.
-- ============================================================


-- ============================================================
-- STEP 1: Create the tiktok database
-- ============================================================
CREATE DATABASE IF NOT EXISTS tiktok;


-- ============================================================
-- STEP 2: Kafka Engine source table
-- Reads raw JSON messages from BOTH Redpanda topics in real-time.
-- Data flows: Redpanda -> this table -> Materialized Views -> MergeTree tables
-- ============================================================
CREATE TABLE IF NOT EXISTS tiktok.tiktok_kafka_source
(
    streamer    String,
    category    String,
    archetype   String,
    event_type  String,
    received_at String,   -- ISO 8601 string
    data        String    -- raw JSON blob of the event payload
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list         = 'redpanda:9092',
    kafka_topic_list          = 'tiktok.events.npc,tiktok.events.ecommerce',
    kafka_group_name          = 'clickhouse-consumer',
    kafka_format              = 'JSONEachRow',
    kafka_num_consumers       = 2,
    kafka_skip_broken_messages = 10;


-- ============================================================
-- STEP 3: MergeTree destination tables (Gold analytics layer)
-- Partitioned by month, ordered by streamer and timestamp.
-- TTL: data is automatically dropped after 1 year.
-- ============================================================

-- Gift events: NPC triggers, diamond spending, whale detection
CREATE TABLE IF NOT EXISTS tiktok.tiktok_gifts
(
    event_timestamp DateTime64(3, 'UTC'),
    streamer        LowCardinality(String),
    category        LowCardinality(String),
    archetype       LowCardinality(String),
    user_id         String,
    unique_id       String,
    nickname        String,
    gift_id         UInt64,
    gift_name       LowCardinality(String),
    diamond_count   UInt32,
    repeat_count    UInt32,
    is_streaking    UInt8,   -- 1 = mid-combo, 0 = final/standalone
    is_repeat_end   UInt8,   -- 1 = combo ended
    usd_value       Nullable(Float32)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (category, archetype, streamer, event_timestamp)
TTL event_timestamp + INTERVAL 1 YEAR;

-- Comment events: chat messages, sentiment, catchphrase mirroring
CREATE TABLE IF NOT EXISTS tiktok.tiktok_comments
(
    event_timestamp DateTime64(3, 'UTC'),
    streamer        LowCardinality(String),
    category        LowCardinality(String),
    archetype       LowCardinality(String),
    user_id         String,
    unique_id       String,
    nickname        String,
    comment_text    String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (category, archetype, streamer, event_timestamp)
TTL event_timestamp + INTERVAL 1 YEAR;

-- Like events: engagement, tap velocity
CREATE TABLE IF NOT EXISTS tiktok.tiktok_likes
(
    event_timestamp  DateTime64(3, 'UTC'),
    streamer         LowCardinality(String),
    category         LowCardinality(String),
    archetype        LowCardinality(String),
    user_id          String,
    unique_id        String,
    batch_count      UInt32,    -- likes in this batch
    total_room_likes UInt64     -- cumulative likes in the room
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (category, archetype, streamer, event_timestamp)
TTL event_timestamp + INTERVAL 1 YEAR;

-- Viewer count snapshots: audience retention over time
CREATE TABLE IF NOT EXISTS tiktok.tiktok_room_viewers
(
    event_timestamp DateTime64(3, 'UTC'),
    streamer        LowCardinality(String),
    category        LowCardinality(String),
    archetype       LowCardinality(String),
    viewer_count    UInt32,
    popularity      UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (category, archetype, streamer, event_timestamp)
TTL event_timestamp + INTERVAL 1 YEAR;

-- Product pin events: e-commerce live shop product showcases
CREATE TABLE IF NOT EXISTS tiktok.tiktok_product_pins
(
    event_timestamp    DateTime64(3, 'UTC'),
    streamer           LowCardinality(String),
    category           LowCardinality(String),
    archetype          LowCardinality(String),
    product_id         String,
    title              String,
    price              String,
    open_url           String,
    live_product_count UInt32,
    action_type        Nullable(Int32)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (archetype, streamer, event_timestamp)
TTL event_timestamp + INTERVAL 1 YEAR;


-- ============================================================
-- STEP 4: Materialized Views
-- Auto-route incoming Kafka stream into the MergeTree tables
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS tiktok.mv_gifts
TO tiktok.tiktok_gifts AS
SELECT
    parseDateTimeBestEffortOrNull(received_at) AS event_timestamp,
    streamer,
    category,
    if(archetype = '', 'general', archetype) AS archetype,
    JSONExtractString(data, 'user_id')      AS user_id,
    JSONExtractString(data, 'unique_id')    AS unique_id,
    JSONExtractString(data, 'nickname')     AS nickname,
    JSONExtractUInt(data, 'gift_id')        AS gift_id,
    JSONExtractString(data, 'gift_name')    AS gift_name,
    JSONExtractUInt(data, 'diamond_count')  AS diamond_count,
    JSONExtractUInt(data, 'repeat_count')   AS repeat_count,
    toUInt8(JSONExtractBool(data, 'streaking'))   AS is_streaking,
    toUInt8(JSONExtractBool(data, 'repeat_end'))  AS is_repeat_end,
    JSONExtractFloat(data, 'usd_value')     AS usd_value
FROM tiktok.tiktok_kafka_source
WHERE event_type = 'gift';

CREATE MATERIALIZED VIEW IF NOT EXISTS tiktok.mv_comments
TO tiktok.tiktok_comments AS
SELECT
    parseDateTimeBestEffortOrNull(received_at) AS event_timestamp,
    streamer,
    category,
    if(archetype = '', 'general', archetype) AS archetype,
    JSONExtractString(data, 'user_id')   AS user_id,
    JSONExtractString(data, 'unique_id') AS unique_id,
    JSONExtractString(data, 'nickname')  AS nickname,
    JSONExtractString(data, 'comment')   AS comment_text
FROM tiktok.tiktok_kafka_source
WHERE event_type = 'comment';

CREATE MATERIALIZED VIEW IF NOT EXISTS tiktok.mv_likes
TO tiktok.tiktok_likes AS
SELECT
    parseDateTimeBestEffortOrNull(received_at)  AS event_timestamp,
    streamer,
    category,
    if(archetype = '', 'general', archetype) AS archetype,
    JSONExtractString(data, 'user_id')          AS user_id,
    JSONExtractString(data, 'unique_id')        AS unique_id,
    JSONExtractUInt(data, 'batch_count')        AS batch_count,
    JSONExtractUInt(data, 'total_room_likes')   AS total_room_likes
FROM tiktok.tiktok_kafka_source
WHERE event_type = 'like';

CREATE MATERIALIZED VIEW IF NOT EXISTS tiktok.mv_room_viewers
TO tiktok.tiktok_room_viewers AS
SELECT
    parseDateTimeBestEffortOrNull(received_at) AS event_timestamp,
    streamer,
    category,
    if(archetype = '', 'general', archetype) AS archetype,
    JSONExtractUInt(data, 'viewer_count')      AS viewer_count,
    JSONExtractUInt(data, 'popularity')        AS popularity
FROM tiktok.tiktok_kafka_source
WHERE event_type = 'room_users';

CREATE MATERIALIZED VIEW IF NOT EXISTS tiktok.mv_product_pins
TO tiktok.tiktok_product_pins AS
SELECT
    parseDateTimeBestEffortOrNull(received_at)  AS event_timestamp,
    streamer,
    category,
    if(archetype = '', 'general', archetype) AS archetype,
    JSONExtractString(data, 'product_id')       AS product_id,
    JSONExtractString(data, 'title')            AS title,
    JSONExtractString(data, 'price')            AS price,
    JSONExtractString(data, 'open_url')         AS open_url,
    JSONExtractUInt(data, 'live_product_count') AS live_product_count,
    JSONExtractInt(data, 'action_type')         AS action_type
FROM tiktok.tiktok_kafka_source
WHERE event_type = 'product_pin';


-- ============================================================
-- STEP 5: Gold Analytics Views (Behavioral & Retail Intelligence)
-- ============================================================

-- View 1: NPC Whale Concentration & Revenue Dependency
-- Measures whether a streamer relies on 1 whale or the general audience
CREATE OR REPLACE VIEW tiktok.v_npc_whale_analytics AS
SELECT
    streamer,
    archetype,
    count(DISTINCT unique_id) AS total_unique_gifters,
    sum(diamond_count * repeat_count) AS total_diamonds,
    max(user_diamonds) AS top_whale_diamonds,
    round(max(user_diamonds) / sum(diamond_count * repeat_count) * 100, 2) AS top_whale_revenue_share_pct
FROM (
    SELECT
        streamer,
        archetype,
        unique_id,
        sum(diamond_count * repeat_count) AS user_diamonds
    FROM tiktok.tiktok_gifts
    WHERE category = 'npc' AND is_streaking = 0
    GROUP BY streamer, archetype, unique_id
)
GROUP BY streamer, archetype
ORDER BY total_diamonds DESC;

-- View 2: NPC Streak & FOMO Multiplier Analysis
-- Measures average combo streak lengths and diamonds generated during combos
CREATE OR REPLACE VIEW tiktok.v_npc_streak_dynamics AS
SELECT
    streamer,
    archetype,
    gift_name,
    count() AS total_gift_events,
    avg(repeat_count) AS avg_combo_streak,
    max(repeat_count) AS max_combo_streak,
    sum(diamond_count * repeat_count) AS total_diamonds_generated
FROM tiktok.tiktok_gifts
WHERE category = 'npc' AND is_repeat_end = 1
GROUP BY streamer, archetype, gift_name
ORDER BY total_diamonds_generated DESC;

-- View 3: E-Commerce Cross-Category Retail Performance
-- Compares product pin velocity and viewer engagement across retail verticals
CREATE OR REPLACE VIEW tiktok.v_ecommerce_vertical_performance AS
SELECT
    archetype AS product_category,
    streamer,
    count(DISTINCT product_id) AS unique_products_showcased,
    count() AS total_pin_events,
    min(event_timestamp) AS session_start,
    max(event_timestamp) AS session_last_pin
FROM tiktok.tiktok_product_pins
WHERE category = 'ecommerce'
GROUP BY archetype, streamer
ORDER BY total_pin_events DESC;
