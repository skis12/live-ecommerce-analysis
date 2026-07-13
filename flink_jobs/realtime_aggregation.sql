-- ============================================================
-- Flink SQL 实时聚合作业 (Flink 1.20 工作版)
-- Kafka → Event Time + Watermark + TUMBLE窗口 → JDBC → MySQL
--
-- 部署: sql-client.sh -f realtime_aggregation.sql
-- 注意: TO_TIMESTAMP 使用显式格式串是因为 ISO 8601 的 'T' 分隔符
--       Flink 默认不认，必须显式指定。
-- ============================================================

-- 源表: 弹幕数据 (Kafka)
CREATE TABLE kafka_danmaku (
    room_id     STRING,
    room_name   STRING,
    `timestamp` STRING,                                      -- JSON 字段名为 timestamp (SQL 保留字，需反引号)
    event_time  AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),  -- ISO 8601 格式
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND                -- 允许5秒乱序
) WITH (
    'connector' = 'kafka',
    'topic' = 'live_danmaku',
    'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
    'properties.group.id' = 'flink-production',
    'scan.startup.mode' = 'latest-offset',                   -- 实时: 只消费新数据
--  'scan.startup.mode' = 'earliest-offset',                 -- 回放: 消费全部历史数据
--  'scan.bounded.mode' = 'latest-offset',                   -- 回放: 读完历史数据后停止
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 结果表: MySQL (JDBC Sink)
CREATE TABLE mysql_flink_metrics (
    window_start  STRING,
    window_end    STRING,
    room_id       STRING,
    room_name     STRING,
    danmaku_cnt   BIGINT,
    PRIMARY KEY (window_start, room_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://hadoop01:3306/live_analytics',
    'table-name' = 'live_flink_metrics',
    'username' = 'root',
    'password' = 'Xy@123456',
    'sink.buffer-flush.max-rows' = '10',
    'sink.buffer-flush.interval' = '10s'
);

-- 核心: Event Time + 1分钟 TUMBLE 窗口 + 直接写 MySQL
INSERT INTO mysql_flink_metrics
SELECT
    CAST(TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS STRING) AS window_start,
    CAST(TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS STRING)   AS window_end,
    room_id,
    room_name,
    COUNT(*)                                                        AS danmaku_cnt
FROM kafka_danmaku
GROUP BY
    TUMBLE(event_time, INTERVAL '1' MINUTE),
    room_id,
    room_name;
