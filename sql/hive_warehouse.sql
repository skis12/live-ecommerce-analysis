-- 直播数据分析系统 - Hive数仓
-- ODS → DWD → DWS → ADS 四层架构

-- ===== ODS层: 原始数据 =====
CREATE DATABASE IF NOT EXISTS live_ods;
USE live_ods;

CREATE EXTERNAL TABLE IF NOT EXISTS ods_room_info (
    room_id STRING, title STRING, streamer STRING,
    online INT, likes BIGINT, fans BIGINT, live_status INT,
    timestamp STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS ods_danmaku (
    room_id STRING, uid BIGINT, username STRING, text STRING,
    is_vip BOOLEAN, timestamp STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS ods_gifts (
    room_id STRING, uid BIGINT, username STRING, gift_name STRING,
    gift_count INT, price DOUBLE, timestamp STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS ods_orders (
    room_id STRING, product_name STRING, price DOUBLE,
    quantity INT, user_id BIGINT, timestamp STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

-- ===== DWS层: 汇总指标 =====
CREATE DATABASE IF NOT EXISTS live_dws;
USE live_dws;

CREATE TABLE IF NOT EXISTS dws_live_minute_metrics (
    window_time STRING, room_id STRING,
    danmaku_cnt INT, gift_amount DOUBLE, order_amount DOUBLE,
    order_cnt INT, active_users INT, gmv DOUBLE
) STORED AS ORC;

-- ===== ADS层: 应用指标 =====
CREATE DATABASE IF NOT EXISTS live_ads;
USE live_ads;

CREATE TABLE IF NOT EXISTS ads_product_ranking AS
SELECT product_name, SUM(quantity) total_qty,
       SUM(price * quantity) total_amount
FROM live_ods.ods_orders
GROUP BY product_name
ORDER BY total_amount DESC;
