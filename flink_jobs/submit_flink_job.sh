#!/bin/bash
# ============================================================
# Flink SQL 作业提交脚本 (Flink 1.20)
# 用法: bash submit_flink_job.sh [earliest|latest]
#   latest  — 只处理新数据 (实时模式)
#   earliest — 回放全部历史数据 (默认)
# ============================================================

FLINK_HOME=${FLINK_HOME:-/opt/bigdata/flink}
JAVA_HOME=${JAVA_HOME:-/usr/local/jdk}
SQL_DIR="$(cd "$(dirname "$0")" && pwd)"
MODE=${1:-latest}

echo "=== Flink SQL 实时聚合 提交 ==="
echo "Flink: $FLINK_HOME"
echo "SQL:   $SQL_DIR/realtime_aggregation.sql"
echo "Mode:  $MODE"

# 创建 MySQL 结果表
mysql -u root -p"Xy@123456" -e "
CREATE TABLE IF NOT EXISTS live_analytics.live_flink_metrics (
    window_start VARCHAR(30),
    window_end   VARCHAR(30),
    room_id      VARCHAR(30),
    room_name    VARCHAR(100),
    danmaku_cnt  BIGINT DEFAULT 0,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, room_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
" 2>/dev/null && echo "MySQL 表就绪" || echo "MySQL 连接失败"

echo ""

if [ "$MODE" = "earliest" ]; then
    echo "模式: 回放全部历史数据 (earliest-offset + bounded)"
    # 回放模式: 修改 scan.startup.mode 为 earliest-offset, 并启用 bounded mode
    sed -i "s/'scan.startup.mode' = 'latest-offset'/'scan.startup.mode' = 'earliest-offset'/" "$SQL_DIR/realtime_aggregation.sql"
    sed -i "s/--  'scan.bounded.mode'/'scan.bounded.mode'/" "$SQL_DIR/realtime_aggregation.sql"
else
    echo "模式: 实时消费新数据 (latest-offset)"
    # 确保是 latest-offset
    sed -i "s/'scan.startup.mode' = 'earliest-offset'/'scan.startup.mode' = 'latest-offset'/" "$SQL_DIR/realtime_aggregation.sql"
    sed -i "s/'scan.bounded.mode' = 'latest-offset'/--  'scan.bounded.mode' = 'latest-offset'/" "$SQL_DIR/realtime_aggregation.sql"
fi

echo ""
echo "提交到 Flink 集群..."

# 提交作业 (sql-client 会自己启动内嵌 Gateway)
$FLINK_HOME/bin/sql-client.sh -f "$SQL_DIR/realtime_aggregation.sql"

echo ""
echo "=== 提交完成 ==="
echo "查看作业: http://hadoop01:8081"
echo "查看结果: mysql> SELECT * FROM live_analytics.live_flink_metrics ORDER BY window_start DESC LIMIT 10;"
