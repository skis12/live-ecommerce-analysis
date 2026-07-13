#!/bin/bash
# ============================================================
# Flink SQL 连接器下载脚本 (Flink 1.20, 在 hadoop01 上执行)
# 下载 Kafka + JDBC + MySQL 驱动，一次执行即可
# ============================================================

set -e
FLINK_HOME=${FLINK_HOME:-/opt/bigdata/flink}
FLINK_LIB=$FLINK_HOME/lib
JAVA_HOME=${JAVA_HOME:-/usr/local/jdk}

echo "=== Flink 1.20 SQL 连接器下载 ==="
echo "Flink: $FLINK_HOME"
echo "Java:  $JAVA_HOME"

# Flink SQL Kafka connector (匹配 Flink 1.20)
echo "[1/3] Kafka SQL Connector (3.3.0-1.20)..."
wget -q -O "$FLINK_LIB/flink-sql-connector-kafka-3.3.0-1.20.jar" \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" 2>/dev/null \
    && echo "  OK ($(ls -lh $FLINK_LIB/flink-sql-connector-kafka-3.3.0-1.20.jar | awk '{print $5}'))" \
    || echo "  FAIL"

# Flink JDBC connector
echo "[2/3] JDBC SQL Connector (3.3.0-1.20)..."
wget -q -O "$FLINK_LIB/flink-connector-jdbc-3.3.0-1.20.jar" \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar" 2>/dev/null \
    && echo "  OK ($(ls -lh $FLINK_LIB/flink-connector-jdbc-3.3.0-1.20.jar | awk '{print $5}'))" \
    || echo "  FAIL"

# MySQL JDBC driver
echo "[3/3] MySQL JDBC Driver (8.0.33)..."
wget -q -O "$FLINK_LIB/mysql-connector-j-8.0.33.jar" \
    "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" 2>/dev/null \
    && echo "  OK ($(ls -lh $FLINK_LIB/mysql-connector-j-8.0.33.jar | awk '{print $5}'))" \
    || echo "  FAIL"

echo ""
echo "=== 完成 ==="
echo "重启 Flink 集群使连接器生效:"
echo "  $FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh"
