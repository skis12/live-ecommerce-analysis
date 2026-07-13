#!/bin/bash
# 直播数据分析系统 - 一键启动 (hadoop01上以root执行)
export JAVA_HOME=/usr/local/jdk
export HADOOP_HOME=/usr/local/hadoop-3.5.0
export HIVE_HOME=/opt/bigdata/hive
export FLINK_HOME=/opt/bigdata/flink
export KAFKA_HOME=/opt/kafka/kafka_2.13-3.9.2
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin:$FLINK_HOME/bin:$KAFKA_HOME/bin:$PATH
export HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root YARN_NODEMANAGER_USER=root

echo "=========================================="
echo "  直播数据分析系统 - 一键启动"
echo "=========================================="

# 1. MySQL
echo "[1/7] MySQL..."
systemctl start mysqld 2>/dev/null
sleep 2
mysql -u root -p"Xy@123456" -e "CREATE DATABASE IF NOT EXISTS live_analytics CHARACTER SET utf8mb4" 2>/dev/null
echo "OK"

# 2. Hadoop HDFS
echo "[2/7] Hadoop HDFS..."
start-dfs.sh 2>/dev/null
sleep 5
echo "OK"

# 3. ZooKeeper + Kafka
echo "[3/7] ZooKeeper + Kafka..."
systemctl start zookeeper kafka 2>/dev/null
sleep 5
echo "OK"

# 4. Hive Metastore + HiveServer2
echo "[4/7] Hive..."
su - soft01 -c "export JAVA_HOME=/usr/local/jdk HADOOP_HOME=/usr/local/hadoop-3.5.0 HIVE_HOME=/opt/bigdata/hive PATH=/usr/local/jdk/bin:/usr/local/hadoop-3.5.0/bin:/opt/bigdata/hive/bin:/usr/bin:/bin && nohup hive --service metastore > /dev/null 2>&1 &"
sleep 3
su - soft01 -c "export JAVA_HOME=/usr/local/jdk HADOOP_HOME=/usr/local/hadoop-3.5.0 HIVE_HOME=/opt/bigdata/hive PATH=/usr/local/jdk/bin:/usr/local/hadoop-3.5.0/bin:/opt/bigdata/hive/bin:/usr/bin:/bin && nohup hive --service hiveserver2 > /dev/null 2>&1 &"
echo "OK"

# 5. Flink
echo "[5/7] Flink..."
systemctl start flink-jobmanager flink-taskmanager 2>/dev/null
for h in hadoop02 hadoop03; do ssh $h "systemctl start flink-taskmanager 2>/dev/null" & done
sleep 3
echo "OK"

# 6. Hive ETL (仅首次全量, 后续可注释掉用增量)
echo "[6/7] Hive ETL (增量)..."
su - soft01 -c "export JAVA_HOME=/usr/local/jdk HADOOP_HOME=/usr/local/hadoop-3.5.0 HIVE_HOME=/opt/bigdata/hive PATH=/usr/local/jdk/bin:/usr/local/hadoop-3.5.0/bin:/opt/bigdata/hive/bin:/usr/bin:/bin && python3 /home/soft01/hive_etl.py" 2>/dev/null &
echo "OK"

echo "[7/7] 完成!"
echo ""
echo "  大屏:        http://hadoop01:8002"
echo "  企业大屏:    http://hadoop01:8002/enterprise"
echo "  Flink WebUI: http://hadoop01:8081"
echo "  HDFS:        http://hadoop01:9870"
echo "=========================================="
