"""
Hive 数仓 ETL — MySQL → HDFS → Hive (ODS→DWD→DWS→ADS)
在 hadoop01 上以 soft01 执行
用法: python hive_etl.py [--full] [--incremental]
"""
import sys, os, pymysql, csv, time, logging
from datetime import datetime, timedelta
from io import StringIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, HDFS_BASE, HIVE_DB

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_CFG = DB_CONFIG

# Hive 建表 SQL
DDL = {
    'ods_room_info': f"""
        CREATE DATABASE IF NOT EXISTS {HIVE_DB};
        USE {HIVE_DB};
        CREATE EXTERNAL TABLE IF NOT EXISTS ods_room_info (
            room_id STRING, room_name STRING, title STRING, streamer STRING,
            online INT, like_count BIGINT, status STRING,
            has_cart BOOLEAN, has_commerce BOOLEAN,
            ts STRING
        ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE LOCATION '{HDFS_BASE}/ods/room_info';
    """,
    'ods_danmaku': f"""
        USE {HIVE_DB};
        CREATE EXTERNAL TABLE IF NOT EXISTS ods_danmaku (
            room_id STRING, room_name STRING, username STRING,
            text STRING, msg_type STRING,
            gift_name STRING, gift_count INT,
            ts STRING
        ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE LOCATION '{HDFS_BASE}/ods/danmaku';
    """,
    'dws_room_metrics': f"""
        USE {HIVE_DB};
        CREATE TABLE IF NOT EXISTS dws_room_minute_metrics (
            window_time STRING, room_id STRING, room_name STRING,
            danmaku_cnt INT, gift_cnt INT, online_avg INT,
            like_rate INT
        ) STORED AS ORC;
    """,
    'ads_user_segments': f"""
        USE {HIVE_DB};
        CREATE TABLE IF NOT EXISTS ads_user_segments (
            room_name STRING, segment STRING, user_count INT,
            dt STRING
        ) STORED AS ORC;
    """,
}


class HiveETL:
    def __init__(self, full_load=False):
        self.db = pymysql.connect(**DB_CFG)
        self.full_load = full_load
        self.today = datetime.now().strftime('%Y-%m-%d')

    def _hive_cmd(self, sql: str) -> str:
        """通过 beeline 执行 Hive SQL"""
        cmd = f'''beeline -u jdbc:hive2://localhost:10000 -e "{sql}" 2>/dev/null'''
        result = os.popen(cmd).read()
        return result

    def _hdfs_put(self, local_path: str, hdfs_path: str):
        """上传文件到 HDFS"""
        os.system(f'hdfs dfs -mkdir -p {hdfs_path} 2>/dev/null')
        # 全量则覆盖,增量则追加
        flag = '-f' if self.full_load else '-f'
        cmd = f'hdfs dfs -put {flag} {local_path} {hdfs_path}/'
        logger.debug(f'HDFS: {cmd}')
        os.system(cmd)

    def init_tables(self):
        """创建 Hive 表结构"""
        logger.info("Creating Hive tables...")
        for name, sql in DDL.items():
            result = self._hive_cmd(sql)
            if 'Error' in result:
                logger.error(f"DDL Error ({name}): {result[:200]}")
            else:
                logger.info(f"  {name} OK")

    def export_mysql_to_csv(self, table: str, columns: list, where: str = '') -> str:
        """从 MySQL 导出数据到临时 CSV"""
        cols = ', '.join(columns)
        sql = f"SELECT {cols} FROM {table}"
        if where: sql += f" WHERE {where}"
        sql += " ORDER BY id"

        cur = self.db.cursor()
        cur.execute(sql)

        tmpfile = f'/tmp/etl_{table}_{int(time.time())}.tsv'
        with open(tmpfile, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            for row in cur:
                writer.writerow([str(v) if v is not None else '' for v in row])

        cur.close()
        logger.info(f"  Exported {table}: {cur.rowcount} rows")
        return tmpfile

    def load_ods(self):
        """ODS 层: MySQL → HDFS → Hive External Table"""
        logger.info("=== ODS Layer ===")

        # room_info
        cols = ['room_id', 'room_name', 'title', 'streamer',
                'online', 'like_count', 'status', 'has_cart', 'has_commerce', 'timestamp']
        where = '' if self.full_load else f"created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)"
        f = self.export_mysql_to_csv('live_metrics', cols, where)
        if os.path.exists(f) and os.path.getsize(f) > 0:
            self._hdfs_put(f, f'{HDFS_BASE}/ods/room_info')
            os.remove(f)

        # danmaku
        cols = ['room_id', 'room_name', 'username', 'text', 'msg_type',
                'gift_name', 'gift_count', 'timestamp']
        where = '' if self.full_load else f"created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)"
        f = self.export_mysql_to_csv('danmaku', cols, where)
        if os.path.exists(f) and os.path.getsize(f) > 0:
            self._hdfs_put(f, f'{HDFS_BASE}/ods/danmaku')
            os.remove(f)

    def build_dws(self):
        """DWS 层: 分钟级聚合指标"""
        logger.info("=== DWS Layer ===")
        sql = f"""
            USE {HIVE_DB};
            INSERT OVERWRITE TABLE dws_room_minute_metrics
            SELECT
                from_unixtime(unix_timestamp(ts, 'yyyy-MM-dd''T''HH:mm:ss'), 'yyyy-MM-dd HH:mm:00') as window_time,
                room_id, room_name,
                COUNT(*) as danmaku_cnt,
                SUM(CASE WHEN msg_type='gift' THEN gift_count ELSE 0 END) as gift_cnt,
                CAST(AVG(online) AS INT) as online_avg,
                CAST(MAX(like_count) - MIN(like_count) AS INT) as like_rate
            FROM ods_room_info
            WHERE ts IS NOT NULL
            GROUP BY from_unixtime(unix_timestamp(ts, 'yyyy-MM-dd''T''HH:mm:ss'), 'yyyy-MM-dd HH:mm:00'),
                     room_id, room_name;
        """
        result = self._hive_cmd(sql)
        logger.info(f"DWS result: {result[:200] if result else 'OK'}")

    def build_ads(self):
        """ADS 层: 用户分群"""
        logger.info("=== ADS Layer ===")
        # 基于弹幕频率分群
        sql = f"""
            USE {HIVE_DB};
            INSERT OVERWRITE TABLE ads_user_segments
            SELECT
                room_name,
                CASE
                    WHEN msg_cnt >= 100 THEN '5-超级粉(100+)'
                    WHEN msg_cnt >= 50 THEN '4-铁杆粉(50-99)'
                    WHEN msg_cnt >= 10 THEN '3-忠实粉丝(10-49)'
                    WHEN msg_cnt >= 3 THEN '2-活跃观众(3-9)'
                    ELSE '1-路人(1-2)'
                END as segment,
                COUNT(*) as user_count,
                from_unixtime(unix_timestamp()) as dt
            FROM (
                SELECT room_name, username, COUNT(*) as msg_cnt
                FROM ods_danmaku WHERE msg_type='chat'
                GROUP BY room_name, username
            ) t
            GROUP BY room_name,
                CASE
                    WHEN msg_cnt >= 100 THEN '5-超级粉(100+)'
                    WHEN msg_cnt >= 50 THEN '4-铁杆粉(50-99)'
                    WHEN msg_cnt >= 10 THEN '3-忠实粉丝(10-49)'
                    WHEN msg_cnt >= 3 THEN '2-活跃观众(3-9)'
                    ELSE '1-路人(1-2)'
                END;
        """
        result = self._hive_cmd(sql)
        logger.info(f"ADS result: {result[:200] if result else 'OK'}")

    def run(self):
        t0 = time.time()
        logger.info(f"ETL Start ({'FULL' if self.full_load else 'INCREMENTAL'})")
        self.init_tables()
        self.load_ods()
        self.build_dws()
        self.build_ads()
        logger.info(f"ETL Done in {time.time()-t0:.1f}s")

    def close(self):
        self.db.close()


if __name__ == '__main__':
    full = '--full' in sys.argv
    etl = HiveETL(full_load=full)
    try:
        etl.run()
    finally:
        etl.close()
