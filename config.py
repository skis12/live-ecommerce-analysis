"""
统一配置模块 — 所有子模块从此导入
环境变量优先级 > 默认值（默认值仅用于本地开发VM集群）
生产环境务必通过环境变量覆盖密码等敏感信息
"""
import os

# ============================================================
# MySQL 数据库
# ============================================================
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'hadoop01'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'Xy@123456'),
    'database': os.getenv('MYSQL_DATABASE', 'live_analytics'),
    'charset': 'utf8mb4',
}

# ============================================================
# Kafka
# ============================================================
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'hadoop01:9092,hadoop02:9092,hadoop03:9092').split(',')

KAFKA_TOPICS = {
    'room_info': 'live_room_info',
    'danmaku': 'live_danmaku',
    'gifts': 'live_gifts',
}

# ============================================================
# Hive / HDFS (仅 hive_etl.py 使用)
# ============================================================
HIVE_DB = os.getenv('HIVE_DB', 'live_dw')
HDFS_BASE = os.getenv('HDFS_BASE', '/user/hive/warehouse/live')

# ============================================================
# 直播间配置
# ============================================================
ROOMS = {
    '994154756317': '影视飓风',
    '646454278948': '与晖同行',
}

# ============================================================
# 抖音直播 Cookie（会过期，需定期通过浏览器重新获取）
# ============================================================
# DOUYIN_COOKIE 从环境变量读取，或从 crawler/douyin_cookie.txt 读取
_cookie_file = os.path.join(os.path.dirname(__file__), 'crawler', 'douyin_cookie.txt')
_default_cookie = ''
if os.path.exists(_cookie_file):
    with open(_cookie_file, 'r') as f:
        _default_cookie = f.read().strip()
DOUYIN_COOKIE = os.getenv('DOUYIN_COOKIE', _default_cookie)

# ============================================================
# FastAPI / JWT
# ============================================================
SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'live-analytics-pro-2026')
API_PORT = int(os.getenv('API_PORT', '8002'))

# ============================================================
# 实时处理阈值
# ============================================================
ALERT_THRESHOLDS = {
    'online_drop_pct': 30,
    'online_spike_pct': 50,
    'danmaku_surge': 100,
    'gmv_increase_pct': 100,
}
