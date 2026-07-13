"""
Kafka消费者 v3 — 弹幕+礼物+房间数据 → MySQL + 实时聚合
消费: live_danmaku, live_gifts, live_room_info
写入: danmaku, room_stats, live_metrics, live_aggregates
"""
import sys, os, pymysql, json, logging, time, threading
from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, KAFKA_BROKERS, ROOMS

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_CFG = DB_CONFIG
BOOTSTRAP = KAFKA_BROKERS
ROOM_NAMES = ROOMS


class WindowAggregator:
    """60秒滑动窗口实时聚合"""
    def __init__(self):
        self.rooms = defaultdict(lambda: {'online': [], 'likes': [], 'danmaku': 0})
        self.start = time.time()

    def add(self, room_id: str, room_name: str, online: int = 0):
        r = self.rooms[room_id]
        r['room_name'] = room_name
        if online > 0: r['online'].append(online)
        r['danmaku'] += 1

    def flush(self):
        elapsed = max(1, time.time() - self.start)
        results = []
        for rid, r in self.rooms.items():
            onlines = r['online']
            results.append({
                'room_id': rid, 'room_name': r.get('room_name', ''),
                'online_avg': sum(onlines) // max(1, len(onlines)),
                'online_max': max(onlines) if onlines else 0,
                'online_min': min(onlines) if onlines else 0,
                'danmaku_rate': int(r['danmaku'] / elapsed * 60),
                'window_start': datetime.fromtimestamp(self.start).isoformat()
            })
        self.__init__()
        return results


class LiveDataPipeline:
    def __init__(self):
        self.db = pymysql.connect(**DB_CFG)
        self.cursor = self.db.cursor()
        self.counts = defaultdict(int)
        self.aggregator = WindowAggregator()
        self.total = 0

        # 确保表存在
        self._init_tables()

        # Kafka消费者 - 订阅3个topic
        self.consumer = KafkaConsumer(
            'live_danmaku', 'live_gifts', 'live_room_info',
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='pipeline-consumer-v3',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def _init_tables(self):
        c = self.cursor
        c.execute("""CREATE TABLE IF NOT EXISTS live_aggregates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            room_id VARCHAR(30), room_name VARCHAR(50),
            online_avg INT, online_max INT, online_min INT,
            danmaku_rate INT, window_start VARCHAR(30),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        # 给 live_metrics 补 room_name 列（如果不存在）
        try:
            c.execute("ALTER TABLE live_metrics ADD COLUMN room_name VARCHAR(50) AFTER room_id")
        except: pass
        self.db.commit()

    def process(self):
        logger.info(f"Pipeline启动, 等待Kafka数据...")
        for msg in self.consumer:
            try:
                d = msg.value
                topic = msg.topic
                rid = d.get('room_id', '')
                rname = d.get('room_name', '') or ROOM_NAMES.get(rid, rid)
                ts = d.get('timestamp', datetime.now().isoformat())

                if topic == 'live_danmaku':
                    self.cursor.execute("""INSERT INTO danmaku (room_id, room_name, username, user_id, text, msg_type, timestamp)
                        VALUES (%s,%s,%s,%s,%s,'chat',%s)""",
                        (rid, rname[:50], d.get('username', '')[:100], str(d.get('user_id', '')),
                         d.get('text', '')[:500], ts))
                    self.counts['danmaku'] += 1
                    self.aggregator.add(rid, rname)

                elif topic == 'live_gifts':
                    self.cursor.execute("""INSERT INTO danmaku (room_id, room_name, username, gift_name, gift_count, msg_type, timestamp)
                        VALUES (%s,%s,%s,%s,%s,'gift',%s)""",
                        (rid, rname[:50], d.get('username', '')[:100],
                         d.get('gift_name', ''), d.get('gift_count', 1), ts))
                    self.counts['gift'] += 1

                elif topic == 'live_room_info':
                    src = d.get('source', 'api')
                    if src == 'websocket':
                        # WebSocket精确统计
                        self.cursor.execute("""INSERT INTO room_stats (room_id, room_name, online_count, total_users, timestamp)
                            VALUES (%s,%s,%s,%s,%s)""",
                            (rid, rname[:50], int(d.get('online', 0)), str(d.get('total_users', '')), ts))
                    else:
                        # HTTP API全量数据
                        self.cursor.execute("""INSERT INTO live_metrics (room_id, room_name, title, streamer, online, like_count,
                            status, has_cart, cart_total, has_commerce, timestamp)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (rid, rname[:50], d.get('title', '')[:200], d.get('streamer', '')[:100],
                             d.get('online', ''), d.get('like_count', 0), d.get('status', d.get('status_str', '')),
                             1 if d.get('has_cart') else 0, d.get('cart_total', 0),
                             1 if d.get('has_commerce') else 0, ts))
                    self.counts['stats'] += 1
                    # 同时更新聚合器
                    try:
                        online_num = int(str(d.get('online', '0')).replace('+', '').replace('w', '0000').replace('万', '0000'))
                        self.aggregator.add(rid, rname, online_num)
                    except: pass

                # 每100条提交
                self.total += 1
                if self.total % 100 == 0:
                    self.db.commit()

                # 每60秒输出聚合
                if time.time() - self.aggregator.start >= 60:
                    for agg in self.aggregator.flush():
                        self.cursor.execute("""INSERT INTO live_aggregates (room_id, room_name, online_avg, online_max, online_min, danmaku_rate, window_start)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                            (agg['room_id'], agg['room_name'], agg['online_avg'], agg['online_max'],
                             agg['online_min'], agg['danmaku_rate'], agg['window_start']))
                    self.db.commit()

                # 每1000条日志
                if self.total % 1000 == 0:
                    logger.info(f"[{self.total}] 弹幕:{self.counts['danmaku']} 礼物:{self.counts['gift']} 统计:{self.counts['stats']}")

            except Exception as e:
                logger.error(f"Process error: {e}")
                try: self.db.ping(reconnect=True)
                except: pass

    def close(self):
        self.db.commit()
        self.cursor.close()
        self.db.close()
        self.consumer.close()


if __name__ == '__main__':
    pipeline = LiveDataPipeline()
    try:
        pipeline.process()
    except KeyboardInterrupt:
        logger.info(f"Stopped. Total: {pipeline.total}")
        pipeline.close()
