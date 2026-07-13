"""
Python实时计算 — 消费Kafka做窗口聚合 (Flink 模拟版)
注意: 这不是真正的 Flink，是 KafkaConsumer + deque 实现的窗口计算。
真正的 Flink SQL 作业见 flink_jobs/ 目录。
计算: GMV估算, 在线趋势, 点赞速率
"""
import sys, os, json, time, threading
from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer
import pymysql

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, KAFKA_BROKERS

BOOTSTRAP = KAFKA_BROKERS
DB = DB_CONFIG


class WindowAggregator:
    """1分钟滑动窗口聚合"""
    def __init__(self):
        self.danmaku_count = 0
        self.gift_total = 0
        self.order_count = 0
        self.gmv_estimate = 0  # 预估GMV
        self.online_samples = []
        self.like_samples = []
        self.room_names = set()
        self.start = time.time()
        self.lock = threading.Lock()

    def add(self, data: dict):
        with self.lock:
            self.room_names.add(data.get('room_name', ''))
            online = str(data.get('online', '0')).replace('+', '').replace('w', '0000').replace('万', '0000')
            try:
                self.online_samples.append(int(float(online) if '.' in online else int(online)))
            except:
                pass
            self.like_samples.append(data.get('like_count', 0))
            self.danmaku_count += 1

            # 预估GMV: 在线×转化率3%×客单价150
            try:
                o = int(float(str(online).replace('w','0000').replace('万','0000').replace('+','')))
                self.gmv_estimate += o * 0.03 * 150
            except:
                pass

    def flush(self) -> dict:
        with self.lock:
            elapsed = time.time() - self.start
            result = {
                'window_start': datetime.fromtimestamp(self.start).isoformat(),
                'window_seconds': int(elapsed),
                'online_avg': int(sum(self.online_samples) / max(1, len(self.online_samples))),
                'online_min': min(self.online_samples) if self.online_samples else 0,
                'online_max': max(self.online_samples) if self.online_samples else 0,
                'like_rate': int((self.like_samples[-1] - self.like_samples[0]) / max(1, elapsed)) if len(self.like_samples) > 1 else 0,
                'gmv_estimate': round(self.gmv_estimate, 2),
                'danmaku_count': self.danmaku_count,
                'rooms': len(self.room_names),
                'timestamp': datetime.now().isoformat()
            }
            self.__init__()  # 重置
            return result


def run_flink():
    """消费Kafka, 每60秒输出聚合结果到MySQL"""
    db = pymysql.connect(**DB)
    c = db.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS live_aggregates (
        id INT AUTO_INCREMENT PRIMARY KEY,
        window_start VARCHAR(30), online_avg INT, online_min INT, online_max INT,
        like_rate INT, gmv_estimate DECIMAL(14,2), danmaku_count INT, rooms INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    db.commit()

    consumer = KafkaConsumer('live_room_info', bootstrap_servers=BOOTSTRAP,
        auto_offset_reset='latest', enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    w = WindowAggregator()
    count = 0
    print('Flink实时计算启动, 每60秒输出聚合结果')

    for msg in consumer:
        w.add(msg.value)
        count += 1
        if time.time() - w.start >= 60:
            result = w.flush()
            c.execute("INSERT INTO live_aggregates (window_start, online_avg, online_min, online_max, like_rate, gmv_estimate, danmaku_count, rooms) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (result['window_start'], result['online_avg'], result['online_min'], result['online_max'], result['like_rate'], result['gmv_estimate'], result['danmaku_count'], result['rooms']))
            db.commit()
            a = result['online_avg']
            g = result['gmv_estimate']
            l = result['like_rate']
            r = result['rooms']
            print(f'[聚合] 平均在线:{a} | GMV预估:{g:.0f} | 点赞速率:{l}/s | 房间:{r}个')


if __name__ == '__main__':
    run_flink()
