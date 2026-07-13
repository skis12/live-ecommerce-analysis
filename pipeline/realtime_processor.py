"""
Python 实时处理 — Kafka消费 + 窗口聚合 + CEP异常告警 (Flink 模拟版)
注意: 这不是真正的 Flink，是 KafkaConsumer + deque + 阈值判断实现的。
真正的 Flink SQL 作业见 flink_jobs/ 目录。
计算: GMV预估, 在线波动, 弹幕速率, 异常检测
"""
import sys, os, json, time, logging, threading
from collections import defaultdict, deque
from datetime import datetime
from kafka import KafkaConsumer
import pymysql

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, KAFKA_BROKERS, ROOMS, ALERT_THRESHOLDS

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

BOOTSTRAP = KAFKA_BROKERS
DB_CFG = DB_CONFIG
ROOM_NAMES = ROOMS


class RoomState:
    """单个房间的状态管理（Flink KeyedState 等价）"""
    def __init__(self, room_id: str, room_name: str):
        self.room_id = room_id
        self.room_name = room_name
        self.online_history = deque(maxlen=10)  # 最近10次采样
        self.danmaku_count = 0
        self.gift_count = 0
        self.gift_value = 0
        self.last_online = 0
        self.last_likes = 0
        self.alerts = deque(maxlen=20)

    def update_metrics(self, data: dict):
        """更新房间指标"""
        online_str = str(data.get('online', '0')).replace('+', '').replace('w', '0000').replace('万', '0000')
        try:
            online = int(float(online_str))
        except:
            return

        self.online_history.append(online)
        self.last_online = online
        self.last_likes = data.get('like_count', 0)

    def update_danmaku(self):
        self.danmaku_count += 1

    def update_gift(self, gift_name: str, gift_count: int):
        self.gift_count += gift_count
        # 礼物估值（简单映射）
        gift_prices = {'小心心': 0.1, '大啤酒': 1, '棒棒糖': 0.5, '告白气球': 5,
                       '私人飞机': 50, '跑车': 100, '火箭': 500, '嘉年华': 3000}
        self.gift_value += gift_prices.get(gift_name, 1) * gift_count

    def check_anomalies(self) -> list:
        """CEP 异常检测"""
        alerts = []
        if len(self.online_history) < 3:
            return alerts

        recent = list(self.online_history)
        current = recent[-1]
        baseline = sum(recent[:-1]) / max(1, len(recent) - 1)

        # 1. 在线人数骤降
        if baseline > 100 and current < baseline * (1 - ALERT_THRESHOLDS['online_drop_pct'] / 100):
            alerts.append({
                'type': 'online_drop', 'severity': 'HIGH',
                'msg': f'{self.room_name} 在线骤降: {baseline:.0f}→{current}',
                'current': current, 'baseline': round(baseline),
                'time': datetime.now().isoformat()
            })

        # 2. 在线人数飙涨
        if baseline > 100 and current > baseline * (1 + ALERT_THRESHOLDS['online_spike_pct'] / 100):
            alerts.append({
                'type': 'online_spike', 'severity': 'MEDIUM',
                'msg': f'{self.room_name} 在线暴涨: {baseline:.0f}→{current}',
                'current': current, 'baseline': round(baseline),
                'time': datetime.now().isoformat()
            })

        return alerts

    def snapshot(self) -> dict:
        """返回当前快照"""
        ol = list(self.online_history)
        return {
            'room_id': self.room_id, 'room_name': self.room_name,
            'online_current': ol[-1] if ol else 0,
            'online_avg': round(sum(ol)/max(1,len(ol))),
            'online_trend': 'up' if len(ol)>=2 and ol[-1]>ol[-2] else 'down' if len(ol)>=2 else 'stable',
            'danmaku_count': self.danmaku_count,
            'gift_value': round(self.gift_value, 2),
            'gmv_estimate': round(self.danmaku_count * 0.03 * 150 + self.gift_value, 2),
            'alerts': list(self.alerts)
        }


class RealtimeProcessor:
    """Flink 风格实时处理主类"""

    def __init__(self):
        self.states = {}
        self.window_start = time.time()
        self.db = pymysql.connect(**DB_CFG)
        self.cursor = self.db.cursor()
        self._init_db()

        # Kafka consumers
        self.consumer = KafkaConsumer(
            'live_room_info', 'live_danmaku', 'live_gifts',
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='realtime-processor-v2',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        # 启动告警推送线程
        self.alert_callback = None

    def _init_db(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS live_realtime_metrics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            room_id VARCHAR(30), room_name VARCHAR(50),
            online_current INT, online_avg INT, online_trend VARCHAR(10),
            danmaku_rate INT, gift_value DECIMAL(10,2), gmv_estimate DECIMAL(14,2),
            alert_count INT, window_start VARCHAR(30),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS live_alerts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            room_name VARCHAR(50), alert_type VARCHAR(30),
            severity VARCHAR(10), message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        self.db.commit()

    def get_state(self, room_id: str, room_name: str) -> RoomState:
        if room_id not in self.states:
            self.states[room_id] = RoomState(room_id, room_name or ROOM_NAMES.get(room_id, room_id))
        return self.states[room_id]

    def run(self):
        logger.info("Realtime Processor started (Flink-style)")
        logger.info(f"Alerts: online drop>{ALERT_THRESHOLDS['online_drop_pct']}%, spike>{ALERT_THRESHOLDS['online_spike_pct']}%")

        for msg in self.consumer:
            try:
                d = msg.value
                topic = msg.topic
                rid = d.get('room_id', '')
                rname = d.get('room_name', '') or ROOM_NAMES.get(rid, '')
                state = self.get_state(rid, rname)

                if topic == 'live_room_info':
                    state.update_metrics(d)

                elif topic == 'live_danmaku':
                    state.update_danmaku()

                elif topic == 'live_gifts':
                    state.update_gift(d.get('gift_name', ''), d.get('gift_count', 1))

                # 异常检测
                new_alerts = state.check_anomalies()
                for alert in new_alerts:
                    state.alerts.append(alert)
                    # 存入数据库
                    self.cursor.execute("INSERT INTO live_alerts (room_name, alert_type, severity, message) VALUES (%s,%s,%s,%s)",
                        (rname, alert['type'], alert['severity'], alert['msg']))
                    self.db.commit()
                    logger.warning(f"[ALERT] {alert['msg']}")

                # 每60秒输出窗口结果
                if time.time() - self.window_start >= 60:
                    for rid, st in self.states.items():
                        snap = st.snapshot()
                        self.cursor.execute("""INSERT INTO live_realtime_metrics
                            (room_id, room_name, online_current, online_avg, online_trend,
                             danmaku_rate, gift_value, gmv_estimate, alert_count, window_start)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (snap['room_id'], snap['room_name'], snap['online_current'],
                             snap['online_avg'], snap['online_trend'],
                             snap['danmaku_count'], snap['gift_value'], snap['gmv_estimate'],
                             len(snap['alerts']), datetime.fromtimestamp(self.window_start).isoformat()))
                        self.db.commit()

                        logger.info(f"[{snap['room_name']}] 在线:{snap['online_current']} "
                                    f"弹幕:{snap['danmaku_count']} "
                                    f"GMV预估:¥{snap['gmv_estimate']:.0f} "
                                    f"趋势:{snap['online_trend']} "
                                    f"告警:{len(snap['alerts'])}")

                    # 重置窗口
                    self.window_start = time.time()
                    for st in self.states.values():
                        st.danmaku_count = 0
                        st.gift_count = 0
                        st.gift_value = 0

            except Exception as e:
                logger.error(f"Process error: {e}")

    def close(self):
        self.db.commit()
        self.cursor.close()
        self.db.close()
        self.consumer.close()


if __name__ == '__main__':
    processor = RealtimeProcessor()
    try:
        processor.run()
    except KeyboardInterrupt:
        logger.info("Stopped")
        processor.close()
