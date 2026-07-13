"""
抖音直播全量数据采集 v3 — 多房间并行, 1秒间隔
支持: 影视飓风 + 与晖同行, 自动处理直播结束
"""
import sys, os, json, time, logging, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_BROKERS, ROOMS, DOUYIN_COOKIE

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOMS_LIST = [{'id': rid, 'name': name} for rid, name in ROOMS.items()]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Cookie': DOUYIN_COOKIE or os.getenv('DOUYIN_COOKIE', '')
}
API_URL = 'https://live.douyin.com/webcast/room/web/enter/?aid=6383&app_name=douyin_web&live_id=1&device_platform=web'


class DouyinCrawler:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks=0, compression_type='gzip', max_block_ms=2000,
            linger_ms=5, batch_size=32768
        )
        self.counts = {r['name']: 0 for r in ROOMS_LIST}
        self.offline_count = {r['name']: 0 for r in ROOMS_LIST}
        self.total = 0

    def fetch_room(self, room: dict):
        """采集单个房间"""
        rid, name = room['id'], room['name']
        try:
            resp = requests.get(f'{API_URL}&web_rid={rid}', headers=HEADERS, timeout=5)
            d = resp.json()

            if d.get('status_code') != 0:
                self.offline_count[name] += 1
                if self.offline_count[name] <= 3:
                    logger.warning(f'[{name}] API返回异常: status_code={d.get("status_code")}')
                return

            data_list = d.get('data', {}).get('data', [])
            if not data_list:
                self.offline_count[name] += 1
                return

            r = data_list[0] if isinstance(data_list, list) else {}
            if not r:
                self.offline_count[name] += 1
                return

            self.offline_count[name] = 0  # 恢复正常

            ts = datetime.now().isoformat()
            owner = r.get('owner', {})
            stats = r.get('stats', {})
            cart = r.get('room_cart', {})
            auth = r.get('room_auth', {})

            data = {
                'room_id': rid,
                'room_name': name,
                'title': r.get('title', ''),
                'streamer': owner.get('nickname', ''),
                'online': r.get('user_count_str', ''),
                'like_count': r.get('like_count', 0),
                'status': '直播中' if r.get('status') == 2 else '未开播',
                'status_code': r.get('status', 0),
                'has_cart': cart.get('contain_cart', False),
                'cart_total': cart.get('total', 0),
                'total_users': stats.get('total_user_str', ''),
                'can_danmaku': auth.get('Danmaku', False),
                'can_gift': auth.get('Gift', False),
                'has_commerce': r.get('has_commerce_goods', False),
                'timestamp': ts
            }
            self.producer.send('live_room_info', value=data)
            self.counts[name] += 1
            self.total += 1

        except requests.exceptions.Timeout:
            self.offline_count[name] += 1
        except requests.exceptions.ConnectionError:
            self.offline_count[name] += 1
        except Exception as e:
            logger.debug(f'[{name}] Error: {e}')

    def run(self):
        logger.info(f'启动采集: {len(ROOMS_LIST)} 个房间, 1秒间隔')
        for r in ROOMS_LIST:
            logger.info(f'  {r["name"]}: https://live.douyin.com/{r["id"]}')

        while True:
            t0 = time.time()

            # 并行采集所有房间
            with ThreadPoolExecutor(max_workers=len(ROOMS_LIST)) as pool:
                list(pool.map(self.fetch_room, ROOMS_LIST))

            # 每30次输出统计
            if self.total % 30 == 0:
                parts = []
                for name, cnt in self.counts.items():
                    off = self.offline_count.get(name, 0)
                    mark = ' ⚠离线' if off > 5 else ''
                    parts.append(f'{name}:{cnt}{mark}')
                logger.info(f'[{self.total}] {", ".join(parts)}')

            # 确保采集间隔不小于1秒
            elapsed = time.time() - t0
            if elapsed < 1:
                time.sleep(1 - elapsed)


if __name__ == '__main__':
    DouyinCrawler().run()
