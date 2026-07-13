"""
B站多直播间高速采集 - 15房间并行,2秒间隔
"""
import json, time, logging, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

H = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://live.bilibili.com/'}
ROOM_URL = 'https://api.live.bilibili.com/room/v3/area/getRoomList'
KAFKA = ['hadoop01:9092', 'hadoop02:9092', 'hadoop03:9092']
AREA_IDS = [0, 1, 3, 4, 5, 9]  # 全站+娱乐+网游+手游+单机+生活


class FastBilibiliCrawler:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=KAFKA,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks=1, max_block_ms=3000, compression_type='gzip')
        self.count = 0
        self.room_cache = {}  # 缓存房间详情

    def _fetch_area(self, aid):
        """拿一个分区的房间列表"""
        try:
            params = {'platform': 'web', 'parent_area_id': aid, 'area_id': 0,
                      'sort_type': 'online', 'page_size': 5, 'page': 1}
            resp = requests.get(ROOM_URL, params=params, headers=H, timeout=5)
            return resp.json().get('data', {}).get('list', resp.json().get('data', []))
        except:
            return []

    def _fetch_room_detail(self, rid):
        """拿单个房间详情"""
        try:
            resp = requests.get(
                f'https://api.live.bilibili.com/room/v1/Room/get_info?room_id={rid}',
                headers=H, timeout=5)
            d = resp.json()
            return d.get('data', {}).get('room_info', {})
        except:
            return {}

    def fetch_all(self):
        """并行拉取所有分区,合并去重"""
        rooms = []
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(self._fetch_area, aid): aid for aid in AREA_IDS}
            for f in as_completed(futures):
                data = f.result()
                if isinstance(data, list):
                    for r in data:
                        if r.get('online', 0) > 500:
                            rooms.append(r)

        # 去重排序
        seen = set()
        unique = []
        for r in sorted(rooms, key=lambda x: -x.get('online', 0)):
            rid = str(r['roomid'])
            if rid not in seen:
                seen.add(rid)
                unique.append(r)
        return unique[:15]

    def push_to_kafka(self, rooms):
        """推送房间数据到Kafka"""
        ts = datetime.now().isoformat()
        batch = []
        for r in rooms:
            data = {
                'room_id': str(r['roomid']),
                'streamer': r.get('uname', ''),
                'title': r.get('title', ''),
                'online': r.get('online', 0),
                'area': r.get('area_name', ''),
                'source': 'bilibili',
                'timestamp': ts
            }
            batch.append(data)
            self.producer.send('live_room_info', value=data)

        self.count += len(batch)
        top = rooms[0] if rooms else {}
        logger.info(f'[{self.count}] {len(batch)}房间 | TOP1:{top.get(\"uname\",\"\")} '
                    f'热度:{top.get(\"online\",0):,} | {top.get(\"title\",\"\")[:20]}')

    def run(self):
        logger.info(f'B站高速采集启动 (并行{len(AREA_IDS)}区,2秒间隔)')
        while True:
            t0 = time.time()
            rooms = self.fetch_all()
            if rooms:
                self.push_to_kafka(rooms)
            elapsed = time.time() - t0
            sleep_time = max(0.5, 2 - elapsed)
            time.sleep(sleep_time)


if __name__ == '__main__':
    FastBilibiliCrawler().run()
