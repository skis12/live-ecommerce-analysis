"""
抖音多直播间同时采集 - 各自独立WebSocket
"""
import sys, os, json, time, logging, threading
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'DouyinLiveWebFetcher'))

from liveMan import DouyinLiveWebFetcher as BaseFetcher
from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_BROKERS, ROOMS

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 多直播间配置
ROOMS_LIST = [{'id': rid, 'name': name} for rid, name in ROOMS.items()]

KAFKA = KAFKA_BROKERS


class MultiRoomFetcher(BaseFetcher):
    """继承原类, 覆盖消息处理"""

    def __init__(self, live_id, room_name):
        super().__init__(live_id)
        self.room_name = room_name
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks=1, max_block_ms=2000
        )
        self.counts = {'chat': 0, 'gift': 0, 'member': 0, 'stats': 0}

    def _send(self, topic, data):
        data['room_name'] = self.room_name
        data['room_id'] = self.live_id
        try: self.producer.send(topic, value=data)
        except: pass

    def _parseChatMsg(self, payload):
        super()._parseChatMsg(payload)
        from protobuf.douyin import ChatMessage
        msg = ChatMessage().parse(payload)
        self._send('live_danmaku', {
            'username': msg.user.nick_name, 'user_id': str(msg.user.id),
            'text': msg.content, 'timestamp': datetime.now().isoformat()
        })
        self.counts['chat'] += 1

    def _parseGiftMsg(self, payload):
        super()._parseGiftMsg(payload)
        from protobuf.douyin import GiftMessage
        msg = GiftMessage().parse(payload)
        self._send('live_gifts', {
            'username': msg.user.nick_name, 'gift_name': msg.gift.name,
            'gift_count': msg.combo_count, 'timestamp': datetime.now().isoformat()
        })
        self.counts['gift'] += 1

    def _parseMemberMsg(self, payload):
        super()._parseMemberMsg(payload)
        self.counts['member'] += 1

    def _parseRoomUserSeqMsg(self, payload):
        super()._parseRoomUserSeqMsg(payload)
        from protobuf.douyin import RoomUserSeqMessage
        msg = RoomUserSeqMessage().parse(payload)
        self._send('live_room_info', {
            'online': str(msg.total), 'total_users': str(msg.total_pv_for_anchor),
            'source': 'websocket', 'timestamp': datetime.now().isoformat()
        })
        self.counts['stats'] += 1


def run_room(room):
    """在独立线程中运行一个房间的采集, 断线自动重连"""
    target_dir = os.path.join(os.path.dirname(__file__), 'DouyinLiveWebFetcher')
    retry = 0
    while True:
        try:
            os.chdir(target_dir)
            logger.info(f'[{room["name"]}] 启动采集... (重试#{retry})' if retry else f'[{room["name"]}] 启动采集...')
            retry += 1
            fetcher = MultiRoomFetcher(room['id'], room['name'])
            fetcher.start()
            # start() 正常返回 = 连接断开, 等5秒重连
            logger.warning(f'[{room["name"]}] 连接断开, 5秒后重连...')
            time.sleep(5)
        except Exception as e:
            logger.error(f'[{room["name"]}] 异常: {e}, 10秒后重连...')
            time.sleep(10)


def stats_printer(fetchers):
    """定期输出统计"""
    while True:
        time.sleep(30)
        for f in fetchers[:]:
            if f.counts:
                total = sum(f.counts.values())
                if total > 0:
                    logger.info(f'[{f.room_name}] 弹幕:{f.counts["chat"]} '
                                f'礼物:{f.counts["gift"]} 进场:{f.counts["member"]} '
                                f'统计:{f.counts["stats"]} 总计:{total}')


if __name__ == '__main__':
    logger.info(f'多直播间采集启动: {len(ROOMS_LIST)}个房间')
    fetchers = []
    for room in ROOMS_LIST:
        t = threading.Thread(target=run_room, args=(room,), daemon=True)
        t.start()
        time.sleep(5)  # 错开启动,避免同时签名计算
        logger.info(f'等待下一个房间...')

    # 主线程打印统计
    while True:
        time.sleep(30)
        total_all = 0
        for f in fetchers[:]:
            if hasattr(f, 'counts'):
                total_all += sum(f.counts.values())
        logger.info(f'[总计] 全房间累计: {total_all}条')
