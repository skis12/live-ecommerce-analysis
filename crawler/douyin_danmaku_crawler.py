"""
抖音直播弹幕+礼物采集器 - 基于 DouyinLiveWebFetcher
输出到 Kafka
"""
import sys, os, json, time, logging
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'DouyinLiveWebFetcher'))

from liveMan import DouyinLiveWebFetcher as BaseFetcher
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

ROOM_ID = '994154756317'
KAFKA = ['hadoop01:9092', 'hadoop02:9092', 'hadoop03:9092']


class KafkaDouyinFetcher(BaseFetcher):
    """继承原始类,覆盖消息处理方法,输出到Kafka"""

    def __init__(self, live_id):
        super().__init__(live_id)
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks=1, max_block_ms=3000
        )
        self.counts = {'chat': 0, 'gift': 0, 'like': 0, 'enter': 0, 'follow': 0, 'stats': 0}

    def _send(self, topic, data):
        try: self.producer.send(topic, value=data)
        except: pass

    def _parseChatMsg(self, payload):
        super()._parseChatMsg(payload)
        from protobuf.douyin import ChatMessage
        msg = ChatMessage().parse(payload)
        self._send('live_danmaku', {
            'room_id': ROOM_ID, 'username': msg.user.nick_name,
            'user_id': str(msg.user.id), 'text': msg.content,
            'timestamp': datetime.now().isoformat()
        })
        self.counts['chat'] += 1

    def _parseGiftMsg(self, payload):
        super()._parseGiftMsg(payload)
        from protobuf.douyin import GiftMessage
        msg = GiftMessage().parse(payload)
        self._send('live_gifts', {
            'room_id': ROOM_ID, 'username': msg.user.nick_name,
            'gift_name': msg.gift.name, 'gift_count': msg.combo_count,
            'timestamp': datetime.now().isoformat()
        })
        self.counts['gift'] += 1

    def _parseLikeMsg(self, payload):
        super()._parseLikeMsg(payload)
        from protobuf.douyin import LikeMessage
        msg = LikeMessage().parse(payload)
        self._send('live_danmaku', {
            'room_id': ROOM_ID, 'username': msg.user.nick_name,
            'text': f'[点赞] x{msg.count}', 'timestamp': datetime.now().isoformat()
        })
        self.counts['like'] += 1

    def _parseMemberMsg(self, payload):
        super()._parseMemberMsg(payload)
        self.counts['enter'] += 1

    def _parseSocialMsg(self, payload):
        super()._parseSocialMsg(payload)
        self.counts['follow'] += 1

    def _parseRoomUserSeqMsg(self, payload):
        super()._parseRoomUserSeqMsg(payload)
        from protobuf.douyin import RoomUserSeqMessage
        msg = RoomUserSeqMessage().parse(payload)
        self._send('live_room_info', {
            'room_id': ROOM_ID, 'online': str(msg.total),
            'total_users': str(msg.total_pv_for_anchor),
            'source': 'websocket', 'timestamp': datetime.now().isoformat()
        })
        self.counts['stats'] += 1

    def _wsOnOpen(self, ws):
        super()._wsOnOpen(ws)
        # Start stats printer
        import threading
        def printer():
            while True:
                time.sleep(30)
                total = sum(self.counts.values())
                if total > 0:
                    logger.info(f'弹幕:{self.counts["chat"]} 礼物:{self.counts["gift"]} '
                                f'点赞:{self.counts["like"]} 进场:{self.counts["enter"]} '
                                f'关注:{self.counts["follow"]} 统计:{self.counts["stats"]} 总计:{total}')
        threading.Thread(target=printer, daemon=True).start()

    def _wsOnClose(self, ws, *args):
        logger.info('WebSocket closed, reconnecting in 10s...')
        time.sleep(10)
        self.start()


if __name__ == '__main__':
    logger.info(f'抖音弹幕采集启动, 房间: {ROOM_ID}')
    fetcher = KafkaDouyinFetcher(ROOM_ID)
    try:
        fetcher.start()
    except KeyboardInterrupt:
        logger.info('Stopped')
