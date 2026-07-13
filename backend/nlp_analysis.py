"""
弹幕 NLP 分析模块
- SnowNLP 情感分析 (0~1, >0.6=正面, <0.4=负面)
- jieba 分词 + TF-IDF 关键词提取
- 情感四象限：高/低活跃 × 正面/负面
"""
import sys, os, pymysql, time, logging, re
from collections import Counter, defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# TTL缓存（避免Dashboard每次刷新都跑NLP，5秒刷新太频繁）
_cache = {}
CACHE_TTL = 30  # 30秒缓存

def _cached(key, fn, *args):
    now = time.time()
    if key in _cache and now - _cache[key]['ts'] < CACHE_TTL:
        return _cache[key]['data']
    data = fn(*args)
    _cache[key] = {'ts': now, 'data': data}
    return data

DB_CFG = DB_CONFIG

# 停用词
STOP_WORDS = set('''
的 了 在 是 我 有 和 就 不 人 都 一 一个 上 也 很 到 说 要 去 你
会 着 没有 看 好 自己 这 那 他 她 它 们 什么 怎么 哪 为什么 吗
吧 呢 啊 哦 呀 哈 啦 呗 咦 喂 嘛 哇 嗯 哎 哟 呵 嘿 哼 得 地
个 但 是 为 所以 因为 如果 虽然 然而 而且 或者 不过 还 就 才
又 再 也 都 已经 正在 将 要 可以 能 会 应该 必须 可能 一定
大家 知道 觉得 真的 可以 这样 那个 这个 怎么 看到 一下 啥 没
'''.split())

try:
    from snownlp import SnowNLP
    HAS_SNOWNLP = True
except ImportError:
    HAS_SNOWNLP = False
    logger.warning("snownlp not installed, sentiment analysis disabled")

try:
    import jieba
    import jieba.analyse
    HAS_JIEBA = True
except ImportError:
    HAS_JIEBA = False
    logger.warning("jieba not installed, keyword extraction disabled")


def get_conn():
    return pymysql.connect(**DB_CFG)


def analyze_sentiment(text: str) -> dict:
    """单条弹幕情感分析"""
    if not HAS_SNOWNLP or not text:
        return {'score': 0.5, 'label': '中性'}
    try:
        s = SnowNLP(text)
        score = s.sentiments
        if score > 0.6: label = '正面'
        elif score < 0.4: label = '负面'
        else: label = '中性'
        return {'score': round(score, 3), 'label': label}
    except:
        return {'score': 0.5, 'label': '中性'}


def analyze_room_sentiment(room_name: str, limit: int = 5000) -> dict:
    return _cached(f'sent_{room_name}_{limit}', _analyze_room_sentiment, room_name, limit)

def _analyze_room_sentiment(room_name: str, limit: int = 5000) -> dict:
    """整个直播间的情感分布"""
    db = get_conn(); cur = db.cursor()
    cur.execute("SELECT text FROM danmaku WHERE room_name=%s AND msg_type='chat' AND text IS NOT NULL ORDER BY id DESC LIMIT %s", (room_name, limit))
    rows = cur.fetchall()
    cur.close(); db.close()

    if not rows: return {'error': '无数据'}
    positive, negative, neutral = 0, 0, 0
    scores = []
    for (text,) in rows:
        s = analyze_sentiment(text)
        scores.append(s['score'])
        if s['label'] == '正面': positive += 1
        elif s['label'] == '负面': negative += 1
        else: neutral += 1

    total = len(rows)
    return {
        'total': total, 'positive': positive, 'negative': negative, 'neutral': neutral,
        'positive_rate': round(positive/total*100, 1),
        'negative_rate': round(negative/total*100, 1),
        'neutral_rate': round(neutral/total*100, 1),
        'avg_score': round(sum(scores)/len(scores), 3),
        'score_distribution': {
            '0.0-0.2(极度负面)': sum(1 for s in scores if s < 0.2),
            '0.2-0.4(较为负面)': sum(1 for s in scores if 0.2 <= s < 0.4),
            '0.4-0.6(中性)': sum(1 for s in scores if 0.4 <= s < 0.6),
            '0.6-0.8(较为正面)': sum(1 for s in scores if 0.6 <= s < 0.8),
            '0.8-1.0(非常正面)': sum(1 for s in scores if s >= 0.8)
        }
    }


def extract_keywords(room_name: str, limit: int = 2000, topk: int = 50) -> list:
    return _cached(f'kw_{room_name}_{limit}', _extract_keywords, room_name, limit, topk)

def _extract_keywords(room_name: str, limit: int = 2000, topk: int = 50) -> list:
    """提取关键词 (jieba TF-IDF)"""
    if not HAS_JIEBA: return []
    db = get_conn(); cur = db.cursor()
    cur.execute("SELECT text FROM danmaku WHERE room_name=%s AND msg_type='chat' AND text IS NOT NULL ORDER BY id DESC LIMIT %s", (room_name, limit))
    rows = cur.fetchall()
    cur.close(); db.close()

    all_text = '\n'.join(text for (text,) in rows if text and len(text) >= 2)
    if not all_text: return []

    # jieba TF-IDF
    keywords = jieba.analyse.extract_tags(all_text, topK=topk, withWeight=True)
    return [{'word': w, 'weight': round(weight, 3)} for w, weight in keywords if w not in STOP_WORDS and len(w) >= 2]


def sentiment_timeline(room_name: str, interval_min: int = 5, periods: int = 24) -> list:
    """情感时序变化（每5分钟一个窗口）"""
    db = get_conn(); cur = db.cursor()
    results = []
    for i in range(periods):
        start = f"NOW() - INTERVAL {(i+1)*interval_min} MINUTE"
        end = f"NOW() - INTERVAL {i*interval_min} MINUTE"
        cur.execute("""SELECT text FROM danmaku WHERE room_name=%s AND msg_type='chat'
            AND text IS NOT NULL AND created_at BETWEEN %s AND %s""",
            (room_name, start, end))
        rows = cur.fetchall()
        if rows:
            scores = []
            for (text,) in rows:
                s = analyze_sentiment(text)
                scores.append(s['score'])
            results.append({
                'time': datetime.now().strftime('%H:%M'),
                'avg_sentiment': round(sum(scores)/len(scores), 3),
                'count': len(rows)
            })
    cur.close(); db.close()
    return list(reversed(results))


def sentiment_quadrant(room_name: str, limit: int = 1000) -> dict:
    return _cached(f'quad_{room_name}_{limit}', _sentiment_quadrant, room_name, limit)

def _sentiment_quadrant(room_name: str, limit: int = 1000) -> dict:
    """情感四象限分析：
    - 高活跃+正面：忠实粉丝
    - 高活跃+负面：情绪激动用户（需安抚）
    - 低活跃+正面：路人好感
    - 低活跃+负面：流失风险
    """
    db = get_conn(); cur = db.cursor()
    cur.execute("""SELECT username, COUNT(*) as cnt, GROUP_CONCAT(text SEPARATOR ' ') as texts
        FROM danmaku WHERE room_name=%s AND msg_type='chat' AND text IS NOT NULL
        GROUP BY username HAVING cnt >= 2 ORDER BY cnt DESC LIMIT %s""", (room_name, limit))
    rows = cur.fetchall()
    cur.close(); db.close()

    quadrant = {'忠实粉丝': 0, '情绪用户': 0, '路人好感': 0, '流失风险': 0}
    samples = {k: [] for k in quadrant}

    for username, cnt, texts in rows:
        s = analyze_sentiment(texts or '')
        if cnt >= 10 and s['label'] == '正面':
            q = '忠实粉丝'
        elif cnt >= 10 and s['label'] == '负面':
            q = '情绪用户'
        elif cnt < 10 and s['label'] in ('正面', '中性'):
            q = '路人好感'
        else:
            q = '流失风险'
        quadrant[q] += 1
        if len(samples[q]) < 3: samples[q].append({'name': username, 'cnt': cnt, 'sentiment': s['score']})

    return {'quadrant': quadrant, 'samples': samples}


def word_cloud_data(room_name: str, limit: int = 3000) -> list:
    return _cached(f'wc_{room_name}_{limit}', _word_cloud_data, room_name, limit)

def _word_cloud_data(room_name: str, limit: int = 3000) -> list:
    """生成词云数据（词频统计）"""
    if not HAS_JIEBA: return []
    db = get_conn(); cur = db.cursor()
    cur.execute("SELECT text FROM danmaku WHERE room_name=%s AND msg_type='chat' AND text IS NOT NULL ORDER BY id DESC LIMIT %s", (room_name, limit))
    rows = cur.fetchall()
    cur.close(); db.close()

    word_count = Counter()
    for (text,) in rows:
        if not text or len(text) < 2: continue
        words = jieba.cut(text)
        for w in words:
            w = w.strip()
            if len(w) >= 2 and w not in STOP_WORDS and not re.match(r'^[0-9\s\.\,\?\!"\';:：　]+$', w):
                word_count[w] += 1

    # 取 top 100
    result = [{'name': w, 'value': c} for w, c in word_count.most_common(100) if c >= 2]
    return result


if __name__ == '__main__':
    # 测试
    print("=== 影视飓风 情感分析 ===")
    r = analyze_room_sentiment('影视飓风', limit=500)
    if 'error' not in r:
        print(f"正面: {r['positive_rate']}% | 负面: {r['negative_rate']}% | 平均分: {r['avg_score']}")

    print("\n=== 关键词 Top10 ===")
    kw = extract_keywords('影视飓风', limit=500)
    for k in kw[:10]:
        print(f"  {k['word']}: {k['weight']}")

    print("\n=== 四象限 ===")
    sq = sentiment_quadrant('影视飓风', limit=500)
    for k, v in sq['quadrant'].items():
        print(f"  {k}: {v}人")
