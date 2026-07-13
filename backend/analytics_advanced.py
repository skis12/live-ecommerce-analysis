"""
高级分析模块
- RFM 用户分群 (Recency/Frequency/Monetary)
- 漏斗转化分析 (进直播间→发弹幕→送礼物→重复互动)
- 用户留存率分析 (Day1/3/7)
"""
import sys, os, pymysql
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

DB_CFG = DB_CONFIG

def get_conn():
    return pymysql.connect(**DB_CFG)


# ===== RFM 用户分群 =====

def rfm_analysis(room_name: str, min_messages: int = 3) -> dict:
    """
    直播场景 RFM:
    - Recency: 距离最后一次发弹幕的小时数 (越小越好)
    - Frequency: 总弹幕数 (越多越好)
    - Monetary: 对于直播，用互动活跃度(弹幕频率/观看时长替代)
    """
    db = get_conn(); cur = db.cursor()

    # 计算每个用户的 RFM
    cur.execute("""
        SELECT username,
               COUNT(*) as freq,
               MAX(created_at) as last_time,
               MIN(created_at) as first_time,
               COUNT(DISTINCT DATE(created_at)) as active_days
        FROM danmaku WHERE room_name=%s AND msg_type='chat' AND username IS NOT NULL
        GROUP BY username HAVING COUNT(*) >= %s
        ORDER BY freq DESC LIMIT 5000
    """, (room_name, min_messages))
    rows = cur.fetchall()
    cur.close(); db.close()

    if not rows:
        return {'error': '无数据'}

    now = datetime.now()
    users = []
    for username, freq, last_time, first_time, active_days in rows:
        recency = max(0, (now - last_time).total_seconds() / 3600)  # 小时
        # lifespan = 活跃天数
        lifespan = max(1, (last_time - first_time).days if first_time else 1)
        # engagement = 频率/活跃天数 (日平均互动)
        engagement = round(freq / max(1, active_days), 1)
        users.append({
            'username': username,
            'recency': round(recency, 1),
            'frequency': freq,
            'engagement': engagement,
            'active_days': active_days
        })

    # 计算 R/F 中位数用于划分
    recencies = sorted([u['recency'] for u in users])
    freqs = sorted([u['frequency'] for u in users])
    n = len(users)
    r_med = recencies[n // 2]
    f_med = freqs[n // 2]

    # RFM 分群
    groups = {
        '核心粉丝(R低F高)': [], '新晋活跃(R低F低)': [],
        '沉睡用户(R高F高)': [], '流失风险(R高F低)': []
    }
    for u in users:
        r_high = u['recency'] <= r_med
        f_high = u['frequency'] >= f_med
        if r_high and f_high: g = '核心粉丝(R低F高)'
        elif r_high and not f_high: g = '新晋活跃(R低F低)'
        elif not r_high and f_high: g = '沉睡用户(R高F高)'
        else: g = '流失风险(R高F低)'
        groups[g].append(u)

    return {
        'total_users': n,
        'r_median': round(r_med, 1),
        'f_median': f_med,
        'groups': {k: {'count': len(v), 'top3': v[:3]} for k, v in groups.items()},
        'top_users': users[:20]
    }


# ===== 漏斗转化分析 =====

def funnel_analysis(room_name: str) -> dict:
    """
    直播间转化漏斗:
    1. 进过直播间(有记录) → 2. 发过弹幕 → 3. 发过≥5条弹幕 → 4. 活跃用户(≥10条)
    """
    db = get_conn(); cur = db.cursor()

    # 总独立用户数 (从 danmaku 表)
    cur.execute("SELECT COUNT(DISTINCT username) as c FROM danmaku WHERE room_name=%s", (room_name,))
    total = cur.fetchone()[0] or 1

    # 发过弹幕的
    cur.execute("SELECT COUNT(DISTINCT username) as c FROM danmaku WHERE room_name=%s AND msg_type='chat'", (room_name,))
    chatters = cur.fetchone()[0] or 0

    # 发过 >=5 条
    cur.execute("""SELECT COUNT(*) as c FROM (
        SELECT username FROM danmaku WHERE room_name=%s AND msg_type='chat'
        GROUP BY username HAVING COUNT(*) >= 5) t""", (room_name,))
    active5 = cur.fetchone()[0] or 0

    # 发过 >=10 条 (高活跃)
    cur.execute("""SELECT COUNT(*) as c FROM (
        SELECT username FROM danmaku WHERE room_name=%s AND msg_type='chat'
        GROUP BY username HAVING COUNT(*) >= 10) t""", (room_name,))
    active10 = cur.fetchone()[0] or 0

    # 发过礼物的
    cur.execute("SELECT COUNT(DISTINCT username) as c FROM danmaku WHERE room_name=%s AND msg_type='gift'", (room_name,))
    gifters = cur.fetchone()[0] or 0

    # 高活跃且送礼物的
    cur.execute("""SELECT COUNT(DISTINCT a.username) FROM
        (SELECT username FROM danmaku WHERE room_name=%s AND msg_type='chat' GROUP BY username HAVING COUNT(*) >= 10) a
        INNER JOIN
        (SELECT DISTINCT username FROM danmaku WHERE room_name=%s AND msg_type='gift') b
        ON a.username = b.username""", (room_name, room_name))
    vip = cur.fetchone()[0] or 0

    cur.close(); db.close()

    steps = [
        {'name': '独立观众', 'value': total, 'rate': 100},
        {'name': '发弹幕互动', 'value': chatters, 'rate': round(chatters/max(1,total)*100, 1)},
        {'name': '活跃互动(≥5条)', 'value': active5, 'rate': round(active5/max(1,total)*100, 1)},
        {'name': '高活跃(≥10条)', 'value': active10, 'rate': round(active10/max(1,total)*100, 1)},
        {'name': '送礼用户', 'value': gifters, 'rate': round(gifters/max(1,total)*100, 1)},
        {'name': '核心用户(高活跃+送礼)', 'value': vip, 'rate': round(vip/max(1,total)*100, 1)},
    ]
    return {'room': room_name, 'funnel': steps}


# ===== 留存率分析 =====

def retention_analysis(room_name: str) -> dict:
    """
    用户留存: 首次出现后 Day1/Day3/Day7 还回来的比例
    """
    db = get_conn(); cur = db.cursor()

    # 获取每个用户的首次出现日期
    cur.execute("""
        SELECT username, MIN(DATE(created_at)) as first_day
        FROM danmaku WHERE room_name=%s AND msg_type='chat' AND username IS NOT NULL
        GROUP BY username
    """, (room_name,))
    user_first = {row[0]: row[1] for row in cur.fetchall()}

    # 获取每个用户所有活跃日期
    cur.execute("""
        SELECT username, DATE(created_at) as active_day
        FROM danmaku WHERE room_name=%s AND msg_type='chat' AND username IS NOT NULL
        GROUP BY username, DATE(created_at)
    """, (room_name,))
    user_days = defaultdict(set)
    for username, day in cur.fetchall():
        user_days[username].add(day)

    cur.close(); db.close()

    # 计算留存
    def calc_retention(day_offset: int) -> dict:
        retained, total = 0, 0
        for username, first_day in user_first.items():
            total += 1
            target = first_day + timedelta(days=day_offset)
            if target in user_days[username]:
                retained += 1
        return {'retained': retained, 'total': total, 'rate': round(retained/max(1,total)*100, 1)}

    # Daily retention for first 14 days
    daily = []
    for d in range(1, 15):
        r = calc_retention(d)
        daily.append({'day': d, 'retained': r['retained'], 'rate': r['rate']})

    return {
        'room': room_name,
        'total_users': len(user_first),
        'day1': calc_retention(1)['rate'],
        'day3': calc_retention(3)['rate'],
        'day7': calc_retention(7)['rate'],
        'daily': daily
    }


# ===== 用户旅程数据 =====

def user_journey(room_name: str, limit: int = 5) -> list:
    """
    抽样用户的行为时间线 (首次→活跃→高潮→最近)
    """
    db = get_conn(); cur = db.cursor()

    # 随机取活跃用户
    cur.execute("""
        SELECT username FROM danmaku WHERE room_name=%s AND msg_type='chat'
        GROUP BY username HAVING COUNT(*) >= 10 ORDER BY RAND() LIMIT %s
    """, (room_name, limit))
    users = [row[0] for row in cur.fetchall()]

    journeys = []
    for username in users:
        cur.execute("""SELECT text, msg_type, gift_name, gift_count, created_at
            FROM danmaku WHERE room_name=%s AND username=%s
            ORDER BY id LIMIT 20""", (room_name, username))
        events = []
        for text, msg_type, gift_name, gift_count, ts in cur.fetchall():
            if msg_type == 'gift':
                events.append({'type': 'gift', 'detail': f'{gift_name} x{gift_count}', 'time': str(ts)})
            else:
                events.append({'type': 'chat', 'detail': text[:30], 'time': str(ts)})

        stats = {
            'total_messages': len(events),
            'first': str(events[0]['time']) if events else '',
            'last': str(events[-1]['time']) if events else ''
        }
        journeys.append({'username': username, 'stats': stats, 'timeline': events[:10]})

    cur.close(); db.close()
    return journeys


if __name__ == '__main__':
    print("=== RFM 分析 ===")
    r = rfm_analysis('影视飓风')
    if 'error' not in r:
        for g, info in r['groups'].items():
            print(f"  {g}: {info['count']}人")
        print(f"  Top: {[u['username'] for u in r['top_users'][:5]]}")

    print("\n=== 漏斗分析 ===")
    f = funnel_analysis('影视飓风')
    for s in f['funnel']:
        print(f"  {s['name']}: {s['value']} ({s['rate']}%)")

    print("\n=== 留存率 ===")
    ret = retention_analysis('影视飓风')
    print(f"  Day1: {ret['day1']}%  Day3: {ret['day3']}%  Day7: {ret['day7']}%")
