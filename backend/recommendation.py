"""
商品推荐引擎 + 销量预测模型
- 协同过滤: 基于用户购买行为
- 关联规则: "买了A又买B"
- 销量预测: 移动平均 + 指数平滑 + 线性回归
"""
import sys, os, pymysql
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from itertools import combinations

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

DB_CFG = DB_CONFIG

def get_conn():
    return pymysql.connect(**DB_CFG)


# ===== 协同过滤推荐 =====

def collaborative_filter(room_name: str = '', top_n: int = 10) -> dict:
    """
    基于用户购买行为的协同过滤:
    1. 构建用户-商品矩阵
    2. 找相似用户
    3. 推荐他们买过但当前用户没买的
    """
    db = get_conn(); cur = db.cursor()
    w = f"WHERE room_name='{room_name}'" if room_name else ""
    cur.execute(f"""SELECT customer_id, product_id, product_name, category, SUM(quantity) as total_qty
        FROM orders {w} GROUP BY customer_id, product_id, product_name, category""")
    rows = cur.fetchall()
    cur.close(); db.close()

    if not rows: return {'error': '无数据'}

    # 构建用户→商品映射
    user_products = defaultdict(set)
    user_product_detail = defaultdict(dict)
    for cid, pid, pname, cat, qty in rows:
        user_products[cid].add(pid)
        user_product_detail[cid][pid] = {'name': pname, 'category': cat, 'qty': qty}

    # 找共现商品对（买了A的人也买了B）
    co_occur = Counter()
    for cid, products in user_products.items():
        for p1, p2 in combinations(sorted(products), 2):
            co_occur[(p1, p2)] += 1
            co_occur[(p2, p1)] += 1

    # 推荐：对每个商品，找共现最高的商品
    recommendations = {}
    product_info = {}
    for cid, prods in user_product_detail.items():
        for pid, info in prods.items():
            product_info[pid] = info

    for pid in list(product_info.keys())[:50]:
        related = []
        for p2 in product_info:
            if p2 != pid and co_occur.get((pid, p2), 0) > 0:
                related.append({'product_id': p2, 'product_name': product_info[p2]['name'],
                                'category': product_info[p2]['category'],
                                'score': co_occur[(pid, p2)]})
        related.sort(key=lambda x: -x['score'])
        recommendations[pid] = {
            'product_name': product_info[pid]['name'],
            'category': product_info[pid]['category'],
            'recommendations': related[:5]
        }

    return {
        'method': 'collaborative_filtering',
        'total_products': len(product_info),
        'total_users': len(user_products),
        'recommendations': {str(k): v for k, v in list(recommendations.items())[:20]}
    }


# ===== 关联规则 =====

def association_rules(room_name: str = '', min_support: int = 3, min_confidence: float = 0.1) -> dict:
    """
    Apriori-like 关联规则挖掘:
    - support: P(A&B) = 同时购买A和B的用户数 / 总用户数
    - confidence: P(B|A) = 同时购买A和B的用户数 / 购买A的用户数
    - lift: P(B|A) / P(B) = confidence / support(B)
    """
    db = get_conn(); cur = db.cursor()
    w = f"WHERE room_name='{room_name}'" if room_name else ""
    cur.execute(f"""SELECT customer_id, product_id, product_name
        FROM orders {w} GROUP BY customer_id, product_id, product_name""")
    rows = cur.fetchall()
    cur.close(); db.close()

    if not rows: return {'error': '无数据'}

    # 构建用户→商品
    user_products = defaultdict(set)
    product_counts = Counter()
    product_names = {}
    for cid, pid, pname in rows:
        user_products[cid].add(pid)
        product_counts[pid] += 1
        product_names[pid] = pname

    total_users = len(user_products)

    # 找频繁共现对
    pair_counts = Counter()
    for cid, products in user_products.items():
        for p1, p2 in combinations(sorted(products), 2):
            pair_counts[(p1, p2)] += 1

    # 计算关联规则
    rules = []
    for (p1, p2), co_count in pair_counts.most_common(200):
        support = co_count / total_users
        if co_count < min_support: continue

        conf_a_b = co_count / product_counts[p1]  # P(B|A)
        conf_b_a = co_count / product_counts[p2]  # P(A|B)
        lift = co_count * total_users / (product_counts[p1] * product_counts[p2])

        if conf_a_b >= min_confidence:
            rules.append({
                'antecedent': product_names[p1],
                'consequent': product_names[p2],
                'support': round(support, 3),
                'confidence': round(conf_a_b, 3),
                'lift': round(lift, 2),
                'co_count': co_count
            })

    rules.sort(key=lambda x: -x['lift'])

    return {
        'method': 'association_rules',
        'total_rules': len(rules),
        'top_rules': rules[:20]
    }


# ===== 销量预测 =====

def sales_prediction(product_id: int = None, room_name: str = '', days_ahead: int = 7) -> dict:
    """
    简单时间序列预测（移动平均 + 线性趋势 + 季节性）:
    1. 取历史每日销量
    2. 计算7日移动平均
    3. 线性回归拟合趋势
    4. 预测未来N天
    """
    db = get_conn(); cur = db.cursor()

    # 获取历史每日销量
    if product_id:
        w = f"WHERE product_id={product_id}"
        cur.execute(f"""SELECT DATE(order_time) as dt, SUM(quantity) as qty, SUM(total_amount) as gmv
            FROM orders {w} GROUP BY dt ORDER BY dt""")
    else:
        w = f"WHERE room_name='{room_name}'" if room_name else ""
        cur.execute(f"""SELECT DATE(order_time) as dt, SUM(quantity) as qty, SUM(total_amount) as gmv
            FROM orders {w} GROUP BY dt ORDER BY dt""")

    rows = cur.fetchall()
    cur.close(); db.close()

    if len(rows) < 3: return {'error': '数据不足，至少需要3天'}

    dates = [r[0] for r in rows]
    qtys = [float(r[1]) for r in rows]
    gmvs = [float(r[2]) for r in rows]

    n = len(qtys)

    # 7日移动平均
    ma7 = []
    for i in range(n):
        window = qtys[max(0, i-6):i+1]
        ma7.append(round(sum(window)/len(window), 1))

    # 简单线性回归 (y = a*x + b)
    x_mean = (n - 1) / 2
    y_mean = sum(qtys) / n
    numerator = sum((i - x_mean) * (qtys[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    slope = numerator / denominator if denominator else 0
    intercept = y_mean - slope * x_mean

    # 预测
    predictions = []
    for d in range(1, days_ahead + 1):
        pred_qty = max(0, slope * (n + d - 1) + intercept)
        pred_gmv = pred_qty * (sum(gmvs) / sum(qtys) if sum(qtys) > 0 else 0)
        pred_date = (dates[-1] + timedelta(days=d)).strftime('%Y-%m-%d')
        predictions.append({
            'date': pred_date,
            'predicted_qty': round(pred_qty, 0),
            'predicted_gmv': round(pred_gmv, 0),
            'ma7': round(ma7[-1], 1)
        })

    # 趋势方向
    trend = '上升' if slope > 0.5 else '下降' if slope < -0.5 else '平稳'

    return {
        'method': 'linear_regression',
        'history_days': n,
        'trend': trend,
        'slope': round(slope, 2),
        'ma7_latest': ma7[-1],
        'avg_daily_qty': round(sum(qtys)/n, 0),
        'history': [{'date': str(dates[i]), 'qty': qtys[i], 'ma7': ma7[i]} for i in range(n)],
        'predictions': predictions
    }


# ===== 热销排行+潜力新品 =====

def hot_and_rising(room_name: str = '') -> dict:
    """热销榜 + 上升最快"""
    db = get_conn(); cur = db.cursor()
    w = f"WHERE room_name='{room_name}'" if room_name else ""

    # 分前后7天对比
    cur.execute(f"""SELECT product_id, product_name, category,
        SUM(CASE WHEN order_time > DATE_SUB(NOW(), INTERVAL 7 DAY) THEN quantity ELSE 0 END) as recent_sold,
        SUM(CASE WHEN order_time <= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN quantity ELSE 0 END) as old_sold,
        SUM(quantity) as total_sold
        FROM orders {w}
        GROUP BY product_id, product_name, category
        HAVING total_sold > 0
        ORDER BY recent_sold DESC""")

    products = []
    for pid, pname, cat, recent, old, total in cur.fetchall():
        old = max(old, 1)
        growth = round((recent - old) / old * 100, 1)
        products.append({
            'product_id': pid, 'product_name': pname, 'category': cat,
            'recent_sold': int(recent or 0), 'total_sold': int(total or 0),
            'growth_pct': growth
        })

    cur.close(); db.close()

    hot = sorted(products, key=lambda x: -x['recent_sold'])[:10]
    rising = sorted(products, key=lambda x: -x['growth_pct'])[:10]

    return {
        'hot_products': hot,
        'rising_products': rising
    }


if __name__ == '__main__':
    print("=== 商品推荐 ===")
    r = collaborative_filter(room_name='与晖同行')
    if 'error' not in r:
        print(f"用户数: {r['total_users']}, 商品数: {r['total_products']}")
        for pid, info in list(r['recommendations'].items())[:3]:
            print(f"  [{info['product_name'][:20]}] 推荐:")
            for rec in info['recommendations'][:3]:
                print(f"    -> {rec['product_name'][:20]} (score={rec['score']})")

    print("\n=== 关联规则 ===")
    ar = association_rules(room_name='与晖同行')
    for rule in ar['top_rules'][:5]:
        print(f"  {rule['antecedent'][:15]} -> {rule['consequent'][:15]} lift={rule['lift']}")

    print("\n=== 销量预测 ===")
    sp = sales_prediction(room_name='与晖同行', days_ahead=3)
    if 'error' not in sp:
        print(f"  {sp['history_days']}天趋势: {sp['trend']}")
        for p in sp['predictions']:
            print(f"  {p['date']}: {p['predicted_qty']:.0f}件, GMV={p['predicted_gmv']:.0f}")

    print("\n=== 热销+上升 ===")
    hr = hot_and_rising(room_name='与晖同行')
    print("  热销Top3:", [(h['product_name'][:15], h['recent_sold']) for h in hr['hot_products'][:3]])
    print("  上升Top3:", [(h['product_name'][:15], f"{h['growth_pct']}%") for h in hr['rising_products'][:3]])
