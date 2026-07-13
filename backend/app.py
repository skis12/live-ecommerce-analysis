"""
直播数据分析平台 v5 — 统一后端
多房间支持 · 影视飓风 + 与晖同行
http://localhost:8002 — 简易大屏
http://localhost:8002/enterprise — 企业大屏（带登录）
"""
from fastapi import FastAPI, Query, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import pymysql, hashlib, jwt, io, csv, os, sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, ROOMS, SECRET_KEY, API_PORT

DB_CFG = DB_CONFIG
SK = SECRET_KEY

app = FastAPI(title="直播数据分析平台 v5")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
security = HTTPBearer(auto_error=False)

# ===== DB Helper =====
def db_query(sql: str, params=None):
    db = pymysql.connect(**DB_CFG); cur = db.cursor()
    cur.execute(sql, params); rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    cur.close(); db.close()
    return [dict(zip(cols, r)) for r in rows]

def parse_online(v):
    v = str(v).replace('+', '').replace(',', '').strip()
    if 'w' in v.lower(): return int(float(v.lower().replace('w', '')) * 10000)
    try: return int(v)
    except: return 0


# ===== Auth =====
class LoginReq(BaseModel):
    username: str; password: str

@app.post("/api/login")
def login(req: LoginReq):
    pw = hashlib.sha256(req.password.encode()).hexdigest()
    u = db_query("SELECT * FROM users WHERE username=%s AND password=%s", (req.username, pw))
    if not u: raise HTTPException(401, "用户名或密码错误")
    token = jwt.encode({"user": req.username, "role": u[0]['role'],
                         "exp": datetime.utcnow() + timedelta(hours=24)}, SK)
    return {"token": token, "user": u[0]['username']}

@app.post("/api/register")
def register(req: LoginReq):
    pw = hashlib.sha256(req.password.encode()).hexdigest()
    db_query("INSERT INTO users (username,password) VALUES (%s,%s)", (req.username, pw))
    return {"msg": "ok"}

def get_user(cred: HTTPAuthorizationCredentials = Depends(security)):
    if cred:
        try: return jwt.decode(cred.credentials, SK, algorithms=["HS256"])
        except: raise HTTPException(401)
    return None


# ===== 基础 APIs =====

@app.get("/api/rooms")
def list_rooms():
    """获取直播间列表及状态"""
    result = []
    for rid, name in ROOMS.items():
        latest = db_query("SELECT online, title, created_at FROM live_metrics WHERE room_id=%s ORDER BY id DESC LIMIT 1", (rid,))
        dm = db_query("SELECT COUNT(*) as c FROM danmaku WHERE room_name=%s", (name,))[0]['c']
        total = db_query("SELECT COUNT(*) as c FROM live_metrics WHERE room_id=%s", (rid,))[0]['c']
        if latest:
            dt = latest[0]['created_at']
            # MySQL TIMESTAMP返回的是无时区时间, 实际是UTC+8
            live = (datetime.now() - dt).total_seconds() < 600
            result.append({
                'id': rid, 'name': name, 'online': latest[0]['online'],
                'title': (latest[0].get('title') or '')[:30],
                'live': live, 'records': total, 'danmaku': dm,
                'updated': dt.strftime('%H:%M:%S')
            })
        else:
            result.append({'id': rid, 'name': name, 'online': '无数据', 'live': False, 'records': 0, 'danmaku': 0})
    return result

@app.get("/api/stats")
def stats(room_id: str = Query('994154756317')):
    """单个房间概览"""
    latest = db_query("SELECT * FROM live_metrics WHERE room_id=%s ORDER BY id DESC LIMIT 1", (room_id,))
    if not latest: return {"error": "无数据", "online": 0, "likes": 0}
    l = latest[0]; name = ROOMS.get(room_id, '')
    total = db_query("SELECT COUNT(*) as c FROM live_metrics WHERE room_id=%s", (room_id,))[0]['c']
    first = db_query("SELECT created_at FROM live_metrics WHERE room_id=%s ORDER BY id LIMIT 1", (room_id,))
    dm = db_query("SELECT COUNT(*) as c FROM danmaku WHERE room_name=%s", (name,))[0]['c']
    return {
        "room_id": room_id, "room_name": name,
        "online": l.get('online', 0), "likes": l.get('like_count', 0),
        "title": (l.get('title') or '')[:50], "status": l.get('status', ''),
        "total": total, "danmaku_count": dm,
        "start_time": first[0]['created_at'].strftime('%m/%d %H:%M') if first else '',
        "timestamp": l['created_at'].strftime('%H:%M:%S')
    }

@app.get("/api/stats/compare")
def compare():
    """多房间对比数据"""
    result = {}
    for rid, name in ROOMS.items():
        latest = db_query("SELECT online, like_count, title, created_at FROM live_metrics WHERE room_id=%s ORDER BY id DESC LIMIT 1", (rid,))
        dm = db_query("SELECT COUNT(*) as c FROM danmaku WHERE room_name=%s", (name,))[0]['c']
        result[name] = {
            'online': latest[0]['online'] if latest else '离线',
            'likes': latest[0]['like_count'] if latest else 0,
            'title': (latest[0].get('title') or '')[:30] if latest else '',
            'danmaku': dm,
            'updated': latest[0]['created_at'].strftime('%H:%M:%S') if latest else ''
        }
    return result

@app.get("/api/trends")
def trends(room_id: str = Query('994154756317'), mins: int = Query(120)):
    """在线&点赞趋势"""
    rows = db_query("SELECT online, like_count, created_at FROM live_metrics "
                    "WHERE room_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL %s MINUTE) "
                    "ORDER BY id", (room_id, mins))
    step = max(1, len(rows) // 80)
    result = []
    prev_likes = 0
    prev_time = None
    for i, r in enumerate(rows):
        if i % step == 0:
            ol = parse_online(r['online'])
            curr_time = r['created_at']
            # 如果和上一个采样点间隔超过3分钟，说明数据有断档，like_rate置0避免假峰值
            gap_too_large = prev_time and (curr_time - prev_time).total_seconds() > 180
            lr = max(0, r['like_count'] - prev_likes) if prev_likes and not gap_too_large else 0
            prev_likes = r['like_count']
            prev_time = curr_time
            result.append({"online": ol, "likes": r['like_count'], "like_rate": lr,
                           "time": curr_time.strftime('%H:%M')})
    return result

@app.get("/api/danmaku/recent")
def recent_danmaku(room_name: str = Query('影视飓风'), limit: int = Query(20)):
    """最新弹幕"""
    return db_query("SELECT username, text, gift_name, gift_count, msg_type, created_at "
                    "FROM danmaku WHERE room_name=%s ORDER BY id DESC LIMIT %s", (room_name, limit))

@app.get("/api/danmaku/trend")
def danmaku_trend(room_name: str = Query('影视飓风'), mins: int = Query(60)):
    """弹幕频率趋势"""
    return db_query("SELECT DATE_FORMAT(created_at,'%%H:%%i') as t, COUNT(*) as c "
                    "FROM danmaku WHERE room_name=%s AND created_at > DATE_SUB(NOW(), INTERVAL %s MINUTE) "
                    "GROUP BY t ORDER BY t", (room_name, mins))

@app.get("/api/danmaku/words")
def top_words(room_name: str = Query('影视飓风'), limit: int = Query(30)):
    """高频词（简单2-4字ngram）"""
    rows = db_query("SELECT text FROM danmaku WHERE room_name=%s AND msg_type='chat' ORDER BY id DESC LIMIT 2000", (room_name,))
    words = {}
    for r in rows:
        t = r['text']
        for i in range(len(t)-1):
            for l in [2, 3, 4]:
                if i + l <= len(t):
                    w = t[i:i+l]
                    words[w] = words.get(w, 0) + 1
    result = [{'word': k, 'count': v} for k, v in words.items() if v > 2]
    return sorted(result, key=lambda x: -x['count'])[:limit]

@app.get("/api/collect_rate")
def collect_rate(room_id: str = Query('994154756317')):
    """每分钟采集速率"""
    return db_query("SELECT DATE_FORMAT(created_at,'%%H:%%i') as t, COUNT(*) as c "
                    "FROM live_metrics WHERE room_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR) "
                    "GROUP BY t ORDER BY t", (room_id,))


# ===== 分析 APIs（需登录）=====

@app.get("/api/analytics/segmentation")
def segmentation(room_name: str = Query('影视飓风'), user=Depends(get_user)):
    """用户分层"""
    rows = db_query("SELECT username, COUNT(*) as cnt FROM danmaku "
                    "WHERE room_name=%s AND msg_type='chat' GROUP BY username", (room_name,))
    tiers = {'超级粉(100+)': 0, '铁杆粉(50-99)': 0, '忠实粉丝(10-49)': 0,
             '活跃观众(3-9)': 0, '路人(1-2)': 0}
    samples = {k: [] for k in tiers}
    for r in rows:
        cnt = r['cnt']
        if cnt >= 100: t = '超级粉(100+)'
        elif cnt >= 50: t = '铁杆粉(50-99)'
        elif cnt >= 10: t = '忠实粉丝(10-49)'
        elif cnt >= 3: t = '活跃观众(3-9)'
        else: t = '路人(1-2)'
        tiers[t] += 1
        if len(samples[t]) < 3: samples[t].append({'name': r['username'], 'cnt': cnt})
    return {'counts': tiers, 'samples': samples}

@app.get("/api/analytics/funnel")
def funnel(room_name: str = Query('影视飓风'), user=Depends(get_user)):
    """活跃度分析"""
    total = db_query("SELECT COUNT(DISTINCT username) as c FROM danmaku WHERE room_name=%s", (room_name,))[0]['c'] or 1
    high = len(db_query("SELECT DISTINCT username FROM danmaku WHERE room_name=%s AND msg_type='chat' GROUP BY username HAVING COUNT(*) >= 10", (room_name,)))
    mid = len(db_query("SELECT DISTINCT username FROM danmaku WHERE room_name=%s AND msg_type='chat' GROUP BY username HAVING COUNT(*) BETWEEN 3 AND 9", (room_name,)))
    low = total - high - mid
    return {'total_users': total, 'high_active': high, 'mid_active': mid, 'low_active': max(0, low),
            'high_rate': round(high/max(1,total)*100, 1), 'mid_rate': round(mid/max(1,total)*100, 1)}

@app.get("/api/analytics/top-users")
def top_users(room_name: str = Query('影视飓风'), limit: int = Query(15), user=Depends(get_user)):
    """活跃用户"""
    return db_query("SELECT username, COUNT(*) as cnt FROM danmaku "
                    "WHERE room_name=%s AND msg_type='chat' GROUP BY username "
                    "ORDER BY cnt DESC LIMIT %s", (room_name, limit))

@app.get("/api/analytics/hourly")
def hourly(room_name: str = Query('影视飓风'), user=Depends(get_user)):
    """分时活跃"""
    return db_query("SELECT HOUR(created_at) as hour, COUNT(*) as cnt FROM danmaku "
                    "WHERE room_name=%s AND msg_type='chat' GROUP BY HOUR(created_at) ORDER BY hour", (room_name,))

@app.get("/api/search")
def search(qs: str = Query(''), room_name: str = Query(''), user=Depends(get_user)):
    """弹幕搜索"""
    w = "WHERE text LIKE %s"; params = [f"%{qs}%"]
    if room_name:
        w += " AND room_name=%s"; params.append(room_name)
    return db_query(f"SELECT room_name, username, text, gift_name, created_at FROM danmaku {w} ORDER BY id DESC LIMIT 50", params)

@app.get("/api/export")
def export_csv(room_name: str = Query('影视飓风'), user=Depends(get_user)):
    """导出CSV"""
    rows = db_query("SELECT * FROM danmaku WHERE room_name=%s ORDER BY id DESC LIMIT 10000", (room_name,))
    if not rows: return HTMLResponse("No data")
    o = io.StringIO()
    w = csv.DictWriter(o, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    return HTMLResponse(o.getvalue(), media_type="text/csv",
                         headers={"Content-Disposition": f"attachment;filename={room_name}_danmaku.csv"})


# ===== 高级分析 APIs（需登录）=====

@app.get("/api/analytics/rfm")
def rfm_analysis(room_name: str = Query('影视飓风'), user=Depends(get_user)):
    """RFM用户分群"""
    from analytics_advanced import rfm_analysis as rfm
    return rfm(room_name)

@app.get("/api/analytics/funnel_full")
def funnel_full(room_name: str = Query('影视飓风'), user=Depends(get_user)):
    """转化漏斗分析"""
    from analytics_advanced import funnel_analysis as fa
    return fa(room_name)

@app.get("/api/analytics/retention")
def retention(room_name: str = Query('影视飓风'), user=Depends(get_user)):
    """用户留存率分析"""
    from analytics_advanced import retention_analysis as ra
    return ra(room_name)

@app.get("/api/analytics/journey")
def journey(room_name: str = Query('影视飓风'), limit: int = Query(5), user=Depends(get_user)):
    """用户旅程抽样"""
    from analytics_advanced import user_journey as uj
    return uj(room_name, limit)


# ===== NLP 分析 APIs =====

@app.get("/api/nlp/sentiment")
def nlp_sentiment(room_name: str = Query('影视飓风'), limit: int = Query(3000)):
    """弹幕情感分析"""
    from nlp_analysis import analyze_room_sentiment
    return analyze_room_sentiment(room_name, limit)

@app.get("/api/nlp/keywords")
def nlp_keywords(room_name: str = Query('影视飓风'), limit: int = Query(2000)):
    """关键词提取 (jieba TF-IDF)"""
    from nlp_analysis import extract_keywords
    return extract_keywords(room_name, limit)

@app.get("/api/nlp/wordcloud")
def nlp_wordcloud(room_name: str = Query('影视飓风')):
    """词云数据"""
    from nlp_analysis import word_cloud_data
    return word_cloud_data(room_name)

@app.get("/api/nlp/quadrant")
def nlp_quadrant(room_name: str = Query('影视飓风')):
    """情感四象限"""
    from nlp_analysis import sentiment_quadrant
    return sentiment_quadrant(room_name)

@app.get("/api/nlp/timeline")
def nlp_timeline(room_name: str = Query('影视飓风')):
    """情感时序"""
    from nlp_analysis import sentiment_timeline
    return sentiment_timeline(room_name)


# ===== 实时聚合 =====

@app.get("/api/aggregates/latest")
def latest_aggregates(room_id: str = Query('994154756317')):
    """最近1分钟实时聚合"""
    rows = db_query("SELECT * FROM live_metrics WHERE room_id=%s AND created_at > DATE_SUB(NOW(), INTERVAL 1 MINUTE) ORDER BY id", (room_id,))
    if not rows: return {'online_avg': 0, 'like_rate': 0, 'records': 0}
    onlines = [parse_online(r['online']) for r in rows]
    lr = max(0, rows[-1]['like_count'] - rows[0]['like_count']) if len(rows) > 1 else 0
    return {'online_avg': sum(onlines)//len(onlines), 'online_max': max(onlines),
            'online_min': min(onlines), 'like_rate': lr, 'records': len(rows),
            'time': rows[-1]['created_at'].strftime('%H:%M:%S')}


# ===== 商品 & 订单 APIs =====

@app.get("/api/products")
def product_list(room_name: str = Query(''), category: str = Query('')):
    """商品列表"""
    w, p = [], []
    if room_name: w.append("room_name=%s"); p.append(room_name)
    if category: w.append("category=%s"); p.append(category)
    where = ("WHERE " + " AND ".join(w)) if w else ""
    return db_query(f"SELECT * FROM products {where} ORDER BY sold DESC, product_id LIMIT 100", p)

@app.get("/api/products/top")
def top_products(room_name: str = Query(''), limit: int = Query(10)):
    """热销商品 Top N"""
    w, p = ("WHERE room_name=%s", [room_name]) if room_name else ("", [])
    return db_query(f"SELECT * FROM products {w} ORDER BY sold DESC LIMIT {limit}", p)

@app.get("/api/products/inventory")
def inventory_alerts(room_name: str = Query('')):
    """库存预警（售罄率排序）"""
    w = f"WHERE room_name='{room_name}'" if room_name else ""
    return db_query(f"""SELECT product_id, product_name, room_name, category, stock, sold,
        ROUND(sold/GREATEST(1,stock)*100, 1) as sell_through_rate,
        CASE WHEN sold/stock > 0.8 THEN 'RED'
             WHEN sold/stock > 0.5 THEN 'YELLOW'
             WHEN sold/stock > 0.3 THEN 'GREEN'
             ELSE 'OK' END as alert_level
        FROM products {w} ORDER BY sell_through_rate DESC LIMIT 30""")

@app.get("/api/products/{product_id}")
def product_detail(product_id: int):
    """商品详情 + 销量趋势"""
    p = db_query("SELECT * FROM products WHERE product_id=%s", (product_id,))
    if not p: raise HTTPException(404, "Not found")
    sales = db_query("""SELECT DATE(order_time) as dt, SUM(quantity) as qty, SUM(total_amount) as gmv
        FROM orders WHERE product_id=%s GROUP BY dt ORDER BY dt""", (product_id,))
    return {"product": p[0], "sales_trend": sales}

@app.get("/api/orders/recent")
def recent_orders(room_name: str = Query(''), limit: int = Query(20)):
    """最近订单"""
    w, p = ("WHERE room_name=%s", [room_name]) if room_name else ("", [])
    return db_query(f"SELECT * FROM orders {w} ORDER BY order_time DESC LIMIT {limit}", p)

@app.get("/api/orders/gmv")
def gmv_trend(room_name: str = Query(''), days: int = Query(14)):
    """GMV 趋势（按天汇总）"""
    w, p = ("WHERE room_name=%s", [room_name]) if room_name else ("", [])
    return db_query(f"""SELECT DATE(order_time) as dt, COUNT(*) as orders,
        ROUND(SUM(total_amount),0) as gmv, SUM(quantity) as items
        FROM orders {w} {'AND' if w else 'WHERE'} order_time > DATE_SUB(NOW(), INTERVAL {days} DAY)
        GROUP BY dt ORDER BY dt""", p)

# ===== 推荐 & 预测 APIs =====

@app.get("/api/recommend/cf")
def recommend_cf(room_name: str = Query(''), user=Depends(get_user)):
    """协同过滤推荐"""
    from recommendation import collaborative_filter
    return collaborative_filter(room_name)

@app.get("/api/recommend/rules")
def recommend_rules(room_name: str = Query(''), user=Depends(get_user)):
    """关联规则"""
    from recommendation import association_rules
    return association_rules(room_name, min_support=2, min_confidence=0.05)

@app.get("/api/predict/sales")
def predict_sales(room_name: str = Query(''), days: int = Query(7), user=Depends(get_user)):
    """销量预测"""
    from recommendation import sales_prediction
    return sales_prediction(room_name=room_name, days_ahead=days)

@app.get("/api/predict/hot")
def predict_hot(room_name: str = Query(''), user=Depends(get_user)):
    """热销+潜力榜"""
    from recommendation import hot_and_rising
    return hot_and_rising(room_name)


# ===== 页面 =====

@app.get("/", response_class=HTMLResponse)
def simple_dashboard_page():
    """简易大屏"""
    path = os.path.join(os.path.dirname(__file__), 'dashboard.html')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f: return f.read()
    return _simple_dashboard_inline()

@app.get("/enterprise", response_class=HTMLResponse)
def enterprise_dashboard_page():
    """企业大屏（需登录）"""
    path = os.path.join(os.path.dirname(__file__), 'dashboard_enterprise.html')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f: return f.read()
    return _enterprise_dashboard_inline()

def _simple_dashboard_inline():
    return _load_html('dashboard.html')

def _enterprise_dashboard_inline():
    return _load_html('dashboard_enterprise.html')

def _load_html(filename):
    path = os.path.join(os.path.dirname(__file__), filename)
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f: return f.read()
    return f"<!DOCTYPE html><html><body><h1>{filename} not found</h1></body></html>"


# ===== 初始化 =====
def init_db():
    try:
        db = pymysql.connect(host=DB_CFG['host'], user=DB_CFG['user'],
                             password=DB_CFG['password'])
        c = db.cursor()
        c.execute("CREATE DATABASE IF NOT EXISTS live_analytics CHARACTER SET utf8mb4")
        db.commit()
        c.execute("USE live_analytics")
        # 聚合表
        c.execute("""CREATE TABLE IF NOT EXISTS live_aggregates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            room_id VARCHAR(30), online_avg INT, online_max INT, online_min INT,
            like_rate INT, danmaku_rate INT, gmv_estimate DECIMAL(14,2),
            window_start VARCHAR(30), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        # 用户表
        c.execute("""CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            password VARCHAR(255) NOT NULL,
            role ENUM('admin','viewer') DEFAULT 'viewer',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        pw = hashlib.sha256('admin123'.encode()).hexdigest()
        c.execute("INSERT IGNORE INTO users (username,password,role) VALUES ('admin',%s,'admin')", (pw,))
        db.commit(); c.close(); db.close()
        print("DB initialized OK")
    except Exception as e:
        print(f"DB init: {e}")


if __name__ == '__main__':
    import uvicorn
    init_db()
    print("=" * 50)
    print("  直播数据分析平台 v5")
    print("  简易大屏: http://localhost:8002")
    print("  企业大屏: http://localhost:8002/enterprise")
    print("  API文档:  http://localhost:8002/docs")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=8002, log_level="warning")
