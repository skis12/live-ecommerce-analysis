"""
逼真商品数据生成器
影视飓风 → 数码/影视设备 (20件)
与晖同行 → 综合百货 (397件)
输出: products, orders, inventory 表 → MySQL
"""
import pymysql, random, time, sys, os, json
from datetime import datetime, timedelta
from faker import Faker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

fake = Faker('zh_CN')
random.seed(42)
fake.seed_instance(42)

DB_CFG = DB_CONFIG

# ============================================================
# 影视飓风 — 数码影视设备 (20 SKU)
# ============================================================
YINGSHI_PRODUCTS = [
    # (品名, 品类, 成本, 售价, 库存, 直播价)
    ('索尼 A7M4 全画幅微单相机', '相机', 12999, 14999, 50, 14599),
    ('佳能 EOS R6 Mark II', '相机', 11499, 13499, 30, 12999),
    ('大疆 RS 4 Pro 稳定器', '稳定器', 3999, 4999, 80, 4799),
    ('大疆 Mini 4 Pro 无人机', '无人机', 3788, 4788, 60, 4588),
    ('罗德 Wireless PRO 无线麦克风', '音频', 2199, 2799, 120, 2599),
    ('森海塞尔 MKH 416 枪麦', '音频', 6299, 7599, 25, 7299),
    ('Aputure 300d II 影视灯', '灯光', 5199, 6499, 40, 6199),
    ('神牛 SL150W III 补光灯', '灯光', 1280, 1680, 100, 1580),
    ('SmallRig 兔笼套件', '配件', 399, 599, 200, 549),
    ('铁头 DJI 4D 专业套件', '配件', 8999, 10999, 15, 10499),
    ('Apple MacBook Pro 16 M4 Pro', '电脑', 17499, 19999, 35, 19499),
    ('LG 32UN880 4K显示器', '显示器', 4499, 5499, 45, 5199),
    ('闪迪 2TB CFexpress Type B', '存储', 3299, 3999, 90, 3799),
    ('致态 TiPro7000 2TB SSD', '存储', 899, 1199, 150, 1099),
    ('Blackmagic ATEM Mini Pro', '导播', 2580, 3280, 30, 3080),
    ('Elgato Stream Deck XL', '导播', 1280, 1680, 55, 1580),
    ('智云 FIVERAY 60W 手持灯', '灯光', 699, 899, 80, 849),
    ('蒲公英 5合1 反光板', '配件', 89, 149, 300, 129),
    ('猛犸 Lark M2 无线麦', '音频', 599, 799, 180, 749),
    ('印迹 羚羊 TC7 三脚架', '支撑', 1699, 2199, 70, 1999),
]

# ============================================================
# 与晖同行 — 综合百货 (397 SKU，按品类分布)
# ============================================================
YUHUI_CATEGORIES = {
    '图书文教': {
        'ratio': 0.25,
        'items': [
            ('《人类简史》三部曲 精装版', 89, 149, 500, 129),
            ('《百年孤独》50周年纪念版', 39, 69, 800, 59),
            ('《三体》全集 刘慈欣', 59, 99, 600, 89),
            ('《平凡的世界》路遥 全三册', 69, 108, 400, 98),
            ('《中国通史》吕思勉 精装', 128, 198, 200, 178),
            ('《苏东坡传》林语堂', 28, 49, 900, 42),
            ('《思考，快与慢》卡尼曼', 49, 79, 450, 69),
            ('《论语》中华经典藏书', 18, 32, 1000, 28),
            ('《活着》余华', 25, 45, 700, 39),
            ('《小王子》中英双语', 22, 39, 600, 35),
            ('《数学之美》吴军', 35, 59, 350, 49),
            ('《经济学原理》曼昆', 68, 108, 250, 98),
        ],
    },
    '食品饮料': {
        'ratio': 0.30,
        'items': [
            ('东北五常大米 5kg', 35, 59, 2000, 49),
            ('云南普洱茶饼 357g 熟茶', 68, 128, 800, 99),
            ('新疆阿克苏冰糖心苹果 5斤', 25, 45, 1500, 39),
            ('四川爱媛38号果冻橙 5斤', 19, 35, 1800, 29),
            ('内蒙古风干牛肉干 500g', 49, 79, 600, 69),
            ('柳州螺蛳粉 6包装', 29, 49, 1200, 42),
            ('黄山毛峰 明前特级 100g', 58, 108, 500, 88),
            ('宁夏枸杞 特级 250g', 35, 59, 1000, 49),
            ('大连即食海参 500g', 168, 268, 300, 238),
            ('贵州茅台镇酱香酒 500ml', 89, 168, 400, 149),
        ],
    },
    '日用家居': {
        'ratio': 0.20,
        'items': [
            ('加厚羽绒被 200×230cm', 199, 359, 300, 299),
            ('四件套纯棉床品 1.8m', 129, 229, 500, 199),
            ('日本收纳箱 特大号 3个装', 39, 69, 1200, 59),
            ('304不锈钢保温壶 2L', 49, 89, 700, 79),
            ('乳胶枕 人体工学 一对装', 79, 149, 400, 129),
            ('除螨仪 紫外线杀菌', 159, 269, 350, 239),
            ('折叠收纳凳 大容量', 29, 55, 1000, 45),
            ('竹纤维洗碗布 20条装', 12, 25, 2000, 19),
        ],
    },
    '小家电': {
        'ratio': 0.15,
        'items': [
            ('小米空气炸锅 4.5L', 199, 299, 400, 279),
            ('小熊加湿器 5L 静音', 79, 139, 600, 119),
            ('苏泊尔电饭煲 4L', 229, 359, 300, 329),
            ('美的破壁机 1.75L', 299, 469, 250, 429),
            ('九阳豆浆机 免洗', 259, 399, 350, 369),
            ('飞利浦电动牙刷 HX6', 169, 269, 500, 239),
            ('戴森吸尘器 V12', 2999, 3699, 120, 3499),
            ('石头扫地机器人 P10', 1999, 2599, 200, 2399),
        ],
    },
    '文创办公': {
        'ratio': 0.10,
        'items': [
            ('故宫日历 2026年', 49, 89, 1500, 69),
            ('LAMY 凌美钢笔 狩猎系列', 99, 169, 400, 149),
            ('国潮手账本 A5', 29, 55, 800, 45),
            ('敦煌文创 书签套装', 35, 69, 600, 59),
            ('小米巨能写中性笔 10支', 9, 19, 3000, 15),
            ('莫奈油画 帆布袋', 19, 39, 500, 29),
        ],
    },
}


def create_products():
    """生成所有商品数据"""
    products = []
    product_id = 1

    # 影视飓风 20件
    for name, cat, cost, price, stock, live_price in YINGSHI_PRODUCTS:
        products.append({
            'product_id': product_id,
            'product_name': name,
            'category': cat,
            'room_name': '影视飓风',
            'room_id': '994154756317',
            'cost_price': cost,
            'original_price': price,
            'live_price': live_price,
            'stock': stock,
            'sold': 0,
            'margin': round((live_price - cost) / cost * 100, 1),
            'is_hot': live_price < price * 0.9,
        })
        product_id += 1

    # 与晖同行 397件
    for cat_name, cat_info in YUHUI_CATEGORIES.items():
        base_items = cat_info['items']
        target_count = int(397 * cat_info['ratio'])

        for i in range(target_count):
            # 循环使用基础品类模板，用 Faker 生成变体
            if i < len(base_items):
                name, cost, price, stock, live_price = base_items[i]
            else:
                # 用 Faker 生成同品类的变体商品
                template = random.choice(base_items)
                base_name = template[0]
                variants = ['升级款', '经典款', '旗舰款', '便携款', '尊享款', 'mini款', 'Pro款', '青春款']
                name = f'{base_name[:6]}{random.choice(variants)}'
                cost = template[1] + random.randint(-20, 30)
                price = template[2] + random.randint(-30, 50)
                live_price = template[4] + random.randint(-15, 25)
                stock = random.randint(100, 2000)

            cost = max(1, cost)  # 避免除零
            live_price = max(cost + 5, live_price)
            price = max(live_price, price)

            products.append({
                'product_id': product_id,
                'product_name': name,
                'category': cat_name,
                'room_name': '与晖同行',
                'room_id': '646454278948',
                'cost_price': round(cost, 2),
                'original_price': round(price, 2),
                'live_price': round(live_price, 2),
                'stock': stock,
                'sold': 0,
                'margin': round((live_price - cost) / cost * 100, 1),
                'is_hot': random.random() < 0.25,
            })
            product_id += 1

    # 截断到 397 + 20 = 417
    yuhui = [p for p in products if p['room_name'] == '与晖同行']
    yingshi = [p for p in products if p['room_name'] == '影视飓风']
    products = yingshi + yuhui[:397]

    return products


def generate_orders(products: list, days: int = 14, orders_per_day: int = 500):
    """生成订单数据（过去N天，模拟直播带货节奏）"""
    orders = []
    order_id = 1
    now = datetime.now()

    for day_offset in range(days, 0, -1):
        date = now - timedelta(days=day_offset)
        hour = random.randint(7, 23)  # 直播时间段
        minute = random.randint(0, 59)

        n_orders = orders_per_day + random.randint(-200, 300)

        for _ in range(n_orders):
            product = random.choice(products)
            qty = random.choices([1, 2, 3, 5, 10], weights=[50, 25, 10, 5, 10])[0]
            qty = min(qty, product['stock'] // 10)
            if qty <= 0: continue

            unit_price = product['live_price']
            total = round(unit_price * qty, 2)

            ts = date.replace(hour=hour, minute=minute, second=random.randint(0, 59))
            ts += timedelta(minutes=random.randint(0, 120))

            orders.append({
                'order_id': order_id,
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'room_name': product['room_name'],
                'category': product['category'],
                'quantity': qty,
                'unit_price': unit_price,
                'total_amount': total,
                'order_time': ts.strftime('%Y-%m-%d %H:%M:%S'),
                'customer_id': f'USER_{random.randint(10000, 99999)}',
            })
            order_id += 1

    return orders


def insert_to_mysql(products: list, orders: list):
    """写入 MySQL"""
    db = pymysql.connect(**DB_CFG)
    c = db.cursor()

    # 建表
    c.execute("""CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY, product_name VARCHAR(200), category VARCHAR(50),
        room_name VARCHAR(50), room_id VARCHAR(30),
        cost_price DECIMAL(10,2), original_price DECIMAL(10,2), live_price DECIMAL(10,2),
        stock INT, sold INT DEFAULT 0, margin DECIMAL(5,1), is_hot BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS orders (
        order_id INT AUTO_INCREMENT PRIMARY KEY, product_id INT, product_name VARCHAR(200),
        room_name VARCHAR(50), category VARCHAR(50),
        quantity INT, unit_price DECIMAL(10,2), total_amount DECIMAL(12,2),
        order_time DATETIME, customer_id VARCHAR(30),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_room (room_name), INDEX idx_time (order_time), INDEX idx_product (product_id)
    )""")

    c.execute("TRUNCATE TABLE products")
    c.execute("TRUNCATE TABLE orders")

    # Insert products
    for p in products:
        c.execute("""INSERT INTO products (product_id, product_name, category, room_name, room_id,
            cost_price, original_price, live_price, stock, sold, margin, is_hot)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (p['product_id'], p['product_name'], p['category'], p['room_name'], p['room_id'],
             p['cost_price'], p['original_price'], p['live_price'], p['stock'], p['sold'],
             p['margin'], p['is_hot']))

    # Batch insert orders (每批1000条)
    batch = []
    for o in orders:
        batch.append((o['product_id'], o['product_name'], o['room_name'], o['category'],
                      o['quantity'], o['unit_price'], o['total_amount'],
                      o['order_time'], o['customer_id']))
        if len(batch) >= 1000:
            c.executemany("""INSERT INTO orders (product_id, product_name, room_name, category,
                quantity, unit_price, total_amount, order_time, customer_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", batch)
            batch = []
    if batch:
        c.executemany("""INSERT INTO orders (product_id, product_name, room_name, category,
            quantity, unit_price, total_amount, order_time, customer_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""", batch)

    # Update sold count
    c.execute("""UPDATE products p SET sold = (
        SELECT COALESCE(SUM(quantity), 0) FROM orders o WHERE o.product_id = p.product_id
    )""")

    db.commit()
    rows_p = c.execute("SELECT COUNT(*) FROM products")
    rows_o = c.execute("SELECT COUNT(*) FROM orders")
    c.close(); db.close()
    return rows_p, rows_o


def generate_inventory_alerts(products: list):
    """三级库存预警"""
    alerts = []
    for p in products:
        ratio = p['sold'] / max(1, p['stock'])  # 已售/库存比
        if ratio > 0.8:
            level = 'RED-紧急补货'
        elif ratio > 0.5:
            level = 'YELLOW-预警库存'
        elif ratio > 0.3:
            level = 'GREEN-正常偏低'
        else:
            level = 'OK-库存充足'

        alerts.append({
            'product_id': p['product_id'],
            'product_name': p['product_name'],
            'room_name': p['room_name'],
            'stock': p['stock'],
            'sold': p['sold'] if p['sold'] else 0,
            'sell_through_rate': round(p['sold'] / max(1, p['stock']) * 100, 1),
            'alert_level': level,
        })

    # 按售罄率排序
    alerts.sort(key=lambda x: -x['sell_through_rate'])
    return alerts


if __name__ == '__main__':
    t0 = time.time()
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 14

    print(f"生成商品数据 (影视飓风20件 + 与晖同行397件)...")
    products = create_products()
    print(f"  Products: {len(products)}")

    print(f"生成过去{days}天订单...")
    orders = generate_orders(products, days=days, orders_per_day=400)
    print(f"  Orders: {len(orders)}")

    print("写入 MySQL...")
    p_count, o_count = insert_to_mysql(products, orders)
    print(f"  Products: {p_count} rows, Orders: {o_count} rows")

    # 库存预警 Top10
    alerts = generate_inventory_alerts(products)
    print(f"\n=== 库存预警 Top10 ===")
    for a in alerts[:10]:
        print(f"  {a['alert_level']} | {a['room_name']} | {a['product_name'][:20]} | "
              f"库存{a['stock']} 已售{a['sold']} ({a['sell_through_rate']}%)")

    # GMV 统计
    total_gmv = sum(o['total_amount'] for o in orders)
    yingshi_gmv = sum(o['total_amount'] for o in orders if o['room_name'] == '影视飓风')
    yuhui_gmv = sum(o['total_amount'] for o in orders if o['room_name'] == '与晖同行')
    print(f"\n=== GMV 统计 ===")
    print(f"  总GMV: ¥{total_gmv:,.0f}")
    print(f"  影视飓风: ¥{yingshi_gmv:,.0f}")
    print(f"  与晖同行: ¥{yuhui_gmv:,.0f}")
    print(f"\nDone in {time.time()-t0:.1f}s")
