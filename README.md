# 直播电商实时数据分析平台

> Live E-commerce Real-Time Data Analytics Platform (Lambda Architecture)

面向抖音直播平台的端到端实时数据采集、处理、分析、可视化全链路系统。支持双直播间并行采集，覆盖实时流计算、离线数仓、NLP情感分析、用户分群、商品推荐、销量预测等模块。

![Tech Stack](https://img.shields.io/badge/Kafka-3.x-231F20?logo=apachekafka)
![Hive](https://img.shields.io/badge/Hive-3.x-FDEE21?logo=apachehive)
![Hadoop](https://img.shields.io/badge/Hadoop-3.x-66CCFF?logo=apachehadoop)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?logo=fastapi)
![ECharts](https://img.shields.io/badge/ECharts-5.5-AA344D)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python)

---

## 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                   抖音直播平台                            │
│  live.douyin.com/994154756317 (影视飓风)                  │
│  live.douyin.com/646454278948 (与晖同行)                  │
└──────────┬─────────────────────┬────────────────────────┘
           │ HTTP API (1s轮询)     │ WebSocket (实时推送)
           ▼                      ▼
┌────────────────────┐  ┌──────────────────────────┐
│ douyin_full_       │  │ douyin_multi_room.py     │
│ crawler.py         │  │ • 弹幕/礼物/进场/统计     │
│ • 41字段房间信息    │  │ • Protobuf协议解析        │
│ • ThreadPool并行   │  │ • 每房间独立线程+自动重连  │
└────────┬───────────┘  └──────────┬───────────────┘
         │                         │
         ▼                         ▼
┌────────────────────────────────────────────────────────┐
│              Apache Kafka (3节点集群)                    │
│  live_room_info  │  live_danmaku  │  live_gifts        │
└────────┬───────────────────────────────────────────────┘
         │
    ┌────┴──────────┐
    ▼               ▼
┌────────────┐ ┌─────────────────────────────────────────┐
│ realtime_  │ │  kafka_to_mysql.py                       │
│ processor  │ │  • 消费3个Topic → MySQL 3张表             │
│ .py        │ │  • 60秒窗口聚合 → live_aggregates         │
│ • GMV预估  │ │  • 每100条batch commit                    │
│ • CEP告警  │ └──────────────┬──────────────────────────┘
└────────────┘                │
                    ┌─────────┴──────────┐
                    ▼                    ▼
              ┌───────────┐     ┌──────────────────────┐
              │ hive_etl  │     │ FastAPI (app.py)     │
              │ .py       │     │ • 30个REST API       │
              │ MySQL→    │     │ • JWT认证            │
              │ HDFS→Hive │     │ • NLP+推荐+分析       │
              └─────┬─────┘     └──────────┬───────────┘
                    │                      │
                    ▼                      ▼
              ┌───────────┐     ┌──────────────────────┐
              │ Hive数仓   │     │ ECharts大屏           │
              │ ODS→DWD→  │     │ / → 简易大屏          │
              │ DWS→ADS   │     │ /enterprise → 企业大屏 │
              │ ORC存储   │     │ 12s刷新+独立分步加载   │
              └───────────┘     └──────────────────────┘
```

### 数据流 (Lambda 架构)

- **实时链路 (秒级)**: 抖音API → Kafka → kafka_to_mysql.py → MySQL → FastAPI → 浏览器
- **离线链路 (分钟级)**: MySQL → hive_etl.py → HDFS → Hive (ODS→DWD→DWS→ADS)

---

## 项目结构

```
live-ecommerce-analysis/
├── config.py                      # 统一配置（环境变量注入）
├── .env.example                   # 环境变量模板
├── .gitignore
│
├── crawler/                       # 数据采集层
│   ├── douyin_full_crawler.py     # HTTP全量采集 (41字段, 1s间隔)
│   ├── douyin_multi_room.py       # WebSocket弹幕/礼物采集 (Protobuf)
│   ├── douyin_danmaku_crawler.py  # WS独立采集脚本
│   ├── bilibili_multi_crawler.py  # B站扩展采集
│   ├── run_danmaku.bat            # Windows快速启动
│   └── DouyinLiveWebFetcher/      # 抖音WS协议库 (签名+Protobuf)
│
├── pipeline/                      # 数据管道层
│   ├── kafka_to_mysql.py          # Kafka消费→MySQL入库+60s窗口聚合
│   ├── realtime_processor.py      # 实时计算: GMV估算+CEP异常告警
│   ├── flink_realtime.py          # Python窗口聚合 (Flink模拟版,已由flink_jobs/替代)
│   ├── hive_etl.py                # Hive数仓ETL: MySQL→HDFS→Hive
│   └── generate_product_data.py   # 商品/订单数据生成器 (Faker)
│
├── backend/                       # 业务应用层
│   ├── app.py                     # FastAPI主后端 (端口8002, 30个API)
│   ├── nlp_analysis.py            # NLP: 情感分析+关键词+词云+四象限
│   ├── analytics_advanced.py      # 高级分析: RFM+漏斗+留存+用户旅程
│   ├── recommendation.py          # 推荐: 协同过滤+关联规则+销量预测
│   ├── dashboard.html             # 简易大屏 (无需登录)
│   └── dashboard_enterprise.html  # 企业大屏 (3Tab/10图表/JWT认证)
│
├── sql/
│   └── hive_warehouse.sql         # Hive数仓DDL (ODS/DWD/DWS/ADS)
│
├── scripts/
│   ├── start_all.sh               # VM端一键启动集群服务
│   └── find_live.py               # 搜索抖音直播间ID
│
├── data_simulator/                # 数据模拟器 (预留)
├── docs/                          # 文档
├── flink_jobs/                    # Flink SQL (Event Time+Watermark+JDBC)
│   ├── realtime_aggregation.sql   #   主作业: Kafka→TUMBLE窗口→MySQL
│   ├── setup_connectors.sh        #   一键下载Kafka+JDBC+MySQL连接器
│   └── submit_flink_job.sh        #   提交脚本 (支持实时/回放模式)
├── frontend/                      # 前端 (预留)
│
├── start_silent.vbs               # Windows一键静默启动
└── README.md
```

---

## 核心功能

### 1. 实时数据采集
- HTTP API 每 1 秒轮询双直播间，采集 41 个字段（在线人数、点赞、标题、购物车状态等）
- WebSocket 长连接实时接收弹幕、礼物、用户进场消息，Protobuf 协议解析
- 断线自动重连机制（5-15秒恢复），独立线程互不干扰

### 2. 实时计算引擎
- 60 秒滑动窗口聚合：在线均值/峰值、弹幕速率、GMV 预估
- 4 条 CEP 规则异常检测：在线骤降 30%、飙涨 50%、GMV 翻倍、刷屏 (>100条/分钟)
- 已累计产出 1,900+ 条告警记录

### 3. 离线数据仓库
- MySQL → HDFS → Hive 四层数仓 (ODS/DWD/DWS/ADS)
- ODS 层 TextFile 保留原始数据，上层 ORC 列存加速查询
- 支持全量和增量 ETL 模式

### 4. NLP 弹幕分析
- SnowNLP 情感分析 (正面/中性/负面三级)
- jieba TF-IDF 关键词提取 (Top 50)
- 词云生成 (Top 100)
- 情感四象限用户分群 (忠实粉丝/情绪用户/路人好感/流失风险)
- 30 秒内存 TTL 缓存，首次请求 ~3 秒，缓存命中 <50ms

### 5. 用户分析
- RFM 四群分群 (核心粉丝/新晋活跃/沉睡用户/流失风险)
- 6 层转化漏斗 (独立观众→发弹幕→活跃互动→高活跃→送礼→核心用户)
- Day1/3/7 留存率分析
- 用户活跃分层 (超级粉/铁杆粉/忠实粉丝/活跃观众/路人)

### 6. 推荐与预测
- 协同过滤推荐 (用户-商品共现矩阵)
- 关联规则挖掘 (支持度/置信度/提升度)
- 7 天销量预测 (移动平均 + 指数平滑 + 线性回归)

### 7. 可视化大屏
- ECharts 5.5 企业大屏，3 标签页 / 10 种图表
- 独立分步加载架构：解决定时刷新 × 异步加载的竞态条件
- 12 秒自动刷新，同房间定时刷新零 DOM 替换、零视觉闪烁
- 30 个 RESTful API (FastAPI + JWT 认证)

---

## 数据库设计

| 表名 | 行数 | 来源 | 用途 |
|------|------|------|------|
| `live_metrics` | 12万+ | HTTP API → Kafka | 房间指标 (在线/点赞/标题等11字段) |
| `danmaku` | 10万+ | WebSocket → Kafka | 弹幕+礼物明细 |
| `room_stats` | 2万+ | WebSocket → Kafka | WS 推送在线统计 |
| `live_realtime_metrics` | 500+ | realtime_processor | 60s 窗口聚合指标 |
| `live_alerts` | 1,900+ | realtime_processor CEP | 异常告警记录 |
| `live_aggregates` | — | kafka_to_mysql | 管道层窗口聚合 |
| `products` | 415件 | generate_product_data | 商品信息 |
| `orders` | 7,146笔 | generate_product_data | 模拟订单 |

---

## 快速开始

### 环境要求

- **集群**: 3 节点 Hadoop + Kafka (CentOS 7 VM)
- **开发机**: Windows 11, Python 3.12
- **数据库**: MySQL 8.0 (hadoop01:3306)

### 1. 配置

```bash
cp .env.example .env
# 编辑 .env 填入真实密码
```

### 2. 启动集群服务 (VM端)

```bash
ssh hadoop01
bash scripts/start_all.sh
# 依次启动: Hadoop HDFS → MySQL → Zookeeper → Kafka → Hive Metastore → HiveServer2
```

### 3. 启动数据管道 (Windows端)

```bash
# 终端1: 启动采集器 (HTTP + WebSocket)
python crawler/douyin_full_crawler.py

# 终端2: Kafka消费入库
python pipeline/kafka_to_mysql.py

# 终端3: 实时处理+CEP告警 (可选)
python pipeline/realtime_processor.py
```

或一键启动:
```bash
start_silent.vbs
```

### 4. 启动后端 & 大屏

```bash
python backend/app.py
# 简易大屏: http://localhost:8002
# 企业大屏: http://localhost:8002/enterprise (admin / admin123)
# API文档:  http://localhost:8002/docs
```

### 5. 离线数仓ETL (按需执行)

```bash
# 全量导入
python pipeline/hive_etl.py --full

# 增量导入 (最近1小时)
python pipeline/hive_etl.py
```

### 6. 生成模拟商品/订单数据

```bash
python pipeline/generate_product_data.py
```

---

## 技术决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 消息队列 | Kafka | 高吞吐、持久化、3节点高可用 |
| 实时计算 | Python 模拟 Flink | 快速原型验证，核心概念 (窗口/CEP/状态) 一致 |
| 离线数仓 | Hive + ORC | 列存压缩，SQL 分析友好 |
| 后端框架 | FastAPI | 异步高性能，自动生成 Swagger 文档 |
| 可视化 | ECharts 5.5 | 功能丰富，社区活跃 |
| 架构模式 | Lambda | 实时+离线双链路互补 |
| NLP | SnowNLP + jieba | 中文友好，离线可用 |

### 已知局限 & 改进方向

- **Flink 模拟版** → 改用真正的 Flink SQL，支持 Checkpoint 故障恢复
- **MySQL** → ClickHouse/Doris，OLAP 查询快 100 倍
- **单文件 HTML** → React/Vue 组件化，提高可维护性
- **模拟商品/订单数据** → 对接真实交易数据或公开数据集
- **无监控告警** → Prometheus + Grafana 采集管道指标

---

## 性能指标

| 指标 | 数值 |
|------|------|
| 端到端延迟 | < 5 秒 (抖音 API → 浏览器大屏) |
| HTTP 采集间隔 | 1 秒 (双房间并行) |
| Kafka 消费延迟 | < 2 秒 |
| API 查询响应 | < 200ms (NLP 首次约 3 秒) |
| 累计数据量 | 23 万+ 条 |
| 日均入库 | 5,000+ 条 |

---

## License

MIT — 本项目为校内生产实习成果，仅供学习和研究使用。
