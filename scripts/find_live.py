"""
查找热门抖音带货直播间 ID
"""
import httpx
import json
import re

def search_douyin_live(keyword="带货"):
    """搜索抖音带货直播间"""
    # 使用抖音搜索 API
    url = f"https://www.douyin.com/search/{keyword}?type=live"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        resp = httpx.get(url, headers=headers, follow_redirects=True, timeout=10)
        # 提取直播间 ID
        room_ids = re.findall(r'room_id["\']?\s*[:=]\s*["\']?(\d+)', resp.text)
        unique_rooms = list(set(room_ids))[:10]

        print(f"找到 {len(unique_rooms)} 个带货直播间:")
        for rid in unique_rooms:
            print(f"  https://live.douyin.com/{rid}")
        return unique_rooms
    except Exception as e:
        print(f"搜索失败: {e}")

if __name__ == '__main__':
    print("正在搜索热门带货直播间...")
    rooms = search_douyin_live()

    # 手动推荐的热门带货直播间 ID(这些是公开已知的)
    print("\n=== 推荐的热门带货直播间 ===")
    recommendations = [
        "62801875579",  # 东方甄选 (需验证)
        "123456789",    # 示例,需替换
    ]
    for rid in recommendations[:1]:
        print(f"  https://live.douyin.com/{rid}")

    print("\n请在浏览器打开上述链接,复制地址栏中的数字ID")
    print("然后将 ROOM_ID 填入 crawler/douyin_crawler.py")
