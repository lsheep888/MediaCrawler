# -*- coding: utf-8 -*-
"""
批量爬取所有品牌数据
从数据库获取品牌列表，生成搜索关键词，并运行爬虫
"""

import subprocess
import sys
import os
import pymysql
import time

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('BEACON_DB_HOST', 'localhost'),
    'port': int(os.getenv('BEACON_DB_PORT', 3306)),
    'user': os.getenv('BEACON_DB_USER', 'root'),
    'password': os.getenv('BEACON_DB_PASSWORD', '123456'),
    'database': os.getenv('BEACON_DB_NAME', 'beacon'),
    'charset': 'utf8mb4',
}

def get_brands(limit=10):
    """从数据库获取品牌列表"""
    conn = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name FROM brands ORDER BY name LIMIT %s", (limit,))
            return cursor.fetchall()
    finally:
        conn.close()

def generate_keywords(brand_name):
    """为品牌生成搜索关键词"""
    return [
        f"{brand_name} 博主避雷",
        f"{brand_name} 品牌方 避雷",
        f"{brand_name} 媒介 避雷",
        f"{brand_name} 合作 避雷",
        f"{brand_name} 结款",
        f"{brand_name} 商单 踩坑",
    ]

def run_crawler_for_brand(brand_name, keywords):
    """为品牌运行爬虫"""
    # 更新配置文件中的关键词
    keywords_str = ",".join(keywords)
    
    # 设置环境变量
    env = os.environ.copy()
    env['MEDIA_CRAWLER_KEYWORDS'] = keywords_str
    
    print(f"\n{'='*60}")
    print(f"[爬取品牌] {brand_name}")
    print(f"[搜索关键词] {keywords_str}")
    print(f"{'='*60}")
    
    # 运行爬虫
    try:
        result = subprocess.run(
            [sys.executable, 'main.py'],
            env=env,
            timeout=300,  # 5分钟超时
            capture_output=True,
            text=True
        )
        print(f"[输出] {result.stdout[-500:] if len(result.stdout) > 500 else result.stdout}")
        if result.stderr:
            print(f"[错误] {result.stderr[-200:]}")
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"[超时] 爬取超时")
        return False
    except Exception as e:
        print(f"[异常] {e}")
        return False

def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser(description='批量爬取品牌数据')
    parser.add_argument('--limit', type=int, default=10, help='爬取品牌数量')
    parser.add_argument('--brand', type=str, help='指定单个品牌名称')
    args = parser.parse_args()
    
    if args.brand:
        # 爬取单个品牌
        keywords = generate_keywords(args.brand)
        run_crawler_for_brand(args.brand, keywords)
    else:
        # 爬取多个品牌
        brands = get_brands(args.limit)
        print(f"[开始] 从数据库获取到 {len(brands)} 个品牌")
        
        success_count = 0
        for i, brand in enumerate(brands, 1):
            brand_name = brand['name']
            print(f"\n[进度] {i}/{len(brands)}")
            
            keywords = generate_keywords(brand_name)
            if run_crawler_for_brand(brand_name, keywords):
                success_count += 1
            
            # 品牌之间等待一段时间
            if i < len(brands):
                print("[等待] 5秒后继续下一个品牌...")
                time.sleep(5)
        
        print(f"\n[完成] 成功爬取 {success_count}/{len(brands)} 个品牌")

if __name__ == "__main__":
    main()
