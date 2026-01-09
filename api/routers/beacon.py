# -*- coding: utf-8 -*-
"""
Beacon 舆情爬虫 API
为 Java 后端提供异步爬取接口，直接将结果写入数据库
"""

import asyncio
import uuid
from datetime import datetime, date
from typing import Dict, List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, BackgroundTasks, HTTPException
import pymysql
import os

router = APIRouter(prefix="/beacon", tags=["beacon"])

# ==================== 模型定义 ====================

class CrawlBrandRequest(BaseModel):
    brand_id: str
    brand_name: str
    keywords: Optional[List[str]] = None  # 可选自定义关键词

class CrawlBatchRequest(BaseModel):
    brands: List[CrawlBrandRequest]

class TaskStatus(BaseModel):
    task_id: str
    status: str  # pending, running, completed, failed
    brand_id: str
    brand_name: str
    crawled_count: int = 0
    saved_count: int = 0
    error: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None

# ==================== 任务管理 ====================

# 内存中的任务状态存储
_tasks: Dict[str, TaskStatus] = {}

def get_db_connection():
    """获取数据库连接"""
    return pymysql.connect(
        host=os.getenv('BEACON_DB_HOST', 'localhost'),
        port=int(os.getenv('BEACON_DB_PORT', 3306)),
        user=os.getenv('BEACON_DB_USER', 'root'),
        password=os.getenv('BEACON_DB_PASSWORD', '123456'),
        database=os.getenv('BEACON_DB_NAME', 'beacon'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# ==================== 爬虫逻辑 ====================

# B2B 商业纠纷关键词 (用于分类过滤)
PAYMENT_KEYWORDS = ["结款", "账期", "拖欠", "扣款", "回款", "不给钱", "催款", "款项", "尾款", "定金"]
WORKFLOW_KEYWORDS = ["媒介", "审稿", "改稿", "pr", "brief", "对接人", "合同", "发票", "kol", "koc", "mcn"]
COOPERATION_KEYWORDS = ["广子", "置换", "寄拍", "报备", "商单", "种草", "恰饭", "推广", "植入", "带货"]
ATTITUDE_KEYWORDS = ["已读不回", "跑路", "骗稿", "白嫖", "失联", "拉黑", "鸽", "不靠谱", "避雷", "黑名单"]

C2C_PRODUCT_KEYWORDS = ["闷痘", "过敏", "烂脸", "爆痘", "难吃", "口感", "假货", "山寨", "掉色", "起球"]
C2C_CONSUMER_KEYWORDS = ["回购", "购买", "下单", "买了", "用了", "性价比", "价格", "便宜"]

def classify_content(content: str) -> dict:
    """分类内容是否为 B2B 商业纠纷"""
    if not content:
        return {"is_b2b": False, "confidence": 0, "risk_tags": []}
    
    lower_content = content.lower()
    b2b_score = 0
    c2c_score = 0
    risk_tags = []
    
    # 计算 B2B 得分
    for kw in PAYMENT_KEYWORDS:
        if kw in lower_content:
            b2b_score += 3
            risk_tags.append("款项问题")
    for kw in WORKFLOW_KEYWORDS:
        if kw.lower() in lower_content:
            b2b_score += 2
    for kw in COOPERATION_KEYWORDS:
        if kw in lower_content:
            b2b_score += 1.5
    for kw in ATTITUDE_KEYWORDS:
        if kw in lower_content:
            b2b_score += 3
            risk_tags.append("态度问题")
    
    # 计算 C2C 得分
    for kw in C2C_PRODUCT_KEYWORDS:
        if kw in lower_content:
            c2c_score += 2
    for kw in C2C_CONSUMER_KEYWORDS:
        if kw in lower_content:
            c2c_score += 1
    
    # 判断
    is_b2b = b2b_score >= 3 and b2b_score > c2c_score * 1.5
    confidence = min(0.95, 0.5 + b2b_score * 0.03) if is_b2b else 0
    
    return {
        "is_b2b": is_b2b,
        "confidence": confidence,
        "risk_tags": list(set(risk_tags))
    }

def analyze_sentiment(content: str) -> str:
    """简单情感分析"""
    if not content:
        return "neutral"
    
    negative_kws = ["避雷", "差评", "拖欠", "不推荐", "坑", "垃圾", "失望", "问题", "投诉"]
    positive_kws = ["推荐", "好评", "满意", "及时", "专业", "良心", "赞", "喜欢", "优秀"]
    
    for kw in negative_kws:
        if kw in content:
            return "negative"
    for kw in positive_kws:
        if kw in content:
            return "positive"
    return "neutral"

async def run_crawler_task(task_id: str, brand_id: str, brand_name: str, keywords: Optional[List[str]] = None):
    """
    处理爬虫数据任务（方案 A：读取已爬取的 JSON 文件）
    
    使用方式：
    1. 先手动运行 MediaCrawler 爬取数据
    2. 然后调用此 API 将数据导入数据库
    """
    import json
    import glob
    
    task = _tasks.get(task_id)
    if not task:
        return
    
    task.status = "running"
    
    try:
        # 默认关键词（用于匹配 JSON 中的 source_keyword）
        if not keywords:
            keywords = [
                f"{brand_name} 博主避雷",
                f"{brand_name} 品牌方 避雷",
                f"{brand_name} 媒介 避雷",
                f"{brand_name} 合作 避雷",
                f"{brand_name} 结款",
                f"{brand_name} 商单 踩坑",
                f"{brand_name} 避雷",  # 简单匹配
            ]
        
        # 读取 JSON 文件
        crawler_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        data_dir = os.path.join(crawler_dir, "data", "xhs", "json")
        
        all_notes = []
        
        if os.path.exists(data_dir):
            # 获取所有 search JSON 文件
            files = glob.glob(os.path.join(data_dir, "search_*.json"))
            print(f"[Beacon] Found {len(files)} JSON files in {data_dir}")
            
            for json_file in files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                    # 过滤匹配当前品牌关键词的数据
                    for item in data:
                        source_keyword = item.get('source_keyword', '')
                        # 检查是否包含品牌名
                        if brand_name in source_keyword:
                            all_notes.append(item)
                except Exception as e:
                    print(f"[Beacon] Error reading {json_file}: {e}")
                    continue
        
        print(f"[Beacon] Found {len(all_notes)} notes for brand: {brand_name}")
        task.crawled_count = len(all_notes)
        
        # 分类过滤并写入数据库
        if all_notes:
            try:
                conn = get_db_connection()
                with conn.cursor() as cursor:
                    for note in all_notes:
                        title = note.get('title', '')
                        desc = note.get('desc', '')
                        content = f"{title} {desc}"
                        
                        # 分类
                        classification = classify_content(content)
                        if not classification['is_b2b']:
                            print(f"[Beacon] Skipped C2C content: {title[:30]}...")
                            continue
                        
                        # 检查是否已存在
                        note_id = note.get('note_id', '')
                        cursor.execute(
                            "SELECT id FROM sentiment_notes WHERE note_id = %s",
                            (note_id,)
                        )
                        if cursor.fetchone():
                            print(f"[Beacon] Note already exists: {note_id}")
                            continue
                        
                        # 插入数据
                        cursor.execute("""
                            INSERT INTO sentiment_notes 
                            (brand_id, source, note_id, title, summary, url, publish_date, 
                             sentiment, risk_tags, classification_confidence, is_commercial)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            brand_id,
                            'xiaohongshu',
                            note_id,
                            title[:255] if title else '',
                            desc[:2000] if desc else '',
                            note.get('note_url', ''),
                            date.today(),
                            analyze_sentiment(content),
                            ','.join(classification['risk_tags']),
                            classification['confidence'],
                            True
                        ))
                        task.saved_count += 1
                        print(f"[Beacon] Saved: {title[:30]}...")
                    
                    conn.commit()
                conn.close()
                print(f"[Beacon] Saved {task.saved_count} notes to database")
            except Exception as e:
                print(f"[Beacon] Database error: {e}")
                task.error = str(e)
        
        task.status = "completed"
        task.completed_at = datetime.now()
        print(f"[Beacon] Task completed: crawled={task.crawled_count}, saved={task.saved_count}")
        
    except Exception as e:
        task.status = "failed"
        task.error = str(e)
        task.completed_at = datetime.now()
        print(f"[Beacon] Task failed: {e}")

# ==================== API 路由 ====================

@router.post("/crawl/brand")
async def crawl_brand(request: CrawlBrandRequest, background_tasks: BackgroundTasks):
    """提交单品牌爬取任务"""
    task_id = str(uuid.uuid4())
    
    task = TaskStatus(
        task_id=task_id,
        status="pending",
        brand_id=request.brand_id,
        brand_name=request.brand_name,
        created_at=datetime.now()
    )
    _tasks[task_id] = task
    
    # 添加后台任务
    background_tasks.add_task(
        run_crawler_task,
        task_id,
        request.brand_id,
        request.brand_name,
        request.keywords
    )
    
    return {
        "success": True,
        "task_id": task_id,
        "message": f"爬取任务已提交: {request.brand_name}"
    }

@router.post("/crawl/batch")
async def crawl_batch(request: CrawlBatchRequest, background_tasks: BackgroundTasks):
    """提交批量品牌爬取任务"""
    task_ids = []
    
    for brand in request.brands:
        task_id = str(uuid.uuid4())
        task = TaskStatus(
            task_id=task_id,
            status="pending",
            brand_id=brand.brand_id,
            brand_name=brand.brand_name,
            created_at=datetime.now()
        )
        _tasks[task_id] = task
        task_ids.append(task_id)
        
        background_tasks.add_task(
            run_crawler_task,
            task_id,
            brand.brand_id,
            brand.brand_name,
            brand.keywords
        )
    
    return {
        "success": True,
        "task_ids": task_ids,
        "message": f"已提交 {len(task_ids)} 个爬取任务"
    }

@router.get("/task/{task_id}")
async def get_task_status(task_id: str):
    """查询任务状态"""
    task = _tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "task_id": task.task_id,
        "status": task.status,
        "brand_id": task.brand_id,
        "brand_name": task.brand_name,
        "crawled_count": task.crawled_count,
        "saved_count": task.saved_count,
        "error": task.error,
        "created_at": task.created_at.isoformat(),
        "completed_at": task.completed_at.isoformat() if task.completed_at else None
    }

@router.get("/tasks")
async def list_tasks(limit: int = 20):
    """列出最近的任务"""
    sorted_tasks = sorted(_tasks.values(), key=lambda t: t.created_at, reverse=True)
    return {
        "tasks": [
            {
                "task_id": t.task_id,
                "status": t.status,
                "brand_name": t.brand_name,
                "saved_count": t.saved_count,
                "created_at": t.created_at.isoformat()
            }
            for t in sorted_tasks[:limit]
        ]
    }

@router.get("/brands")
async def list_brands():
    """从数据库获取品牌列表"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name FROM brands ORDER BY name LIMIT 100")
            brands = cursor.fetchall()
        conn.close()
        return {
            "success": True,
            "count": len(brands),
            "brands": [{"id": str(b["id"]), "name": b["name"]} for b in brands]
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.post("/crawl/all-brands")
async def crawl_all_brands(background_tasks: BackgroundTasks, limit: int = 10):
    """
    从 brands 表读取品牌列表，自动导入已爬取的数据
    
    使用方式：
    1. 先手动运行 MediaCrawler 爬取品牌数据
    2. 调用此接口自动匹配品牌并导入数据库
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name FROM brands ORDER BY name LIMIT %s", (limit,))
            brands = cursor.fetchall()
        conn.close()
        
        if not brands:
            return {"success": False, "message": "没有找到品牌数据", "count": 0}
        
        task_ids = []
        for brand in brands:
            brand_id = str(brand["id"])
            brand_name = brand["name"]
            
            task_id = str(uuid.uuid4())
            task = TaskStatus(
                task_id=task_id,
                status="pending",
                brand_id=brand_id,
                brand_name=brand_name,
                created_at=datetime.now()
            )
            _tasks[task_id] = task
            task_ids.append({"task_id": task_id, "brand_name": brand_name})
            
            background_tasks.add_task(
                run_crawler_task,
                task_id,
                brand_id,
                brand_name,
                None
            )
        
        return {
            "success": True,
            "message": f"已为 {len(brands)} 个品牌提交导入任务",
            "tasks": task_ids
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
