"""FastAPI入口：提供HTTP下单接口"""

import json
import uuid
import logging
import time
import threading
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from mq_client import MQClient
import config
from datetime import datetime
import db
import cache

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title = "订单系统")

# 全局MQ客户端（启动时连接）
mq_client: MQClient = None

@app.on_event("startup")
def startup():
    """应用启动时建立MQ连接 + 声明基础设施"""
    global mq_client
    mq_client = MQClient()
    mq_client.connect(enable_confirm=True)   # 关键：开启Confirm
    mq_client.setup_order_infrastructure()
    logger.info("✅ FastAPI 启动，MQ 已就绪")

@app.on_event("shutdown")
def shutdown():
    """应用关闭时释放连接"""
    global mq_client
    if mq_client:
        mq_client.close()

class OrderRequest(BaseModel):
    """下单请求体"""
    order_id: str = Field(..., description="订单ID,含'bad'会模拟失败")
    amount: float = Field(..., description = "金额，必须大于0")

@app.post("/order")
def create_order(req: OrderRequest):
    """
    创建订单接口

    流程：接收请求 -> 发消息到MQ -> 立即返回
    实际处理由consumer.py异步完成
    """
    if not mq_client:
        raise HTTPException(status_code=503, detail="MQ未就绪")

    # 构造消息（含全局唯一 message_id 用于幂等）
    message = json.dumps({
        'message_id': str(uuid.uuid4()),
        'order_id': req.order_id,
        'amount': req.amount
    })

    # 发送到MQ（带Confirm确认）
    success = mq_client.publish(
        routing_key = config.ORDER_ROUTING_KEY,
        message = message.encode('utf-8')
    )

    if not success:
        raise HTTPException(status_code=500,detail="消息发送失败")

    return {
        "code": 0,
        "msg": "订单已提交，处理中",
        "order_id": req.order_id
    }

def _serialize_order(order:dict) -> dict:
    """
    把 DB 返回的订单 dict 转成可 JSON 序列化的格式
    datetime -> 字符串
    Decimal -> float(金额)
    """
    if not order:
        return order
    result = dict(order)
    if isinstance(result.get("create_time"), datetime):
        result["create_time"] = result["create_time"].strftime("%Y-%m-%d %H:%M:%S")
    if result.get("amount") is not None:
        result["amount"] = float(result["amount"])
    return result

def _serialize_order(order:dict) -> dict:
    """datetime -> str, Decimal -> float, 方便 JSON 序列化"""
    if not order:
        return order
    result = dict(order)
    if isinstance(result.get("create_time"), datetime):
        result["create_time"] = result["create_time"].strftime("%Y-%m-%d %H:%M:%S")
    if result.get("amount") is not None:
        result["amount"] = float(result["amount"])
    return result

@app.get("/order/{order_id}")
def get_order(order_id: str):
    """
    查询订单（带缓存）
    流程：查缓存 -> miss 查 DB -> 写缓存
    """
    # 1.查缓存
    cached = cache.get_order_from_cache(order_id)
    if cached == "NULL":
        logger.info(f"🟡 命中空值缓存，订单不存在: {order_id}")
        raise HTTPException(status_code=404, detail=f"订单不存在：{order_id}")
    if cached is not None:
        logger.info(f"🟢 缓存命中: {order_id}")
        return cached

    # 2.缓存没命中，查 DB
    logger.info(f"🔴 缓存未命中，查 DB: {order_id}")
    order = db.get_order(order_id) 
    if not order:
        # 3. DB 也没有 -> 写空值缓存，防穿透
        cache.set_null_cache(order_id)
        raise HTTPException(status_code=404, detail=f"订单不存在:{order_id}")
    # 4. DB 有 -> 写缓存，返回
    serialized = _serialize_order(order)
    cache.set_order_cache(order_id, serialized)
    logger.info(f"📝 已写入缓存: {order_id}")
    return serialized

@app.put("/order/{order_id}/status")
def update_order_status(order_id:str, status: str):
    """
    更新订单状态（延迟双删）
    流程：删缓存 -> 更新 DB -> 延迟再删缓存
    """
    # 1.检查订单是否存在
    order = db.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail=f"订单不存在:{order_id}")

    # 2.先删一次缓存
    cache.delete_order_cache(order_id)

    # 3.更新 DB
    db.update_order_status(order_id, status)

    # 4.延迟再删一次（异步，不阻塞请求）
    def delayed_delete():
        time.sleep(config.CACHE_DOUBLE_DELETE_DELAY) 
        cache.delete_order_cache(order_id)

    threading.Thread(target=delayed_delete, daemon=True).start()

    return {"order_id": order_id, "status": status, "msg": "更新成功"}

@app.get("/health")
def health():
    """健康检查接口"""
    return {"status": "ok"}