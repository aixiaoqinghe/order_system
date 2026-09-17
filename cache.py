"""
order_system Redis 缓存封装
依赖：pip install redis
"""
import json
import random
import redis

import config

# 连接 Redis (和 consumer 幂等用的是同一个)
redis_client = redis.Redis(
    host = config.REDIS_HOST,
    port = config.REDIS_PORT,
    db = config.REDIS_DB,
    decode_responses = True,
    socket_connect_timeout = 3,
)

# 空值缓存标记：区分“订单不存在”和“缓存没命中”
NULL_FLAG = "__NULL__"

# 缓存 key 前缀
ORDER_CACHE_PREFIX = "order:cache:"

def _cache_key(order_id: str) -> str:
    return f"{ORDER_CACHE_PREFIX}{order_id}"

def _random_ttl(base: int, jitter: int) -> int:
    """基础 TTL + 随机偏移，防雪崩"""
    return base + random.randint(0, jitter)

def get_order_from_cache(order_id: str):
    """
    从缓存读订单
    返回：
    - dict: 命中真实数据
    - None: 缓存没命中（要去查 DB ）
    - "NULL": 命中空值标记(订单不存在，直接返回404)
    """
    key = _cache_key(order_id)
    cached = redis_client.get(key)
    if cached is None:
        return None            # 没命中，查 DB
    if cached == NULL_FLAG:
        return "NULL"          # 命中空值，订单不存在
    return json.loads(cached)  # 命中真实数据

def set_order_cache(order_id: str, order: dict):
    """写入订单缓存， TTL 加随机防雪崩"""
    key = _cache_key(order_id)
    ttl = _random_ttl(config.CACHE_TTL, config.CACHE_TTL_JITTER)
    redis_client.setex(key, ttl, json.dumps(order, ensure_ascii=False))

def set_null_cache(order_id: str):
    key = _cache_key(order_id)
    redis_client.setex(key, config.NULL_CACHE_TTL, NULL_FLAG)

def delete_order_cache(order_id: str, status: str = None):
    """删除订单缓存（更新订单时用）"""
    redis_client.delete(_cache_key(order_id))