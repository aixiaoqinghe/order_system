"""
公共 Redis 连接————所有 Redis 用途（缓存/锁/计数器）共用这一个
"""
import redis
import config

redis_client = redis.Redis(
    host = config.REDIS_HOST,
    port = config.REDIS_PORT,
    db = config.REDIS_DB,
    decode_responses = True,
    socket_connect_timeout = 3,
)