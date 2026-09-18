import config
import uuid

from common.redis_conn import redis_client

# Lua:只有 value 是自己的才删
UNLOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

def acquire_lock(order_id: str, expire: int = None):
    """
    加锁：SET NX EX, value 用 UUID
    返回：成功返回 value (用于释放), 失败返回 None
    """
    if expire is None:
        expire = config.LOCK_EXPIRE
    key = f"{config.LOCK_KEY_PREFIX}{order_id}"
    value = str(uuid.uuid4())
    got = redis_client.set(key, value, nx = True, ex = expire)
    return value if got else None

def release_lock(order_id: str, value: str):
    """
    释放锁： Lua 校验 value,只删自己的锁
    返回：1 = 删除成功，0 = 不是自己的锁（不删）
    """
    key = f"{config.LOCK_KEY_PREFIX}{order_id}"
    return redis_client.eval(UNLOCK_LUA, 1, key, value)
