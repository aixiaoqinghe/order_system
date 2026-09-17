"""消费者：处理订单（幂等 + 重试 + 死信）"""

import json
import logging
import redis
import pika
from mq_client import MQClient
import config

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s'
)
logging.getLogger('pika').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ========== Redis 连接 ==========
redis_client = redis.Redis(
    host = 'localhost',
    port = 6379,
    db = 0,
    decode_responses = True
)

# 幂等键的过期时间（秒），防止Redis无限增长
IDEMPOTENT_TTL = 3600   # 1小时

def is_duplicate(message_id: str) -> bool:
    """
    检查消息是否重复（幂等性核心）

    使用 Redis SETNX:
    - 返回 True: 第一次处理（键不存在，SET成功）
    - 返回 False:重复消息（键已存在，SET失败）
    """
    key = f"order:processes:{message_id}"

    # SETNX + EXPIRE 原子操作（用set的nx = True + ex参数）
    is_first_time = redis_client.set(
        key,
        "1",
        nx = True,  # 只有键不存在时设置
        ex = IDEMPOTENT_TTL   # 过期时间
    )

    # is_first_time 为True表示设置成功（第一次）
    # 为None表示键已存在（重复）
    return is_first_time is None

def callback(ch, method, properties, body):
    """消息处理"""
    headers = properties.headers or {}
    retry_count = headers.get('x-retry-count', 0)

    try:
        data = json.loads(body.decode('utf-8'))
        message_id = data['message_id']
        order_id = data['order_id']
        amount = data['amount']

        # # ===== 1. Redis 幂等性检查 =====
        if is_duplicate(message_id):
            logger.warning(f"⚠️ 重复消息，跳过: {message_id}")
            ch.basic_ack(method.delivery_tag)
            return

        logger.info(f"📥 处理订单 (重试:{retry_count}): {order_id}")

        # ===== 2. 模拟业务 =====
        if 'bad' in order_id: 
            raise ValueError(f"订单 {order_id} 处理失败")

        # ===== 3. 落库 =====
        from db import insert_order  
        # 返回 False = 主键冲突（订单已存在，重复消费）
        # 这种情况不该重试，仍然ACK
        # 抛异常 = MySQL 挂了等系统错误，走重试
        inserted = insert_order(order_id, amount)
        if not inserted:
            logger.warning(f"⚠️ 订单已存在，跳过落库: {order_id}")

        # ===== 4.处理成功 =====
        logger.info(f"✅ 订单处理成功: {order_id}")
        ch.basic_ack(delivery_tag = method.delivery_tag)

    except Exception as e:
        logger.error(f"❌ 处理失败: {e}")

        # ===== 5. 重试或进死信 =====
        if retry_count < config.MAX_RETRIES:
            new_count = retry_count + 1
            logger.info(f"🔄 第 {new_count} 次重试...")
            redis_client.delete(f"order:processes:{message_id}")
            ch.basic_publish(
                exchange = config.ORDER_EXCHANGE,
                routing_key = config.ORDER_ROUTING_KEY,
                body = body,
                properties = pika.BasicProperties(
                    delivery_mode = 2,
                    headers = {'x-retry-count': new_count}
                )
            )
            ch.basic_ack(delivery_tag = method.delivery_tag)
        else:
            logger.error(f"💀 超过最大重试次数，进死信队列")
            redis_client.delete(f"order:processes:{message_id}")
            ch.basic_nack(delivery_tag = method.delivery_tag, requeue = False)

def main():
    # 启动前检查Redis连接
    try:
        redis_client.ping()
        logger.info("✅ Redis 连接成功")
    except redis.ConnectionError:
        logger.error("❌ Redis 连接失败，请先启动 Redis")
        return

    client = MQClient()
    client.connect()
    client.setup_order_infrastructure()

    ch = client._channel
    ch.basic_qos(prefetch_count = config.PREFETCH_COUNT)
    ch.basic_consume(
        queue = config.ORDER_QUEUE,
        on_message_callback = callback,
        auto_ack = False
    )

    logger.info("📥 订单消费者启动，等待消息...")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    finally:
        client.close()

if __name__ == '__main__':
    main()