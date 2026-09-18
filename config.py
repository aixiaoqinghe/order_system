"""统一配置管理"""
import os
from dotenv import load_dotenv

load_dotenv()

# RabbitMQ 连接
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
RABBITMQ_HEARTBEAT = 60

# 交换机
ORDER_EXCHANGE = 'order_exchange'
ORDER_EXCHANGE_TYPE = 'topic'
DLX_EXCHANGE = 'order_dlx_exchange'
DLX_EXCHANGE_TYPE = 'direct'

# 队列
ORDER_QUEUE = 'order_queue'
DLX_QUEUE = 'order_dlx_queue'

# 路由键
ORDER_ROUTING_KEY = 'order.created'
DLX_ROUTING_KEY = 'order.dlx'

# 业务参数
MAX_RETRIES = 3
PREFETCH_COUNT = 10

# MySQL 配置
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'order_system')

# Redis 配置（缓存用）
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# 缓存参数
CACHE_TTL = 300                   # 缓存基础过期时间（秒）
CACHE_TTL_JITTER = 60             # 随机偏移，防雪崩
NULL_CACHE_TTL = 60               # 空值缓存过期时间，防穿透
CACHE_DOUBLE_DELETE_DELAY = 1     # 延迟双删的延迟时间（秒）

# 分布式锁
LOCK_EXPIRE = 10                  # 分布式锁过期时间（秒）
LOCK_KEY_PREFIX = "order:lock:"