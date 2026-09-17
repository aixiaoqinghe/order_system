import pika
import logging
import time
from typing import Optional, Dict, Any
import config

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class MQClient:
    def __init__(self):
        self._params = pika.ConnectionParameters(
            host = config.RABBITMQ_HOST,
            port = config.RABBITMQ_PORT,
            credentials = pika.PlainCredentials(
                config.RABBITMQ_USER, config.RABBITMQ_PASS
            ),
            heartbeat = config.RABBITMQ_HEARTBEAT
        )
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.Channel] = None

    def connect(self, enable_confirm: bool = False):
        """建立连接，enable_confirm = True开启生产者确认"""
        self._connection = pika.BlockingConnection(self._params)
        self._channel = self._connection.channel()

        if enable_confirm:
            self._channel.confirm_delivery()
            logger.info("✅ Publisher Confirm 已开启")

        logger.info("✅ RabbitMQ 连接成功")
        return self

    def ensure_connection(self):
        if not self._connection or self._connection.is_closed:
            logger.warning("连接断开，重连中...")
            self.connect()
        return self._channel

    def setup_order_infrastructure(self):
        """一次性声明所有交换机/队列（幂等）"""
        ch = self.ensure_connection()

        # 死信交换机 + 死信队列
        ch.exchange_declare(
            exchange = config.DLX_EXCHANGE,
            exchange_type = config.DLX_EXCHANGE_TYPE,
            durable = True
        )
        ch.queue_declare(
            queue = config.DLX_QUEUE,
            durable = True
        )

        ch.queue_bind(
            queue = config.DLX_QUEUE,
            exchange = config.DLX_EXCHANGE,
            routing_key = config.DLX_ROUTING_KEY
        )

        # 主交换机
        ch.exchange_declare(
            exchange = config.ORDER_EXCHANGE,
            exchange_type = config.ORDER_EXCHANGE_TYPE,
            durable = True
        )

        # 主队列（挂死信配置）
        ch.queue_declare(
            queue = config.ORDER_QUEUE,
            durable = True,
            arguments = {
                'x-dead-letter-exchange': config.DLX_EXCHANGE,
                'x-dead-letter-routing-key': config.DLX_ROUTING_KEY
            }
        )
        ch.queue_bind(
            queue = config.ORDER_QUEUE,
            exchange = config.ORDER_EXCHANGE,
            routing_key = config.ORDER_ROUTING_KEY
        )

        logger.info("✅ 订单基础设施声明完成")

    def publish(self, routing_key: str, message: bytes, headers:Optional[Dict[str, Any]] = None) -> bool:
        """发布消息(带Confirm确认)"""
        try:
            ch = self.ensure_connection()
            ch.basic_publish(
                exchange = config.ORDER_EXCHANGE,
                routing_key = routing_key,
                body = message,
                properties = pika.BasicProperties(
                    delivery_mode = 2,
                    headers = headers or {}
                ),
                mandatory = True
            )
            logger.info(f"📤 消息已确认发送: routing_key={routing_key}")
            return True
        except pika.exceptions.UnroutableError:
            logger.error("❌ 消息无法路由")
            return False
        except pika.exceptions.NackError:
            logger.error("❌ Broker 拒绝消息")
            return False

    def close(self):
        if self._connection and not self._connection.is_closed:
            self._connection.close()
            logger.info("🔌 连接已关闭")