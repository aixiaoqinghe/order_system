"""生产者：模拟用户下单"""

import json
import uuid
from mq_client import MQClient
import config

def create_order(order_id: str, amount: float):
    """创建订单并发送到MQ"""
    client = MQClient()
    client.connect(enable_confirm=True)   # 关键：开启Confirm
    client.setup_order_infrastructure()

    message = json.dumps({
        'message_id': str(uuid.uuid4()),
        'order_id': order_id,
        'amount': amount,
        'timestamp': __import__('time').time()
    })

    success = client.publish(
        routing_key = config.ORDER_ROUTING_KEY,
        message = message.encode('utf-8')
    )

    if success:
        print(f"✅ 订单已提交: {order_id}")
    else:
        print(f"❌ 订单提交失败: {order_id}")
    client.close()

if __name__ == '__main__':
    "模拟下3单：2 正常 + 1 坏"
    create_order('order_001', 99.9)
    create_order('order_002', 199.9)
    create_order('order_bad_003', 299.9)