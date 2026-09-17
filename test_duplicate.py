import json
import uuid
from mq_client import MQClient
import config

client = MQClient()
client.connect(enable_confirm=True)
client.setup_order_infrastructure()

# 用同一个message_id发两次
fixed_id = "test-fixed-id-001"

for i in range(2):
    msg = json.dumps({
        'message_id': fixed_id,   # 相同ID
        'order_id': f'order-dup-{i}',
        'amount': 99.9
    })
    client.publish(config.ORDER_ROUTING_KEY, msg.encode())
    print(f"发送第{i+1}次")

client.close()