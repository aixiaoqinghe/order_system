"""
并发测试：两个请求同时更新同一订单，验证分布式锁
依赖：pip install requests
"""

import threading
import requests

BASE_URL = "http://localhost:8000"
ORDER_ID = "order-001"
STATUS = "paid"

# 用 Barrier让两个线程“同时”发出请求
barrier = threading.Barrier(2)

results = []                # 存每个线程的结果
lock = threading.Lock()     # 保护 results 的写

def update_order(thread_name):
    """一个线程：等所有线程就绪后，同时发请求"""
    # 等两个线程都到齐，一起冲
    barrier.wait()
    try:
        resp = requests.put(
            f"{BASE_URL}/order/{ORDER_ID}/status",
            params = {"status": STATUS},
            timeout = 5,
        )
        with lock:
            results.append((thread_name, resp.status_code, resp.json()))
    except Exception as e:
        with lock:
            results.append((thread_name, "ERROR", str(e)))

if __name__ == "__main__":
    print("=== 并发测试：两个请求同时更新同一订单 ===\n")

    t1 = threading.Thread(target=update_order, args=("线程A",))
    t2 = threading.Thread(target=update_order, args=("线程B",))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print("结果:")
    for name, status, body in results:
        print(f"{name}: HTTP {status} -> {body}")

    # 统计
    success = sum(1 for _, s, _ in results if s == 200)
    conflict = sum(1 for _, s, _ in results if s ==409)
    print(f"\n成功:{success} 冲突(被锁拦):{conflict}")
