"""
order_system MySQL 连接封装
提供：插入订单、查询订单
依赖：pip install pymysql
"""

import pymysql
from pymysql.cursors import DictCursor   # 游标返回字典，方便按字段名取值
import config

def get_connection():
    """
    创建 MySQL 连接
    每次调用新建连接：学习够用，生产要用连接池(如 DBUtils)
    """
    try:
        conn = pymysql.connect(
            host = config.MYSQL_HOST,
            port = config.MYSQL_PORT,
            user = config.MYSQL_USER,
            password = config.MYSQL_PASSWORD,
            database = config.MYSQL_DATABASE,
            charset = 'utf8mb4',
            cursorclass = DictCursor,     # 查询结果以 dict 返回
            autocommit = False,           # 手动提交，保证事务
        )
        return conn
    except pymysql.Error as e:
        print(f"[ERROR] MySQL 连接失败：{e}")
        raise

def insert_order(order_id, amount, status = "created"):
    """
    插入订单
    返回 True = 成功， False = 失败
    """
    conn = None
    try:
        conn = get_connection()
        # cursor = conn.cursor()   # 拿游标（用来执行SQL）
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO orders (order_id, amount, status)
                VALUES (%s, %s, %s)
            """
            cursor.execute(sql, (order_id, amount, status))
        conn.commit()
        return True
    except pymysql.err.IntegrityError:
        # 主键冲突 = 订单已存在（重复消费），不算错误
        if conn:
            conn.rollback()
        print(f"[WARN] 订单已存在，跳过：{order_id}")
        return False
    except pymysql.MySQLError as e:
        if conn:
            conn.rollback()
        print(f"[ERROR] 插入订单失败：{e}")
        return False
    finally:
        if conn:
            conn.close()

def get_order(order_id):
    """
    查询订单
    返回 dict (订单存在) 或 None (不存在)
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "SELECT order_id, amount, status, create_time FROM orders WHERE order_id = %s"
            cursor.execute(sql, (order_id,))
            return cursor.fetchone()   # 没有则返回None
    except pymysql.MySQLError as e:
        print(f"[ERROR] 查询订单失败：{e}")
        return None
    finally:
        if conn:
            conn.close()

def update_order_status(order_id, status):
    """更新订单状态"""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = "UPDATE orders SET status = %s WHERE order_id = %s"
            cursor.execute(sql, (status, order_id))
            conn.commit()
            return cursor.rowcount > 0    # cursor.rowcount是PyMySQL游标对象在执行完insert/update/delete后，会把MySQL返回的受影响行数赋值给rowcount
    except pymysql.MySQLError as e:
        if conn:
            conn.rollback()
        print(f"[ERROR] 更新订单状态失败：{e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # 自测：插入一条，再查出来
    test_id = "test-order-001"
    insert_order(test_id, 99.99)
    print("查询结果：", get_order(test_id))