import mysql.connector
import re

class LogicalPlanDatabaseHandler:
    def __init__(self, host, user, password, database):
        """
        初始化数据库连接并创建必要的表。

        Args:
            host (str): 数据库主机名。
            user (str): 数据库用户名。
            password (str): 数据库密码。
            database (str): 数据库名称。
        """
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.initialize_database()

    def initialize_database(self):
        """初始化用于存储逻辑计划的数据库表。"""
        cursor = self.connection.cursor()
        # 修改 logical_plans 表，添加 plan_text 字段
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS logical_plans (
            id INT AUTO_INCREMENT PRIMARY KEY,
            num_nodes INT,
            plan_text TEXT
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS logical_plan_nodes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            logical_plan_id INT,
            node_order INT,
            operation VARCHAR(255),
            FOREIGN KEY (logical_plan_id) REFERENCES logical_plans(id)
        )
        """)
        self.connection.commit()
        cursor.close()

    def parse_logical_plan(self, plan_text):
        """
        解析逻辑计划文本，提取操作列表，支持新的 'Step' 格式。

        Args:
            plan_text (str): 逻辑计划的字符串表示。

        Returns:
            List[str]: 提取的操作列表。
        """
        nodes = []
        plan_text = plan_text.strip()
        steps = re.split(r'Step \d+:', plan_text)
        for step in steps[1:]:  # 跳过第一个空元素
            # 提取 'Operator: xxx'
            match = re.search(r'Operator:\s*(.*)', step)
            if match:
                operation = match.group(1).strip().lower()
                nodes.append(operation)
        return nodes

    def save_logical_plan(self, plan_text):
        """
        将逻辑计划保存到 MySQL 数据库中。

        Args:
            plan_text (str): 逻辑计划的字符串表示。

        Returns:
            int: 保存的逻辑计划的 ID。
        """
        # 解析计划以获取操作列表
        operations = self.parse_logical_plan(plan_text)
        num_nodes = len(operations)

        cursor = self.connection.cursor()
        # 插入到 logical_plans 表，存储 plan_text
        cursor.execute(
            "INSERT INTO logical_plans (num_nodes, plan_text) VALUES (%s, %s)",
            (num_nodes, plan_text)
        )
        plan_id = cursor.lastrowid

        # 将节点插入到 logical_plan_nodes 表
        for idx, operation in enumerate(operations):
            cursor.execute(
                "INSERT INTO logical_plan_nodes (logical_plan_id, node_order, operation) VALUES (%s, %s, %s)",
                (plan_id, idx, operation)
            )
        self.connection.commit()
        cursor.close()
        print(f"Logical plan saved with ID: {plan_id}")
        return plan_id

    def compare_logical_plan(self, input_plan_text):
        """
        比较输入的逻辑计划与数据库中的记录，找到节点数量和操作序列完全对应的记录。

        Args:
            input_plan_text (str): 输入的逻辑计划的字符串表示。

        Returns:
            List[str]: 匹配的完整逻辑计划内容列表。
        """
        input_operations = self.parse_logical_plan(input_plan_text)
        num_nodes = len(input_operations)

        cursor = self.connection.cursor(dictionary=True)

        # 查找具有相同节点数量的逻辑计划
        cursor.execute("SELECT id, plan_text FROM logical_plans WHERE num_nodes = %s", (num_nodes,))
        plans = cursor.fetchall()

        matching_plans = []

        for plan in plans:
            plan_id = plan['id']
            plan_text = plan['plan_text']
            # 获取此计划的操作列表
            cursor.execute(
                "SELECT operation FROM logical_plan_nodes WHERE logical_plan_id = %s ORDER BY node_order",
                (plan_id,)
            )
            rows = cursor.fetchall()
            plan_operations = [row['operation'] for row in rows]

            # 比较操作列表
            if plan_operations == input_operations:
                matching_plans.append(plan_text)

        cursor.close()
        return matching_plans

    def close_connection(self):
        """关闭数据库连接。"""
        self.connection.close()
