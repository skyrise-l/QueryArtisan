import re
import os
from ..config.config import *
import sqlite3
import subprocess
import traceback
import time
import pymysql

class run_time:
    @staticmethod
    def replace_path_in_file(file_path, old_path, new_path):
        try:
            # 打开文件并读取内容
            with open(file_path, 'r', encoding='utf-8') as file:
                file_contents = file.read()

            # 替换路径
            updated_contents = file_contents.replace(old_path, new_path)

            # 写回到文件
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(updated_contents)

            print("路径替换完成。")
        except Exception as e:
            print(f"处理文件时出错: {e}")


    @staticmethod
    def run(file_path, query_table):
        conn = sqlite3.connect(file_path)
        conn.row_factory = sqlite3.Row   
        cursor = conn.cursor()

        cursor.execute(f"SELECT id, evaluate FROM {query_table}")

        rows = cursor.fetchall()

        for row in rows:
            if row['evaluate'] == "1":
                file = str(row['id']) + "_1"  + '.py'
                targetdir = CODE_DIR
                path = os.path.join(targetdir, file)
                run_time.replace_path_in_file(path, "/mnt/d/数据库/GPT3_project/src", "/home/lwh")
                time.sleep(1)
            totalTime = 0
            for i in range(5):
                
                try:
                    start_time = time.time()
                    result = subprocess.run(["python3", file], cwd=targetdir)
                    totalTime += time.time() - start_time
                except Exception as e:
                    traceback_info = traceback.format_exc()
                    print(f"execute code error: {traceback_info}")
                    print(f"执行文件时发生错误: {e}")
                    # 打印错误堆栈跟踪信息
                    print("运行代码发生错误 row :" + row['id'])
                    break
                time.sleep(2)

            totalTime /= 5.0

            query = f"UPDATE {query_table} SET gpt_time = ? WHERE id = ?"
            params = (totalTime, row['id'])
            cursor.execute(query, params)

            conn.commit()

        cursor.close()
        conn.close()
    
    @staticmethod
    def reset_mysql_cache(cursor):
        try:
            cursor.execute("FLUSH TABLES;")
            cursor.execute("RESET QUERY CACHE;")
        except pymysql.MySQLError as e:
            print(f"Failed to reset cache: {e}")


    @staticmethod
    def run_mysql(file_path, query_table):
        # MySQL 数据库配置
        mysql_config = {
            'user': 'root',
            'password': 'password',
            'host': '127.0.0.1',
        }

        sqlite_conn = sqlite3.connect(file_path)
        sqlite_conn.row_factory = sqlite3.Row   
        cursor = sqlite_conn.cursor()

        cursor.execute(f"SELECT id, db_id, sql,sql_time, evaluate FROM {query_table}")

        rows = cursor.fetchall()

        for row in rows:
            if not row['sql_time']:#row['evaluate'] == "1" and not row['sql_time']:
                print(row['id'])
                try:
                    with pymysql.connect(**mysql_config) as mysql_conn:
                        try:
                            with mysql_conn.cursor() as mysql_cursor:
                                row_id = row['id']
                                db_id = row['db_id']
                                sql = row['sql']

                                # 切换到对应的 MySQL 数据库
                                mysql_cursor.execute(f"USE {db_id}")
                                print("Executing SQL for ID:", row_id)
                                print(sql)

                                total_query_time = 0
                                for _ in range(4):
                                    start_time = datetime.now()
                                    mysql_cursor.execute(sql)
                                    results = mysql_cursor.fetchall()
                                    end_time = datetime.now()
                                    query_duration = (end_time - start_time).total_seconds()
                                    total_query_time += query_duration
                                    time.sleep(2)
                                    print("query_duration :" + str(query_duration))
                                    # 重置 MySQL 缓存的函数调用
                                    run_time.reset_mysql_cache(mysql_cursor)

                                average_query_duration = total_query_time / 4
                                print("average_query_duration :" + str(average_query_duration))
                                update_query = f"UPDATE {query_table} SET sql_time = {average_query_duration} WHERE id = {row_id}"
                                sqlite_conn.execute(update_query)
                                sqlite_conn.commit()
                        except (pymysql.MySQLError, sqlite3.Error) as e:
                            print(f"An error occurred: {e}")
                            continue  # 跳过当前行，继续处理下一行
                except pymysql.MySQLError as e:
                    print(f"Error connecting to MySQL: {e}")
        

                # 关闭 SQLite 数据库连接
        sqlite_conn.close()
