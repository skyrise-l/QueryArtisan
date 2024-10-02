import re
import os
from ..config.config import *
import sqlite3
import subprocess
import traceback
import time
import pandas as pd
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
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()

            # 查找import语句的最后一行
            last_import_index = None
            for i, line in enumerate(lines):
                if line.startswith('import') or line.startswith('from'):
                    last_import_index = i

            if last_import_index is not None:
                lines.insert(last_import_index + 1, 'import time\n')
            else:
                lines.insert(0, 'import time\n')

            # 在最后一个import语句后面插入import time
            # 添加开始时间记录
            lines.insert(last_import_index + 2, '\nstart_time = time.time()\n')

            # 添加结束时间和总时间打印
            lines.append('\nend_time = time.time()\n')
            lines.append('print(end_time - start_time)\n')

            with open(file_path, 'w', encoding='utf-8') as file:
                file.writelines(lines)

            print("计时代码插入完成。")
            
        except Exception as e:
            print(f"处理文件时出错: {e}")

    @staticmethod
    def replace_path_in_file2(file_path, old_path, new_path):
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
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()

            # 查找import语句的最后一行
            last_import_index = None
            for i, line in enumerate(lines):
                if line.startswith('import') or line.startswith('from'):
                    last_import_index = i

            # 查找最后一个.astype(str)或read_csv的位置
            last_astype_or_readcsv_index = None
            for i in range(len(lines) - 1, -1, -1):
                if '.astype(str)' in lines[i] or 'read_csv' in lines[i]:
                    last_astype_or_readcsv_index = i
                    break

            # 查找f.write或to_csv的位置
            write_or_tocsv_index = None
            for i, line in enumerate(lines):
                if '.write' in line or 'to_csv' in line:
                    write_or_tocsv_index = i
                    break

            # 如果找到import语句
            if last_import_index is not None:
                # 在最后一个import语句之后插入时间开始计算的代码
                lines.insert(last_import_index + 1, '\nimport time\n')

            if last_astype_or_readcsv_index is not None and write_or_tocsv_index is not None:
                # 在最后一个.astype(str)或read_csv之后插入开始计时代码
                lines.insert(last_astype_or_readcsv_index + 2, '\nstart_time = time.time()\n')

                # 在f.write或to_csv之前插入结束计时代码
                lines.insert(write_or_tocsv_index, '\nend_time = time.time()\n')
                lines.insert(write_or_tocsv_index + 1, "print(end_time - start_time)\n")

            # 将修改后的内容写回文件
            with open(file_path, 'w', encoding='utf-8') as file:
                file.writelines(lines)

            print("计时代码插入完成。")
            
        except Exception as e:
            print(f"处理文件时出错: {e}")

    @staticmethod
    def run(file_path, query_table):
        conn = sqlite3.connect(file_path)
        conn.row_factory = sqlite3.Row   
        cursor = conn.cursor()

        cursor.execute(f"SELECT id, evaluate, gpt_time, opt_time FROM {query_table}")

        rows = cursor.fetchall()

        for row in rows:
            if row['evaluate'] == "1" and (not row['opt_time'] or (row['opt_time'] and float(row['opt_time']) == 0.0)):
                
                file = str(row['id']) + "_1"  + '.py'
                print(file)
                targetdir = CODE_DIR
                path = os.path.join(targetdir, file)
                run_time.replace_path_in_file(path, "/mnt/d/数据库/GPT3_project/src", "/home/lwh")
                time.sleep(1)
                totalTime = 0
                for i in range(3):
                    try:
                        result = subprocess.run(["python3", file], cwd=targetdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        print(result.stdout)
                        totalTime += float(result.stdout)
                    except Exception as e:
                        traceback_info = traceback.format_exc()
                        print(f"execute code error: {traceback_info}")
                        print(f"执行文件时发生错误: {e}")
                        # 打印错误堆栈跟踪信息
                        print("运行代码发生错误 row :" + row['id'])
                        time.sleep(2)
                        break
                    time.sleep(2)

                totalTime /= 3.0

                query = f"UPDATE {query_table} SET opt_time = ? WHERE id = ?"
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
            'password': 'lwh417',
            'host': '127.0.0.1',
        }

        sqlite_conn = sqlite3.connect(file_path)
        sqlite_conn.row_factory = sqlite3.Row   
        cursor = sqlite_conn.cursor()

        cursor.execute(f"SELECT id, db_id, sql,sql_time,gpt_time, evaluate FROM {query_table}")

        rows = cursor.fetchall()

        for row in rows:
            row_id = row['id']
            sql_time = row['sql_time']
            gpt_time = row['gpt_time']
    
            if float(row['id']) < 1050:
                continue
           # if not pd.isna(sql_time) :#and float(sql_time) > 1:
            #    continue
            if sql_time or not gpt_time:
                continue
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
                            for _ in range(3):
                                start_time = datetime.now()
                                mysql_cursor.execute(sql)
                                results = mysql_cursor.fetchall()
                                end_time = datetime.now()
                                query_duration = (end_time - start_time).total_seconds()
                                total_query_time += query_duration
                                print("query_duration:" + str(query_duration))
                                time.sleep(2)
                                # 重置 MySQL 缓存的函数调用
                                #run_time.reset_mysql_cache(mysql_cursor)

                            average_query_duration = total_query_time / 3
                            print("average_query_duration :" + str(average_query_duration))
                            update_query = f"UPDATE {query_table} SET sql_time = {average_query_duration} WHERE id = {row_id}"
                            sqlite_conn.execute(update_query)
                            sqlite_conn.commit()
                    except (pymysql.MySQLError, sqlite3.Error) as e:
                        print(f"An error occurred: {e}")
                        print("!!!!!!!!!!!!!!!!!!!!!!!")
                        time.sleep(2)
                        continue  # 跳过当前行，继续处理下一行
            except pymysql.MySQLError as e:
                print(f"Error connecting to MySQL: {e}")
        

                # 关闭 SQLite 数据库连接
        sqlite_conn.close()
