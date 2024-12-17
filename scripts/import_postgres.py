import os
import psycopg2
import csv

# 数据库连接配置
db_host = "localhost"
db_user = "postgres"
db_password = "newpassword"

# 目标文件夹路径
target_folder = "/mnt/d/数据库/GPT3_project/src/GPT3/data/bird"

# 连接到PostgreSQL的默认数据库
conn = psycopg2.connect(dbname='postgres', user=db_user, password=db_password, host=db_host)
conn.autocommit = True
cursor = conn.cursor()

# 遍历目标文件夹下的子文件夹
for database_folder in os.listdir(target_folder):
    database_path = os.path.join(target_folder, database_folder)

    # 检查是否为文件夹
    if os.path.isdir(database_path):
        # 检查数据库是否存在
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_folder}'")
        if cursor.fetchone() is None:
            # 创建数据库
            cursor.execute(f"CREATE DATABASE {database_folder}")
            print(f"Database {database_folder} created.")
        else:
            print(f"Database {database_folder} already exists.")

        # 连接到新创建的数据库
        db_conn = psycopg2.connect(dbname=database_folder, user=db_user, password=db_password, host=db_host)
        db_cursor = db_conn.cursor()

        # 读取database_description文件
        description_folder = os.path.join(database_path, "database_description")
        for description_file in os.listdir(description_folder):
            if description_file.endswith(".csv"):
                with open(os.path.join(description_folder, description_file), mode='r') as file:
                    reader = csv.DictReader(file)
        
                    table_name = description_file.replace(".csv", "")
                    
                    # 检查表是否存在
                    quoted_table_name = f'"{table_name}"'
                    print(table_name)
                    db_cursor.execute(f"SELECT to_regclass('{quoted_table_name}')")
                    if db_cursor.fetchone()[0] is None:
                        create_table_query = f"CREATE TABLE {quoted_table_name} ("
                        
                        # 构建创建表的SQL语句
                        columns = []
                        for row in reader:
                            column_name = row['original_column_name'].strip()
                           #if column_name[0].isdigit() or ' ' in column_name or column_name == "similar" or '-' in column_name or '/' in column_name or column_name == "end" or '+' in column_name or column_name.lower() == "group" or column_name == "Primary":
                            column_name = f'"{column_name}"'
                            data_type = row['data_format']
                            if data_type == 'integer':
                                columns.append(f"{column_name} INTEGER")
                            elif data_type == 'double':
                                columns.append(f"{column_name} DOUBLE PRECISION")
                            elif data_type == 'date':
                               # columns.append(f"{column_name} DATE")
                                columns.append(f"{column_name} TEXT")
                            else:  # 默认为text
                                columns.append(f"{column_name} TEXT")
                        
                        create_table_query += ", ".join(columns) + ")"
                        print(create_table_query)
                        db_cursor.execute(create_table_query)
                        print(f"Table {quoted_table_name} created in database {database_folder}.")
                    else:
                        print(f"Table {quoted_table_name} already exists in database {database_folder}.")
            db_conn.commit()
        
        db_cursor.close() 
        db_conn.close()

cursor.close()
conn.close()

print("数据库和表创建完毕，数据导入完成。")