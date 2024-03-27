
import os
import re
import json
from ..config.config import DATA_JSON_DIR, DATA_GRAPH_DIR, USER_CONFIG_DIR
from ..utils.read_logical import read_logical

class process_util:

    @staticmethod
    def heart():
        with open('/mnt/d/数据库/GPT3_project/src/heart.log', 'a') as file:
        # 写入心跳信息到文件
            file.write('Heartbeat message: Program is running normally.\n')

    @staticmethod
    def preprocess(db_id, table_id):
        print(db_id, table_id)
   
    @staticmethod
    def check_sql_functions(sql_statement):
        functions = ['COUNT', 'MIN', 'MAX']  # 要检查的SQL函数列表

        # 使用正则表达式匹配函数名
        pattern = r'\b(' + '|'.join(functions) + r')\b'
        match = re.search(pattern, sql_statement, re.IGNORECASE)

        if match:
            return True  # SQL语句中包含指定的函数
        else:
            return False  # SQL语句中不包含指定的函数 

    @staticmethod
    def exact_sql(text):
        # print(text)
        pattern = re.compile(r'```.*\s([\s\S]*?)\s```')
        result = re.findall(pattern, text)
        if len(result) == 0:
            print("未找到代码。")
            return
        else:
            r = ""
            for i in result:
                r += i 
                r += '\n'
            return r
        
    @staticmethod
    def exact_code(message):
        
            pattern = r'```.*?(import pandas.*?)```'

            matches = re.findall(pattern, message, re.DOTALL)
            code = ""

            if not matches:
                print("没有发现代码")
                return message

            for match in matches:
                # 去除末尾的所有反引号并累加到code
                code += re.sub(r'`+$', '', match.strip())

            return code

        
    @staticmethod
    def separator(thread_logger, flag):
        if flag == 1:
            thread_logger.log("##############################################################")
            thread_logger.log("##############################################################")
            thread_logger.log("##############################################################")
        elif flag == 2:
            thread_logger.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            thread_logger.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            thread_logger.log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    @staticmethod  
    def extract_tuples_from_string(data_str):
        # 提取每个元组内的所有值
        pattern = r"\(([^)]+)\)"

        tuples_str = re.findall(pattern, data_str)

        extracted_tuples = []
        for tuple_str in tuples_str:
            values = [v.strip(" '") for v in tuple_str.split(',')]
            extracted_tuples.append(values)
            
        return extracted_tuples
    
    @staticmethod  
    def list_files(directory):
        file_list = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_list.append(file)

        return file_list
    
    @staticmethod  
    def list_graph_files(directory):
        result = {}

        for item in os.listdir(directory):
            if os.path.isdir(os.path.join(directory, item)):
                folder_name = item
                nodes_files = []
                edges_files = []

                for file in os.listdir(os.path.join(directory, folder_name)):
                    file_path = os.path.join(directory, folder_name, file)
                    if os.path.isfile(file_path):
                        if 'nodes' in file:
                            nodes_files.append(file)
                        elif 'edges' in file:
                            edges_files.append(file)

                result[folder_name] = {'nodes_files': nodes_files, 'edges_files': edges_files}


        return result

    @classmethod
    def create_nested_structure(cls, obj):
            structure = {}
            if isinstance(obj, dict):
                for key, value in obj.items():
                    structure[key] = process_util.create_nested_structure(value)
            elif isinstance(obj, list) and obj:
                # Only creating structure from the first item if it's a list
                structure = process_util.create_nested_structure(obj[0])
            return structure

    @staticmethod  
    def JSON_nested_structure(filename):

        file_path =os.path.join(DATA_JSON_DIR, filename) 

        with open(file_path, 'r') as file:
            data = json.load(file)
        
        # Creating the nested structure
        nested_structure = process_util.create_nested_structure(data)

        return nested_structure

    @staticmethod  
    def get_example(opt, dbname):
        if dbname == "wikisql":
            example_flle = "example3.txt"
        else:
            if opt:
                example_flle = "example2.txt"
            else :
                example_flle = "example2.txt"
        file_path =os.path.join(USER_CONFIG_DIR, example_flle) 

        with open(file_path, 'r') as file:
            file_content = file.read()

        return file_content
    
    @staticmethod  
    def gen_plan(steps, columns_type):
        uid = 1
        message = ""
        for operation, Target_columns, Target_steps, Details in steps:
            if operation != "read" and operation != "write":
                Target_columns = read_logical.del_table_ref(Target_columns, columns_type)
                Details = read_logical.del_table_ref(Details, columns_type)
            message += "Step " + str(uid) + ": "
            message += "Operator: " + operation + ".\n"
            message += "Target_columns: " + Target_columns + ".\n"
            message += "Target_steps: " + Target_steps + ".\n"
            message += "Details: " + Details + ".\n\n"
            uid += 1

        return message