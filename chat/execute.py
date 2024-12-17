
import subprocess
import traceback
import os
from ..config.config import CODE_DIR, GPT_RESULT_DIR, SQL_RESULT_DIR
import re
import csv
from ..utils.process_util import process_util
import pandas as pd


class execute_plan:
    def __init__(self, file_flag, thread_logger, code_flag = 1):
        self.file_flag = file_flag
        self.thread_logger = thread_logger
        self.code_flag = code_flag
        return 

    def execute_code(self, id):
        file = str(id) + "_" + str(self.code_flag) + '.py'
        targetdir = CODE_DIR

        file_path = GPT_RESULT_DIR + str(id) + ".txt"
        if os.path.exists(file_path):
            os.remove(file_path)

        try:
            result = subprocess.run(["python3", file], capture_output=True, text=True, cwd=targetdir)

        except Exception as e:
            traceback_info = traceback.format_exc()
            self.thread_logger.log(f"execute code error: {traceback_info}")
            self.thread_logger.log(f"执行文件时发生错误: {e}")

        return result
    
    def round_floats(self, df):
        for col in df.select_dtypes(include=['float']):
            df[col] = df[col].apply(lambda x: round(x, 2))

    def is_equals(self, set1, set2):
        # 在没有明显行标识符的情况下，查询的数据没有意义，在这种情况下，LLM 可能会多查询一列作为行的标识，这种情况认为是正确的。
        # 之所以不考虑行出现的顺序，在于我们的参照答案是通过数据库SQL查询出来的，这个顺序和数据分析的默认顺序有时候并不一致，如果强行要求每一行顺序相同并不合理。
        if len(set1) != len(set2):
            return False
    
        for row1 in set1:
            if not any(set(row1).issubset(row2) for row2 in set2):
                return False
        return True
    
    def evaluate(self, id):
        try:
            gpt_path = GPT_RESULT_DIR + str(id) + ".txt"
            sql_path = SQL_RESULT_DIR + str(id) + ".csv"
            nan_replacement = 0
            df_csv = pd.read_csv(sql_path, low_memory=False)
            df_csv.dropna(how='all', inplace=True)
            df_csv.fillna(nan_replacement, inplace=True)
            self.round_floats(df_csv)
            set_csv = [tuple(row) for row in df_csv.to_numpy()]

            try:
                df_txt_gpt = pd.read_csv(gpt_path, sep=",", low_memory=False)
            except pd.errors.EmptyDataError:
                df_txt_gpt = pd.DataFrame()
            
            df_txt_gpt.fillna(nan_replacement, inplace=True)
            self.round_floats(df_txt_gpt)
            set_txt_comma = [tuple(row) for row in df_txt_gpt.to_numpy()]

            if self.is_equals(set_csv, set_txt_comma):
                return True  

        except Exception as e:
            traceback_info = traceback.format_exc()
            self.thread_logger.log(f"compare code error: {traceback_info}")
            self.thread_logger.log(f"比较文件结果时发生错误: {e}")
            return False


        return False
    