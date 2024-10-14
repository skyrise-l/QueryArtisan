
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

    def is_subset_sorted2(self, set1, set2):
        # 在没有明显行标识符的情况下，查询的数据没有意义，在这种情况下，LLM 可能会多查询一列作为行的标识，这种情况认为是正确的。
        if len(set1) != len(set2):
            return False
    
        for row1 in set1:
            if not any(set(row1).issubset(row2) for row2 in set2):
                return False
        return True
    
    def is_subset_sorted(self, set1, set2):
        if len(set1) != len(set2):
            return False
    
        def is_subsequence(list1, list2):
            it = iter(list2)
            return all(any(c == ch for c in it) for ch in list1)
        
        for row1 in set1:
            if not any(is_subsequence(row1, row2) for row2 in set2):
                return False
        return True
        
    def special_case_check(self, df_csv, df_txt_gpt):
        csv_empty = df_csv.empty
        gpt_empty = df_txt_gpt.empty

        if csv_empty and gpt_empty:
            return True

        csv_single_zero_row = len(df_csv) == 1 and (df_csv.iloc[0] == 0).all()
        gpt_single_zero_row = len(df_txt_gpt) == 1 and (df_txt_gpt.iloc[0] == 0).all()
        return (csv_empty and gpt_single_zero_row) or (gpt_empty and csv_single_zero_row)

   
    def new_evaluate(self, id):
        try:
            gpt_path = GPT_RESULT_DIR + str(id) + ".txt"
            sql_path = SQL_RESULT_DIR + str(id) + ".csv"
            nan_replacement = 0
            df_csv = pd.read_csv(sql_path, low_memory=False)
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

            
            if df_csv.shape == df_txt_gpt.shape:
                if df_csv.equals(df_txt_gpt):
                    return True
                
            if self.special_case_check(df_csv, df_txt_gpt):
                return True

            if self.is_subset_sorted2(set_csv, set_txt_comma):
                return True  

        except Exception as e:
            traceback_info = traceback.format_exc()
            self.thread_logger.log(f"compare code error: {traceback_info}")
            self.thread_logger.log(f"比较文件结果时发生错误: {e}")
            return False


        return False
    

    def evaluate(self, id):
        try:
            gpt_path = GPT_RESULT_DIR + str(id) + ".txt"
            sql_path = SQL_RESULT_DIR + str(id) + ".csv"
            nan_replacement = "NaN"

            df_csv = pd.read_csv(sql_path, low_memory=False)
            df_csv.fillna(nan_replacement, inplace=True)
            self.round_floats(df_csv)
            set_csv = {tuple(row) for row in df_csv.to_numpy()}

   
            try:
                df_txt_gpt = pd.read_csv(gpt_path, sep=",", low_memory=False)
            except pd.errors.EmptyDataError:
           
                df_txt_gpt = pd.DataFrame()
            
            df_txt_gpt.fillna(nan_replacement, inplace=True)
            self.round_floats(df_txt_gpt)
            set_txt_comma = {tuple(row) for row in df_txt_gpt.to_numpy()}

            if df_csv.shape == df_txt_gpt.shape:
                if df_csv.equals(df_txt_gpt):
                    return True

            if self.is_subset_sorted(set_csv, set_txt_comma):
                return True  

            df_txt_gpt_no_header = pd.read_csv(gpt_path, sep=",", low_memory=False, header=None)
            df_txt_gpt_no_header.fillna(nan_replacement, inplace=True)
            self.round_floats(df_txt_gpt_no_header)
            set_txt_comma_noh = {tuple(row) for row in df_txt_gpt_no_header.to_numpy()}

            if df_csv.shape == df_txt_gpt_no_header.shape:
                if df_csv.equals(df_txt_gpt_no_header):
                    return True
            
            if self.is_subset_sorted(set_csv, set_txt_comma_noh):
                return True

            df_tab_gpt = pd.read_csv(gpt_path, sep="\t", low_memory=False)
            df_tab_gpt.fillna(nan_replacement, inplace=True)
            self.round_floats(df_tab_gpt)
            set_txt_tab = {tuple(row) for row in df_tab_gpt.to_numpy()}

            if self.is_subset_sorted(set_csv, set_txt_tab):
                return True
        
            df_tab_gpt_no_header = pd.read_csv(gpt_path, sep=",", low_memory=False, header=None)
            df_tab_gpt_no_header.fillna(nan_replacement, inplace=True)
            self.round_floats(df_tab_gpt_no_header)
            set_tab_comma_noh = {tuple(row) for row in df_tab_gpt_no_header.to_numpy()}
            
            if self.is_subset_sorted(set_csv, set_tab_comma_noh):
                return True
        
        except Exception as e:
            traceback_info = traceback.format_exc()

            self.thread_logger.log(f"compare code error: {traceback_info}")
            self.thread_logger.log(f"比较文件结果时发生错误: {e}")
            return False
        
        return False
    

    def old_evaluate(self, data_str, id):
        path = GPT_RESULT_DIR + str(id) + ".csv"

        tuples_data = process_util.extract_tuples_from_string(data_str)

        csv_is_empty = True
        all_rows_matched = True

        with open(path, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            csv_rows = [[cell.strip() for cell in row] for row in csvreader]
            for row in csv_rows:
     
                csv_is_empty = False  
                if not any(set(row) == set(each_tuple) for each_tuple in tuples_data):
                    all_rows_matched = False
                
        if (csv_is_empty and not tuples_data) or all_rows_matched:
            return True
        else:
            
            return False
        

