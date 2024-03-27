import re
import os
from ..config.config import LOGICAL_DIR

class parser_analysis:
    def __init__(self, columns_type):
        self.columns_type = columns_type
        self.keywords = {"and", "or", "not", "like", "between", "max", "min", "sum", "count", "avg", "NULL"}

    def process_expression(self, expression):
        i = 0
        new_expression = ""
        while i < len(expression):
            if expression[i].isspace():
                i += 1
                continue

            if expression[i] == "'":  # 单引号变量
                i, var = self.read_until(expression, i + 1, "'")
              #  new_expression += f"'{var}'"
            elif expression[i] == '"':  # 双引号列名
                i, var = self.read_until(expression, i + 1, '"')
              #  new_expression += f'"{var}"'
            elif expression[i].isalpha():  # 可能是列名或关键词
                i, var = self.read_until(expression, i, " ", "(", ")", "=", "<", ">", "+", "-", "*", "/", "!")
                if var.lower() not in self.keywords:
                    var = self.add_table_name(var)
                if not var.startswith("expr:"):
                    print(var)
             #   new_expression += var
            else:
              #  new_expression += expression[i]
                i += 1
        new_expression = expression
        return new_expression

    def read_until(self, string, start, *stop_chars):
        end = start
        while end < len(string) and not any(string[end] == ch for ch in stop_chars):
            end += 1
        return end, string[start:end]


    def add_table_name(self, column_name):
        for table, columns in self.columns_type.items():
            if column_name in columns:
                return f'"{table}.{column_name}"'
        return column_name  # 如果找不到合适的表名，返回原始列名

    @staticmethod
    def is_alphanumeric_string(s):
        """
        Check if a string starts with an alphabet and consists of only alphabets and numbers.
        :param s: string to check
        :return: True if the condition is met, False otherwise
        """
        # Check if the string is non-empty and starts with an alphabet
        if s and s[0].isalpha():
            # Check if all characters are either alphabets, numbers, or underscores
            return all(c.isalnum() or c == '_' for c in s)
        return False
    
    @staticmethod
    def find_table(column_name, columns_type):
        for table, columns in columns_type.items():
            if column_name in columns:
                return f'"{table}.{column_name}"'
        return column_name  # 如果找不到合适的表名，返回原始列名