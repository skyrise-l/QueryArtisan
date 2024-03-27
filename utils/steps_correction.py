
from ..config.config import *
import re
import os
from ..config.config import LOGICAL_DIR
from ..utils.parser_analysis import parser_analysis
from dateutil.parser import parse

from datetime import datetime

class Step:
    def __init__(self, operation, target_columns, details, order):
        self.operation = operation
        self.target_columns = target_columns
        self.details = details
        self.order = order
        self.dependent_steps = []

class StepManager:
    def __init__(self, columns_type, initial_steps=None):
        self.steps = {}
        self.columns_type = columns_type
        self.current_order = 0
        if initial_steps:
            self.initialize_steps(initial_steps)

    def insert_step_at_beginning(self, operation, target_columns, details, target_steps=None):

        new_order = 0
        new_step = Step(operation, target_columns, details, new_order)
        self.steps[new_order] = new_step


        for order, step in list(self.steps.items())[1:]:  
            step.order += 1
            self.steps[step.order] = step
        self.current_order += 1

        if target_steps:
            for target_step_order in target_steps:
                target_step = self.steps.get(target_step_order + 1)
                if target_step:
                    target_step.dependent_steps.append(new_step)

    def add_step(self, operation, target_columns, details, target_steps=None):
        order = self.current_order
        new_step = Step(operation, target_columns, details, order)
        self.steps[order] = new_step
        self.current_order += 1
   
        if target_steps:
            for target_step_order in target_steps:
                target_step = self.steps.get(target_step_order)
                if target_step:
            
                    new_step.dependent_steps.append(target_step)

        return new_step

    def remove_step(self, order):
        if order in self.steps:
            del self.steps[order]
            for step in self.steps.values():
                step.dependent_steps = [dep for dep in step.dependent_steps if dep.order != order]

    def get_step(self, order):
        return self.steps.get(order)

    def update_step(self, order, operation=None, target_columns=None, details=None, target_steps=None):
        step = self.get_step(order)
        if step:
            step.operation = operation if operation is not None else step.operation
            step.target_columns = target_columns if target_columns is not None else step.target_columns
            step.details = details if details is not None else step.details
            if target_steps is not None:
                step.dependent_steps = []
                for target_step_order in target_steps:
                    target_step = self.get_step(target_step_order)
                    if target_step:
                        step.dependent_steps.append(target_step)

    def parse_target_steps(self, target_steps_str):
        if target_steps_str:
            numbers = re.findall(r'\d+', target_steps_str)
            return [int(num) - 1 for num in numbers] 
        else:
            return []
        

    def initialize_steps(self, initial_steps):
        for operation, Target_columns, Target_steps, Details in initial_steps:
       

            Target_steps = self.parse_target_steps(Target_steps)
            self.add_step(operation, Target_columns, Details, Target_steps)

class steps_correction():
    @staticmethod
    def check_foreign_key_relation(table1, col1, table2, col2, table_key):
  
        def is_foreign_key_to(fk_table, fk_col, pk_table, pk_col):
            for fk in table_key[fk_table].get('ret_fks', []):
                for fk_column, pk_reference in fk.items():
                    pk_table_ref, pk_column_ref = pk_reference.split('.')
                    if fk_col == fk_column and pk_table == pk_table_ref and pk_col == pk_column_ref:
                        return True
            return False

        return is_foreign_key_to(table1, col1, table2, col2) or is_foreign_key_to(table2, col2, table1, col1)

    @staticmethod
    def get_correct_join_condition(table1, table2, table_key):
        for fk in table_key[table1]['ret_fks']:
            for fk_col, pk_col in fk.items():
                if pk_col.startswith(table2 + '.'):
                    return f'"{table1}.{fk_col}" = "{pk_col}"'
        for fk in table_key[table2]['ret_fks']:
            for fk_col, pk_col in fk.items():
                if pk_col.startswith(table1 + '.'):
                    return f'"{table2}.{fk_col}" = "{pk_col}"'
        return ''  

    @staticmethod
    def read_correction(step_manager, columns_type, data):
        all_tables = list(columns_type.keys())
        read_tables = []

        for step in step_manager.steps.values():
            if step.operation == 'read' and any(table in step.details for table in all_tables):
                read_tables.extend([table for table in all_tables if table in step.details])

        missing_tables = set(all_tables) - set(read_tables)
        for table in missing_tables:
      
            file_path = os.path.join(DATA_CONFIG_DIR, data['db_id'], table + ".csv")
            message = f"(1) use pandas to read {table}.csv As {table}, file_path is :'{file_path}'. (2) Perform data preprocessing on {table}"
            step_manager.insert_step_at_beginning('read', "None", message)

    @staticmethod
    def join_correction(step_manager, table_key, data):
        for step in step_manager.steps.values():
            if step.operation == 'join':
                pattern = r'join\s*Step\s*(\d+)\s*on\s*(.*?)(?=\s+join|\n|$)'
                join_match = re.search(pattern, step.details, re.IGNORECASE)
                
                if join_match:
                    
                    join_conditions = join_match.group(2)
            
                 
                    condition_matches = re.findall(r'\"?[\w\.\s]+\"?\s*=\s*\"?[\w\.\s]+\"?', join_conditions)
            
                    for condition in condition_matches:
                  
                        condition_pattern = r'\"?([\w\.]+)\.(\w+)\"?\s*=\s*\"?([\w\.]+)\.(\w+)\"?'
                        condition_match = re.search(condition_pattern, condition)
                        if condition_match:
                            table1, col1, table2, col2 = condition_match.groups()
                            table1 = table1.strip().rstrip('.').strip('"')
                            table2 = table2.strip().rstrip('.').strip('"')
                            col1 = col1.strip().rstrip('.').strip('"')
                            col2 = col2.strip().rstrip('.').strip('"')

                            if not steps_correction.check_foreign_key_relation(table1, col1, table2, col2, table_key):
                             
                                correct_condition = steps_correction.get_correct_join_condition(table1, table2, table_key)
                                step.details = step.details.replace(condition, correct_condition)

    @staticmethod
    def simple_logical_correction(steps, columns_type, table_key, data):

        step_manager = StepManager(columns_type, steps)

        # optional
        steps_correction.read_correction(step_manager, columns_type, data)
        steps_correction.join_correction(step_manager, table_key, data)

        formatted_steps = []
        for order in sorted(step_manager.steps.keys()):
            step = step_manager.get_step(order)
            
            if step.dependent_steps:
                target_steps = ', '.join(f"Step {dep_step.order + 1}" for dep_step in step.dependent_steps)
            else:
                target_steps = 'None'

            formatted_steps.append((step.operation, step.target_columns, target_steps, step.details))

        return formatted_steps
