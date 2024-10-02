import re
import os
from ..config.config import LOGICAL_DIR
from ..utils.parser_analysis import parser_analysis
from dateutil.parser import parse

class DataOperationNode:
    def __init__(self, operation, Target_columns, Target_steps, Details, specific_info, uid, source_table):
        self.operation = operation
        self.Target_columns = Target_columns
        self.details = Details
        self.Target_steps = Target_steps
        self.id = uid
        self.fatherId = 0
        self.children = []
        self.specific_info = specific_info
        self.source_table = source_table

    def add_child(self, child_node):
        self.children.append(child_node)

    def __repr__(self):
        return f"Operation: {{{self.operation}}}, Target_steps: {{{self.Target_steps}}}, Target_columns: {{{self.Target_columns}}}, specific_info: {self.specific_info}, source_table: {self.source_table}"

    
class DataOperationTree:
    def __init__(self):
        self.uid = 1
        self.total_agg_columns = []
        self.total_agg_full_columns = []
        self.total_agg_alias = []
        self.group_columns = []
        self.nodes = {}

    def add_operation(self, operation, Target_columns, Target_steps, Details, specific_info, uid, source_table):
        new_node = DataOperationNode(operation, Target_columns, Target_steps, Details, specific_info, uid, source_table)
        now_name = "step " + str(uid)
        self.nodes[now_name] = new_node

        if operation == "concat" or operation == "join":
                
            if operation == "concat":
                self.nodes[now_name].id = -1
            
    '''
        if input_source in self.nodes:
            find = input_source
            while (find in self.nodes) and (self.nodes[find].id == -1):
                if (self.nodes[find].fatherId == 0):
                    self.nodes[output].fatherId = 0
                    self.nodes[output].input_source = self.nodes[find].specific_info['target1']
                else :
                    find = self.nodes[find].specific_info['target1']
            if find in self.nodes:      
                self.nodes[output].fatherId = self.nodes[find].id
            else :
                self.nodes[output].fatherId = 0
        else:
            self.nodes[output].fatherId = 0

        if (operation == 'concat'):
            self.nodes[output].id = -1
            find1 = specific_info['target1']
            find2 = specific_info['target2']

            if find1 in self.nodes:
                while self.nodes[find1].fatherId != 0:
                    find1 = self.nodes[find1].input_source
                find1 = self.nodes[find1].input_source
   
            if find2 in self.nodes:
                while self.nodes[find2].fatherId != 0:
                    find2 = self.nodes[find2].input_source
                find2 = self.nodes[find2].input_source

            table_map[find1].extend(table_map[find2])
            table_map[find2] = table_map[find1]
    '''
            
    def get_operation(self, output):
        return self.nodes.get(output)

    def __repr__(self):
        return '\n'.join(str(node) for node in self.nodes.values())

    def parse_AS_columns(self, columns_str, columns_type):
        # Define patterns
        aggregate_pattern = r'\b(max|min|sum|avg|count)\b\s*\((\bdistinct\b\s+)?(.*?)\)'
        as_alias_pattern = r'\bAS\b\s+([\w\.]+|\"[\w\s\.]+\")'
        normal_column_pattern =  r'^(\b[\w\.]+|\"[\w\s\.]+\")$'

        # Lists to store parsed columns, aliases, and distinct flags
        columns = []
        aliases = []
        distincts = []

        # Splitting by comma for individual column elements
        column_elements = columns_str.split(',')

        for element in column_elements:
            element = element.strip()
            
            # Check for 'AS' alias
            as_match = re.search(as_alias_pattern, element, re.IGNORECASE)
            if as_match:
                alias = as_match.group(1)
                element = element[:as_match.start()].strip()  # Get the part before 'AS'
            else:
                alias = "0"
           
            # Check for aggregate functions
            agg_match = re.search(aggregate_pattern, element, re.IGNORECASE)
            if agg_match:
                function = agg_match.group(1)
                distinct = 1 if agg_match.group(2) else 0
                column_expr = agg_match.group(3).strip()
                full_column_expr = f'{function}({column_expr})'
                
                inner_columns = re.findall(r'\"[\w\s\.]+\"', column_expr)
                if inner_columns:
                    for inner_column in inner_columns:
                        self.total_agg_columns.append(inner_column)
                        self.total_agg_full_columns.append(full_column_expr)
                        self.total_agg_alias.append(alias)
                    
                    if parser_analysis.is_alphanumeric_string(column_expr):
                        column_expr = parser_analysis.find_table(column_expr, columns_type)
                        full_column_expr = f'{function}({column_expr})'
         
                else:
                    self.total_agg_columns.append(column_expr)
                
                columns.append(full_column_expr)
                aliases.append(alias)
                distincts.append(distinct)
            else:
                # Process as normal column or expression
                if re.match(normal_column_pattern, element, re.IGNORECASE):
                    column = element
                else:
                    column = f'expr:{element}'

                columns.append(column)
                aliases.append(alias)
                distincts.append(0)
  
        return columns, aliases, distincts
    
    def parse_specific_operation_details(self, operation, Target_steps, Target_columns, details, columns_type):
        steps = []
        specific_info = {}
        parse_Instant = parser_analysis(columns_type)
        
        if operation == 'select':
            match = re.search(r'Select\s+(.*?)\s+from', details, re.IGNORECASE)
            if not match:
                raise ValueError(f"Select operation format error: {details}")
            
            columns_str = match.group(1)
            specific_info['columns'], specific_info['aliases'], specific_info['distincts'] = self.parse_AS_columns(columns_str, columns_type)
           # specific_info['columns'] = [parse_Instant.process_expression(i) for i in specific_info['columns']]
            
            steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))

        elif operation == 'filter':
            where_pattern = re.compile(
                r'Step\s+\d+\s+Where\s+(.*?)(?=\s*Step\s+\d+\s+Where|\s*$)', 
                re.IGNORECASE | re.DOTALL
            )

            # 使用正则表达式匹配并处理条件
            matches = where_pattern.findall(details)
            if matches:
                # 直接合并匹配的条件
                conditions = ' '.join(matches).strip().rstrip('.')
                specific_info = {'conditions': conditions}
            else:
                filter_match = re.search(r'Filter\s+(.*?)\s+from', details, re.IGNORECASE)
                if filter_match:
                    specific_info = {'conditions': filter_match.group(1).strip()}
                else:
                    filter_match = re.search(r'Filter\s+(.*?)\s+where', details, re.IGNORECASE)
                    if filter_match:
                        specific_info = {'conditions': filter_match.group(1).strip()}
                    else:
                        raise ValueError("Filter operation format error.")
     
            steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))

        elif operation == 'order_by':
            match = re.search(r'Order\s+Step\s+\d+\s+by\s+(.*?)(\n|$)', details, re.IGNORECASE)
            if not match:
                specific_info['columns'] = []
                specific_info['flags'] = []
                return specific_info

            order_by_part = match.group(1).strip().rstrip('.')
            columns_with_flags = order_by_part.split(',')
            columns = []
            flags = []

            for item in columns_with_flags:
                item = item.strip()
                if ' DESC' in item:
                    columns.append(item.replace(' DESC', ''))
                    flags.append(2)
                elif ' ASC' in item:
                    columns.append(item.replace(' ASC', ''))
                    flags.append(1)
                else:
                    columns.append(item)
                    flags.append(3)

            specific_info['columns'] = columns
            specific_info['flags'] = flags
            steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))

        if operation == 'limit':
            pattern = r"limit\s+(\d+)(?:\s*,\s*(\d+))?"
            match = re.search(pattern, details, re.IGNORECASE)

            if match:
                limit_num, limit_offset = match.groups()
                specific_info['limit_num'] = limit_num
                specific_info['limit_offset'] = limit_offset if limit_offset else "0"
            steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))

        elif operation == 'concat':
            tmp = Target_steps.split(",")
            specific_info_tmp['target'] = [i.strip() for i in tmp]
            steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))

        elif operation == 'join':

            pattern = r'join\s*Step\s*(\d+)\s*on\s*(.*?)(?=\s+join|\n|$)'
            join_match = re.search(pattern, details, re.IGNORECASE)
            
            if join_match:
                
                join_conditions = join_match.group(2)
      
                # Split join conditions
                condition_matches = re.findall(r'\"[\w\.\s]+\"\s*=\s*\"[\w\.\s]+\"', join_conditions)
    
                join_flag = 0
                for condition in condition_matches:
                    # 分割每个 join 条件以获取表名和列名
                    condition_pattern = r'\"(.*?)\.(.*?)\"\s*=\s*\"(.*?)\.(.*?)\"'
                    condition_match = re.search(condition_pattern, condition)
                    if condition_match:
                        table1, col1, table2, col2 = condition_match.groups()
                        table1 = table1.strip().rstrip('.').strip('"')
                        table2 = table2.strip().rstrip('.').strip('"')
                        col1 = col1.strip().rstrip('.').strip('"')
                        col2 = col2.strip().rstrip('.').strip('"')

                        specific_info_tmp = {}
                        specific_info_tmp['table1'] = table1
                        specific_info_tmp['table2'] = table2
                        specific_info_tmp['A_expr'] = f"\"{table1}.{col1}\" = \"{table2}.{col2}\""
                        tmp = Target_steps.split(",")    
                        specific_info_tmp['target'] = [i.strip() for i in tmp]
                
                    if join_flag != 0:
                        steps.append((operation, Target_columns, Target_steps, details, specific_info_tmp, self.uid + join_flag + 100))
                    else :
                        steps.append((operation, Target_columns, Target_steps, details, specific_info_tmp, self.uid))
                    join_flag += 1
                    
 
        elif operation == 'having':
            match = re.search(r'having\s+(.*?)(\n|$)', details, re.IGNORECASE)
            if not match:
                raise ValueError(f"Having operation format error: {details}")
            specific_info['conditions'] = match.group(1).strip().rstrip('.')
            steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))

        elif operation == 'group_by':
            match = re.search(r'Group\s+Step\s+\d+\s+by\s+(.*?)(\n|$)', details, re.IGNORECASE)
            if not match:
                raise ValueError(f"Group by operation format error: {details}")
            specific_info['conditions'] = match.group(1).strip().rstrip('.').split(", ")
            for i in specific_info['conditions']:
                self.group_columns.append(i)
            steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))
        
        elif operation.lower() in ['aggregation', 'count', 'sum', 'min', 'avg', 'max']:
            if Target_columns == "*":  
                specific_info['columns'] = operation + "(*)"
                specific_info['aliases'] = "0" 
                specific_info['distincts'] = 0
                steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))
            else:
                specific_info['columns'], specific_info['aliases'], specific_info['distincts'] = self.parse_AS_columns(details, columns_type)
            #    specific_info['columns'] = [parse_Instant.process_expression(i) for i in specific_info['columns']]
                steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))
        else :
            steps.append((operation, Target_columns, Target_steps, details, specific_info, self.uid))
  
        return steps

    def parse_operations(self, steps, columns_type):
        valid_operations = {'read', 'select', 'order_by', 'distinct', 'filter', 'limit', 'concat', 'join', 'having', 'group_by', 'write', 'aggregation', 'count', 'sum', 'min', 'avg', 'max'}
        node_operations = {'select', 'order_by', 'distinct', 'limit', 'filter','concat', 'having', 'group_by','join', 'aggregation','count', 'sum', 'min', 'avg', 'max'}
       
        results = []
        self.uid = 0
        for operation, Target_steps, Target_columns, Details in steps:
            self.uid += 1

            if operation not in valid_operations:
                raise ValueError("Unsupported operation: " + operation)
            
            if operation not in node_operations:
                continue
          
            specific_infos = self.parse_specific_operation_details(operation, Target_steps, Target_columns, Details, columns_type)
            for i in specific_infos:
                results.append(i)
                
        
        return results

def save_tree_to_file(tree, filename):
    file_path = os.path.join(LOGICAL_DIR, "tree/" + filename[:-4] + "_tree.txt")
    if os.path.exists(file_path):
        # 删除文件
        os.remove(file_path)
        
    with open(file_path, 'w') as file:
        for node in tree.nodes.values():
            if node.id != -1:
                file.write(f"Node: {node}\n")

    with open(file_path, 'r') as file:
        file_contents = file.read()
    
    file_contents = file_contents.replace("\\'", "'").replace('\\"', '"')

    # 将修改后的内容写回文件
    with open(file_path, 'w') as file:
        file.write(file_contents)

def deal_tree(tree, all_tables, columns_type):
    total_agg_columns = tree.total_agg_columns
    total_agg_full_columns = tree.total_agg_full_columns
    total_agg_alias = tree.total_agg_alias
    used_agg_columns = [False] * len(total_agg_columns)
    have_group_columns = tree.group_columns
    groupBy_columns = []
    having_expr_full = ""
    
    for node in tree.nodes.values():
        
        if node.operation == "filter":
            
            conditions = node.specific_info.get("conditions", "")
            having_expr_parts = []
            remaining_conditions_parts = []

            # Split the conditions to process each part
            parts = re.split(r'(\band\b|\bor\b)', conditions, flags=re.IGNORECASE)

            if not parts:
                parts = [conditions]

            column_pattern = r'\"[\w\s\.]+\"'    # 正则表达式匹配 "table_name.column_name"

            was_alias_part = False

            for part in parts:
       
                part = part.strip().rstrip('.')
                isfind = 0
                
                if part.lower() in ['and', 'or']:
                    # If it's a logical operator, add it to the appropriate list
                    if was_alias_part:
                        having_expr_parts.append(part)
                    else:
                        remaining_conditions_parts.append(part)
                    continue
                          
                keywords = {"max(", "min(", "count(", "avg(", "sum("}
                for keyword in keywords:
                    if keyword in part.lower():  
                        having_expr_parts.append(part)
                        was_alias_part = True
                        isfind = 1
                        break
                
                if isfind == 1:
                    continue
                
                for alias, full_column in zip(total_agg_alias, total_agg_full_columns):
                    if alias == "0":
                        continue
                    if alias in part:
                        
                        # Replace alias with full column name for having expression
                        alias_quote = "'" + alias + "'" 
                        part = part.replace(alias_quote, full_column)
                        part = part.replace(alias, full_column)
                        having_expr_parts.append(part)
                        was_alias_part = True
                        break
                else:
                    # If part does not contain alias, it remains in conditions
                    remaining_conditions_parts.append(part)
                    was_alias_part = False

            def remove_trailing_logical_operators(expr):
                expr = expr.strip()
                if expr.endswith(' and'):
                    expr = expr[:-4].strip()
                elif expr.endswith(' or'):
                    expr = expr[:-3].strip()
                return expr
            # Combine parts for having expression and remaining conditions
            having_expr = ' '.join(having_expr_parts).strip()
            remaining_conditions = ' '.join(remaining_conditions_parts).strip()
            having_expr = remove_trailing_logical_operators(having_expr.strip())
            remaining_conditions = remove_trailing_logical_operators(remaining_conditions.strip())

            # Update the node's specific_info with remaining conditions
            node.specific_info["conditions"] = remaining_conditions

            if remaining_conditions == "":
                node.id = -1
            
            if having_expr_full:
                having_expr_full += having_expr + " and "
            else:
                having_expr_full += having_expr

            
    for node in tree.nodes.values():
    
        if total_agg_columns or having_expr_full:
            if node.operation == "select":
                # Extract specific info from the node
                columns = node.specific_info.get("columns", [])
                aliases = node.specific_info.get("aliases", [])
                distincts = node.specific_info.get("distincts", [])
                aggregate_pattern = r'\b(max|min|sum|avg|count)\b\s*\((\bdistinct\b\s+)?(.*?)\)'
                t1 = False
                t2 = False
                for column in columns:
                    agg_match = re.search(aggregate_pattern, column, re.IGNORECASE)

                    if agg_match:
                        t1 = True
                    else:
                        t2 = True
                
                if t1 and t2:
                    for column in columns:
                        agg_match = re.search(aggregate_pattern, column, re.IGNORECASE)
                        if agg_match:
                            column_expr = agg_match.group(3).strip()
                
                            if column_expr not in have_group_columns:
                                groupBy_columns.append(column)


    if having_expr_full:
        column_pattern = r'(?<!\()"[\w\s.]+"(?!\))'    # 正则表达式匹配 "table_name.column_name"
        column_matches = re.findall(column_pattern, having_expr_full)   

        groupBy_columns = list(set(column_matches).union(set(groupBy_columns)))
        specific_info = {}
        specific_info['conditions'] = having_expr_full
        new_node = DataOperationNode("having", "auto", "auto", "group by", specific_info, 100, list(all_tables))
        uid = tree.uid + 1
        tree.uid += 1
        now_name = "step " + str(uid)
        tree.nodes[now_name] = new_node

    
    #for column in total_agg_columns:
      #  if column not in have_group_columns and column not in groupBy_columns:
      #      groupBy_columns.append(column)
    
    if groupBy_columns:
        specific_info = {}
        specific_info['conditions'] = list(set(groupBy_columns))
        new_node = DataOperationNode("group_by", "auto", "auto", "group by", specific_info, 100, list(all_tables))
        uid = tree.uid + 1
        tree.uid += 1
        now_name = "step " + str(uid)
        tree.nodes[now_name] = new_node

    if total_agg_columns:
        for node in tree.nodes.values():
            if node.operation == "aggregation":
                columns = node.specific_info.get("columns", [])

                # Iterate over columns and remove those that match used aggregate columns
                columns = [col for i, col in enumerate(columns) if not used_agg_columns[i]]

                # Update the node's specific_info
                node.specific_info["columns"] = columns

                # If no columns left, set node id to -1
                if not columns:
                    node.id = -1        

    last_node = list(tree.nodes.values())[-1]
    last_node.source_table = list(all_tables)
    

class read_logical():
    @staticmethod
    def del_table_ref(string, columns_type):
    # 第一步：替换所有被双引号包裹的 'table.column' 为 'column'
        for table, columns in columns_type.items():
            for column in columns:
                # 匹配被双引号包裹的 'table.column'
                pattern_quoted = rf'"{re.escape(table)}\.{re.escape(column)}"'
                string = re.sub(pattern_quoted, f'"{column}"', string)

                # 匹配不被双引号包裹的 'table.column'
                pattern = rf'{re.escape(table)}\.{re.escape(column)}'
                string = re.sub(pattern, column, string)

        # 第二步：替换剩余的 'table.' 为 ' '
        for table in columns_type.keys():
            pattern = rf'{re.escape(table)}\.'
            string = re.sub(pattern, ' ', string)

        return string

    @staticmethod
    def add_table_map(details, uid, table_map):

        # 使用正则表达式匹配所有的 *.csv 中的 *
        matches = re.findall(r'(\w+)\.csv', details)
        
        unique_matches = set(matches)

        matches = list(unique_matches)

        if matches:
            cleaned_matches = [match for match in matches]
        else :
            raise Exception("This is not found csv_filenmae.")
        
        now_name = "step " + str(uid)
        table_map[now_name] = cleaned_matches

        # 使用正则表达式匹配 'as' 到 'from' 之间的内容
        match = re.search(r"read\s+['\"]\S+['\"]\s+as\s+(.+?)\s+from", details)

        if match:
            extracted_name = match.group(1).strip()
            table_map[extracted_name] = cleaned_matches[0]

    @staticmethod
    def correct_table_name(string, columns_dict):
        # Gather all column names across tables for error correction
        all_columns = set()
        for columns in columns_dict.values():
            all_columns.update(columns)

        # Function to replace incorrect table names based on columns_dict
        def replacement_func(match):
            table, column = match.groups()
            # If the column exists in a different table, correct the table name
            if column in all_columns:
                correct_table = next(t for t, cols in columns_dict.items() if column in cols)
                return f'"{correct_table}.{column}"'
            else:
                return match.group(0)

        # Apply the correction
        # Pattern to match 'table.column' that are not already quoted
        pattern = rf'(?<!")(\b\w+)\.(\w+)(?!\w|"|\.)'
        corrected_string = re.sub(pattern, replacement_func, string, flags=re.IGNORECASE)

        return corrected_string
    
    @staticmethod
    def process_variables(operation, target_columns, target_steps, details, table_map, columns_type):
        # 替换表名为初始别名
        def replace_table_names(string, map_dict):
            for key, values in map_dict.items():
                if not key.lower().startswith('step'):
                    # Pattern to match ' table_name.', '(table_name.', or at the start of the string
                    pattern = rf'(?<!\w){key}\b(?=\.)'
                    if isinstance(values, list):
                        for value in values:
                            string = re.sub(pattern, value, string, flags=re.IGNORECASE)
                    else:
                        string = re.sub(pattern, values, string, flags=re.IGNORECASE)
            return string

        # Replace table names in all variables
        operation = replace_table_names(operation, table_map)
        target_columns = replace_table_names(target_columns, table_map)
        target_steps = replace_table_names(target_steps, table_map)
        details = replace_table_names(details, table_map)
        
        def replace_lonely_column_names(string, columns_dict):
        # Function to search and replace lonely column names with "table.column" format
            for table, columns in columns_dict.items():
         
                for column in columns:
                    # Pattern to match column name surrounded by spaces, start or end of string
                    pattern = rf'(?<=\s){column}(?=\s)'
                    replacement = f'"{table}.{column}"'
                    string = re.sub(pattern, replacement, string, flags=re.IGNORECASE)
            return string
        
        #operation = replace_lonely_column_names(operation, columns_type)
       # target_columns = replace_lonely_column_names(target_columns, columns_type)
       # target_steps = replace_lonely_column_names(target_steps, columns_type)
       # details = replace_lonely_column_names(details, columns_type)
        
        # Function to properly quote table.column patterns
        def quote_table_column(string, columns_dict):
            # Function to properly quote table.column patterns considering special cases
           
            for table, columns in columns_dict.items():
                
                for column in sorted(columns, key=len, reverse=True):  # Sort columns by length, longest first
                    if column == 'true_column_names':
                        continue
                    
                    # Patterns to match:
                    # 1. 'table.column' or "table.column" (with quotes)
                    # 2. table.column (without quotes)
                    # considering spaces, '(' or start of the string and ensuring column isn't already quoted
                    # and not part of a longer column name
                    patterns = [
                        rf"'{table}\.{column}'",  # 'table.column'
                        rf'(?<!\w){table}\.\'{column}\'',
                        rf'\'{table}\'\.{column}\b(?!:|\w|\.)',
                        rf'(?<!\w){table}\.\`{column}`',
                        rf'\`{table}\`\.{column}\b(?!:|\w|\.)',
                        rf'(?<!\w){table}\.\"{column}\"',
                    ]

                    for pattern in patterns:
                        replacement = f'"{table}.{column}"'
                        string, _ = re.subn(pattern, replacement, string, flags=re.IGNORECASE)
        
            return string
        

        def map_table_column(input_string, columns_dic):
            for table, columns in columns_dic.items():
                # 检查是否存在true_column_names字典
                if 'true_column_names' in columns:
                    # 按列名长度降序排序
                    sorted_column_names = sorted(columns['true_column_names'].items(), key=lambda item: len(item[0]), reverse=True)
                    for real_name, original_name in sorted_column_names:
                        # 创建正则表达式以匹配双引号包裹的列名或者独立的列名
                        pattern = rf'(?<!\w)"{re.escape(real_name)}"(?!\w)|\b{re.escape(real_name)}\b'
                        input_string = re.sub(pattern, original_name, input_string)
            return input_string
        
        target_columns = map_table_column(target_columns, columns_type)
        details = map_table_column(details, columns_type)

        target_columns = quote_table_column(target_columns, columns_type)
        details = quote_table_column(details, columns_type)


        target_columns = read_logical.correct_table_name(target_columns, columns_type)
        details = read_logical.correct_table_name(details, columns_type)
        
        return operation, target_columns, target_steps, details
    
    @staticmethod
    def parse_steps(text, columns_type):
        results = []
        table_map = {}
        text += "\n"
        step_pattern = re.compile(
            r"Step\s+(\d+):" # 匹配 "Step" 后跟一个数字和冒号
            r"\s*Operator:\s*(.*?)(?:\.|\n|$)" # 捕获 "Operator" 的值，使句号可选
            r"\s*Target\s+columns:\s*(.*?)(?=\s*Target\s+steps:)" # 捕获 "Target columns" 的值，直到遇到 "Target steps:"
            r"\s*Target\s+steps:\s*(.*?)(?=\s*Operation\s+details:)" # 捕获 "Target steps" 的值，直到遇到 "Operation details:"
            r"\s*Operation\s+details:\s*(.*?)(?=\n\s*\n|Step\s+\d+:|$)", # 捕获 "Operation details" 的值
            re.IGNORECASE | re.DOTALL # 忽略大小写并使点号匹配换行符
        )
        
        steps = step_pattern.findall(text)
    
        uid = 0
        for step in steps:
            
            step_number = step[0]  # Step 后的数字
            operation = step[1].strip().strip('.')
            Target_columns = step[2].strip().strip('.')
            Target_steps = step[3].strip().lower().strip('.')
            Details = step[4].strip().strip('.')
            

            if uid != 0 and step_number == "1" and operation == "read":
                break
            
            
            uid += 1
            if operation == "read":
                read_logical.add_table_map(Details, uid, table_map)
            else:
                if operation == "write":
                    continue
                operation, Target_columns, Target_steps, Details = read_logical.process_variables(operation, Target_columns, Target_steps, Details, table_map, columns_type)
            results.append((operation, Target_columns, Target_steps, Details))
           
        return results

    @staticmethod
    def logical_tree(filename, steps, columns_type):

        data_flow = DataOperationTree()

        all_tables = []
        for table, columns in columns_type.items():
            all_tables.append(table)

        for operation, Target_columns, Target_steps, Details, specific_info, uid in data_flow.parse_operations(steps, columns_type):
            data_flow.add_operation(operation, Target_columns, Target_steps, Details, specific_info, uid, all_tables)


        deal_tree(data_flow, all_tables, columns_type)

        save_tree_to_file(data_flow, filename)

        return data_flow
    
    @staticmethod
    def simple_logical_deal(gpt_answer, columns_type):
  
        steps = read_logical.parse_steps(gpt_answer, columns_type)

        return steps

    @staticmethod
    def simple_opt(steps, columns_type):
        #data_flow = DataOperationTree()
        message = ""
        uid = 1
        group_columns = []
        group = False
        select_columns = []
        for operation, Target_columns, Target_steps, Details in steps:
            specific_info = {}
            if operation == "group_by":
                match = re.search(r'Group\s+Step\s+\d+\s+by\s+(.*?)(\n|$)', Details, re.IGNORECASE)
                if match:
                    specific_info['conditions'] = match.group(1).strip().rstrip('.').split(", ")
                    for i in specific_info['conditions']:
                        group_columns.append(i)
                        group = True
            
            if group == True:
                if operation == "select":
                    match = re.search(r'Select\s+(.*?)\s+from', Details, re.IGNORECASE)
                    if match:
                        columns_str = match.group(1)
                        select_columns, _, _ = read_logical.parse_columns(columns_str, columns_type)
                
                if operation == "order_by":
                    Details = Details.strip().rstrip('.')

                    if group_columns:
                        flag = 1
                        for column in group_columns:
                            if column in Details:
                                Details += f", {column} DESC."
                                flag = 2
                                break
                        if flag == 1:
                            for column in group_columns:
                                Details += f", {column} DESC."
                                flag = 2
                                break

                        
            if operation != "read":
                Target_columns = read_logical.del_table_ref(Target_columns, columns_type)
                Details = read_logical.del_table_ref(Details, columns_type)

            message += "Step " + str(uid) + ": "
            message += "Operator: " + operation + ".\n"
            message += "Target_columns: " + Target_columns + ".\n"
            message += "Target_steps: " + Target_steps + ".\n"
            message += "Details: " + Details + ".\n\n"
            uid += 1

        return message

    @staticmethod
    def parse_columns(columns_str, columns_type):
        # Define patterns
        aggregate_pattern = r'\b(max|min|sum|avg|count)\b\s*\((\bdistinct\b\s+)?(.*?)\)'
        as_alias_pattern = r'\bAS\b\s+([\w\.]+|\"[\w\s\.]+\")'
        normal_column_pattern =  r'^(\b[\w\.]+|\"[\w\s\.]+\")$'

        # Lists to store parsed columns, aliases, and distinct flags
        columns = []
        aliases = []
        distincts = []

        # Splitting by comma for individual column elements
        column_elements = columns_str.split(',')

        for element in column_elements:
            element = element.strip()
            
            # Check for 'AS' alias
            as_match = re.search(as_alias_pattern, element, re.IGNORECASE)
            if as_match:
                alias = as_match.group(1)
                element = element[:as_match.start()].strip()  # Get the part before 'AS'
            else:
                alias = "0"
           
            # Check for aggregate functions
            agg_match = re.search(aggregate_pattern, element, re.IGNORECASE)
            if agg_match:
                pass
            else:
                # Process as normal column or expression
                if re.match(normal_column_pattern, element, re.IGNORECASE):
                    column = element

                    columns.append(column)
                    aliases.append(alias)
                    distincts.append(0)
  
        return columns, aliases, distincts
