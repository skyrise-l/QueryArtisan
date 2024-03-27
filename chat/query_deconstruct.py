from ..config.config import USER_CONFIG_DIR, DATA_CONFIG_DIR, DATA_JSON_DIR, DATA_GRAPH_DIR, DATA_TABLE_DIR, GPT_RESULT_DIR
import pandas as pd
import os
from ..utils.process_util import process_util
import ast

class query_deconstruct:
    def __init__(self, file_flag, logger):
        self.file_path = {}
        self.columns_type = {}
        self.table_key = {}
        self.file_flag = file_flag
        self.logger = logger
        self.may_trouble_column = []
        return 

    def get_define(self, data, pre_define):
        prenl = pre_define

        prenl += self.get_columns_des_text(data)

        prenl += self.get_logical_prompt(data)

        return prenl
    
    def get_columns_des_text2(self, data):

        base_path = os.path.join(DATA_CONFIG_DIR, data['db_id'])

        folder_path = os.path.join(base_path, "database_description/")

        all_files = ast.literal_eval(data['table_id'])

        all_columns = ast.literal_eval(data['columns'])

        for file in os.listdir(base_path):
            if file.endswith(".csv"):
                self.columns_type[file[:-4]] = {}

        csv_files = [file + ".csv" for file in all_files]

        message = "## 2. Now for the second step, I will provide you with all the data information that you may need for this data analysis. You should use all mentioned tables whenever possible.\n"
        
        message += "Please note that the column names for the data analysis you are about to perform are highly specialized. You need to parse the query with great caution, as there will definitely be synonymous expressions for column names within the query. You need to identify them and differentiate them from the column values that you intend to use for conditional.\n"

        message += "Please remember this information:\n"
        m1 = "You should use the column name provided in the table information below as the reference for logical planning and code generation. The column_description and column name explanation serves as a description of the column, aiding in your understanding of its meaning and parsing the query.\n"
               
        m1 = ""
        id = 1
        for file_name in csv_files:
            file_path = os.path.join(folder_path, file_name)
            file_path_raw = os.path.join(base_path, file_name)
            self.file_path[file_name[:-4]] = file_path_raw
            true_column_names = {}
            df = pd.read_csv(file_path, encoding='utf-8')


            first_row = df.iloc[0]

            m1 += "Table " + str(id) + ' name is: "' + file_name + '", Its absolute path is:' + '"' + file_path_raw + '"\n'

            id += 1
            
            if file_name[:-4] in all_columns:
                target_columns = all_columns[file_name[:-4]]
                m1 += "Its column infomation are as follows:\n"
                tmp_key = {}
                
                if "ret_pks" in  first_row and first_row["ret_pks"] != "[]":
                        ret_fks = ast.literal_eval(first_row["ret_pks"])
                        tmp_key["ret_pks"] = ret_fks
                        for item in ret_fks:
                            for key, value in item.items():
                                table_name, column_name = value.split('.')
                                if key in all_columns[file_name[:-4]] and table_name in all_columns and column_name in all_columns[table_name]:
                                    m1 += 'FOREIGN KEY: \"' + file_name[:-4] + "." + key + '\"=\"' + value + "\".(join on t1.col1 = t2.col2).\n"
                                
                self.table_key[file_name[:-4]] = tmp_key

                for index, row in df.iterrows(): 

                    if not pd.isna(row['original_column_name']) and row['original_column_name'] in target_columns:
                        m1 += 'The column name is : "' +  str(row['original_column_name']) + '".'
                    else:
                        continue
                    
                    if not pd.isna(row['data_format']):
                        m1 += "The column data_format is '" + str(row['data_format']) + "'."
                        self.columns_type[file_name[:-4]][row['original_column_name']] = str(row['data_format'])

                    if 'column_name' in row and not pd.isna(row['column_name']):
                        m1 += "column name Explanation: " + row['column_name'] + "."

                    if 'column_description' in row and not pd.isna(row['column_description']):
                        m1 += "The column_description is '" + str(row['column_description']) + "'."
                
                    if 'value_description' in row and not pd.isna(row['value_description']):
                        m1 += "Other column information: '" + str(row['value_description']) + "'."    

                    m1 += '\n'
            else:
                m1 += "This table does not provide column information, indicating that the query may involve other aspects such as the number of rows in the table."
            
        
        message += m1
        return message
    
    def get_columns_des_text(self, data):

        base_path = os.path.join(DATA_CONFIG_DIR, data['db_id'])

        folder_path = os.path.join(base_path, "database_description/")

        all_files = ast.literal_eval(data['table_id'])

        all_columns = ast.literal_eval(data['columns'])

        for file in os.listdir(base_path):
            if file.endswith(".csv") and file[:-4] in all_columns:
                self.columns_type[file[:-4]] = {}

        csv_files = [file + ".csv" for file in all_files]

        message = "## 2. Now for the second step, I will provide you with all the data information that you may need for this data analysis. You should use all mentioned tables whenever possible.\n"
        
        message += "Please note that the column names for the data analysis you are about to perform are highly specialized. You need to parse the query with great caution, as there will definitely be synonymous expressions for column names within the query. You need to identify them and differentiate them from the column values that you intend to use for conditional.\n"

        message += "Please remember this information:\n"
        m1 = "You should use the column name provided in the table information below as the reference for logical planning and code generation. The column_description and column name explanation serves as a description of the column, aiding in your understanding of its meaning and parsing the query.\n"
               
        m1 = ""
        id = 1
        for file_name in csv_files:
            file_path = os.path.join(folder_path, file_name)
            table_desc_path = os.path.join(folder_path, "table_description.csv")
            file_path_raw = os.path.join(base_path, file_name)
            self.file_path[file_name[:-4]] = file_path_raw
            true_column_names = {}
            df = pd.read_csv(file_path, encoding='utf-8')

            df2 = pd.read_csv(table_desc_path, encoding='utf-8')

            first_row = df.iloc[0]

            m1 += "Table " + str(id) + ' name is: "' + file_name + '", Its absolute path is:' + '"' + file_path_raw + '"\n'

            m1 += "the table content is about :" + df2[df2['table'] == file_name[:-4]]['table_descrip'].values[0] + ".\n"
            id += 1
            
            if file_name[:-4] in all_columns:
                target_columns = all_columns[file_name[:-4]]
                m1 += "Its column infomation are as follows:\n"
                tmp_key = {}
                
          
                if "ret_pks" in  first_row and first_row["ret_pks"] != "[]":
                        ret_fks = ast.literal_eval(first_row["ret_pks"])
                        tmp_key["ret_pks"] = ret_fks
                        for item in ret_fks:
                            for key, value in item.items():
                                table_name, column_name = value.split('.')
                                if key in all_columns[file_name[:-4]] and table_name in all_columns and column_name in all_columns[table_name]:
                                    m1 += 'FOREIGN KEY: \"' + file_name[:-4] + "." + key + '\"=\"' + value + "\".(join on t1.col1 = t2.col2).\n"
                                
                self.table_key[file_name[:-4]] = tmp_key

                for index, row in df.iterrows(): 

                    if not pd.isna(row['original_column_name']) and row['original_column_name'] in target_columns:
                        m1 += 'The column name is : "' +  str(row['original_column_name']) + '".'
                    else:
                        continue
                    
                    if not pd.isna(row['data_format']):
                        m1 += "The column data_format is '" + str(row['data_format']) + "'."
                        self.columns_type[file_name[:-4]][row['original_column_name']] = str(row['data_format'])

                    if 'column_name' in row and not pd.isna(row['column_name']):
                        m1 += "column name Explanation: " + row['column_name'] + "."

                    if 'column_description' in row and not pd.isna(row['column_description']):
                        m1 += "The column_description is '" + str(row['column_description']) + "'."
                
                    if 'value_description' in row and not pd.isna(row['value_description']):
                        m1 += "Other column information: '" + str(row['value_description']) + "'."    

                    m1 += '\n'
            else:
                m1 += "This table does not provide column information, indicating that the query may involve other aspects such as the number of rows in the table."
            
        
        message += m1
        return message
    
    
    def get_table_message(self, table_list):
        if not table_list:
            m1 = "# The objective of this data analysis does not include relational data tables."
        else :
            m1 = "# The relational data for this data analysis is as follows:\n"
         
            for file_name in table_list:
                file_path = os.path.join(DATA_TABLE_DIR, file_name)
                
                m1 += "Table name is: '" + file_name + "', Its absolute path is:'" + file_path + "', Its column names are as follows:"
                df = pd.read_csv(file_path, encoding='unicode_escape')
                # 获取列名列表
                columns = df.columns.tolist()
                for i, column in enumerate(columns):
                    if i == len(columns) - 1:
                        m1 += column + "."
                    else:
                        m1 += column + ","
                m1 += '\n'
        return m1
    
    def get_json_message(self, json_list):
        if not json_list:
            m1 = "# The objective of this data analysis does not include json data."
        else :
            m1 = "# The json data for this data analysis is as follows:\n"
          
            for file_name in json_list:
                file_path = os.path.join(DATA_JSON_DIR, file_name)
                
                m1 += "Json name is: '" + file_name + "', Its absolute path is:'" + file_path + "', Its metadata are as follows:"
                m1 += str(process_util.JSON_nested_structure(file_name))

                m1 += '\n'
        return m1

    def get_graph_message(self, graph_list):
        if not graph_list:
            m1 = "# The objective of this data analysis does not include graph data."
        else :
            m1 = "# The graph data for this data analysis is as follows:\n"
     
            for graph, graph_files in graph_list.items():
                
                m1 += "The graph name is '" + graph + "'. (1) The nodes data csv files include:"

                for node_file in graph_files['nodes_files']:
                    file_path = os.path.join(DATA_GRAPH_DIR, graph, node_file)

                    m1 += "The one node file is: '" + node_file + "', Its absolute path is:'" + file_path + "', Its column names are as follows:"
                
                    df = pd.read_csv(file_path, encoding='unicode_escape')
                 
                    columns = df.columns.tolist()
                    for i, column in enumerate(columns):
                        if i == len(columns) - 1:
                            m1 += column + "."
                        else:
                            m1 += column + ","
                    m1 += '\n'

                m1 += "(2) The edges data csv files include:"

                for edge_file in graph_files['edges_files']:
                    file_path = os.path.join(DATA_GRAPH_DIR, graph, edge_file)

                    m1 += "The one edge file is: '" + node_file + "', Its absolute path is:'" + file_path + "', Its column names are as follows:"
                
                    df = pd.read_csv(file_path, encoding='unicode_escape')
       
                    columns = df.columns.tolist()
                    for i, column in enumerate(columns):
                        if i == len(columns) - 1:
                            m1 += column + "."
                        else:
                            m1 += column + ","
                    m1 += '\n'

        return m1

    
    def new_get_columns_text(self, data):
        message = "## 2.In the second step, I will provide you with all the data information used in your current data analysis, which may include three types of data: relational data tables, JSON data, and graph data. The specific data will be presented in the following three ways. Please remember this information:\n"

        '''
        2.现在第二步，我将给出你本次数据分析所用到的所有数据信息，将可能包括三种类型的数据:关系型数据表、json数据、graph数据，具体数据将分为下列三种方式给出，请你先记住这些方式：
        （1）关系型数据表：每张表以一个csv文件给出，包括表的绝对路径及其列名。
        （2）json数据：按照json文件格式给出，会给出json文件的绝对路径及其解析后的元数据。
        （3）graph数据：每个图数据集按照若干节点边和若干边表的csv文件形式给出，节点表包括不同节点的基本属性，边表表示不同类型边代表的节点与节点之间的关系。将给出节点表和边表的绝对路径和属性名，其中边表中source属性代表源节点，target属性表示目标节点。
        '''
        message += "(1) Relational Data Tables: Each table will be provided in the form of a CSV file, including the absolute path of the table and its column names.\n"

        message += "(2) JSON Data: Presented in the JSON file format, the absolute path of the JSON file and its parsed metadata will be provided.\n"

        message += '(3) Graph Data: Each graph dataset will be presented in the form of CSV files for multiple node tables and edge tables. The node table includes basic attributes of different nodes, while the edge table represents relationships between nodes of different types. The absolute paths and attribute names of both the node table and edge table will be provided, where the "source" attribute in the edge table represents the source node, and the "target" attribute represents the target node.\n'
            
        message += "Next comes the specific data information, please remember these details:\n"

        
        table_list = process_util.list_files(DATA_TABLE_DIR)
        json_list = process_util.list_files(DATA_JSON_DIR)
        graph_list = process_util.list_graph_files(DATA_GRAPH_DIR)

        message += self.get_table_message(table_list)   
        message += self.get_json_message(json_list)
        message += self.get_graph_message(graph_list)
        
        return message
    

    def get_logical_prompt(self, data):
        
      
        message = "## 3. Now, let's move on to the third step. Below, I will provide natural language query for this data analysis. These statements will inquire within the scope of the tables provided earlier. Please analyze and generate a complete data analysis logical plan that includes reading data files, executing the corresponding queries, and saving the results to a file. The syntax and format of the logical plan should follow the information provided earlier.\n"
        
      
        presql = data['query']# data['sql'].replace('\t', ' ').replace('\n', ' ')

        message += "The specific query statement is: '(" + presql + ")'.\n"
        
        if data['evidence']:
            message += "Additionally, there is a crucial piece of information for you to remember. This pertains to hints related to natural language queries, and it will be very helpful for you in generating this data analysis workflow:'"
            message += data['evidence'] + "'."

        message += "## Besides, here are some important points to keep in mind when generating a logical plan:\n"

      #  message += "(1) Please pay close attention to strings appearing in this format within the query: '\"value\"'. This format, enclosed by both double and single quotation marks, indicates that the double quotation marks are actually part of the value, while the single quotation marks signify a value reference. Therefore, the extracted condition value should be: column = '\"value\"'.\n"

        message += "(2) Also, pay attention to phrases such as How many, How much, total number, average, earliest, latest, sum, and similar terms that appear in the query. This may indicate the need to perform aggregate functions, such as count, sum, avg.\n"

        if self.file_flag % 2 == 1:
            message += "#(3) When counting columns related to IDs in a natural language query, there may be an implicit need for a distinct operation. It requires you to assess based on the actual content of the natural language query. It may be necessary to perform a distinct operation on the column first and then proceed with the count operation.\n"
        else :
            message += "#(3) Exercise caution in using the 'distinct' to a specific column,  Make sure to use it only when the natural language query explicitly requires deduplication. And please do not distinct the query target results, as it can easily lead to incorrect results. \n"
        
       # message += "#(4) When using order_by operator, if the sorted column only contains the result of an aggregate function, you must add the columns used for grouping in the aggregate function into order column to avoid ambiguity. The order for this column should be clearly DESC.\n"
        
     # message += "For example, operation detailsin Step 2: 'group Step 1 by id' and operation details in Step 3: 'order Step 2 by count(*) > 1 DESC' can optimize the Step3 order operation details to 'order Step 2 by count(*) > 1 DESC, id DESC'.\n"

        message += "#(5) When generating the operations detail for generating a logical plan, please pay special attention to the case sensitivity of column names. Columns with the same meaning in different tables might have different cases, such as 'Client_id' versus 'client_id'. Be sure to distinguish between them.\n"
       
        message += "#(6) Do not arbitrarily change the case of the condition values obtained in the query; string comparisons throughout the entire data analysis are case-sensitive.\n"

        message += "#(7) A crucial point! When parsing queries, please make use of your understanding as much as possible rather than relying solely on literal word matching. This is because queries might employ different vocabulary or phrases to express the meaning of column names. It's essential to truly grasp the intent of the query before proceeding with logical plan generation.\n"
        
        message += "#(8) group_bt operations are typically used when there are aggregate functions and aggregate function conditions. If you only need to query the result of a specific aggregate function and other columns simultaneously, grouping is usually not necessary. For example, select MAX(col) and col2 AS result.\n"

        message += "#(9) When generating a logical plan, all the tables I provided should be utilized. If I have specified relevant foreign key relationships, please use joins to connect them via the foreign keys as much as possible; otherwise, there is a high probability of error. Additionally, please note that each join step should only connect two tables.\n"
         
        return message
    
    def get_project_opt_struct(self):
        op_exlain = [
            "read operator format: (Step N: Operator: read.\n Target columns: None.\n Operator: read.\n target steps:None.\n operation details: Use pandas to read table: 'table_1', file_path is '/home/data/table_1.csv'.)\n Step explanation: 'This step reads a data file from the local.'\n",
            "select operator format: (Step N: Operator: select.\n Target columns: \"table1.column1\", \"table2.column2\", max(\"table2.column3\") as highest_column3.\n target steps: Step X.\n operation details: Select \"table1.column1\",\"table2.column2\",max(\"table2.column3\") as highest_column3 from Step X.)\n Step explanation: 'This step selects columns in the temporary view output from the target step. In the operation details, the column name after select uses the format of \"table_name.column_name\" of the original data to avoid confusion. From is followed by the target step.'\n",
            "filter operator format: (Step N: Operator: filter.\n Target columns: \"table1.column1\".\n target steps: Step X.\n operation details: Filter Step X Where \"table1.column1\" > 'x'.)\n Step explanation: 'This step filters the output of Step X based on specified conditions.'\n",
            "order_by operator format: (Step N: Operator: order_by.\n Target columns: \"table1.column1\".\n target steps: Step X.\n operation details: Order Step X by \"table_1.column1\" ASC.)\n Step explanation: 'This step sorts the output of Step X based on the specified column, where the column name is followed by the keywords ASC or DESC to represent ascending or descending order, respectively.'\n",
            "distinct operator format: (Step N: Operator: distinct.\n Target columns: None.\n target steps: Step X.\n operation details: Distinct Step X.)\n Step explanation: 'This step distinct the output of Step X.'\n",
            "join operator format: (Step N: Operator: join.\n Target columns: \"table_1.column1\",\"table_2.column2\" .\n target steps: Step X, Step Y.\n operation details: Step X join Step Y on \"table_1.column1\" = \"table_2.column2\".)\n Step explanation: This step performs a join operation on the temporary views generated by two steps. The table names and column names involved in the join conditions use the original names to avoid confusion.\n"
            "concat operator format: (Step N: Operator: concat.\n Target columns: None.\n target steps: Step X, Step Y.\n operation details: Concat Step X and Step Y.)\n Step explanation: This step concatenates the temporary views generated by two steps, meaning it combines the columns of the two temporary tables.\n"
            "limit operator format: (Step N: Operator: limit.\n Target columns: None.\n target steps: Step X.\n operation details: Limit x, y on Step X.)\n Step explanation: This step retrieves a specified number of rows from the output of the target step. Here, 'x' represents the number of rows to be retrieved, and 'y' represents the offset.\n"
            "group_by operator format: (Step N: Operator: group_by.\n Target columns: \"table_1.column_1\".\n target steps: Step X.\n operation details: Group Step X by \"table_1.column_1\".)\n Step explanation: This step groups the results of Step X according to several columns.\n",
            "write operator format: (Step N: Operator: write.\n Target columns: None.\n target steps: Step X.\n operation details: Write Step X to file_1_path.)\n Step explanation: This step is typically the final one, as it saves the results of the entire data analysis to a specified file.\n"
        ]
        prenl = "Now, I will need you to undergo a data analysis process. I will provide information or requirements one by one. Please remember the relevant information and complete the corresponding tasks according to the given instructions:\n"
   
        prenl += "## 1.First, I have defined a set of logical plan syntax. Please remember them and generate logical plans according to this syntax when I subsequently ask you to do so. The syntax for logical plans is as follows:\n"
      
        prenl +=  "#(1) Syntax 1: Every step in the logical plan should be represented by an operator (indicating the operation to be performed in this step, with only one operation per step), target columns (indicating the columns on which the operation is performed for this step, can be empty), target steps (indicating further operations on the results of which steps for this step, can be empty), operation details (indicating the specific operation for this step, cannot be empty).\n"

        prenl += "#(2) Syntax 2: The operators mentioned in Syntax 2, along with their corresponding step formats and explanations for each step, are provided below:\n"
        
        for ex in op_exlain:
            prenl += ex

        prenl += "\n#(3) Syntax 3: If aggregation functions such as max, sum, and others are needed, they should be used as a column, not as standalone operators. For example, in the syntax of the select operator, 'max(table_1.column_1) as max_column1' should be used as a target column for the select operator, following this rule for all the mentioned operators.\n"
        
        prenl += "#(4) Syntax 4: Column names used in all operators (unless an alias is set using AS) must be in the form of \"table_name.column_name\" to avoid confusion. Additionally, it is advisable to enclose column names in double quotes for clarity, while corresponding column values should be enclosed in single quotes (except for numeric types).\n"
    
        prenl += "#(5) Syntax 5: It is essential to ensure that each step of the logical plan performs only one data operation. Even if the operations are the same, please generate multiple steps.\n"
        return prenl
    

    def get_raw_define(self, data):
        path = GPT_RESULT_DIR + str(data['id']) + ".csv"
        prenl = "Now I need you to help me complete a data analysis task. I will provide a natural language question along with the relevant data table (in CSV format), corresponding column names, and detailed explanations for some of the column names. I need you to generate Python data analysis code for the given question (considering the use of the pandas library)." 
        presql = data['query']
        prenl += "At first, the most important: The specific query statement is: '(" + presql + ")'.\n"
        
        prenl += self.get_columns_des_text(data)

        prenl += "Now, let's proceed with the third step. Please generate the corresponding Python code. And the specific requirements are as follows:\n"
    
        prenl += "(1) Generate corresponding Python execution code for each logical plan step.\n"
 
        prenl+= "(2) Ensure that all code is within the same context.\n"

        path = GPT_RESULT_DIR + str(data['id']) + ".txt"
    
        prenl += "(3) The result of the code need write to the path: '" + path + "'."
        
        prenl += "Please note that the target file for saving is in TXT format. If the saved content is only a single computed value, it should be saved in the following example code format:"

        prenl += "average_score = table_7['rating_score'].mean()\nwith open(result_path, 'w') as f:\nf.write(\"result\\n\")\nf.write(str(average_score))\n"

        prenl += "If the saved content is the DataFrame data format, it should be saved in the following example code format:"
        
        prenl += "df.to_csv('result_1.txt', sep=',', index=False)"

        prenl += "(4) Please generate the Python code using '```python' at the beginning and using '```' at the end for better identification."
       
        prenl += "(5) \nDon't forget to import the pandas library.\n"

        prenl += "## Now you can begin generating the data analysis code for the given question."

        return prenl
    
    def get_columns_row_text(self, data):

        base_path = os.path.join(DATA_CONFIG_DIR, data['db_id'])

        folder_path = os.path.join(base_path, "database_description/")

        csv_files = []
        for file in os.listdir(folder_path):
            if file.endswith(".csv"):
                csv_files.append(file)

        message = "## 2. Now for the second step, I will provide you with all the data information that yo may need for this data analysis.\n"
    
        message += "Please remember this information:\n"
     
        m1 = ""
        id = 1
        for file_name in csv_files:
            file_path = os.path.join(folder_path, file_name)
            file_path_raw = os.path.join(base_path, file_name)
            self.file_path[file_name[:-4]] = file_path_raw
            self.columns_type[file_name[:-4]] = {}
 
            df = pd.read_csv(file_path, encoding='utf-8')

            first_row = df.iloc[0]

            m1 += "Table " + str(id) + ' name is: "' + file_name + '", Its absolute path is:' + '"' + file_path_raw + '"\n'
            id += 1
            
            tmp_key = {}
          
                
            if "ret_pks" in  first_row and not pd.isna(first_row["ret_pks"]):
                tmp_key["ret_pks"] = ast.literal_eval(first_row["ret_pks"])
                for item in tmp_key["ret_pks"]:
                    for key, value in item.items():
                        table_name, column_name = value.split('.')
                        m1 += 'FOREIGN KEY: \"' + key + '\" REFERENCES \"' + value + '\" (table_name.column_name).\n'

            self.table_key[file_name[:-4]] = tmp_key

            m1 += "Its column infomation are as follows:\n"
            for index, row in df.iterrows(): 

                if not pd.isna(row['original_column_name']):
                    m1 += 'The Column is : "' +  str(row['original_column_name']) + '".'
                else:
                    continue
                
                if not pd.isna(row['data_format']):
                    m1 += "The column data_format is '" + str(row['data_format']) + "'."
                    self.columns_type[file_name[:-4]][row['original_column_name']] = str(row['data_format'])

                if 'column_description' in row and not pd.isna(row['column_description']):
                    m1 += "The column_description is '" + str(row['column_description']) + "'."
            
                if 'value_description' in row and not pd.isna(row['value_description']):
                    m1 += "Other column information: '" + str(row['value_description']) + "'."    

                m1 += '\n'
      
           
       
        message += m1
        return message
    
    