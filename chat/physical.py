

from ..config.config import CODE_DIR, GPT_RESULT_DIR
from ..utils.process_util import process_util


class physical_plan:

    def __init__(self, thread_logger, opt = False, columns_type = {}, file_flag = 0, code_flag = 0, may_trouble_column = {}):
        self.opt = opt
        self.columns_type = columns_type
        self.file_flag = file_flag
        self.thread_logger = thread_logger
        self.code_flag = code_flag
        self.may_trouble_column = may_trouble_column
        return 
    

    def get_define(self, id, new_logical_plan):

        if self.opt:
            prenl = self.get_physical_opt_prompt(id, new_logical_plan)
        else :
            prenl = self.get_physical_prompt(id, new_logical_plan)

        return prenl

    def get_physical_prompt(self, id, new_logical_plan):
        message = ""

        columns = self.extract_target_columns(new_logical_plan)

    
        message += "Now, let's proceed with the fourth step. Based on the logical plan your generate before, please generate the corresponding Python code. The specific requirements are as follows:\n"
     
        message += "(1) Generate corresponding Python execution code for each logical plan step.\n"
    
        message += "(2) Ensure that all code is within the same context.\n"

        message += "(3) All the read operators in the logical plans I provided contain 'Perform data preprocessing'. This means that data processing is required after reading the data. Currently, what you need to do includes converting the values of boolean type columns or object type columns into string type, as shown in the example code below:\n"

        message +=  "(for col in data.columns: if twitter[col].dtype == 'bool' or twitter[col].dtype == 'object':data[col] = data[col].astype(str))\n"

        message += "(4)When comparing column values, it is crucial to pay attention to the column's type and handle the values to be compared differently based on their types. Otherwise, even if the values are equal, incorrect referencing may lead to inequality. For instance, comparing the string value 'true' using a boolean type comparison would result in an incorrect judgment.\n"
        message += "Now, I remind you of the format of the columns used in the logical plan. When generating code, especially when comparing column values, you must use different methods of column value comparison based on the format of the column:\n"
        
        for column in columns:
            try:
                if '.' in column:
                    table, column = column.split('.')
                    message += "column " + "'" + column + "' type is '" + self.columns_type[table][column] + "'.\n"
            except Exception as e:
                import traceback
                exception_info = traceback.format_exception(type(e), e, e.__traceback__)
                exception_message = ''.join(exception_info)
                self.thread_logger.log(exception_message)


        message += "#If the type of the prompted column is text or another string type, consider it as a string comparison. For example, if the logical plan has a condition like 'Filter status = 1', and the column 'status' is of text type, then 1 should also be treated as a string, enclosed in quotes. The generated code should look like the following:\n"

        message += "(table2_filtered = table2[table2['status'] == '1'])\n"

        message += "#At the same time, if the type of the prompted column is integer or double type, then direct numerical comparison should be performed. Assuming the logical plan has a condition: 'Filter status = 1', and since the column 'status' is of integer type, 1 should be treated as a number and should not be enclosed in quotes. The generated code should look like the following:\n"

        message += "(table2_filtered = table2[table2['status'] == 1]\n"

        message += "#Meanwhile, when the type of the prompted column is date, the column values should be processed using pandas' to_datetime function before comparison. The generated code should look like the following:\n"

        message += "#table_6 = table_5[(pd.to_datetime(table_5['rating_timestamp_utc']) <= pd.to_datetime('12/31/2013'))]\n"


        message += "(5) When generating code for logical plan operators, please try to adhere to the following operator-code usage in the code examples:"

        message += "#count operator code example: result = table2['CustomerID'].count() #Note: Please use the aggregation functions from the Pandas library as much as possible for this type of aggregation function operator, including other aggregation functions such as sum, min, etc.\n"

        message += "(6) When the logical plan involves a 'LIKE' operation in the filter conditions, please be aware to use fillna('') for filling in the blanks when performing the condition comparison, as shown in the example code below:\n"
        message += "df_filtered = df[df['Title'].fillna('').str.startswith('test')]\n df_filtered2 = df[df['column_name'].fillna('').str.contains('^test')]\n"

        path = GPT_RESULT_DIR + str(id) + ".txt"
      
        message += "(7) The logical plan's write Operator 'result_path' is: '" + path + "'."
        
        message += "Please note that the target file for saving is in TXT format. If the saved content is only a single computed value, it should be saved in the following example code format:"

        message += "average_score = table_7['rating_score'].mean()\nwith open(result_path, 'w') as f:\nf.write(str(average_score))\n"

        message += "If the saved content is the DataFrame data format, it should be saved in the following example code format:"
        
        message += "df.to_csv('result_1.txt', sep=',', index=False)"
     
        return message
    
    def get_physical_opt_prompt(self, id, new_logical_plan):
        message = ""

      
        message += "I have optimized the logical plan you provided. Now, please completely disregard the logic plan you've generated before. Remember the optimized logic plan I provided."
        
        message += "Now, please generate corresponding Python code based on the optimized logical plan I provided, along with specific requirements. The optimized logical plan is:\n"

        message += new_logical_plan
   
        message += "\nThe specific requirements are as follows:"

        message += "(1) Ensure that all code is within the same context.\n"

        message += "(2) When generating code, please consider that some steps may can be combined. For example, the 'group' operation is often combined with the 'having' operation for conditional filtering on aggregate functions. An example code is as follows:\n"

        message += "Step2 = Step1.groupby('group_column').filter(lambda x: x['value_column'].mean() > value) \n"
        
        message += "(3) Additionally, I have identified some columns that may cause confusion in Pandas processing. When reading the corresponding table, please make sure to use the following format to read the CSV, replacing file_path with the path to the corresponding table: \n"

        for table in self.may_trouble_column:
            message += f"{table} = pd.read_csv(file_path, dtype={{column: str for column in self.may_trouble_column[table]}})"

        message += "(4) When performing column value comparisons, please pay attention to the types of the relevant columns as indicated in the logical plan. Different comparison code should be employed based on the varying types of columns.\n"
      
        message += "#If the type of the prompted column is text type, consider it as a string comparison. For example, if the logical plan has a condition like 'Filter status = 1', and the column 'status' is of text type, then 1 should also be treated as a string, enclosed in quotes. The generated code should look like the following:\n"

        message += "(table2_filtered = table2[table2['status'] == '1'])\n"

        message += "#At the same time, if the type of the prompted column is integer or double type, then direct numerical comparison should be performed. Assuming the logical plan has a condition: 'Filter status = 1', and since the column 'status' is of integer type, 1 should be treated as a number and should not be enclosed in quotes. The generated code should look like the following:\n"

        message += "(table2_filtered = table2[table2['status'] == 1]\n"

        message += "#Meanwhile, when the type of the prompted column is date, the column values should be processed using pandas' to_datetime function before comparison. The generated code should look like the following:\n"

        message += "#table_6 = table_5[(pd.to_datetime(table_5['rating_timestamp_utc']) <= pd.to_datetime('12/31/2013'))]\n"

        message += "(5) When the logical plan involves a 'LIKE' operation in the filter conditions, please be aware to use fillna('') for filling in the blanks when performing the condition comparison, as shown in the example code below:\n"
        message += "df_filtered = df[df['Title'].fillna('').str.startswith('test')]\n df_filtered2 = df[df['column_name'].fillna('').str.contains('^test')]\n"

        path = GPT_RESULT_DIR + str(id) + ".txt"
      
        message += "(6) The logical plan's write Operator result path is: '" + path + "'."

        message += "If the saved content is only a single computed value, you should ensure to write the column name 'result\\n' first, and then write the results, it should be saved in the following example code format:"

        message += "average_score = table_7['rating_score'].mean()\nwith open(result_path, 'w') as f:\nf.write(\"result\\n\")\nf.write(str(average_score))\n"

        message += "If the saved content is the DataFrame data format, it should be saved in the following example code format:"
        
        message += "df.to_csv('result_1.txt', sep=',', index=False)"

        message += "(7) Please generate the Python code using '```python' at the beginning and using '```' at the end for better identification."
      
        message += "(8) \nDon't forget to import the pandas library.\n"

        message += "(9) Note that in the select operator, distinct(column) typically corresponding code implementation should be df['column'].drop_duplicates() to get a new DataFrame.\n"
        
        message += "However, if it is count(distinct column), it means need to obtain a specific function value. In this case, you may consider using df['column'].unique()."

        message += "(10) If necessary, you can adjust the order of the logical plan to ensure that the code can be executed correctly."
        
        message += "(11) Regarding the recommended coding style for group_by and aggregation, the code is written as follows:"

        message += "grouped_data = df.groupby('id')['amount'].sum().reset_index()] (Group 'id' and calculate the sum of the 'amount' column.)\n"
    
        message += "order_by operations recommended coding style as follows: sorted_agg_df = grouped_data.sort_values(by=['amount', 'id'], ascending=[True, False]) (This is sorting the sum aggregation results of the 'amount' column and the grouped column 'id'.)\n"

        return message
    

    def save_code(self, answner, id):
        
        code = process_util.exact_code(answner)

        with open(CODE_DIR + str(id) + "_" + str(self.code_flag) + '.py', 'w', encoding='utf-8') as file:
            file.write(code)
            self.thread_logger.log(f"代码已成功保存到{id}.py文件中。")


    def extract_target_columns(self, plan_text):
        steps = plan_text.strip().split("Step ")
        target_columns = set()

        for step in steps[1:]:
            lines = step.split("\n")
            for line in lines:
                if "Target columns:" in line:
                    columns = line.split("Target columns: ")[1].strip()
                    if columns != "None":

                        for column in columns.split(","):
                      
                            target_columns.add(column.strip())

        return list(target_columns) 


