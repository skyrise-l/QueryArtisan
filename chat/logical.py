

from ..config.config import LOGICAL_DIR, GPT_RESULT_DIR
import os
import json
import re
from ..utils.read_logical import read_logical

class ExecutionStep:

    def __init__(self, node_type, details=None):
        self.node_type = node_type
        self.step_details = details if details else {}
        self.children = []
        
        self.step_number = None
        self.parent_steps = []

    def add_child(self, child_step):
        self.children.append(child_step)

class logical_plan:

    def __init__(self, thread_logger):
        self.step_count = 1
        self.only_one_order = 0
        self.only_one_select = 0
        self.limit = {}
        self.order_by = {'columns':[], 'flags':[]}
        self.read_table = {}
        self.results = ""
        self.logger = thread_logger
        
        return 

    def getOrderKey(self, flag):
        if flag == 2 :
            return "DESC"
        else :
            return "ASC"
        
    def count_nodes(self, node):
        count = 1  
        for child in node.children:
            count += self.count_nodes(child)
        return count
    
    def dfs_assign_step_number(self, node, current_step=[1]):
        
        for child in node.children:
            self.dfs_assign_step_number(child, current_step)
        
        node.step_number = current_step[0]
        current_step[0] += 1
        return current_step[0]
    
    def add_column_type_des(self, string, column_type):
    
        columns_with_types = {}
        for table, columns in column_type.items():
            for column, col_type in columns.items():
                columns_with_types[column] = col_type
   
        found_columns = set(re.findall(r'\b(' + '|'.join(map(re.escape, columns_with_types.keys())) + r')\b', string))

        
        column_types_found = {col: columns_with_types[col] for col in found_columns if col in columns_with_types}

        message = "Please note the types of columns that appear in the filter conditions:"
        
        for column, c_type in column_types_found.items():
            message += "column '" + column + "' type is '"  + c_type + "'.\n"

        return message
    

    def update_parent_steps(self, node):
        for child in node.children:
            node.parent_steps.append(child.step_number)
        
        for child in node.children:
            self.update_parent_steps(child)
    
    def process_gpt_plan(self, gpt_plan):

        for node in gpt_plan.nodes.values():
  
            if node.operation == "limit":
                self.limit = [node.specific_info['limit_num'], node.specific_info['limit_offset']]
            elif node.operation == "order_by":
                self.order_by['columns'].extend(node.specific_info['columns'])
                self.order_by['flags'].extend(node.specific_info['flags'])
    
    def process_flexible_string(self, s):
    
        s = re.sub(r'::text', '', s)
        s = re.sub(r'\(\((.*?)\)\)', r'(\1)', s)

        return s

    def start_deal(self, node):
        step_details = {}
        step_details['select'] = node.get("Output")
        step = ExecutionStep("select", step_details)
        head = step
        if self.order_by['columns']:
            step_details = self.order_by
            step2 = ExecutionStep("Sort", step_details)
            step2.add_child(step)
            head = step2
        
        return head, step

    
    def traverse_execution_plan(self, node, now_count):
        node_type = node.get("Node Type", 'Unknown')

        step_details = {}

        step = ExecutionStep(node_type, step_details)
        ret_step = step

        if node_type == "Limit":
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
                start_step.add_child(step)
            if self.limit != {}:
                limit = self.limit
            else:
                raise Exception("gpt plan limit error")
            step.step_details = limit
        elif node_type == "Unique":
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
                start_step.add_child(step)
            else:
                pass
        elif node_type == "Sort":
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
              
                start_step.add_child(self.traverse_execution_plan(node["Plans"][0], now_count + 1))
                return ret_step
            else:
                return self.traverse_execution_plan(node["Plans"][0], now_count + 1)
        elif node_type == "Aggregate":
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
                step = start_step
                if node.get("Group Key"):
                    step2 = ExecutionStep("group_by", {})
                    step2.step_details['group_by'] = node.get("Group Key")
                    if node.get("Filter"):
                        step3 = ExecutionStep("filter", {})
                        step3.step_details['Filter'] = self.process_flexible_string(node.get("Filter"))
                        step3.add_child(step2)
                        step.add_child(step3)
                    else:
                        step.add_child(step2)
                    step = step2
            else:   
                if node.get("Group Key"):
                    step = ExecutionStep("group_by", {})
                    step.step_details['group_by'] = node.get("Group Key")
                    if node.get("Filter"):
                        step2 = ExecutionStep("filter", {})
                        step2.step_details['Filter'] = self.process_flexible_string(node.get("Filter"))
                        step2.add_child(step)
                        ret_step = step2
                    else:
                        ret_step = step
                else:
                    return self.traverse_execution_plan(node["Plans"][0], now_count + 1)

        elif node_type == "Merge Join":
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
                start_step.add_child(step)
            else:
                pass
            step.node_type = "Nested Loop"
            if node.get("Merge Cond") != None:
                Join_Filter = self.process_flexible_string(node.get("Merge Cond"))
                step.step_details['Join Filter'] = Join_Filter
            else:
                step.step_details['concat'] = 1
        elif node_type == "Nested Loop":
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
                start_step.add_child(step)
            else:
                pass
            if node.get("Join Filter") != None:
                Join_Filter = self.process_flexible_string(node.get("Join Filter"))
                step.step_details['Join Filter'] = Join_Filter
            else:
                step.step_details['concat'] = 1
        elif node_type == "Materialize":
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
                start_step.add_child(self.traverse_execution_plan(node["Plans"][0], now_count + 1))
                return ret_step
            else:
                return self.traverse_execution_plan(node["Plans"][0], now_count + 1)
        elif node_type == "Hash Join":
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
                start_step.add_child(step)
            else:
                pass
            step.step_details["Join Filter"] = self.process_flexible_string(node.get("Hash Cond"))
            step.step_details["join_type"] = node.get("Join Type")
        elif node_type == "Seq Scan":
            step.step_details["read"] = node.get("Relation Name")
            step3 = step
            if "Filter" in node:
                step_details["Filter"] = self.process_flexible_string(node.get("Filter"))
                step_details["Filter"] = step_details["Filter"].replace("<>", "!=")
                step_details["Filter"] = step_details["Filter"].replace("~~", "like")

                step2  = ExecutionStep("filter", step_details)
                step2.add_child(step)
                ret_step = step2
                step3 = step2
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
                start_step.add_child(step3)
            else:
                pass
        elif node_type == "Hash":
            if now_count == 1:
                ret_step, start_step = self.start_deal(node)
                start_step.add_child(self.traverse_execution_plan(node["Plans"][0], now_count + 1))
                return ret_step
            else:
                return self.traverse_execution_plan(node["Plans"][0], now_count + 1)
 
        if "Plans" in node:
            for subplan in node["Plans"]:
                child_step = self.traverse_execution_plan(subplan, now_count + 1)
                if child_step:
                    step.add_child(child_step)

        return ret_step
    
    def dfs_print_execution_plan(self, node):
       
        for child in node.children:
            self.dfs_print_execution_plan(child)

        print("node.node_type:" + node.node_type)
        print("node.details:" + str(node.step_details))
        print("self.parent_steps:" + str(node.parent_steps))
        print("self.step_number:" + str(node.step_number))
        print("\n")

    def generate_prompt(self, node, file_path, column_type):
        for child in node.children:
            self.generate_prompt(child, file_path, column_type)

        node_type = node.node_type

        prompt = ""

        step = ""

        if node_type == "Limit":
            step = "Step " + str(node.step_number) + ": Operator: limit.\n" 
            step += "Target Steps: Step " + str(node.parent_steps[0]) 
            step += "\nSpecific operation: Perform a limit operation on the Step " + str(node.parent_steps[0]) + " Step results, which take rows: " + str(node.step_details[0]) + ", offset is: " + str(node.step_details[1]) + ".\n"
            #print(step) 
        elif node_type == "Sort":
            step = "Step " + str(node.step_number) + ": Operator: sort by.\n" 
            step += "Target Steps: Step " + str(node.parent_steps[0])
            step += "\nSpecific operation: Perform a sort operation on the Step " + str(node.parent_steps[0]) + " results, the sort rule are: "
            for column, flag in zip(node.step_details['columns'], node.step_details['flags']): 
                step += "column:'" + column + "' order is '" + self.getOrderKey(flag) + "', "
            if step.endswith(", "):
                step = step[:-2] + ".\n"
           ## print(step)
        elif node_type == "Aggregate":
            step = "Step " + str(node.step_number) + ": Operator: select.\n"
            step += "Target Steps: Step " + str(node.parent_steps[0])
            step += "\nSpecific operation: Perform a select operation on the Step " + str(node.parent_steps[0]) + " results, select columns: "
            for i in node.step_details["Aggregate"]:
                step += self.process_flexible_string(i) + ", "
                
            if step.endswith(", "):
                step = step[:-2] + ".\n"
           # print(step) 
        elif node_type == "select":
            step = "Step " + str(node.step_number) + ": Operator: select.\n" 
            step += "Target Steps: Step " + str(node.parent_steps[0])
            step += "\nSpecific operation: Perform a select operation on the Step " + str(node.parent_steps[0]) + " results, select columns: "
            for i in node.step_details["select"]:
                step += self.process_flexible_string(i) + ", "
                
            if step.endswith(", "):
                step = step[:-2] + ".\n"
           # print(step)        
        elif node_type == "Nested Loop":
            if 'concat' in node.step_details:
                step = "Step " + str(node.step_number) + ": Operator: concat.\n"
                step += "Target Steps: Step " + str(node.parent_steps[0]) + ", Step " + str(node.parent_steps[1])
                step += "\nSpecific operation: Perform a concat operation on the Step " + str(node.parent_steps[0]) + " and Step " + str(node.parent_steps[1]) + " results.\n"          
            else:
                step = "Step " + str(node.step_number) + ": Operator: join.\n" 
                step += "Target Steps: Step " + str(node.parent_steps[0]) + ", Step " + str(node.parent_steps[1])
                step += "\nSpecific operation: Perform a join operation on the Step " + str(node.parent_steps[0]) + " and Step " + str(node.parent_steps[1]) + " results, "
                step += "join condition is:" + node.step_details["Join Filter"] + ".\n"
           # print(step) 
        elif node_type == "Hash Join":
            step = "Step " + str(node.step_number) + ": Operator: join.\n" 
            step += "Target Steps: Step " + str(node.parent_steps[0]) + ", Step " + str(node.parent_steps[1])
            step += "\nSpecific operation: Perform a join operation on the Step " + str(node.parent_steps[0]) + " and Step " + str(node.parent_steps[1]) + " results, "
            step += "join condition is:" + node.step_details["Join Filter"] + ".\n"
           # print(step) 
        elif node_type == "Seq Scan":
            step = "Step " + str(node.step_number) + ": Operator: read.\n" 
            step += "Target Steps: None."
            step += "\nSpecific operation: (1) use pandas to read " + node.step_details["read"] + ".csv As " + node.step_details["read"] + ", file_path is : '"  
            step += file_path[node.step_details["read"]] + "'. "
            step += "(2) Perform data preprocessing on " + node.step_details["read"] + "\n"
          #  print(step)
        elif node_type == "filter":
            step = "Step " + str(node.step_number) + ": Operator: filter.\n" 
            step += "Target Steps: Step " + str(node.parent_steps[0])
            step += "\nSpecific operation: Perform a filter operation on the Step " + str(node.parent_steps[0]) + " results, the conditions are: "  
            step += node.step_details["Filter"] + ".\n"
            step += self.add_column_type_des(node.step_details["Filter"], column_type)
         #   print(step) 
        elif node_type == "group_by":
            step = "Step " + str(node.step_number) + ": Operator: group_by.\n"
            step += "Target Steps: Step " + str(node.parent_steps[0])
            step += "\nSpecific operation: Perform a group_by operation on the Step " + str(node.parent_steps[0]) + " results, group columns: "
            for i in node.step_details["group_by"]:
                step += self.process_flexible_string(i) + ", "
                
            if step.endswith(", "):
                step = step[:-2] + ".\n"  
        elif node_type == "Unique":
            step = "Step " + str(node.step_number) + ": Operator: distinct.\n" 
            step += "Target Steps: Step " + str(node.parent_steps[0])
            step += "\nSpecific operation: Perform a distinct operation on the Step " + str(node.parent_steps[0]) + " results.\n"  
       
        step += "\n"
        self.results += step
    

    def deal_json_plan(self, index, gpt_plan, all_file_path, column_type):
        
        file_path = os.path.join(LOGICAL_DIR, "log/logical_plan_" + str(index) + "_tree.log")

        self.process_gpt_plan(gpt_plan)

        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        combined_lines = ''.join(lines)

        if combined_lines.endswith(']'):
            json_str = combined_lines
            time_comsum = None
        else:
            json_str, time_comsum = combined_lines.rsplit(']', 1)
            json_str += ']'
            time_comsum = float(time_comsum) / 1e9

        execution_plan = json.loads(json_str)
        
        tree_plan = self.traverse_execution_plan(execution_plan[0]["Plan"], 1)

        self.step_count = self.dfs_assign_step_number(tree_plan, current_step=[1])

        self.update_parent_steps(tree_plan)

        self.generate_prompt(tree_plan, all_file_path, column_type)

        path = GPT_RESULT_DIR + str(index) + ".txt"
        self.results += "Step " + str(self.step_count) + ": Operator: write.\n"
        self.results += "Target Steps: Step " + str(self.step_count - 1)
        self.results += "\nSpecific operation: Write the result of the Step " + str(self.step_count - 1) + " to the file:'" + path + "'.\n"

        return time_comsum, self.results
    

    def gen_check_message():
        messages = "The logical plan you generated seems to contain errors. Please review the information I provided, and attempt to regenerate the logical plan. Make sure to check the syntax, column names, and logical relationships used."

        return messages