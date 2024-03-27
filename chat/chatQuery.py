
import os
import sqlite3
import threading
import time
import re
import traceback
from ..openai.my_api import MyApi
from ..utils.run_time import run_time
from ..utils.process_util import process_util
from ..utils.log import ThreadLogging
from ..utils.read_logical import read_logical
from ..utils.steps_correction import steps_correction
from ..utils.TokenThreadPool import TokenThreadPool, TokenThreadPool_one
from .preprocessing import preprocessing
from .query_deconstruct import query_deconstruct
from .physical import physical_plan
from .logical import logical_plan
from .execute import execute_plan
from ..config.config import *
import psycopg2
import sys
#warnings.filterwarnings('ignore', category=UserWarning)

query_table = "bench_raw"    # "data_lake_query"
file_flag = 0
db_lock = threading.Lock()
opt_logger = None

class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[80m"
    CYAN = "\033[96m"
    LIGHT_BLUE = "\033[38;5;39m"

class ThreadSafeSQLite:
    def __init__(self, db_path, logger):
        self.db_path = db_path
        self.logger = logger
        self.lock = threading.Lock()

    def thread_safe_write(self, query, params=()):
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(query, params)
                    conn.commit()
                except sqlite3.DatabaseError as e:
                    self.logger.log("thread safe write error")
                    conn.rollback()
                finally:
                    cursor.close()
                
class chatStart:
    def __init__(self, api_keys_list, DEBUG = False, start = 0, opt = False, onekey = False, raw = False):
        self.DEBUG = DEBUG
        self.onekey = onekey
        self.start = start
        self.token_pool = api_keys_list
        self.token_key = api_keys_list[0]
        self.pos = 0
        self.max_retries = len(self.token_pool)
        self.opt = opt
        self.raw = raw
    
    def change_token(self):
        if self.pos == self.max_retries - 1:
            self.pos = 0
            self.token_key = self.token_pool[self.pos]
            return False
        else:
            self.pos += 1
            self.token_key = self.token_pool[self.pos]
            
        return True
    
    def process_row(self, token, row, db, thread_logger, pool, oneKey):
        thread_logger.log(f"开始, id: {row['id']}")
        print(f"Thread for row {row['id']} with token {token} started.")
        try:
            flag = 1
            chatgpt = MyApi(token, row['id'])
            result = chatQuery(chatgpt, pool, row['db_id'], thread_logger, DEBUG=self.DEBUG, opt = self.opt, oneKey = oneKey).gpt_init(row, db, flag)
    
            if result:
                print(f"Thread for row {row['id']} with token {token} success finished.")
                if not oneKey:
                    time.sleep(10)
            else:
                query = f"UPDATE {query_table} SET evaluate = 2  WHERE id = ?"
                params = (row['id'])
                db.thread_safe_write(query, params)
                print(f"Thread for row {row['id']} with token {token} finished, but not get success result.")
        finally:
            thread_logger.close_log()
    
    def process_row_raw(self, token, row, db, thread_logger, pool):
        thread_logger.log(f"开始, id: {row['id']}")
        print(f"Thread for row {row['id']} with token {token} started.")
        try:
            flag = 1
            chatgpt = MyApi(token, row['id'])
            result = chatQuery(chatgpt, pool, row['db_id'], thread_logger, DEBUG=self.DEBUG, opt = self.opt, oneKey = True).gpt_init_raw(row, db, flag)
    
            if result:
                print(f"Thread for row {row['id']} with token {token} success finished.")
            else:
                query = f"UPDATE {query_table} SET evaluate = 2  WHERE id = ?"
                params = (row['id'])
                db.thread_safe_write(query, params)
                print(f"Thread for row {row['id']} with token {token} finished, but not get success result.")
        finally:
            thread_logger.close_log()
    
    def start_gpt(self):
        dbfile = os.path.join(USER_CONFIG_DIR, "gpt_project-bench.db") 
        #self.evalute_all()
        self.run_code()
       # run_time.run(dbfile, "unibench")
        run_time.run_mysql(dbfile, "bench")
        
            
    def run_code(self):
        global query_table
        global opt_logger
        dbfile = os.path.join(USER_CONFIG_DIR, "gpt_project-bench.db") 
        conn = sqlite3.connect(dbfile)
        conn.row_factory = sqlite3.Row   

        cursor = conn.cursor()
        if self.raw:
            query_table = "bench_raw"

        cursor.execute(f"SELECT id, db_id, table_id, columns, query, evaluate, gpt_time, opt_time, evidence FROM {query_table} LIMIT -1 OFFSET {self.start}")
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        t = 0

        important_logfile = os.path.join(LOG_DIR, "important.log") 
        thread_logger = ThreadLogging(important_logfile)
        db = ThreadSafeSQLite(dbfile, thread_logger)

        opt_postgres = os.path.join(LOG_DIR, "opt_postgres.log") 
        opt_logger = ThreadLogging(opt_postgres)

        if not self.raw:
            print(query_table)
            pool = TokenThreadPool_one(10)

            pool.run() 
        
            BATCH_SIZE = 100
            
            for i in range(0, len(rows), BATCH_SIZE):
                batch_rows = rows[i:i + BATCH_SIZE] 
         
                for row in batch_rows:
                    if row['evaluate'] == "1":# or row['evaluate'] == "2":    
                        continue

                    log_filename = LOG_DIR + f"log_{row['id']}.log"
                    thread_logger = ThreadLogging(log_filename)

                    pool.add_task(self.process_row, (self.token_pool[0], row, db, thread_logger, pool, self.onekey))

                pool.tasks.join()
        else:
            print(query_table)
            pool = TokenThreadPool_one(10)

            pool.run() 
        
            BATCH_SIZE = 100
            
            for i in range(0, len(rows), BATCH_SIZE):
                batch_rows = rows[i:i + BATCH_SIZE] 
              
                for row in batch_rows:
                    if row['evaluate'] == "1": 
                        continue

                    log_filename = LOG_DIR + f"log_{row['id']}.log"
                    thread_logger = ThreadLogging(log_filename)


                    pool.add_task(self.process_row_raw, (self.token_pool[0], row, db, thread_logger, pool))

            
                pool.tasks.join() 

    def evalute_all(self):
        dbfile = os.path.join(USER_CONFIG_DIR, "gpt_project-bench.db")
        conn = sqlite3.connect(dbfile)
        conn.row_factory = sqlite3.Row

        cursor = conn.cursor()

        cursor.execute(
            f"SELECT id, query, sql, logical_plan, columns, table_id, evidence, db_id, evaluate FROM bench"
        )

        rows = cursor.fetchall()
        file_flag = 1
        flag = 0
        index = 0
        for row in rows:
            
           # if row['evaluate'] == "1":
            #    continue
     
            print(row['id'])
            '''
                log_filename = LOG_DIR + f"log_{row['id']}.log"
                thread_logger = ThreadLogging(log_filename)
                execute_Instant = execute_plan(1, thread_logger)
                
            #  result = execute_Instant.execute_code(row['id'])
            #  print(result)
                
                evaluate = execute_Instant.new_evaluate(row['id'])
                
                print(evaluate )
                if evaluate == False:
                    if (row['evaluate'] == "1"):
                        print("this is a trouble :"  + row['id'])
                    #query = f"UPDATE {query_table} SET evaluate = 2 WHERE id = {row['id']}" 
                # cursor.execute(query)
                else :
                    query = f"UPDATE {query_table} SET evaluate = 1 WHERE id = {row['id']}" 
                    cursor.execute(query)
                conn.commit()
            '''
            log_filename = LOG_DIR + f"log_{row['id']}.log"
            thread_logger = ThreadLogging(log_filename)
            if 1 or row['logical_plan']: #and row['evaluate'] != 1:
                if row["id"] != "58":
                    continue
                print(row["id"])
                id = row["id"]
                example = process_util.get_example(self.opt, "bench")
                preprocessing_Instant = preprocessing(file_flag, self.opt)

                project_define = preprocessing_Instant.get_define(example)

                query_deconstruct_Instant = query_deconstruct(file_flag, thread_logger)

                query_deconstruct_define = query_deconstruct_Instant.get_define(row, project_define)
               # print(query_deconstruct_define)
                
                filePath = query_deconstruct_Instant.file_path

                columns_type = query_deconstruct_Instant.columns_type

                table_key = query_deconstruct_Instant.table_key
                #print(row['logical_plan']
                logical_Instant = logical_plan(thread_logger)
                
                try :
                    file_path = LOGICAL_DIR + "logical_plan_" + str(row['id']) + ".txt"
                    with open(file_path, 'r') as file:
                        content = file.read()
                              
                    #print(content)
                    read_Instant = read_logical() 
                    steps = read_Instant.simple_logical_deal(content, columns_type)
                    
                    tmp_gpt_plan = process_util.gen_plan(steps, columns_type)
                  
                except Exception as e:
                    
                    exception_info = traceback.format_exception(type(e), e, e.__traceback__)
                    exception_message = ''.join(exception_info)
                    print(Colors.RED + exception_message + Colors.RESET)     
                    print("Logic plan preliminary analysis failed, row :" + str(id))
                    time.sleep(3)
                    #continue
         
                correction_Instant = steps_correction()
            
                try :
                    steps = correction_Instant.simple_logical_correction(steps, columns_type, table_key, row)
                    tmp_gpt_plan2 = read_Instant.simple_opt(steps, columns_type)
                except Exception as e:
                    exception_info = traceback.format_exception(type(e), e, e.__traceback__)
                    exception_message = ''.join(exception_info)
                    print(Colors.RED + exception_message + Colors.RESET)   
                    print("The initial optimization of the logic plan failed, row :" + str(id))
         
                    time.sleep(3) 
                ##print(tmp_gpt_plan2)
     
                try:
                    db_lock.acquire()
                    
                    tree_plan = gpt_plan = read_Instant.logical_tree("logical_plan_" + str(row['id']) + ".txt", steps, columns_type)

                    file_path = os.path.join(LOGICAL_DIR, "log/logical_plan_" + str(id) + "_tree.log")
                    if os.path.exists(file_path):
                        os.remove(file_path)

                    conn_mysql = psycopg2.connect(
                        host = 'localhost',
                        user = POSTGRES_USER,
                        password = 'newpassword',
                        dbname = row['db_id'],
                        port = POSTGRES_PORT
                    )

                    with conn_mysql.cursor() as cursor_mysql:
                        cursor_mysql.execute(f"SELECT {id};")

                    
                    ime_comsum, new_logical_plan = logical_Instant.deal_json_plan(id, tree_plan, filePath, columns_type)
                    db_lock.release()
                    
                except Exception as e:
                    exception_info = traceback.format_exception(type(e), e, e.__traceback__)
                    exception_message = ''.join(exception_info)
                    thread_logger.log(f"exception_message :{exception_message}") 
                    thread_logger.log(f"The database optimization failed, resorting to conventional optimization.") 
        
                    time.sleep(10) 

                    db_lock.release()
              
                    return tmp_gpt_plan2
            
            
class chatQuery:
    def __init__(self, chatgpt, pool, dbname, thread_logger, DEBUG = False, start = 0, opt = False, oneKey = False):
        self.oneKey = oneKey
        self.chatgpt = chatgpt
        self.DEBUG = False
        self.token_limit = False
        self.start = start
        self.pos = 0
        self.__show_debug = DEBUG
        self.pool = pool

        self.startOpt = opt
        self.opt = self.startOpt
        self.file_flag = 0
        self.table_key = {}

        self.thread_logger = thread_logger
        self.example = process_util.get_example(self.opt, dbname)
    
    def gpt_init_raw(self, row, db, code_flag):

        result = False
        while self.file_flag < 5:
            try:
                result = self.gpt_run_raw(row, db, code_flag)
                if result: 
                    break
                self.thread_logger.log("Did not get the correct result, loop again.")
            except BaseException as e:
                exception_info = traceback.format_exception(type(e), e, e.__traceback__)
                exception_message = "".join(exception_info)

                self.thread_logger.log(f"error：{exception_message}")
                self.thread_logger.log(f"error：{self.file_flag}")
                if not self.oneKey:
                    time.sleep(20)
                process_util.separator(self.thread_logger, 2)
            finally:
                if self.token_limit:
                    self.pool.remove_token(self.chatgpt.api_key)
                    return False
            self.file_flag += 1

        return result

    def gpt_run_raw(self, row, db, code_flag):
        self.opt = self.startOpt
      
        self.new_conversation()
        query_deconstruct_Instant = query_deconstruct(self.file_flag, self.thread_logger)

        query_deconstruct_define = query_deconstruct_Instant.get_raw_define(row)
        
        physical_Instant = physical_plan(self.thread_logger, self.opt, {}, file_flag, code_flag)

        if self.__show_debug:
            self.thread_logger.log(f"prompt :{query_deconstruct_define}") 
    
        answner = self.__talk(query_deconstruct_define)

        if self.__show_debug:
            self.thread_logger.log(f"answner :{answner}") 

        physical_Instant.save_code(answner, row['id'])

        process_util.separator(self.thread_logger, 1)

        result, evaluate = self.step_5(row, code_flag)
        err = result.stderr
        returncode = result.returncode

        if returncode != 0:
            self.thread_logger.log(f"execute code error : {err}")

        if evaluate == False:
            self.thread_logger.log(f"execute code result : {result}")
            query = f"UPDATE {query_table} SET is_opt = ?, evaluate = 2  WHERE id = ?"
            params = (str(self.opt), row['id'])
            db.thread_safe_write(query, params)
        else :
            query = f"UPDATE {query_table} SET is_opt = ?, evaluate = 1  WHERE id = ?"
            params = (str(self.opt), row['id'])
            db.thread_safe_write(query, params)

            self.thread_logger.log(f"result right, process success")

        return evaluate

    def gpt_run_raw2(self, row, db, code_flag):
        self.opt = self.startOpt
      
        self.new_conversation()

        pre_define = self.step_1()

        query_deconstruct_Instant = query_deconstruct(self.file_flag, self.thread_logger)

        query_deconstruct_define = query_deconstruct_Instant.get_raw_define(row)

        define = pre_define + query_deconstruct_define
        
        physical_Instant = physical_plan(self.thread_logger, self.opt, {}, file_flag, code_flag)

        if self.__show_debug:
            self.thread_logger.log(f"prompt :{define}") 
    
        answner = self.__talk(define)

        if self.__show_debug:
            self.thread_logger.log(f"answner :{answner}") 

        define = query_deconstruct_Instant.get_raw_define3(row)

        if self.__show_debug:
            self.thread_logger.log(f"prompt :{define}") 
    
        answner = self.__talk(define)

        if self.__show_debug:
            self.thread_logger.log(f"answner :{answner}") 


        physical_Instant.save_code(answner, row['id'])

        process_util.separator(self.thread_logger, 1)

        result, evaluate = self.step_5(row, code_flag)
        err = result.stderr
        returncode = result.returncode

        if returncode != 0:
            self.thread_logger.log(f"execute code error : {err}")

        if evaluate == False:
            self.thread_logger.log(f"execute code result : {result}")
            query = f"UPDATE {query_table} SET is_opt = ?, evaluate = 2  WHERE id = ?"
            params = (str(self.opt), row['id'])
            db.thread_safe_write(query, params)
        else :
            query = f"UPDATE {query_table} SET is_opt = ?, evaluate = 1  WHERE id = ?"
            params = (str(self.opt), row['id'])
            db.thread_safe_write(query, params)

            self.thread_logger.log(f"result right, process success")

        return evaluate
    
    def gpt_init(self, row, db, code_flag):

        result = False
        while self.file_flag < 4:
            try:
                result = self.gpt_run(row, db, code_flag)
                if result:
                    break
                self.thread_logger.log("Did not get the correct result, loop again.")
                if not self.oneKey:
                    time.sleep(20)
            except BaseException as e:
                exception_info = traceback.format_exception(type(e), e, e.__traceback__)
                exception_message = "".join(exception_info)

                self.thread_logger.log(f"error：{exception_message}")
                self.thread_logger.log(f"error：{self.file_flag}")
                if not self.oneKey:
                    time.sleep(20)
                process_util.separator(self.thread_logger, 2)
            finally:
                if self.token_limit:
                    self.pool.remove_token(self.chatgpt.api_key)
                    return False
            self.file_flag += 1

        return result

    def gpt_run(self, row, db, code_flag):

        self.opt = self.startOpt
      
        self.new_conversation()
        # Step1 
        
        pre_define = self.step_1()

        # Step2 
        query_deconstruct_define, file_path, columns_type, may_trouble_column = self.step_2(row, pre_define)

        # Step3 
        new_logical_plan = self.step_3(row, query_deconstruct_define, file_path, columns_type)

        if self.opt == False:
            return False

        query = f"UPDATE {query_table} SET logical_plan = ? WHERE id = ?"
        params = (new_logical_plan, row['id'])
        db.thread_safe_write(query, params)
        
        #Step4 
        self.step_4(row, new_logical_plan, columns_type, code_flag, may_trouble_column)

        # Step5 
        result, evaluate = self.step_5(row, code_flag)

        err = result.stderr
        returncode = result.returncode

        if returncode != 0:
            self.thread_logger.log(f"execute code error : {err}")

        if evaluate == False:
            self.thread_logger.log(f"execute code result : {result}")
            query = f"UPDATE {query_table} SET is_opt = ?, evaluate = 2  WHERE id = ?"
            params = (str(self.opt), row['id'])
            db.thread_safe_write(query, params)
        else :
            query = f"UPDATE {query_table} SET is_opt = ?, evaluate = 1  WHERE id = ?"
            params = (str(self.opt), row['id'])
            db.thread_safe_write(query, params)

            self.thread_logger.log(f"result right, process success")

        return evaluate

    def step_1(self):
        
        self.thread_logger.log("Step One: Execute the pre-training section.")

        preprocessing_Instant = preprocessing(file_flag, self.opt)
        
        project_define = preprocessing_Instant.get_define(self.example)

        process_util.separator(self.thread_logger, 1)

        return project_define

    def step_2(self, data, pre_define):       
        self.thread_logger.log("Step Two: Parsing the structure of the problem.")

        query_deconstruct_Instant = query_deconstruct(self.file_flag, self.thread_logger)

        query_deconstruct_define = query_deconstruct_Instant.get_define(data, pre_define)
        
        self.table_key = query_deconstruct_Instant.table_key

        file_path = query_deconstruct_Instant.file_path

        columns_type = query_deconstruct_Instant.columns_type

        may_trouble_column = query_deconstruct_Instant.may_trouble_column

        self.thread_logger.log("Parsing of the problem structure completed, commence obtaining the logic plan.")

        process_util.separator(self.thread_logger, 1)
        
        return query_deconstruct_define, file_path, columns_type, may_trouble_column


    def step_3(self, data, query_deconstruct_define, filePath, columns_type):      
        self.thread_logger.log("Step Three: Logic plan generation and optimization.") 

        if self.__show_debug:
            self.thread_logger.log(f"prompt :{query_deconstruct_define}") 

        answner = self.__talk(query_deconstruct_define)

        if self.__show_debug:
            self.thread_logger.log(f"answner :{answner}") 

        id = data['id']

        logical_Instant = logical_plan(self.thread_logger)
        read_Instant = read_logical()
    
        try :
            steps = read_Instant.simple_logical_deal(answner, columns_type)
            tmp_gpt_plan = process_util.gen_plan(steps, columns_type)
        except Exception as e:
            exception_info = traceback.format_exception(type(e), e, e.__traceback__)
            exception_message = ''.join(exception_info)
            self.thread_logger.log(f"exception_message :{exception_message}") 
            self.thread_logger.log(f"Logic plan preliminary analysis failed") 
           
            self.opt = False
            return answner
        
        correction_Instant = steps_correction()
        
        try :
            steps = correction_Instant.simple_logical_correction(steps, columns_type, self.table_key, data)
            tmp_gpt_plan2 = read_Instant.simple_opt(steps, columns_type)
        except Exception as e:
            exception_info = traceback.format_exception(type(e), e, e.__traceback__)
            exception_message = ''.join(exception_info)
            self.thread_logger.log(f"exception_message :{exception_message}") 
            self.thread_logger.log(f"The initial optimization of the logic plan failed.") 
     
            self.opt = False
            return tmp_gpt_plan

        new_logical_plan = tmp_gpt_plan2
  
        try:
            db_lock.acquire()
            tree_plan = read_Instant.logical_tree("logical_plan_" + str(id) + ".txt", steps, columns_type)

            file_path = os.path.join(LOGICAL_DIR, "log/logical_plan_" + str(id) + "_tree.log")
            if os.path.exists(file_path):
                os.remove(file_path)

            conn_mysql = psycopg2.connect(
                host='localhost',
                user=POSTGRES_USER,
                password='newpassword',
                dbname=data['db_id'],
                port= POSTGRES_PORT
            )

            with conn_mysql.cursor() as cursor_mysql:
                cursor_mysql.execute(f"SELECT {id};")
   
            ime_comsum, new_logical_plan = logical_Instant.deal_json_plan(id, tree_plan, filePath, columns_type)
            db_lock.release()
            
        except Exception as e:
            exception_info = traceback.format_exception(type(e), e, e.__traceback__)
            exception_message = ''.join(exception_info)
            self.thread_logger.log(f"exception_message :{exception_message}") 
            self.thread_logger.log(f"The database optimization failed, resorting to conventional optimization.") 
        
            self.opt = False
            time.sleep(10) 

            db_lock.release()
        
            return tmp_gpt_plan2
       
        process_util.separator(self.thread_logger, 1)

        return new_logical_plan

    def step_4(self, data, new_logical_plan, columns_type, code_flag, may_trouble_column):     
        self.thread_logger.log("Step Four: Physical plan generation.") 
    
        id = data[0]

        physical_Instant = physical_plan(self.thread_logger, self.opt, columns_type, file_flag, code_flag, may_trouble_column)

        physical_define = physical_Instant.get_define(id, new_logical_plan)

        if self.__show_debug:
            self.thread_logger.log(f"prompt :{physical_define}") 
    
        answner = self.__talk(physical_define)

        if self.__show_debug:
            self.thread_logger.log(f"answner :{answner}") 

        physical_Instant.save_code(answner, id)

        process_util.separator(self.thread_logger, 1)
        

    def step_5(self, data, code_flag):       
        self.thread_logger.log("Step Five: Code optimization and execution.") 

        id = data['id']

        execute_Instant = execute_plan(file_flag, self.thread_logger, code_flag)
        
        result = execute_Instant.execute_code(id)

        evaluate = execute_Instant.new_evaluate(id)

        process_util.separator(self.thread_logger, 1)

        return result, evaluate
        
    def new_conversation(self):
        self.chatgpt.messages = self.chatgpt.init_message()

    def __talk(self, prompt):
        if len(prompt) > 40000:
            print("The prompt is too lengthy. Please contact the administrator.")
            sys.exit(1)

        if self.oneKey:
            target_url = "https://api.pumpkinaigc.online/v1/chat/completions" 
        else:
            target_url ="https://api.openai.com/v1/chat/completions"
        answner = self.chatgpt.talk(prompt,target_url)
        if answner == "rate limit":
            self.token_limit = True
        return answner
    
    
 
    