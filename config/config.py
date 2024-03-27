# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from os import getenv
from os.path import join


USER_CONFIG_DIR = "/mnt/d/database/GPT3_project/src/GPT3/config/"

DATA_CONFIG_DIR = "/mnt/d/database/GPT3_project/src/GPT3/data/"      

DATA_JSON_DIR = "/mnt/d/database/GPT3_project/src/GPT3/data/dlbench/json/"

DATA_GRAPH_DIR = "/mnt/d/database/GPT3_project/src/GPT3/data/dlbench/graph/"

DATA_TABLE_DIR = "/mnt/d/database/GPT3_project/src/GPT3/data/dlbench/table/"

CODE_DIR = "/mnt/d/database/GPT3_project/src/GPT3/code/dlbench_raw/"

GPT_RESULT_DIR = "/mnt/d/database/GPT3_project/src/GPT3/result/unibench_gpt/"

SQL_RESULT_DIR = "/mnt/d/database/GPT3_project/src/GPT3/result/unibench_sql/"

LOGICAL_DIR = "/mnt/d/database/GPT3_project/src/GPT3/data/logical_plan/"

LOG_DIR = "/mnt/d/database/GPT3_project/src/GPT3/log/"

POSTGRES_PORT = "5432"

POSTGRES_USER = "postgres"