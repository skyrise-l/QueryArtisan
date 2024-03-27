
#ifndef CONVERT_H
#define CONVERT_H

#include "postgres.h"

#include <ctype.h>
#include <limits.h>

#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_trigger.h"
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/gramparse.h"
#include "parser/parser.h"
#include "parser/gram.h"
#include "parser/parse_expr.h"
#include "storage/lmgr.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/numeric.h"
#include "utils/xml.h"

extern int globalVariable;

typedef struct SelectLimit
{
	Node *limitOffset;
	Node *limitCount;
	LimitOption limitOption;
} SelectLimit;

typedef struct {
    char columns[10][100]; 
    char alias[10][100];
    int distinct[10];
    int column_count;
} SelectInfo;

typedef struct {
    char columns[10][100];  
    int column_count;
    SortByDir sortByDirArray[10];
} OrderByInfo;

typedef struct {
    char a_expr[1000];
} HavingInfo;

typedef struct {
    char a_exprs[10][1000];
    int expr_count;
} GroupBYInfo;

typedef struct {
    char a_expr[1000];  
} WhereInfo;

typedef struct {
    char table_1[100];
    char table_2[100];
    char column1[100];
    char column2[100];
    char A_expr[1000];  
} JoinInfo;

typedef struct {
    char start[10];
    char offset[10];
} LimitInfo;

typedef struct {
    char columns[10][100]; 
    char alias[10][100];
    int distinct[10];
    int column_count;
} AggInfo;


typedef struct DataOperationNode {
    int id;
    int fatherId;
    char operation[50];
    char Target_columns[300];
    char Target_steps[100];
    char source_table[10][100];
    int table_count;
    union {
        SelectInfo select_info;
        OrderByInfo order_by_info;
        WhereInfo where_info;
        JoinInfo joinInfo;
        LimitInfo limitInfo;
        HavingInfo havingInfo;
        GroupBYInfo groupBYInfo;
        AggInfo aggInfo;
        // 其他操作类型的信息...
    } specific_info;

    int children[10]; // Array of pointers to child nodes
    int childcount;
} DataOperationNode;

typedef struct {
    int NodeNum;
    DataOperationNode NodeTree[100];
} DataOperationTree;

void print_node(DataOperationNode* node);

List* convert_start(const char * query_string, List *raw_parsetree_list);

void log_write(char * string, int flag);

void echo_expr(A_Expr *a_expr);

Node* parseA_exprClause(Node *pre, char *string);

List* my_start(const char * query_string, List *raw_parsetree_list);

#endif