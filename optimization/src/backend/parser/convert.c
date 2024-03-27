#include "parser/convert.h"
#include <stdio.h>
#include <string.h>

#define OrderBY_op 2
#define Select_op 1
#define DEBUG_TARGET
#define DEBUG_FROM
#define DEBUG_ORDER
#define EXPLAIN_STATUS
#define DEBUG_WHERE

static char final_source_table[10][50];
static int visit_table[10] = {0};
static Node* join_nodes[10] = {(Node *)NIL};;
static int final_source_table_count;
int globalVariable = 0;

static void free_pointer(){
    int i;
    final_source_table_count = 0;
    memset(visit_table, 0, sizeof(visit_table));

    for (i = 0; i < 10; i ++){
        join_nodes[i] = (Node *)NIL;
        memset(final_source_table[i], '\0', sizeof(final_source_table[i]));
    }
}

static RawStmt *
makeRawStmt(Node *stmt, int stmt_location)
{
	RawStmt    *rs = makeNode(RawStmt);

	rs->stmt = stmt;
	rs->stmt_location = stmt_location;
	rs->stmt_len = 0;			/* might get changed later */
	return rs;
}

void log_write(char * string, int flag){
    const char *base_filename = "/mnt/d/数据库/GPT3_project/src/GPT3/data/logical_plan/log/logical_plan_";
    const char *base_filename2 = "/mnt/d/数据库/GPT3_project/src/GPT3/data/logical_plan/log/important";
    char filename[150];
    FILE *file_log;

    if (flag != 1){
        sprintf(filename, "%s%d_tree.log", base_filename, globalVariable);
    } else {
        sprintf(filename, "%s.log", base_filename2);
    }
    
    file_log = fopen(filename, "a");

    fprintf(file_log, "%s", string);
    fclose(file_log);
}

static void get_final_table_list(DataOperationNode* node){
    int i;
    final_source_table_count = node->table_count;
    for(i = 0; i < node->table_count; i ++) {
        strncpy(final_source_table[i], node->source_table[i], 100);
    }
}

static void print_columnRef(List *column_field){
    Value *v;
    ListCell *filed_tmp;

    foreach(filed_tmp, column_field){
        v = lfirst(filed_tmp);
        log_write(v->val.str, 0);
        log_write("\n", 0);
        //sprintf(print_tmp, "%d", v->type);
        //log_write(print_tmp, 0);
        //log_write("\n", 0);
    }
}

void echo_expr(A_Expr *a_expr){
    Node *n1 = a_expr->lexpr;
    Node *n2 = a_expr->rexpr;
    A_Expr *e;
    A_Const* con;
    Value * v;
    ColumnRef * c;
    char print_tmp[100];

    if (a_expr->kind == AEXPR_BETWEEN){
        return;
    }

    v = lfirst(list_head(a_expr->name));

    if (v->type == T_String){
        log_write(v->val.str, 0);
    }

    log_write("\nleft:\n", 0);

    if (n1->type == T_A_Expr){
        e = (A_Expr*)n1;
        echo_expr((A_Expr*)e->lexpr);
    } else if (n1->type == T_A_Const){
        con = (A_Const*)n1;
        if (con->val.type == T_Integer){
            sprintf(print_tmp, "%d", con->val.val.ival);
            log_write(print_tmp, 0);
        } else {
            log_write(con->val.val.str, 0);
        }
    } else if (n1->type == T_ColumnRef){
        c = (ColumnRef *) n1;
        print_columnRef(c->fields);
    }

    log_write("\nright:\n", 0);

    if (n2->type == T_A_Expr){
        e = (A_Expr*)n2;
        echo_expr((A_Expr*)e->lexpr);
    } else if (n2->type == T_A_Const){
        con = (A_Const*)n2;
        if (con->val.type == T_Integer){
            sprintf(print_tmp, "%d", con->val.val.ival);
            log_write(print_tmp, 0);
        } else {
            log_write(con->val.val.str, 0);
        }
    } else if (n2->type == T_ColumnRef){
        c = (ColumnRef *) n2;
        print_columnRef(c->fields);
    }
}

static void echo_join_expr(JoinExpr * join_expr) {
    JoinExpr * join_sub_expr;
    RangeVar * join_table_ref;
    log_write("join expr:\n", 0);
    log_write("join left:\n", 0);
    if (join_expr->larg->type == T_RangeVar){
        join_table_ref = (RangeVar *)join_expr->larg;
        log_write("table->relname:\n", 0);
        log_write(join_table_ref->relname, 0);
        log_write("\n", 0);
    } else {
        join_sub_expr = (JoinExpr *)join_expr->larg;
        echo_join_expr(join_sub_expr);
    }
    log_write("join right:\n", 0);
    if (join_expr->rarg->type == T_RangeVar){
        join_table_ref = (RangeVar *)join_expr->rarg;
        log_write("table->relname:\n", 0);
        log_write(join_table_ref->relname, 0);
        log_write("\n", 0);
    } else {
        join_sub_expr = (JoinExpr *)join_expr->rarg;
        echo_join_expr(join_sub_expr);
    }
    log_write("quals expr:\n", 0);
    echo_expr(join_expr->quals);
    log_write("\n", 0);
}

static void try_echo(List* parsetree_list){

    ListCell *parsetree_item;
    RawStmt *rawfirst;
    SelectStmt *parsetree;

    #ifdef DEBUG_TARGET
    List *target_clause;
    ResTarget* target_res;
    ListCell *target_item;
    ColumnRef *column_ref;
    FuncCall *funcCall;
    List* funcArgs;
    ListCell* funcArg;

    char print_tmp[100];
    #endif

    #ifdef DEBUG_FROM
    ListCell *from_item;
    RangeVar *from_res;
    List *from_clause;
    JoinExpr* join_expr;
    #endif

    #ifdef DEBUG_WHERE
    A_Expr *a_expr;
    List * expr_list;
    ListCell *expr_item;
    A_Expr *expr;
    BoolExpr *bool_expr;
    #endif

    #ifdef DEBUG_ORDER
    SortBy * sortItem;
    ColumnRef* column_ref2;
    ListCell * sortCell;
    #endif
    
    foreach(parsetree_item, parsetree_list)
	{
        rawfirst = lfirst_node(RawStmt, parsetree_item);
        parsetree = (SelectStmt *)rawfirst->stmt;

        #ifdef DEBUG_TARGET
        target_clause = parsetree->targetList;
        log_write("the now is target_clause:\n", 0);

        foreach(target_item, target_clause)
	    {
            target_res = lfirst_node(ResTarget, target_item);

            log_write("this is alias:\n", 0);
            log_write(target_res->name, 0);

            if (target_res->val->type == T_FuncCall){

                funcCall = (FuncCall *)target_res->val;
                print_columnRef(funcCall->funcname);

                funcArgs = funcCall->args;

                foreach (funcArg, funcArgs){
     
                    column_ref = (ColumnRef *)lfirst(funcArg);
                    print_columnRef(column_ref->fields);
                }
            } else {
                column_ref = (ColumnRef *)(target_res->val);
                print_columnRef(column_ref->fields);
            }
            log_write("\n", 0);
        }
        #endif
        
        #ifdef DEBUG_FROM
        from_clause = parsetree->fromClause;
        log_write("the now is from_clause:\n", 0);

        foreach(from_item, from_clause)
	    {
            from_res = lfirst_node(RangeVar, from_item);
            if (from_res->type == T_RangeVar){
                log_write("from_res->catalogname:\n", 0);
                log_write(from_res->catalogname, 0);
                log_write("\n", 0);
                log_write("from_res->schemaname:\n", 0);
                log_write(from_res->schemaname, 0);
                log_write("\n", 0);
                log_write("from_res->relname:\n", 0);
                log_write(from_res->relname, 0);
                log_write("\n", 0);
            } else if (from_res->type == T_JoinExpr){
                join_expr = (JoinExpr *)from_res;
                echo_join_expr(join_expr);
            }
            
        
        }
        #endif

        #ifdef DEBUG_WHERE
        {
            if (parsetree->whereClause != (Node *)NIL){
                log_write("the now is where_clause:", 0);
                
                //sprintf(print_tmp, "%d", parsetree->whereClause->type);
                //log_write(print_tmp, 0);
                
                if (parsetree->whereClause->type == T_BoolExpr){
                    bool_expr = (BoolExpr *)(parsetree->whereClause);;
                    expr_list = bool_expr->args; 
                    foreach(expr_item, expr_list)
	                {
                        log_write("expr_item:\n", 0);
                        expr = lfirst_node(A_Expr, expr_item);
                        echo_expr(expr);
                    }
                } else {
                    a_expr = (A_Expr *)(parsetree->whereClause);
                    echo_expr(a_expr);
                }
                
            }
        }
        #endif

        #ifdef DEBUG_ORDER
        {
            log_write("the now is sort_clause:\n", 0);
            foreach(sortCell, parsetree->sortClause)
	        {
                sortItem = lfirst_node(SortBy, sortCell);
                column_ref2 = (ColumnRef *)(sortItem->node);
                print_columnRef(column_ref2->fields);
                log_write("\n", 0);
            }
        }
        #endif
    }
}

static void parse_select_info(const char *info_str, SelectInfo *select_info) {
    const char *current, *end;
    const char *quoteEnd;
    int i, len;

    // 初始化 distinct 数组为 0
    memset(select_info->distinct, 0, sizeof(select_info->distinct));

    // 解析 columns
    current = strstr(info_str, "'columns': [");
    if (current) {
        current += 12; // 跳过 "'columns': ["
        end = strchr(current, ']');
        for (i = 0; i < 10 && current < end; i++) {
            current = strchr(current, '\'');
            if (!current || current >= end) break;
            current++; // 跳过单引号

            quoteEnd = strchr(current, '\'');
            if (!quoteEnd || quoteEnd >= end) break;

            len = quoteEnd - current;
            if (len > 99) len = 99;
            strncpy(select_info->columns[i], current, len);
            select_info->columns[i][len] = '\0';

            current = quoteEnd + 1;
        }
        select_info->column_count = i;
    }
  
    current = strstr(info_str, "'aliases': [");
    if (current) {
        current += 12;
        end = strchr(current, ']');
        for (i = 0; i < select_info->column_count && current < end; i++) {
            current = strchr(current, '\'');
            if (!current || current >= end) break;
            current++;

            quoteEnd = strchr(current, '\'');
            if (!quoteEnd || quoteEnd >= end) break;

            len = quoteEnd - current;
            if (len > 99) len = 99;
            strncpy(select_info->alias[i], current, len);
            select_info->alias[i][len] = '\0';

            current = quoteEnd + 1;
        }
    }
  
    // 解析 distincts
    current = strstr(info_str, "'distincts': [");
    if (current) {
        current += 14;
        end = strchr(current, ']');
        for (i = 0; i < select_info->column_count && current < end; i++) {
            if (*current == '0' || *current == '1') {
                select_info->distinct[i] = *current - '0';
            }
            current = strchr(current, ',');
            if (!current || current >= end) break;
            current++;
        }
    }
    
}


static void parse_OrderBy_info(const char *info_str, OrderByInfo *orderBy_info) {
    const char *start, *end;
    int i = 0, len, op;

    // 解析 columns
    start = strstr(info_str, "'columns': [");
    if (start) {
        start += 12; // 跳过 "'columns': ["

        // 解析 columns
        while (start != NULL && i < 10) {
            start = strchr(start, '\'');
            if (start == NULL) break;
            start++; // 跳过 '\''

            // 查找字符串结束的单引号
            end = strchr(start, '\'');
            if (end == NULL) break;

            len = end - start;
            if (len > 49) len = 49; // 确保不会溢出列名数组

            strncpy(orderBy_info->columns[i], start, len);
            orderBy_info->columns[i][len] = '\0'; // 确保字符串以null结束

            i++;

            if (*(end + 1) == ']') {
                break; 
            }
            start = strchr(end, ',');
        }
    }
    orderBy_info->column_count = i;

    // 解析 flags
    start = strstr(info_str, "'flags': [");
    if (start) {
        start += 10; // 跳过 "'flags': ["
        for (int j = 0; j < orderBy_info->column_count; j++) {
            if (start == NULL) break;

            op = *start - '0';

            switch (op)
            {
            case 1:
                orderBy_info->sortByDirArray[j] = SORTBY_ASC;
                break;
            case 2:
                orderBy_info->sortByDirArray[j] = SORTBY_DESC;
                break;
            default:
                orderBy_info->sortByDirArray[j] = SORTBY_DEFAULT;
                break;
            }

            start += 3;
        }
    }
}

static void parse_where_info(const char *info_str, WhereInfo *where_info) {
    const char *conditionsPtr = strstr(info_str, "'conditions':");
    const char *startQuote;
    const char *endQuote;
    size_t length;
    
    if (conditionsPtr != NULL) {
        conditionsPtr += strlen("'conditions':");  // 移动指针到值的起始位置

        // 查找单引号包裹的内容
        while (*conditionsPtr == ' ' || *conditionsPtr == '\t') {
            conditionsPtr++;
        }
        startQuote = conditionsPtr;
        endQuote = strrchr(conditionsPtr, '}');

        if (startQuote != NULL && endQuote != NULL && endQuote > startQuote) {
            // 计算长度并复制内容
            length = endQuote - startQuote - 1;
            strncpy(where_info->a_expr, startQuote + 1, length -1);
            where_info->a_expr[length] = '\0';
        }
    }
}

static void parse_join_info(const char *info_str, JoinInfo *join_info){
    const char *keywords[] = {"'table1': '", "'table2': '", "'A_expr': '"};
    char *result;
    char *closing_quote;

    for (int i = 0; i < 3; ++i) {
        result = strstr(info_str, keywords[i]);

        if (result != NULL) {
            result += 11;

            closing_quote = strchr(result, '\'');
            if (closing_quote != NULL) {
                size_t content_length = closing_quote - result;

                strncpy(i == 0 ? join_info->table_1 : (i == 1 ? join_info->table_2 : join_info->A_expr),result,content_length);

                if (i == 0){
                    join_info->table_1[content_length] = '\0';
                } 
                else if (i == 1){
                    join_info->table_2[content_length] = '\0';
                } else{
                    join_info->A_expr[content_length] = '\0';
                }
            }
        }
    }

}

static void parse_limit_info(const char *info_str, LimitInfo *limit_info){
    const char *numStart = strstr(info_str, "limit_num':");
    const char *offsetStart = strstr(info_str, "limit_offset':");

    const char *firstQuote, *secondQuote, *thirdQuote, *fourthQuote;
    
    // 对于 limit_num
    if (numStart) {
        numStart += 11; // 移动到第一个单引号后面
        firstQuote = strchr(numStart, '\'');
        secondQuote = strchr(firstQuote + 1, '\'');
        strncpy(limit_info->start, firstQuote + 1, secondQuote - firstQuote - 1);
        limit_info->start[secondQuote - firstQuote - 1] = '\0';
    }

    // 对于 limit_offset
    if (offsetStart) {
        offsetStart += 14; // 移动到第一个单引号后面
        thirdQuote = strchr(offsetStart, '\'');
        fourthQuote = strchr(thirdQuote + 1, '\'');
        strncpy(limit_info->offset, thirdQuote + 1, fourthQuote - thirdQuote - 1);
        limit_info->offset[fourthQuote - thirdQuote - 1] = '\0';
    }
}


static void parse_having_info(const char *info_str, HavingInfo *havingInfo) {
    const char *conditionsPtr = strstr(info_str, "'conditions':");
    const char *startQuote;
    const char *endQuote;
    size_t length;
    
    if (conditionsPtr != NULL) {
        conditionsPtr += strlen("'conditions':");  // 移动指针到值的起始位置

        // 查找单引号包裹的内容
        startQuote = strchr(conditionsPtr, '\'');
        endQuote = strrchr(conditionsPtr, '}');

        if (startQuote != NULL && endQuote != NULL && endQuote > startQuote) {
            // 计算长度并复制内容
            length = endQuote - startQuote - 1;
            strncpy(havingInfo->a_expr, startQuote + 1, length -1);
            havingInfo->a_expr[length] = '\0';
        }
    }
}

static void parse_groupBy_info(const char *info_str, GroupBYInfo *groupByInfo) {
    const char *current;
    const char *quoteEnd;
    const char *end;
    char tmp_string[5000];
    int i = 0, len;

    current = strstr(info_str, "'conditions': [");
    if (current != NULL) {
        current += 15;
        end = strchr(current, ']');

        for (i = 0; i < 10 && current < end; i++) {
            current = strchr(current, '\'');
            if (!current || current >= end) break;
            current++; // 跳过单引号

            quoteEnd = strchr(current, '\'');
            if (!quoteEnd || quoteEnd >= end) break;

            len = quoteEnd - current;
            if (len > 99) len = 99;
            strncpy(groupByInfo->a_exprs[i], current, len);
            groupByInfo->a_exprs[i][len] = '\0';

            current = quoteEnd + 1;
        }
        groupByInfo->expr_count = i;
            
    }
  
}

static void parse_aggregation_info(const char *info_str, AggInfo *aggInfo) {
    const char *start, *end;
    int i = 0, len;

    // 解析 columns
    start = strstr(info_str, "'columns': [");
    if (start) {
        start += 12; // 跳过 "'columns': ["

        while (start != NULL && i < 10) {
            start = strchr(start, '\'');
            if (start == NULL) break;
            start++; // 跳过 '\''

            end = strchr(start, '\'');
            if (end == NULL) break;

            len = end - start;
            if (len > 99) len = 99; // 确保不会溢出列名数组

            strncpy(aggInfo->columns[i], start, len);
            aggInfo->columns[i][len] = '\0'; // 确保字符串以null结束

            i++;
            if (*(end + 1) == ']') {
                break; 
            }
            start = strchr(end, ',');
        }
    }
    aggInfo->column_count = i;

    // 重置 i 用于解析 alias
    i = 0;
    // 解析 aliases
    start = strstr(info_str, "'aliases': [");
    if (start) {
        start += 12; // 跳过 "'aliases': ["

        while (start != NULL && i < aggInfo->column_count) {
            start = strchr(start, '\'');
            if (start == NULL) break;
            start++; // 跳过 '\''

            end = strchr(start, '\'');
            if (end == NULL) break;

            len = end - start;
            if (len > 99) len = 99; // 确保不会溢出别名数组

            strncpy(aggInfo->alias[i], start, len);
            aggInfo->alias[i][len] = '\0'; // 确保字符串以null结束

            i++;
            if (*(end + 1) == ']') {
                break; 
            }
            start = strchr(end, ',');
        }
    }

     // 解析 distinct
    start = strstr(info_str, "'distincts': [");
    if (start) {
        start += 14; // 跳过 "'distincts': ["

        i = 0;
        while (start != NULL && i < aggInfo->column_count) {
            if (*start == '0' || *start == '1') {
                aggInfo->distinct[i] = *start - '0';
                i++;
            }
            if (*(start + 1) == ']') {
                break; 
            }
            start++;
        }
    }
}

static void parse_specific_info(DataOperationNode *node, char *info_str) {
    if (strcmp(node->operation, "select") == 0) {
        parse_select_info(info_str, &node->specific_info.select_info);
    } else if (strcmp(node->operation, "order_by") == 0) {
        parse_OrderBy_info(info_str, &node->specific_info.order_by_info);                                                                  
    } else if (strcmp(node->operation, "filter") == 0) {
        parse_where_info(info_str, &node->specific_info.where_info);   
    } else if (strcmp(node->operation, "join") == 0) {
        parse_join_info(info_str, &node->specific_info.joinInfo);   
    } else if (strcmp(node->operation, "limit") == 0) {
        parse_limit_info(info_str, &node->specific_info.limitInfo);   
    } else if (strcmp(node->operation, "having") == 0) {
        parse_having_info(info_str, &node->specific_info.havingInfo);   
    } else if (strcmp(node->operation, "group_by") == 0) {
        parse_groupBy_info(info_str, &node->specific_info.groupBYInfo);   
    } else if (strcmp(node->operation, "aggregation") == 0) {
        parse_aggregation_info(info_str, &node->specific_info.aggInfo);   
    }
}

static void parse_table_list(DataOperationNode* node, char *line){

    char *source_table_start = strstr(line, "source_table: [");
    int table_index = 0;
    char *source_table_end;
    char source_table_str[500] = {0};
    char *token;

    if (source_table_start) {
        source_table_start += strlen("source_table: [");  // 移动到 '[' 后面的第一个字符
        source_table_end = strchr(source_table_start, ']');
        if (source_table_end) {   
            strncpy(source_table_str, source_table_start, source_table_end - source_table_start);

            // 分割 source_table 字符串并保存到数组中
            token = strtok(source_table_str, ", ");
            
            while (token != NULL && table_index < 10) {
                // 去掉单引号字符
                if (token[0] == '\'') {
                    strncpy(node->source_table[table_index], token + 1, 48);  // 去掉开头的单引号并复制
                    node->source_table[table_index][strlen(token) - 2] = '\0';  // 去掉末尾的单引号
                } else {
                    strncpy(node->source_table[table_index], token, 49);
                }
                token = strtok(NULL, ", ");
                table_index++;
            }
        }
    }
    node->table_count = table_index;
}

void print_node(DataOperationNode* node) {

    const char *base_filename = "/mnt/d/数据库/GPT3_project/src/GPT3/data/logical_plan/log/logical_plan_";
    char filename[150];
    FILE *file;
       
    sprintf(filename, "%s%d_tree.log", base_filename, globalVariable);
    file = fopen(filename, "a");
    if (file == NULL) {
        perror("Failed to open file");
        return ;
    }
    fprintf(file, "Operation: {%s}\nTarget_columns: {%s}\nTarget_steps: {%s}\nsource table are:",
           node->operation, node->Target_columns, node->Target_steps);
        
    for (int i = 0; i < node->table_count; i++){
        fprintf(file, "%s,", node->source_table[i]);         
    }   
    fprintf(file, "\n");   

    if (strcmp(node->operation, "select") == 0) {
        fprintf(file, "select columns are:");
        for (int i = 0; i < node->specific_info.select_info.column_count; i++){
            fprintf(file, "%s,", node->specific_info.select_info.columns[i]); 
            fprintf(file, "%s,", node->specific_info.select_info.alias[i]);        
            fprintf(file, "%d.\n", node->specific_info.select_info.distinct[i]); 
        }       
    } else if (strcmp(node->operation, "order_by") == 0) {
        fprintf(file, "order by columns are:");
        for (int i = 0; i < node->specific_info.order_by_info.column_count; i++){
            fprintf(file, "%s,", node->specific_info.order_by_info.columns[i]);  
            fprintf(file, "%d.", node->specific_info.order_by_info.sortByDirArray[i]);       
        }                                                   
    } else if (strcmp(node->operation, "filter") == 0) {
        fprintf(file, "filter conditions are:");
        fprintf(file, "%s", node->specific_info.where_info.a_expr);  
    }
    else if (strcmp(node->operation, "join") == 0) {
        fprintf(file, "join are:");
        fprintf(file, "%s %s %s", node->specific_info.joinInfo.table_1, node->specific_info.joinInfo.table_2, node->specific_info.joinInfo.A_expr);  
    }
    else if (strcmp(node->operation, "limit") == 0) {
        fprintf(file, "limit are:");
        fprintf(file, "%s %s", node->specific_info.limitInfo.start, node->specific_info.limitInfo.offset);  
    }
    else if (strcmp(node->operation, "group_by") == 0) {
        fprintf(file, "group_by are:");
       for (int i = 0; i < node->specific_info.groupBYInfo.expr_count; i++){
            fprintf(file, "%s,", node->specific_info.groupBYInfo.a_exprs[i]);  
        }  
    }
    else if (strcmp(node->operation, "having") == 0) {
        fprintf(file, "having are:");
        fprintf(file, "%s", node->specific_info.havingInfo.a_expr);  
    }
    else if (strcmp(node->operation, "aggregation") == 0) {
        fprintf(file, "aggregation are:");
        for (int i = 0; i < node->specific_info.aggInfo.column_count; i++){
            fprintf(file, "%s,", node->specific_info.aggInfo.columns[i]); 
            fprintf(file, "%s,", node->specific_info.aggInfo.alias[i]);        
            fprintf(file, "%d.\n", node->specific_info.aggInfo.distinct[i]); 
        }     
    }

    fprintf(file, "\n");

    fclose(file);
}

static List* generate_aggregation(DataOperationNode* node , List* pre){   // target获取
    int j = 0;
    ResTarget *t;
    FuncCall * funcCall;

    for (j = 0; j < node->specific_info.aggInfo.column_count; j ++){
        
        t = makeNode(ResTarget);
        if (node->specific_info.aggInfo.alias[j][0] != '0'){
            t->name = strdup(node->specific_info.aggInfo.alias[j]);
        } else {
            t->name = NULL;
        }
        
        t->indirection = NIL;
        t->val = parseA_exprClause((Node *) NIL, node->specific_info.aggInfo.columns[j]);
        if (t->val->type == T_FuncCall && node->specific_info.aggInfo.distinct[j] == 1){
            funcCall = (FuncCall *)t->val;
            funcCall->agg_distinct = true;
            t->val = (Node*)funcCall;
        } 

        t->location = -1;

        if (pre != NIL){
            pre = lappend(pre, t);
        } 
        else {
            pre = list_make1(t);
        }     
    }  
    return pre;
}

static List* generate_target(DataOperationNode* node , List* pre){   // target获取
    int j = 0;
    ResTarget *t;
    FuncCall * funcCall;

    for (j = 0; j < node->specific_info.select_info.column_count; j ++){
        
        t = makeNode(ResTarget);
        if (node->specific_info.select_info.alias[j][0] != '0'){
            t->name = strdup(node->specific_info.select_info.alias[j]);
        } else {
            t->name = NULL;
        }
        
        t->indirection = NIL;
        t->val = parseA_exprClause((Node *) NIL, node->specific_info.select_info.columns[j]);
        if (t->val->type == T_FuncCall && node->specific_info.select_info.distinct[j] == 1){
            funcCall = (FuncCall *)t->val;
            funcCall->agg_distinct = true;
            t->val = (Node*)funcCall;
        } 

        t->location = -1;

        if (pre != NIL){
            pre = lappend(pre, t);
        } 
        else {
            pre = list_make1(t);
        }     
    }  
    return pre;
}

static List* generate_orderBy(DataOperationNode* node , List* pre){   // target获取
    int j = 0;
    SortBy *t;
    List *ret = pre;

    for (j = 0; j < node->specific_info.order_by_info.column_count; j ++){
        t = makeNode(SortBy);
        t->node = parseA_exprClause((Node *) NIL, node->specific_info.order_by_info.columns[j]);
        t->sortby_dir = node->specific_info.order_by_info.sortByDirArray[j];  
        t->sortby_nulls = SORTBY_NULLS_DEFAULT;
        t->useOp = NIL;
        t->location = -1;

        if (ret != NIL){
            ret = lappend(ret, t);
        } 
        else {
            ret = list_make1(t);
        }     
    } 
    return ret;
}

static Node* make_tableRef(char *table_name){
    RangeVar* r;
    Node *table_ref;

    r = makeRangeVar(NULL, strdup(table_name), -1);
    // relation_expr
    r->inh = true;
    r->alias = NULL;
    // table ref
    table_ref = (Node *)r;

    return table_ref;
}

static void generate_join(JoinInfo * joinInfo){
    int j, left = 0;
    int index = -1;
    Node* ret;
    Node* pre = (Node*)NIL;
    JoinExpr *n = makeNode(JoinExpr);
    n->jointype = JOIN_INNER;
    n->isNatural = false;  

    for (j = 0; j < final_source_table_count; j ++){
        if (strcmp(joinInfo->table_1, final_source_table[j]) == 0 && visit_table[j] == 1){
            index = j;
            left = 1;
            break;
        } 
        else if (strcmp(joinInfo->table_2, final_source_table[j]) == 0 && visit_table[j] == 1){
            index = j;
            break;
        }
    }
    if (index != -1){
        pre = join_nodes[index];
    }
 
    if (pre == (Node *)NIL){
        n->larg = make_tableRef(joinInfo->table_1);
        n->rarg = make_tableRef(joinInfo->table_2);
    } else {
        if (left == 1){
            n->larg = pre;
            n->rarg = make_tableRef(joinInfo->table_2);
        }
        else {
            n->larg = pre;
            n->rarg = make_tableRef(joinInfo->table_1);
        }
    }
    
    n->quals = parseA_exprClause((Node *)NIL, joinInfo->A_expr); /* ON clause */

    ret = (Node *)n;

    for (j = 0; j < final_source_table_count; j ++){
        if (pre != (Node *)NIL){
            if (join_nodes[j] == pre){
                join_nodes[j] = ret;
            } 
        }
        if (strcmp(joinInfo->table_1, final_source_table[j]) == 0){
            join_nodes[j] = ret;
            visit_table[j] = 1;
        }
        if (strcmp(joinInfo->table_2, final_source_table[j]) == 0){
            join_nodes[j] = ret;
            visit_table[j] = 1;
        }
    }
}

static List* generate_from(){  
    int j = 0, i = 0;
    List *filter = NIL;
    Node *table_ref;
    Node* uniqueArray[10]; 
    int uniqueCount = 0;

    //先找没有join的表
    for (j = 0; j < final_source_table_count; j ++){

        if (visit_table[j] != 1){
        
            table_ref = (Node *)make_tableRef(final_source_table[j]);

            if (filter != NIL){
                filter = lappend(filter, table_ref);
            } 
            else {
  
                filter = list_make1(table_ref);
            }  
        }
    } 

    for (i = 0; i < final_source_table_count; i ++){
        // 判断当前指针是否已经存在于临时数组中
        int exists = 0;
        for (size_t j = 0; j < uniqueCount; ++j) {
            if (join_nodes[i] == uniqueArray[j]) {
                exists = 1;
                break;
            }
        }

        // 如果不存在，将其添加到临时数组中
        if (!exists && visit_table[i] == 1) {

            uniqueArray[uniqueCount++] = join_nodes[i];
        }
    }


    // 将不同的指针加入filter中
    for (i = 0; i < uniqueCount; i ++) {
  
        if (filter != NIL){
            filter = lappend(filter, uniqueArray[i]);
        } 
        else {
       
            filter = list_make1(uniqueArray[i]);
        }
    }

    return filter;
}

static SelectLimit* generate_limit(LimitInfo* limitInfo){
    SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
    if (limitInfo->offset[0] != '\0' && limitInfo->offset[0] != '0'){
        n->limitOffset = parseA_exprClause((Node*)NIL, limitInfo->offset);
    } else {
        n->limitOffset = (Node*)NIL;
    }
    if (limitInfo->start[0] != '\0' && limitInfo->start[0] != '0'){
        n->limitCount = parseA_exprClause((Node*)NIL, limitInfo->start);
    } else {
        n->limitCount = (Node*)NIL;
    }

    n->limitOption = LIMIT_OPTION_COUNT;
    return n;
}

static List* generate_group_by(GroupBYInfo* group_by_info, List* group_clause){
    int i;
    List * ret = group_clause;
    Node * tmp = (Node *)NIL;
    for (i = 0; i < group_by_info->expr_count; i ++){

        tmp = parseA_exprClause((Node *) NIL, group_by_info->a_exprs[i]);
    
        if (ret != NIL){
            ret = lappend(ret, tmp);
        } 
        else {
            ret = list_make1(tmp);
        }
    }
    return ret;
}

static ExplainStmt* generate_Explain(Node *query) {

    ExplainStmt *n = makeNode(ExplainStmt);
    DefElem *arg;
    List* t;
    n->query = query;
    
    
    // verbose 参数
    arg = makeDefElem(strdup("verbose"), NULL, -1);
    t = list_make1(arg);

    // format json
    arg = makeDefElem(strdup("format"), (Node *)makeString(strdup("json")), -1);
    t = lappend(t, arg);

    n->options = t;

    return n;
}

static List* convert_tree(DataOperationTree* dataTree){
    Node * tree;
    SelectStmt *n = makeNode(SelectStmt);
    List* parse_tree;
    List* tmp_target_list = NIL;
    List* tmp_sort_list = NIL;
    Node *tmp_where_node = (Node*)NIL;
    SelectLimit* select_limit = (SelectLimit* )NIL;
    Node *having_clasue = (Node*)NIL;
    List* group_clause = NIL;

    n->distinctClause = NIL;
    n->targetList = NIL;  
    n->intoClause = (IntoClause* )NIL; 
    n->fromClause = NIL;
    n->whereClause = (Node *)NIL;  
    n->groupClause = NIL;  
    n->havingClause = (Node *)NIL;  
    n->windowClause = NIL; 

    for (int i = 1; i <= dataTree->NodeNum; i++){
    
        if (strcmp(dataTree->NodeTree[i].operation, "select") == 0) {
            
            tmp_target_list = generate_target(&dataTree->NodeTree[i], tmp_target_list);
        } 
        else if (strcmp(dataTree->NodeTree[i].operation, "order_by") == 0) {
            tmp_sort_list = generate_orderBy(&dataTree->NodeTree[i], tmp_sort_list);  
        }
        else if (strcmp(dataTree->NodeTree[i].operation, "filter") == 0) {
            tmp_where_node = parseA_exprClause(tmp_where_node, dataTree->NodeTree[i].specific_info.where_info.a_expr);   
        }
        else if (strcmp(dataTree->NodeTree[i].operation, "distinct") == 0) {
            // gram.c 40685
            List* distinct = list_make1(NIL);
            n->distinctClause = distinct;
        }
        else if (strcmp(dataTree->NodeTree[i].operation, "join") == 0) { 
            generate_join(&dataTree->NodeTree[i].specific_info.joinInfo);
        }
        else if (strcmp(dataTree->NodeTree[i].operation, "limit") == 0) { 
            select_limit = generate_limit(&dataTree->NodeTree[i].specific_info.limitInfo);
        } 
        else if (strcmp(dataTree->NodeTree[i].operation, "group_by") == 0) { 
            group_clause = generate_group_by(&dataTree->NodeTree[i].specific_info.groupBYInfo, group_clause);
        }
        else if (strcmp(dataTree->NodeTree[i].operation, "having") == 0) { 
            having_clasue = parseA_exprClause((Node*) NIL, dataTree->NodeTree[i].specific_info.havingInfo.a_expr);
        }  
        else if (strcmp(dataTree->NodeTree[i].operation, "aggregation") == 0) {
            tmp_target_list = generate_aggregation(&dataTree->NodeTree[i], tmp_target_list);
        } 
    }

    n->targetList = tmp_target_list;
    n->sortClause = tmp_sort_list;
    n->whereClause = tmp_where_node;
    n->fromClause = generate_from();
    n->groupClause = group_clause;
    n->havingClause = having_clasue;

    if (select_limit != (SelectLimit*) NIL){
        if (select_limit->limitCount != (Node*) NIL){
            n->limitCount = select_limit->limitCount;
        }
        if (select_limit->limitOffset != (Node*) NIL){
            n->limitOffset = select_limit->limitOffset;     
        }
    }

    tree = (Node *)n;

    #ifdef EXPLAIN_STATUS 
        tree = (Node *) generate_Explain(tree);
    #endif

    parse_tree = list_make1(makeRawStmt(tree, 0));

    return parse_tree;
}


List* convert_start(const char * query_string, List *raw_parsetree_list){
    List *ret_tree;
    int i,j, number = 0;
    const char *prefix = "SELECT ";
    
    for (i = 0; prefix[i] != '\0'; i++) {
        if (query_string[i] != prefix[i]) {
            return raw_parsetree_list;
        }
    }
    //try_echo(raw_parsetree_list);
    //return raw_parsetree_list;

    for (j = i; isdigit(query_string[j]); j++) {
        number = number * 10 + (query_string[j] - '0');
    }

    if (query_string[j] != ';') {
        return raw_parsetree_list;
    }
    globalVariable = number;
    if (number == 10000) {
        log_write(query_string, 1);
    } else {
        DataOperationTree dataTree; 
        const char *base_filename = "/mnt/d/数据库/GPT3_project/src/GPT3/data/logical_plan/tree/logical_plan_";
        char filename[150];
        FILE *file;
        char line[1000];
        int id = 1;

        sprintf(filename, "%s%d_tree.txt", base_filename, globalVariable);
        file = fopen(filename, "r");
        if (file == NULL){
            return NIL;
        }

        while (fgets(line, sizeof(line), file)) {
            char operation[100], target_steps[100], target_columns[300];
            char *start, *end, specific_info[1000] = {0};
            // 解析 specific_info
            start = strstr(line, "specific_info:");
            end = start ? strchr(start, '}') : NULL; 
            if (start && end) {  
                strncpy(specific_info, start, end - start + 1);
                specific_info[end - start + 1] = '\0';

                // 解析 Operation, Target_steps, Target_columns
                sscanf(line, "Node: Operation: {%[^}]}, Target_steps: {%[^}]}, Target_columns: {%[^}]},", 
                    operation, target_steps, target_columns);

                // 将解析的数据存储到结构体中
                strcpy(dataTree.NodeTree[id].operation, operation);
                strcpy(dataTree.NodeTree[id].Target_steps, target_steps);
                strcpy(dataTree.NodeTree[id].Target_columns, target_columns);
                
                // 解析 specific_info 和 source_table
                parse_specific_info(&dataTree.NodeTree[id], specific_info); 
                parse_table_list(&dataTree.NodeTree[id], line);

                id++;
            }
            
            /*
                if (dataTree.NodeTree[id].fatherId > 0) {
                    dataTree.NodeTree[dataTree.NodeTree[id].fatherId].children[dataTree.NodeTree[dataTree.NodeTree[id].fatherId].childcount++] = id;
                }
            */
        }
        dataTree.NodeNum = id - 1;
        get_final_table_list(&dataTree.NodeTree[dataTree.NodeNum]);

        for (int i = 1; i <= dataTree.NodeNum; i++){
            //print_node(&dataTree.NodeTree[i]);
        }

        fclose(file);

        ret_tree = convert_tree(&dataTree);

        free_pointer();

        //try_echo(ret_tree);
        //try_echo(raw_parsetree_list);
        return ret_tree;
    } 
    
    return raw_parsetree_list;
}

List* my_start(const char * query_string, List *raw_parsetree_list){
    FILE *file;
    int num[1000];
    int i = 0;

    if (strncmp(query_string, "select", 5) != 0){
        return NIL;
    }
    // 打开文件
    file = fopen("/mnt/d/数据库/GPT3_project/src/GPT3/config/bird_train_big_id.txt", "r");
    if (file == NULL) {
        log_write("Error opening file", 1);
        return NIL;
    }

    // 读取文件中的每一行
    while (fscanf(file, "%d", &num[i]) == 1) {
        i++;
        if (i >= 1000) {
            break;
        }
    }

    fclose(file);

    // 打印读取的数据，用于验证
    for (int j = 0; j < i; j++) {
        globalVariable = num[j];
        convert_start(query_string, raw_parsetree_list);
    }
    return NIL;
}


