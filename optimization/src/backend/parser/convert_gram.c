#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "parser/convert.h"

#define MAX_TOKEN_LENGTH 100

// 定义可能的 token 类型
typedef enum {
    TOKEN_AND,
    TOKEN_OR,
    TOKEN_NOT,
    TOKEN_LIKE,
    TOKEN_BETWEEN,
    TOKEN_OPERATOR,
    TOKEN_IDENTIFIER,
    TOKEN_CONSTSTR,
    TOKEN_FLOAT,
    TOKEN_INT,
    TOKEN_FUNC,
    TOKEN_END,
    TOKEN_START,
    TOKEN_UNKNOWN
} TokenType;

// 定义 token 结构
typedef struct {
    TokenType type;
    char value[MAX_TOKEN_LENGTH];
} Token;

const char *keywords[] = {"and", "or", "not", "like", "between", "max", "min", "sum", "count", "avg", NULL};

static void toLowerCase(char *str) {
    if (str == NULL) return;

    while (*str) {
        *str = tolower((unsigned char)*str);
        str++;
    }
}

static Node* create_columnRef(char *colname, List *indirection, int location){
	ColumnRef  *c = makeNode(ColumnRef);
	c->location = location;

	c->fields = lcons(makeString(strdup(colname)), indirection);
	return (Node *) c;
}

static Node *make_ColumnRef(char * str){
    char table_name[50];
    List * table_index = NIL;

    char *dotPosition = strchr(str, '.');

    if (dotPosition != NULL) {
        size_t dotIndex = dotPosition - str;
        strncpy(table_name, str, dotIndex);
        table_name[dotIndex] = '\0'; 

        table_index = list_make1((Node *)makeString(strdup(dotPosition + 1)));
        
        str = table_name;
    } 

    return (Node*)create_columnRef(str, table_index, -1);
}

static Node * makeIntConst(int val, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Integer;
	n->val.val.ival = val;
	n->location = location;

	return (Node *)n;
}

static Node *makeFloatConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Float;
	n->val.val.str = strdup(str);
	n->location = location;

	return (Node *)n;
}

static Node * makeStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = strdup(str);
	n->location = location;

	return (Node *)n;
}

static Node *makeAndExpr(Node *lexpr, Node *rexpr, int location)
{
	Node	   *lexp = lexpr;

	/* Look through AEXPR_PAREN nodes so they don't affect flattening */
	while (IsA(lexp, A_Expr) &&
		   ((A_Expr *) lexp)->kind == AEXPR_PAREN)
		lexp = ((A_Expr *) lexp)->lexpr;
	/* Flatten "a AND b AND c ..." to a single BoolExpr on sight */
	if (IsA(lexp, BoolExpr))
	{
		BoolExpr *blexpr = (BoolExpr *) lexp;

		if (blexpr->boolop == AND_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (Node *) blexpr;
		}
	}
	return (Node *) makeBoolExpr(AND_EXPR, list_make2(lexpr, rexpr), location);
}

static Node *makeOrExpr(Node *lexpr, Node *rexpr, int location)
{
	Node	   *lexp = lexpr;

	/* Look through AEXPR_PAREN nodes so they don't affect flattening */
	while (IsA(lexp, A_Expr) &&
		   ((A_Expr *) lexp)->kind == AEXPR_PAREN)
		lexp = ((A_Expr *) lexp)->lexpr;
	/* Flatten "a OR b OR c ..." to a single BoolExpr on sight */
	if (IsA(lexp, BoolExpr))
	{
		BoolExpr *blexpr = (BoolExpr *) lexp;

		if (blexpr->boolop == OR_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (Node *) blexpr;
		}
	}
	return (Node *) makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), location);
}

static Node *makeNotExpr(Node *expr, int location)
{
	return (Node *) makeBoolExpr(NOT_EXPR, list_make1(expr), location);
}

static Node *makeMyFunc(char **input_string, char *func){
    char extractedString[1000];
    char *ptr = *input_string;
    Node *ret;
    char *token;
    FuncCall *n;
    Node *func_arg_expr;
    List *func_arg_list = NIL;
    List *func_name;
    int distinct = 0;

    char *start = strchr(ptr, '(');
    char *end = strchr(ptr, ')');
    *input_string = end + 1;

    toLowerCase(func);
    func_name = list_make1(makeString(strdup(func)));
    
    if (*(start+1) == '*'){
        n = makeFuncCall(func_name, NIL, -1);
        n->agg_order = NIL;
        n->agg_star = true;
    }
    else if (start != NULL && end != NULL) {
        // 计算括号内字符串的长度
        size_t length = end - start - 1;
        
        // 复制括号内的字符串到提取的数组中
        strncpy(extractedString, start + 1, length);
        extractedString[length] = '\0';  // 添加字符串结束符

        // 用逗号分隔字符串并打印
        token = strtok(extractedString, ",");

        while (token != NULL) {
            while (*token && isspace(*token)) token++;
            if (strncasecmp(token, "distinct", 8) == 0) {
                distinct = 1;
                token += 8;
            }
            func_arg_expr = parseA_exprClause((Node *)NIL, token);  
            if (func_arg_list != NIL){
                func_arg_list = lappend(func_arg_list, func_arg_expr);
            } else {
                func_arg_list = list_make1(func_arg_expr);
            }
            token = strtok(NULL, ",");
        } 
        n = makeFuncCall(func_name, func_arg_list, -1);
        n->agg_order = NIL;
        if (distinct == 1){
            n->agg_distinct = true;
        }
    
    } else {
        return (Node *)NIL;
    }

    ret = (Node *)n;

    return ret;
}

static int checkKeyword(const char *word) {
    int i;
    for (i = 0; keywords[i] != NULL; i++) {
        if (strcasecmp(word, keywords[i]) == 0) {
            if (i < 5){
                return i; 
            } else {
                return TOKEN_FUNC;
            }
            
        }
    }
    return TOKEN_IDENTIFIER; // 不是关键词
}

static Token getNextToken(char ** input_string) {
    Token token;
    char *start;
    char *ptr = *input_string;

    token.type = TOKEN_UNKNOWN;
    token.value[0] = '\0';

     // 跳过空白
    while (*ptr && isspace(*ptr)) ptr++;

    // 检测输入结束
    if (*ptr == '\0') {
        token.type = TOKEN_END;
        return token;
    }

    // 识别数字
    if (isdigit(*ptr) || (*ptr == '.' && isdigit(*(ptr + 1)))) {
        int hasDecimal = 0; // 标记是否有小数点
        start = ptr;
        while (*ptr && (isdigit(*ptr) || *ptr == '.')) {
            if (*ptr == '.') {
                if (hasDecimal) break; // 第二次遇到小数点则停止
                hasDecimal = 1;
            }
            ptr++;
        }
        strncpy(token.value, start, ptr - start);
        token.value[ptr - start] = '\0';
        token.type = hasDecimal ? TOKEN_FLOAT : TOKEN_INT;

    }
    // 识别字符串常量
    else if (*ptr == '\'') {
        ptr++; // 跳过开头的单引号
        start = ptr;
        while (*ptr && *ptr != '\'') ptr++; // 找到结尾的单引号
        strncpy(token.value, start, ptr - start);
        token.value[ptr - start] = '\0';
        ptr++; // 跳过结尾的单引号
        token.type = TOKEN_CONSTSTR;
    }
    // 识别标识符或关键词
    else if (*ptr == '"') { // 检测双引号开头的情况
        start = ptr + 1; // 跳过开始的双引号
        ptr++; // 移动到下一个字符
        while (*ptr && *ptr != '"') ptr++; // 继续读取直到找到另一个双引号
        if (*ptr == '"') { // 确保找到了闭合的双引号
            strncpy(token.value, start, ptr - start);
            token.value[ptr - start] = '\0';
            token.type = TOKEN_IDENTIFIER; 
            ptr++; // 跳过结束的双引号
        }
    } else if (isalpha(*ptr)) { 
        start = ptr;
        while (*ptr && (isalnum(*ptr) || *ptr == '_' || *ptr == '.')) ptr++;
        strncpy(token.value, start, ptr - start);
        token.value[ptr - start] = '\0';
        token.type = checkKeyword(token.value);
    }
    // 识别操作符
    else {
        token.type = TOKEN_OPERATOR;
        token.value[0] = *ptr;
        // 检查是否为多字符操作符
        if ((*(ptr + 1) == '=' || *(ptr + 1) == '>') && 
            (*ptr == '<' || *ptr == '>' || *ptr == '!' || *ptr == '=')) {
            token.value[1] = *(ptr + 1);
            token.value[2] = '\0';
            ptr++;
        }
        else {
            token.value[1] = '\0';
        }
        ptr++;
    }
    *input_string = ptr;
    return token;
}

static Node* ConstProduce(char **input_string, Token token){
    int number;
    switch (token.type)
    {
    case TOKEN_IDENTIFIER: 
        return make_ColumnRef(token.value);
    case TOKEN_CONSTSTR:
        return makeStringConst(token.value, -1);
    case TOKEN_INT:
        number = atoi(token.value); 
        return makeIntConst(number, -1);
    case TOKEN_FLOAT:
        return makeFloatConst(token.value, -1);
    case TOKEN_FUNC:
        return makeMyFunc(input_string, token.value);
    default:
        log_write("ConstProduce error", 1);
    }
    return (Node *)NIL;
}

static Node* parseComplexExpression(char **input_string, Node *pre, Token* ret_token) {
    Token token;
    Node *n;
    Node *tmp;

    int number;

    token = getNextToken(input_string);
    ret_token->type = token.type;

    switch (token.type)
    {
    case TOKEN_AND:
        return pre;
    case TOKEN_OR:
        return pre;
    case TOKEN_NOT:
        return makeNotExpr(parseComplexExpression(input_string, pre, ret_token), -1);
    case TOKEN_LIKE:
        token = getNextToken(input_string);
        tmp = ConstProduce(input_string, token);
        n = (Node *)makeSimpleA_Expr(AEXPR_LIKE, "~~", pre, tmp, -1);
        return parseComplexExpression(input_string, n, ret_token);
    case TOKEN_BETWEEN:
        token = getNextToken(input_string);
        tmp = ConstProduce(input_string, token);
        token = getNextToken(input_string);
        if (token.type != TOKEN_AND){
            log_write("between and syntax\n", 1);
            return (Node *)NIL;
        }
        n = (Node *) makeSimpleA_Expr(AEXPR_BETWEEN,"BETWEEN",pre,(Node *) list_make2(tmp, parseComplexExpression(input_string, (Node *)NIL, ret_token)), -1);
        return n;
    case TOKEN_OPERATOR:
        if (token.value[0] == '+'){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "+", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '-'){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "-", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '%'){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "%", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '*'){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "*", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '/'){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "/", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '^'){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "^", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '<' && token.value[1] == '='){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "<=", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '>' && token.value[1] == '='){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, ">=", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '!' && token.value[1] == '='){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "<>", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '<' && token.value[1] == '>'){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "<>", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '<'){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "<", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '>'){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, ">", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else if(token.value[0] == '='){
            token = getNextToken(input_string);
            tmp = ConstProduce(input_string, token);
            n = (Node *)makeSimpleA_Expr(AEXPR_OP, "=", pre, tmp, -1);
            return parseComplexExpression(input_string, n, ret_token);
        }
        else {
            log_write("token operator error\n", 1);
            break;   
        }
    case TOKEN_IDENTIFIER: 
        n = make_ColumnRef(token.value);
        return parseComplexExpression(input_string, n, ret_token);
    case TOKEN_CONSTSTR:
        n = makeStringConst(token.value, -1);
        return parseComplexExpression(input_string, n, ret_token);
    case TOKEN_INT:
        number = atoi(token.value); 
        n = makeIntConst(number, -1);
        return parseComplexExpression(input_string, n, ret_token);
    case TOKEN_FLOAT:
        n = makeFloatConst(token.value, -1);
        return parseComplexExpression(input_string, n, ret_token);
    case TOKEN_FUNC:
        n = makeMyFunc(input_string, token.value);
        return parseComplexExpression(input_string, n, ret_token);
    case TOKEN_END:
        return pre;
    default:
        return pre;
    }
    return pre;
}

static Node* process_expr(Node *pre, char *string) {
    Node *result;
    Token token, pretoken;

    token.type = TOKEN_START;
    if (pre != (Node *)NIL){
        pretoken.type = TOKEN_AND;
    } else {
        pretoken.type = TOKEN_START;
    }
 
    while(token.type != TOKEN_END){
        result = parseComplexExpression(&string, (Node *)NIL, &token);
       // char print_tmp[20];sprintf(print_tmp, "token.type:%d\n", token.type);
       //log_write(print_tmp, 1);
        switch (pretoken.type){
            case TOKEN_AND:
                result = makeAndExpr(pre, result, -1);
                break;
            case TOKEN_OR:
                result = makeOrExpr(pre, result, -1);
                break;
            case TOKEN_END:
                break;
            default:
                break;
        }
        pre = result;
        pretoken.type = token.type;

    }
    return pre;
}


Node* parseA_exprClause(Node *pre, char *string) {
    Node* ret;

    ret = process_expr(pre, string);
   
    return ret; 
}