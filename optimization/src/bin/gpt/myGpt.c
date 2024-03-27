
#include "postgres_fe.h"
#include "parser/convert.h"
#include <stdio.h>
#include <string.h>
#include "postgres.h"

int main(int argc, char *argv[]) {
    char query_string[256] = "test";

    List * ret = convert_start(query_string, NIL);

    return 0;
}