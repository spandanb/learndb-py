# lark grammar for a subset of learndb-sql using
GRAMMAR = '''
        program          : stmnt
                         | terminated
                         | (terminated)+ stmnt?

        ?terminated      : stmnt ";"
        ?stmnt           : select_stmnt | drop_stmnt | delete_stmnt | update_stmnt | truncate_stmnt | insert_stmnt
                         | create_stmnt

        // we only want logically valid statements; and from is required for all other clauses
        // and so is nested under from clause
        select_stmnt     : select_clause from_clause?
        select_clause    : "select"i selectable ("," selectable)*
        selectable       : expr

        from_clause      : "from"i source where_clause? group_by_clause? having_clause? order_by_clause? limit_clause?
        where_clause     : "where"i condition
        group_by_clause  : "group"i "by"i column_name ("," column_name)*
        having_clause    : "having"i condition
        order_by_clause  : "order"i "by"i (column_name ("asc"i|"desc"i)?)*
        limit_clause     : "limit"i INTEGER_NUMBER ("offset"i INTEGER_NUMBER)?

        source            : single_source
                          | joining

        single_source      : table_name table_alias?

        //split conditioned and unconditioned (cross) join as cross join does not have an on-clause
        ?joining          : unconditioned_join | conditioned_join
        conditioned_join  : source join_modifier? "join"i single_source "on"i condition
        unconditioned_join : source "cross"i "join"i single_source

        join_modifier    : inner | left_outer | right_outer | full_outer

        inner            : "inner"i
        left_outer       : "left"i ["outer"i]
        right_outer      : "right"i ["outer"i]
        full_outer       : "full"i ["outer"i]
        cross            : "cross"i

        // `expr` is the de-facto root of the expression hierarchy
        expr             : condition
        condition        : or_clause
        or_clause        : and_clause
                         | or_clause "or"i and_clause
        and_clause       : predicate
                         | and_clause "and"i predicate

        // predicate and comparison are separate so =, <> have lower precedence than other comp ops
        predicate        : comparison
                         | predicate ( EQUAL | NOT_EQUAL ) comparison
        comparison       : term
                         | comparison ( LESS_EQUAL | GREATER_EQUAL | LESS | GREATER ) term
        term             : factor
                         | term ( MINUS | PLUS ) factor
        factor           : unary
                         | factor ( SLASH | STAR ) unary
        unary            : primary
                         | ( BANG | MINUS ) unary

        primary          : literal
                         | nested_select
                         | column_name
                         | func_call

        literal          : INTEGER_NUMBER | REAL_NUMBER | STRING | TRUE | FALSE | NULL

        nested_select    : "(" select_stmnt ")"

        // func calls; positional invocations only for now
        func_call        : func_name "(" func_arg_list ")"
        // TODO: add support for named args in func_arg_list
        // arbitrary expr can be a function argument, since we want to support algebraic expressions on func arguments,
        // e.g. some_func(col_x + 1)
        func_arg_list    : (expr ",")* expr

        create_stmnt     : "create"i "table"i table_name "(" column_def_list ")"
        ?column_def_list  : (column_def ",")* column_def
        ?column_def       : column_name datatype primary_key? not_null?
        datatype         : INTEGER | TEXT | BOOL | NULL | REAL

        primary_key      : "primary"i "key"i
        not_null         : "not"i "null"i

        drop_stmnt       : "drop"i "table"i table_name

        insert_stmnt     : "insert"i "into"i table_name "(" column_name_list ")" "values"i "(" value_list ")"
        column_name_list : (column_name ",")* column_name
        value_list       : (literal ",")* literal

        delete_stmnt     : "delete"i "from"i table_name where_clause?

        update_stmnt     : "update"i table_name "set"i column_name "=" literal where_clause?

        truncate_stmnt   : "truncate"i table_name

        // datatype values
        TRUE             : "true"i
        FALSE            : "false"i

        // func names are globally defined, i.e. not a multipart scoped name
        func_name        : IDENTIFIER
        column_name      : SCOPED_IDENTIFIER
        table_name       : SCOPED_IDENTIFIER
        table_alias      : IDENTIFIER

        // keywords
        INTEGER          : "integer"i
        TEXT             : "text"i
        BOOL             : "bool"i
        NULL             : "null"i
        // floating point type
        REAL            : "real"i

        // operators
        STAR              : "*"
        LEFT_PAREN        : "("
        RIGHT_PAREN       : ")"
        LEFT_BRACKET      : "["
        RIGHT_BRACKET     : "]"
        DOT               : "."
        EQUAL             : "="
        LESS              : "<"
        GREATER           : ">"
        COMMA             : ","
        MINUS             : "-"
        PLUS              : "+"
        SLASH             : "/"
        BANG              : "!"

        // 2-char ops
        LESS_EQUAL        : "<="
        GREATER_EQUAL     : ">="
        NOT_EQUAL         : "<>" | "!="

        // todo: remove
        SEMICOLON         : ";"

        IDENTIFIER       : ("_" | ("a".."z") | ("A".."Z"))* ("_" | ("a".."z") | ("A".."Z") | ("0".."9"))+
        SCOPED_IDENTIFIER : (IDENTIFIER ".")* IDENTIFIER

        // single quoted string
        // NOTE: this doesn't have any support for escaping
        SINGLE_QUOTED_STRING  : /'[^']*'/
        STRING: SINGLE_QUOTED_STRING | DOUBLE_QUOTED_STRING

        // ref: https://github.com/lark-parser/lark/blob/master/lark/grammars/common.lark
        %import common.ESCAPED_STRING   -> DOUBLE_QUOTED_STRING
        %import common.SIGNED_INT       -> INTEGER_NUMBER
        // floating point number
        %import common.SIGNED_NUMBER    -> REAL_NUMBER
        %import common.WS
        %ignore WS
'''
