# this is a grammar for a subset of learndb-sql using
# lark syntax
GRAMMAR = '''
        program          : stmnt 
                         | terminated
                         | (terminated)+ stmnt?
        
        ?terminated      : stmnt ";"
        ?stmnt           : select_stmnt | drop_stmnt | delete_stmnt | update_stmnt | truncate_stmnt | insert_stmnt 
                         | create_stmnt
        select_stmnt     : select_clause from_clause? group_by_clause? having_clause? order_by_clause? limit_clause?

        select_clause    : "select"i selectables
        //selectables      : column_name ("," column_name)*
        selectables      : primary ("," primary)*
        from_clause      : "from"i source where_clause?
        where_clause     : "where"i condition
        group_by_clause  : "group"i "by"i column_name ("," column_name)*
        having_clause    : "having"i condition
        order_by_clause  : "order"i "by"i (column_name ("asc"i|"desc"i)?)*
        limit_clause     : "limit"i INTEGER_NUMBER ("offset"i INTEGER_NUMBER)?

        // NOTE: there should be no on-clause on cross join and this will have to enforced post parse
        ?source           : table_name table_alias?
                          | joining

        ?joining          : source join_modifier? "join"i table_name table_alias?
                          | source join_modifier? "join"i table_name table_alias? "on"i condition

        //join_modifier    : "inner"i | ("left"i "outer"i?) | ("right"i "outer"i?) | ("full"i "outer"i?) | "cross"i
        join_modifier    : inner | left_outer | right_outer | full_outer | cross

        inner            : "inner"i
        left_outer       : "left"i ["outer"i]
        right_outer      : "right"i ["outer"i]
        full_outer       : "full"i ["outer"i]
        cross            : "cross"i

        condition        : or_clause
        ?or_clause        : and_clause
                         | or_clause "or"i and_clause
        ?and_clause       : predicate
                         | and_clause "and"i predicate

        ?predicate       : comparison
                         | predicate ( EQUAL | NOT_EQUAL ) comparison
        ?comparison      : term
                         | comparison ( LESS_EQUAL | GREATER_EQUAL | LESS | GREATER ) term
        ?term            : factor
                         | term ( "-" | "+" ) factor
        ?factor          : unary
                         | factor ( "/" | "*" ) unary
        ?unary           : primary
                         | ( "!" | "-" ) unary
        ?primary         : INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true"i | "false"i | "null"i
                         | IDENTIFIER 
                         | SCOPED_IDENTIFIER
                         | nested_select
                         | func_call

        ?value : INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true"i | "false"i | "null"i

        // should this be expr
        nested_select    : "(" select_stmnt ")"

        // func calls; positional invocations only for now
        func_call        : func_name "(" func_arg_list ")" 
        func_arg_list    : (primary ",")* primary

        create_stmnt     : "create"i "table"i table_name "(" column_def_list ")"
        column_def_list  : (column_def ",")* column_def
        column_def       : column_name datatype ("primary"i "key"i)? ("not"i "null"i)?
        datatype         : "integer"i | "text"i | "bool"i | "null"i | "float"i

        drop_stmnt       : "drop"i "table"i table_name

        insert_stmnt     : "insert"i "into"i table_name "(" column_name_list ")" "values"i "(" value_list ")"
        column_name_list : (column_name ",")* column_name
        value_list       : (value ",")* value

        delete_stmnt     : "delete"i "from"i table_name where_clause?

        update_stmnt     : "update"i table_name "set"i column_name "=" value where_clause?

        truncate_stmnt   : "truncate"i table_name

        FLOAT_NUMBER     : INTEGER_NUMBER "." ("0".."9")*

        // func names are globally defined
        func_name        : IDENTIFIER
        column_name      : SCOPED_IDENTIFIER
        table_name       : SCOPED_IDENTIFIER
        table_alias      : IDENTIFIER

        // keywords
        // define keywords as they have higher priority
        // todo: nuke these?
        SELECT.5           : "select"i
        FROM.5             : "from"i
        WHERE.5            : "where"i
        JOIN.5             : "join"i
        ON.5               : "on"i

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
        
        %import common.ESCAPED_STRING   -> DOUBLE_QUOTED_STRING
        %import common.SIGNED_NUMBER    -> INTEGER_NUMBER
        %import common.WS
        %ignore WS
'''
