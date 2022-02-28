
import logging
from lark import Lark, logger

logger.setLevel(logging.DEBUG)


grammar = '''

        program          : (stmnt ";") * EOF

        stmnt            : select_expr
                         | create_stmnt
                         | drop_stmnt
                         | insert_stmnt
                         | update_stmnt
                         | delete_stmnt
                         | truncate_stmnt

        select_expr       : select_clause
                          | from_clause
                          | where_clause
                          | group_by_clause
                          | having_clause
                          | order_by_clause
                          | limit_clause

        select_clause    : "select" (expr ",")* (expr)

        expr             : column_name
                         | case_stmnt
                         | func_call
                         | sub_query
                         | literal
                         | or_clauses
                         | "(" expr ")"

        case_stmnt       : "case" ("when" expr "then" expr)+ "else" expr
        func_call        : IDENTIFIER "(" (func_arg ",")* )
        func_arg         : expr

        from_clause      : "from" from_location
        from_location    : source_name source_alias?
                         | joined_objects
                         | ( select_expr ) source_alias
        joined_objects   : cross_join | non_cross_join
        inner_join       : from_location "inner"? "join" from_location "on" expr
        outer_join       : from_location ("left" | "right" | "full") "outer"? "on" expr
        cross_join       : from_location "cross" "join" from_location

        source_name      : IDENTIFIER
        column_name      : IDENTIFIER

        or_clauses       : or_clause*
        or_clause        : (and_clause "or")* (and_clause)
        and_clause       : (predicate "and")* (predicate)

        and_clause       : (predicate "and")* (predicate)
        predicate        : term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term )
        term             : factor ( ( "-" | "+" ) factor )*
        factor           : unary ( ( "/" | "*" ) unary )*
        unary            : ( "!" | "-" ) unary
                         | primary
        primary          : INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                         | "(" expr ")"
                         | IDENTIFIER

        sub_query        : "(" select_expr ")"

        where_clause     : "where" expr
        group_by_clause  : "group" "by" (column_name ",")? (column_name)+
        having_clause    : "having" or_clause
        order_by_clause  : "order" "by" (column_name (asc|desc)?)*
        limit_clause     : "limit" INTEGER_NUMBER, ("offset" INTEGER_NUMBER)?

        create_stmnt     : "create" "table" table_name "(" column_def_list ")"
        column_def_list  : (column_def ",")* column_def
        column_def       : column_name datatype ("primary key")? ("not null")?
        table_name       : IDENTIFIER

        drop_stmnt       : "drop" "table" table_name

        insert_stmnt     : "insert" "into" table_name "(" column_name_list ")" "values" "(" value_list ")"
        column_name_list : (column_name ",")* column_name
        value_list       : (value ",")* value

        delete_stmnt     : "delete" "from" table_name ("where" where_clause)?

        update_stmnt     : "update" table_name "set" column_name = value ("where" where_clause)?

        truncate_stmnt   : "truncate" table_name

        or_clause        : (and_clause "or")* (and_clause)
        and_clause       : (predicate "and")* (predicate)

        and_clause       : (predicate "and")* (predicate)
        predicate        : term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term )
        term             : factor ( ( "-" | "+" ) factor )*
        factor           : unary ( ( "/" | "*" ) unary )*
        unary            : ( "!" | "-" ) unary
                         | primary
        primary          : INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                         | "(" expr ")"
                         | IDENTIFIER

        //INTEGER_NUMBER   : (0..9)+
        //FLOAT_NUMBER     : ("0".."9")+"."("0".."9)*
        // STRING           : '.*'
        //IDENTIFIER       : ("_" | ("a".."z") | ("A".."Z") | ("0".."9")+)+

		WHITESPACE: (" " | "\n")+z

        %import common.ESCAPED_STRING   -> STRING
        %import common.SIGNED_NUMBER    -> INTEGER_NUMBER
		%ignore WHITESPACE

'''
grammar2 = r'''
program          : (stmnt ";")* stmnt
stmnt            : select_expr
select_expr      : select_clause from_clause
select_clause    : "select"i column_name
from_clause      : "from"i from_location
from_location    : joined_objects

                 //(source_name [source_alias])

                 //| joined_objects
                 //| (sub_query [source_alias])

joined_objects   : inner_join | cross_join | outer_join
inner_join       : from_location ["inner"] "join" from_location "on" or_clauses
outer_join       : from_location ("left" | "right" | "full") "outer"? "join" "on" or_clauses
cross_join       : from_location "cross" "join"

expr             : sub_query
                 | or_clauses
                // | "(" expr ")"
                 | func_call
                 | case_stmnt
                 | column_name

or_clauses       : or_clause*
or_clause        : (and_clause "or")* and_clause
and_clause       : (predicate "and")* predicate
predicate        : term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term )*
term             : factor ( ( "-" | "+" ) factor )*
factor           : unary ( ( "/" | "*" ) unary )*
unary            : ( "!" | "-" ) unary
                 | primary
primary          : INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                 | "(" expr ")"
                 | IDENTIFIER

sub_query        : "(" select_expr ")"


case_stmnt       : "case" ("when" expr "then" expr)+ "else" expr
func_call        : func_name "(" (func_arg ",")* ")"
func_arg         : expr

column_name:  SCOPED_IDENTIFIER
source_name:  IDENTIFIER
source_alias: IDENTIFIER
func_name   : IDENTIFIER

IDENTIFIER       : ("_" | ("a".."z") | ("A".."Z") | ("0".."9")+)+
SCOPED_IDENTIFIER : (IDENTIFIER ".")* IDENTIFIER
FLOAT_NUMBER     : INTEGER_NUMBER "." ("0".."9")*
SEMICOLON        : ";"

%import common.ESCAPED_STRING   -> STRING
%import common.SIGNED_NUMBER    -> INTEGER_NUMBER
%import common.WS
%ignore WS
'''

grammar3 = '''
select_expr      : select_clause from_clause SEMICOLON
select_clause    : SELECT column_name
from_clause      : FROM from_location
from_location    : joined_objects
//from_location    : source_name source_alias "inner"? "join" source_name source_alias "on" or_clauses

joined_objects   : inner_join | outer_join
inner_join       : from_location "inner"? "join" from_location "on" or_clauses
outer_join       : from_location ("left" | "right" | "full") "outer"? "join" "on" or_clauses
cross_join       : from_location "cross" "join"

or_clauses       : or_clause*
?or_clause        : (and_clause "or")* and_clause
?and_clause       : (predicate "and")* predicate
?predicate        : term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term )*
?term             : factor ( ( "-" | "+" ) factor )*
?factor           : unary ( ( "/" | "*" ) unary )*
?unary            : ( "!" | "-" ) unary
                 | primary
?primary          : INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                 //| "(" expr ")"
                 | SCOPED_IDENTIFIER
                 | IDENTIFIER

column_name:  SCOPED_IDENTIFIER | IDENTIFIER
source_name:  IDENTIFIER
source_alias: IDENTIFIER

IDENTIFIER       : ("_" | ("a".."z") | ("A".."Z") | ("0".."9")+)+
SCOPED_IDENTIFIER : (IDENTIFIER ".")* IDENTIFIER
FLOAT_NUMBER     : INTEGER_NUMBER "." ("0".."9")*
SEMICOLON        : ";"
SELECT.1           : "select"i
FROM.1            : "from"i

%import common.ESCAPED_STRING   -> STRING
%import common.SIGNED_NUMBER    -> INTEGER_NUMBER
%import common.WS
%ignore WS

'''

grammar4 = '''
select_expr      : select_clause from_clause SEMICOLON
select_clause    : SELECT column_name
from_clause      : FROM from_location
from_location    : joined_objects
//from_location    : source_name source_alias "inner"? "join" source_name source_alias "on" or_clauses

joined_objects   : inner_join | outer_join
inner_join       : source_name source_alias "inner"? "join" source_name source_alias "on" or_clauses
//outer_join       : source_name source_alias ("left" | "right" | "full") "outer"? "join" source_name source_alias "on" or_clauses
cross_join       : source_name "cross" "join"

or_clauses       : or_clause*
?or_clause        : (and_clause "or")* and_clause
?and_clause       : (predicate "and")* predicate
?predicate        : term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term )*
?term             : factor ( ( "-" | "+" ) factor )*
?factor           : unary ( ( "/" | "*" ) unary )*
?unary            : ( "!" | "-" ) unary
                 | primary
?primary          : INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                 //| "(" expr ")"
                 | SCOPED_IDENTIFIER
                 | IDENTIFIER

column_name:  SCOPED_IDENTIFIER | IDENTIFIER
source_name:  IDENTIFIER
source_alias: IDENTIFIER

IDENTIFIER       : ("_" | ("a".."z") | ("A".."Z") | ("0".."9")+)+
SCOPED_IDENTIFIER : (IDENTIFIER ".")* IDENTIFIER
FLOAT_NUMBER     : INTEGER_NUMBER "." ("0".."9")*
SEMICOLON        : ";"
SELECT.1           : "select"i
FROM.1            : "from"i

%import common.ESCAPED_STRING   -> STRING
%import common.SIGNED_NUMBER    -> INTEGER_NUMBER
%import common.WS
%ignore WS

'''


def sql_parser():

    # parser = Lark(grammar,  start="program")
    parser = Lark(grammar3,  parser='earley', start="select_expr", debug=True, ambiguity='explicit')
    #text = "select cola from foo;"
    text = "select cola from foo f join bar b on f.x <> b.w;"
    print(parser.parse(text))


def json_parser():

    jsonparser = Lark(r"""
        value: dict
             | list
             | ESCAPED_STRING
             | SIGNED_NUMBER
             | "true" | "false" | "null"

        list : "[" [value ("," value)*] "]"

        dict : "{" [pair ("," pair)*] "}"
        pair : ESCAPED_STRING ":" value

        %import common.ESCAPED_STRING
        %import common.SIGNED_NUMBER
        %import common.WS
        %ignore WS

        """, start='value')

    print(jsonparser.parse('{"key": ["item0", "item1", 3.14]}'))
    # print( _.pretty() )

# json_parser()
sql_parser()
