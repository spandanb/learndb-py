
import logging
from lark import Lark, logger

logger.setLevel(logging.DEBUG)


# this is a grammar for a subset of learndb-sql
grammar = '''
        program          : (stmnt)
        ?stmnt            : select_stmnt | drop_stmnt
        ?select_stmnt     : select_clause from_clause? group_by_clause? having_clause? order_by_clause? limit_clause? SEMICOLON

        select_clause    : SELECT selectables
        selectables      : column_name ("," column_name)*

        having_clause    : "having"i or_clauses
        order_by_clause  : "order"i "by" (column_name ("asc"i|"desc"i)?)*
        limit_clause     : "limit"i INTEGER_NUMBER ("offset"i INTEGER_NUMBER)?


        // NOTE: there should be no on-clause on cross join and this will have to enforced post parse
        source           : table_name ( join_modifier? "join" table_name "on" condition)?
        join_modifier    : "inner" | ("left" "outer"?) | ("right" "outer"?) | ("full" "outer"?) | "cross"
        condition        : or_clauses

        or_clauses       : or_clause*
        or_clause        : and_clause ("or" and_clause)*
        and_clause       : predicate ("and" predicate)*
        predicate        : term ( ( ">" | ">=" | "<" | "<=" | "<>" | "=" ) term )*
        term             : factor ( ( "-" | "+" ) factor )*
        factor           : unary ( ( "/" | "*" ) unary )*
        unary            : ( "!" | "-" ) unary
                         | primary
        primary          : INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                         | IDENTIFIER

        drop_stmnt       : "drop" "table" table_name

        FLOAT_NUMBER     : INTEGER_NUMBER "." ("0".."9")*

        column_name      : IDENTIFIER
        table_name       : IDENTIFIER

        SELECT           : "select"i
        FROM             : "from"i
        WHERE            : "where"i
        SEMICOLON        : ";"

        IDENTIFIER       : ("_" | ("a".."z") | ("A".."Z") | ("0".."9")+)+

        %import common.ESCAPED_STRING   -> STRING
        %import common.SIGNED_NUMBER    -> INTEGER_NUMBER
        %import common.WS
        %ignore WS
'''
class SqlParser:
    """
    sql parser that exposes methods for different parts
    of the parsed sql. This is primarily, because I am not very
    familiar with lark, and in specific changing affecting the
    shape of the ast that is constructed. Thus, I don't want to bake
    this logic into the VM.
    """
    def __init__(self):
        self.lark = Lark(grammar, parser='earley', start="program", debug=True)
        self.tree = None

    def parse(self, text):
        self.lark.parse(text)


def old_func():
    parser = Lark(grammar, parser='earley', start="program", debug=True)
    # text = "select cola from foo;"
    text = "select cola from foo f join bar b on f.x <> b.w;"
    text = "select cola, colb from foo where cola <> colb and colx > coly;"
    text = "select cola, colb from foo join bar on where cola <> colb and colx > coly;"
    text = "select cola, colb from foo left outer join bar on where cola <> colb and colx > coly having foo > 4;    "
    #text = "drop table foo"
    #print(parser.parse(text))
    return parser.parse(text)


if __name__ == "__main__":
    x = old_func()
    # TODO: next look at how to extract values from tree
    # try and leverage tree native API
