from __future__ import annotations
import logging
import sys
from dataclasses import dataclass
from lark import Lark, logger, ast_utils, Transformer
from typing import Any, List

logger.setLevel(logging.DEBUG)

this_module = sys.modules[__name__]


class _Ast(ast_utils.Ast):
    # this will be skipped
    pass

    def pretty(self, depth = 0) -> str:
        """
        return a pretty printed strinG
        :return:
        """



@dataclass
class Program(_Ast, ast_utils.AsList):
    statements: List[_Stmnt]


class _Stmnt(_Ast):
    pass


@dataclass
class SelectStmnt(_Ast):
    select_clause: _Selectables
    from_clause: FromClause = None
    group_by_clause: Any = None
    having_clause: Any = None
    order_by_clause: Any = None
    limit_clause: Any = None


@dataclass
class _Selectables(_Ast,  ast_utils.AsList):
    selections: List[Selectable]


@dataclass
class Selectable(_Ast):
    item: Any


@dataclass
class FromClause(_Ast):
    source: Any
    # where clauses is nested in from, i.e. in a select
    # a where clause without a from clause is invalid
    where_clause: Any = None


@dataclass
class SourceX(_Ast):
    source: Any
    join_modifier: Any = None
    other_source: Any = None
    join_condition: Any = None


# NOTES about lark
# NOTE: terminal we want to capture must be named

# this is a grammar for a subset of learndb-sql
grammar = '''
        program          : stmnt*
        stmnt            : select_stmnt | drop_stmnt
        select_stmnt     : select_clause from_clause? group_by_clause? having_clause? order_by_clause? limit_clause? SEMICOLON

        select_clause    : "select"i selectables
        selectables      : column_name ("," column_name)*
        from_clause      : "from"i source where_clause?
        where_clause     : "where"i condition
        group_by_clause  : "group"i "by"i column_name ("," column_name)*
        having_clause    : "having"i condition
        order_by_clause  : "order"i "by" (column_name ("asc"i|"desc"i)?)*
        limit_clause     : "limit"i INTEGER_NUMBER ("offset"i INTEGER_NUMBER)?

        // NOTE: there should be no on-clause on cross join and this will have to enforced post parse
        source               : table_name
                             | joining
        
        joining              : source join_modifier? "join"i table_name table_alias? "on"i condition
        
        //joining            : "join"i table_name "on"i condition
        //                   | "join"i table_name "on"i joining condition
                     
        // doesn't work - adapted from working
        //source             : table_name joining?
        //joinings           : joining
        //                   | "join"i table_name joinings        
        //joining            : join_modifier?  "join"i table_name table_alias? "on"i condition
                           
        // doesn't work
        //source           : joining? table_name table_alias?
        //joining          : source join_modifier? JOIN source ON condition
        
        // doesn't work
        //source           : table_name table_alias? joined_source?
        //joined_source    : join_modifier? JOIN table_name table_alias? ON condition
        
        join_modifier    : "inner" | ("left" "outer"?) | ("right" "outer"?) | ("full" "outer"?) | "cross"
        
        condition        : or_clause
        or_clause        : and_clause
                         | or_clause "or"i and_clause
        and_clause       : predicate
                         | and_clause "and"i predicate
   
        predicate        : comparison
                         | predicate ( EQUAL | NOT_EQUAL ) comparison
        comparison       : term
                         | comparison ( LESS_EQUAL | GREATER_EQUAL | LESS | GREATER ) term
        term             : factor
                         | term ( "-" | "+" ) factor
        factor           : unary
                         | factor ( "/" | "*" ) unary 
        unary            : primary
                         | ( "!" | "-" ) unary                 
        primary          : INTEGER_NUMBER | FLOAT_NUMBER | STRING | "true" | "false" | "null"
                         | IDENTIFIER

        drop_stmnt       : "drop"i "table"i table_name

        FLOAT_NUMBER     : INTEGER_NUMBER "." ("0".."9")*

        column_name      : IDENTIFIER
        table_name       : IDENTIFIER
        table_alias      : IDENTIFIER

        // keywords
        // define keywords as they have higher priority
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
        NOT_EQUAL         : ("<>" | "!=")

        SEMICOLON         : ";"

        IDENTIFIER.9       : ("_" | ("a".."z") | ("A".."Z"))* ("_" | ("a".."z") | ("A".."Z") | ("0".."9"))+

        %import common.ESCAPED_STRING   -> STRING
        %import common.SIGNED_NUMBER    -> INTEGER_NUMBER
        %import common.WS
        %ignore WS
'''



class ToAst(Transformer):
    pass
    # todo: this should convert literals to datatype


def driver0():
    parser = Lark(grammar, parser='earley', start="program", debug=True) # , ambiguity='explicit')
    # text = "select cola from foo;"
    text = "select cola from foo f join bar b on f.x <> b.w;"
    text = "select cola, colb from foo where cola <> colb and colx > coly;"
    text = "select cola, colb from foo join bar on where cola <> colb and colx > coly;"
    text = "select cola, colb from foo left outer join bar on where cola <> colb and colx > coly having foo > 4;    "
    text = """select cola, colb from foo left outer join bar b on x = 1 join jar j on jb > xw where cola <> colb and colx > coly"""
    text = """select cola, colb from foo left outer join bar b on x = 1 join bar b on b = c where cola <> colb;  """
    text = """select cola, colb from foo join bar join bar join jar where cola <> colb;  """
    text = """select cola, colb from foo join bar on x = 1 join bar on y = x join jar on x=y where cola <> colb;"""
    text = """select cola, colb from foo join bar on x = 1 join bar on y = x left join jar on x=y where cola <> colb group by foofoo;"""
    text = """select cola, colb from foo join bar b on x = 1 join bar on y = x left join jar on x=y where cola <> colb group by foofoo;"""
    #text = """select cola, colb from foo left outer join bar on x = 1  where cola <> colb and colx > coly;"""
    #text = "drop table foo"
    #print(parser.parse(text))
    tree = parser.parse(text)
    print(tree.pretty())


def driver():
    parser = Lark(grammar, parser='earley', start="program", debug=True)
    # text = "select cola from foo;"
    text = "select cola from foo f join bar b on f.x <> b.w;"
    text = "select cola, colb from foo where cola <> colb and colx > coly;"
    text = "select cola, colb from foo join bar on where cola <> colb and colx > coly;"
    text = "select cola, colb from foo left outer join bar on where cola <> colb and colx > coly having foo > 4;    "
    text = """select cola, colb from foo left outer join bar b on x = 1 left join jar j on jb = xw where cola <> colb and colx > coly;"""
    #text = "drop table foo"
    #print(parser.parse(text))
    tree = parser.parse(text)
    transformer = ast_utils.create_transformer(this_module, ToAst())
    tree = transformer.transform(tree)
    print(tree)
    #print(tree.children[0].select_clause.children[0].Selections)
    return tree


driver0()
#driver()
