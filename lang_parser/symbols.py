from __future__ import annotations
"""
Contains symbol classes used by parser
"""

from typing import Any, List, Type, Tuple, Union
from dataclasses import dataclass
from .tokens import Token


@dataclass
class Symbol:
    """
    Symbol is the root of parser hierarchy.
    Comparable to how tokens compose the tokenizer's output, i.e. a stream of tokens,
    Symbols compose the parser's output, i.e. the AST

    """
    def accept(self, visitor: 'Visitor') -> Any:
        return visitor.visit(self)


@dataclass
class Program(Symbol):
    statements: List[Union[SelectExpr, CreateTableStmnt]]


@dataclass
class CreateTableStmnt(Symbol):
    table_name: Token
    column_def_list: List[ColumnDef]


@dataclass
class ColumnDef(Symbol):
    column_name: Token
    datatype: Token


@dataclass
class SelectExpr(Symbol):
    """
    NOTE: Expr produce one or more value(s) but do not change the system;
    Stmnt change the state of the system; and may or may not
    return value
    """
    selectable: Selectable
    from_location: Token
    where_clause: Any = None  # optional


@dataclass
class Selectable(Symbol):
    selectables: List


@dataclass
class WhereClause(Symbol):
    and_clauses: List[AndClause]


@dataclass
class AndClause(Symbol):
    predicates: List[Predicate]


@dataclass
class Predicate(Symbol):
    # note: this does not support unary function;
    # will likely need to update when predicates can be functions
    first: Term
    op: Token
    second: Term


@dataclass
class Term(Symbol):
    value: Any
