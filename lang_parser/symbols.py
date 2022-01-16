from __future__ import annotations
"""
Contains symbol classes used by parser
"""
from enum import Enum, auto

from typing import Any, List, Optional, Union
from dataclasses import dataclass
from .tokens import Token
from .visitor import  Visitor


class JoinType(Enum):
    Inner = auto()
    LeftOuter = auto()
    RightOuter = auto()
    FullOuter = auto()
    Cross = auto()


@dataclass
class Symbol:
    """
    Symbol is the root of parser hierarchy.
    Comparable to how tokens compose the tokenizer's output, i.e. a stream of tokens,
    Symbols compose the parser's output, i.e. the AST
    """
    def accept(self, visitor: Visitor) -> Any:
        return visitor.visit(self)


@dataclass
class Program(Symbol):
    statements: List[Union[SelectExpr, CreateStmnt, InsertStmnt, DeleteStmnt, DropStmnt, TruncateStmnt]]


@dataclass
class CreateStmnt(Symbol):
    table_name: Token
    column_def_list: List[ColumnDef]


@dataclass
class InsertStmnt(Symbol):
    table_name: Token
    column_name_list: List
    value_list: List


@dataclass
class SelectExpr(Symbol):
    """
    NOTE: Expr produce one or more value(s) but do not change the system;
    Stmnt change the state of the system; and may or may not
    return value
    """
    selectable: Selectable
    from_location: AliasableSource
    where_clause: WhereClause = None
    group_by_clause: Any = None
    having_clause: Any = None
    order_by_clause: Any = None
    limit_clause: Any = None


@dataclass
class Joining(Symbol):
    """
    Represents a join between two data sources
    """
    left_source: AliasableSource
    right_source: AliasableSource
    join_type: JoinType
    on_clause: OnClause = None


@dataclass
class AliasableSource(Symbol):
    """
    Represents a source of data that can have an alias
    This could be:
     - a single table,
     - a single view,
     - two or more joined objects
     - subquery
    """
    # todo: rename to source
    # the union refers to the fact that source can be a single source
    # or a joined source
    source_name: Union[Token, Joining]
    alias_name: Optional[Token] = None


@dataclass
class DeleteStmnt(Symbol):
    table_name: Token
    where_clause: Any = None


@dataclass
class DropStmnt(Symbol):
    table_name: Token


@dataclass
class TruncateStmnt(Symbol):
    table_name: Token


@dataclass
class UpdateStmnt(Symbol):
    table_name: Token
    column_name: Token
    value: Token
    where_clause: Any = None


@dataclass
class ColumnDef(Symbol):
    column_name: Token
    datatype: Token
    is_primary_key: bool = False
    is_nullable: bool = False


@dataclass
class Selectable(Symbol):
    selectables: List


@dataclass
class WhereClause(Symbol):
    # where cond is a single disjunction (or) of many conjunctions (and)
    or_clause: List[AndClause]


@dataclass
class OnClause(Symbol):
    # identical structure to `WhereClause`
    or_clause: List[AndClause]


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
