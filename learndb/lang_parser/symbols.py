from __future__ import annotations

import abc

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum, auto
from lark import Transformer, Tree, ast_utils, Token
from typing import Any, List, Union, Optional, Type, Tuple


from .visitor import Visitor


# constants

WHITESPACE = " "

# enum types


class JoinType(Enum):
    Inner = auto()
    LeftOuter = auto()
    RightOuter = auto()
    FullOuter = auto()
    Cross = auto()


class ColumnModifier(Enum):
    PrimaryKey = auto()
    NotNull = auto()
    Nil = auto()  # no modifier - likely not needed


class OrderingQualifier(Enum):
    Ascending = auto()
    Descending = auto()


class SymbolicDataType(Enum):
    """
    Enums for system datatypes
    NOTE: This represents data-types as understood by the parser; hence "Symbolic" suffix.
    There is 1-1 correspondence to VM's notions of datatypes, which are datatypes we can
    do algebra atop.
    """

    Integer = auto()
    Text = auto()
    Real = auto()
    Blob = auto()
    Boolean = auto()


class ComparisonOp(Enum):
    Greater = auto()
    Less = auto()
    LessEqual = auto()
    GreaterEqual = auto()
    Equal = auto()
    NotEqual = auto()


class ArithmeticOp(Enum):
    Addition = auto()
    Subtraction = auto()
    Multiplication = auto()
    Division = auto()


# symbol class


class Symbol(ast_utils.Ast):
    """
    The root of AST hierarchy
    """

    def accept(self, visitor: Visitor) -> Any:
        return visitor.visit(self)

    def __hash__(self):
        return self.__class__.__name__ + str(self.__dict__)

    def find_descendents(
        self, descendent_type: Union[Type[Symbol], Tuple[Type[Symbol]]]
    ) -> List:
        """
        Search through all descendents via BFS
        and return list of matches.

        :param descendent_type: this can be single type or a tuple of types
        """
        matches = []

        queue = deque()
        queue.append(self)
        while queue:
            node = queue.popleft()
            if isinstance(node, descendent_type):
                matches.append(node)
            # iterate over children
            for attr_name in dir(node):
                attr = getattr(node, attr_name)
                if isinstance(attr, Symbol):
                    queue.append(attr)
                if isinstance(attr, Iterable):
                    for element in attr:
                        if isinstance(element, Symbol):
                            queue.append(element)

        return matches


# create statement


class CreateStmnt(Symbol):
    def __init__(self, table_name: Tree = None, column_def_list: Tree = None):
        self.table_name = table_name
        self.columns = column_def_list
        self.validate()

    def validate(self):
        """
        Ensure one and only one primary key
        """
        pkey_count = len([col for col in self.columns if col.is_primary_key])
        if pkey_count != 1:
            raise ValueError(f"Expected 1 primary key received {pkey_count}")

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"{self.__class__.__name__}({self.__dict__})"


@dataclass
class DropStmnt(Symbol):
    table_name: TableName


# create statement helpers


class ColumnDef(Symbol):
    def __init__(
        self,
        column_name: Tree = None,
        datatype: Tree = None,
        column_modifier=ColumnModifier.Nil,
    ):
        self.column_name = column_name
        self.datatype = datatype
        self.is_primary_key = column_modifier == ColumnModifier.PrimaryKey
        self.is_nullable = (
            column_modifier != ColumnModifier.NotNull
            and column_modifier != ColumnModifier.PrimaryKey
        )

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"Column(name: {self.column_name}, datatype: {self.datatype}, pkey: {self.is_primary_key}, nullable: {self.is_nullable})"


@dataclass
class Comparison(Symbol):
    left_op: Any
    right_op: Any
    operator: Any


# select stmnt
@dataclass
class SelectStmnt(Symbol):
    select_clause: Any
    # all other clauses depend on from clause and hence are
    # nested under for_clause; this implicitly means that a select
    # clause can only return a scalar; since if it could return a table
    # we would want to support a condition, and grouping on it; i.e.
    # where, and group by, .. clauses without a from a clause.
    from_clause: Any = None


# select stmnt helpers


@dataclass
class SelectClause(Symbol):
    selectables: List[Any]


class FromClause(Symbol):
    def __init__(
        self,
        source,
        where_clause=None,
        group_by_clause=None,
        having_clause=None,
        order_by_clause=None,
        limit_clause=None,
    ):
        self.source = source
        # where clause can only be defined if a from clause is defined
        self.where_clause = where_clause
        self.group_by_clause = group_by_clause
        self.having_clause = having_clause
        self.order_by_clause = order_by_clause
        self.limit_clause = limit_clause


@dataclass
class SingleSource(Symbol):
    table_name: TableName
    table_alias: Any = None


# wrap around from source
@dataclass
class FromSource(Symbol):
    source: Any


class UnconditionedJoin(Symbol):
    def __init__(self, left_source, right_source):
        self.left_source = left_source
        self.right_source = right_source
        self.join_type = JoinType.Cross


class ConditionedJoin(Symbol):
    """
    AST classes are responsible for translating parse tree matched rules
    to more intuitive properties, that the VM can operate on.
    Additionally, they should enforce any local constraints, e.g. 1 primary key
    """

    def __init__(self, left_source, right_source, condition, join_modifier=None):
        self.left_source = left_source
        self.right_source = right_source
        self.condition = condition
        self.join_type = self._join_modifier_to_type(join_modifier)

    @staticmethod
    def _join_modifier_to_type(join_modifier) -> JoinType:
        # TODO: THIS CAN BE DONE IN THE TRANSFORMER
        if join_modifier is None:
            return JoinType.Inner
        modifier = join_modifier.children[0].data  # not sure why it's a list
        modifier = modifier.lower()
        if modifier == "inner":
            return JoinType.Inner
        elif modifier == "left_outer":
            return JoinType.LeftOuter
        elif modifier == "right_outer":
            return JoinType.RightOuter
        else:
            assert modifier == "full_outer"
            return JoinType.FullOuter


class Joining(abc.ABC):
    pass


# makes (Un)ConditionedJoin a subclass of Joining
Joining.register(ConditionedJoin)
Joining.register(UnconditionedJoin)


@dataclass
class WhereClause(Symbol):
    condition: Any  # OrClause


# root of expr hierarchy
@dataclass
class Expr(Symbol):
    expr: Any


@dataclass
class OrClause(Symbol):
    and_clauses: Any

    def append_and_clause(self, and_clause):
        self.and_clauses.append(and_clause)


@dataclass
class AndClause(Symbol):
    predicates: List[Any]

    def append_predicate(self, predicate):
        self.predicates.append(predicate)


@dataclass
class GroupByClause(Symbol):
    columns: List[Any]


@dataclass
class HavingClause(Symbol):
    condition: Any


@dataclass
class OrderByClause(Symbol):
    columns: List[OrderedColumn]


@dataclass
class OrderedColumn(Symbol):
    column: ColumnName
    qualifier: OrderingQualifier


@dataclass
class LimitClause(Symbol):
    limit: int
    offset: Any = None


@dataclass
class InsertStmnt(Symbol):
    table_name: Any
    column_name_list: ColumnNameList
    value_list: ValueList


@dataclass
class ColumnNameList(Symbol):
    names: List[ColumnName]


@dataclass
class ValueList(Symbol):
    values: List[Any]


@dataclass
class DeleteStmnt(Symbol):
    table_name: Any
    where_condition: Any = None


@dataclass
class Program(Symbol):
    statements: list


@dataclass
class TableName(Symbol):
    table_name: str

    def __hash__(self):
        return hash(self.table_name)

    def __eq__(self, other):
        return hasattr(other, "table_name") and self.table_name == other.table_name


@dataclass
class ColumnName(Symbol):
    """
    Represents a column named like: 1) tbl.cola or 2) cola
    """

    name: Any

    def get_parent_alias(self) -> Optional[str]:
        """
        Return parent alias, if exists,
        e.g. for name "tbl.cola", this method would return tbl
        for name "cola", this method would return None
        """

        parts = self.name.split(".")
        if len(parts) > 1:
            return ".".join(parts[:-1])
        return None

    def get_base_name(self) -> str:
        """ """
        return self.name.split(".")[-1]


@dataclass
class FuncCall(Symbol):
    name: str
    args: List


@dataclass
class Literal(Symbol):
    value: Any
    type: SymbolicDataType


@dataclass
class BinaryArithmeticOperation(Symbol):
    operator: ArithmeticOp
    operand1: Any
    operand2: Any


class ToAst(Transformer):
    """
    Convert parse tree to AST.

    Design Note: Most symbols in the grammar get their own specific Symbol subclass.
    But in some cases, objects of child class (or a list thereof) is returned by
    the parent handler. The specific decision is driven by trying the return the easiest to
    interpret AST, from the VMs perspective. Further, some symbols in the grammar are needed
    to ensure when the input sql is parsed, it is done according to implicit precedence rules.
    However, when constructing the AST, we can discard these pseudo-classes.

    NOTE: methods are organized logically by statement types
    """

    # helpers

    # simple classes - top level statements

    @staticmethod
    def program(args) -> Program:
        return Program(args)

    @staticmethod
    def create_stmnt(args) -> CreateStmnt:
        return CreateStmnt(args[0], args[1])

    @staticmethod
    def drop_stmnt(args) -> DropStmnt:
        return DropStmnt(args[0])

    @staticmethod
    def select_stmnt(args) -> SelectStmnt:
        """select_clause from_clause? group_by_clause? having_clause? order_by_clause? limit_clause?"""
        # this return a logically valid structure,
        # i.e. select is always needed, but where, group by, and having require a from clause
        # and hence are nested under from clause
        return SelectStmnt(*args)

    @staticmethod
    def insert_stmnt(args) -> InsertStmnt:
        return InsertStmnt(*args)

    @staticmethod
    def delete_stmnt(args) -> DeleteStmnt:
        return DeleteStmnt(*args)

    # select stmnt components

    @staticmethod
    def select_clause(args) -> SelectClause:
        return SelectClause(args)

    def from_clause(self, args) -> FromClause:
        # setup iteration over args
        args_iter = iter(args)
        count = len(args)
        assert count >= 1

        arg = next(args_iter)
        count -= 1
        # assert isinstance(arg, FromSource)
        assert isinstance(arg, SingleSource) or isinstance(arg, Joining)

        # unwrap any nested FromSource
        fclause = FromClause(arg)
        fclause.source = FromSource(fclause.source)

        if count == 0:
            return fclause

        while count > 0:
            arg = next(args_iter)
            count -= 1
            if isinstance(arg, WhereClause):
                fclause.where_clause = arg
            elif isinstance(arg, GroupByClause):
                fclause.group_by_clause = arg
            elif isinstance(arg, HavingClause):
                fclause.having_clause = arg
            elif isinstance(arg, LimitClause):
                fclause.limit_clause = arg
            elif isinstance(arg, OrderByClause):
                fclause.order_by_clause = arg

        return fclause

    def table_alias(self, args):
        assert len(args) == 1
        return args[0]

    def where_clause(self, args):
        assert len(args) == 1
        return WhereClause(args[0])

    def group_by_clause(self, args):
        return GroupByClause(args)

    def having_clause(self, args):
        assert len(args) == 1
        return HavingClause(args[0])

    def order_by_clause(self, args):
        # assume default ordering: asc
        # args is a list that starts with column_name
        # the next arg coud
        return OrderByClause(args)

    def ordered_column(self, args):
        if len(args) == 1:
            # default ascending order
            return OrderedColumn(args[0], OrderingQualifier.Ascending)
        else:
            assert len(args) == 2
            return OrderedColumn(args[0], args[1])

    def limit_clause(self, args):
        if len(args) == 1:
            return LimitClause(args[0])
        else:
            assert len(args) == 2
            return LimitClause(*args)

    def source(self, args):
        assert len(args) == 1
        # return FromSource(args[0])
        return args[0]

    def single_source(self, args):
        assert len(args) <= 2
        name = args[0]
        alias = args[1] if len(args) > 1 else None
        return SingleSource(name, alias)

    def joining(self, args):
        breakpoint()
        raise NotImplementedError

    def conditioned_join(self, args):
        if len(args) == 3:
            return ConditionedJoin(*args)
        else:
            assert len(args) == 4
            return ConditionedJoin(args[0], args[2], args[3], join_modifier=args[1])

    def unconditioned_join(self, args):
        assert len(args) == 2
        return UnconditionedJoin(args[0], args[1])

    def expr(self, args) -> Expr:
        assert len(args) == 1
        return Expr(args[0])

    def condition(self, args):
        if len(args) == 1:
            # unwrap
            return args[0]
        return args

    @staticmethod
    def simplify_or_clause(or_clause: OrClause):
        """
        Utility method to simplify `or_clause`. Simplify means that if `or_clause`
        contains only a single primitive (literal or reference), i.e. without any logical
        or arithmetic operations, then return the primitive; else return the entire or_clause
        # TODO: move
        """
        primitive_types = (Literal, ColumnName, FuncCall)
        descendents = or_clause.find_descendents(primitive_types)
        if len(descendents) == 1:
            # only a single primitive- unwrap
            return descendents[0]
        else:
            # some complex operation
            return or_clause

    def comparison(self, args):
        """
        NOTE: Many rules follow this pattern where there are 2 cases;
        1) if len(args) == 1, we unwrap
        and 2) if there are more args, we wrap in the appropriate object.

        This is because the rule is like:
        condition -> term
                    | comparison ( LESS_EQUAL | GREATER_EQUAL | LESS | GREATER ) term

        Case 1) corresponds to the `term`, while the second case, we want to wrap in container object
        """
        if len(args) == 1:
            return args[0]
        assert len(args) == 3
        return Comparison(left_op=args[0], right_op=args[2], operator=args[1])

    def predicate(self, args):
        """
        NOTE: predicate and comparison handle comparison, but different ops
        to better handle precedence
        """
        if len(args) == 1:
            return args[0]
        assert len(args) == 3
        return Comparison(left_op=args[0], right_op=args[2], operator=args[1])

    def term(self, args):
        if len(args) == 1:
            return args[0]
        if len(args) == 3:
            val = BinaryArithmeticOperation(args[1], args[0], args[2])
            return val
        return args

    def factor(self, args):
        if len(args) == 1:
            return args[0]
        if len(args) == 3:
            val = BinaryArithmeticOperation(args[1], args[0], args[2])
            return val
        return args

    def unary(self, args):
        if len(args) == 1:
            return args[0]
        return args

    def selectable(self, args):
        if len(args) == 1:
            return args[0]
        else:
            raise ValueError("Unexpected arity")

    def or_clause(self, args) -> OrClause:
        if len(args) == 1:
            return args[0]
        else:
            assert len(args) == 2
            ret = OrClause(args)
            return ret

    def and_clause(self, args):
        if len(args) == 1:
            return args[0]
        else:
            assert len(args) == 2
            if isinstance(args[0], Comparison):
                # 1. first time we visit this, both args will be `Comparison` objects
                assert isinstance(args[1], Comparison)
                return AndClause(args)
            else:
                assert isinstance(args[0], AndClause)
                assert isinstance(args[1], Comparison)
                # 2. but subsequent reductions will have args[0] be an AndClause
                # any other `Condition`s will be attached to this AndClause
                # NOTE: the parse tree encodes this precedence information via this
                # nesting; but this is not needed explicitly, rather predicates in the
                # AndClause will be evaluated left to right by the virtual machine
                and_clause = args[0]
                and_clause.append_predicate(args[1])
                return and_clause

    def primary(self, args):
        assert len(args) == 1
        return args[0]

    def literal(self, args):
        if len(args) == 1:
            return args[0]
        else:
            breakpoint()
            raise ValueError()

    # func calls - right now only used in select
    def func_name(self, args):
        return args[0]

    def func_arg_list(self, args):
        return args

    def func_call(self, args):
        return FuncCall(args[0], args[1])

    # create stmnt components

    def table_name(self, args: list):
        assert len(args) == 1
        return TableName(args[0])

    def column_def_list(self, args):
        return args

    def column_name(self, args):
        assert len(args) == 1
        val = args[0]
        return ColumnName(val)

    def datatype(self, args):
        """
        Convert datatype text to arg
        """
        datatype = args[0].lower()
        if datatype == "integer":
            return SymbolicDataType.Integer
        elif datatype == "real":
            return SymbolicDataType.Real
        elif datatype == "text":
            return SymbolicDataType.Text
        elif datatype == "blob":
            return SymbolicDataType.Blob
        else:
            raise ValueError(f"Unrecognized datatype [{datatype}]")

    def primary_key(self, _):
        # this rule doesn't have any children nodes
        return ColumnModifier.PrimaryKey

    def not_null(self, _):
        # this rule doesn't have any children nodes
        return ColumnModifier.NotNull

    def desc(self, _):
        return OrderingQualifier.Descending

    def asc(self, _):
        return OrderingQualifier.Ascending

    def column_def(self, args):
        """
        ?column_def       : column_name datatype primary_key? not_null?

        check with if, else conds
        """
        column_name = args[0]
        datatype = args[1]
        # any remaining args are column modifiers
        modifier = ColumnModifier.Nil
        if len(args) >= 3:
            # the logic here is that if the primary key modifier is used
            # not null is redudanct; and the parser ensures/requires primary
            # key mod must be specified before not null
            # todo: this more cleanly, e.g. primary key implies not null, uniqueness
            # modifiers could be a flag enum, which can be or'ed
            modifier = args[2]
        val = ColumnDef(column_name, datatype, modifier)
        return val

    # insert stmnt components

    @staticmethod
    def column_name_list(args):
        return ColumnNameList(args)

    @staticmethod
    def value_list(args):
        return ValueList(args)

    def INTEGER_NUMBER(self, arg: Token):
        return Literal(int(arg), SymbolicDataType.Integer)

    def REAL_NUMBER(self, arg: Token):
        return Literal(float(arg), SymbolicDataType.Real)

    # comparison ops

    def GREATER(self, arg):
        return ComparisonOp.Greater

    def LESS(self, arg):
        return ComparisonOp.Less

    def LESS_EQUAL(self, arg):
        return ComparisonOp.LessEqual

    def GREATER_EQUAL(self, arg):
        return ComparisonOp.GreaterEqual

    def EQUAL(self, arg):
        return ComparisonOp.Equal

    def NOT_EQUAL(self, arg):
        return ComparisonOp.NotEqual

    def STRING(self, arg):
        # remove quotes
        assert arg[0] == "'" == arg[-1] or arg[0] == '"' == arg[-1]
        unquoted = arg[1:-1]
        return Literal(unquoted, SymbolicDataType.Text)

    def MINUS(self, arg):
        return ArithmeticOp.Subtraction

    def PLUS(self, arg):
        return ArithmeticOp.Addition

    def SLASH(self, arg):
        return ArithmeticOp.Division

    def STAR(self, arg):
        return ArithmeticOp.Multiplication
