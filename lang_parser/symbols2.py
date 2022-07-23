"""
symbols.py contains Ast classes that are passed to lark's
Ast generation utils which replace a generic tree class with named
children. This module contains other Ast classes,
e.g. that must be instantiated manually, enums etc.
"""
import abc
from typing import Tuple, List
from lark import Lark, Transformer, Tree, v_args
from .symbols import (
    unwrap_tree_atom,
    unwrap_tree_list,
    _Symbol,
    DataType,
    SingleSource,
    UnconditionedJoin,
    JoinType
)


class Joining(abc.ABC):
    pass


class ConditionedJoin(_Symbol):
    """
    AST classes are responsible for translating parse tree matched rules
    to more intuitive properties, that the VM can operate on.
    Additionally, they should enforce any local constraints, e.g. 1 primary key
    """

    def __init__(self, source=None, join_modifier=None, single_source=None, condition=None):
        self.left_source = source
        self.right_source = single_source
        self.condition = condition
        self.join_type = self._join_modifier_to_type(join_modifier)

    @staticmethod
    def _join_modifier_to_type(join_modifier) -> JoinType:
        if join_modifier is None:
            return JoinType.Inner
        modifier = join_modifier.children[0].data  # not sure why it's a list
        modifier = modifier.lower()
        if modifier == "left_outer":
            return JoinType.LeftOuter
        elif modifier == "right_outer":
            return JoinType.RightOuter
        else:
            assert modifier == "full_outer"
            return JoinType.FullOuter


class ColumnDef2(_Symbol):

    def __init__(self, column_name: Tree = None, datatype: Tree = None, primary_key: Tree = None, not_null: Tree = None):
        self.column_name = unwrap_tree_atom(column_name)
        self.datatype = self._datatype_to_type(unwrap_tree_atom(datatype))  # remove unwrap method
        self.is_primary_key = primary_key is not None
        self.is_nullable = not_null is None

    @staticmethod
    def _datatype_to_type(datatype: str):
        datatype = datatype.lower()
        if datatype == "integer":
            return DataType.Integer
        elif datatype == "real":
            return DataType.Real
        elif datatype == "text":
            return DataType.Text
        elif datatype == "blob":
            return DataType.Blob
        else:
            raise ValueError(f"Unrecognized datatype [{datatype}]")

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.prettystr()
        #return f'{self.__class__.__name__}({self.__dict__})'


# makes (Un)ConditionedJoin a subclass of Joining
Joining.register(ConditionedJoin)
Joining.register(UnconditionedJoin)


@v_args(tree=True)
class ToAst(Transformer):
    """
    todo: this class should convert literals to datatype

    When creating an AST, for rules which have optional tokens
    I defer creating Ast types, so that a specific method
    in the transformer (this) can specify which rules map to
    which constructor params.

    Any rule that has optional token's in the body (not at tail)
    will need to be deferred constructed.

    These methods should also return the converted Ast class

    NOTE: if an Ast class is defined in symbols mod; that will have take precedence over
    transformer.<handler>
    """

    def rules_to_kwargs(self, args) -> dict:
        """helper to convert rule names to kw args"""
        kwargs = {arg.data: arg for arg in args}
        return kwargs

    def conditioned_join(self, tree):
        """
        """
        params = self.rules_to_kwargs(tree.children)
        return ConditionedJoin(**params)

    def column_def(self, tree):
        params = self.rules_to_kwargs(tree.children)
        return ColumnDef2(**params)


class CreateStmnt(_Symbol):
    def __init__(self, table_name: Tree = None, column_def_list: Tree = None):
        self.table_name = unwrap_tree_atom(table_name)
        self.columns = unwrap_tree_list(column_def_list)  # todo: this unwrapping may be unnecessary
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
        return f'{self.__class__.__name__}({self.__dict__})'


class SecondTransformer(Transformer):
    """
    Handle conversion of all parse rule/tokens to AST classes.
    Note, this must handle where optional args are only at tail (normal)
    and  when they're in the body, via rule_name to kw mapping
    """
    def create_stmnt(self, args):
        pass
        assert len(args) == 2
        assert len(args[0].children) == 1
        table_name = args[0].children[0]

    # kw constructors

    def rules_to_kwargs(self, args) -> dict:
        """helper to convert rule names to kw args"""
        kwargs = {arg.data: arg for arg in args}
        return kwargs

    def conditioned_join(self, tree):
        """
        """
        params = self.rules_to_kwargs(tree.children)
        return ConditionedJoin(**params)

    def column_def(self, tree):
        params = self.rules_to_kwargs(tree.children)
        return ColumnDef2(**params)



