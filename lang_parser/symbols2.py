"""
symbols.py contains Ast classes that are passed to lark's
Ast generation utils which replace a generic tree class with named
children. This module contains other Ast classes,
e.g. that must be instantiated manually, enums etc.
"""
import abc
from typing import Tuple, List
from lark import Lark, Transformer, Tree, v_args
from .symbols import _Symbol, SingleSource, UnconditionedJoin, JoinType, ColumnQualifier


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
    def _join_modifier_to_type(join_modifier=None) -> JoinType:
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


class ColumnDef(_Symbol):
    # column_name datatype primary_key? not_null?

    def __init__(self, column_name = None, datatype=None, primary_key=None, not_null=None):
        self.column_name = column_name
        self.datatype = datatype
        self.is_primary_key = primary_key is not None
        self.is_not_null = not_null is not None

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
    """

    def rules_to_kwargs(self, args) -> dict:
        kwargs = {arg.data: arg for arg in args}
        return kwargs

    def conditioned_join(self, tree):
        """
        """
        params = self.rules_to_kwargs(tree.children)
        return ConditionedJoin(**params)

    def column_def(self, tree):
        params = self.rules_to_kwargs(tree.children)
        return ColumnDef(**params)

