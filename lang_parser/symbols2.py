"""
symbols.py contains Ast classes that are passed to lark's
Ast generation utils which replace a generic tree class with named
children. This module contains other Ast classes,
e.g. that must be instantiated manually, enums and other lo
"""
import abc
from typing import Tuple, List
from lark import Lark, Transformer, Tree, v_args
from .symbols import _Symbol, SingleSource, UnconditionedJoin, JoinType, ColumnQualifier


class Field:
    """
    Defines a field in a Ast symbol class.
    This allows us to mark which fields are optional.
    """
    def __init__(self, name, optional=False, ast_types: Tuple = None, rule_name=None):
        self.name = name
        self.optional = optional
        # one and only one of the following 2 should be set
        self.ast_types = ast_types  # should be list of valid ast_types
        self.rule_name = rule_name  # name of
        assert ast_types is None or isinstance(ast_types, tuple)
        assert rule_name is None or isinstance(rule_name, str)


class Joining(abc.ABC):
    pass


class ConditionedJoin(_Symbol):

    def __init__(self, source, join_modifier, other_source, condition):
        self.source = source
        self.other_source = other_source
        self.condition = condition
        self.join_type = ConditionedJoin.join_modifier_to_type(join_modifier)

    @staticmethod
    def join_modifier_to_type(join_modifier) -> JoinType:
        # TODO: complete me
        return JoinType.Inner


ConditionedJoinFields = [
            Field(name="source", optional=False, ast_types=(SingleSource, Joining)),
            Field(name="join_modifier", optional=True, rule_name="join_modifier"),
            Field(name="other_source", optional=False, ast_types=(SingleSource,)),
            Field(name="condition", optional=True, rule_name="condition")
]


class ColumnDef(_Symbol):
    # column_name datatype primary_key? not_null?

    def __init__(self, column_name, datatype, is_primary_key, is_not_null):
        self.column_name = column_name
        self.datatype = datatype
        self.is_primary_key = is_primary_key
        self.is_not_null = is_not_null

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.prettystr()
        #return f'{self.__class__.__name__}({self.__dict__})'


ColumnDefFields = [
            Field(name="column_name", optional=False, rule_name="column_name"),
            Field(name="datatype", optional=False, rule_name="datatype"),
            Field(name="primary_key", optional=True, ast_types=(bool,)),
            Field(name="not_null", optional=True, ast_types=(bool,)),
]


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

    def resolve_params(self, fields: List, args):
        """
        This should resolve, i.e. map either an arg or
        None to each field.
        NOTE: count(fields) >= len(args).

        This resolution will not resolve correctly, e.g.
        rule : token? token

        TOFIX: This seems to return None when there is no match

        """
        resolved = [None] * len(fields)
        argptr = 0
        for i, field in enumerate(fields):
            if argptr >= len(args):
                break
            arg = args[argptr]
            # handle rule
            if field.rule_name is not None:
                # check whether this rule matches the arg
                matched = False
                if isinstance(arg, Tree) and field.rule_name == arg.data:
                    resolved[i] = arg
                    argptr += 1
                    matched = True
            # handle ast type
            else:
                assert field.ast_types is not None, "expected not None field.ast_types"
                matched = False
                if isinstance(arg, field.ast_types):
                    resolved[i] = arg
                    argptr += 1
                    matched = True

            if not matched and not field.optional:
                # likely, the fields are mis-configured
                raise ValueError(f"Expected rule [{field.rule_name}], but received [{arg.data}]")

        return resolved

    def conditioned_join(self, tree):
        """
        """
        args = tree.children
        # resolve children
        params = self.resolve_params(ConditionedJoinFields, args)
        # construct and return ast
        return ConditionedJoin(*params)

    def column_def(self, tree):
        params = self.resolve_params(ColumnDefFields, tree.children)
        return ColumnDef(*params)

    def not_null(self, tree):
        #return ColumnQualifier.NotNull
        return True

    def primary_key(self, tree):
        #return ColumnQualifier.PrimaryKey
        return True


