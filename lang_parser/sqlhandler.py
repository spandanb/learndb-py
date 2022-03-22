from __future__ import annotations
import abc
import os
import logging

from lark import Lark, Transformer, Tree, ast_utils, v_args
from lark.exceptions import UnexpectedInput  # root of all lark exceptions
from typing import List, Tuple

from . import symbols
from .symbols import _Symbol, SingleSource, UnconditionedJoin
from .grammar import GRAMMAR

"""
TODO: move the sqlhandler into a separate module (other than symbols.py)
"""


logger = logging.getLogger(__name__)


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

    # perhaps move this out- it's making printout of class look busy
    fields = [
            Field(name="source", optional=False, ast_types=(SingleSource, Joining)),
            Field(name="join_modifier", optional=True, rule_name="join_modifier"),
            Field(name="other_source", optional=False, ast_types=(SingleSource,)),
            Field(name="condition", optional=True, rule_name="condition")
    ]

    def __init__(self, source, join_modifier, other_source, condition):
        self.source = source
        self.join_modifier = join_modifier
        self.other_source = other_source
        self.condition = condition





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


    A tree class has props (children: List, data: Token, meta)
    I can use this to embed rule_name in derived class.

    These methods should also return the converted Ast class
    """

    def resolve_params(self, fields: List, args):
        """
        This should resolve, i.e. map either an arg or
        None to each field.
        NOTE: count(fields) >= len(args).

        This resolution will not resolve correctly, e.g.
        rule : token? token

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
        params = self.resolve_params(ConditionedJoin.fields, args)
        # construct and return ast
        return ConditionedJoin(*params)



class SqlFrontEnd:
    """
    Parser for learndb lang, based on lark definition
    """
    def __init__(self, raise_exception=False):
        self.parser = None
        self.parsed = None  # parsed AST
        self.exc = None  # exception
        self.is_succ = False
        self.raise_exception = raise_exception
        self._init()

    def _init(self):
        self.parser = Lark(GRAMMAR, parser='earley', start="program", debug=True)  # , ambiguity='explicit')

    def error_summary(self):
        if self.exc is not None:
            return str(self.exc)

    def is_success(self):
        """
        whether parse operation is success
        # TODO: this and other methods should raise if no parse
        :return:
        """
        return self.is_succ

    def get_parsed(self):
        return self.parsed

    def parse(self, text: str):
        """

        :param text:
        :return:
        """
        # parse tree
        try:
            print(self.parser.parse(text).pretty())
            # return

            # Ast
            tree = self.parser.parse(text)
            transformer = ast_utils.create_transformer(symbols, ToAst())
            tree = transformer.transform(tree)
            pretty = tree.prettyprint()
            pretty = os.linesep.join(pretty)
            print("$"*100)
            print(tree)
            print("$." * 70)
            print(pretty)
            print("$" * 100)
            # print(tree.children[0].select_clause.children[0].Selections)
            self.parsed = tree
            self.is_succ = True
            self.exc = None
        except UnexpectedInput as e:
            self.exc = e
            self.parsed = None
            self.is_succ = False
            if self.raise_exception:
                raise


