from __future__ import annotations
import os
import logging

from lark import Lark, ast_utils
from lark.exceptions import UnexpectedInput  # root of all lark exceptions
from typing import List, Tuple

from . import symbols
from .symbols2 import ToAst
from .grammar import GRAMMAR


logger = logging.getLogger(__name__)



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


