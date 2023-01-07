from __future__ import annotations
import logging

from lark import Lark, ast_utils, Tree, Token
from lark.exceptions import UnexpectedInput  # root of all lark exceptions

from .symbols import ToAst
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
        self.parser = Lark(GRAMMAR, parser='earley', start="program", debug=True)

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
            tree = self.parser.parse(text)
            logging.info("Outputing untransformed AST..........")
            print(tree.pretty())
            transformer = ToAst()
            tree = transformer.transform(tree)
            self.parsed = tree
            self.is_succ = True
            self.exc = None
        except UnexpectedInput as e:
            self.exc = e
            self.parsed = None
            self.is_succ = False
            if self.raise_exception:
                raise


