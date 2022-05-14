from __future__ import annotations
import logging

from lark import Lark, ast_utils, Tree, Token
from lark.exceptions import UnexpectedInput  # root of all lark exceptions

from . import symbols
from .symbols import _Symbol
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

    def remove_tree_wrapper(self, root):
        """
        Given a parsed program; remove any encapsulating tree objects
        Traverse the tree, and replace any Tree types (Lark's internal type)
        with the contained child
        """
        stack = [root]
        while stack:
            node = stack.pop()
            if isinstance(node, Token):
                # this should be caught before being added
                continue

            # unwrap all property
            for prop_name in dir(node):
                if prop_name.startswith("_"):
                    continue  # ignore

                prop = getattr(node, prop_name)
                logger.info(f'node[{type(node)}]{node},  prop[{type(prop)}]{prop}')
                if callable(prop) or prop is None or isinstance(prop, Token): # skip
                    continue

                if isinstance(prop, list):
                    for idx in range(len(prop)):
                        item = prop[idx]
                        if isinstance(item, Token):
                            continue
                        if isinstance(item, Tree):
                            # unwrap tree
                            assert len(prop.children) == 1
                            unwrapped = prop.children[0]
                            prop[idx] = unwrapped
                            # recurse down
                            stack.append(unwrapped)
                        else:
                            # recurse down
                            stack.append(item)
                else:
                    if isinstance(prop, Tree):
                        # unwrap tree
                        assert len(prop.children) == 1
                        unwrapped = prop.children[0]
                        setattr(node, prop_name, unwrapped)
                        # recurse down
                        stack.append(unwrapped)
                    else:
                        # recurse down
                        stack.append(prop)

        # check node's children
        return root

    def remove_tree_wrapper2(self):
        """
        Remove any tree wrapper nodes; only recurse down know types, e.g.
        Tree, and defined AST types
        """
        if isinstance(obj, _Symbol)


    def parse(self, text: str):
        """

        :param text:
        :return:
        """
        # parse tree
        try:
            #print(self.parser.parse(text).pretty())
            # return
            #print("$" * 100)
             # Ast
            tree = self.parser.parse(text)
            transformer = ast_utils.create_transformer(symbols, ToAst())
            tree = transformer.transform(tree)
            print(tree)
            #breakpoint()
            tree = self.remove_tree_wrapper(tree)
            print(tree)
            #breakpoint()

            #pretty = tree.prettyprint()
            #pretty = os.linesep.join(pretty)
            #print("$"*100)

            #print("$." * 70)
            #print(pretty)
            #print("$" * 100)
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


