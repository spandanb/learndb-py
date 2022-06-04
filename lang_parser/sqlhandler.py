from __future__ import annotations
import logging

from lark import Lark, ast_utils, Tree, Token
from lark.exceptions import UnexpectedInput  # root of all lark exceptions

from . import symbols
from .symbols import _Symbol
from .symbols2 import ToAst, SecondTransformer
from .symbols3 import ToAst2, ToAst3
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

    def remove_tree_wrapper3(self, root):
        """
        Remove any tree wrapper nodes; only recurse down know types, e.g.
        Tree, and defined AST types.

        NOTE: This requires all meaningful classes have an AST classes, and no valuable
        info be stored in Lark.Tree nodes, as those will be pruned by this method

        """
        stack = [root]
        # I'm using a set, but this is likely due to a bug in my traversal
        # because otherwise it gets stuck in an inf loop
        seen = set()
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
                if callable(prop) or prop is None or isinstance(prop, Token):  # skip
                    continue

                if isinstance(prop, list):
                    for idx in range(len(prop)):
                        item = prop[idx]
                        if isinstance(item, Tree):
                            # unwrap single-child tree
                            if len(item.children) == 1:
                                unwrapped = item.children[0]
                                prop[idx] = unwrapped
                            else:
                                # attach all children
                                # prop[idx] = item.children
                                # todo: how to handle this cases?
                                # breakpoint()
                                raise ValueError(f"node {node} prop {prop.data} has children: [num: {len(prop.children)}], {prop.children}")

                        # check and recurse
                        item = prop[idx]
                        if not callable(item) and item is not None and not isinstance(item, Token):
                            if id(item) not in seen:
                                seen.add(id(item))
                                stack.append(item)

                else:
                    if isinstance(prop, Tree):
                        # unwrap tree
                        if len(prop.children) == 1:
                            unwrapped = prop.children[0]  # unwrap single
                            setattr(node, prop_name, unwrapped)
                        else:
                            # raise ValueError(f"node {node} prop {prop.data} has children: [num: {len(prop.children)}], {prop.children}")
                            # breakpoint()
                            # is this correct?
                            setattr(node, prop_name, prop.children)  # attach as list

                    prop = getattr(node, prop_name)
                    if not isinstance(prop, list):
                        if not callable(prop) and prop is not None and not isinstance(prop, Token):
                            if id(prop) not in seen:
                                stack.append(prop)
                                seen.add(id(prop))
                    else:
                        for item in prop:
                            if not callable(item) and item is not None and not isinstance(item, Token):
                                if id(item) not in seen:
                                    stack.append(item)
                                    seen.add(id(item))


        # check node's children
        return root


    def remove_tree_wrapper4(self, root):
        """
        Remove any tree wrapper nodes; only recurse down know types, e.g.
        Tree, and defined AST types.

        NOTE: This requires all meaningful classes have an AST classes, and no valuable
        info be stored in Lark.Tree nodes, as those will be pruned by this method

        """
        propcount = 0
        stack = [root]
        # I'm using a set, but this is likely due to a bug in my traversal
        # because otherwise it gets stuck in an inf loop; actually maybe
        # objects are referenced by many different refs
        seen = set()
        while stack:
            node = stack.pop()
            if isinstance(node, Token) or callable(node) or node is None or isinstance(node, int) \
                    or isinstance(node, float) or isinstance(node, str):
                continue

            # unwrap all properties
            for prop_name in dir(node):
                if prop_name.startswith("_"):
                    continue  # ignore

                # unwrap
                prop = getattr(node, prop_name)
                if isinstance(prop, list):
                    for idx in range(len(prop)):
                        item = prop[idx]
                        if isinstance(item, Tree):
                            # unwrap single-child tree
                            if len(item.children) == 1:
                                prop[idx] = item.children[0]
                            else:
                                prop[idx] = [child for child in item.children]

                elif isinstance(prop, Tree):
                    # unwrap tree
                    if len(prop.children) == 1:
                        # unwrap single
                        setattr(node, prop_name, prop.children[0])
                    else:
                        setattr(node, prop_name, prop.children)  # attach as list

                # check and recurse
                prop = getattr(node, prop_name)
                if isinstance(prop, list):
                    for item in prop:
                        if id(item) not in seen:
                            stack.append(item)
                            seen.add(id(item))
                else:
                    if id(prop) not in seen:
                        stack.append(prop)
                        seen.add(id(prop))

        # check node's children
        return root

    def unwrap(self, node):
        """
        recursively impl of remove_tree_wrapper4
        """
        if isinstance(node, Token) or isinstance(node, int) or isinstance(node, float) or isinstance(node, str) or\
                node is None:
            return node

        if isinstance(node, Tree):
            if len(node.children) == 0:
                node = node.children[0]
            else:
                node = node.children
            return self.unwrap(node)
        if isinstance(node, list):
            for idx in range(len(node)):
                item = node[idx]
                node[idx] = self.unwrap(item)
        else:
            for prop_name in dir(node):
                if prop_name.startswith("_"):
                    continue
                prop = getattr(node, prop_name)
                try:
                    setattr(node, prop_name, self.unwrap(prop))
                except Exception:
                    # some attrs aren't settable
                    pass
        #if isinstance(node, list) and len(node) == 1:
        #    return node[0]
        return node

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
            text = "select cola, 9 from catalog where cond != true"

            print(f"parsing text [note manual overide]: {text}")
            tree = self.parser.parse(text)
            # first transformation
            #transformer = ast_utils.create_transformer(symbols, ToAst())
            #tree = transformer.transform(tree)
            #print(tree)

            # second transformation
            # attempt this without, create_transformerxx
            # second = SecondTransformer()
            # tree = second.transform(tree)
            #breakpoint()
            #tree = self.remove_tree_wrapper4(tree)
            #tree = self.unwrap(tree)

            print(tree)

            transformer = ToAst3()
            tree = transformer.transform(tree)
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


