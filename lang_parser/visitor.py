from utils import camel_to_snake


class HandlerNotFoundException(Exception):
    """
    A specific handler (method) is not found;
    defined by me
    """
    pass


class Visitor:
    """
    Conceptually, Visitor is an interface/abstract class,
    where different concrete Visitors, e.g. AstPrinter can handle
    different tasks, e.g. printing tree, evaluating tree.
    This indirection allows us to add new behaviors for the parser
    via a new concrete class; instead of either: 1) modifying
    the parser symbol classes (OOF), or 2) adding a new function
    for any new behavior (e.g. functional)

    See following for visitor design pattern in python:
     https://refactoring.guru/design-patterns/visitor/python/example
    """

    def visit(self, symbol: 'Symbol'):
        """
        this will determine which specific handler to invoke; dispatch
        """
        suffix = camel_to_snake(symbol.__class__.__name__)
        # determine the name of the handler method from class of expr
        # NB: this requires the class and handler have the
        # same name in PascalCase and snake_case, respectively
        handler = f'visit_{suffix}'
        if hasattr(self, handler):
            return getattr(self, handler)(symbol)
        else:
            print(f"Visitor does not have {handler}")
            raise HandlerNotFoundException()
