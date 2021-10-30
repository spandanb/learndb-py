from visitor import Visitor


class Interpreter(Visitor):
    """
    Visitor for interpreting/executing AST
    """
    def __init__(self, statements):
        self.statements = statements

    def interpret(self):
        pass