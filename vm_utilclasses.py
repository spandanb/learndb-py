"""
Collection of classes
"""
from enum import Enum, auto
from typing import Any
from lang_parser.visitor import Visitor
from lang_parser.symbols3 import (Symbol,
                                  OrClause,
                                  AndClause,
                                  ColumnName
                                  )


class NameRegistry:
    """
    This should be the interface for registering and resolving names.

    For now, this will mirror methods, exposed by the VM, to resolve names,
    so that this object can be passed instead of the VM.
    Perhaps, later all name registry and resolution logic can be moved here.
    """
    def resolve_name(self):
        pass



class InterpreterMode(Enum):
    # to evaluate the type of an expression
    TypeEval = auto()
    # to evaluate the value of an expression
    ValueEval = auto()


class ExpressionInterpreter(Visitor):
    """
    Interprets expressions.
    Conceptually similar to a VM, i.e. both implement Visitor pattern.
    However, a VM visits a statement in order to execute it, i.e. potentially change persisted database state.
    The Interpreter is purely stateless, in that sensse. It would be provide stateless functionality like
    evaluating expressions, determining expression type.

    TODO: I don't think this should implement the Visitor pattern; as that seems to be only useful in static type
    languages.   And moreover makes the modelling more difficult.

    This should have all the methods to evaluate expr.
    """
    def __init__(self, name_registry: NameRegistry):
        self.name_registry = name_registry
        self.record = None
        self.mode = InterpreterMode.ValueEval

    def set_mode(self, mode: InterpreterMode):
        self.mode = mode

    def set_record(self, record):
        self.record = record

    def evaluate(self, expr: Symbol, params=None):
        """
        execute statement
        :param expr:
        :param params: to determine how to interpret expr, specifically, all the different scenarios
        an expr needs to be evaluated in, e.g. when evaluating a condition, when evaluating a
        :return:
        """
        return_value = expr.accept(self)
        return return_value

    def evaluate_or_clause(self, or_clause: OrClause, record) -> Any:
        """
        This should handle both logical (eval to bool), and algebraic expressions (eval to value)
        """
        for and_clause in or_clause.and_clauses:
            for predicate in and_clause:
                pass

    def visit_or_clause(self, or_clause: OrClause):
        or_value = None
        value_unset = True
        for and_clause in or_clause.and_clauses:
            value = self.evaluate(and_clause)
            if value_unset:
                or_value = value
            else:
                # todo: eval logical or
                raise NotImplementedError
        return or_value

    def visit_and_clause(self, and_clause: AndClause):
        """
        NOTE: This handles both where the and_clause is evals to a bool, and
        to an value
        """
        and_value = None
        # ensure value is set before we begin and'ing
        value_unset = True
        for predicate in and_clause.predicates:
            pred_val = self.evaluate(predicate)
            if value_unset:
                # set first value as is
                and_value = pred_val
                value_unset = False
            else:
                # todo: if predicate evaluates to a bool value, we'll track the boolean value of the expression
                # otherwise, we'll track values. If all values are truthy, I'll return the last value, else, the
                # first falsey value - this behaves like coalesce, ifnull utils
                raise NotImplementedError

        return and_value

    def visit_column_name(self, column: ColumnName) -> Any:
        if self.mode == InterpreterMode.TypeEval:
            raise NotImplementedError
        else:
            assert self.mode == InterpreterMode.ValueEval
            val = self.record.get(column.name)
            return val


    def get_applyable(self, or_clause: OrClause):
        """
        Return an object with .apply
        """



