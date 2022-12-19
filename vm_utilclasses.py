"""
Collection of classes
"""
from enum import Enum, auto
from typing import Any
from lark import Token

from dataexchange import Response
from datatypes import is_term_valid_for_datatype, Integer, Float, Text, Blob
from lang_parser.visitor import Visitor
from lang_parser.symbols3 import (Symbol,
                                  OrClause,
                                  AndClause,
                                  ColumnName,
                                  ComparisonOp,
                                  Comparison,
                                  Literal,
                                  DataType as SymbolicDataType,
                                  )
from record_utils import Record, MultiRecord


class NameRegistry:
    """
    This should be the interface for registering and resolving names.

    For now, this will mirror methods, exposed by the VM, to resolve names,
    so that this object can be passed instead of the VM.
    Perhaps, later all name registry and resolution logic can be moved here.
    """

    def __init__(self):
        self.record = None

    def set_record(self, record):
        self.record = record

    def is_name(self, operand) -> bool:
        """
        Return true if operand is a name, i.e. IDENTIFIER or SCOPED_IDENTIFIER
        """
        if isinstance(operand, Token) and (operand.type == "IDENTIFIER" or operand.type == "SCOPED_IDENTIFIER"):
            return True
        elif isinstance(operand, ColumnName):
            return True
        else:
            return False

    def resolve_name(self, operand) -> Response:
        """
        This is only valid if called on a name, i.e. is_name(operand) == True.
        Note: This returns Response to distiguish resolve failed, from resolved to None
        """
        if isinstance(operand, ColumnName):
            val = self.record.get(operand.name)
            return val

        # NOTE: this was adapated from vm.check_resovle_name
        raise NotImplementedError



class InterpreterMode(Enum):
    # to evaluate the type of an expression
    TypeEval = auto()
    # to evaluate the bool value of an expression
    BoolEval = auto()
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
        """
        Set the mode. This determines how an expr is evaluated
        """
        self.mode = mode

    def set_record(self, record):
        self.name_registry.set_record(record)
        self.record = record

    # evaluation

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

    def is_truthy(self, value) -> bool:
        """
        Return truthy value of `value`. Will follow Python convention
        """

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

    def visit_comparison(self, comparison: Comparison) -> bool:
        """
        Visit comparison and evaluate to boolean
        """
        # convert operands to values that can be compared
        if self.name_registry.is_name(comparison.left_op):
            left_value = self.name_registry.resolve_name(comparison.left_op)
        else:
            # else convert literal to value
            assert isinstance(comparison.left_op, Literal)
            left_value = self.evaluate(comparison.left_op)

        if self.name_registry.is_name(comparison.right_op):
            right_value = comparison.right_op
        else:
            assert isinstance(comparison.right_op, Literal)
            right_value = self.evaluate(comparison.right_op)

        if comparison.operator == ComparisonOp.Greater:
            pred_value = left_value > right_value
        elif comparison.operator == ComparisonOp.Less:
            pred_value = left_value < right_value
        elif comparison.operator == ComparisonOp.GreaterEqual:
            pred_value = left_value >= right_value
        elif comparison.operator == ComparisonOp.LessEqual:
            pred_value = left_value <= right_value
        elif comparison.operator == ComparisonOp.Equal:
            pred_value = left_value == right_value
        else:
            assert comparison.operator == ComparisonOp.NotEqual
            pred_value = left_value != right_value
        return pred_value

    def visit_column_name(self, column: ColumnName) -> Any:
        if self.mode == InterpreterMode.TypeEval:
            raise NotImplementedError
        else:
            assert self.mode == InterpreterMode.ValueEval
            val = self.record.get(column.name)
            return val

    def visit_literal(self, literal: Literal) -> Any:
        # convert symbolic type to actual type object
        data_type = self.symbol_to_actual_datatype(literal.type)
        assert is_term_valid_for_datatype(data_type, literal.value)
        return literal.value

    def symbol_to_actual_datatype(self, data_type: SymbolicDataType):
        """
        Convert symbols.DataType to datatypes.DataType
        """
        if data_type == SymbolicDataType.Integer:
            return Integer
        elif data_type == SymbolicDataType.Real:
            return Float
        elif data_type == SymbolicDataType.Blob:
            return Blob
        elif data_type == SymbolicDataType.Text:
            return Text
        else:
            raise Exception(f"Unknown type {data_type}")

