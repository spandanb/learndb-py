"""
Collection of classes
"""
from enum import Enum, auto
from typing import Any, Type
from lark import Token

from dataexchange import Response
from datatypes import is_term_valid_for_datatype, DataType, Integer, Float, Text, Blob
from lang_parser.visitor import Visitor
from lang_parser.symbols3 import (Symbol,
                                  OrClause,
                                  AndClause,
                                  ColumnName,
                                  ComparisonOp,
                                  Comparison,
                                  Literal,
                                  DataType as SymbolicDataType,
                                  BinaryArithmeticOperation,
                                  ArithmeticOp,
                                  FuncCall
                                  )
from functions import get_function_names_list, resolve_function_name
from record_utils import Record, MultiRecord


class SemanticAnalysisError(Exception):
    pass



class NameRegistry:
    """
    This should be the interface for registering and resolving names.
    Name resolutions is done over column names from: 1) a schema, 2) a record

    For now, this will mirror methods, exposed by the VM, to resolve names,
    so that this object can be passed instead of the VM.
    Perhaps, later all name registry and resolution logic can be moved here.
    """

    def __init__(self):
        #
        self.record = None
        # schema to resolve names from
        self.schema = None
        # register names of all functions
        self.functions = set(get_function_names_list())

    def set_record(self, record):
        self.record = record

    def set_schema(self, schema):
        self.schema = schema

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

        # NOTE: this was adapated from vm.check_resolve_name
        raise NotImplementedError

    def resolve_name_type(self, operand: str) -> Response:
        if self.schema.has_column(operand):
            column = self.schema.get_column_by_name(operand)
            return Response(True, body=column.datatype)
        return Response(False, error_message=f"Unable to resolve column [{operand}]")

    def resolve_func_name(self, func_name: str) -> Response:
        """
        Resolve the func_name.
        """
        if func_name in self.functions:
            return Response(True, body=resolve_function_name(func_name))
        return Response(False, error_message=f"Function [{func_name}] not found")


class ExpressionInterpreter(Visitor):
    """
    Interprets expressions.
    Conceptually similar to a VM, i.e. both implement Visitor pattern.
    However, a VM visits a statement in order to execute it, i.e. potentially change persisted database state.
    The Interpreter is purely stateless- providing stateless functionality like
    evaluating expressions to value, to booleans, determining expression type, and other utils like stringify exprs.

    TODO: consolidate all expr evaluation logic here
    TODO: since semantic analyzer handles type checking, remove support for InterpreterMode.EvalType; likely mode can
    itself be removed, since I can enforce only booleans for conditions.
    """
    def __init__(self, name_registry: NameRegistry):
        self.name_registry = name_registry
        self.record = None

    def set_record(self, record):
        self.name_registry.set_record(record)
        self.record = record

    # evaluation

    def evaluate(self, expr: Symbol) -> Any:
        """
        execute statement. NOTE: evaluation is affected by: 1) mode and 2) record that expression is evaluated over
        """
        return_value = expr.accept(self)
        return return_value

    def evaluate_over_record(self, expr: Symbol, record) -> Any:
        self.set_record(record)
        return self.evaluate(expr)

    def evaluate_over_recordset(self, expr: Symbol, group_key, group_recordset_iter):
        # TODO implement me
        raise NotImplementedError

    # section: other public utils

    def stringify(self, expr: OrClause) -> str:
        return str(expr)

    # section: visit methods

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

    def visit_binary_arithmetic_operation(self, operation: BinaryArithmeticOperation):
        op1_value = self.evaluate(operation.operand1)
        op2_value = self.evaluate(operation.operand2)
        if operation.operator == ArithmeticOp.Addition:
            return op1_value + op2_value
        elif operation.operator == ArithmeticOp.Subtraction:
            return op1_value - op2_value
        elif operation.operator == ArithmeticOp.Multiplication:
            return op1_value * op2_value
        else:
            assert operation.operator == ArithmeticOp.Division
            if isinstance(op1_value, int):
                return op1_value // op2_value
            else:
                return op1_value / op2_value

    def visit_func_call(self, func_call: FuncCall):
        # get function
        resp = self.name_registry.resolve_func_name(func_call.name)
        assert resp.success
        func = resp.body
        # evaluate any arguments
        evaluated_pos_arg = [self.evaluate(arg) for arg in func_call.args]
        # NOTE: we currently only support positional args
        return func.apply(evaluated_pos_arg, {})

    def visit_column_name(self, column: ColumnName) -> Any:
        val = self.record.get(column.name)
        return val

    def visit_literal(self, literal: Literal) -> Any:
        # convert symbolic type to actual type object
        data_type = self.symbol_to_actual_datatype(literal.type)
        assert is_term_valid_for_datatype(data_type, literal.value)
        return literal.value

    # section: helpers

    def is_truthy(self, value) -> bool:
        """
        Return truthy value of `value`. Will follow Python convention
        """
        breakpoint()

    def symbol_to_actual_datatype(self, data_type: SymbolicDataType) -> Type[DataType]:
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


class SemanticAnalyzer(Visitor):
    """
    Performs semantic analysis:
        - evaluate expr types
        - determine if expr is valid,
            -- an expr may be invalid due to non-existent function, or column references
            -- type incompatible operation

    NOTE: (for now) type checking will be strict, i.e. no auto conversions,
        e.g. 2+ 2.0 will fail due to a type mismatch

    """
    def __init__(self, name_registry: NameRegistry):
        self.name_registry = name_registry
        self.error_message = ""

    def analyze(self, expr: Symbol) -> Response:
        """
        Public method
        Determine type of expr,
        Returns ResponseType[DataType].
        This will terminate type analysis, at the first failure
        """
        try:
            return_value = self.evaluate(expr)
            return Response(True, body=return_value)
        except SemanticAnalysisError:
            return Response(False, error_message=self.error_message)

    def evaluate(self, expr: Symbol) -> Any:
        return_value = expr.accept(self)
        return return_value

    def visit_or_clause(self, or_clause: OrClause):
        or_value = None
        value_unset = True
        for and_clause in or_clause.and_clauses:
            value = self.evaluate(and_clause)
            if value_unset:
                or_value = value
                value_unset = False
            else:
                # NOTE: and clause can only be applied over booleans (true, false, null), else error
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
                # NOTE: and clause can only be applied over booleans (true, false, null), else error
                raise NotImplementedError

        return and_value

    def visit_binary_arithmetic_operation(self, operation: BinaryArithmeticOperation):
        # evaluate operators, then check type
        op1_type = self.evaluate(operation.operand1)
        op2_type = self.evaluate(operation.operand2)
        # for now, we will only support strict type checking, i.e.
        if op1_type != op2_type:
            self.error_message = (f"Type mismatch; {operation.operand1} is of type {op1_type}; "
                                 f"{operation.operand2} is of type {op2_type}")
            raise SemanticAnalysisError()
        return op1_type

    def visit_func_call(self, func_call: FuncCall):
        resp = self.name_registry.resolve_func_name(func_call.name)
        if not resp.success:
            self.error_message = resp.error_message
            raise SemanticAnalysisError()

        func = resp.body
        return func.return_type

    def visit_column_name(self, column_name: ColumnName) -> Type[DataType]:
        resp = self.name_registry.resolve_name_type(column_name.name)
        if resp.success:
            return resp.body
        # name registry was unable to resolve name
        self.error_message = f"Name registry failed to resolve column [{column_name}] due to: [{resp.error_message}]"
        raise SemanticAnalysisError()



