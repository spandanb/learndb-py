import numbers
from typing import Any, Union

from .constants import REAL_EPSILON
from .datatypes import is_term_valid_for_datatype, DataType, Integer, Real, Text, Blob
from .functions import resolve_scalar_func_name, resolve_aggregate_func_name
from .lang_parser.visitor import Visitor
from .lang_parser.symbols import (Symbol,
                                 OrClause,
                                 AndClause,
                                 ColumnName,
                                 ComparisonOp,
                                 Comparison,
                                 Literal,
                                 SymbolicDataType,
                                 BinaryArithmeticOperation,
                                 ArithmeticOp,
                                 FuncCall,
                                 Expr
                                 )

from .name_registry import NameRegistry
from .record_utils import SimpleRecord, ScopedRecord, GroupedRecord, InvalidNameException
from .vm_utils import EvalMode, datatype_from_symbolic_datatype


class ExpressionInterpreter(Visitor):
    """
    Interprets expressions.
    Conceptually similar to a VM, i.e. both implement Visitor pattern.
    However, a VM visits a statement in order to execute it, i.e. potentially change persisted database state.
    The Interpreter is purely stateless- providing stateless functionality like
    evaluating expressions to value, to booleans, determining expression type, and other utils like stringify exprs.
    """
    def __init__(self, name_registry: NameRegistry):
        self.name_registry = name_registry
        # mode determines whether this is evaluating an expr over a scalar record, or a grouped recordset
        self.mode = None
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

    def evaluate_over_record(self, expr: Symbol, record: Union[SimpleRecord, ScopedRecord]) -> Any:
        """
        Evaluate `expr` over `record` i.e. evaluating any column references from value in `record`
        """
        self.mode = EvalMode.Scalar
        self.set_record(record)
        return self.evaluate(expr)

    def evaluate_over_grouped_record(self, expr: Symbol, record: GroupedRecord):
        """
        Evaluate `expr` over `record` i.e. evaluating any column references from value in `record`
        """
        self.mode = EvalMode.Grouped
        self.set_record(record)
        return self.evaluate(expr)

    # section: other public utils

    @staticmethod
    def simplify_expr(expr: Expr) -> Symbol:
        """
        Utility method to simplify `expr`. Simplify means that if `expr`
        contains only a single primitive (literal or reference), i.e. without any logical
        or arithmetic operations, then return the primitive; else return the entire or_clause
        NOTE: This operation is O(size of all descendents rooted at `expr`)
        """
        primitive_types = (Literal, ColumnName, FuncCall)
        descendents = expr.find_descendents(primitive_types)
        if len(descendents) == 1:
            # only a single primitive- unwrap
            return descendents[0]
        else:
            # some complex/compound operation
            return expr

    def stringify(self, expr: Expr) -> str:
        """
        Simplify and stringify expr
        """
        simplified = self.simplify_expr(expr)
        if isinstance(simplified, ColumnName):
            return simplified.name
        elif isinstance(simplified, Literal):
            return str(simplified.value)
        else:
            return str(simplified)

    # section: visit methods

    def visit_expr(self, expr: Expr):
        # Expr is root of expression hierarchy
        return self.evaluate(expr.expr)

    def visit_or_clause(self, or_clause: OrClause) -> Union[bool, Any]:
        """
        Evaluate or clause.
        NOTE: This handles both cases, 1) where the and_clause evaluates to a boolean, and
        2) to a value. In case 2) it would evaluate the truthyness of the values, and return
        the last truthy value, if all are truthy or the first falsey value.
        """
        or_value = None
        value_unset = True
        for and_clause in or_clause.and_clauses:
            value = self.evaluate(and_clause)
            if value_unset:
                # set value as is
                or_value = value
                if isinstance(value, bool) and value is True:
                    # early exit, entire expression will evaluate to True
                    return True
            else:
                # how to update the value depends on whether it's a bool or non-bool value
                if isinstance(value, bool):
                    if value is True:
                        return True
                else:
                    # return first truthy value
                    breakpoint()
                    raise NotImplementedError

        return or_value

    def visit_and_clause(self, and_clause: AndClause) -> Union[bool, Any]:
        """
        Evaluate and clause.
        NOTE: This handles both cases, 1) where the and_clause evaluates to a boolean, and
        2) to a value. In case 2) it would evaluate the truthyness of the values, and return
        the last truthy value, if all are truthy or the first falsey value.
        """
        and_value = None
        # ensure value is set before we begin and'ing
        value_unset = True
        for predicate in and_clause.predicates:
            value = self.evaluate(predicate)
            if isinstance(value, bool):
                # if predicate evaluates to a bool value, we'll track the boolean value of the expression
                if value is False:
                    # early exit, entire clause will be True
                    return False
                else:
                    and_value = value
            else:
                # non-bool, we'll track values. If all values are truthy, I'll return the last value, else, the
                # first falsey value - this behaves like coalesce, ifnull utils; similar to python and
                if value_unset:
                    # set first value as is
                    and_value = value
                    value_unset = False
                else:
                    breakpoint()
                    raise NotImplementedError

        return and_value

    def visit_comparison(self, comparison: Comparison) -> bool:
        """
        Visit comparison and evaluate to boolean
        """
        # convert operands to values that can be compared
        if self.name_registry.is_name(comparison.left_op):
            resp = self.name_registry.resolve_name(comparison.left_op)
            assert resp.success
            left_value = resp.body
        else:
            # else evaluate to get value
            left_value = self.evaluate(comparison.left_op)

        if self.name_registry.is_name(comparison.right_op):
            resp = self.name_registry.resolve_name(comparison.right_op)
            assert resp.success
            right_value = resp.body
        else:
            right_value = self.evaluate(comparison.right_op)

        if comparison.operator != ComparisonOp.Equal and comparison.operator != ComparisonOp.NotEqual:
            # equality and inequality can be for any datatypes
            # less than etc. comparisons are only defined for numeric types
            assert isinstance(left_value, numbers.Number) and isinstance(right_value, numbers.Number)

        # NOTE: we handle both integer and real (floating point) numbers
        # if the two numbers are integers or are more than REAL_EPSILON apart, we can do a strict comparison
        # however, if they are not so; we must evaluate fuzzy comparison
        if isinstance(left_value, float) and abs(left_value - right_value) <= REAL_EPSILON:
            return self.evaluate_fuzzy_comparison(comparison, left_value, right_value, REAL_EPSILON)
        else:
            return self.evaluate_strict_comparison(comparison, left_value, right_value)

    @staticmethod
    def evaluate_strict_comparison(comparison, left_value: Union[int, float], right_value: Union[int, float]) -> bool:
        """
        Evaluate strict comparison between `left_value` and `right_value`
        """
        if comparison.operator == ComparisonOp.Equal:
            pred_value = left_value == right_value
        elif comparison.operator == ComparisonOp.NotEqual:
            pred_value = left_value != right_value
        elif comparison.operator == ComparisonOp.Greater:
            pred_value = left_value > right_value
        elif comparison.operator == ComparisonOp.Less:
            pred_value = left_value < right_value
        elif comparison.operator == ComparisonOp.GreaterEqual:
            pred_value = left_value >= right_value
        else:
            assert comparison.operator == ComparisonOp.LessEqual
            pred_value = left_value <= right_value
        return pred_value

    @staticmethod
    def evaluate_fuzzy_comparison(comparison, left_value: float, right_value: float, epsilon: float):
        """
        Evaluate fuzzy comparison between `left_value` and `right_value`.
        NOTE: real numbers can't be exactly compared; two reals are equal if
        they are within epsilon of each other. A number with an epsilon can be
        viewed as a range.

        NOTE: This behavior may need to be revisited. For now it provides something
        sensible enough.
        """
        if comparison.operator == ComparisonOp.Equal:
            pred_value = abs(left_value - right_value) <= epsilon
        elif comparison.operator == ComparisonOp.NotEqual:
            pred_value = abs(left_value - right_value) > epsilon
        elif comparison.operator == ComparisonOp.Greater:
            pred_value = left_value + epsilon > right_value
        elif comparison.operator == ComparisonOp.Less:
            pred_value = left_value - epsilon < right_value
        elif comparison.operator == ComparisonOp.GreaterEqual:
            pred_value = left_value + epsilon >= right_value
        else:
            assert comparison.operator == ComparisonOp.LessEqual
            pred_value = left_value - epsilon <= right_value
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
        """
        Evaluate
        """
        if self.mode == EvalMode.Scalar:
            # get function
            resp = resolve_scalar_func_name(func_call.name)
            assert resp.success
            func = resp.body

            # NOTE: for scalar case the args may be expressions, e.g. square(col_a + 1)
            # and hence should be evaluated before applying the function
            evaluated_pos_arg = [self.evaluate(arg) for arg in func_call.args]
            # NOTE: we currently only support positional args
            return func.apply(evaluated_pos_arg, {})
        else:
            # NOTE: for grouped case, we need to handle 2 cases:
            # case 1) scalar function over grouped column; this is the same as the scalar case
            resp = resolve_scalar_func_name(func_call.name)
            if resp.success:
                func = resp.body
                evaluated_pos_arg = [self.evaluate(arg) for arg in func_call.args]
                # NOTE: we currently only support positional args
                return func.apply(evaluated_pos_arg, {})

            # case 2) aggregate function over non-grouped column; here the function should accept
            # only a single argument, i.e. column name, of non-grouped column.
            # This is because, semantically, for currently supported aggregate functions, i.e.
            # min, max, count, etc, it's unclear what multiple arguments could mean, and is hence unsupported.
            resp = resolve_aggregate_func_name(func_call.name)
            assert resp.success  # NOTE: this has been confirmed by SemanticAnalyzer
            func = resp.body
            arg_column_name = func_call.args[0].expr.name
            # get list of column values from recordset
            value_list = self.record.recordset_to_values(arg_column_name)
            # wrap value list, since agg function expects a list of pos args, where first arg is value list
            return func.apply([value_list], {})

    def visit_column_name(self, column: ColumnName) -> Any:
        val = self.record.get(column.name)
        return val

    def visit_literal(self, literal: Literal) -> Any:
        # convert symbolic type to actual type object
        data_type = datatype_from_symbolic_datatype(literal.type)
        assert is_term_valid_for_datatype(data_type, literal.value)
        return literal.value

