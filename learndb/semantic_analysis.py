from enum import Enum, auto
from typing import Optional, Type

from .dataexchange import Response
from .datatypes import DataType
from .functions import resolve_scalar_func_name, resolve_aggregate_func_name
from .lang_parser.symbols import (
    Symbol,
    Expr,
    OrClause,
    AndClause,
    BinaryArithmeticOperation,
    FuncCall,
    ColumnName,
    Literal,
)
from .lang_parser.visitor import Visitor
from .name_registry import NameRegistry
from .vm_utils import datatype_from_symbolic_datatype, EvalMode


class SemanticAnalysisError(Exception):
    pass


class SemanticAnalysisFailure(Enum):
    TypeMismatch = auto()
    FunctionDoesNotExist = auto()
    ColumnDoesNotExist = auto()
    # aggregate function called on grouping column
    FunctionMismatch = auto()


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
        self.mode = None
        self.failure_type: Optional[SemanticAnalysisFailure] = None
        self.error_message = ""
        # schema used to check column existence, etc.
        self.schema = None

    def analyze_no_schema(self, expr):
        """
        Public method.
        Analyze an expr with no schema
        """
        self.mode = EvalMode.NoSchema
        self.schema = None
        return self.analyze(expr)

    def analyze_scalar(self, expr: Symbol, schema):
        """
        Public method.
        Analyze a scalar schema
        """
        self.mode = EvalMode.Scalar
        self.schema = schema
        return self.analyze(expr)

    def analyze_grouped(self, expr: Symbol, schema):
        """
        Public method.
        Analyze a grouped schema
        """
        self.mode = EvalMode.Grouped
        self.schema = schema
        return self.analyze(expr)

    def analyze(self, expr: Symbol) -> Response[Type[DataType]]:
        """
        Determine type of expr,
        Returns ResponseType[DataType].
        This will terminate type analysis, at the first failure
        """
        try:
            return_value = self.evaluate(expr)
            return Response(True, body=return_value)
        except SemanticAnalysisError:
            return Response(
                False, status=self.failure_type, error_message=self.error_message
            )

    def evaluate(self, expr: Symbol) -> Type[DataType]:
        return_value = expr.accept(self)
        return return_value

    def visit_expr(self, expr: Expr):
        return self.evaluate(expr.expr)

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
            self.error_message = (
                f"Type mismatch; {operation.operand1} is of type {op1_type}; "
                f"{operation.operand2} is of type {op2_type}"
            )
            raise SemanticAnalysisError()
        return op1_type

    def visit_func_call(self, func_call: FuncCall):
        """
        Validate:
        1) function exists,
        2) for scalar case, function is scalar
        3) for grouped case, this depends on the column
        """
        func_name = func_call.name
        # 1. handle scalar case
        if self.mode == EvalMode.Scalar:
            # 1.1. check if function exists
            # 2.1. function must be a scalar function
            resp = resolve_scalar_func_name(func_name)
            if not resp.success:
                # function not found
                self.error_message = resp.error_message
                raise SemanticAnalysisError()

            func = resp.body
            return func.return_type

        # 2. handle no schema case
        elif self.mode == EvalMode.NoSchema:
            # NOTE: this will also be a scalar function
            resp = resolve_scalar_func_name(func_name)
            if not resp.success:
                # function not found
                self.error_message = resp.error_message
                raise SemanticAnalysisError()

            func = resp.body
            return func.return_type

        # 3. handle grouped case
        else:
            assert self.mode == EvalMode.Grouped
            # case 1: if function is applied to a grouping column, function must be a scalar function
            # case 2: if function is applied to a non-grouping column, function must be an aggregate function

            # first attempt to resolve scalar
            resp = resolve_scalar_func_name(func_name)
            if resp.success:
                # enforce any column references are grouping columns
                # arguments could be an arbitrary expr over grouping columns
                columns = func_call.find_descendents(ColumnName)
                for column in columns:
                    if not self.schema.is_grouping_column(column.name):
                        self.failure_type = SemanticAnalysisFailure.FunctionMismatch
                        self.error_message = (
                            "Scalar function in grouped select expects grouping columns"
                        )
                        raise SemanticAnalysisError()

                func = resp.body
                return func.return_type

            resp = resolve_aggregate_func_name(func_name)
            if resp.success:
                # aggregate functions
                # currently, we only support functions that take a single column reference to a non-grouping column
                # i.e. min, max, count, etc.
                if len(func_call.args) != 1:
                    self.failure_type = SemanticAnalysisFailure.FunctionMismatch
                    self.error_message = (
                        f"Aggregate function expects one and only one column reference; "
                        f"received {len(func_call.args)}"
                    )
                    raise SemanticAnalysisError()

                arg_expr = func_call.args[0]
                column_name = arg_expr.expr

                if not isinstance(column_name, ColumnName):
                    self.failure_type = SemanticAnalysisFailure.FunctionMismatch
                    self.error_message = (
                        "Aggregate function expects a single column reference"
                    )
                    raise SemanticAnalysisError()

                if not self.schema.has_column(column_name.name):
                    self.failure_type = SemanticAnalysisFailure.ColumnDoesNotExist
                    self.error_message = f"column does not exist [{column_name.name}]"
                    raise SemanticAnalysisError()

                if not self.schema.is_non_grouping_column(column_name.name):
                    # ensure column_arg is a non-grouping column
                    self.failure_type = SemanticAnalysisFailure.FunctionMismatch
                    self.error_message = (
                        f"Expected non-grouping column as arg to aggregate function; "
                        f"received column [{column_name.name}] for function [{func_name}] "
                    )
                    raise SemanticAnalysisError()

                func = resp.body
                return func.return_type

            # function does not exist
            self.failure_type = SemanticAnalysisFailure.FunctionDoesNotExist
            self.error_message = f"Function {func_name} not found"
            raise SemanticAnalysisError()

    def visit_column_name(self, column_name: ColumnName) -> Type[DataType]:
        if self.mode == EvalMode.NoSchema:
            # no column resolution in NoSchema mode
            self.error_message = (
                f"Unexpected column name [{column_name}] in query without source"
            )
            self.failure_type = SemanticAnalysisFailure.ColumnDoesNotExist
            raise SemanticAnalysisError()

        resp = self.name_registry.resolve_column_name_type(column_name.name)
        if resp.success:
            return resp.body
        # name registry was unable to resolve name
        self.error_message = f"Name registry failed to resolve column [{column_name}] due to: [{resp.error_message}]"
        self.failure_type = SemanticAnalysisFailure.ColumnDoesNotExist
        raise SemanticAnalysisError()

    def visit_literal(self, literal: Literal) -> Type[DataType]:
        return datatype_from_symbolic_datatype(literal.type)
