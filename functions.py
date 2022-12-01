"""
This should implement functions. Specifically:
    - the function declaration interface
        -- adn the ability to get the return type
    - Here, I assume the datatype are the same as those exposed to ddl definitions, i.e.
        Integer, Float, Text, Blob.
    - perhaps the ability to execute the functions


There will be 2 kinds of functions: native and defined in language
Functions declarations should apply to both, but only lang functions will have a Function object


Native functions will have a declaration
"""
from collections.abc import Mapping
from typing import List, Set, Dict, Any, Callable
from dataclasses import dataclass

from dataexchange import Response
from datatypes import DataType, Integer, Float, Text, Blob


class UnknownFunctionInvocation(Exception):
    # todo: nuke, unused
    pass


class Argument:
    pass


@dataclass
class PositionalParam:
    arg_type: DataType


@dataclass
class NamedParam:
    arg_name: str
    arg_type: DataType


class FunctionDefinition:
    """
    Represents a function definition
    """
    def __init__(self, pos_params: List[PositionalParam],
                 named_params: Set[NamedParam],
                 body: Callable,
                 return_type: DataType):
        self.pos_params = pos_params
        self.named_params = named_params
        self.body = body
        self._return_type = return_type

    def validate_args(self, pos_args: List, named_args: Set) -> Response:
        """
        Validate pos args on existense and type;
        validate named args on name, and type
        """
        # validate positional params
        # positional params are all required
        if len(pos_args) != self.pos_params:
            return Response(False, error_message=f"Arity mismatch between expected params [{len(pos_args)}] "
                                                 f"and received args [{self.pos_params}]")
        # validate types
        for idx, parg in enumerate(pos_args):
            param = self.pos_params[idx]
            # TODO: what is the type of parg
            print(f"type of parg is {parg}")
            breakpoint()

        # validate named params
        # named params must be declared params
        for narg in named_args:
            if narg not in self.named_params:
               breakpoint()

    def apply(self, *args, **kwargs):
        """
        This models native functions, where each native function
        must override this method with function specific logic.
        For a function in leardb-sql, we will have to walk an AST
        """
        # todo: common base should handle verify arity and types of args, using func prototype
        self.body(*args, **kwargs)

    @property
    def return_type(self) -> DataType:
        return self._return_type


# function definition

def number_square_function_body(x):
    """
    Body for integer/float square
    """
    return x*x


# square an int
integer_square_function = FunctionDefinition(
    [PositionalParam(Integer)], set(), number_square_function_body, Integer
)
float_square_function = FunctionDefinition(
    [PositionalParam(Float)], set(), number_square_function_body, Float
)


# if we have same function for integers and floats, we'll name the int function
# with not qualifiers, and name the float function with _float qualifier
_FUNCTION_REGISTRY = {
    "square": integer_square_function,
    "square_float": float_square_function,

}


class FunctionInvocation:
    """
    This represents a function call over concrete value ( as opposed to over a symbolic reference like foo.x)
    # TODO: would these be separate?
    """
    @classmethod
    def create_invocation(cls, func_name: str, func_pos_args: List, func_named_args: dict) -> Response:
        """
        Validate and create a FunctionInvocation.
        Not sure if it makes sense to create a function invocation object
        """
        func_name = func_name.lower()
        func = _FUNCTION_REGISTRY.get(func_name)
        if func is None:
            return Response(False, error_message=f"Function [{func_name}] not found")
        resp = func.validate_args(func_pos_args, func_named_args)
        if not resp.success:
            return Response(False, error_message=f"Validate args failed due to [{resp.error_message}]")
        return cls(func_name, func, func_pos_args, func_named_args)

    @staticmethod
    def validate_invocation(func_name: str, func_pos_args: List, func_named_args: dict) -> Response:
        """
        Validate name and args of invocation
        """
        func_name = func_name.lower()
        func = _FUNCTION_REGISTRY.get(func_name)
        if func is None:
            return Response(False, error_message=f"Function [{func_name}] not found")
        # TODO: will this work with column types?
        # perhaps I should consider a `Column` object
        resp = func.validate_args(func_pos_args, func_named_args)
        if not resp.success:
            return Response(False, error_message=f"Validate args failed due to [{resp.error_message}]")
        return Response(True)

    @staticmethod
    def apply(self):
        pass

    def __init__(self, name: str, funcdef: FunctionDefinition, pos_args: List[Any], named_args: Dict[str, Any]):
        # func being invoked
        self.name = name
        self.func = funcdef
        self.pos_args = pos_args
        self.named_args = named_args

    def apply(self):
        """
        This should apply the function call
        """
        # can apply be done generically?
        self.func.apply(*args, )


class CurrentTimeFunction(FunctionDefinition):
    def apply(self):
        pass


class MaxFunction(FunctionDefinition):
    def apply(self, arg: str):
        pass


class Square(FunctionDefinition):
    """
    square int function, e.g. (2) -> 4
    """
    def __init__(self):
        super().__init__()
        self.pos_params = [PositionalParam(Integer)]

    def apply(self):
        pass


def resolve_function_name(name: str) -> FunctionDefinition:
    """
    Resolve function name, i.e. lookup name in registry.
    In the future this could be extended to support,
    dynamic dispatch, etc.
    """
    name = name.lower()
    if name in _FUNCTION_REGISTRY:
        return _FUNCTION_REGISTRY[name]

    raise ValueError(f"Unable to find function [{name}]")





