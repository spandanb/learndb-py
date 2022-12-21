"""
This implements functions. Specifically:
    - the function declaration interface
        -- and the ability to get the return type
    - Here, I assume the datatype are the same as those exposed to ddl definitions, i.e.
        Integer, Float, Text, Blob.
    - the ability to execute the functions

There will be 2 kinds of functions: native and defined in language
Functions declarations should apply to both, but only lang functions will have a Function object.
Native functions will have a declaration.
"""

from typing import List, Dict, Any, Callable, Type


from dataexchange import Response
from datatypes import DataType, Integer, Float, Text, Blob


class InvalidFunctionArguments(Exception):
    """
    function was invoked with invalid args.
    For position args, either the arity or type didn't match
    For named args, either name didn't exist, or type didn't match
    """


class FunctionDefinition:
    """
    Represents a function definition.

    FUTURE_NOTE: Currently, pos_params are represented as a List[DataType].
    A named_params are represented as Dict[str, DataType], where the key is the param name.
    All pos_params will always be required.
    However, in the future, we may want to support named params with default values.
    In that case, it may be easier to represent this with a new type,
        e.g. NamedParam(arg_type: DataType, has_default_value: bool, default_value: Any)
    """
    def __init__(self,
                 # used to make debugging easier
                 func_name: str,
                 pos_params: List[Type[DataType]],
                 named_params: Dict[str, Type[DataType]],
                 func_body: Callable,
                 return_type: DataType):
        self.name = func_name
        self.pos_params = pos_params
        self.named_params = named_params
        self.body = func_body
        self._return_type = return_type

    def __str__(self):
        return f"FunctionDefinition[{self.name}]"

    def __repr__(self):
        return self.__str__()

    @property
    def return_type(self) -> DataType:
        return self._return_type

    def validate_args(self, pos_args: List[Any], named_args: Dict[str, Any]) -> Response:
        """
        Validate pos and named args.
        Validate pos args on existence and type;
        validate named args on name, and type
        Args:
            pos_args: list of literals
            named_args: dict of param_name -> literal
        """
        # 1. validate positional params
        # 1.1. check arity- positional params are all required
        if len(pos_args) != len(self.pos_params):
            return Response(False, error_message=f"Arity mismatch between expected positional params [{len(pos_args)}] "
                                                 f"and received args [{self.pos_params}]")
        # 1.2. validate types
        for idx, arg in enumerate(pos_args):
            param = self.pos_params[idx]
            # arg is a literal
            if not param.is_valid_term(arg):
                return Response(False, error_message=f"Invalid positional argument type [{arg}] at index {idx}. "
                                                     f"Expected argument of type [{param.typename}]")

        # 2. validate named params
        # 2.1. validate arity - for now all named params are required
        if len(named_args) != len(self.named_params):
            return Response(False, error_message=f"Arity mismatch between expected named params [{len(named_args)}] "
                                                 f"and received args [{self.named_params}]")
        # validate existence and type
        for arg_name, arg_value in named_args.items():
            if arg_name not in self.named_params:
                return Response(False, error_message=f"Unexpected named argument [{arg_name}]")
            else:
                param = self.named_params[arg_name]
                param.is_valid_term(arg_value)
                return Response(False, error_message=f"Invalid named argument type [{arg_name}] for param [{arg_name}]."
                                                     f"Expected argument of type [{param.typename}]")

        return Response(True)

    def apply(self, pos_args: List[Any], named_args: Dict[str, Any]):
        """
        This models native functions, where each specific function
        must override this method with function specific logic.
        For a function in leardb-sql, we will have to walk an AST.

        This accepts a list of `pos_args` and a dict of `named_args`
        This method first evaluates that the args match what is expected by the function definition.
        Then invokes the actual function body/impl
        """
        # 1. validate args
        resp = self.validate_args(pos_args, named_args)
        if not resp.success:
            raise InvalidFunctionArguments(f"Invocation of function [{self.name}] failed with: {resp.error_message}")

        # 2. apply function to args
        return self.body(*pos_args, **named_args)


# function definition


def number_square_function_body(x):
    """
    Body for integer/float square
    """
    return x*x


# square an int
integer_square_function = FunctionDefinition(
    "integer_square", [Integer], {}, number_square_function_body, Integer
)
float_square_function = FunctionDefinition(
    "float_square", [Float], {}, number_square_function_body, Float
)


# if we have same function for integers and floats, we'll name the int function
# with not qualifiers, and name the float function with _float qualifier
_SCALAR_FUNCTION_REGISTRY = {
    "square": integer_square_function,
    "square_float": float_square_function,

}

_AGGREGATE_FUNCTION_REGISTRY = {
}


# public functions

def resolve_function_name(name: str) -> FunctionDefinition:
    """
    Resolve function name, i.e. lookup name in registry.
    In the future this could be extended to support,
    dynamic dispatch, etc.
    """
    name = name.lower()
    if name in _SCALAR_FUNCTION_REGISTRY:
        return _SCALAR_FUNCTION_REGISTRY[name]

    raise ValueError(f"Unable to find function [{name}]")


def get_function_names_list() -> List[str]:
    """Return list of all function names"""
    return list(_SCALAR_FUNCTION_REGISTRY.keys())


def is_aggregate_function(func_name: str) -> bool:
    """
    Return bool if function is an aggregate function
    """
    func_name = func_name.lower()
    return func_name in _AGGREGATE_FUNCTION_REGISTRY


def is_scalar_function(func_name: str) -> bool:
    """
    Return bool if function is an scalar function
    """
    func_name = func_name.lower()
    return func_name in _SCALAR_FUNCTION_REGISTRY
