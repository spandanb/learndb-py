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
from datatypes import DataType, Integer, Float, Text, Blob


class FunctionSignature:
    """
    This is the scafolding for the function signature
    Currently this stores output_type; doing this to unblock schema gen
    but this should also
    This contains the input and output params of the function
    """
    def __init__(self, ouput_type: DataType, input_params=None):
        self.output_type = ouput_type
        # todo: store and handle input_params


# NOTE: there needs to be a seperate function for each
# argument type, NOTE: either function overloading or generics
# will solve this; but those are fairly complex to implement
_FUNCTION_SIGNATURES = {
    "avg": FunctionSignature(Integer),
    "avg_float": FunctionSignature(Float),
    "min": FunctionSignature(Integer),
    "min_float": FunctionSignature(Float),
    "max": FunctionSignature(Integer),
    "max_float": FunctionSignature(Float),
    "count": FunctionSignature(Integer),
}


class Function:
    """
    This represents a function body, for a user defined function- or
    more broadly, a function defined in learndb-sql.
    This is currently unused, but will be needed when implementing
    support for function declaration
    """
    def __init__(self, func_name, func_args):
        self.name = func_name
        self.args = func_args

    def apply(self, *args, **kwargs):
        # subclass should impl
        raise NotImplementedError


def get_function_signature(name: str) -> FunctionSignature:
    """
    """
    name = name.lower()
    if name in _FUNCTION_SIGNATURES:
        return _FUNCTION_SIGNATURES[name]

    raise ValueError(f"Unable to find function [{name}]")





