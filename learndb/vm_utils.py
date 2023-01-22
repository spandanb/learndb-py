from enum import Enum, auto
from typing import Type

from .datatypes import DataType, Integer, Real, Blob, Text
from .lang_parser.symbols import SymbolicDataType


class EvalMode(Enum):
    Scalar = auto()
    Grouped = auto()


def datatype_from_symbolic_datatype(data_type: SymbolicDataType) -> Type[DataType]:
    """
    Convert symbols.DataType to datatypes.DataType
    """
    if data_type == SymbolicDataType.Integer:
        return Integer
    elif data_type == SymbolicDataType.Real:
        return Real
    elif data_type == SymbolicDataType.Blob:
        return Blob
    elif data_type == SymbolicDataType.Text:
        return Text
    else:
        raise Exception(f"Unknown type {data_type}")
