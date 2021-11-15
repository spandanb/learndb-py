"""
Contains classes used for data exchange, i.e.
do not have any "compute" methods.

"""
from typing import Any
from dataclasses import dataclass
from enum import Enum, auto


# section result enums

# NOTE: now that I'm returning Response objects
# I don't need a Success enums - this was also
# when I was following
class MetaCommandResult(Enum):
    Success = auto()
    UnrecognizedCommand = auto()


class PrepareResult(Enum):
    Success = auto()
    UnrecognizedStatement = auto()


class ExecuteResult(Enum):
    Success = auto()
    TableFull = auto()


class StatementType(Enum):
    Uninitialized = auto()
    Insert = auto()
    Select = auto()
    Delete = auto()


@dataclass
class Response:
    """
    Use as a generic class to encapsulate a response and a body
    """
    # is success
    success: bool
    # if fail, why
    error_message: str = None
    # an enum encoding state
    status: Any = None
    # output of operation
    body: Any = None


@dataclass
class Row:
    """
    NOTE: this assumes a fixed table definition. Fixing the
    table definition, like in the tutorial to bootstrap the
    (de)serialize logic.
    Later when I can handle generic schemas this will need to be
    made generic
    """
    identifier: int
    body: str


@dataclass
class Statement:
    statement_type: StatementType
    row_to_insert: Row = None
    key_to_delete: int = None

