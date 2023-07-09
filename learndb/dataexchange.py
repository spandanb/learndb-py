"""
Contains classes used for data exchange, i.e.
do not have any "compute" methods.
"""
from typing import Any, TypeVar, Generic
from dataclasses import dataclass
from enum import Enum, auto

# This is used to parameterize Response type as per: https://stackoverflow.com/a/42989302
T = TypeVar("T")


# section result enums


# NOTE: now that I'm returning Response objects
# I don't need a Success enums - this was also
# when I was following
class MetaCommandResult(Enum):
    Success = auto()
    UnrecognizedCommand = auto()
    InvalidArgument = auto()


class StatementType(Enum):
    Uninitialized = auto()
    Insert = auto()
    Select = auto()
    Delete = auto()


@dataclass
class Response(Generic[T]):
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
    body: T = None

    def __str__(self):
        if self.error_message:
            return f"Response(fail, {self.error_message})"
        else:
            return f"Response(success, {str(self.body)})"

    def __repr__(self):
        return self.__str__()
