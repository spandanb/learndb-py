"""
Database, i.e. storage layer datatypes, as distinct from: 1) parsed AST datatype, 2) execution datatype (i.e. impl
language datatype)
"""
import sys
import struct
from abc import ABCMeta
from typing import Any, Type

from .constants import INTEGER_SIZE, REAL_SIZE


class DataType:
    """
    This is a datatype of a value in the database.

    This provides an interface to provide serde of implemented type
    and details of underlying encoding.

    Note: There can be multiple underlying (physical) types for a given
    datatype. Datatype is logical and can be implemented in multiple
    ways (i.e. details of the serde, encoding length)

    """

    __metaclass__ = ABCMeta
    # non-serializable types are fixed value types, e.g. null, true, false
    # which are in fact only encoded in the header, and not in the data payload
    is_serializable = False
    is_fixed_length = False
    fixed_length = 0
    typename = "Untyped"

    @staticmethod
    def serialize(value) -> bytes:
        """
        serialize argument `value` to byte string
        :param value:
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def deserialize(bstring) -> Any:
        """
        deserialize argument byte string to value (of given type)
        :param bstring:
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def is_valid_term(term) -> bool:
        """
        return True if term can be converted
        to datatype
        :param term:
        :return:
        """
        raise NotImplementedError


class Integer(DataType):
    """
    Represents a fixed-size integer
    """

    is_fixed_length = True
    fixed_length = INTEGER_SIZE
    is_serializable = True
    typename = "Integer"

    @staticmethod
    def serialize(value: int) -> bytes:
        # print("In integer::serialize")
        return value.to_bytes(INTEGER_SIZE, sys.byteorder)

    @staticmethod
    def deserialize(bstring: bytes) -> int:
        return int.from_bytes(bstring, sys.byteorder)

    @staticmethod
    def is_valid_term(term) -> bool:
        return isinstance(term, int)


class Real(DataType):
    """
    Represents a fixed-size floating point number.
    Note: The usual concerns around finite-precision and
    rounding hold here.
    """

    is_fixed_length = True
    fixed_length = REAL_SIZE
    is_serializable = True
    typename = "Real"

    @staticmethod
    def serialize(value: float) -> bytes:
        """
        :param value:
        :return:
        """
        # encodes float according to native byteorder ('=')
        return struct.pack("=f", value)

    @staticmethod
    def deserialize(bstring) -> float:
        """
        :param value:
        :return:
        """
        tpl = struct.unpack("=f", bstring)
        return tpl[0]

    @staticmethod
    def is_valid_term(term) -> bool:
        return isinstance(term, float)


class Text(DataType):
    """
    represents a variable length text
    """

    is_fixed_length = False
    fixed_length = 0
    is_serializable = True
    typename = "Text"

    @staticmethod
    def serialize(value: str):
        return value.encode("utf-8")

    @staticmethod
    def deserialize(bstring: bytes):
        return bstring.decode("utf-8")

    @staticmethod
    def is_valid_term(term) -> bool:
        return isinstance(term, str)


class Boolean(DataType):
    """
    represents a variable length text
    """

    is_fixed_length = False
    fixed_length = 0
    is_serializable = True
    typename = "Text"

    @staticmethod
    def serialize(value: bool):
        return struct.pack("=?", value)

    @staticmethod
    def deserialize(bstring: bytes):
        tpl = struct.unpack("=?", bstring)
        return tpl[0]

    @staticmethod
    def is_valid_term(term) -> bool:
        return isinstance(term, bool)


class Null(DataType):
    """
    Represents a null type. Will represent this as a
    fixed length 0. This could be encoded more efficiently.

    Further, consider, whether records with nulls will
    actually store nulls, i.e. will I use a sparse representation
    to encode the record. However, such sparse representations generally
    require storing the schema definition per record and some checks/flags
    to determine whether a given value is null or not.

    translate
    to slower ops on defined values, since there are additional checks/flags
     needed to access the value.

    """

    is_fixed_length = True
    fixed_length = 0
    is_serializable = False
    typename = "Null"


class Blob(DataType):
    """
    This represent an as-is byte string
    """

    is_fixed_length = False
    fixed_length = 0
    is_serializable = True
    typename = "Blob"

    @staticmethod
    def serialize(value: bytes) -> bytes:
        return value

    @staticmethod
    def deserialize(bstring: bytes) -> bytes:
        return bstring


def is_term_valid_for_datatype(data_type: Type[DataType], term: Any) -> bool:
    """
    Return True, if term is valid for given datatype
    """
    return data_type.is_valid_term(term)
