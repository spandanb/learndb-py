"""
This contain structures to generate and manipulate
schema- including the logical schema (column name, column type),
and the physical representation (number of bytes,
length of encoding) thereof.

This should be split into logical (schema.py) and physical schema (serde.py)

The physical encoding is the file format.
"""

import sys
import struct
from typing import Any, List
from abc import ABCMeta, abstractmethod
from enum import Enum

from dataexchange import Response


# serde constants
# length of encoded bytes
INTEGER_SIZE = 4


class SerialType(Enum):
    """
    serial-type of encoded data
    """0
    Null = 0
    Integer = 1
    Float = 2
    Text = 3
    Blob = 4


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

    @staticmethod
    def serialize(value) -> bytes:
        """
        serialize argument `value` to byte string
        :param value:
        :return:
        """
        raise NotImplemented

    @staticmethod
    def deserialize(bstring) -> Any:
        """
        deserialize argument byte string to value (of given type)
        :param bstring:
        :return:
        """
        raise NotImplemented


class Integer(DataType):
    """
    Represents a fixed-size integer
    """

    is_fixed_length = True
    fixed_length = INTEGER_SIZE
    is_serializable = True

    @staticmethod
    def serialize(value: int) -> bytes:
        # print("In integer::serialize")
        return value.to_bytes(INTEGER_SIZE, sys.byteorder)

    @staticmethod
    def deserialize(bstring: bytes) -> int:
        return int.from_bytes(bstring, sys.byteorder)


class Float(DataType):
    """
    Represents a fixed-size floating point number.
    Note: The usual concerns around finite-precision and
    rounding hold here.
    """

    is_fixed_length = True
    fixed_length = 4
    is_serializable = True

    @staticmethod
    def serialize(value: float) -> bytes:
        """
        :param value:
        :return:
        """
        # encodes float according to native byteorder ('=')
        return struct.pack('=f', value)

    @staticmethod
    def deserialize(bstring) -> float:
        """
        :param value:
        :return:
        """
        tpl = struct.unpack('=f', bstring)
        return tpl[0]


class Text(DataType):
    """
    represents a variable length text
    """
    is_fixed_length = False
    fixed_length = 0
    is_serializable = True

    @staticmethod
    def serialize(value: str):
        return value.encode("utf-8")

    @staticmethod
    def deserialize(bstring: bytes):
        return bstring.decode("utf-8")


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
    fixed_length = INTEGER_SIZE
    is_serializable = False


class Blob(DataType):
    """
    This represent an as-is byte string
    """
    is_fixed_length = False
    fixed_length = 0
    is_serializable = True

    @staticmethod
    def serialize(value: bytes) -> bytes:
        return value

    @staticmethod
    def deserialize(bstring: bytes) -> bytes:
        return bstring


class Schema:
    """
    Represents a schema. This includes
    logical aspects (name, is_primary_key) and physical aspects
    (number of bytes of storage, fixed vs. variable length encoding)

    Note a schema must be valid. If the schema is invalid, this
    should be raised prior to creating.

    NOTE: once constructed a schema should be treated as read-only
    """
    def __init__(self, name: str = None):
        # name of object/entity defined
        self.name = name
        # list of column objects ordered by definition order
        self.columns: List[Column] = []


class CatalogSchema(Schema):
    """
    Hardcoded schema object for the catalog table.

    This corresponds to the following table definition:
    create table catalog (
        type  text,
        name text,
        tbl_name text,
        rootpage integer,
        sql text
    )

    NOTE: This could be bootstrapped by parsing the above schema
    definition text- as all other schemas will be. But this
    will be easier. Yet, even doing that will require special
    handling of the catalog schema. Further, having a hardcoded
    schema will provide an easy validation on the parser.
    """

    def __init__(self):
        super().__init__('catalog')
        self.columns = [
            Column('pkey', Integer, is_primary_key=True),
            Column('name', Text),
            Column('root_pagenum', Integer),
            Column('sql', Text)
        ]


class Column:
    """
    Represents a column in a schema
    """
    def __init__(self, name: str, datatype, is_primary_key: bool = False, is_nullable: bool = False):
        self.name = name
        self.datatype = datatype
        self.is_primary_key = is_primary_key
        self.is_nullable = is_nullable


class Record:
    """
    Represents a record from table.
    This always corresponds to a given schema.
    """
    def __init__(self, schema: Schema = None):
        # unordered mapping from: column-name -> column-value
        self.values = {}
        self.schema = Schema


def serialize_record(record: Record) -> Response:
    """
    Serialize an entire record and return the bytes corresponding
    to a cell.

    For now, serialize each value and concatenate
    the resulting bytes. If this is not performant,
    consider using struct.pack

    See docs/file-format.txt for complete details; the following are
    the key details of a node:

    - (low address) header, cell pointer array, unallocated space, cells (high address)
    - cell ptrs are sorted by key (2 bytes); contain page offset to cell
    - cell -> [key_size(4B), data_size(4B), payload (key, data)]
        -- data can be divided into header and body
        -- data header -> [size of header, serial types (size of variable length value)?
        -- data body -> concatenated bytes of serialized values (in definition order)
        -- all data must fit in a cell, i.e. no overflow- this limits the max content size to what can fit in a single cell

    serial types:
        sqlite for inspiration (https://www.sqlite.org/fileformat2.html#record_format)

            serial-type  byte-length  datatype
            0            0            Null
            1            4            Integer
            2            4            Float
            3            var          Text
            4            var          Blob

        Types with a fixed-value, e.g. null will not be encoded in the data payload.


    """
    # encode columns in definition order
    key = b''
    data_header = b''
    data = b''
    # 1. encode chunks of payload
    for column in record.schema.columns:
        # get column value
        value = record.values.get(column.name)
        # handle key
        if column.is_primary_key:
            # ensure primary key is an int
            # this validation should be done at schema generation time
            assert column.datatype == Integer, "Primary key must be an integer"
            assert value is not None, "Primary key must exist"
            key = column.datatype.serialize(value)
        # handle non-key field
        else:
            # check if a value is required
            if value is None and column.is_nullable is False:
                return Response(False, error_message=f"Required column [{column.name}] missing value")

            if value is None:
                serial_type = SerialType.Null
                serialized_serial_type = Integer.serialize(serial_type.value)
                data_header += serialized_serial_type
            else:
                serial_type = SerialType(column.datatype.__class__.__name__)
                # all columns except null can be serialized;
                # in the future, there may be non-null unserializable types, e.g. bool
                assert column.datatype.is_serializable, f"non-null unserializable column [{column.name}]"

                # serialize header
                serialized_serial_type = Integer.serialize(serial_type.value)
                data_header += serialized_serial_type

                # serialize data
                serialized_value = column.datatype.serialize(value)
                data += serialized_value

                # check if datatype is variable length
                if not column.datatype.is_fixed_length:
                    length = Integer.serialize(len(serialized_value))
                    # encode length in header
                    data_header += length

    # data-header is defined like:
    # [size of header, serial types (size of variable length value)? ]
    # NOTE: the data header, size of header includes self
    data_header_len = Integer.serialize(Integer.fixed_length + len(data_header))
    data_header = data_header_len + data_header

    # 2. assemble chunks as per file format spec into a cell
    # i.e. cell = [key_size(4B), data_size(4B), key(var), data-header(var), data(var) ]
    key_size = Integer.serialize(len(key))
    data_payload = data_header + data
    data_size = Integer.serialize(len(data_payload))
    cell = key_size + data_size + key + data_header
    return Response(True, body=cell)


def deserialize_cell() -> Response:
    pass


def validate_schema(schema: Schema):
    """
    Ensure schema is valid.
    A valid schema must have:
        - integer primary key (this can be handled automatically later)
        - unique column names
        - valid column names
        - valid datatypes

    :param schema:
    :return:
    """
    # validate - single column primary key
    if len([col for col in schema.columns if col.is_primary_key]) != 1:
        return Response(False, body='missing primary key')

    # validate - primary key is integer
    pkey = None
    for col in schema.columns:
        if col.is_primary_key:
            pkey = col
            break
    if pkey.datatype != Integer:
        return Response(False, body='primary key must be of integer type')

    # validate column names are unique
    names = set()
    for col in schema.columns:
        if col.name in names:
            return Response(False, body=f'duplicate column name [{col.name}]')
        names.add(col.name)

    # validate column types are valid
    for col in schema.columns:
        if not issubclass(col.datatype, DataType):
            return Response(False, body=f'invalid datatype for [{col.name}]')

    return Response(True)


def construct_schema():
    pass


if __name__ == '__main__':
    myint = Integer()
