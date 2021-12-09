import sys
from enum import Enum

from constants import (LEAF_NODE_KEY_SIZE_SIZE,
                       LEAF_NODE_DATA_SIZE_SIZE,
                       INTEGER_SIZE,
                       FLOAT_SIZE
                       )

from datatypes import DataType, Null, Integer, Text, Blob, Float
from dataexchange import Response
from schema import Integer, Record, Schema


class InvalidCell(Exception):
    """
    A invalid formatted cell
    """


class SerialType(Enum):
    """
    serial-type of encoded data
    """
    Null = 0
    Integer = 1
    Float = 2
    Text = 3
    Blob = 4


def serialtype_to_datatype(serial_type: SerialType) -> DataType:
    """
    Convert serial type enum to datatype
    :param serial_type:
    :return:
    """
    if serial_type == SerialType.Null:
        return Null
    elif serial_type == SerialType.Integer:
        return Integer
    elif serial_type == SerialType.Float:
        return Float
    elif serial_type == SerialType.Text:
        return Text
    else:
        assert serial_type == SerialType.Blob
        return Blob


def datatype_to_serialtype(datatype: DataType) -> SerialType:
    """
    Convert datatype to serialtype
    :param datatype:
    :return:
    """
    if datatype == Null:
        return SerialType.Null
    elif datatype == Integer:
        return SerialType.Integer
    elif datatype == Float:
        return SerialType.Float
    elif datatype == Text:
        return SerialType.Text
    else:
        assert datatype == Blob
        return SerialType.Blob


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
                serial_type = datatype_to_serialtype(column.datatype)
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
    print(f'In serialize; key-size: {key_size}, data-header-len: {data_header_len}, data_size: {data_size}')
    #print(cell)
    cell = key_size + data_size + key + data_payload
    return Response(True, body=cell)


def deserialize_cell(cell: bytes, schema: Schema) -> Response:
    """
    deserialize cell corresponding to schema
    :param cell:
    :param schema:
    :return: Response[Record]
    """
    values = {}  # colname -> value
    # read the columns in the cell
    offset = 0
    # TODO: seems LEAF_NODE_KEY_SIZE_SIZE is same as INTEGER_SIZE; replace former with latter
    key_size = Integer.deserialize(cell[offset: offset + LEAF_NODE_KEY_SIZE_SIZE])
    offset += LEAF_NODE_KEY_SIZE_SIZE
    data_size = Integer.deserialize(cell[offset: offset + LEAF_NODE_DATA_SIZE_SIZE])
    offset += LEAF_NODE_DATA_SIZE_SIZE

    # read key column
    # bytes corresponding to key
    key_bytes = cell[offset: offset + key_size]
    key = Integer.deserialize(key_bytes)
    key_columns = [col.name for col in schema.columns if col.is_primary_key]
    assert len(key_columns) == 1, "More than 1 key column"
    key_column_name = key_columns[0]
    values[key_column_name] = key
    # after this, offset points past the key bytes, i.e. to the first
    # byte of data payload
    offset += len(key_bytes)

    # keep track of which column (relative position) we have read from
    col_pos = 0

    # read non-key columns
    header_size = Integer.deserialize(cell[offset: offset + INTEGER_SIZE])
    # this is the abs addr value
    header_abs_ubound = offset + header_size
    print(f'In deserialize; key-size: {key_size}, data-header-len: {header_size}, data_size: {data_size}')
    print(cell)
    # process column metadata
    # initialize data header ptr
    # points to first column metadata
    header_offset = offset + INTEGER_SIZE
    # first address where data resides
    data_offset = offset + header_size
    while header_offset < header_abs_ubound:
        # read until all column metadata has been run
        serial_type_value = Integer.deserialize(cell[header_offset: header_offset + INTEGER_SIZE])
        serial_type = SerialType(serial_type_value)
        # resolve datatype
        datatype = serialtype_to_datatype(serial_type)
        # increment header ptr
        header_offset += INTEGER_SIZE

        # check whether column type is variable length
        varlen = 0
        if not datatype.is_fixed_length:
            varlen = Integer.deserialize(cell[header_offset: header_offset + INTEGER_SIZE])
            header_offset += INTEGER_SIZE

        # resolve column name
        column = schema.columns[col_pos]
        col_pos += 1
        if column.is_primary_key:
            # we've already handled the key column above; consider next column
            column = schema.columns[col_pos]
            col_pos += 1

        # read body
        if datatype.is_fixed_length and not datatype.is_serializable:
            # handle fixed-value type, i.e. only null for now, boolean's would be similar
            assert datatype == Null
            values[column.name] = None
        elif datatype.is_fixed_length:
            # handle fixed-length type
            # increment body by a fixed amount
            values[column.name] = datatype.deserialize(cell[data_offset: data_offset + datatype.fixed_length])
            data_offset += datatype.fixed_length
        else:
            assert datatype.is_fixed_length is False
            assert varlen > 0
            # handle variable length type
            # increment body by a variable amount
            data_bstring = cell[data_offset: data_offset + varlen]
            values[column.name] = datatype.deserialize(data_bstring)
            data_offset += varlen

    # add non-existent columns with null values
    for column in schema.columns:
        if column.name not in values:
            values[column.name] = None

    record = Record(values, schema)
    return Response(True, body=record)


