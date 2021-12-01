from enum import Enum

from schema import Integer
from dataexchange import Record, Response


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


def deserialize_cell(cell: bytes, schema: 'Schema') -> Response:
    """
    deserialize cell corresponding to schema
    :param cell:
    :param schema:
    :return: Response[Record]
    """


def get_cell_key(cell: bytes) -> Response:
    """
    return key given cell.
    NOTE: this does not require schema, since key is agnostic
    :param cell:
    :return: Response[int]
    """