import sys

from pager import Pager
from btree import Tree
from datatypes import Row
from constants import ROW_SIZE, ID_SIZE, BODY_SIZE, BODY_OFFSET, ID_OFFSET


class Table:
    """
    Currently `Table` interface is around (de)ser given a row number.
    Ultimately, the table should
    represent the logical-relation-entity, and access to the pager, i.e. the storage
    layer should be done via an Engine, that acts as the storage layer access for
    all tables.
    """
    def __init__(self, pager: Pager, root_page_num: int = 0):
        self.pager = pager
        self.root_page_num = root_page_num
        self.tree = Tree(pager, root_page_num)

    @staticmethod
    def serialize(row: Row) -> bytearray:
        """
        turn row (object) into bytes
        """

        serialized = bytearray(ROW_SIZE)
        ser_id = row.identifier.to_bytes(ID_SIZE, sys.byteorder)
        # strings needs to be encoded
        ser_body = bytes(str(row.body), "utf-8")
        if len(ser_body) > BODY_SIZE:
            raise ValueError("row serialization failed; body too long")

        serialized[ID_OFFSET: ID_OFFSET + ID_SIZE] = ser_id
        serialized[BODY_OFFSET: BODY_OFFSET + len(ser_body)] = ser_body
        return serialized

    @staticmethod
    def deserialize(row_bytes: bytes):
        """

        :param row_bytes:
        :return:
        """
        # read bytes corresponding to columns
        id_bstr = row_bytes[ID_OFFSET: ID_OFFSET + ID_SIZE]
        body_bstr = row_bytes[BODY_OFFSET: BODY_OFFSET + BODY_SIZE]

        # this will need to be revisited when handling other data types
        id_val = int.from_bytes(id_bstr, sys.byteorder)
        # not sure if stripping nulls is valid (for other datatypes)
        body_val = body_bstr.rstrip(b'\x00')
        body_val = body_val.decode('utf-8')
        return Row(id_val, body_val)
