from __future__ import annotations
"""
Python prototype/reference implementation
"""
import os.path
import sys

from typing import Union
from dataclasses import dataclass
from enum import Enum, auto

# section: constants

EXIT_SUCCESS = 0
EXIT_FAILURE = 1

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100

DB_FILE = 'db.file'

NEXT_ROW_INDEX = 1  # for testing

# serialized data layout (row)
ID_SIZE = 6 # length in bytes
BODY_SIZE = 58
ROW_SIZE = ID_SIZE + BODY_SIZE
ID_OFFSET = 0
BODY_OFFSET = ID_OFFSET + ID_SIZE
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE

# serialized data layout (tree nodes)
# common node header layout
NODE_TYPE_SIZE = 8
NODE_TYPE_OFFSET = 0
IS_ROOT_SIZE = 8
IS_ROOT_OFFSET = NODE_TYPE_SIZE
# NOTE: in c these are constants are defined based on width of system register
PARENT_POINTER_SIZE = 32
PARENT_POINTER_OFFSET = NODE_TYPE_SIZE + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

# leaf node header layout
LEAF_NODE_NUM_CELLS_SIZE = 32
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE

# leaf node body layout
LEAF_NODE_KEY_SIZE = 32
LEAF_NODE_KEY_OFFSET = 0
# NOTE: nodes should not cross the page boundary; thus ROW_SIZE is upper
# bounded by remaining space in page
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE


# section: enums

class MetaCommandResult(Enum):
    Success = auto()
    UnrecognizedCommand = auto()


class StatementType(Enum):
    Uninitialized = auto()
    Insert = auto()
    Select = auto()


class PrepareResult(Enum):
    Success = auto()
    UnrecognizedStatement = auto()


class ExecuteResult(Enum):
    Success = auto()
    TableFull = auto()



# section: classes/structs
@dataclass
class Row:
    """
    NOTE: this assumes a fixed table definition. Fixing the
    table definition, like in the tutorial to bootstrap the
    (de)serialize logic.
    Later when I can handle generic schemas this will need to be
    made generic
    """
    identifier : int
    body: str


@dataclass
class Statement:
    statement_type: StatementType
    row_to_insert: Row

# section: helpers

def next_row():
    """
    helper method - creates a simple `Row`
    should be nuked when I can handle generic row definitions
    """
    global NEXT_ROW_INDEX
    row = Row(NEXT_ROW_INDEX, "hello database")
    NEXT_ROW_INDEX += 1
    return row


# section : helper objects/functions, e.g. table, pager

def db_open(filename: str) -> Table:
    """
    opens connection to db, i.e. initializes
    table and pager.
    """
    pager = Pager.pager_open(filename)
    table = Table(pager)
    table.root_page_num = 0

    if pager.num_pages == 0:
        # new database file, initialize page 0 as leaf node
        root_node = pager.get_page(0)
        Tree.initialize_leaf_node(root_node)

    return table


def db_close(table: Table):
    """
    this calls the pager `close`
    """
    table.pager.close(table.pager.num_pages)


class Pager:
    """
    manager of pages in memory (cache)
    and on file
    """
    def __init__(self, filename):
        """
        filename is handled differently from tutorial
        since it passes a fileptr; here I'll manage the file
        with the `Pager` class
        """
        self.pages = [None for _ in range(TABLE_MAX_PAGES)]
        self.filename = filename
        self.fileptr = None
        self.file_length = 0
        self.num_pages = 0
        self.open_file()

    def open_file(self):
        """
        open database file
        """
        # open binary file such that: it is readable, not truncated(random),
        # create if not exists, writable(random)
        # a+b (and more generally any "a") mode can only write to end
        # of file; seeks only applies to read ops
        # r+b allows read and write, without truncation, but errors if
        # the file does not exist
        # NB: this sets the file ptr location to the end of the file
        try:
            self.fileptr = open(self.filename, "r+b")
        except FileNotFoundError:
            self.fileptr = open(self.filename, "w+b")
        self.file_length = os.path.getsize(self.filename)

        if self.file_length % PAGE_SIZE != 0:
            # avoiding exceptions since I want this to be closer to Rust, i.e panic or enum
            print("Db file is not a whole number of pages. Corrupt file.")
            sys.exit(EXIT_FAILURE)

        self.num_pages = self.file_length // PAGE_SIZE

        # warm up page cache, i.e. load data into memory
        # to load data, seek to beginning of file
        self.fileptr.seek(0)
        for page_num in range(self.num_pages):
            self.get_page(page_num)

    @classmethod
    def pager_open(cls, filename):
        """
        this does nothing - keeping it so code is aligned.
        C works with fd (ints), so you can
        open files and pass around an int. For python, I need to
        pass the file ref around.
        """
        return cls(filename)

    def get_page(self, page_num: int) -> bytearray:
        """
        get `page` given `page_num`
        """
        if page_num > TABLE_MAX_PAGES:
            print(f"Tried to fetch page out of bounds (max pages = {TABLE_MAX_PAGES})")
            sys.exit(EXIT_FAILURE)

        if self.pages[page_num] is None:
            # cache miss. Allocate memory and load from file.
            page = bytearray(PAGE_SIZE)

            # determine number of pages in file; there should only be complete pages
            num_pages = self.file_length // PAGE_SIZE
            if page_num < num_pages:
                # this page exists on file, load from file
                # into `page`
                self.fileptr.seek(page_num * PAGE_SIZE)
                read_page = self.fileptr.read(PAGE_SIZE)
                assert len(read_page) == PAGE_SIZE, "corrupt file: read page returned byte array smaller than page"
                page[:PAGE_SIZE] = read_page
            else:
                pass

            self.pages[page_num] = page

            # NOTE: the tutorial has the cond: `page_num >= pager->num_pages`
            # that seems wrong, since the page_num should only be incremented
            # if it's gre
            if page_num > self.num_pages:
                self.page_num += 1

        return self.pages[page_num]

    def close(self, num_pages: int):
        """
        close the connection i.e. flush pages to file
        """
        # this is 0-based
        # NOTE: not sure about this +1;
        for page_num in range(num_pages + 1):
            if self.pages[page_num] is None:
                continue
            self.flush_page(page_num)

    def flush_page(self, page_num: int):
        """
        flush/write page to file
        page_num is the page to write
        size is the number of bytes to write
        """
        if self.pages[page_num] is None:
            print("Tried to flush null page")
            sys.exit(EXIT_FAILURE)

        byte_offset = page_num * PAGE_SIZE
        self.fileptr.seek(byte_offset)
        to_write = self.pages[page_num]
        self.fileptr.write(to_write)



class Cursor:
    """
    Represents a cursor. A cursor understands
    how to traverse the table and how to insert, and remove
    rows from a table.
    """
    def __init__(self, table: Table, page_num: int, cell_num: int = 0):
        self.table = table
        self.page_num = page_num
        self.cell_num = cell_num
        num_cells = Tree.leaf_node_num_cells(table.pager.get_page(table.root_page_num))
        self.end_of_table = cell_num == num_cells

    @classmethod
    def table_start(cls, table: Table) -> Cursor:
        """
        :return: cursor pointing to beginning of table
        """
        return cls(table, 0, 0)

    @classmethod
    def table_end(cls, table: Table) -> Cursor:
        """
        initialize cursor at end of table
        :param table:
        :return:
        """
        root_node = table.pager.get_page(table.root_page_num)
        num_cells = Tree.leaf_node_num_cells(root_node)
        # currently this is only assuming a single node tree
        return cls(table, table.root_page_num, num_cells)

    def get_row(self) -> Row:
        """
        return row pointed by cursor
        :return:
        """
        node = self.table.pager.get_page(self.page_num)
        serialized = Tree.leaf_node_value(node, self.cell_num)
        return Table.deserialize(serialized)

    def insert_row(self, row: Row):
        """
        insert row to location pointed by cursor
        :return:
        """
        # self.table.serialize_row(row, self.row_num)
        node = self.table.pager.get_page(self.table.root_page_num)
        if Tree.leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS:
            print("Unable to insert; max leaf nodes reached")
            return

        # NOTE: `row.identifier` is the sort key for the btree
        Tree.leaf_node_insert(node, self.cell_num, row.identifier, row)

    def advance(self):
        """
        advance the cursor
        :return:
        """
        node = self.table.pager.get_page(self.page_num)
        self.cell_num += 1
        # consider caching RHS value
        if self.cell_num >= Tree.leaf_node_num_cells(node):
            self.end_of_table = True


class Tree:
    """
    collections of methods related to BTree
    Right now, this exposes a very low level API of reading/writing from bytes
    And other actors call the relevant methods.
    """
    @staticmethod
    def initialize_leaf_node(node: bytearray):
        Tree.write_leaf_node_num_cells(node, 0)

    @staticmethod
    def leaf_node_cell_offset(cell_num: int) -> int:
        """
        helper to calculate cell offset; this is the
        offset to the key for the given cell
        """
        return LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE

    @staticmethod
    def leaf_node_cell_value_offset(cell_num: int) -> int:
        """
        returns offset to value
        """
        return Tree.leaf_node_cell_offset(cell_num) + LEAF_NODE_KEY_SIZE

    @staticmethod
    def write_leaf_node_key(node: bytes, cell_num: int, key: int):
        offset = Tree.leaf_node_cell_offset(cell_num)
        value = key.to_bytes(LEAF_NODE_KEY_SIZE, sys.byteorder)
        node[offset: offset + LEAF_NODE_KEY_SIZE] = value

    @staticmethod
    def write_leaf_node_num_cells(node: bytearray, num_cells: int):
        """
        write num of node cells: encode to int
        """
        value = num_cells.to_bytes(LEAF_NODE_NUM_CELLS_SIZE, sys.byteorder)
        node[LEAF_NODE_NUM_CELLS_OFFSET: LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE] = value

    @staticmethod
    def write_leaf_node_value(node: bytes, cell_num: int, value: bytes):
        """
        :param node:
        :param cell_num:
        :param value:
        :return:
        """
        offset = Tree.leaf_node_cell_offset(cell_num) + LEAF_NODE_KEY_SIZE
        node[offset: offset + LEAF_NODE_VALUE_SIZE] = value

    @staticmethod
    def leaf_node_key(node: bytes, cell_num: int) -> int:
        offset = Tree.leaf_node_cell_offset(cell_num)
        bin_num = node[offset: offset + LEAF_NODE_KEY_SIZE]
        return int.from_bytes(bin_num, sys.byteorder)

    @staticmethod
    def leaf_node_num_cells(node: bytes) -> int:
        """
        `node` is exactly equal to a `page`. However,`node` is in the domain
        of the tree, while page is in the domain of storage.
        Using the same naming convention of `prop_name` for getter and `write_prop_name` for setter
        """
        bin_num = node[LEAF_NODE_NUM_CELLS_OFFSET: LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE]
        return int.from_bytes(bin_num, sys.byteorder)

    @staticmethod
    def leaf_node_value(node: bytes, cell_num: int) -> bytes:
        """
        :param node:
        :param cell_num: determines offset
        :return:
        """
        offset = Tree.leaf_node_cell_value_offset(cell_num)
        return node[offset: offset + LEAF_NODE_VALUE_SIZE]

    @staticmethod
    def leaf_node_insert(node, current_cell: int, key: int, value: Row):
        """

        :param node:
        :param current_cell: the current cell that the cursor is pointing to
        :param key:
        :param value:
        :return:
        """
        num_cells = Tree.leaf_node_num_cells(node)
        if num_cells >= LEAF_NODE_MAX_CELLS:
            # node full
            print("Need to implement node splitting")
            sys.exit(EXIT_FAILURE)

        if current_cell < num_cells:
            # make room for new cell
            # not sure if anything is needed
            pass

        Tree.write_leaf_node_num_cells(node, num_cells + 1)
        serialized = Table.serialize(value)
        Tree.write_leaf_node_value(node, current_cell, serialized)

    @staticmethod
    def print_leaf_node(node: bytes):
        num_cells = Tree.leaf_node_num_cells(node)
        print(f"leaf (size {num_cells})")
        for i in range(num_cells):
            key = Tree.leaf_node_key(node, i)
            print(f"{i} - {key}")



class Table:
    """
    Currently `Table` interface is around (de)ser given a row number.
    Ultimately, the table should
    represent the logical-relation-entity, and access to the pager, i.e. the storage
    layer should be done via an Engine, that acts as the storage layer access for
    all tables.
    """
    def __init__(self, pager: Pager):
        self.pager = pager
        self.root_page_num = 0

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

        :param byte_offset:
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

# section: core execution/user-interface logic

def is_meta_command(command: str) -> bool:
    return command[0] == '.'


def do_meta_command(command: str, table: Table) -> MetaCommandResult:
    if command == ".quit":
        db_close(table)
        sys.exit(EXIT_SUCCESS)
    elif command == ".nuke":
        os.remove(DB_FILE)
    return MetaCommandResult.UnrecognizedCommand


def prepare_statement(command: str, statement: Statement) -> PrepareResult:
    """
    prepare a statement
    :param command:
    :param statement: modify in-place to be similar to rust impl

    :return:
    """
    if command.startswith("insert"):
        statement.statement_type = StatementType.Insert
        return PrepareResult.Success
    elif command.startswith("select"):
        statement.statement_type = StatementType.Select
        return PrepareResult.Success
    return PrepareResult.UnrecognizedStatement


def execute_insert(statement: Statement, table: Table) -> ExecuteResult:
    print("executing insert...")
    cursor = Cursor.table_end(table)

    row_to_insert = statement.row_to_insert
    if row_to_insert is None:
        # TODO: nuke me
        row_to_insert = next_row()

    cursor.insert_row(row_to_insert)

    return ExecuteResult.Success


def execute_select(table: Table):
    # get cursor to start of table
    print("executing select...")

    cursor = Cursor.table_start(table)
    while cursor.end_of_table is False:
        print(cursor.get_row())
        cursor.advance()


def execute_statement(statement: Statement, table: Table):
    """
    execute statement
    """
    match statement.statement_type:
        case StatementType.Select:
            execute_select(table)
        case StatementType.Insert:
            execute_insert(statement, table)


def input_handler(input_buffer: str, table: Table):
    """
    handle input buffer; could contain command or meta command
    """
    if is_meta_command(input_buffer):
        match do_meta_command(input_buffer, table):
            case MetaCommandResult.Success:
                return
            case MetaCommandResult.UnrecognizedCommand:
                print("Unrecognized meta command")
                return

    statement = Statement(StatementType.Uninitialized, None)
    match prepare_statement(input_buffer, statement):
        case PrepareResult.Success:
            # will execute below
            pass
        case PrepareResult.UnrecognizedStatement:
            print(f"Unrecognized keyword at start of '{input_buffer}'")
            return

    # handle non-meta command
    execute_statement(statement, table)
    print(f"Executed command '{input_buffer}'")


def main():
    """
    repl
    """
    table = db_open(DB_FILE)
    while True:
        input_buffer = input("db > ")
        input_handler(input_buffer, table)


def test():
    # os.remove(DB_FILE)
    table = db_open(DB_FILE)
    #input_handler('insert', table)
    #input_handler('insert', table)
    input_handler('select', table)
    input_handler('.quit', table)


if __name__ == '__main__':
    # main()
    test()
