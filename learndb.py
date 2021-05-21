from __future__ import annotations
"""
Python prototype/reference implementation
"""
import os.path
import sys

from dataclasses import dataclass
from enum import Enum, auto

# section: constants

EXIT_SUCCESS = 0
EXIT_FAILURE = 1

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100

DB_FILE = 'db.file'

NEXT_ROW_INDEX = 1  # for testing

# constants for serialized data
ID_SIZE = 6 # length in bytes
BODY_SIZE = 122
ROW_SIZE = ID_SIZE + BODY_SIZE
ID_OFFSET = 0
BODY_OFFSET = ID_OFFSET + ID_SIZE
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE

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
    # initialize table row count to file row count
    table.num_rows = pager.rows_in_file
    return table


def db_close(table: Table):
    """
    this calls the pager `close`
    """
    table.pager.close(table.num_rows)


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
        # needed so table can be initialized correctly
        self.rows_in_file = 0
        self.open_file()

    def open_file(self):
        """
        open database file
        """
        # open binary file such that it is readable and not truncated
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
        self.rows_in_file = self.file_length // ROW_SIZE

        # warm up page cache, i.e. load data into memory
        # to load data, seek to beginning of file
        self.fileptr.seek(0)
        full_page_count = self.rows_in_file // ROWS_PER_PAGE
        for page_num in range(full_page_count):
            self.get_page(page_num)
        # if there is a partial page at the end, load it
        if self.rows_in_file % ROWS_PER_PAGE != 0:
            self.get_page(full_page_count)

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

            # determine number of whole pages in file
            num_pages = self.file_length // PAGE_SIZE
            if self.file_length % PAGE_SIZE != 0:
                num_pages += 1

            if page_num <= num_pages:
                # this page exists on file, load from file
                # into `page`
                # this looks abnormal - because read will presumably
                # return a binary buffer; but this is c does, and
                # leaving it for now
                read_page = self.fileptr.read(PAGE_SIZE)
                page[:PAGE_SIZE] = read_page

            self.pages[page_num] = page

        return self.pages[page_num]

    def close(self, num_rows: int):
        """
        `num_rows` is the number of rows in table

        this contains all the cleanup and saving logic.
        The rust code will be closer to this than the C,
        since the impl will contain interconnected methods
        """
        # this is 0-based
        num_full_pages = num_rows // ROWS_PER_PAGE
        for page_num in range(num_full_pages):
            if self.pages[page_num] is None:
                continue
            self.flush_page(page_num, PAGE_SIZE)

        # the tutorial flushes a partial page
        # simplifying and flushing the full page
        if num_rows % ROWS_PER_PAGE != 0:
            self.flush_page(num_full_pages, PAGE_SIZE)

    def flush_page(self, page_num: int, size: int):
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
        to_write = self.pages[page_num][:size]
        self.fileptr.write(to_write)



class Cursor:
    """
    Represents a cursor. A cursor understands
    how to traverse the table and how to insert, and remove
    rows from a table.
    """
    def __init__(self, table: Table, row_num: int):
        self.table = table
        # this corresponds to the current row being pointed to
        # the
        self.row_num = row_num
        self.end_of_table = table.num_rows == 0

    @classmethod
    def table_start(cls, table: Table) -> Cursor:
        """
        :return: cursor pointing to beginning of table
        """
        return cls(table, 0)

    @classmethod
    def table_end(cls, table: Table) -> Cursor:
        """
        initialize cursor at end of table
        :param table:
        :return:
        """
        return cls(table, table.num_rows)

    def get_row(self) -> Row:
        """
        return row pointed by cursor
        :return:
        """
        return self.table.deserialize_row(self.row_num)

    def insert_row(self, row: Row):
        """
        insert row to location pointed by cursor
        :return:
        """
        # row_num = table.num_rows

        self.table.serialize_row(row, self.row_num)
        self.table.num_rows += 1

    def advance(self):
        """
        advance the cursor
        :return:
        """
        self.row_num += 1
        if self.row_num == self.table.num_rows:
            self.end_of_table = True


class Table:
    """
    Currently `Table` interface is around (de)ser given a row number.
    Table interacts with pager. Ultimately, the table should
    represent the logical-relation-entity, and access to the pager, i.e. the storage
    layer should be done via an Engine, that acts as the storage layer access for
    all tables.
    """
    def __init__(self, pager: Pager):
        self.pager = pager
        self.num_rows = pager.rows_in_file

    def serialize_row(self, row: Row, row_num: int):
        """
        serialize a `row` and write it to local cache.
        """
        serialized = self.serialize(row)

        page_num = row_num // ROWS_PER_PAGE
        page = self.pager.get_page(page_num)

        row_offset = row_num % ROWS_PER_PAGE
        byte_offset = row_offset * ROW_SIZE
        page[byte_offset: byte_offset + ROW_SIZE] = serialized

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

    def deserialize_row(self, row_num: int) -> Row:
        """
        deserialize row given a `row_num`
        the deser logic interfaces with page
        """
        page_num = row_num // ROWS_PER_PAGE
        page = self.pager.get_page(page_num)
        row_offset = row_num % ROWS_PER_PAGE
        byte_offset = row_offset * ROW_SIZE

        # read bytes corresponding to columns
        id_bstr = page[byte_offset + ID_OFFSET: byte_offset + ID_OFFSET + ID_SIZE]
        body_bstr = page[byte_offset + BODY_OFFSET: byte_offset + BODY_OFFSET + BODY_SIZE]

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
    if table.num_rows >= TABLE_MAX_PAGES:
        return ExecuteResult.TableFull

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
    table = db_open(DB_FILE)
    #input_handler('insert', table)
    input_handler('select', table)
    input_handler('.quit', table)


if __name__ == '__main__':
    main()
    # test()
