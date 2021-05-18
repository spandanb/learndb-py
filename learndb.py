"""
Python prototype/reference implementation
"""
import sys

from dataclasses import dataclass
from enum import Enum, auto

# section: constants

EXIT_SUCCESS = 0
PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100

NEXT_ROW_INDEX = 1

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


def next_row():
    """
    helper method; should be nuked eventually
    """
    global NEXT_ROW_INDEX
    row = Row(NEXT_ROW_INDEX, "hello database")
    NEXT_ROW_INDEX += 1
    return row


@dataclass
class Statement:
    statement_type: StatementType
    row_to_insert: Row

# section: helpers

# section : table

class Table:
    def __init__(self, num_rows: int = 0):
        self.num_rows = num_rows
        self.pages = [None for _ in range(TABLE_MAX_PAGES)]

# section: core logic

def is_meta_command(command: str) -> bool:
    return command[0] == '.'


def do_meta_command(command: str) -> MetaCommandResult:
    if command == ".quit":
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


def write_to_page(table: Table, row_num: int, serialized: bytes):
    """
    This handles the equivalent of getting page/row location of
    `row_slot` and serialization logic of `serialize_row`
    """
    # determine which page
    page_num = row_num // ROWS_PER_PAGE
    if table.pages[page_num] is None:
        table.pages[page_num] = bytearray(PAGE_SIZE)

    page = table.pages[page_num]
    row_offset = row_num % ROWS_PER_PAGE
    byte_offset = row_offset * ROW_SIZE
    page[byte_offset: byte_offset + ROW_SIZE] = serialized


def serialize_row(row: Row) -> bytearray:
    """
    turn row (object) into bytes
    Unlike in c, where the destination is passed via a pointer
    there is no way to do this in python. Here I will only serialize
    the row to a bytes object. The caller will handle insertion
    """

    serialized = bytearray(ROW_SIZE)
    # truncate the values so they fit into the allocated space per row
    serialized[ID_OFFSET: ID_SIZE] = bytes(str(row.identifier)[:ID_SIZE], "utf-8")
    serialized[BODY_OFFSET: BODY_SIZE] = bytes(str(row.body)[:BODY_SIZE], "utf-8")
    return serialized


def deserialize_row(table, row_num) -> Row:
    """
    deserialize row
    the deser logic will
    """
    page_num = row_num // ROWS_PER_PAGE
    page = table.pages[page_num]
    row_offset = row_num % ROWS_PER_PAGE
    byte_offset = row_offset * ROW_SIZE
    id_bstr = page[byte_offset + ID_OFFSET: byte_offset + ID_OFFSET + ID_SIZE]
    body_bstr = page[byte_offset + BODY_OFFSET: byte_offset + BODY_OFFSET + BODY_SIZE]

    # this will need to be revisited when handling other
    # not sure if stripping nulls is valid
    # will depend on if
    id_val = id_bstr.rstrip(b'\x00')  # remove trailing nulls
    id_val = id_val.decode('utf-8')
    id_val = int(id_val)
    body_val = body_bstr.rstrip(b'\x00')
    body_val = body_val.decode('utf-8')
    return Row(id_val, body_val)


def execute_insert(statement: Statement, table: Table) -> ExecuteResult:
    print("executing insert...")
    if table.num_rows >= TABLE_MAX_PAGES:
        return ExecuteResult.TableFull

    row_to_insert = statement.row_to_insert

    # this logic is different from tutorial
    # since in c, I can pass a pointer to an arbitrary mem location, i.e. to the
    # middle of a buffer, I can only pass refs to objects.

    # serialized bytes
    serialized = serialize_row(row_to_insert)
    row_num = table.num_rows
    table.num_rows += 1

    write_to_page(table, row_num, serialized)
    return ExecuteResult.Success


def execute_select(table: Table):
    print("executing select...")
    for row_num in range(table.num_rows):
        print(deserialize_row(table, row_num))


def execute_statement(statement: Statement, table: Table):
    """
    execute statement
    """
    match statement.statement_type:
        case StatementType.Select:
            execute_select(table)
        case StatementType.Insert:
            execute_insert(statement, table)


def main():

    table = Table()
    while True:
        input_buffer = input("db > ")
        if is_meta_command(input_buffer):
            match do_meta_command(input_buffer):
                case MetaCommandResult.Success:
                    continue
                case MetaCommandResult.UnrecognizedCommand:
                    print("Unrecognized meta command")
                    continue

        statement = Statement(StatementType.Uninitialized, next_row())
        match prepare_statement(input_buffer, statement):
            case PrepareResult.Success:
                # will execute below
                pass
            case PrepareResult.UnrecognizedStatement:
                print(f"Unrecognized keyword at start of '{input_buffer}'")
                continue

        # handle non-meta command
        execute_statement(statement, table)
        print(f"Executed command '{input_buffer}'")


def test():
    table = Table()
    statement = Statement(StatementType.Insert, next_row())
    execute_statement(statement, table)
    statement = Statement(StatementType.Insert, next_row())
    execute_statement(statement, table)
    statement = Statement(StatementType.Select, None)
    execute_statement(statement, table)

if __name__ == '__main__':
    main()
    # test()
