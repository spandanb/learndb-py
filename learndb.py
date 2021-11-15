from __future__ import annotations
"""
Python prototype/reference implementation
"""
import os.path
import sys

from typing import Union, List, Any
from dataclasses import dataclass
from enum import Enum, auto
from random import randint  # for testing
import traceback # for testing


from constants import DB_FILE, USAGE, EXIT_SUCCESS
from database import Database
from datatypes import Response, Row, MetaCommandResult, ExecuteResult, PrepareResult
from pager import Pager

from lang_parser.sqlhandler import SqlFrontEnd
from lang_parser.symbols import Program

from btree import Tree, TreeInsertResult, TreeDeleteResult, NodeType, INTERNAL_NODE_MAX_CELLS
from virtual_machine import VirtualMachine

# section: parsing logic


def parse_insert(command: str) -> Row:
    """
    parse insert statement, formatted like: insert `key`
    :param command:
    :return: row to insert
    """
    tokens = command.split(" ")
    # for now, can only handle commands of the form: insert 3
    assert len(tokens) == 2
    key = int(tokens[1])
    return Row(key, "hello database")


def parse_delete(command: str) -> int:
    """
    parse delete statement, formatted like: delete `key`
    :param command:
    :return: key to delete
    """
    tokens = command.split(" ")
    # for now, can only handle commands of the form: insert 3
    assert len(tokens) == 2
    key = int(tokens[1])
    return key


def validate_existence(rows: List[Row], expected_keys: List[int]):
    """
    check `rows` match `expected_keys`
    NB: This implements a slow N^2 search
    :param rows:
    :param expected_keys:
    :return:
    """
    for row in rows:
        assert row.identifier in expected_keys, f"key [{row.identifier}] not expected"

    for key in expected_keys:
        found = False
        for row in rows:
            if row.identifier == key:
                found = True
                break
        assert found is True, f"key [{key}] not found"

    assert len(rows) == len(expected_keys), f"number of rows [{len(rows)}] != expected_keys [{len(expected_keys)}]"


# section: core execution/user-interface logic

def is_meta_command(command: str) -> bool:
    return command[0] == '.'


def do_meta_command(command: str, database: Database) -> Response:
    if command == ".quit":
        database.db_close()
        # reconsider exiting thus
        sys.exit(EXIT_SUCCESS)
    elif command == ".btree":
        print("Printing tree" + "-"*50)
        database.print_tree()
        print("Finished printing tree" + "-"*50)
        return Response(True, status=MetaCommandResult.Success)
    elif command == ".validate":
        print("Validating tree....")
        database.validate_tree()
        print("Validation succeeded.......")
        return Response(True, status=MetaCommandResult.Success)
    elif command == ".nuke":
        # NB: doesn't work; the file is in use
        os.remove(DB_FILE)
    elif command == ".help":
        print(USAGE)
        return Response(True, status=MetaCommandResult.Success)
    return Response(False, status=MetaCommandResult.UnrecognizedCommand)


def prepare_statement(command)-> Response:
    """
    prepare statement, i.e. parse statement and
    return it's AST. For now the AST structure is the prepared
    statement. This may change, e.g. if frontend changes to output bytecode

    :param command:
    :return:
    """
    parser = SqlFrontEnd()
    parser.parse(command)
    if not parser.is_success():
        return Response(False, error_message=f"parse failed due to: [{parser.error_summary()}]")
    return Response(True, body=parser.get_parsed())


def execute_statement(program: Program, database: Database, virtmachine: VirtualMachine) -> Response:
    """
    execute statement;
    returns return value of child-invocation
    """
    print("In execute_statement; ")
    resp = virtmachine.run(program, database)
    return Response(True)


def input_handler(input_buffer: str, database: Database, virtmachine: VirtualMachine):
    """
    handle input

    The API needs to be cleaned up; but crucially, there
    are 3 entities- input_buffer (user intention), table (state), vm (stateless compute)

    :param input_buffer:
    :param table: This should be something like context, or database the entity, i.e. an embodiment of state
    :return:
    """
    if is_meta_command(input_buffer):
        m_resp = do_meta_command(input_buffer)
        if m_resp.success == MetaCommandResult.Success:
            return Response(True, status=MetaCommandResult.Success)

        elif m_resp == MetaCommandResult.UnrecognizedCommand:
            print("Unrecognized meta command")
            return Response(False, status=MetaCommandResult.UnrecognizedCommand)

    p_resp = prepare_statement(input_buffer)
    if not p_resp.success:
        if p_resp.status == PrepareResult.UnrecognizedStatement:
            print(f"Unrecognized keyword at start of '{input_buffer}'")
            return Response(False, status=PrepareResult.UnrecognizedStatement)

    # handle non-meta command
    # execute statement can be handled by the interpreter
    program = p_resp.body
    e_resp = execute_statement(program, database, virtmachine)
    if e_resp.success:
        print(f"Execution of command '{input_buffer}' succeeded")
        return Response(True, body=e_resp.body)
    else:
        print(f"Execution of command '{input_buffer}' failed")
        return Response(False, error_message=e_resp.error_message)


def repl():
    """
    repl
    """
    table = db_open(DB_FILE)
    virtmachine = VirtualMachine()
    while True:
        input_buffer = input("db > ")
        input_handler(input_buffer, table, virtmachine)


def devloop():
    database = Database(DB_FILE)
    database.db_open()
    virtmachine = VirtualMachine()
    input_handler("select foo from bar", database, virtmachine)
    # input_handler("create table foo (colA text , colB text); select bar from foo", database, virtmachine)

    # handler = SqlFrontEnd()
    # handler.parse()


def devloop_old():
    """
    inner dev-loop
    :return:
    """
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    table = db_open(DB_FILE)

    # insert
    # keys = [72, 79, 96, 38, 47, 99, 1090, 876, 4]
    # keys = [1,2,3,4]
    # keys = [1,2,3,4,5,6]
    # keys = [64, 5, 13, 82]
    #keys = [13, 5, 2, 0]
    # keys = [4,3,2,1]
    # keys = [10, 20, 30, 40, 50, 60, 70]
    # keys = [432, 507, 311, 35, 246, 950, 956, 929, 769, 744, 994, 438]
    # keys = [114, 464, 55, 450, 729, 646, 95, 649, 59, 412, 546, 340, 667, 274, 477, 363, 333, 897, 772, 508, 182, 305, 428, 180, 22]
    # keys = [82, 13, 5, 2, 0]
    # keys = [229, 653, 248, 298, 801, 947, 63, 619, 475, 422, 856, 57, 38]
    keys = [726, 361, 583, 121, 908, 789, 842, 67, 871, 461, 522, 394, 225, 637, 792, 393, 656, 748, 39, 696]
    for key in keys:
        input_handler(f"insert {key}", table)

    input_handler('.btree', table)
    input_handler(".validate", table)

    # table.tree.validate_existence(keys)

    select = input_handler("select", table)

    # print(f'Number of keys inserted: {len(keys)}; select returned: {len(select.body)}')
    validate_existence(select.body, keys)

    # delete keys
    while keys:
        key = keys[0]
        remaining = keys = keys[1:]
        input_handler(f"delete {key}", table)
        input_handler('.btree', table)

        select = input_handler("select", table)
        print(" ")
        #print(f'select returned: {select.is_success} {select.result}')

        # validate all expected keys exist
        validate_existence(select.body, remaining)


    input_handler('.btree', table)
    input_handler('.quit', table)


def parse_args_and_start(args: list):
    """
    parse args and starts
    :return:
    """
    args_description = """Usage:
python learndb.py repl
    // start repl
python learndb.py devloop
    // start a dev-loop function
    """
    if len(args) < 1:
        print(f"Error: run-mode not specified")
        print(args_description)
        return

    runmode = args[0].lower()
    if runmode == 'repl':
        repl()
    elif runmode == 'devloop':
        devloop()
    else:
        print(f"Error: Invalid run mode [{runmode}]")
        print(args_description)
        return


if __name__ == '__main__':
    parse_args_and_start(sys.argv[1:])
    # devloop()