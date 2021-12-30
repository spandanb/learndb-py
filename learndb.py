from __future__ import annotations
"""
Python prototype/reference implementation
"""
import os.path
import sys
import logging

from typing import Union, List, Any
from dataclasses import dataclass
from enum import Enum, auto
from random import randint  # for testing
from pipe import Pipe

from constants import DB_FILE, USAGE, EXIT_SUCCESS, EXIT_FAILURE
from dataexchange import Response, Row, MetaCommandResult, ExecuteResult, PrepareResult
from pager import Pager
from statemanager import StateManager

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


def do_meta_command(command: str, virtual_machine) -> Response:
    state_manager = virtual_machine.state_manager
    if command == ".quit":
        state_manager.close()
        # reconsider exiting thus
        sys.exit(EXIT_SUCCESS)
    elif command == ".btree":
        print("Printing tree" + "-"*50)
        state_manager.print_tree()
        print("Finished printing tree" + "-"*50)
        return Response(True, status=MetaCommandResult.Success)
    elif command == ".validate":
        # TODO: get table name
        print("Validating tree....")
        state_manager.validate_tree()
        print("Validation succeeded.......")
        return Response(True, status=MetaCommandResult.Success)
    elif command == ".nuke":
        # NB: doesn't work; the file is in use
        os.remove(DB_FILE)
    elif command == ".help":
        print(USAGE)
        return Response(True, status=MetaCommandResult.Success)
    return Response(False, status=MetaCommandResult.UnrecognizedCommand)


def prepare_statement(command) -> Response:
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


def execute_statement(program: Program, virtmachine: VirtualMachine) -> Response:
    """
    execute statement;
    returns return value of child-invocation
    """
    print("In execute_statement; ")
    resp = virtmachine.run(program)
    return Response(True)


def input_handler(input_buffer: str, virtmachine: VirtualMachine):
    """
    handle input

    The API needs to be cleaned up; but crucially, there
    are 3 entities- input_buffer (user intention), table (state), vm (stateless compute)

    :param input_buffer:
    :param table: This should be something like context, or database the entity, i.e. an embodiment of state
    :return:
    """
    if is_meta_command(input_buffer):
        m_resp = do_meta_command(input_buffer, virtmachine)
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
    e_resp = execute_statement(program, virtmachine)
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

    # create state manager
    state_manager = StateManager(DB_FILE)

    # create virtual machine
    # output pipe
    pipe = Pipe()
    virtmachine = VirtualMachine(state_manager, pipe)

    while True:
        input_buffer = input("db > ")
        input_handler(input_buffer, virtmachine)
        if pipe.has_msgs():
            print(pipe.read())


def devloop():
    """
    this works through the entire intialize process
    :return:
    """
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    # create state manager
    state_manager = StateManager(DB_FILE)

    # output pipe
    pipe = Pipe()

    # config logger
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s ] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    # create virtual machine
    virtmachine = VirtualMachine(state_manager, pipe)

    # create table
    cmds0 = [
        "create table foo ( colA integer primary key, colB text)",
        # "select pkey, root_pagenum from catalog",
        #"insert into foo (colA, colB) values (0, 'hellew words')",
        "insert into foo (colA, colB) values (1, 'hellew words')",
        "insert into foo (colA, colB) values (89, 'hellew words foo')",
        "insert into foo (colA, colB) values (90, 'hellew words foo')",
        "insert into foo (colA, colB) values (91, 'hellew words foo')",
        "insert into foo (colA, colB) values (92, 'hellew words foo')",
        "insert into foo (colA, colB) values (4, 'hellew words foo')",
        "insert into foo (colA, colB) values (2, 'hellew words foo')",
        "select colA, colB  from foo"
    ]

    cmds = [
        "create table foo ( colA integer primary key, colB text)",
    ]
    # create random records
    keys = list(set(randint(1, 1000) for i in range(100)))
    # keys = [625, 582, 200, 301, 40, 354, 228, 797, 90, 245]
    # keys = [236, 301, 602, 522, 449, 742, 252, 333, 768, 261, 619, 87, 854, 851, 332, 360]

    for key in keys:
        cmds.append(f"insert into foo (colA, colB) values ({key}, 'hellew words foo')")

    cmds.append("select colA, colB  from foo")

    result_keys = []

    for cmd in cmds:
        logging.info(f"handling [{cmd}]")
        p_resp = prepare_statement(cmd)
        if not p_resp.success:
            print(f"failure due to {p_resp.error_message}")
            return EXIT_FAILURE

        virtmachine.run(p_resp.body)
        # state_manager.print_tree("foo")

        # print anything in the output buffer
        while pipe.has_msgs():
            record = pipe.read()
            key = record.get("cola")
            print(f'pipe read: {record}')
            result_keys.append(key)

        state_manager.validate_tree("foo")
        state_manager.print_tree("foo")
        print('*'*100)

    assert result_keys == [k for k in sorted(keys)], f"result {result_keys} doesn't not match {[k for k in sorted(keys)]}"

    # close statemanager to push changed state to disk
    state_manager.close()


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
python learndb.py file <filepath>
    // read file at <filepath>
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
    elif runmode == 'file':
        pass
    else:
        print(f"Error: Invalid run mode [{runmode}]")
        print(args_description)
        return


if __name__ == '__main__':
    parse_args_and_start(sys.argv[1:])
