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


def input_handler(input_buffer: str, virtmachine: VirtualMachine) -> Response:
    """
    receive input, parse input, and execute vm.

    :param input_buffer:
    :param virtmachine:
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
        return Response(False, status={p_resp.status}, error_message=p_resp.error_message)

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


class LearnDB:
    """
    This encapsulates functionality over core db functions-
    exposed via a thin wrapper.
    """

    def __init__(self, db_filepath: str):
        """
        pass
        """
        self.db_filepath = db_filepath
        self.state_manager = None
        self.pipe = None
        self.virtual_machine = None
        self.reset()

    def reset(self):
        self.state_manager = StateManager(self.db_filepath)
        self.pipe = Pipe()
        self.virtual_machine = VirtualMachine(self.state_manager, self.pipe)

    def nuke_dbfile(self):
        """
        remove db file; requires state_manager be shutdown, since it
        holds a ref to file
        :return:
        """
        self.close()
        if os.path.exists(self.db_filepath):
            os.remove(self.db_filepath)
        self.reset()

    def get_pipe(self) -> Pipe:
        return self.pipe

    def close(self):
        """
        must be called before exiting, to persist data to disk
        :return:
        """
        self.state_manager.close()

    def handle_input(self, input_buffer: str) -> Response:
        """
        handle input- parse and execute

        :param input_buffer:
        :return:
        """
        return input_handler(input_buffer, self.virtual_machine)


def repl():
    """
    repl
    """

    # create db client
    db = LearnDB(DB_FILE)

    # get output pipe
    pipe = db.get_pipe()

    while True:
        input_buffer = input("db > ")
        db.handle_input(input_buffer)
        if pipe.has_msgs():
            print(pipe.read())


def devloop():
    """
    this works through the entire intialize process
    :return:
    """

    db = LearnDB(DB_FILE)
    db.nuke_dbfile()

    # output pipe
    pipe = db.get_pipe()

    # create random records
    keys = list(set(randint(1, 1000) for i in range(50)))
    # keys = [0, 1, 89, 90, 92, 4, 2]
    # keys = [625, 582, 200, 301, 40, 354, 228, 797, 90, 245]
    # keys = [236, 301, 602, 522, 449, 742, 252, 333, 768, 261, 619, 87, 854, 851, 332, 360]

    cmds = [
        "create table foo ( colA integer primary key, colB text)",
    ]

    for key in keys:
        cmds.append(f"insert into foo (colA, colB) values ({key}, 'hellew words foo')")

    cmds.append("select colA, colB  from foo")

    result_keys = []

    for cmd in cmds:
        logging.info(f"handling [{cmd}]")
        db.handle_input(cmd)

        # print anything in the output buffer
        while pipe.has_msgs():
            record = pipe.read()
            key = record.get("cola")
            print(f'pipe read: {record}')
            result_keys.append(key)

        db.state_manager.validate_tree("foo")
        db.state_manager.print_tree("foo")
        print('*'*100)

    assert result_keys == [k for k in sorted(keys)], f"result {result_keys} doesn't not match {[k for k in sorted(keys)]}"

    # close db to push changed state to disk
    db.close()


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
    # config logger
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s ] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    parse_args_and_start(sys.argv[1:])
