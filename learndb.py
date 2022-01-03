from __future__ import annotations
"""
Python prototype/reference implementation
"""
import os.path
import math  # for testing
import sys
import random
import logging
import itertools  # for testing

from typing import Union, List, Any
from dataclasses import dataclass
from enum import Enum, auto
from random import randint, shuffle  # for testing
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

    todo: I think this can be nuked

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

    while True:
        input_buffer = input("db > ")
        db.handle_input(input_buffer)

        # get output pipe
        pipe = db.get_pipe()

        while pipe.has_msgs():
            print(pipe.read())


def devloop():
    """
    this works through the entire intialize process
    :return:
    """

    test_cases = [
        [1, 2, 3, 4],
        [64, 5, 13, 82],
        [82, 13, 5, 2, 0],
        [10, 20, 30, 40, 50, 60, 70],
        [72, 79, 96, 38, 47],
        [432, 507, 311, 35, 246, 950, 956, 929, 769, 744, 994, 438],
        [159, 597, 520, 189, 822, 725, 504, 397, 218, 134, 516],
        [159, 597, 520, 189, 822, 725, 504, 397],
        [960, 267, 947, 400, 795, 327, 464, 884, 667, 870, 92],
        [793, 651, 165, 282, 177, 439, 593],
        [229, 653, 248, 298, 801, 947, 63, 619, 475, 422, 856, 57, 38],
        [103, 394, 484, 380, 834, 677, 604, 611, 952, 71, 568, 291, 433, 305],
        [114, 464, 55, 450, 729, 646, 95, 649, 59, 412, 546, 340, 667, 274, 477, 363, 333, 897, 772, 508, 182,
        305, 428,
        180, 22],
        [15, 382, 653, 668, 139, 70, 828, 17, 891, 121, 175, 642, 491, 281, 920],
        [967, 163, 791, 938, 939, 196, 104, 465, 886, 355, 58, 251, 928, 758, 535, 737, 357, 125, 171, 838,
        572, 745,
        999, 417, 393, 458, 292, 904, 158, 286, 900, 859, 668, 183],
        [726, 361, 583, 121, 908, 789, 842, 67, 871, 461, 522, 394, 225, 637, 792, 393, 656, 748, 39, 696],
        [54, 142, 440, 783, 619, 273, 95, 961, 692, 369, 447, 825, 555, 908, 483, 356, 40, 110, 519, 599],
        [413, 748, 452, 666, 956, 926, 94, 813, 245, 237, 264, 709, 706, 872, 535, 214, 561, 882, 646]
    ]

    # basic
    #insert_keys = [967, 163, 791, 938, 939, 196, 104, 465, 886, 355, 58, 251, 928, 758, 535, 737, 357, 125, 171, 838, 572, 745, 999, 417, 393, 458, 292, 904, 158, 286, 900, 859, 668, 183]
    #del_keys =    [967, 163, 791, 938, 939, 196, 104, 465, 886, 355, 58, 251, 928, 758, 535, 737, 357, 125, 171, 838, 572, 745, 999, 417, 393, 458, 292, 904, 158, 286, 859, 668, 183, 900]
    #inner_devloop(insert_keys, del_keys)

    # stress
    for test_case in test_cases:

        insert_keys = test_case
        # del_keys = test_case[:]

        # there is a large number of perms ~O(n!)
        # and they are generated in a predictable order
        # we'll skip based on fixed step- later, this too should be randomized
        num_perms = 4
        total_perms = math.factorial(len(insert_keys))
        del_perms = []

        step_size = min(total_perms//num_perms, 10)
        # iterator over permutations
        perm_iter = itertools.permutations(insert_keys)

        while len(del_perms) < num_perms:
            for _ in range(step_size-1):
                # skip n-1 deletes
                next(perm_iter)
            del_perms.append(next(perm_iter))

        for del_keys in del_perms:
            try:
                inner_devloop(insert_keys, del_keys)
            except Exception as e:
                logging.error(f'Inner devloop failed on: {insert_keys} {del_keys} with {e}')
                raise


def inner_devloop(insert_keys, del_keys):
    """
    perform some ops/validations
    
    :param db: 
    :param insert_keys: 
    :param del_keys: 
    :return: 
    """
    
    db = LearnDB(DB_FILE)
    db.nuke_dbfile()

    print(f'running test case: {insert_keys} {del_keys}')

    # random.shuffle(del_keys)
    cmd = "create table foo ( colA integer primary key, colB text)"
    logging.info(f"handling [{cmd}]")
    resp = db.handle_input(cmd)

    # insert
    for key in insert_keys:
        cmd = f"insert into foo (colA, colB) values ({key}, 'hellew words foo')"
        logging.info(f"handling [{cmd}]")
        resp = db.handle_input(cmd)

    logging.debug("printing tree.......................")
    db.state_manager.print_tree("foo")

    # delete and validate
    for idx, key in enumerate(del_keys):
        cmd = f"delete from foo where colA = {key}"
        logging.info(f"handling [{cmd}]")
        resp = db.handle_input(cmd)
        if not resp.success:
            print(f"cmd {cmd} failed with {resp.status} {resp.error_message}")
            return EXIT_SUCCESS

        resp = db.handle_input("select cola, colb from foo")
        assert resp.success

        # output pipe
        pipe = db.get_pipe()

        result_keys = []
        # print anything in the output buffer
        logging.debug(f'pipe has msgs: {pipe.has_msgs()}')
        while pipe.has_msgs():
            record = pipe.read()
            key = record.get("cola")
            print(f'pipe read: {record}')
            result_keys.append(key)

        # assert result_keys == [k for k in sorted(keys)], f"result {result_keys} doesn't not match {[k for k in sorted(keys)]}"

        logging.debug("printing tree.......................")
        db.state_manager.print_tree("foo")
        # ensure tree is valid
        db.state_manager.validate_tree("foo")

        # check if all keys we expect are there in result
        expected = [key for key in sorted(del_keys[idx+1:])]
        actual = [key for key in sorted(set(result_keys))]
        assert actual == expected, f"expected: {expected}; received {actual}"

        print('*'*100)

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
