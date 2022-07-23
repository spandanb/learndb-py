from __future__ import annotations
"""
This module contains the highest level user-interaction and resource allocation
i.e. management of entities, like parser, virtual machine, pager, etc. that implement
the DBMS functionality that is learndb.
"""
import os
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

from constants import DB_FILE, USAGE, EXIT_SUCCESS, EXIT_FAILURE
from lang_parser.sqlhandler import SqlFrontEnd
from lang_parser.symbols import Program
from dataexchange import Response, MetaCommandResult, ExecuteResult, PrepareResult
from pipe import Pipe
from statemanager import StateManager
#from virtual_machine import VirtualMachine
from virtual_machine2 import VirtualMachine


# section: core execution/user-interface logic

class LearnDB:
    """
    This encapsulates functionality over core db functions-
    exposed via a thin wrapper.
    """

    def __init__(self, db_filepath: str, nuke_db_file: bool = False):
        """

        :param db_filepath: the db file
        :param nuke_db_file: whether to nuke the file before self is initialized
        """
        self.db_filepath = db_filepath
        # NOTE: the method
        if nuke_db_file and os.path.exists(self.db_filepath):
            os.remove(self.db_filepath)
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
        """
        NOTE: get pipe; pipes are recycled if LearnDB.reset is invoked
        :return:
        """
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
        return self.input_handler(input_buffer)

    @staticmethod
    def is_meta_command(command: str) -> bool:
        return command[0] == '.'

    def do_meta_command(self, command: str) -> Response:
        """
        handle execution of meta command
        :param command:
        :param db:
        :return:
        """
        if command == ".quit":
            print("goodbye")
            self.close()
            sys.exit(EXIT_SUCCESS)
        elif command.startswith(".btree"):
            # .btree expects table-name
            splits = command.split(" ")
            if len(splits) != 2:
                print("Invalid argument to .btree| Usage: > .btree <table-name>")
                return Response(False, status=MetaCommandResult.InvalidArgument)
            tree_name = splits[1]
            print("Printing tree" + "-"*50)
            self.state_manager.print_tree(tree_name)
            print("Finished printing tree" + "-"*50)
            return Response(True, status=MetaCommandResult.Success)
        elif command == ".validate":
            print("Validating tree....")
            splits = command.split(" ")
            if len(splits) != 2:
                print("Invalid argument to .validate| Usage: > .validate <table-name>")
                return Response(False, status=MetaCommandResult.InvalidArgument)
            tree_name = splits[1]
            self.state_manager.validate_tree(tree_name)
            print("Validation succeeded.......")
            return Response(True, status=MetaCommandResult.Success)
        elif command == ".nuke":
            self.nuke_dbfile()
        elif command == ".help":
            print(USAGE)
            return Response(True, status=MetaCommandResult.Success)
        return Response(False, status=MetaCommandResult.UnrecognizedCommand)

    @staticmethod
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

    def execute_statement(self, program: Program) -> Response:
        """
        execute statement;
        returns return value of child-invocation
        """
        # logging.info(f"In execute_statement; ")
        results = self.virtual_machine.run(program)
        return Response(True, body=results)

    def input_handler(self, input_buffer: str) -> Response:
        """
        receive input, parse input, and execute vm.

        :param input_buffer:
        :return:
        """
        if self.is_meta_command(input_buffer):
            m_resp = self.do_meta_command(input_buffer)
            if m_resp.success == MetaCommandResult.Success:
                return Response(True, status=MetaCommandResult.Success)

            elif m_resp == MetaCommandResult.UnrecognizedCommand:
                print("Unrecognized meta command")
                return Response(False, status=MetaCommandResult.UnrecognizedCommand)

        p_resp = self.prepare_statement(input_buffer)
        if not p_resp.success:
            if p_resp.status == PrepareResult.UnrecognizedStatement:
                print(f"Unrecognized keyword at start of '{input_buffer}'")
            return Response(False, status={p_resp.status}, error_message=p_resp.error_message)

        # handle non-meta command
        # execute statement can be handled by the interpreter
        program = p_resp.body
        e_resp = self.execute_statement(program)
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

    # create db client
    db = LearnDB(DB_FILE)

    print("Welcome to learndb")
    print("For help use .help")
    while True:
        input_buffer = input("db > ")
        db.handle_input(input_buffer)

        # get output pipe
        pipe = db.get_pipe()

        while pipe.has_msgs():
            print(pipe.read())


def devloop_add_del():
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
        num_perms = 1
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
        # cmd = f"delete from foo where colA = {key} AND colB = 'foo'"
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


def devloop_join():

    db = LearnDB(DB_FILE)
    db.nuke_dbfile()

    # create table
    db.handle_input("create table foo ( cola integer primary key, colb integer, colc integer)")
    db.handle_input("create table bar ( colx integer primary key, coly integer, colz integer)")
    # insert into table
    db.handle_input("insert into foo (cola, colb, colc) values (1, 2, 3)")
    db.handle_input("insert into foo (cola, colb, colc) values (2, 4, 6)")
    db.handle_input("insert into foo (cola, colb, colc) values (3, 10, 8)")
    db.handle_input("insert into bar (colx, coly, colz) values (101, 10, 80)")
    db.handle_input("insert into bar (colx, coly, colz) values (102, 4, 90)")
    # select
    db.handle_input("select b.colx, b.coly, b.colz from foo f join bar b on f.colb = b.coly")

    keys = []

    assert db.get_pipe().has_msgs()
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        # NOTE: records access API is different for Record and MultiRecord
        # keys.append(record.get("b", "colx"))
        rdict = record.to_dict()
        keys.append(rdict)
    # TODO: this is not working
    expected = [101, 102]
    assert keys == expected, f"expected {expected}; received {keys}"


def devloop_old():
    # TODO: before cleaning/nuking these, ensure that the debugging capabilities/flows
    # these expose, are kept somewhere, perhaps a standalone debug driver?
    # somewhere; or perhaps just kept in this file
    db = LearnDB(DB_FILE)
    db.nuke_dbfile()

    db.handle_input("create table foo ( cola integer primary key, colb integer, colc integer)")
    db.handle_input("create table bar ( colx integer primary key, coly integer, colz integer)")
    # db.handle_input("create table car ( colx integer primary key, coly integer, colz integer)")
    db.handle_input("insert into foo ( cola, colb, colc) values (1, 2, 3)")
    db.handle_input("insert into foo ( cola, colb, colc) values (2, 4, 6)")
    db.handle_input("insert into bar ( colx, coly, colz) values (30, 20, 40)")
    # db.handle_input("select cola, colb from foo where cola = 1 or colc = 2")
    # db.handle_input("select cola, colb from foo where cola = 1 and colc = 3 or colb = 2")
    # db.handle_input("select cola, colb from foo where cola = 1 and colc = 3 or colb = 2")
    # db.handle_input("select cola, colb from foo f inner join bar r on f.cola = r.coly")
    db.handle_input("select cola, colb from foo f inner join bar r on f.b = r.y inner join car c on c.x = f.b")
    #db.handle_input("select cola, colb from foo f cross join bar r on f.x = r.y")  # cross join should not have an on-clause
    #db.handle_input("select cola, colb from foo f left join bar r on f.x = r.y")

    assert db.get_pipe().has_msgs()

    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        # key = record.get("cola")
        print(f'pipe read: {record}')


def devloop():
    db = LearnDB(DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()

    text = "select cola from foo f cross join june"
    text = "select cola, colb from foo f left join bar r on fx = ry;"
    text = "select cola, colb from foo f left join bar b on x = 1 left join car c on y = 2 left join dar d on fx = ry;"
    text = "select cola, colb from foo f join bar b on x = 1 left join car c on y = 2 left join dar d on fx = ry;"
    text = "select cola, colb from foo f left join bar b on x = 1;"


    text = "create table foo (cola integer primary key, colb text)"
    text = "select cola from catalog"
    #text = "select cola from foo"
    text = "select cola from foo where cola > 5"
    #resp = db.handle_input(text)
    #db.close()
    #return

    texts = [
        "create table foo (cola integer primary key, colb text)",
        "insert into foo ( cola, colb) values (42, 'hello melo')",
        "insert into foo ( cola, colb) values (1, 'helo melo')",
        #"insert into foo ( cola, colb) values (43, 'lo melossss')",
        #"insert into foo ( cola, colb) values (4, 'hello melo')",
        #"insert into foo ( cola, colb) values (4002, 'heloPIESDS')",
        "insert into foo ( cola, colb) values (99, 'hello bobo')",
        #"delete from foo where cola > 99",
        # "delete from foo",
        #"select cola from foo where cola > 1 and cola < 100 or colb = 'hello'"
        "select cola from foo where cola = 1"
    ]

    texts = [
        "create table foo ( cola integer primary key, colb integer, colc integer)",
        "create table bar ( colx integer primary key, coly integer, colz integer)",
        "create table car ( colx integer primary key, coly integer, colz integer)",
        # insert into table
        #"insert into foo (cola, colb, colc) values (1, 2, 3)",
        #"insert into foo (cola, colb, colc) values (2, 4, 6)",
        "insert into foo (cola, colb, colc) values (3, 10, 8)",
        "insert into bar (colx, coly, colz) values (101, 10, 80)",
        "insert into bar (colx, coly, colz) values (102, 4, 90)",
        "insert into car (colx, coly, colz) values (101, 10, 89)",
        #"insert into car (colx, coly, colz) values (102, 5, 91)",
        # "select b.colx, b.coly, b.colz from foo f",
        "select b.colx, b.coly, b.colz from foo f join bar b on f.colb = b.coly",
        "select b.colx, b.coly, b.colz from foo f join bar b on f.colb = b.coly join car c on f.colb = c.coly",
    ]

    # inner join
    texts = [
        "create table foo ( cola integer primary key, colb integer, colc integer)",
        "create table bar ( colx integer primary key, coly integer, colz integer)",
        "insert into foo (cola, colb, colc) values (1, 2, 3)",
        "insert into foo (cola, colb, colc) values (2, 4, 6)",
        "insert into foo (cola, colb, colc) values (3, 10, 8)",
        "insert into bar (colx, coly, colz) values (101, 10, 80)",
        "insert into bar (colx, coly, colz) values (102, 4, 90)",
        # select
        "select b.colx, b.coly, b.colz from foo f join bar b on f.colb = b.coly",
        #"select cola from bar"
    ]

    # scoped select
    texts = [
        "create table foo ( cola integer primary key, colB integer, colc integer, cold integer)",
        "insert into foo (cola, colb, colc, cold) values (1, 2, 31, 4)",
        "insert into foo (cola, colb, colc, cold) values (2, 4, 6, 8)",
        "insert into foo (cola, colb, colc, cold) values (3, 10, 3, 8)",
        "select f.cola from foo f where f.colb = 4 AND f.colc = 6 OR f.colc = 3"
        #"select f.cola from foo f where f.colb = 4"
    ]

    # group by + having
    texts = [
        "create table items ( custid integer primary key, country integer)",
        "insert into items (custid, country) values (10, 1)",
        "insert into items (custid, country) values (20, 1)",
        "insert into items (custid, country) values (100, 2)",
        "insert into items (custid, country) values (200, 2)",
        "insert into items (custid, country) values (300, 2)",
        # "select f.cola from foo f group by f.colb, f.cola",
        #"select count(custid), country from items group by country",
        "select count(custid), country from items group by country having count(cust_id) > 1",
    ]

    # group by
    texts = [
        "create table items ( custid integer primary key, country integer)",
        "insert into items (custid, country) values (10, 1)",
        "insert into items (custid, country) values (20, 1)",
        "insert into items (custid, country) values (100, 2)",
        "insert into items (custid, country) values (200, 2)",
        "insert into items (custid, country) values (300, 2)",
        "select count(custid), country from items group by country",
        #"select count(*), country from items group by country", # TODO: this fails to parse
    ]


    for text in texts:
        logging.info(f"handling {text}")
        resp = db.handle_input(text)
        logging.info(f"received resp: {resp}")
        while db.pipe.has_msgs():
            logging.info("read from pipe: {}".format(db.pipe.read()))

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
    elif runmode == 'join_dloop':
        devloop_join()
    elif runmode == 'add_del_dloop':
        devloop_add_del()
    elif runmode == 'devloop':
        devloop()
    elif runmode == 'file':
        # todo support input and output file
        # if no output file, write to console
        pass
    else:
        print(f"Error: Invalid run mode [{runmode}]")
        print(args_description)
        return


if __name__ == '__main__':
    # config logger
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s ] %(message)s"
    # log to file
    # logging.basicConfig(format=FORMAT, level=logging.DEBUG, filename=os.path.join(os.getcwd(), "log.log"))
    # log to stdout
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    parse_args_and_start(sys.argv[1:])
