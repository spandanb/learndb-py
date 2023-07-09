from __future__ import annotations
"""
This module contains the highest level user-interaction and resource allocation
i.e. management of entities, like parser, virtual machine, pager, etc. that implement
the DBMS functionality that is learndb.
"""
import os
import os.path
import sys
import logging

from typing import List

from .constants import DB_FILE, USAGE, EXIT_SUCCESS, EXIT_FAILURE
from .lang_parser.sqlhandler import SqlFrontEnd
from .lang_parser.symbols import Program
from .dataexchange import Response, MetaCommandResult
from .pipe import Pipe
from .stress import run_add_del_stress_suite
from .virtual_machine import VirtualMachine, VMConfig


# section: core execution/user-interface logic

def config_logging():
    # config logger
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s ] %(message)s"
    # log to file
    # logging.basicConfig(format=FORMAT, level=logging.DEBUG, filename=os.path.join(os.getcwd(), "log.log"))
    # log to stdout
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)


class LearnDB:
    """
    This provides programmatic interface for interacting with databases managed by Learndb.
    This class defines the handle

    An example flow is like:
    ```
    # create handler instance
    db = LearnDB(db_filepath)

    # submit statement
    resp = db.handle_input("select col_a from foo")
    assert resp.success

    # below are only needed to read results of statements that produce output
    # get output pipe
    pipe = db.get_pipe()

    # print rows
    while pipe.has_msgs():
        print(pipe.read())

    # close handle - flushes any in-memory state
    db.close()
    ```
    """

    def __init__(self, db_filepath: str, nuke_db_file: bool = False):
        """
        :param db_filepath: path to DB file; i.e. file that stores state of this database
        :param nuke_db_file: whether to nuke the file before self is initialized
        """
        self.db_filepath = db_filepath
        # NOTE: the method
        if nuke_db_file and os.path.exists(self.db_filepath):
            os.remove(self.db_filepath)
        self.pipe = None
        self.virtual_machine = None
        self.configure()
        self.reset()

    def reset(self):
        """
        Reset state. Recreates pipe and virtual_machine.
        """
        config = VMConfig(self.db_filepath)
        self.pipe = Pipe()
        if self.virtual_machine:
            self.virtual_machine.terminate()
        self.virtual_machine = VirtualMachine(config, self.pipe)

    def configure(self):
        """
        Handle any configuration tasks
        """
        config_logging()

    def nuke_dbfile(self):
        """
        remove db file.
        This effectively restarts the instance into a clean state.
        :return:
        """
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
        NOTE: must be called before exiting, to persist data to disk
        :return:
        """
        self.virtual_machine.terminate()

    def handle_input(self, input_buffer: str) -> Response:
        """
        handle input- parse and execute

        :param input_buffer:
        :return:
        """
        return self.input_handler(input_buffer)

    @staticmethod
    def is_meta_command(command: str) -> bool:
        return command and command[0] == '.'

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
            self.virtual_machine.state_manager.print_tree(tree_name)
            print("Finished printing tree" + "-"*50)
            return Response(True, status=MetaCommandResult.Success)
        elif command == ".validate":
            print("Validating tree....")
            splits = command.split(" ")
            if len(splits) != 2:
                print("Invalid argument to .validate| Usage: > .validate <table-name>")
                return Response(False, status=MetaCommandResult.InvalidArgument)
            tree_name = splits[1]
            self.virtual_machine.state_manager.validate_tree(tree_name)
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
        return self.virtual_machine.run(program)

    def input_handler(self, input_buffer: str) -> Response:
        """
        receive input, parse input, and execute vm.

        :param input_buffer:
        :return:
        """
        if self.is_meta_command(input_buffer):
            m_resp = self.do_meta_command(input_buffer)
            if m_resp.success:
                return Response(True, status=MetaCommandResult.Success)

            print("Unable to process meta command")
            return Response(False, status=m_resp.status)

        p_resp = self.prepare_statement(input_buffer)
        if not p_resp.success:
            return Response(False, error_message=p_resp.error_message)

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


def repl(db_filepath: str = DB_FILE):
    """
    REPL (read-eval-print loop) for learndb
    """

    # create Learndb handler
    db = LearnDB(db_filepath)

    print("Welcome to learndb")
    print("For help use .help")
    while True:
        input_buffer = input("db > ")
        resp = db.handle_input(input_buffer)
        if not resp.success:
            print(f"Command execution failed due to [{resp.error_message}] ")
            continue

        # get output pipe
        pipe = db.get_pipe()

        while pipe.has_msgs():
            print(pipe.read())


def run_file(input_filepath: str, db_filepath: str = DB_FILE) -> Response:
    """
    Execute statements in file.
    """
    # create Learndb handler
    db = LearnDB(db_filepath)

    if not os.path.exists(input_filepath):
        return Response(False, error_message=f"Argument file [{input_filepath}] not found")

    with open(input_filepath) as fp:
        contents = fp.read()

    resp = db.handle_input(contents)
    if not resp.success:
        print(f"Command execution failed due to [{resp.error_message}] ")

    # get output pipe
    pipe = db.get_pipe()

    while pipe.has_msgs():
        print(pipe.read())

    db.close()


def run_stress(db_filepath: str = DB_FILE):
    """
    Run stress test
    """
    db = LearnDB(db_filepath)
    run_add_del_stress_suite(db)



def devloop():
    # todo: nuke me
    
    db = LearnDB(DB_FILE)  # nuke_db_file=True)

    #texts = ["select name, salary from employees order by salary"]
    texts = ["select name, salary from employees order by salary asc, name desc"]
    texts = [
        """CREATE TABLE fruits ( 
        id INTEGER PRIMARY KEY, 
        name TEXT, 
        avg_weight INTEGER)
        """,
        "insert into fruits (id, name, avg_weight) values (1, 'apple', 200)",
        "insert into fruits (id, name, avg_weight) values (2, 'orange', 140)",
        "insert into fruits (id, name, avg_weight) values (3, 'pineapple', 1000)",
        "insert into fruits (id, name, avg_weight) values (4, 'grape', 5)",
        "insert into fruits (id, name, avg_weight) values (5, 'pear', 166)",
        "insert into fruits (id, name, avg_weight) values (6, 'mango', 150)",
        "insert into fruits (id, name, avg_weight) values (7, 'watermelon', 10000)",
        "insert into fruits (id, name, avg_weight) values (8, 'banana', 118)",
        "insert into fruits (id, name, avg_weight) values (9, 'peach', 147)",
        #"select name, id from fruits order by id limit 5"
    ]
    texts = [
        "select name, avg_weight from fruits order by avg_weight, name desc limit 4"
    ]

    for text in texts:
        logging.info(f"handling. {text}")
        resp = db.handle_input(text)
        logging.info(f"received resp: {resp}")
        while db.pipe.has_msgs():
            logging.info("read from pipe: {}".format(db.pipe.read()))

    db.close()



def parse_args_and_start(args: List):
    """
    parse args and starts
    :return:
    """
    args_description = """Usage:
python run.py repl
    // start repl
python run.py devloop
    // start a dev-loop function
python run.py file <filepath>
    // read file at <filepath>
    """
    if len(args) < 1:
        print("Error: run-mode not specified")
        print(args_description)
        return

    runmode = args[0].lower()
    if runmode == "repl":
        repl()
    elif runmode == "stress":
        run_stress()
    elif runmode == "devloop":
        devloop()
    elif runmode == "file":
        # todo:  and output file
        # if no output file, write to console
        if len(args) < 2:
            print("Error: Expected input filepath")
            print(args_description)
            return
        input_filepath = args[1].lower()
        run_file(input_filepath)
    else:
        print(f"Error: Invalid run mode [{runmode}]")
        print(args_description)
        return

