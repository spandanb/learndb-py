"""
Creating this stub for end-to-end tests. These
should take the form of valid input and output functionally
correct output.
"""
import os.path

from constants import TEST_DB_FILE
from learndb import input_handler
from pipe import Pipe
from statemanager import StateManager
from virtual_machine import VirtualMachine


def test_create_table():
    """
    create table and check existence in catalog
    :return:
    """
    if os.path.exists(TEST_DB_FILE):
        # use a temp file instead?
        os.remove(TEST_DB_FILE)

    state_manager = StateManager(TEST_DB_FILE)

    # output pipe
    pipe = Pipe()

    # create virtual machine
    virtmachine = VirtualMachine(state_manager, pipe)

    # create table
    cmd = "create table foo ( colA integer primary key, colB text)"
    input_handler(cmd, virtmachine)

    # query catalog to ensure table is created
    cmd = "select pkey, root_pagenum, name from catalog"
    input_handler(cmd, virtmachine)

    assert pipe.has_msgs(), "expected messages"
    catalog_record = pipe.read()
    assert catalog_record.get("name") == "foo"


def test_create_table_and_insert():
    """
    create table and insert into table and verify resultset
    :return:
    """
    if os.path.exists(TEST_DB_FILE):
        # use a temp file instead?
        os.remove(TEST_DB_FILE)

    state_manager = StateManager(TEST_DB_FILE)

    # output pipe
    pipe = Pipe()

    # create virtual machine
    virtmachine = VirtualMachine(state_manager, pipe)

    # create table
    cmd = "create table foo ( colA integer primary key, colB text)"
    input_handler(cmd, virtmachine)

    # query catalog to ensure table is created
    cmd = "select pkey, root_pagenum, name from catalog"
    input_handler(cmd, virtmachine)

    assert pipe.has_msgs(), "expected messages"
    catalog_record = pipe.read()
    assert catalog_record.get("name") == "foo"

    # insert into table
    cmd = "insert into foo (colA, colB) values (4, 'hellew words')"
    input_handler(cmd, virtmachine)

    # verify data
    cmd = "select colA, colB  from foo"
    input_handler(cmd, virtmachine)

    assert pipe.has_msgs(), "expected messages"
    record = pipe.read()
    assert record.get("colA") == 4
    assert record.get("colB") == "hellew words"
