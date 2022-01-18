"""
This is a collection of simple end-to-end tests. These should
validate the correctness of any interaction loop that the user
can initiate.
"""
import pytest

from constants import TEST_DB_FILE
from learndb import LearnDB


def test_create_table():
    """
    create table and check existence in catalog
    :return:
    """
    db = LearnDB(TEST_DB_FILE)
    db.nuke_dbfile()

    # output pipe
    pipe = db.get_pipe()

    # create table
    cmd = "create table foo ( colA integer primary key, colB text)"
    db.handle_input(cmd)

    # query catalog to ensure table is created
    cmd = "select pkey, root_pagenum, name from catalog"
    db.handle_input(cmd)

    assert pipe.has_msgs(), "expected messages"
    catalog_record = pipe.read()
    assert catalog_record.get("name") == "foo"


def test_insert():
    """
    create table and insert into table and verify resultset
    NOTE: more thorough insert/delete ops are covered in btree_tests.py
    :return:
    """
    db = LearnDB(TEST_DB_FILE)
    db.nuke_dbfile()
    # output pipe
    pipe = db.get_pipe()

    # create table
    db.handle_input("create table foo ( colA integer primary key, colB text)")

    # insert into table
    db.handle_input("insert into foo (colA, colB) values (4, 'hellew words')")

    # verify data
    db.handle_input("select colA, colB from foo")

    assert pipe.has_msgs(), "expected messages"
    record = pipe.read()
    assert record.get("colA") == 4
    assert record.get("colB") == "hellew words"


def test_select_equality():
    """
    test select with an equality condition

    :return:
    """
    db = LearnDB(TEST_DB_FILE)
    db.nuke_dbfile()

    # create table
    db.handle_input("create table foo ( cola integer primary key, colB integer, colc integer, cold integer)")
    # insert into table
    db.handle_input("insert into foo (cola, colb, colc, cold) values (1, 2, 31, 4)")
    db.handle_input("insert into foo (cola, colb, colc, cold) values (2, 4, 6, 8)")
    db.handle_input("insert into foo (cola, colb, colc, cold) values (3, 10, 3, 8)")
    db.handle_input("insert into foo (cola, colb, colc, cold) values (4, 6, 90, 8)")

    # case 1
    db.handle_input("select cola from foo where cola = 1")
    keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append(record.get("cola"))
    assert keys == [1]

    db.handle_input("select cola from foo where colb = 4 AND colc = 6")
    keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append(record.get("cola"))
    assert keys == [2]

    db.handle_input("select cola from foo where colb = 4 AND colc = 6 OR colc = 3")
    keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append(record.get("cola"))
    assert keys == [2, 3]


def test_select_inequality():
    """

    :return:
    """


def test_join():
    """

    :return:
    """
    db = LearnDB(TEST_DB_FILE)
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
        keys.append(record.get("b", "colx"))
    # TODO: this is not working
    assert keys == [101, 102, 103, "this is not a key"]




def test_delete_equality_on_primary_column():
    """
    test delete with equality condition
    :return:
    """
    db = LearnDB(TEST_DB_FILE)
    db.nuke_dbfile()

    # create table
    db.handle_input("create table foo ( colA integer primary key, colB text)")
    # insert
    db.handle_input("insert into foo (colA, colB) values (4, 'hellew words')")
    # delete
    db.handle_input("delete from foo where colA = 4")
    # verify data
    cmd = "select colA, colB  from foo"
    db.handle_input(cmd)
    pipe = db.get_pipe()
    assert not pipe.has_msgs(), "expected no rows"


def test_delete_equality_on_non_primary_column():
    """

    :return:
    """
    db = LearnDB(TEST_DB_FILE)
    db.nuke_dbfile()

    # create table
    db.handle_input("create table foo ( colA integer primary key, colB text)")
    # insert
    db.handle_input("insert into foo (colA, colB) values (4, 'hello world')")
    db.handle_input("insert into foo (colA, colB) values (5, 'hey world')")
    db.handle_input("insert into foo (colA, colB) values (6, 'bye world')")

    # delete
    db.handle_input("delete from foo where colB = 'hello world'")

    # verify data
    cmd = "select colA, colB  from foo"
    db.handle_input(cmd)
    pipe = db.get_pipe()
    assert pipe.has_msgs(), "expected rows"

    keys = []
    while pipe.has_msgs():
        record = pipe.read()
        keys.append(record.get("colA"))

    assert keys == [5, 6]


def test_delete_inequality():
    """
    test delete with inequality condition
    :return:
    """
    db = LearnDB(TEST_DB_FILE)
    db.nuke_dbfile()

    # create table
    db.handle_input("create table foo ( colA integer primary key, colB text)")
    # insert
    db.handle_input("insert into foo (colA, colB) values (4, 'hellew words')")
    # delete
    db.handle_input("delete from foo where colA = 4")
    # verify data
    cmd = "select colA, colB  from foo"
    db.handle_input(cmd)
    pipe = db.get_pipe()
    assert not pipe.has_msgs(), "expected no rows"


def test_join():
    """
    test join operation
    :return:
    """


def test_update():
    """
    test update statement
    :return:
    """


def test_truncate():
    """
    test truncate stmnt
    :return:
    """


def test_drop():
    """
    test drop table stmnt
    :return:
    """