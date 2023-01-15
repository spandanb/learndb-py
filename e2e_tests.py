"""
This is a collection of simple end-to-end tests. These should
validate the correctness of any interaction loop that the user
can initiate.
"""
import pytest

from constants import TEST_DB_FILE
from learndb import LearnDB


# utils


def read_columns_from_pipe(pipe, column_indices):
    """
    Utility to read from pipe and return list of tuples
    """
    results = []
    while pipe.has_msgs():
        record = pipe.read()
        tpl = tuple(record.at_index(idx) for idx in column_indices)
        #tpl = tuple(record.get(column_name) for column_name in column_names)
        results.append(tpl)
    return results



@pytest.fixture
def db0():
    """
    Return db with schema 0 loaded
    Doesn't seem to work
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()
    commands = [
        "create table foo ( cola integer primary key, colb integer)",
        "insert into foo (cola, colb) values (1, 2)",
        "insert into foo (cola, colb) values (2, 4)",
        "insert into foo (cola, colb) values (3, 6)",
    ]
    for cmd in commands:
        resp = db.handle_input(cmd)
        assert resp.success, f"{cmd} failed with {resp.error_message}"

    return db

# tests


def test_create_table():
    """
    create table and check existence in catalog
    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)

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
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()
    # output pipe
    pipe = db.get_pipe()

    # create table
    db.handle_input("create table foo ( cola integer primary key, colb text)")

    # insert into table
    db.handle_input("insert into foo (cola, colb) values (4, 'hellew words')")

    # verify data
    db.handle_input("select cola, colb from foo")

    assert pipe.has_msgs(), "expected messages"
    record = pipe.read()
    assert record.get("cola") == 4
    assert record.get("colb") == "hellew words"


def test_select_no_condition():
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()
    commands = [
        "create table foo ( cola integer primary key, colB integer, colc integer, cold integer)",
        "insert into foo (cola, colb, colc, cold) values (1, 2, 31, 4)",
        "insert into foo (cola, colb, colc, cold) values (2, 4, 6, 8)"
    ]
    for cmd in commands:
        resp = db.handle_input(cmd)
        assert resp.success, f"{cmd} failed with {resp.error_message}"

    db.handle_input("select cola from foo")
    actual_keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        actual_keys.append(record.get("cola"))

    assert actual_keys == [1, 2]


def test_select_no_condition_mixed_type_schema():
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()
    commands = [
        "create table foo ( cola integer primary key, colb text)",
        "insert into foo (cola, colb) values (1, 'car')",
        "insert into foo (cola, colb) values (2, 'monkey')"
    ]
    for cmd in commands:
        resp = db.handle_input(cmd)
        assert resp.success, f"{cmd} failed with {resp.error_message}"

    db.handle_input("select cola from foo")
    actual_keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        actual_keys.append(record.get("cola"))

    assert actual_keys == [1, 2]


def test_select_equality0(db0):
    """
    test select with an equality condition

    :return:
    """

    # case 1
    db0.handle_input("select cola from foo where cola = 1")
    keys = []
    while db0.get_pipe().has_msgs():
        record = db0.get_pipe().read()
        keys.append(record.get("cola"))
    assert keys == [1]


def test_select_equality1():
    """
    test select with an equality condition

    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()

    commands = [
        "create table foo ( cola integer primary key, colB integer, colc integer, cold integer)",
        "insert into foo (cola, colB, colc, cold) values (1, 2, 31, 4)",
        "insert into foo (cola, colB, colc, cold) values (2, 4, 6, 8)",
        "insert into foo (cola, colB, colc, cold) values (3, 10, 3, 8)",
        "insert into foo (cola, colB, colc, cold) values (4, 6, 90, 8)",
    ]

    for cmd in commands:
        db.handle_input(cmd)

    db.handle_input("select cola from foo where colB = 4 AND colc = 6")
    keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append(record.get("cola"))
    assert keys == [2]

    db.handle_input("select cola from foo where colB = 4 AND colc = 6 OR colc = 3")
    keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append(record.get("cola"))
    assert keys == [2, 3]


def test_select_equality_with_alias():
    """
    For a simple select, i.e. single data source with no-joins,
    the source may or may not have an alias.
    Not sure if this ANSI SQL; but I will enforce that
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)

    # create table
    db.handle_input("create table foo ( cola integer primary key, colB integer, colc integer, cold integer)")
    # insert into table
    db.handle_input("insert into foo (cola, colb, colc, cold) values (1, 2, 31, 4)")
    db.handle_input("insert into foo (cola, colb, colc, cold) values (2, 4, 6, 8)")
    db.handle_input("insert into foo (cola, colb, colc, cold) values (3, 10, 3, 8)")
    db.handle_input("insert into foo (cola, colb, colc, cold) values (4, 6, 90, 8)")
    # NOTE: since f is an alias for foo; all columns must be scoped with f
    db.handle_input("select f.cola from foo f where f.colb = 4 AND f.colc = 6 OR f.colc = 3")
    keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append(record.get("f.cola"))
    assert keys == [2, 3]


def test_select_inequality():
    """
    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)

    # create table
    db.handle_input("create table foo ( cola integer primary key, colB integer, colc integer, cold integer)")
    # insert into table
    db.handle_input("insert into foo (cola, colb, colc, cold) values (1, 2, 31, 4)")
    db.handle_input("insert into foo (cola, colb, colc, cold) values (2, 4, 6, 8)")
    db.handle_input("insert into foo (cola, colb, colc, cold) values (3, 10, 3, 8)")
    db.handle_input("select f.cola from foo f where f.cola < 3")
    keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append(record.get("f.cola"))
    assert keys == [1, 2]


def test_select_on_real_column():
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    statements = [
        "create table foo (cola integer primary key, colb real)",
        "insert into foo (cola, colb) values (1, 1.1)",
        "insert into foo (cola, colb) values (2, 2.2)",
        "select f.cola from foo f where f.colb <> 1.1"
    ]
    for stmnt in statements:
        db.handle_input(stmnt)

    keys = []
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append((record.get("f.cola")))
    assert keys == [2]


def test_select_group_by():
    """
    Group by
    """

    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()

    commands = [
        "create table items ( custid integer primary key, country integer)",
        "insert into items (custid, country) values (10, 1)",
        "insert into items (custid, country) values (20, 1)",
        "insert into items (custid, country) values (100, 2)",
        "insert into items (custid, country) values (200, 2)",
        "insert into items (custid, country) values (300, 2)",
        # "select f.cola from foo f group by f.colb, f.cola",
        "select count(custid), country from items group by country",
    ]

    for cmd in commands:
        resp = db.handle_input(cmd)
        assert resp.success, f"{cmd} failed with {resp.error_message}"

    pipe = db.get_pipe()
    assert pipe.has_msgs()
    values = read_columns_from_pipe(pipe, [0, 1])
    assert values == [(2, 1), (3, 2)]


def test_select_group_by_having():

    commands = [
        "create table items ( custid integer primary key, country integer)",
        "insert into items (custid, country) values (10, 1)",
        "insert into items (custid, country) values (20, 1)",
        "insert into items (custid, country) values (100, 2)",
        "insert into items (custid, country) values (200, 2)",
        "insert into items (custid, country) values (300, 2)",
        # "select f.cola from foo f group by f.colb, f.cola",
        "select count(custid), country from items group by country having count(cust_id) > 1",
    ]
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()

    for cmd in commands:
        resp = db.handle_input(cmd)
        assert resp.success, f"{cmd} failed with {resp.error_message}"

    pipe = db.get_pipe()
    assert pipe.has_msgs()
    values = read_columns_from_pipe(pipe, [0, 1])
    assert values == [(3, 2)]


def test_select_having():
    """
    No group by, only having
    "select "T" from items having count(cust_id) > 1",
    """


def test_select_algebraic_expr():
    """
    The goal is to test some algebra in the selectable expr
    """
    texts = [
        "create table items ( custid integer primary key, country integer)",
        "insert into items (custid, country) values (10, 1)",
        "insert into items (custid, country) values (20, 1)",
        "insert into items (custid, country) values (100, 2)",
        "insert into items (custid, country) values (200, 2)",
        "insert into items (custid, country) values (300, 2)",
        # "select f.cola from foo f group by f.colb, f.cola",
        # "select count(custid), country from items group by country",
        "select (count(custid) + 1) * 2, from items",
    ]


def test_inner_join():
    """

    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
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

    pipe = db.get_pipe()
    assert pipe.has_msgs()
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append(record.get("b.colx"))
    keys.sort()
    assert keys == [101, 102]


def test_left_join():
    """
    test left-outer join
    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
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
    db.handle_input("select b.colx, b.coly, b.colz from foo f left join bar b on f.colb = b.coly")

    keys = []

    assert db.get_pipe().has_msgs()
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append(record.get("b.colx"))
    # create a set, because we can't sort an array of ints and None
    expected_keys = {None, 101, 102}
    for expected_key in expected_keys:
        assert expected_key in keys


def test_cross_join():
    """
    test left-outer join
    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()

    # create table
    db.handle_input("create table foo ( cola integer primary key, colb integer)")
    db.handle_input("create table bar ( colx integer primary key, coly integer)")
    # insert into table
    db.handle_input("insert into foo (cola, colb) values (1, 2)")
    db.handle_input("insert into foo (cola, colb) values (2, 4)")
    db.handle_input("insert into foo (cola, colb) values (3, 10)")
    db.handle_input("insert into bar (colx, coly) values (98, 10)")
    db.handle_input("insert into bar (colx, coly) values (99, 4)")
    # select
    db.handle_input("select b.colx, f.cola from foo f cross join bar b")

    keys = []

    assert db.get_pipe().has_msgs()
    while db.get_pipe().has_msgs():
        record = db.get_pipe().read()
        keys.append((record.get("f.cola"), record.get("b.colx")))
    keys.sort()
    assert keys == [(1, 98), (1, 99), (2, 98), (2, 99), (3, 98), (3, 99)]


def test_delete_equality_on_primary_column():
    """
    test delete with equality condition
    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()

    # create table
    db.handle_input("create table foo ( cola integer primary key, colb text)")
    # insert
    db.handle_input("insert into foo (cola, colb) values (4, 'hello world')")
    db.handle_input("insert into foo (cola, colb) values (5, 'bye world')")
    # delete
    db.handle_input("delete from foo where cola = 4")
    # verify data
    cmd = "select cola, colb  from foo"
    db.handle_input(cmd)
    pipe = db.get_pipe()
    assert pipe.has_msgs(), "expected rows"

    actual_keys = []
    while pipe.has_msgs():
        record = pipe.read()
        actual_keys.append(record.get("cola"))

    assert actual_keys == [5]


def test_delete_equality_on_non_primary_column_str_column():
    """

    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()

    # create table
    db.handle_input("create table foo ( cola integer primary key, colb text)")
    # insert
    db.handle_input("insert into foo (cola, colb) values (4, 'hello world')")
    db.handle_input("insert into foo (cola, colb) values (5, 'hey world')")
    db.handle_input("insert into foo (cola, colb) values (6, 'bye world')")

    # delete
    db.handle_input("delete from foo where colb = 'hello world'")

    # verify data
    cmd = "select cola, colb  from foo"
    db.handle_input(cmd)
    pipe = db.get_pipe()
    assert pipe.has_msgs(), "expected rows"

    keys = []
    while pipe.has_msgs():
        record = pipe.read()
        keys.append(record.get("cola"))

    assert keys == [5, 6]


def test_delete_equality_on_non_primary_column_int_column():
    """

    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    db.nuke_dbfile()

    # create table
    db.handle_input("create table foo ( cola integer primary key, colb text)")
    # insert
    db.handle_input("insert into foo (colA, colB) values (4, 'hello world')")
    db.handle_input("insert into foo (colA, colB) values (5, 'hey world')")
    db.handle_input("insert into foo (colA, colB) values (6, 'bye world')")

    # delete
    db.handle_input("delete from foo where colb = 'hello world'")

    # verify data
    cmd = "select cola, colb  from foo"
    db.handle_input(cmd)
    pipe = db.get_pipe()
    assert pipe.has_msgs(), "expected rows"

    keys = []
    while pipe.has_msgs():
        record = pipe.read()
        keys.append(record.get("cola"))

    assert keys == [5, 6]


def test_delete_inequality():
    """
    test delete with inequality condition
    :return:
    """
    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
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


def test_failure_invalid_column_access():
    """
    This should attempt read on a non-existent column
    """


def test_failure_create_table_with_non_int_primary_key():
    """
    Primary keys must be integers; hence below statement should fail
    """
    "create table foo (cola real primary key, colB integer)"


def test_failure_invalid_insert():
    # NOTE: the  inserts should fail, since they ref more columns than what's in the schema, and in the value lists
    # TODO: create separate fail cases for failures: 1) value list has more columns than schema; the opposite is okay- which
    # just means there are null values
    #  2)  value list is of a different length than column name list
    commands = [
        "create table foo ( cola integer primary key, colb text)",
        "insert into foo (cola, colb, colc, cold) values (1, 'car')",
        "insert into foo (cola, colb, colc, cold) values (2, 'monkey')"
    ]


def test_bugfix_mixed_case_identifiers():
    # this is only for select
    # ensure fix- i.e. to lowercase all identifiers at parse time
    # or do all comparison/checks in a case insensitive-
    # works for all statements

    db = LearnDB(TEST_DB_FILE, nuke_db_file=True)
    commands = [
        "create table foo ( colA integer primary key, colB text)",
        "insert into foo (colA, colB) values (4, 'hello world')",
    ]
    for cmd in commands:
        db.handle_input(cmd)

    # verify data
    cmd = "select colA, colB  from foo"
    db.handle_input(cmd)
    pipe = db.get_pipe()
    assert pipe.has_msgs(), "expected rows"
