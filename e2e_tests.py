"""
Creating this stub for end-to-end tests. These
should take the form of valid input and output functionally
correct output.
"""


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


def test_create_table_and_insert():
    """
    create table and insert into table and verify resultset
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

    # insert into table
    cmd = "insert into foo (colA, colB) values (4, 'hellew words')"
    db.handle_input(cmd)

    # verify data
    cmd = "select colA, colB  from foo"
    db.handle_input(cmd)

    assert pipe.has_msgs(), "expected messages"
    record = pipe.read()
    assert record.get("colA") == 4
    assert record.get("colB") == "hellew words"


def test_join():
    pass